import os, io, csv
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

try:
    import openpyxl
except Exception:
    openpyxl = None

load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
TZ_BOGOTA = ZoneInfo("America/Bogota")

def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)

class GestionRutas:
    """
    Gestión CRUD de la tabla config.rutas.
    Estructura esperada:
        config.rutas (
            id           BIGSERIAL PRIMARY KEY,
            id_linea     BIGINT NOT NULL,          -- clave de negocio / código de línea
            ruta_comercial TEXT NOT NULL,           -- nombre comercial de la ruta
            id_cop       BIGINT NOT NULL REFERENCES config.cop(id),
            estado       SMALLINT NOT NULL DEFAULT 1,
            created_at   TIMESTAMPTZ DEFAULT now(),
            updated_at   TIMESTAMPTZ DEFAULT now()
        )
    """

    def __init__(self):
        self.connection = psycopg2.connect(
            DATABASE_PATH, options="-c timezone=America/Bogota"
        )
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        with self.connection.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.connection.commit()

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None):
            self.connection.close()

    # ── Helpers ─────────────────────────────────────────────────────────────
    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        self.cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return self.cursor.fetchone() is not None

    def _cop_joins_and_exprs(self):
        """Devuelve (joins_sql, comp_expr, zona_expr) según la estructura real de config.cop."""
        has_id_comp = self._col_exists("config", "cop", "id_componente")
        has_id_zona = self._col_exists("config", "cop", "id_zona")
        joins, comp_expr, zona_expr = [], "NULL", "NULL"
        if has_id_comp:
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        if has_id_zona:
            joins.append("LEFT JOIN config.zona z ON z.id = c.id_zona")
            zona_expr = "z.zona"
        return (" ".join(joins), comp_expr, zona_expr)

    def _fila_o_error(self, cur):
        fila = cur.fetchone()
        if not fila:
            raise ValueError("Registro no encontrado")
        return fila

    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _filtro_busqueda(self, q: str):
        if not q:
            return "", []
        qn = f"%{q.strip().upper()}%"
        return (
            " AND (UPPER(r.ruta_comercial) LIKE %s OR CAST(r.id_linea AS TEXT) LIKE %s) ",
            [qn, qn],
        )

    def _u(self, s):
        if s is None:
            return None
        return str(s).strip().upper()

    def _resolver_id_cop(self, id_cop=None, nombre_cop=None):
        if id_cop:
            return int(id_cop)
        if nombre_cop:
            self.cursor.execute(
                "SELECT id FROM config.cop WHERE UPPER(cop) = %s",
                (nombre_cop.strip().upper(),),
            )
            row = self.cursor.fetchone()
            if not row:
                raise ValueError(f"COP '{nombre_cop}' no existe")
            return int(row["id"])
        raise ValueError("Centro de Operación no especificado (ID COP o nombre)")

    # ── Listar rutas ─────────────────────────────────────────────────────────
    def listar_rutas(
        self,
        q=None,
        estado=None,
        id_cop=None,
        componente=None,
        zona=None,
        pagina: int = 1,
        tamano: int = 10,
    ):
        where, params = " WHERE 1=1 ", []

        s, sp = self._filtro_busqueda(q)
        where += s
        params += sp

        if estado is not None:
            where += " AND r.estado=%s "
            params.append(int(estado))
        if id_cop:
            where += " AND r.id_cop=%s "
            params.append(int(id_cop))

        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        if componente:
            where += f" AND UPPER({comp_expr})=%s "
            params.append(self._u(componente))
        if zona:
            where += f" AND UPPER({zona_expr})=%s "
            params.append(self._u(zona))

        sql_count = f"""
            SELECT COUNT(1)
            FROM config.rutas r
            JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where}
        """
        self.cursor.execute(sql_count, params)
        total = self.cursor.fetchone()["count"]

        order = " ORDER BY r.id_linea ASC, r.ruta_comercial ASC "
        pag, pp = self._paginacion(pagina, tamano)
        sqlq = f"""
            SELECT
                r.id,
                r.id_linea,
                r.ruta_comercial,
                r.estado,
                r.id_cop,
                c.cop,
                {comp_expr} AS componente,
                {zona_expr} AS zona,
                to_char(r.created_at, 'YYYY-MM-DD HH24:MI') AS created_at,
                to_char(r.updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at
            FROM config.rutas r
            JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where} {order} {pag}
        """
        self.cursor.execute(sqlq, params + pp)
        return self.cursor.fetchall(), total

    # ── CRUD ─────────────────────────────────────────────────────────────────
    def crear_ruta(
        self,
        id_linea: int,
        ruta_comercial: str,
        id_cop: int,
        estado: int = 1,
    ):
        ruta_u = self._u(ruta_comercial)
        if not ruta_u:
            raise ValueError("La ruta comercial es obligatoria")
        if not id_linea:
            raise ValueError("El ID de línea es obligatorio")

        try:
            self.cursor.execute(
                """
                INSERT INTO config.rutas (id_linea, ruta_comercial, id_cop, estado)
                VALUES (%s, %s, %s, %s)
                RETURNING id, id_linea, ruta_comercial, estado, id_cop,
                          to_char(created_at,'YYYY-MM-DD HH24:MI') AS created_at,
                          to_char(updated_at,'YYYY-MM-DD HH24:MI') AS updated_at
                """,
                (int(id_linea), ruta_u, int(id_cop), int(estado)),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return fila
        except errors.ForeignKeyViolation:
            self.connection.rollback()
            raise ValueError("El Centro de Operación (COP) no existe")
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe una ruta con ese ID de línea y ruta comercial")
        except Exception as e:
            self.connection.rollback()
            raise e

    def actualizar_ruta(
        self,
        id: int,
        id_linea: int,
        ruta_comercial: str,
        id_cop: int,
        estado: int = 1,
    ):
        ruta_u = self._u(ruta_comercial)
        if not ruta_u:
            raise ValueError("La ruta comercial es obligatoria")
        if not id_linea:
            raise ValueError("El ID de línea es obligatorio")

        try:
            self.cursor.execute(
                """
                UPDATE config.rutas
                SET id_linea=%s, ruta_comercial=%s, id_cop=%s, estado=%s, updated_at=now()
                WHERE id=%s
                RETURNING id, id_linea, ruta_comercial, estado, id_cop,
                          to_char(created_at,'YYYY-MM-DD HH24:MI') AS created_at,
                          to_char(updated_at,'YYYY-MM-DD HH24:MI') AS updated_at
                """,
                (int(id_linea), ruta_u, int(id_cop), int(estado), int(id)),
            )
            fila = self._fila_o_error(self.cursor)
            self.connection.commit()
            return fila
        except errors.ForeignKeyViolation:
            self.connection.rollback()
            raise ValueError("El Centro de Operación (COP) no existe")
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe una ruta con ese ID de línea y ruta comercial")
        except Exception as e:
            self.connection.rollback()
            raise e

    def cambiar_estado(self, id: int, estado: int):
        self.cursor.execute(
            """
            UPDATE config.rutas
            SET estado=%s, updated_at=now()
            WHERE id=%s
            RETURNING id, id_linea, ruta_comercial, estado,
                    to_char(created_at,'YYYY-MM-DD HH24:MI') AS created_at,
                    to_char(updated_at,'YYYY-MM-DD HH24:MI') AS updated_at
            """,
            (int(estado), int(id)),
        )
        fila = self._fila_o_error(self.cursor)
        self.connection.commit()
        return fila

    # ── Datos de apoyo ────────────────────────────────────────────────────────
    def listar_cop(self, componente: str = None, zona: str = None, estado: int = 1):
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        where, params = " WHERE 1=1 ", []
        if estado is not None:
            where += " AND c.estado=%s "
            params.append(int(estado))
        if componente:
            where += f" AND UPPER({comp_expr})=%s "
            params.append(self._u(componente))
        if zona:
            where += f" AND UPPER({zona_expr})=%s "
            params.append(self._u(zona))
        sql = f"""
            SELECT c.id, c.cop,
                   {comp_expr} AS componente,
                   {zona_expr} AS zona
            FROM config.cop c
            {joins_cop}
            {where}
            ORDER BY {comp_expr}, {zona_expr}, c.cop
        """
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def listar_componentes(self):
        joins_cop, comp_expr, _ = self._cop_joins_and_exprs()
        sql = f"""
            SELECT DISTINCT {comp_expr} AS componente
            FROM config.cop c
            {joins_cop}
            WHERE c.estado=1 AND {comp_expr} IS NOT NULL
            ORDER BY {comp_expr}
        """
        self.cursor.execute(sql)
        return [r["componente"] for r in self.cursor.fetchall() if r["componente"]]

    def listar_zonas(self, componente: str = None):
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        where, params = " WHERE c.estado=1 ", []
        if componente:
            where += f" AND UPPER({comp_expr})=%s "
            params.append(self._u(componente))
        sql = f"""
            SELECT DISTINCT {zona_expr} AS zona
            FROM config.cop c
            {joins_cop}
            {where} AND {zona_expr} IS NOT NULL
            ORDER BY {zona_expr}
        """
        self.cursor.execute(sql, params)
        return [r["zona"] for r in self.cursor.fetchall() if r["zona"]]

    # ── Normalización para carga masiva ───────────────────────────────────────
    def _normalizar_payload_archivo(self, d: dict):
        """
        Cabeceras esperadas (flexibles, insensibles a mayúsculas/espacios):
            Id_linea | id_linea | ID Linea
            Ruta_comercial | Ruta Comercial | ruta_comercial
            cop | Centro Operacion | id_cop | ID COP
        """

        def get(k):
            # Buscar clave insensible a mayúsculas/espacios/guiones bajos
            for key, val in d.items():
                norm = key.strip().replace(" ", "_").upper() if key else ""
                if norm == k.strip().replace(" ", "_").upper():
                    return None if val is None else str(val).strip()
            return None

        # id_linea
        id_linea_raw = get("Id_linea") or get("ID_Linea") or get("idlinea")
        if id_linea_raw:
            # Excel puede devolver 80.0
            if id_linea_raw.endswith(".0"):
                id_linea_raw = id_linea_raw[:-2]
        if not id_linea_raw or not id_linea_raw.isdigit():
            raise ValueError(f"Id_linea inválido: '{id_linea_raw}'")

        # ruta_comercial
        ruta_raw = get("Ruta_comercial") or get("Ruta_Comercial") or get("Ruta")
        if not ruta_raw:
            raise ValueError("Ruta_comercial vacía")

        # cop → resolver id_cop
        cop_raw = get("cop") or get("Centro_Operacion") or get("ID_COP") or get("id_cop")
        if not cop_raw:
            raise ValueError("Campo COP / Centro Operacion vacío")
        if cop_raw.replace(".", "").isdigit():
            val = cop_raw
            if val.endswith(".0"):
                val = val[:-2]
            id_cop = int(val)
        else:
            id_cop = self._resolver_id_cop(nombre_cop=cop_raw)

        return {
            "id_linea": int(id_linea_raw),
            "ruta_comercial": self._u(ruta_raw),
            "id_cop": id_cop,
            "estado": 1,
        }

    def _upsert_por_id_linea_ruta(self, payload):
        """Upsert: identifica la ruta por (id_linea, ruta_comercial)."""
        self.cursor.execute(
            "SELECT id FROM config.rutas WHERE id_linea=%s AND UPPER(ruta_comercial)=%s",
            (payload["id_linea"], self._u(payload["ruta_comercial"])),
        )
        row = self.cursor.fetchone()
        if row:
            return "actualizado", self.actualizar_ruta(id=row["id"], **payload)
        else:
            return "insertado", self.crear_ruta(**payload)

    # ── Carga masiva efectiva ─────────────────────────────────────────────────
    def carga_masiva_csv_bytes(self, contenido: bytes):
        texto = contenido.decode("utf-8", errors="ignore")
        lector = csv.DictReader(io.StringIO(texto))
        ins, upd, errs = 0, 0, []
        for i, fila in enumerate(lector, start=2):
            try:
                payload = self._normalizar_payload_archivo(fila)
                accion, _ = self._upsert_por_id_linea_ruta(payload)
                if accion == "insertado":
                    ins += 1
                else:
                    upd += 1
            except Exception as e:
                errs.append({"row": i, "error": str(e)})
        return {"insertados": ins, "actualizados": upd, "errores": errs}

    def carga_masiva_xlsx_bytes(self, contenido: bytes):
        if not openpyxl:
            raise ValueError("openpyxl no está disponible en el entorno")
        wb = openpyxl.load_workbook(io.BytesIO(contenido), data_only=True)
        ws = wb.active
        headers = [
            str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))
        ]
        ins, upd, errs = 0, 0, []
        for idx, fila in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            try:
                data = {
                    headers[i]: (fila[i] if i < len(fila) else None)
                    for i in range(len(headers))
                }
                payload = self._normalizar_payload_archivo(data)
                accion, _ = self._upsert_por_id_linea_ruta(payload)
                if accion == "insertado":
                    ins += 1
                else:
                    upd += 1
            except Exception as e:
                errs.append({"row": idx, "error": str(e)})
        return {"insertados": ins, "actualizados": upd, "errores": errs}

    # ── Previsualización (sin escritura en DB) ────────────────────────────────
    def _preview_rows(self, rows):
        muestra, ins, upd, errs = [], 0, 0, []
        for i, data in rows:
            try:
                payload = self._normalizar_payload_archivo(data)
                self.cursor.execute(
                    "SELECT 1 FROM config.rutas WHERE id_linea=%s AND UPPER(ruta_comercial)=%s",
                    (payload["id_linea"], self._u(payload["ruta_comercial"])),
                )
                existe = self.cursor.fetchone() is not None
                accion = "actualizado" if existe else "insertado"
                if accion == "insertado":
                    ins += 1
                else:
                    upd += 1
                if len(muestra) < 50:
                    muestra.append(
                        {
                            "fila": i,
                            "id_linea": payload["id_linea"],
                            "ruta_comercial": payload["ruta_comercial"],
                            "cop_id": payload["id_cop"],
                            "accion": accion,
                        }
                    )
            except Exception as e:
                errs.append({"row": i, "error": str(e)})
        return {"insertados": ins, "actualizados": upd, "errores": errs, "muestra": muestra}

    def previsualizar_csv_bytes(self, contenido: bytes):
        texto = contenido.decode("utf-8", errors="ignore")
        lector = csv.DictReader(io.StringIO(texto))
        rows = list((i, r) for i, r in enumerate(lector, start=2))
        return self._preview_rows(rows)

    def previsualizar_xlsx_bytes(self, contenido: bytes):
        if not openpyxl:
            raise ValueError("openpyxl no está disponible en el entorno")
        wb = openpyxl.load_workbook(io.BytesIO(contenido), data_only=True)
        ws = wb.active
        headers = [
            str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))
        ]
        rows = []
        for idx, fila in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            data = {
                headers[i]: (fila[i] if i < len(fila) else None)
                for i in range(len(headers))
            }
            rows.append((idx, data))
        return self._preview_rows(rows)