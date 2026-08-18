from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from database.database_manager import get_db_connection
import datetime


def _serial(v):
    return v.isoformat() if isinstance(v, (datetime.date, datetime.datetime)) else v


def _fila(row):
    if not row:
        return row
    return {k: _serial(v) for k, v in dict(row).items()}


def _filas(rows):
    return [_fila(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionTipoTarifa
# CRUD sobre config.tipo_tarifa  (KM | PAX | VEH)
# ═══════════════════════════════════════════════════════════════════════════════
class GestionTipoTarifa:

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    def listar(self, q=None, estado=None, pagina=1, tamano=10):
        where, params = " WHERE 1=1 ", []
        if q:
            where += " AND UPPER(tipo) LIKE %s "
            params.append(f"%{q.strip().upper()}%")
        if estado is not None:
            where += " AND estado = %s "
            params.append(bool(estado))

        self.cursor.execute(f"SELECT COUNT(1) FROM config.tipo_tarifa {where}", params)
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"SELECT id, tipo, estado FROM config.tipo_tarifa {where} ORDER BY tipo ASC LIMIT %s OFFSET %s",
            params + [tamano, off],
        )
        return _filas(self.cursor.fetchall()), total

    def listar_activos(self):
        self.cursor.execute(
            "SELECT id, tipo FROM config.tipo_tarifa WHERE estado = TRUE ORDER BY tipo ASC"
        )
        return _filas(self.cursor.fetchall())

    def crear(self, tipo: str):
        texto = tipo.strip().upper()
        if not texto:
            raise ValueError("El tipo de tarifa es obligatorio")
        try:
            self.cursor.execute(
                "INSERT INTO config.tipo_tarifa (tipo) VALUES (%s) RETURNING id, tipo, estado",
                (texto,),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe el tipo de tarifa '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def actualizar(self, id: int, tipo: str):
        texto = tipo.strip().upper()
        if not texto:
            raise ValueError("El tipo de tarifa es obligatorio")
        try:
            self.cursor.execute(
                "UPDATE config.tipo_tarifa SET tipo = %s WHERE id = %s RETURNING id, tipo, estado",
                (texto, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe el tipo de tarifa '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def cambiar_estado(self, id: int, estado: bool):
        try:
            self.cursor.execute(
                "UPDATE config.tipo_tarifa SET estado = %s WHERE id = %s RETURNING id, estado",
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e


# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionTipologiaBus
# CRUD sobre config.tipologia_bus
# ═══════════════════════════════════════════════════════════════════════════════
class GestionTipologiaBus:

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    def listar(self, q=None, estado=None, pagina=1, tamano=10):
        where, params = " WHERE 1=1 ", []
        if q:
            where += " AND UPPER(tipologia) LIKE %s "
            params.append(f"%{q.strip().upper()}%")
        if estado is not None:
            where += " AND estado = %s "
            params.append(bool(estado))

        self.cursor.execute(f"SELECT COUNT(1) FROM config.tipologia_bus {where}", params)
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"SELECT id, tipologia, estado FROM config.tipologia_bus {where} ORDER BY tipologia ASC LIMIT %s OFFSET %s",
            params + [tamano, off],
        )
        return _filas(self.cursor.fetchall()), total

    def listar_activos(self):
        self.cursor.execute(
            "SELECT id, tipologia FROM config.tipologia_bus WHERE estado = TRUE ORDER BY tipologia ASC"
        )
        return _filas(self.cursor.fetchall())

    def crear(self, tipologia: str):
        texto = tipologia.strip().upper()
        if not texto:
            raise ValueError("La tipología es obligatoria")
        try:
            self.cursor.execute(
                "INSERT INTO config.tipologia_bus (tipologia) VALUES (%s) RETURNING id, tipologia, estado",
                (texto,),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe la tipología '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def actualizar(self, id: int, tipologia: str):
        texto = tipologia.strip().upper()
        if not texto:
            raise ValueError("La tipología es obligatoria")
        try:
            self.cursor.execute(
                "UPDATE config.tipologia_bus SET tipologia = %s WHERE id = %s RETURNING id, tipologia, estado",
                (texto, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe la tipología '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def cambiar_estado(self, id: int, estado: bool):
        try:
            self.cursor.execute(
                "UPDATE config.tipologia_bus SET estado = %s WHERE id = %s RETURNING id, estado",
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e


# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionCombustible
# CRUD sobre config.combustible  (ACPM | GNV)
# ═══════════════════════════════════════════════════════════════════════════════
class GestionCombustible:

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    def listar(self, q=None, estado=None, pagina=1, tamano=10):
        where, params = " WHERE 1=1 ", []
        if q:
            where += " AND UPPER(combustible) LIKE %s "
            params.append(f"%{q.strip().upper()}%")
        if estado is not None:
            where += " AND estado = %s "
            params.append(bool(estado))

        self.cursor.execute(f"SELECT COUNT(1) FROM config.combustible {where}", params)
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"SELECT id, combustible, estado FROM config.combustible {where} ORDER BY combustible ASC LIMIT %s OFFSET %s",
            params + [tamano, off],
        )
        return _filas(self.cursor.fetchall()), total

    def listar_activos(self):
        self.cursor.execute(
            "SELECT id, combustible FROM config.combustible WHERE estado = TRUE ORDER BY combustible ASC"
        )
        return _filas(self.cursor.fetchall())

    def crear(self, combustible: str):
        texto = combustible.strip().upper()
        if not texto:
            raise ValueError("El combustible es obligatorio")
        try:
            self.cursor.execute(
                "INSERT INTO config.combustible (combustible) VALUES (%s) RETURNING id, combustible, estado",
                (texto,),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe el combustible '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def actualizar(self, id: int, combustible: str):
        texto = combustible.strip().upper()
        if not texto:
            raise ValueError("El combustible es obligatorio")
        try:
            self.cursor.execute(
                "UPDATE config.combustible SET combustible = %s WHERE id = %s RETURNING id, combustible, estado",
                (texto, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return _fila(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe el combustible '{texto}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def cambiar_estado(self, id: int, estado: bool):
        try:
            self.cursor.execute(
                "UPDATE config.combustible SET estado = %s WHERE id = %s RETURNING id, estado",
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e


# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionTarifas
# CRUD sobre config.tarifas con trazabilidad histórica por período de vigencia.
# La continuidad es validada por el trigger trg_continuidad_tarifas en BD.
# Los solapamientos son bloqueados por el EXCLUDE USING GIST sobre 'periodo'.
# ═══════════════════════════════════════════════════════════════════════════════
class GestionTarifas:

    _SQL_SELECT = """
        SELECT
            t.id,
            t.id_tipo_tarifa,
            tt.tipo           AS tipo_tarifa,
            t.id_tipologia,
            tb.tipologia,
            t.id_combustible,
            c.combustible,
            t.desde,
            t.hasta,
            t.tarifa,
            t.creado_en,
            t.actualizado_en,
            (t.hasta IS NULL)                                   AS vigente,
            COALESCE(t.hasta::text, 'vigente')                  AS hasta_label
        FROM config.tarifas t
        JOIN config.tipo_tarifa   tt ON tt.id = t.id_tipo_tarifa
        JOIN config.tipologia_bus tb ON tb.id = t.id_tipologia
        JOIN config.combustible   c  ON c.id  = t.id_combustible
    """

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # ── Listar con filtros y paginación ───────────────────────────────────────
    def listar(
        self,
        q=None,
        id_tipo_tarifa=None,
        id_tipologia=None,
        id_combustible=None,
        solo_vigentes=None,
        pagina=1,
        tamano=10,
    ):
        where, params = " WHERE 1=1 ", []

        if q:
            qn = f"%{q.strip().upper()}%"
            where += " AND (UPPER(tt.tipo) LIKE %s OR UPPER(tb.tipologia) LIKE %s OR UPPER(c.combustible) LIKE %s) "
            params += [qn, qn, qn]
        if id_tipo_tarifa:
            where += " AND t.id_tipo_tarifa = %s "
            params.append(int(id_tipo_tarifa))
        if id_tipologia:
            where += " AND t.id_tipologia = %s "
            params.append(int(id_tipologia))
        if id_combustible:
            where += " AND t.id_combustible = %s "
            params.append(int(id_combustible))
        if solo_vigentes is True:
            where += " AND t.hasta IS NULL "
        elif solo_vigentes is False:
            where += " AND t.hasta IS NOT NULL "

        sql_count = f"""
            SELECT COUNT(1)
            FROM config.tarifas t
            JOIN config.tipo_tarifa   tt ON tt.id = t.id_tipo_tarifa
            JOIN config.tipologia_bus tb ON tb.id = t.id_tipologia
            JOIN config.combustible   c  ON c.id  = t.id_combustible
            {where}
        """
        self.cursor.execute(sql_count, params)
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"{self._SQL_SELECT} {where} ORDER BY tt.tipo, tb.tipologia, c.combustible, t.desde DESC LIMIT %s OFFSET %s",
            params + [tamano, off],
        )
        return _filas(self.cursor.fetchall()), total

    # ── Consultar tarifa vigente en una fecha de transacción ──────────────────
    def consultar_por_fecha(self, fecha: str, id_tipo_tarifa=None, id_tipologia=None, id_combustible=None):
        where = " WHERE %s::date BETWEEN t.desde AND COALESCE(t.hasta, '9999-12-31'::date) "
        params: list = [fecha]

        if id_tipo_tarifa:
            where += " AND t.id_tipo_tarifa = %s "
            params.append(int(id_tipo_tarifa))
        if id_tipologia:
            where += " AND t.id_tipologia = %s "
            params.append(int(id_tipologia))
        if id_combustible:
            where += " AND t.id_combustible = %s "
            params.append(int(id_combustible))

        self.cursor.execute(
            f"{self._SQL_SELECT} {where} ORDER BY tt.tipo, tb.tipologia, c.combustible",
            params,
        )
        return _filas(self.cursor.fetchall())

    # ── Crear nueva tarifa ────────────────────────────────────────────────────
    def crear(self, id_tipo_tarifa: int, id_tipologia: int, id_combustible: int,
              desde: str, hasta, tarifa: float):
        if not desde:
            raise ValueError("La fecha 'desde' es obligatoria")
        if tarifa is None or float(tarifa) <= 0:
            raise ValueError("La tarifa debe ser un valor positivo")

        hasta_val = hasta if hasta else None
        try:
            self.cursor.execute(
                """
                INSERT INTO config.tarifas
                    (id_tipo_tarifa, id_tipologia, id_combustible, desde, hasta, tarifa)
                VALUES (%s, %s, %s, %s::date, %s::date, %s)
                RETURNING id, id_tipo_tarifa, id_tipologia, id_combustible, desde, hasta, tarifa
                """,
                (int(id_tipo_tarifa), int(id_tipologia), int(id_combustible),
                 desde, hasta_val, float(tarifa)),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return self._enriquecer(fila)
        except Exception as e:
            self.connection.rollback()
            raise ValueError(self._limpiar_error(str(e)))

    # ── Actualizar solo el valor de tarifa (fechas son inmutables) ─────────────
    def actualizar_valor(self, id: int, tarifa: float):
        if tarifa is None or float(tarifa) <= 0:
            raise ValueError("La tarifa debe ser un valor positivo")
        try:
            self.cursor.execute(
                """
                UPDATE config.tarifas SET tarifa = %s
                WHERE id = %s
                RETURNING id, id_tipo_tarifa, id_tipologia, id_combustible, desde, hasta, tarifa
                """,
                (float(tarifa), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return self._enriquecer(fila)
        except Exception as e:
            self.connection.rollback()
            raise ValueError(self._limpiar_error(str(e)))

    # ── Cerrar período: asignar fecha de fin a un registro vigente ─────────────
    def cerrar_periodo(self, id: int, hasta: str):
        if not hasta:
            raise ValueError("La fecha 'hasta' es obligatoria para cerrar el período")
        try:
            self.cursor.execute(
                """
                UPDATE config.tarifas
                SET hasta = %s::date
                WHERE id = %s AND hasta IS NULL
                RETURNING id, id_tipo_tarifa, id_tipologia, id_combustible, desde, hasta, tarifa
                """,
                (hasta, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado o ya tiene fecha de fin asignada")
            self.connection.commit()
            return self._enriquecer(fila)
        except Exception as e:
            self.connection.rollback()
            raise ValueError(self._limpiar_error(str(e)))

    # ── Datos de apoyo para dropdowns ─────────────────────────────────────────
    def listar_tipos(self):
        self.cursor.execute(
            "SELECT id, tipo FROM config.tipo_tarifa WHERE estado = TRUE ORDER BY tipo ASC"
        )
        return _filas(self.cursor.fetchall())

    def listar_tipologias(self):
        self.cursor.execute(
            "SELECT id, tipologia FROM config.tipologia_bus WHERE estado = TRUE ORDER BY tipologia ASC"
        )
        return _filas(self.cursor.fetchall())

    def listar_combustibles(self):
        self.cursor.execute(
            "SELECT id, combustible FROM config.combustible WHERE estado = TRUE ORDER BY combustible ASC"
        )
        return _filas(self.cursor.fetchall())

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _enriquecer(self, fila: dict) -> dict:
        if not fila:
            return fila
        d = _fila(fila)

        self.cursor.execute("SELECT tipo FROM config.tipo_tarifa WHERE id = %s", (d["id_tipo_tarifa"],))
        r = self.cursor.fetchone()
        d["tipo_tarifa"] = r["tipo"] if r else None

        self.cursor.execute("SELECT tipologia FROM config.tipologia_bus WHERE id = %s", (d["id_tipologia"],))
        r = self.cursor.fetchone()
        d["tipologia"] = r["tipologia"] if r else None

        self.cursor.execute("SELECT combustible FROM config.combustible WHERE id = %s", (d["id_combustible"],))
        r = self.cursor.fetchone()
        d["combustible"] = r["combustible"] if r else None

        d["vigente"] = d.get("hasta") is None
        return d

    @staticmethod
    def _limpiar_error(msg: str) -> str:
        return msg.split("CONTEXT:")[0].strip()
