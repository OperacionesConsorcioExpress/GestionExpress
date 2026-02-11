import os, re, uuid, json, mimetypes
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "eds-adjuntos-gestionexpress"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

class Eds_config:
    """
    CRUD configuración EDS (schema eds):
    - eds.eds_ubicaciones
    - eds.eds_surtidores
    - eds.eds_estaciones
    - eds.eds_estacion_surtidor
    """

    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_PATH, cursor_factory=RealDictCursor,
                                    options='-c timezone=America/Bogota')
        with self.conn.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.conn.commit()

    # Context manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            self.conn.close()

    def close(self):
        try:
            if getattr(self, "cur", None):
                self.cur.close()
        finally:
            if getattr(self, "conn", None):
                self.conn.close()

    def _execute(self, sql: str, params: list = None) -> None:
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
        self.conn.commit()

    def _execute_many(self, sql: str, params_list: list) -> None:
        with self.conn.cursor() as c:
            c.executemany(sql, params_list)
        self.conn.commit()

    # --------------------
    # Utilidades
    # --------------------
    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _fetchall(self, sql: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchall()

    def _fetchone(self, sql: str, params: Optional[list] = None) -> Optional[Dict[str, Any]]:
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchone()

    # ============================================================
    # FILTROS (DEPENDIENTES): componentes -> zonas -> cop
    # ============================================================
    def filtros_componentes(self) -> List[Dict[str, Any]]:
        sql = """
            SELECT id, componente
            FROM config.componente
            WHERE estado = 1
            ORDER BY componente ASC;
        """
        # OJO: tu _fetchall exige params; pásale []
        return self._fetchall(sql, [])

    def filtros_zonas(self, id_componente: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Zonas disponibles según el componente, PERO la relación viene de config.cop.
        - Si id_componente viene null => devuelve todas las zonas activas
        (opcionalmente podrías devolver solo las zonas que existan en cop activo; aquí te dejo ambas opciones).
        """

        # OPCIÓN A (más común): todas las zonas activas si no hay componente
        sql = """
            SELECT z.id, z.zona
            FROM config.zona z
            WHERE z.estado = 1
            AND (
                    %s::bigint IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM config.cop c
                        WHERE c.estado = 1
                        AND c.id_zona = z.id
                        AND c.id_componente = %s
                    )
            )
            ORDER BY z.zona ASC;
        """
        return self._fetchall(sql, [id_componente, id_componente])
    
    def filtros_cop(self, id_zona: Optional[int] = None, id_componente: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        COPs filtrados por zona y/o componente (la tabla ya tiene ambos ids).
        """
        sql = """
            SELECT id, cop, id_componente, id_zona
            FROM config.cop
            WHERE estado = 1
            AND (%s::bigint IS NULL OR id_zona = %s)
            AND (%s::bigint IS NULL OR id_componente = %s)
            ORDER BY cop ASC;
        """
        return self._fetchall(sql, [id_zona, id_zona, id_componente, id_componente])

    # ============================================================
    # UBICACIONES (multi COP via config.eds_ubicaciones_cop)
    # ============================================================
    def ubicaciones_listar(
        self,
        pagina: int,
        tamano: int,
        q: Optional[str],
        estado: Optional[int],
        tipo: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], int]:
        qv = q.strip() if q else None

        sql_count = """
            SELECT COUNT(*)::int AS total
            FROM eds.eds_ubicaciones u
            WHERE
                (%s::text IS NULL OR (u.nombre ILIKE '%%' || %s || '%%' OR COALESCE(u.ciudad,'') ILIKE '%%' || %s || '%%'))
                AND (%s::text IS NULL OR u.tipo = %s)
                AND (%s::int  IS NULL OR u.estado = %s);
        """
        total = self._fetchone(sql_count, [qv, qv, qv, tipo, tipo, estado, estado])["total"]

        sql = """
            SELECT
                u.id,
                u.nombre,
                u.tipo,
                u.direccion,
                u.ciudad,
                u.observacion,
                u.estado,
                to_char(u.created_at, 'YYYY-MM-DD HH24:MI') AS created_at,
                to_char(u.updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at,

                -- para mostrar en tabla
                COALESCE(string_agg(DISTINCT c.cop, ' | ' ORDER BY c.cop), '') AS cops_text,

                -- para edición en modal (multi-select)
                COALESCE(array_agg(DISTINCT c.id ORDER BY c.id) FILTER (WHERE c.id IS NOT NULL), '{}'::bigint[]) AS cops_ids,

                -- si te sirve para precargar selects (se deduce de COP seleccionado)
                MIN(c.id_componente) AS id_componente,
                MIN(c.id_zona)       AS id_zona

            FROM eds.eds_ubicaciones u
            LEFT JOIN eds.eds_ubicaciones_cop uc ON uc.id_ubicacion = u.id
            LEFT JOIN config.cop c ON c.id = uc.id_cop
            WHERE
                (%s::text IS NULL OR (u.nombre ILIKE '%%' || %s || '%%' OR COALESCE(u.ciudad,'') ILIKE '%%' || %s || '%%'))
                AND (%s::text IS NULL OR u.tipo = %s)
                AND (%s::int  IS NULL OR u.estado = %s)
            GROUP BY u.id
            ORDER BY u.id DESC
        """
        pag_sql, pag_params = self._paginacion(pagina, tamano)
        data = self._fetchall(sql + pag_sql, [qv, qv, qv, tipo, tipo, estado, estado] + pag_params)
        return data, total

    def ubicaciones_activas(self, estado: int = 1) -> List[Dict[str, Any]]:
        sql = """
            SELECT id, nombre, ciudad
            FROM eds.eds_ubicaciones
            WHERE estado = %s
            ORDER BY nombre ASC;
        """
        return self._fetchall(sql, [estado])

    def ubicacion_crear(
        self,
        nombre: str,
        tipo: str,
        ciudad: Optional[str],
        direccion: Optional[str],
        observacion: Optional[str],
        estado: int,
        cops_ids: List[int],
    ) -> Dict[str, Any]:
        if not cops_ids:
            raise ValueError("Debes seleccionar al menos un COP.")

        sql_ins = """
            INSERT INTO eds.eds_ubicaciones (nombre, tipo, direccion, ciudad, observacion, estado)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *;
        """

        sql_rel = """
            INSERT INTO eds.eds_ubicaciones_cop (id_ubicacion, id_cop)
            VALUES (%s, %s)
            ON CONFLICT (id_ubicacion, id_cop) DO NOTHING;
        """

        with self.conn.cursor() as c:
            # 1) Insert ubicación
            c.execute(sql_ins, [nombre, tipo, direccion, ciudad, observacion, estado])
            row = c.fetchone()

            # 2) Insert relaciones COP (multi)
            c.executemany(
                sql_rel,
                [(row["id"], int(id_cop)) for id_cop in cops_ids]
            )

        self.conn.commit()
        return row

    def ubicacion_actualizar(
        self,
        id_ubicacion: int,
        nombre: str,
        tipo: str,
        ciudad: Optional[str],
        direccion: Optional[str],
        observacion: Optional[str],
        estado: int,
        cops_ids: List[int],
    ) -> Dict[str, Any]:
        if not cops_ids:
            raise ValueError("Debes seleccionar al menos un COP.")

        sql_upd = """
            UPDATE eds.eds_ubicaciones
            SET nombre=%s, tipo=%s, direccion=%s, ciudad=%s, observacion=%s, estado=%s,
                updated_at = NOW()
            WHERE id=%s
            RETURNING *;
        """

        sql_del = """
            DELETE FROM eds.eds_ubicaciones_cop
            WHERE id_ubicacion=%s;
        """

        sql_rel = """
            INSERT INTO eds.eds_ubicaciones_cop (id_ubicacion, id_cop)
            VALUES (%s, %s)
            ON CONFLICT (id_ubicacion, id_cop) DO NOTHING;
        """

        with self.conn.cursor() as c:
            # 1) Update ubicación
            c.execute(sql_upd, [nombre, tipo, direccion, ciudad, observacion, estado, id_ubicacion])
            row = c.fetchone()

            if not row:
                raise ValueError(f"Ubicación {id_ubicacion} no existe.")

            # 2) Reset relaciones
            c.execute(sql_del, [id_ubicacion])

            # 3) Insert relaciones nuevas
            c.executemany(
                sql_rel,
                [(id_ubicacion, int(id_cop)) for id_cop in cops_ids]
            )

        self.conn.commit()
        return row

    def ubicacion_cambiar_estado(self, id_ubicacion: int, estado: int) -> Dict[str, Any]:
        sql = """
            UPDATE eds.eds_ubicaciones
            SET estado=%s
            WHERE id=%s
            RETURNING *;
        """
        return self._fetchone(sql, [estado, id_ubicacion])
    
    # ============================================================
    # SURTIDORES
    # ============================================================
    def surtidores_listar(self, pagina: int, tamano: int, q: Optional[str], estado: Optional[int], tipo_combustible: Optional[str]) -> Tuple[List[Dict], int]:
        qv = q.strip() if q else None

        sql_count = """
            SELECT COUNT(*)::int AS total
            FROM eds.eds_surtidores
            WHERE
                (%s::text IS NULL OR nombre ILIKE '%%' || %s || '%%')
                AND (%s::text IS NULL OR tipo_combustible = %s)
                AND (%s::int  IS NULL OR estado = %s);
        """
        total = self._fetchone(sql_count, [qv, qv, tipo_combustible, tipo_combustible, estado, estado])["total"]

        sql = """
            SELECT id, nombre, tipo_combustible, estado,
                    to_char(created_at, 'YYYY-MM-DD HH24:MI') AS created_at,
                    to_char(updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at
            FROM eds.eds_surtidores
            WHERE
                (%s::text IS NULL OR nombre ILIKE '%%' || %s || '%%')
                AND (%s::text IS NULL OR tipo_combustible = %s)
                AND (%s::int  IS NULL OR estado = %s)
            ORDER BY id DESC
        """
        pag_sql, pag_params = self._paginacion(pagina, tamano)
        data = self._fetchall(sql + pag_sql, [qv, qv, tipo_combustible, tipo_combustible, estado, estado] + pag_params)
        return data, total

    def surtidores_activos(self, estado: int = 1) -> List[Dict[str, Any]]:
        sql = """
            SELECT id, nombre, tipo_combustible
            FROM eds.eds_surtidores
            WHERE estado = %s
            ORDER BY nombre ASC;
        """
        return self._fetchall(sql, [estado])

    def surtidor_crear(self, nombre: str, tipo_combustible: str, estado: int) -> Dict:
        sql = """
            INSERT INTO eds.eds_surtidores (nombre, tipo_combustible, estado)
            VALUES (%s, %s, %s)
            RETURNING *;
        """
        return self._fetchone(sql, [nombre, tipo_combustible, estado])

    def surtidor_actualizar(self, id_surtidor: int, nombre: str, tipo_combustible: str, estado: int) -> Dict:
        sql = """
            UPDATE eds.eds_surtidores
            SET nombre=%s, tipo_combustible=%s, estado=%s
            WHERE id=%s
            RETURNING *;
        """
        return self._fetchone(sql, [nombre, tipo_combustible, estado, id_surtidor])

    def surtidor_cambiar_estado(self, id_surtidor: int, estado: int) -> Dict:
        sql = """
            UPDATE eds.eds_surtidores
            SET estado=%s
            WHERE id=%s
            RETURNING *;
        """
        return self._fetchone(sql, [estado, id_surtidor])

    # ============================================================
    # EDS (ESTACIONES)
    # ============================================================
    def eds_listar(self, pagina: int, tamano: int, q: Optional[str], estado: Optional[int], id_ubicacion: Optional[int]) -> Tuple[List[Dict], int]:
        qv = q.strip() if q else None

        sql_count = """
            SELECT COUNT(*)::int AS total
            FROM eds.eds_estaciones e
            WHERE
                (%s::text IS NULL OR (e.codigo ILIKE '%%'||%s||'%%' OR e.nombre ILIKE '%%'||%s||'%%'))
                AND (%s::int  IS NULL OR e.estado = %s)
                AND (%s::bigint IS NULL OR e.id_ubicacion = %s);
        """
        total = self._fetchone(sql_count, [qv, qv, qv, estado, estado, id_ubicacion, id_ubicacion])["total"]

        sql = """
            SELECT
                e.id,
                e.codigo,
                e.nombre,
                e.id_ubicacion,
                u.nombre AS ubicacion,
                COALESCE(COUNT(es.id_surtidor),0)::int AS cantidad_surtidores,
                COALESCE(ARRAY_AGG(es.id_surtidor ORDER BY es.id_surtidor) FILTER (WHERE es.id_surtidor IS NOT NULL), '{}') AS surtidores_ids,
                e.estado,
                to_char(e.created_at, 'YYYY-MM-DD HH24:MI') AS created_at,
                to_char(e.updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at
            FROM eds.eds_estaciones e
            JOIN eds.eds_ubicaciones u ON u.id = e.id_ubicacion
            LEFT JOIN eds.eds_estacion_surtidor es ON es.id_estacion = e.id
            WHERE
                (%s::text IS NULL OR (e.codigo ILIKE '%%'||%s||'%%' OR e.nombre ILIKE '%%'||%s||'%%'))
                AND (%s::int  IS NULL OR e.estado = %s)
                AND (%s::bigint IS NULL OR e.id_ubicacion = %s)
            GROUP BY e.id, u.nombre
            ORDER BY e.id DESC
        """
        pag_sql, pag_params = self._paginacion(pagina, tamano)
        data = self._fetchall(sql + pag_sql, [qv, qv, qv, estado, estado, id_ubicacion, id_ubicacion] + pag_params)
        return data, total

    def eds_crear(self, codigo: str, nombre: str, id_ubicacion: int, surtidores_ids: List[int], estado: int) -> Dict:
        # Crear EDS
        sql = """
            INSERT INTO eds.eds_estaciones (codigo, nombre, id_ubicacion, estado)
            VALUES (%s, %s, %s, %s)
            RETURNING *;
        """
        eds = self._fetchone(sql, [codigo, nombre, id_ubicacion, estado])

        # Relación surtidores
        self._eds_set_surtidores(eds["id"], surtidores_ids or [])
        # devolver con surtidores_ids
        eds["surtidores_ids"] = surtidores_ids or []
        return eds

    def eds_actualizar(self, id_eds: int, codigo: str, nombre: str, id_ubicacion: int, surtidores_ids: List[int], estado: int) -> Dict:
        sql = """
            UPDATE eds.eds_estaciones
            SET codigo=%s, nombre=%s, id_ubicacion=%s, estado=%s
            WHERE id=%s
            RETURNING *;
        """
        eds = self._fetchone(sql, [codigo, nombre, id_ubicacion, estado, id_eds])
        self._eds_set_surtidores(id_eds, surtidores_ids or [])
        eds["surtidores_ids"] = surtidores_ids or []
        return eds

    def eds_cambiar_estado(self, id_eds: int, estado: int) -> Dict:
        sql = """
            UPDATE eds.eds_estaciones
            SET estado=%s
            WHERE id=%s
            RETURNING *;
        """
        return self._fetchone(sql, [estado, id_eds])

    def _eds_set_surtidores(self, id_eds: int, surtidores_ids: List[int]) -> None:
        # Reemplazar relaciones: borra y reinserta
        with self.conn.cursor() as c:
            c.execute("DELETE FROM eds.eds_estacion_surtidor WHERE id_estacion=%s;", [id_eds])

            if surtidores_ids:
                c.execute("""
                    INSERT INTO eds.eds_estacion_surtidor (id_estacion, id_surtidor)
                    SELECT %s, x::bigint
                    FROM UNNEST(%s::bigint[]) AS x
                    ON CONFLICT DO NOTHING;
                """, [id_eds, surtidores_ids])

class RegistroEDS:
    def __init__(self):
        self.conn = psycopg2.connect(
            DATABASE_PATH,
            cursor_factory=RealDictCursor,
            options='-c timezone=America/Bogota'
        )
        with self.conn.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            self.conn.close()

    # ---------------------------
    # Helpers DB
    # ---------------------------
    def _fetchone(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchone()

    def _fetchall(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchall()

    def _execute(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])

    # ---------------------------
    # Azure Blob (evidencias)
    # ---------------------------
    def _sanitize_filename(self, name: str) -> str:
        name = (name or "archivo").strip()
        name = name.replace("\\", "_").replace("/", "_")
        name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
        return name[:120] if len(name) > 120 else name

    def _parse_fecha_operativa(self, fecha_operativa: str) -> datetime:
        # Espera "YYYY-MM-DD"
        try:
            return datetime.strptime(str(fecha_operativa), "%Y-%m-%d")
        except Exception:
            # fallback a hoy Bogotá
            return datetime.now()

    def _blob_upload_bytes(self, content: bytes, blob_path: str, content_type: str) -> str:
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")

        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_NAME)

        # Por si el container no existe en algún entorno (no rompe si ya existe)
        try:
            container.create_container()
        except ResourceExistsError:
            pass

        blob = container.get_blob_client(blob_path)
        blob.upload_blob(
            content,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type or "application/octet-stream")
        )
        # Devuelve el path para guardarlo en BD
        return blob_path

    # ---------------------------
    # Catálogos / selección EDS
    # ---------------------------
    def eds_activas(self) -> List[Dict[str, Any]]:
        sql = """
            SELECT e.id, e.codigo, e.nombre, e.id_ubicacion
            FROM eds.eds_estaciones e
            WHERE e.estado = 1
            ORDER BY e.nombre ASC;
        """
        return self._fetchall(sql)

    def eds_detalle(self, id_eds: int) -> Dict[str, Any]:
        """
        Retorna:
        - datos EDS
        - ubicación
        - COPs asociados a la ubicación (eds.eds_ubicaciones_cop)
        - surtidores asociados a la EDS (eds.eds_estacion_surtidor + eds.eds_surtidores)
        """
        eds = self._fetchone("""
            SELECT e.id, e.codigo, e.nombre, e.id_ubicacion
            FROM eds.eds_estaciones e
            WHERE e.id = %s;
        """, [id_eds])
        if not eds:
            raise ValueError("EDS no encontrada")

        ubic = self._fetchone("""
            SELECT u.id, u.nombre, u.tipo, u.ciudad, u.direccion, u.observacion
            FROM eds.eds_ubicaciones u
            WHERE u.id = %s;
        """, [eds["id_ubicacion"]])

        cops = self._fetchall("""
            SELECT c.id, c.cop
            FROM eds.eds_ubicaciones_cop uc
            JOIN config.cop c ON c.id = uc.id_cop
            WHERE uc.id_ubicacion = %s
            ORDER BY c.cop ASC;
        """, [eds["id_ubicacion"]])

        surtidores = self._fetchall("""
            SELECT s.id, s.nombre, s.tipo_combustible
            FROM eds.eds_estacion_surtidor es
            JOIN eds.eds_surtidores s ON s.id = es.id_surtidor
            WHERE es.id_estacion = %s
                AND s.estado = 1
            ORDER BY s.nombre ASC;
        """, [id_eds])

        return {
            "eds": eds,
            "ubicacion": ubic,
            "cops": cops,
            "surtidores": surtidores
        }

    def _cops_ids_de_eds(self, id_eds: int) -> List[int]:
        row = self._fetchone("SELECT id_ubicacion FROM eds.eds_estaciones WHERE id=%s;", [id_eds])
        if not row:
            return []
        id_ubicacion = row["id_ubicacion"]
        cops = self._fetchall("""
            SELECT id_cop
            FROM eds.eds_ubicaciones_cop
            WHERE id_ubicacion = %s;
        """, [id_ubicacion])
        return [int(x["id_cop"]) for x in cops]

    # ---------------------------
    # Buses + últimos registros
    # ---------------------------
    def bus_sugerencias(self, q: str, id_eds: Optional[int] = None, limit: int = 8) -> List[Dict[str, Any]]:
        """
        Retorna sugerencias por placa o no_interno (LIKE), opcionalmente filtradas a los COP de la EDS.
        """
        q = (q or "").strip().upper()
        if not q:
            return []

        where_extra = ""
        params = []

        if id_eds:
            cops_ids = self._cops_ids_de_eds(int(id_eds))
            if cops_ids:
                where_extra = " AND b.id_cop = ANY(%s::bigint[]) "
                params.append(cops_ids)
            else:
                # Si la EDS no tiene COPs asociados, devolvemos vacío (para no confundir)
                return []

        sql = f"""
            SELECT
                b.id,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.combustible,
                b.id_cop
            FROM config.buses_cexp b
            WHERE (
                UPPER(b.placa) LIKE %s
            OR UPPER(b.no_interno::text) LIKE %s
            )
            {where_extra}
            ORDER BY
            CASE WHEN UPPER(b.placa) = %s THEN 0 ELSE 1 END,
            b.placa ASC
            LIMIT %s;
        """
        params = [f"%{q}%", f"%{q}%", q] + params + [int(limit)]
        return self._fetchall(sql, params)
    
    def _ultimo_registro_bus(self, id_bus: int) -> Optional[Dict[str, Any]]:
        return self._fetchone("""
            SELECT
                r.id,
                r.odometro,
                r.volumen,
                r.fecha_hora_registro,
                r.registrado_por
            FROM eds.eds_registro r
            WHERE r.id_bus = %s
            ORDER BY r.fecha_hora_registro DESC
            LIMIT 1;
        """, [id_bus])

    def bus_lookup(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Busca por placa o no_interno (exacto o parcial), retorna 1 match más cercano.
        Retorna además el nombre del COP como: cop_nombre (config.cop.cop)
        """
        q = (query or "").strip().upper()
        if not q:
            return None

        bus = self._fetchone("""
            SELECT
                b.id,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.combustible,
                b.id_cop,
                c.cop AS cop_nombre
            FROM config.buses_cexp b
            LEFT JOIN config.cop c ON c.id = b.id_cop
            WHERE UPPER(b.placa) = %s
                OR UPPER(b.no_interno::text) = %s
                OR UPPER(b.placa) LIKE %s
                OR UPPER(b.no_interno::text) LIKE %s
            ORDER BY
                CASE WHEN UPPER(b.placa) = %s THEN 0 ELSE 1 END,
                CASE WHEN UPPER(b.no_interno::text) = %s THEN 0 ELSE 1 END,
                b.placa ASC
            LIMIT 1;
        """, [q, q, f"%{q}%", f"%{q}%", q, q])

        if not bus:
            return None

        ult = self._ultimo_registro_bus(bus["id"])
        bus["ultimo"] = ult
        return bus

    def pendientes_hoy(self, id_eds: int, fecha: str) -> List[Dict[str, Any]]:
        """
        Pendientes = buses de COPs asociados a la ubicación de la EDS que aún NO tienen registro en el día.
        """
        # Validación defensiva
        row = self._fetchone("SELECT id_ubicacion FROM eds.eds_estaciones WHERE id=%s;", [id_eds])
        if not row:
            return []

        sql = """
            WITH cops AS (
                SELECT uc.id_cop
                FROM eds.eds_estaciones e
                JOIN eds.eds_ubicaciones_cop uc ON uc.id_ubicacion = e.id_ubicacion
                WHERE e.id = %s
            )
            SELECT
                b.id AS id_bus,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.combustible,
                ult.odometro AS ult_odometro,
                ult.volumen AS ult_tanqueo,
                ult.registrado_por AS registrado_por
            FROM config.buses_cexp b
            JOIN cops ON cops.id_cop = b.id_cop
            LEFT JOIN LATERAL (
                SELECT r.odometro, r.volumen, r.registrado_por
                FROM eds.eds_registro r
                WHERE r.id_bus = b.id
                ORDER BY r.fecha_hora_registro DESC
                LIMIT 1
            ) ult ON TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM eds.eds_registro r2
                WHERE r2.id_eds = %s
                AND r2.id_bus = b.id
                AND (r2.fecha_hora_registro AT TIME ZONE 'America/Bogota')::date = %s::date
            )
            ORDER BY b.placa ASC;
        """
        return self._fetchall(sql, [id_eds, id_eds, fecha])

    def ya_registrados(self, id_eds: int, fecha: str) -> List[Dict[str, Any]]:
        """
        Registros del día (por EDS) con info del bus.
        """
        return self._fetchall("""
            SELECT
                r.id,
                r.id_bus,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.combustible,
                r.odometro,
                r.volumen,
                r.odometro_funcional,
                r.observacion,
                r.fecha_hora_registro,
                r.registrado_por
            FROM eds.eds_registro r
            JOIN config.buses_cexp b ON b.id = r.id_bus
            WHERE r.id_eds = %s
                AND (r.fecha_hora_registro AT TIME ZONE 'America/Bogota')::date = %s::date
            ORDER BY r.fecha_hora_registro DESC;
        """, [id_eds, fecha])

    def alertas_dia(self, id_eds: int, fecha: str) -> List[Dict[str, Any]]:
        return self._fetchall("""
            SELECT
                a.*,
                b.placa
            FROM eds.alertas_eds a
            LEFT JOIN config.buses_cexp b ON b.id = a.id_bus
            WHERE a.id_eds = %s
            AND a.fecha = %s::date
            ORDER BY a.created_at DESC;
        """, [id_eds, fecha])

    # ---------------------------
    # Reglas de negocio (extensibles)
    # ---------------------------
    def _es_bus_asignado_a_eds(self, id_eds: int, id_bus: int) -> bool:
        cops_ids = self._cops_ids_de_eds(id_eds)
        if not cops_ids:
            return False
        row = self._fetchone("SELECT id_cop FROM config.buses_cexp WHERE id=%s;", [id_bus])
        if not row:
            return False
        return int(row["id_cop"]) in set(int(x) for x in cops_ids)

    def _promedio_delta_tipologia(self, tipologia: str, dias: int = 7) -> Optional[float]:
        """
        Calcula promedio del delta (odometro actual - odometro anterior) para una tipología
        en los últimos N días (histórico de eds_registro).
        """
        if not tipologia:
            return None

        # obtenemos registros por bus, ordenados, calculamos delta con window
        rows = self._fetchall(f"""
            WITH x AS (
                SELECT
                    r.id_bus,
                    r.odometro,
                    r.fecha_hora_registro,
                    LAG(r.odometro) OVER (PARTITION BY r.id_bus ORDER BY r.fecha_hora_registro) AS odo_prev
                FROM eds.eds_registro r
                JOIN config.buses_cexp b ON b.id = r.id_bus
                WHERE b.tipologia = %s
                  AND (r.fecha_hora_registro AT TIME ZONE 'America/Bogota')::date >=
                      ((now() AT TIME ZONE 'America/Bogota')::date - %s::int)
            )
            SELECT AVG((odometro - odo_prev)::numeric) AS avg_delta
            FROM x
            WHERE odo_prev IS NOT NULL
                AND (odometro - odo_prev) > 0;
        """, [tipologia, dias])

        if not rows or rows[0].get("avg_delta") is None:
            return None
        return float(rows[0]["avg_delta"])

    def _crear_alerta(self, fecha: str, id_bus: Optional[int], id_eds: Optional[int], id_surtidor: Optional[int],
                    tipo: str, severidad: str, mensaje: str, payload: Optional[dict], registrado_por: str) -> None:
        self._execute("""
            INSERT INTO eds.alertas_eds (fecha, id_bus, id_eds, id_surtidor, tipo, severidad, mensaje, payload, registrado_por)
            VALUES (%s::date, %s, %s, %s, %s, %s, %s, %s::jsonb, %s);
        """, [fecha, id_bus, id_eds, id_surtidor, tipo, severidad, mensaje, json.dumps(payload or {}), registrado_por])

    def registrar(self,
                id_bus: int,
                id_eds: int,
                id_surtidor: int,
                odometro: int,
                volumen: Optional[float],
                odometro_funcional: bool,
                observacion: Optional[str],
                registrado_por: str,
                fecha_operativa: str,
                evidencias: Optional[List[Tuple[bytes, str, str]]] = None
                ) -> Dict[str, Any]:

        # Validaciones mínimas
        if not id_bus or not id_eds or not id_surtidor:
            raise ValueError("Faltan datos: bus, EDS o surtidor")
        if odometro is None:
            raise ValueError("El odómetro es obligatorio")

        bus = self._fetchone("""
            SELECT id, placa, no_interno, tipologia, combustible, id_cop
            FROM config.buses_cexp
            WHERE id=%s;
        """, [id_bus])
        if not bus:
            raise ValueError("Bus no encontrado")

        # Regla: no permitir odo <= último odo
        ult = self._ultimo_registro_bus(id_bus)
        if ult and ult.get("odometro") is not None:
            if int(odometro) <= int(ult["odometro"]):
                raise ValueError(f"Odómetro inválido: {odometro} debe ser mayor al último ({ult['odometro']})")

        # Evidencias (si llegan) -> PLACA/AÑO/MES/uuid.ext
        evidencia_urls = []
        if evidencias:
            fecha_dt = self._parse_fecha_operativa(fecha_operativa)
            year = f"{fecha_dt.year:04d}"
            month = f"{fecha_dt.month:02d}"

            placa = (bus.get("placa") or "SIN_PLACA").strip().upper().replace(" ", "")
            placa = re.sub(r"[^A-Z0-9_-]+", "", placa) or "SIN_PLACA"

            for content_bytes, filename, content_type in evidencias:
                safe_name = self._sanitize_filename(filename)
                uid = uuid.uuid4().hex

                # Estructura solicitada: PLACA/YYYY/MM/...
                blob_name = f"{placa}/{year}/{month}/{uid}_{safe_name}"

                # Sube y guarda el path
                path = self._blob_upload_bytes(
                    content=content_bytes,
                    blob_path=blob_name,
                    content_type=content_type or "application/octet-stream"
                )
                evidencia_urls.append(path)

        # Inserta registro
        row = self._fetchone("""
            INSERT INTO eds.eds_registro
            (id_bus, id_eds, id_surtidor, odometro, volumen, odometro_funcional, observacion, evidencia_urls, fecha_hora_registro, registrado_por)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, now(), %s)
            RETURNING *;
        """, [
            id_bus, id_eds, id_surtidor,
            int(odometro),
            volumen,
            bool(odometro_funcional),
            observacion,
            json.dumps(evidencia_urls),
            registrado_por
        ])

        # Alertas:
        # 1) Bus no asignado a la EDS
        asignado = self._es_bus_asignado_a_eds(id_eds, id_bus)
        if not asignado:
            self._crear_alerta(
                fecha=fecha_operativa,
                id_bus=id_bus,
                id_eds=id_eds,
                id_surtidor=id_surtidor,
                tipo="BUS_NO_ASIGNADO",
                severidad="WARN",
                mensaje=f"Bus {bus.get('placa')} no pertenece a los COP asignados a esta EDS (se permitió el registro).",
                payload={"id_cop_bus": bus.get("id_cop")},
                registrado_por=registrado_por
            )

        # 2) Odómetro atípico vs promedio tipología (si hay histórico)
        if ult and ult.get("odometro") is not None and bus.get("tipologia"):
            delta = int(odometro) - int(ult["odometro"])
            prom = self._promedio_delta_tipologia(bus.get("tipologia"), dias=7)
            if prom and prom > 0:
                # Umbrales simples (fáciles de ajustar después)
                if delta > prom * 1.7 or delta < prom * 0.5:
                    self._crear_alerta(
                        fecha=fecha_operativa,
                        id_bus=id_bus,
                        id_eds=id_eds,
                        id_surtidor=id_surtidor,
                        tipo="ODOMETRO_ATIPICO",
                        severidad="WARN",
                        mensaje=f"Delta odómetro atípico para tipología {bus.get('tipologia')}: Δ={delta} vs prom≈{round(prom,2)}",
                        payload={"delta": delta, "promedio": prom, "tipologia": bus.get("tipologia")},
                        registrado_por=registrado_por
                    )

        # Respuesta enriquecida
        row["bus"] = bus
        row["asignado_a_eds"] = asignado
        row["ultimo_anterior"] = ult
        return row

class GestionEDS:
    """Gestión de parámetros preoperacionales de motos (mantenimientos y km óptimo)."""

    def __init__(self):
        try:
            self.connection = psycopg2.connect(DATABASE_PATH,
                options='-c timezone=America/Bogota')
            self.cursor = self.connection.cursor()
            
            # Fallback defensivo (por si el options no se respeta en algún entorno)
            with self.connection.cursor() as c:
                c.execute("SET TIME ZONE 'America/Bogota';")
            self.connection.commit()
            
        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None):
            self.connection.close()