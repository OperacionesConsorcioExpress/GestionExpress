import os
import base64
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import get_db_connection

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_SNE_INFORMES = "sne-informes-objecion"
SYNC_OBJECION_VENCIDA_LOCK_KEY = 540021

TZ_BOGOTA = ZoneInfo("America/Bogota")
logger = logging.getLogger(__name__)

def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)

class GestionSneObjecion:
    """
    Modelo de gestion y objecion de kilometros SNE.

    Tablas involucradas:
        sne.ics              - registros ICS cargados
        sne.gestion_sne      - gestion/estado por revisor (PK = id_ics)
        config.rutas         - id_linea -> ruta_comercial
        config.cop           - id_cop   -> cop / componente / zona
        config.componente    - id       -> componente
        config.zona          - id       -> zona
        public.usuarios      - id       -> revisor / usuario_asigna
    """

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        self._col_cache: dict = {}
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

    # ── Blob Storage SNE Informes ─────────────────────────────────────────────────
    def _blob_upload_sne_informe(self, content: bytes, blob_path: str) -> str:
        """Sube bytes al contenedor sne-informes-objecion y retorna el path."""
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_SNE_INFORMES)
        try:
            container.create_container()
        except ResourceExistsError:
            pass
        blob = container.get_blob_client(blob_path)
        blob.upload_blob(
            content,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
        )
        return blob_path

    def subir_pdf_objecion(self, pdf_bytes: bytes, id_ics: int, fecha_referencia) -> str:
        """
        Sube el PDF de informe de objeción al blob storage.
        La carpeta y el nombre del archivo se construyen con la FECHA DEL REGISTRO ICS
        (no con la fecha de guardado), para organizar los PDFs por fecha de servicio.
        Estructura: sne-informes-objecion/YYYY/MM/DDMMYYYY_IDICS.pdf
        Ejemplo:    2026/03/25032026_105360984.pdf
        fecha_referencia: datetime.date o datetime.datetime con la fecha del ICS.
        """
        if fecha_referencia is None:
            from datetime import datetime as _dt
            fecha_referencia = _dt.now(TZ_BOGOTA)
        anio = str(fecha_referencia.year)
        mes  = f"{fecha_referencia.month:02d}"
        dia  = f"{fecha_referencia.day:02d}{mes}{anio}"
        nombre    = f"{dia}_{id_ics}.pdf"
        blob_path = f"{anio}/{mes}/{nombre}"
        return self._blob_upload_sne_informe(pdf_bytes, blob_path)

    # ── Helpers ──────────────────────────────────────────────────────────────────
    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        key = (schema, table, column)
        if key not in self._col_cache:
            self.cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s AND column_name=%s
                LIMIT 1
                """,
                (schema, table, column),
            )
            self._col_cache[key] = self.cursor.fetchone() is not None
        return self._col_cache[key]

    def _cop_joins_and_exprs(self):
        """Mismo patron que gestion_rutas para cop -> componente/zona."""
        has_id_comp = self._col_exists("config", "cop", "id_componente")
        has_id_zona = self._col_exists("config", "cop", "id_zona")
        joins, comp_expr, zona_expr = [], "NULL", "NULL"
        if has_id_comp:
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        if has_id_zona:
            joins.append("LEFT JOIN config.zona z ON z.id = c.id_zona")
            zona_expr = "z.zona"
        return " ".join(joins), comp_expr, zona_expr

    # ── Filtros de fecha e ICS ───────────────────────────────────────────────────
    def ultima_fecha_ics(self) -> str | None:
        """Retorna la ultima fecha con registros en sne.ics."""
        self.cursor.execute(
            "SELECT MAX(fecha)::text AS ultima_fecha FROM sne.ics"
        )
        row = self.cursor.fetchone()
        return row["ultima_fecha"] if row else None

    def listar_id_ics_por_fecha(self, fecha: str, usuario_id: int = None):
        """
        Lista los id_ics unicos de sne.ics para una fecha dada.
        Si se provee usuario_id, solo los que tiene asignados ese revisor.
        """
        if usuario_id:
            self.cursor.execute(
                """
                SELECT DISTINCT i.id_ics
                FROM sne.ics i
                JOIN sne.gestion_sne gs ON gs.id_ics = i.id_ics
                WHERE i.fecha = %s AND gs.revisor = %s
                ORDER BY i.id_ics
                """,
                (fecha, usuario_id),
            )
        else:
            self.cursor.execute(
                "SELECT DISTINCT id_ics FROM sne.ics WHERE fecha = %s ORDER BY id_ics",
                (fecha,),
            )
        return [r["id_ics"] for r in self.cursor.fetchall()]

    def listar_responsables(
        self,
        usuario_id: int,
        fecha: str = None,
        id_ics: int = None,
        id_linea: int = None,
        id_concesion: int = None,
        id_cop: int = None,
        zona: str = None,
        componente: str = None,
        tab: str = "revisar",
    ):
        """
        Lista responsables realmente asociados a los ICS del usuario
        por medio de sne.ics_motivo_resp.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        where = " WHERE gs.revisor = %s "
        params: list = [usuario_id]

        if tab == "revisar":
            # Asignados (estado_asignacion=1) que aún NO fueron objetados
            where += " AND gs.estado_asignacion = 1 AND (gs.estado_objecion IS DISTINCT FROM 1) "
        elif tab == "revisados":
            # Asignados que ya fueron objetados (estado_objecion=1)
            where += " AND gs.estado_asignacion = 1 AND gs.estado_objecion = 1 "
        elif tab == "validar":
            # Tab en construcción: no retorna registros aún
            where += " AND 1 = 0 "

        if fecha:
            where += " AND i.fecha = %s "
            params.append(fecha)
        if id_ics:
            where += " AND i.id_ics = %s "
            params.append(int(id_ics))
        if id_linea:
            where += " AND i.id_linea = %s "
            params.append(int(id_linea))
        if id_concesion:
            where += " AND i.id_concesion = %s "
            params.append(int(id_concesion))
        if id_cop:
            where += " AND r.id_cop = %s "
            params.append(int(id_cop))
        if zona:
            where += f" AND UPPER({zona_expr}) = %s "
            params.append(zona.strip().upper())
        if componente:
            where += f" AND UPPER({comp_expr}) = %s "
            params.append(componente.strip().upper())

        self.cursor.execute(
            f"""
            SELECT DISTINCT rs.id, rs.responsable
            FROM sne.gestion_sne gs
            JOIN sne.ics i              ON i.id_ics = gs.id_ics
            JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            JOIN sne.responsable_sne rs ON rs.id = imr.responsable
            LEFT JOIN config.rutas r    ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c      ON c.id = r.id_cop
            {joins_cop}
            {where}
            ORDER BY rs.responsable
            """,
            params,
        )
        return self.cursor.fetchall()

    def sincronizar_objeciones_vencidas(self) -> dict:
        """
        Actualiza en lote a vencido los ICS cuyo debido proceso ya cerro.

        Regla de negocio:
        - Solo cambia registros con estado_objecion = 0.
        - Nunca toca estado_objecion = 1 porque ya fue objetado.
        """
        ahora = ahora_bogota()
        lock_adquirido = False

        try:
            self.cursor.execute(
                "SELECT pg_try_advisory_lock(%s) AS locked",
                (SYNC_OBJECION_VENCIDA_LOCK_KEY,),
            )
            row_lock = self.cursor.fetchone() or {}
            lock_adquirido = bool(row_lock.get("locked"))

            if not lock_adquirido:
                self.connection.rollback()
                return {
                    "ok": True,
                    "actualizados": 0,
                    "ids_ics": [],
                    "ejecutado_en": ahora.isoformat(),
                    "lock_adquirido": False,
                }

            sets = ["estado_objecion = 2"]
            params: list = []

            if self._col_exists("sne", "gestion_sne", "actualizado_en"):
                sets.append("actualizado_en = %s")
                params.append(ahora)

            if self._col_exists("sne", "gestion_sne", "updated_at"):
                sets.append("updated_at = %s")
                params.append(ahora)

            params.append(ahora.replace(tzinfo=None))
            self.cursor.execute(
                f"""
                UPDATE sne.gestion_sne
                SET {', '.join(sets)}
                WHERE estado_objecion = 0
                    AND fecha_cierre_dp IS NOT NULL
                    AND fecha_cierre_dp < %s
                RETURNING id_ics
                """,
                params,
            )
            rows = self.cursor.fetchall()
            self.connection.commit()

            ids_actualizados = [r["id_ics"] for r in rows]
            return {
                "ok": True,
                "actualizados": len(ids_actualizados),
                "ids_ics": ids_actualizados,
                "ejecutado_en": ahora.isoformat(),
                "lock_adquirido": True,
            }
        except Exception:
            self.connection.rollback()
            logger.exception("Error sincronizando objeciones vencidas SNE")
            raise
        finally:
            if lock_adquirido:
                try:
                    self.cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (SYNC_OBJECION_VENCIDA_LOCK_KEY,),
                    )
                    self.connection.commit()
                except Exception:
                    self.connection.rollback()
                    logger.exception("No fue posible liberar el lock de objeciones vencidas SNE")

    def listar_todos_responsables(self):
        """Lista TODOS los responsables activos desde sne.responsable_sne sin filtrar por usuario."""
        self.cursor.execute(
            "SELECT id, responsable FROM sne.responsable_sne WHERE estado = TRUE ORDER BY responsable"
        )
        return self.cursor.fetchall()

    def listar_acciones(self):
        """Lista todas las acciones disponibles desde sne.acciones."""
        self.cursor.execute(
            "SELECT id_acc, accion FROM sne.acciones ORDER BY id_acc"
        )
        return self.cursor.fetchall()

    def listar_justificaciones(self, id_acc: int = None):
        if id_acc:
            self.cursor.execute(
                "SELECT id_justificacion, id_acc, justificacion FROM sne.justificacion WHERE id_acc = %s AND estado = TRUE ORDER BY id_justificacion",
                (id_acc,)
            )
        else:
            self.cursor.execute(
                "SELECT id_justificacion, id_acc, justificacion FROM sne.justificacion WHERE estado = TRUE ORDER BY id_acc, id_justificacion"
            )
        return self.cursor.fetchall()

    def listar_motivos_notas_activos(self):
        """Lista motivos de notas activos desde sne.motivos_notas."""
        self.cursor.execute(
            "SELECT id, motivo_nota FROM sne.motivos_notas WHERE estado = TRUE ORDER BY motivo_nota"
        )
        return self.cursor.fetchall()

    def listar_motivos_por_responsable(self, id_responsable: int = None):
        """Lista motivos activos desde sne.motivos_eliminacion filtrados por responsable."""
        if id_responsable:
            self.cursor.execute(
                """
                SELECT me.id, me.motivo, me.responsable
                FROM sne.motivos_eliminacion me
                WHERE me.responsable = %s AND me.estado = TRUE
                ORDER BY me.motivo
                """,
                (id_responsable,)
            )
        else:
            self.cursor.execute(
                "SELECT id, motivo, responsable FROM sne.motivos_eliminacion WHERE estado = TRUE ORDER BY responsable, motivo"
            )
        return self.cursor.fetchall()

    def listar_motivos_responsables_por_ics(self, id_ics: int):
        """
        Retorna los motivos y responsables asignados a un ICS
        desde sne.ics_motivo_resp cruzando con sus tablas de descripción.
        Solo lectura — no se agregan ni eliminan desde el frontend.
        """
        self.cursor.execute(
            """
            SELECT
                imr.id_ics,
                imr.motivo       AS id_motivo,
                me.motivo        AS motivo_nombre,
                imr.responsable  AS id_responsable,
                rs.responsable   AS responsable_nombre
            FROM sne.ics_motivo_resp imr
            JOIN sne.motivos_eliminacion me ON me.id  = imr.motivo
            JOIN sne.responsable_sne    rs ON rs.id   = imr.responsable
            WHERE imr.id_ics = %s
            ORDER BY rs.responsable, me.motivo
            """,
            (id_ics,)
        )
        return self.cursor.fetchall()

    # ── Filtros de selects (cascada componente -> zona -> cop -> ruta) ───────────
    def listar_componentes(self):
        """Lista componentes unicos para el select del filtro."""
        joins_cop, comp_expr, _ = self._cop_joins_and_exprs()
        self.cursor.execute(
            f"""
            SELECT DISTINCT {comp_expr} AS componente
            FROM config.cop c
            {joins_cop}
            WHERE c.estado = 1 AND {comp_expr} IS NOT NULL
            ORDER BY {comp_expr}
            """
        )
        return [r["componente"] for r in self.cursor.fetchall() if r["componente"]]

    def listar_zonas(self, componente: str = None):
        """Lista zonas filtradas opcionalmente por componente."""
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        where, params = " WHERE c.estado = 1 ", []
        if componente:
            where += f" AND UPPER({comp_expr}) = %s "
            params.append(componente.strip().upper())
        self.cursor.execute(
            f"""
            SELECT DISTINCT {zona_expr} AS zona
            FROM config.cop c
            {joins_cop}
            {where} AND {zona_expr} IS NOT NULL
            ORDER BY {zona_expr}
            """,
            params,
        )
        return [r["zona"] for r in self.cursor.fetchall() if r["zona"]]

    def listar_cop(self, componente: str = None, zona: str = None):
        """Lista COP filtrados por componente y/o zona."""
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        where, params = " WHERE c.estado = 1 ", []
        if componente:
            where += f" AND UPPER({comp_expr}) = %s "
            params.append(componente.strip().upper())
        if zona:
            where += f" AND UPPER({zona_expr}) = %s "
            params.append(zona.strip().upper())
        self.cursor.execute(
            f"""
            SELECT c.id, c.cop, {comp_expr} AS componente, {zona_expr} AS zona
            FROM config.cop c
            {joins_cop}
            {where}
            ORDER BY {comp_expr}, {zona_expr}, c.cop
            """,
            params,
        )
        return self.cursor.fetchall()

    def listar_rutas_por_cop(self, id_cop: int = None, zona: str = None):
        """
        Lista rutas comerciales filtradas por COP y/o zona.
        Muestra ruta_comercial en el select.
        """
        joins_cop, _, zona_expr = self._cop_joins_and_exprs()
        where, params = " WHERE r.estado = 1 ", []
        if id_cop:
            where += " AND r.id_cop = %s "
            params.append(int(id_cop))
        if zona:
            where += f" AND UPPER({zona_expr}) = %s "
            params.append(zona.strip().upper())
        self.cursor.execute(
            f"""
            SELECT r.id, r.id_linea, r.ruta_comercial, r.id_cop, c.cop
            FROM config.rutas r
            JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where}
            ORDER BY r.ruta_comercial
            """,
            params,
        )
        return self.cursor.fetchall()

    # ── Proceamiento de Mapas, trazados de rutas ───────────────────────────────────────────────
    def listar_posicionamientos(
        self,
        movil_bus: str,
        fecha: str = None,
        fecha_ini: str = None,
        fecha_fin: str = None,
        hora_ini: str = '00:00:00',
        hora_fin: str = '23:59:59',
    ):
        """
        Filtra por rango de timestamps construido desde fecha + hora.
        Usa fecha_evento >= ts_ini AND < ts_fin para aprovechar índice.
        hora_ini / hora_fin en formato HH:MM:SS
        """
        fecha_ini = fecha_ini or fecha
        fecha_fin = fecha_fin or fecha_ini

        self.cursor.execute(
            """
            SELECT
                p.id,
                p.fecha_evento::date::text              AS fecha,
                to_char(p.fecha_evento AT TIME ZONE 'America/Bogota', 'HH24:MI:SS') AS hora,
                p.estado_localizacion,
                p.nombre_estado,
                p.movil_bus,
                p.servbus,
                p.longitud,
                p.latitud,
                p.posicion,
                p.vel_m_s,
                p.dist_m,
                p.id_viaje
            FROM config.posicionamientos p
            WHERE p.movil_bus = %s
                AND p.fecha_evento >= (%s || ' ' || %s)::timestamptz
                AND p.fecha_evento <= (%s || ' ' || %s)::timestamptz
                AND p.latitud  IS NOT NULL
                AND p.longitud IS NOT NULL
            ORDER BY p.fecha_evento ASC
            LIMIT 10000
            """,
            (movil_bus, fecha_ini, hora_ini, fecha_fin, hora_fin),
        )
        return self.cursor.fetchall()

    # ── Listado principal de ICS ─────────────────────────────────────────────────
    def listar_ics_por_usuario(
        self,
        usuario_id: int,
        fecha: str = None,
        id_ics: int = None,
        id_linea: int = None,
        id_concesion: int = None,
        id_cop: int = None,
        zona: str = None,
        componente: str = None,
        id_responsable: int = None,
        texto_busqueda: str = None,
        orden: str = None,
        tab: str = "revisar",
        pagina: int = 1,
        tamano: int = 50,
    ):
        """
        Trae los registros ICS asignados al usuario logueado.

        Logica de tabs:
        - revisar   -> revisor > 0 y estado_asignacion = 1 (pendiente)
        - revisados -> estado_asignacion = 1 AND estado_objecion = 1 (ya objetado)
        - validar   -> en construcción (retorna vacío)

        NOTA: sne.gestion_sne NO tiene columna 'id'; la PK es id_ics.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        # Verificar si la columna fecha_cierre_dp existe (puede faltar en entornos sin carga ETL)
        has_fecha_cierre_dp = self._col_exists("sne", "gestion_sne", "fecha_cierre_dp")
        fecha_cierre_dp_expr = (
            "to_char(gs.fecha_cierre_dp, 'YYYY-MM-DD HH24:MI:SS')"
            if has_fecha_cierre_dp else "NULL::text"
        )

        where = " WHERE gs.revisor = %s "
        params: list = [usuario_id]

        if tab == "revisar":
            # Asignados (estado_asignacion=1) que aún NO fueron objetados
            where += " AND gs.estado_asignacion = 1 AND (gs.estado_objecion IS DISTINCT FROM 1) "
        elif tab == "revisados":
            # Asignados que ya fueron objetados (estado_objecion=1)
            where += " AND gs.estado_asignacion = 1 AND gs.estado_objecion = 1 "
        elif tab == "validar":
            # Tab en construcción: no retorna registros aún
            where += " AND 1 = 0 "

        if fecha:
            where += " AND i.fecha = %s "
            params.append(fecha)
        if id_ics:
            where += " AND i.id_ics = %s "
            params.append(int(id_ics))
        if id_linea:
            where += " AND i.id_linea = %s "
            params.append(int(id_linea))
        if id_concesion:
            where += " AND i.id_concesion = %s "
            params.append(int(id_concesion))
        if id_cop:
            where += " AND r.id_cop = %s "
            params.append(int(id_cop))
        if zona:
            where += f" AND UPPER({zona_expr}) = %s "
            params.append(zona.strip().upper())
        if componente:
            where += f" AND UPPER({comp_expr}) = %s "
            params.append(componente.strip().upper())
        if id_responsable:
            where += """
                AND EXISTS (
                    SELECT 1
                    FROM sne.ics_motivo_resp imr
                    WHERE imr.id_ics = i.id_ics
                      AND imr.responsable = %s
                )
            """
            params.append(int(id_responsable))
        if texto_busqueda and texto_busqueda.strip():
            termino = f"%{texto_busqueda.strip()}%"
            search_clauses = [
                "i.fecha::text ILIKE %s",
                "i.id_ics::text ILIKE %s",
                "COALESCE(r.ruta_comercial::text, '') ILIKE %s",
                "COALESCE(i.servicio::text, '') ILIKE %s",
                "COALESCE(i.tabla::text, '') ILIKE %s",
                "COALESCE(i.viaje_linea::text, '') ILIKE %s",
                "COALESCE(i.id_viaje::text, '') ILIKE %s",
                "COALESCE(i.sentido::text, '') ILIKE %s",
                "COALESCE(i.vehiculo_real::text, '') ILIKE %s",
                "COALESCE(i.hora_ini_teorica::text, '') ILIKE %s",
                "COALESCE(i.km_prog_ad::text, '') ILIKE %s",
                "COALESCE(i.conductor::text, '') ILIKE %s",
                "COALESCE(i.km_elim_eic::text, '') ILIKE %s",
                "COALESCE(i.km_ejecutado::text, '') ILIKE %s",
                "COALESCE(i.offset_inicio::text, '') ILIKE %s",
                "COALESCE(i.offset_fin::text, '') ILIKE %s",
                "COALESCE(i.km_revision::text, '') ILIKE %s",
                "COALESCE(i.motivo::text, '') ILIKE %s",
                """
                EXISTS (
                    SELECT 1
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
                    WHERE imr.id_ics = i.id_ics
                      AND COALESCE(me.motivo, '') ILIKE %s
                )
                """,
                """
                EXISTS (
                    SELECT 1
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.responsable_sne rs ON rs.id = imr.responsable
                    WHERE imr.id_ics = i.id_ics
                      AND COALESCE(rs.responsable, '') ILIKE %s
                )
                """,
            ]
            where += " AND (" + " OR ".join(search_clauses) + ") "
            params.extend([termino] * len(search_clauses))

        # COUNT
        sql_count = f"""
            SELECT COUNT(*) AS count
            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            {joins_cop}
            {where}
        """
        self.cursor.execute(sql_count, params)
        total = self.cursor.fetchone()["count"]

        offset = (pagina - 1) * tamano
        order_sql = {
            "km_revision_desc": "i.km_revision DESC NULLS LAST, gs.id_ics ASC",
            "km_revision_asc": "i.km_revision ASC NULLS LAST, gs.id_ics ASC",
            "id_ics_desc": "gs.id_ics DESC",
            "id_ics_asc": "gs.id_ics ASC",
        }.get(orden, "i.fecha DESC, gs.id_ics ASC")
        sql = f"""
            SELECT
                gs.id_ics,
                gs.revisor,
                gs.estado_asignacion,
                gs.estado_objecion,
                gs.estado_transmitools,
                to_char(gs.fecha_hora_asignacion, 'YYYY-MM-DD HH24:MI') AS fecha_asignacion,
                to_char(gs.fecha_hora_objecion,   'YYYY-MM-DD HH24:MI') AS fecha_objecion,
                {fecha_cierre_dp_expr}                                   AS fecha_cierre_dp,

                i.fecha::text                                            AS fecha,
                i.id_linea,
                i.servicio,
                i.tabla,
                i.viaje_linea,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                i.hora_ini_teorica::text                                 AS hora_ini_teorica,
                i.km_prog_ad,
                i.conductor,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                i.motivo,
                i.id_concesion,
                r.ruta_comercial,
                r.id_cop,
                c.cop                                                    AS cop_nombre,
                {comp_expr}                                              AS componente,
                {zona_expr}                                              AS zona,

                -- Motivos resueltos desde ics_motivo_resp → motivos_eliminacion
                (
                    SELECT string_agg(me.motivo, ', ' ORDER BY me.motivo)
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
                    WHERE imr.id_ics = i.id_ics
                )                                                        AS motivo_nombre,

                -- Responsables resueltos desde ics_motivo_resp → responsable_sne
                (
                    SELECT string_agg(rs.responsable, ', ' ORDER BY rs.responsable)
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.responsable_sne rs ON rs.id = imr.responsable
                    WHERE imr.id_ics = i.id_ics
                )                                                        AS responsable_nombre,

                -- Km objetado y ruta del informe PDF (última objeción guardada para este ICS)
                (
                    SELECT og.km_objetado
                    FROM sne.objecion_guardada og
                    WHERE og.id_ics = i.id_ics
                    ORDER BY og.fecha_guardado DESC
                    LIMIT 1
                )                                                        AS km_objetado_guardado,
                (
                    SELECT og.reporte
                    FROM sne.objecion_guardada og
                    WHERE og.id_ics = i.id_ics
                    ORDER BY og.fecha_guardado DESC
                    LIMIT 1
                )                                                        AS reporte

            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            {joins_cop}
            {where}
            ORDER BY {order_sql}
            LIMIT %s OFFSET %s
        """
        self.cursor.execute(sql, params + [tamano, offset])
        raw = self.cursor.fetchall()

        # ── Calcular estado_objecion_desc y auto-vencimiento ──────────────────
        ahora = ahora_bogota()
        registros = []
        for row in raw:
            row = dict(row)
            estado = row.get("estado_objecion")
            fecha_cierre_str = row.get("fecha_cierre_dp")

            # Parsear fecha_cierre_dp
            fecha_cierre = None
            if fecha_cierre_str:
                try:
                    fecha_cierre = datetime.strptime(fecha_cierre_str, "%Y-%m-%d %H:%M:%S")
                    fecha_cierre = fecha_cierre.replace(tzinfo=TZ_BOGOTA)
                except Exception:
                    fecha_cierre = None

            # Determinar estado descriptivo
            if estado == 1:
                row["estado_objecion_desc"] = "Objetado"
            elif estado == 2:
                row["estado_objecion_desc"] = "Vencido"
            elif estado == 0 or estado is None:
                if fecha_cierre and fecha_cierre < ahora:
                    # Vencido: fecha_cierre ya pasó → auto-cambiar a 2
                    row["estado_objecion_desc"] = "Vencido"
                else:
                    row["estado_objecion_desc"] = "Abierto"
            else:
                row["estado_objecion_desc"] = "--"

            registros.append(row)

        return registros, total

    def obtener_detalle_ics(self, id_ics: int, usuario_id: int):
        """
        Detalle completo de un ICS para el modal de gestion.
        Solo retorna si el id_ics esta asignado al revisor dado.
        NOTA: sne.gestion_sne usa id_ics como PK, no tiene columna 'id'.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        has_fcp = self._col_exists("sne", "gestion_sne", "fecha_cierre_dp")
        fcp_expr = "to_char(gs.fecha_cierre_dp, 'YYYY-MM-DD HH24:MI:SS')" if has_fcp else "NULL::text"
        self.cursor.execute(
            f"""
            SELECT
                gs.id_ics,
                gs.revisor,
                gs.estado_asignacion,
                gs.usuario_asigna,
                gs.estado_objecion,
                gs.usuario_objeta,
                gs.estado_transmitools,
                to_char(gs.fecha_hora_asignacion, 'YYYY-MM-DD HH24:MI') AS fecha_asignacion,
                to_char(gs.fecha_hora_objecion,   'YYYY-MM-DD HH24:MI') AS fecha_objecion,
                {fcp_expr}                                               AS fecha_cierre_dp,
                i.fecha::text                                            AS fecha,
                i.id_linea,
                i.servicio,
                i.tabla,
                i.viaje_linea,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                i.hora_ini_teorica::text                                 AS hora_ini_teorica,
                i.km_prog_ad,
                i.conductor,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                i.motivo,
                i.id_concesion,
                r.ruta_comercial,
                r.id_cop,
                c.cop                                                    AS cop_nombre,
                {comp_expr}                                              AS componente,
                {zona_expr}                                              AS zona,

                -- Motivos resueltos desde ics_motivo_resp → motivos_eliminacion
                (
                    SELECT string_agg(me.motivo, ', ' ORDER BY me.motivo)
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
                    WHERE imr.id_ics = i.id_ics
                )                                                        AS motivo_nombre,

                -- Responsables resueltos desde ics_motivo_resp → responsable_sne
                (
                    SELECT string_agg(rs.responsable, ', ' ORDER BY rs.responsable)
                    FROM sne.ics_motivo_resp imr
                    JOIN sne.responsable_sne rs ON rs.id = imr.responsable
                    WHERE imr.id_ics = i.id_ics
                )                                                        AS responsable_nombre
            
            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            {joins_cop}
            WHERE gs.id_ics = %s AND gs.revisor = %s
            """,
            (id_ics, usuario_id),
        )
        return self.cursor.fetchone()

    def estadisticas_usuario(self, usuario_id: int, fecha: str = None):
        """
        Estadisticas para los cards del header.
        Reglas de estado aplicadas:
          - Abierto  : estado_objecion=0 Y fecha_cierre_dp > ahora (plazo vigente)
          - Objetado : estado_objecion=1  (guardado dentro del plazo — definitivo)
          - Vencido  : estado_objecion=2 O (estado_objecion=0 Y fecha_cierre_dp <= ahora)
        """
        where, params = " WHERE gs.revisor = %s ", [usuario_id]
        if fecha:
            where += " AND i.fecha = %s "
            params.append(fecha)
        self.cursor.execute(
            f"""
            SELECT
                COUNT(*)                                            AS total_asignados,
                -- Pendientes = asignados que NO fueron objetados
                COUNT(*) FILTER (WHERE gs.estado_objecion IS DISTINCT FROM 1)
                                                                    AS total_pendientes,
                COUNT(*) FILTER (WHERE gs.estado_objecion = 1)     AS total_objetados,
                COUNT(*) FILTER (
                    WHERE (gs.estado_objecion = 0 OR gs.estado_objecion IS NULL)
                      AND (gs.fecha_cierre_dp IS NULL OR gs.fecha_cierre_dp > NOW() AT TIME ZONE 'America/Bogota')
                )                                                   AS total_abiertos,
                COUNT(*) FILTER (
                    WHERE gs.estado_objecion = 2
                    OR (
                        (gs.estado_objecion = 0 OR gs.estado_objecion IS NULL)
                        AND gs.fecha_cierre_dp IS NOT NULL
                        AND gs.fecha_cierre_dp < NOW() AT TIME ZONE 'America/Bogota'
                    )
                )                                                   AS total_vencidos,
                -- KM revisión total del día/usuario
                COALESCE(SUM(i.km_revision), 0)                    AS km_revision_total,
                -- KM objetado = suma de km_objetado guardados en objecion_guardada (último por id_ics)
                COALESCE(SUM(og_lat.km_objetado), 0)               AS km_objetado_total,
                COALESCE(SUM(i.km_prog_ad), 0)                     AS km_totales_asignados,
                COALESCE(SUM(i.km_ejecutado), 0)                   AS km_ejecutados
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN LATERAL (
                SELECT km_objetado
                FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics
                ORDER BY fecha_guardado DESC
                LIMIT 1
            ) og_lat ON true
            {where}
            """,
            params,
        )
        return self.cursor.fetchone()

    # ── Dashboard de Resultados ──────────────────────────────────────────────────

    def _dash_where(self, usuario_id, fecha_ini, fecha_fin, id_linea=None, id_cop=None):
        """Construye WHERE + params comunes para consultas del dashboard."""
        where  = " WHERE gs.revisor = %s AND gs.estado_asignacion = 1 "
        params = [usuario_id]
        if fecha_ini:
            where += " AND i.fecha >= %s "; params.append(fecha_ini)
        if fecha_fin:
            where += " AND i.fecha <= %s "; params.append(fecha_fin)
        if id_linea:
            where += " AND i.id_linea = %s "; params.append(int(id_linea))
        if id_cop:
            where += " AND r.id_cop = %s "; params.append(int(id_cop))
        return where, params

    def dashboard_comportamiento_registros(self, usuario_id, fecha_ini=None, fecha_fin=None,
                                           id_linea=None, id_cop=None):
        """Conteo diario de IDs Asignados e IDs Revisados (estado_objecion=1)."""
        where, params = self._dash_where(usuario_id, fecha_ini, fecha_fin, id_linea, id_cop)
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text                                                AS fecha,
                COUNT(gs.id_ics)                                            AS ids_asignados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion = 1)     AS ids_revisados
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """, params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def dashboard_comportamiento_km(self, usuario_id, fecha_ini=None, fecha_fin=None,
                                    id_linea=None, id_cop=None):
        """Suma diaria de km_revision y km_objetado (de objecion_guardada)."""
        where, params = self._dash_where(usuario_id, fecha_ini, fecha_fin, id_linea, id_cop)
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text                                                           AS fecha,
                COALESCE(SUM(i.km_revision), 0)                                        AS km_revision,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1 THEN og_lat.km_objetado
                               ELSE 0 END), 0)                                          AS km_objetado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """, params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def dashboard_distribucion_motivos(self, usuario_id, fecha_ini=None, fecha_fin=None,
                                       id_linea=None, id_cop=None):
        """Distribución de km_revision por motivo y por responsable."""
        where, params = self._dash_where(usuario_id, fecha_ini, fecha_fin, id_linea, id_cop)
        # Por motivo
        self.cursor.execute(
            f"""
            SELECT me.motivo AS etiqueta, COALESCE(SUM(i.km_revision), 0) AS km_total
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
            {where}
            GROUP BY me.motivo ORDER BY km_total DESC LIMIT 15
            """, params,
        )
        por_motivo = [dict(r) for r in self.cursor.fetchall()]
        # Por responsable
        self.cursor.execute(
            f"""
            SELECT rs.responsable AS etiqueta, COALESCE(SUM(i.km_revision), 0) AS km_total
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            JOIN sne.responsable_sne rs ON rs.id = imr.responsable
            {where}
            GROUP BY rs.responsable ORDER BY km_total DESC LIMIT 15
            """, params,
        )
        por_responsable = [dict(r) for r in self.cursor.fetchall()]
        return {"por_motivo": por_motivo, "por_responsable": por_responsable}

    def dashboard_por_ruta(self, usuario_id, fecha_ini=None, fecha_fin=None,
                           id_linea=None, id_cop=None):
        """Datos por año/mes/ruta para scatter chart y tabla."""
        where, params = self._dash_where(usuario_id, fecha_ini, fecha_fin, id_linea, id_cop)
        self.cursor.execute(
            f"""
            SELECT
                EXTRACT(YEAR  FROM i.fecha)::int                            AS anio,
                EXTRACT(MONTH FROM i.fecha)::int                            AS mes,
                COALESCE(r.ruta_comercial::text, 'Sin Ruta')                AS ruta_comercial,
                COUNT(gs.id_ics)                                            AS ids_asignados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion = 1)     AS ids_revisados,
                COALESCE(SUM(i.km_revision), 0)                             AS km_revision,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0)     AS km_objetado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
            GROUP BY EXTRACT(YEAR FROM i.fecha), EXTRACT(MONTH FROM i.fecha), r.ruta_comercial
            ORDER BY anio, mes, ruta_comercial
            """, params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def dashboard_meta_fechas(self, usuario_id):
        """
        Retorna la última fecha con asignaciones del usuario y el primer día
        de ese mes — usados como fecha_fin / fecha_ini por defecto en el dashboard.
        """
        self.cursor.execute(
            """
            SELECT
                MAX(i.fecha)::text                              AS ultima_fecha,
                date_trunc('month', MAX(i.fecha))::date::text  AS primer_dia_mes
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            WHERE gs.revisor = %s
              AND gs.estado_asignacion = 1
            """,
            (usuario_id,),
        )
        from datetime import date as _date
        row = self.cursor.fetchone()
        if row and row.get("ultima_fecha"):
            return {
                "ultima_fecha":   row["ultima_fecha"],
                "primer_dia_mes": row["primer_dia_mes"],
            }
        # Fallback: hoy + primer día del mes actual
        hoy = _date.today()
        return {
            "ultima_fecha":   hoy.isoformat(),
            "primer_dia_mes": hoy.replace(day=1).isoformat(),
        }

    def dashboard_rutas_disponibles(self, usuario_id, fecha_ini=None, fecha_fin=None):
        """
        Rutas comerciales distintas del usuario dentro del rango de fechas,
        para poblar el select de filtro del dashboard.
        """
        where  = " WHERE gs.revisor = %s AND gs.estado_asignacion = 1 "
        params = [usuario_id]
        if fecha_ini:
            where += " AND i.fecha >= %s "; params.append(fecha_ini)
        if fecha_fin:
            where += " AND i.fecha <= %s "; params.append(fecha_fin)
        self.cursor.execute(
            f"""
            SELECT DISTINCT
                i.id_linea,
                COALESCE(r.ruta_comercial::text, 'Sin Ruta') AS ruta_comercial
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            {where}
            ORDER BY ruta_comercial
            """,
            params,
        )
        return [dict(row) for row in self.cursor.fetchall()]

    # ── Reportes de tablas relacionadas por id_ics ───────────────────────────────
    REPORT_TABLES = {
        "detallado":           {"schema": "sne", "table": "detallado",           "label": "Detallado"},
        "tabla_acciones":      {"schema": "sne", "table": "tabla_acciones",      "label": "Acciones"},
        "acciones_regulacion": {"schema": "sne", "table": "acciones_regulacion", "label": "Acciones de Regulación"},
        "actividad_bus":       {"schema": "sne", "table": "actividad_bus",       "label": "Actividad Bus"},
        "asignaciones":        {"schema": "sne", "table": "asignaciones",        "label": "Asignaciones"},
        "desvios":             {"schema": "sne", "table": "desvios",             "label": "Desvíos"},
        "bitacora_notas":      {"schema": "sne", "table": "bitacora_notas",      "label": "Notas Bitácora"},
        "validaciones":        {"schema": "sne", "table": "validaciones",        "label": "Validaciones"},
        "viajestat":           {"schema": "sne", "table": "viajestat",           "label": "ViajeStat"},
    }

    def _tabla_existe(self, schema: str, table: str) -> bool:
        """Verifica si una tabla existe en el schema dado."""
        self.cursor.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return self.cursor.fetchone() is not None

    def obtener_conteo_reportes(self, id_ics: int) -> dict:
        """
        Retorna un dict con conteos de registros para cada tabla de reporte.
        Si la tabla no existe en la DB indica existe=False.
        """
        resultado = {}
        for key, meta in self.REPORT_TABLES.items():
            schema, table = meta["schema"], meta["table"]
            if not self._tabla_existe(schema, table):
                resultado[key] = {"label": meta["label"], "existe": False, "total": 0}
                continue
            self.cursor.execute(
                f'SELECT COUNT(*) AS total FROM {schema}."{table}" WHERE id_ics = %s',
                (id_ics,),
            )
            row = self.cursor.fetchone()
            resultado[key] = {
                "label": meta["label"],
                "existe": True,
                "total": int(row["total"]) if row else 0,
            }
        return resultado

    def obtener_reporte(self, id_ics: int, tabla_key: str) -> dict | None:
        """
        Retorna columnas + datos de una tabla de reporte para un id_ics.
        Las columnas se obtienen dinámicamente desde information_schema.
        Retorna None si tabla_key no está en la whitelist.
        """
        if tabla_key not in self.REPORT_TABLES:
            return None
        meta   = self.REPORT_TABLES[tabla_key]
        schema, table = meta["schema"], meta["table"]

        if not self._tabla_existe(schema, table):
            return {
                "key":     tabla_key,
                "label":   meta["label"],
                "existe":  False,
                "columnas": [],
                "datos":   [],
                "total":   0,
            }

        self.cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        columnas = [{"nombre": c["column_name"], "tipo": c["data_type"]} for c in self.cursor.fetchall()]

        self.cursor.execute(
            f'SELECT * FROM {schema}."{table}" WHERE id_ics = %s ORDER BY 1',
            (id_ics,),
        )
        datos = []
        for row in self.cursor.fetchall():
            row_dict = {}
            for k, v in dict(row).items():
                row_dict[k] = v.isoformat() if hasattr(v, "isoformat") else v
            datos.append(row_dict)

        return {
            "key":      tabla_key,
            "label":    meta["label"],
            "existe":   True,
            "columnas": columnas,
            "datos":    datos,
            "total":    len(datos),
        }

    def obtener_todos_reportes(self, id_ics: int) -> list:
        """Retorna datos completos de todas las tablas de reporte para un id_ics."""
        resultado = []
        for key in self.REPORT_TABLES:
            data = self.obtener_reporte(id_ics, key)
            if data is not None:
                resultado.append(data)
        return resultado

    # -- Registro y Guardado de la objeción de un ICS desde el modal de gestión --
    def actualizar_gestion(
        self,
        id_ics: int,
        usuario_id: int,
        estado_asignacion: int = None,
        estado_objecion: int = None,
        estado_transmitools: int = None,
        motivo: str = None,
        id_responsable: int = None,
        id_accion: int = None,
        id_justificacion: int = None,
        km_objetado: float = None,
    ):
        """
        Actualiza el estado de gestion de un ICS.
        Solo el revisor asignado puede modificarlo.
        NOTA: sne.gestion_sne usa id_ics como PK, no tiene columna 'id'.
        """
        # Actualizar campos en sne.ics si aplica
        ics_sets, ics_params = [], []
        if motivo is not None:
            ics_sets.append("motivo = %s")
            ics_params.append(motivo)
        if km_objetado is not None:
            ics_sets.append("km_objetado = %s")
            ics_params.append(km_objetado)
        if ics_sets:
            ics_params.append(id_ics)
            self.cursor.execute(
                f"UPDATE sne.ics SET {', '.join(ics_sets)} WHERE id_ics = %s",
                ics_params,
            )

        self.cursor.execute(
            "SELECT id_ics FROM sne.gestion_sne WHERE id_ics = %s AND revisor = %s",
            (id_ics, usuario_id),
        )
        row = self.cursor.fetchone()
        if not row:
            raise ValueError(
                f"ICS {id_ics} no encontrado o no asignado al usuario {usuario_id}"
            )

        sets, params = [], []
        ahora = ahora_bogota()

        if estado_asignacion is not None:
            sets.append("estado_asignacion = %s")
            params.append(estado_asignacion)
            if estado_asignacion == 2:
                sets.append("fecha_hora_asignacion = %s")
                params.append(ahora)

        if estado_objecion is not None:
            sets.append("estado_objecion = %s")
            params.append(estado_objecion)
            if estado_objecion == 1:
                sets.append("usuario_objeta = %s")
                params.append(usuario_id)
                sets.append("fecha_hora_objecion = %s")
                params.append(ahora)

        if estado_transmitools is not None:
            sets.append("estado_transmitools = %s")
            params.append(estado_transmitools)
            if estado_transmitools == 1:
                sets.append("fecha_hora_transmitools = %s")
                params.append(ahora)

        if id_responsable is not None:
            sets.append("id_responsable = %s")
            params.append(id_responsable)

        if id_accion is not None:
            sets.append("id_accion = %s")
            params.append(id_accion)

        if id_justificacion is not None:
            sets.append("id_justificacion = %s")
            params.append(id_justificacion)

        if motivo is not None:
            self.cursor.execute(
                "UPDATE sne.ics SET motivo = %s WHERE id_ics = %s",
                (motivo, id_ics),
            )

        if sets:
            sets.append("actualizado_en = %s")
            params.append(ahora)
            params.append(id_ics)
            self.cursor.execute(
                f"UPDATE sne.gestion_sne SET {', '.join(sets)} WHERE id_ics = %s",
                params,
            )
            self.connection.commit()

        return True

    # -- Guardado completo de objeción → sne.objecion_guardada + gestion_sne ------
    def guardar_objecion_completa(
        self,
        id_ics: int,
        usuario_id: int,
        km_objetado: float = None,
        km_no_objetado: float = None,
        id_responsable: int = None,
        id_motivo: int = None,
        id_accion: int = None,
        id_justificacion: int = None,
        nota_objecion: str = None,
        tiempo_objecion_seg: int = None,
        ruta_reporte: str = None,
    ):
        """
        Guarda la objeción en sne.objecion_guardada y actualiza sne.gestion_sne.
        Operación atómica en una sola transacción.

        Reglas de estado:
          Abierto  (0): fecha_cierre_dp no ha pasado y no está objetado.
          Objetado (1): se guardó la objeción dentro del plazo — estado definitivo,
                        ya no se recalcula como vencido.
          Vencido      : no está objetado Y fecha_cierre_dp ya pasó — no se puede guardar.
        """
        ahora = ahora_bogota()

        # ── Verificar que el ICS no esté vencido antes de guardar ────────────────
        self.cursor.execute(
            "SELECT estado_objecion, fecha_cierre_dp FROM sne.gestion_sne WHERE id_ics = %s",
            (id_ics,),
        )
        gs_check = self.cursor.fetchone()
        if gs_check:
            est_actual  = gs_check.get("estado_objecion")
            fcp_raw     = gs_check.get("fecha_cierre_dp")  # datetime o None
            if est_actual != 1:  # solo valida si NO está ya objetado
                if fcp_raw is not None:
                    fcp_tz = (
                        fcp_raw.replace(tzinfo=TZ_BOGOTA)
                        if fcp_raw.tzinfo is None
                        else fcp_raw
                    )
                    if fcp_tz < ahora:
                        raise ValueError(
                            f"El ICS {id_ics} se encuentra fuera de los tiempos de "
                            "objeción del debido proceso y no puede ser guardado."
                        )

        # Obtener km_revision actual desde sne.ics
        self.cursor.execute(
            "SELECT km_revision FROM sne.ics WHERE id_ics = %s",
            (id_ics,),
        )
        row_ics = self.cursor.fetchone()
        km_revision = float(row_ics["km_revision"]) if row_ics and row_ics["km_revision"] is not None else None

        # INSERT en sne.objecion_guardada
        self.cursor.execute(
            """
            INSERT INTO sne.objecion_guardada (
                id_ics, km_revision, km_objetado, km_no_objetado,
                responsable_final, motivo_final, accion, justificacion,
                nota_objecion, usuario_registra, fecha_guardado,
                tiempo_objecion_seg, reporte
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            """,
            (
                id_ics, km_revision, km_objetado, km_no_objetado,
                id_responsable, id_motivo, id_accion, id_justificacion,
                nota_objecion, usuario_id, ahora,
                tiempo_objecion_seg, ruta_reporte,
            ),
        )

        # UPDATE sne.gestion_sne: marcar como objetado
        # No se toca estado_asignacion — solo se actualiza estado_objecion
        self.cursor.execute(
            """
            UPDATE sne.gestion_sne
            SET estado_objecion     = 1,
                usuario_objeta      = %s,
                fecha_hora_objecion = %s,
                actualizado_en      = %s
            WHERE id_ics = %s
            """,
            (usuario_id, ahora, ahora, id_ics),
        )

        self.connection.commit()
        return True

    # -- Detalle de objeción guardada para tab Revisados (modo lectura) -----------
    def obtener_detalle_objecion_guardada(self, id_ics: int):
        """
        Retorna el detalle de la objeción guardada para un id_ics,
        con los nombres descriptivos de responsable, motivo, acción y justificación.
        Se usa en el modal de la pestaña 'Revisados' para mostrar en modo lectura.
        """
        self.cursor.execute(
            """
            SELECT
                og.id,
                og.id_ics,
                og.km_revision,
                og.km_objetado,
                og.km_no_objetado,
                og.responsable_final,
                rs.responsable                                   AS responsable_nombre,
                og.motivo_final,
                me.motivo                                        AS motivo_nombre,
                og.accion,
                ac.accion                                        AS accion_nombre,
                og.justificacion,
                ju.justificacion                                 AS justificacion_nombre,
                og.nota_objecion,
                og.usuario_registra,
                to_char(og.fecha_guardado AT TIME ZONE 'America/Bogota',
                         'YYYY-MM-DD HH24:MI:SS')               AS fecha_guardado,
                og.tiempo_objecion_seg,
                og.reporte
            FROM sne.objecion_guardada og
            LEFT JOIN sne.responsable_sne    rs ON rs.id              = og.responsable_final
            LEFT JOIN sne.motivos_eliminacion me ON me.id             = og.motivo_final
            LEFT JOIN sne.acciones           ac ON ac.id_acc          = og.accion
            LEFT JOIN sne.justificacion      ju ON ju.id_justificacion = og.justificacion
            WHERE og.id_ics = %s
            ORDER BY og.fecha_guardado DESC
            LIMIT 1
            """,
            (id_ics,),
        )
        return self.cursor.fetchone()
