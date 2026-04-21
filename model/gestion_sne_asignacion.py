import os
import math
import unicodedata
import uuid
from decimal import Decimal
from typing import Optional, Any
from psycopg2.extras import RealDictCursor
from psycopg2 import extensions as pg_extensions
from datetime import date, datetime
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import _get_pool as get_db_pool

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "xxxxxxxxxx"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")
CAPACIDAD_HABIL_DEFAULT = 70
CAPACIDAD_SABADO_DEFAULT = 35
CAPACIDAD_DOM_FEST_DEFAULT = 25
REVISORES_CONFIG_LOCK_KEY = 540011
REVERSION_MOTIVO_MIN_WORDS = 8
REVERSION_MOTIVO_MIN_CHARS = 30
REVERSION_MOTIVO_MAX_LENGTH = 1000

CAPACIDAD_POR_TIPO_DIA = {
    "habil": CAPACIDAD_HABIL_DEFAULT,
    "sabado": CAPACIDAD_SABADO_DEFAULT,
    "domingo": CAPACIDAD_DOM_FEST_DEFAULT,
    "festivo": CAPACIDAD_DOM_FEST_DEFAULT,
}

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

def _normalize_spaces(value: Any) -> str:
    return " ".join(str(value or "").strip().split())

def _validar_motivo_reversion(value: Any) -> str:
    motivo = _normalize_spaces(value)
    if not motivo:
        raise ValueError("Debes ingresar un motivo de reversion obligatorio")

    palabras = [part for part in motivo.split(" ") if part]
    if len(palabras) < REVERSION_MOTIVO_MIN_WORDS and len(motivo) < REVERSION_MOTIVO_MIN_CHARS:
        raise ValueError(
            f"El motivo de reversion debe tener minimo {REVERSION_MOTIVO_MIN_WORDS} palabras "
            f"o {REVERSION_MOTIVO_MIN_CHARS} caracteres"
        )

    return motivo[:REVERSION_MOTIVO_MAX_LENGTH]

def _json_safe_value(value):
    """Convierte valores no compatibles con JSON (NaN/Infinity) a None."""
    if isinstance(value, Decimal):
        try:
            if value.is_nan() or value.is_infinite():
                return None
            return float(value)
        except Exception:
            return None

    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return value

def _parse_iso_date(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None

def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return _parse_iso_date(value)

def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

def _normalize_datetime_for_compare(value: Any) -> Optional[datetime]:
    dt = _coerce_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TIMEZONE_BOGOTA)
    return dt.astimezone(TIMEZONE_BOGOTA)

def _normalize_component_key(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFD", raw)
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    if normalized == "ALIMENTACION":
        return "ALIMENTACION"
    if normalized == "ZONAL":
        return "ZONAL"
    if normalized == "GENERAL":
        return "GENERAL"
    return normalized

def _component_label(value: Any) -> str:
    key = _normalize_component_key(value)
    if key == "ZONAL":
        return "Zonal"
    if key == "ALIMENTACION":
        return "Alimentacion"
    if key == "GENERAL":
        return "General"
    return "Sin componente"

class RegistroSNE:
    def __init__(self):
        self.conn = get_db_pool().getconn()
        if not self.conn.closed:
            self.conn.rollback()
        self.conn.cursor_factory = RealDictCursor
        self._col_cache: dict[tuple[str, str, str], bool] = {}
        self._reviewer_config_ready = False
        self._manual_assignment_ready = False
        self._gestion_sne_ready = False
        self._reversion_history_ready = False
        with self.conn.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.conn.commit()

    def __enter__(self):
        self._sync_expired_assignment_states()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if not self.conn.closed:
                self.conn.rollback()
                self.conn.cursor_factory = pg_extensions.cursor
                get_db_pool().putconn(self.conn)

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

    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        key = (schema, table, column)
        if key not in self._col_cache:
            with self.conn.cursor() as c:
                c.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND column_name = %s
                    LIMIT 1
                    """,
                    (schema, table, column),
                )
                self._col_cache[key] = c.fetchone() is not None
        return self._col_cache[key]

    def _cop_joins_and_exprs(self) -> tuple[str, str]:
        joins = [
            "LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1",
            "LEFT JOIN config.cop c ON c.id = r.id_cop",
        ]
        comp_expr = "NULL::text"
        if self._col_exists("config", "cop", "id_componente"):
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        return " ".join(joins), comp_expr

    def _concesion_join_and_expr(self) -> tuple[str, str]:
        if self._col_exists("config", "zona", "id") and self._col_exists("config", "zona", "zona"):
            return (
                "LEFT JOIN config.zona zcon ON zcon.id = i.id_concesion",
                "COALESCE(NULLIF(zcon.zona, ''), i.id_concesion::text)",
            )
        return "", "i.id_concesion::text"

    def _user_display_name_expr(self, alias: str) -> str:
        return (
            f"COALESCE("
            f"NULLIF(TRIM(CONCAT_WS(' ', {alias}.nombres::text, {alias}.apellidos::text)), ''), "
            f"NULLIF({alias}.username::text, ''), "
            f"''"
            f")"
        )

    def _normalize_operativa_view(self, vista: Optional[str]) -> str:
        normalized = str(vista or "pendientes").strip().lower()
        aliases = {
            "asignados_hoy": "ejecutados",
            "historico": "ejecutados",
        }
        normalized = aliases.get(normalized, normalized)
        if normalized not in {"pendientes", "ultima_ejecucion", "ejecutados"}:
            return "pendientes"
        return normalized

    def _normalize_ids(self, ids_ics: list[int | str], empty_message: str) -> list[int]:
        normalizados: list[int] = []
        for raw_id in ids_ics or []:
            try:
                current_id = int(raw_id)
            except (TypeError, ValueError):
                raise ValueError("Todos los id_ics deben ser numericos")
            if current_id not in normalizados:
                normalizados.append(current_id)

        if not normalizados:
            raise ValueError(empty_message)
        return normalizados

    def _ensure_gestion_sne_table(self) -> None:
        if self._gestion_sne_ready:
            return

        self._execute("CREATE SCHEMA IF NOT EXISTS sne;")
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sne.gestion_sne (
                id_ics                   BIGINT PRIMARY KEY,
                revisor                  INTEGER      NOT NULL DEFAULT 0,
                estado_asignacion        INTEGER      NOT NULL DEFAULT 0,
                usuario_asigna           INTEGER,
                fecha_hora_asignacion    TIMESTAMPTZ,
                id_ejecucion             VARCHAR(60),
                estado_objecion          INTEGER      NOT NULL DEFAULT 0,
                usuario_objeta           INTEGER,
                fecha_hora_objecion      TIMESTAMPTZ,
                estado_transmitools      INTEGER      NOT NULL DEFAULT 0,
                fecha_hora_transmitools  TIMESTAMPTZ,
                created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            );
            """
        )
        self._execute(
            """
            ALTER TABLE sne.gestion_sne
            ADD COLUMN IF NOT EXISTS usuario_asigna INTEGER;
            """
        )
        self._execute(
            """
            ALTER TABLE sne.gestion_sne
            ADD COLUMN IF NOT EXISTS fecha_hora_asignacion TIMESTAMPTZ;
            """
        )
        self._execute(
            """
            ALTER TABLE sne.gestion_sne
            ADD COLUMN IF NOT EXISTS id_ejecucion VARCHAR(60);
            """
        )
        self._execute(
            """
            ALTER TABLE sne.gestion_sne
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """
        )
        self._execute(
            """
            ALTER TABLE sne.gestion_sne
            ADD COLUMN IF NOT EXISTS fecha_cierre_dp TIMESTAMP;
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gestion_sne_estado_asignacion
                ON sne.gestion_sne (estado_asignacion, id_ics);
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gestion_sne_id_ejecucion
                ON sne.gestion_sne (id_ejecucion);
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gestion_sne_fecha_hora_asignacion
                ON sne.gestion_sne (fecha_hora_asignacion DESC);
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gestion_sne_fecha_cierre_dp
                ON sne.gestion_sne (fecha_cierre_dp);
            """
        )

        self._col_cache[("sne", "gestion_sne", "usuario_asigna")] = True
        self._col_cache[("sne", "gestion_sne", "fecha_hora_asignacion")] = True
        self._col_cache[("sne", "gestion_sne", "id_ejecucion")] = True
        self._col_cache[("sne", "gestion_sne", "fecha_cierre_dp")] = True
        self._gestion_sne_ready = True

    def _ensure_reversion_history_table(self) -> None:
        if self._reversion_history_ready:
            return

        self._execute("CREATE SCHEMA IF NOT EXISTS sne;")
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sne.historial_reversion_asignacion (
                id                                      BIGSERIAL PRIMARY KEY,
                id_ics                                  BIGINT NOT NULL,
                id_ejecucion                            VARCHAR(60),
                revisor_anterior                        INTEGER,
                usuario_asigna_anterior                 INTEGER,
                fecha_hora_asignacion_anterior          TIMESTAMPTZ,
                estado_asignacion_anterior              INTEGER,
                estado_objecion_anterior                INTEGER,
                estado_transmitools_anterior            INTEGER,
                fecha_cierre_dp                         TIMESTAMP,
                usuario_revierte_id                     INTEGER,
                usuario_revierte                        VARCHAR(100),
                motivo                                  TEXT,
                resultado                               VARCHAR(40) NOT NULL,
                detalle                                 TEXT,
                ip_origen                               VARCHAR(45),
                created_at                              TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hist_reversion_asignacion_ics
                ON sne.historial_reversion_asignacion (id_ics, created_at DESC);
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hist_reversion_asignacion_ejecucion
                ON sne.historial_reversion_asignacion (id_ejecucion, created_at DESC);
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hist_reversion_asignacion_resultado
                ON sne.historial_reversion_asignacion (resultado, created_at DESC);
            """
        )
        self._reversion_history_ready = True

    def _apply_dp_state(self, row: dict[str, Any], today: date) -> dict[str, Any]:
        fecha_cierre = _coerce_date(row.get("fecha_cierre_dp"))
        if fecha_cierre is None:
            row["dias_para_cierre_dp"] = None
            row["estado_dp"] = "SIN_FECHA_DP"
            row["dp_vigente"] = False
            return row

        dias = (fecha_cierre - today).days
        row["dias_para_cierre_dp"] = dias
        row["dp_vigente"] = dias >= 0
        if dias < 0:
            row["estado_dp"] = "VENCIDO"
        elif dias == 0:
            row["estado_dp"] = "VENCE_HOY"
        else:
            row["estado_dp"] = "VIGENTE"
        return row

    def _apply_dp_state_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        today = now_bogota().date()
        for row in rows:
            if isinstance(row, dict):
                self._apply_dp_state(row, today)
        return rows

    def _sync_expired_assignment_states(self) -> None:
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()

        today = now_bogota().date()
        with self.conn.cursor() as c:
            c.execute(
                """
                UPDATE sne.gestion_sne gs
                SET
                    estado_asignacion = 2
                WHERE gs.fecha_cierre_dp IS NOT NULL
                  AND gs.fecha_cierre_dp::date < %s
                  AND COALESCE(gs.estado_asignacion, 0) <> 2
                """,
                [today],
            )
            c.execute(
                """
                UPDATE sne.gestion_sne gs
                SET
                    estado_asignacion = CASE
                        WHEN COALESCE(gs.revisor, 0) > 0
                          OR gs.fecha_hora_asignacion IS NOT NULL
                        THEN 1
                        ELSE 0
                    END,
                    updated_at = NOW()
                WHERE COALESCE(gs.estado_asignacion, 0) = 2
                  AND (
                      gs.fecha_cierre_dp IS NULL
                      OR gs.fecha_cierre_dp::date >= %s
                  )
                """,
                [today],
            )
            c.execute(
                """
                DELETE FROM sne.ics_revisor_asignacion a
                USING sne.gestion_sne gs
                WHERE a.id_ics = gs.id_ics
                  AND COALESCE(gs.estado_asignacion, 0) = 2
                  AND COALESCE(gs.revisor, 0) = 0
                  AND gs.fecha_hora_asignacion IS NULL
                """
            )

    def _resolve_latest_execution_id(
        self,
        fecha: Optional[str] = None,
        usuario_asigna_id: Optional[int] = None,
        fecha_ejecucion: Optional[str] = None,
    ) -> Optional[str]:
        self._ensure_gestion_sne_table()

        def _lookup(filter_user: bool) -> Optional[str]:
            where_clauses = [
                "gs.id_ejecucion IS NOT NULL",
                "gs.fecha_hora_asignacion IS NOT NULL",
                "COALESCE(gs.revisor, 0) > 0",
            ]
            params: list[Any] = []
            fecha_operativa = _parse_iso_date(fecha)
            fecha_ejec = _parse_iso_date(fecha_ejecucion)

            if fecha_operativa is not None:
                where_clauses.append("i.fecha = %s")
                params.append(fecha_operativa)
            elif fecha:
                where_clauses.append("i.fecha::text = %s")
                params.append(str(fecha))
            if fecha_ejec is not None:
                where_clauses.append(
                    "(gs.fecha_hora_asignacion AT TIME ZONE 'America/Bogota')::date = %s"
                )
                params.append(fecha_ejec)
            elif fecha_ejecucion:
                where_clauses.append(
                    "(gs.fecha_hora_asignacion AT TIME ZONE 'America/Bogota')::date = %s::date"
                )
                params.append(str(fecha_ejecucion))
            if filter_user and usuario_asigna_id is not None:
                where_clauses.append("gs.usuario_asigna = %s")
                params.append(int(usuario_asigna_id))

            row = self._fetchone(
                f"""
                SELECT
                    gs.id_ejecucion,
                    MAX(gs.fecha_hora_asignacion) AS fecha_hora_asignacion
                FROM sne.gestion_sne gs
                JOIN sne.ics i
                  ON i.id_ics = gs.id_ics
                WHERE {" AND ".join(where_clauses)}
                GROUP BY gs.id_ejecucion
                ORDER BY MAX(gs.fecha_hora_asignacion) DESC, gs.id_ejecucion DESC
                LIMIT 1
                """,
                params,
            )
            return str(row["id_ejecucion"]) if row and row.get("id_ejecucion") else None

        if usuario_asigna_id is not None:
            result = _lookup(True)
            if result:
                return result
        return _lookup(False)

    def _build_execution_summary(self, id_ejecucion: Optional[str]) -> Optional[dict[str, Any]]:
        self._ensure_gestion_sne_table()
        if not id_ejecucion:
            return None

        joins_cop, comp_expr = self._cop_joins_and_exprs()
        reviewer_name_expr = self._user_display_name_expr("ur")
        executor_name_expr = self._user_display_name_expr("ua")

        summary = self._fetchone(
            f"""
            SELECT
                gs.id_ejecucion,
                COUNT(*) AS total,
                MIN(gs.usuario_asigna) AS usuario_asigna,
                MAX(gs.fecha_hora_asignacion) AS fecha_hora_asignacion,
                COALESCE(MAX({executor_name_expr}), '') AS usuario_asigna_nombre
            FROM sne.gestion_sne gs
            LEFT JOIN public.usuarios ua
              ON ua.id = gs.usuario_asigna
            WHERE gs.id_ejecucion = %s
            GROUP BY gs.id_ejecucion
            """,
            [id_ejecucion],
        )
        if not summary:
            return None

        reviewers = self._fetchall(
            f"""
            SELECT
                COALESCE(NULLIF({reviewer_name_expr}, ''), 'Sin revisor') AS label,
                COUNT(*) AS total
            FROM sne.gestion_sne gs
            LEFT JOIN public.usuarios ur
              ON ur.id = NULLIF(gs.revisor, 0)
            WHERE gs.id_ejecucion = %s
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1 ASC
            """,
            [id_ejecucion],
        )
        components = self._fetchall(
            f"""
            SELECT
                COALESCE(NULLIF({comp_expr}, ''), 'Sin componente') AS label,
                COUNT(*) AS total
            FROM sne.gestion_sne gs
            JOIN sne.ics i
              ON i.id_ics = gs.id_ics
            {joins_cop}
            WHERE gs.id_ejecucion = %s
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1 ASC
            """,
            [id_ejecucion],
        )

        return {
            "id_ejecucion": str(summary["id_ejecucion"]),
            "total": int(summary["total"] or 0),
            "usuario_asigna": int(summary["usuario_asigna"]) if summary.get("usuario_asigna") is not None else None,
            "usuario_asigna_nombre": _json_safe_value(summary.get("usuario_asigna_nombre")),
            "fecha_hora_asignacion": _json_safe_value(summary.get("fecha_hora_asignacion")),
            "por_revisor": [
                {
                    "label": _json_safe_value(row.get("label")),
                    "total": int(row.get("total") or 0),
                }
                for row in (reviewers or [])
            ],
            "por_componente": [
                {
                    "label": _json_safe_value(row.get("label")),
                    "total": int(row.get("total") or 0),
                }
                for row in (components or [])
            ],
        }

    # ---------------------------
    # Tabla ICS — datos completos
    # ---------------------------
    _KM_COLS = frozenset(
        ("km_prog_ad", "km_elim_eic", "km_ejecutado", "offset_inicio", "offset_fin", "km_revision")
    )

    def listar_ics_tabla(
        self,
        pagina: int = 1,
        tamano: int = 50,
        include_summary: bool = True,
        vista: str = "pendientes",
        id_ejecucion: Optional[str] = None,
        usuario_actual_id: Optional[int] = None,
        id_ics: Optional[str] = None,
        fecha: Optional[str] = None,
        id_linea: Optional[str] = None,
        ruta_comercial: Optional[str] = None,
        vehiculo_real: Optional[str] = None,
        conductor: Optional[str] = None,
        motivos_filter: Optional[str] = None,
        responsables_filter: Optional[str] = None,
        servicio: Optional[str] = None,
        tabla: Optional[str] = None,
        viaje_linea: Optional[str] = None,
        id_viaje: Optional[str] = None,
        sentido: Optional[str] = None,
        hora_ini_teorica: Optional[str] = None,
        hora_ini_teorica_min: Optional[str] = None,
        hora_ini_teorica_max: Optional[str] = None,
        motivo_original: Optional[str] = None,
        id_concesion: Optional[str] = None,
        fecha_carga: Optional[str] = None,
        fecha_ejecucion: Optional[str] = None,
        componente: Optional[str] = None,
        revisor_asignado: Optional[str] = None,
        usuario_asigna_nombre: Optional[str] = None,
        km_ranges: Optional[dict] = None,
    ) -> dict:
        """
        km_ranges: {col_name: (min_val, max_val)} donde cada valor puede ser None.
        Ej.: {"km_prog_ad": (5.0, None), "km_revision": (None, 20.0)}
        """
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()
        offset = (pagina - 1) * tamano
        joins_cop, comp_expr = self._cop_joins_and_exprs()
        concesion_join, concesion_expr = self._concesion_join_and_expr()
        ruta_expr = "COALESCE(NULLIF(r.ruta_comercial, ''), i.id_linea::text)"
        group_by_component = f", {comp_expr}" if comp_expr != "NULL::text" else ""
        group_by_concesion = ", zcon.zona" if concesion_join else ""
        group_by_ruta = ", r.ruta_comercial"
        vista_norm = self._normalize_operativa_view(vista)
        latest_execution_id = None
        if vista_norm == "ultima_ejecucion":
            requested_execution_id = str(id_ejecucion or "").strip() or None
            latest_execution_id = requested_execution_id or self._resolve_latest_execution_id(
                fecha=fecha,
                usuario_asigna_id=usuario_actual_id,
                fecha_ejecucion=fecha_ejecucion,
            )
            if not latest_execution_id:
                return {
                    "data": [],
                    "resumen_rows": [] if include_summary else None,
                    "total": 0,
                    "pagina": pagina,
                    "tamano": tamano,
                    "paginas": 0,
                    "vista": vista_norm,
                    "id_ejecucion": None,
                    "execution_summary": None,
                }

        reviewer_name_expr = self._user_display_name_expr("ur")
        executor_name_expr = self._user_display_name_expr("ua")
        reviewer_display_expr = (
            "CASE "
            "WHEN COALESCE(gs.estado_asignacion, 0) = 0 "
            f"THEN COALESCE(NULLIF(a.revisor_nombre, ''), NULLIF({reviewer_name_expr}, '')) "
            f"ELSE COALESCE(NULLIF({reviewer_name_expr}, ''), NULLIF(a.revisor_nombre, '')) "
            "END"
        )
        reviewer_user_id_expr = (
            "CASE "
            "WHEN COALESCE(gs.estado_asignacion, 0) = 0 "
            "THEN a.revisor_user_id "
            "ELSE NULLIF(gs.revisor, 0) "
            "END"
        )
        reviewer_origin_expr = (
            "CASE "
            "WHEN COALESCE(gs.estado_asignacion, 0) = 0 "
            "THEN COALESCE(a.origen, '') "
            "ELSE 'gestion_sne' "
            "END"
        )

        where_clauses: list[str] = []
        params_data: list = []
        params_count: list = []

        if vista_norm == "pendientes":
            where_clauses.append(
                "("
                "COALESCE(gs.estado_asignacion, 0) = 0 "
                "OR ("
                "COALESCE(gs.estado_asignacion, 0) = 2 "
                "AND COALESCE(gs.revisor, 0) = 0 "
                "AND gs.fecha_hora_asignacion IS NULL"
                ")"
                ")"
            )
        elif vista_norm == "ultima_ejecucion":
            where_clauses.append("gs.id_ejecucion = %s")
            params_data.append(latest_execution_id)
            params_count.append(latest_execution_id)
        elif vista_norm == "ejecutados":
            where_clauses.append("COALESCE(gs.revisor, 0) > 0")
            where_clauses.append("gs.fecha_hora_asignacion IS NOT NULL")
            fecha_ejec = _parse_iso_date(fecha_ejecucion)
            if fecha_ejecucion:
                if fecha_ejec is not None:
                    where_clauses.append(
                        "(gs.fecha_hora_asignacion AT TIME ZONE 'America/Bogota')::date = %s"
                    )
                    params_data.append(fecha_ejec)
                    params_count.append(fecha_ejec)
                else:
                    where_clauses.append(
                        "(gs.fecha_hora_asignacion AT TIME ZONE 'America/Bogota')::date = %s::date"
                    )
                    params_data.append(str(fecha_ejecucion))
                    params_count.append(str(fecha_ejecucion))

        if id_ics:
            where_clauses.append("i.id_ics::text ILIKE %s")
            params_data.append(f"%{id_ics}%")
            params_count.append(f"%{id_ics}%")

        if fecha:
            fechas_list = [f.strip() for f in fecha.split(',') if f.strip()]
            fechas_tipadas = [_parse_iso_date(item) for item in fechas_list]
            if fechas_list and all(item is not None for item in fechas_tipadas):
                if len(fechas_tipadas) == 1:
                    where_clauses.append("i.fecha = %s")
                    params_data.append(fechas_tipadas[0])
                    params_count.append(fechas_tipadas[0])
                elif len(fechas_tipadas) > 1:
                    placeholders = ', '.join(['%s'] * len(fechas_tipadas))
                    where_clauses.append(f"i.fecha IN ({placeholders})")
                    params_data.extend(fechas_tipadas)
                    params_count.extend(fechas_tipadas)
            elif len(fechas_list) == 1:
                where_clauses.append("i.fecha::text = %s")
                params_data.append(fechas_list[0])
                params_count.append(fechas_list[0])
            elif len(fechas_list) > 1:
                placeholders = ', '.join(['%s'] * len(fechas_list))
                where_clauses.append(f"i.fecha::text IN ({placeholders})")
                params_data.extend(fechas_list)
                params_count.extend(fechas_list)

        if id_linea:
            where_clauses.append("i.id_linea::text ILIKE %s")
            params_data.append(f"%{id_linea}%")
            params_count.append(f"%{id_linea}%")

        if ruta_comercial:
            where_clauses.append(f"{ruta_expr} ILIKE %s")
            params_data.append(f"%{ruta_comercial}%")
            params_count.append(f"%{ruta_comercial}%")

        if vehiculo_real:
            where_clauses.append("i.vehiculo_real ILIKE %s")
            params_data.append(f"%{vehiculo_real}%")
            params_count.append(f"%{vehiculo_real}%")

        if conductor:
            where_clauses.append("i.conductor::text ILIKE %s")
            params_data.append(f"%{conductor}%")
            params_count.append(f"%{conductor}%")

        if motivos_filter:
            where_clauses.append(
                "EXISTS ("
                "  SELECT 1 FROM sne.ics_motivo_resp imr2"
                "  JOIN sne.motivos_eliminacion me2 ON me2.id = imr2.motivo"
                "  WHERE imr2.id_ics = i.id_ics AND me2.motivo ILIKE %s"
                ")"
            )
            params_data.append(f"%{motivos_filter}%")
            params_count.append(f"%{motivos_filter}%")

        if responsables_filter:
            where_clauses.append(
                "EXISTS ("
                "  SELECT 1 FROM sne.ics_motivo_resp imr3"
                "  JOIN sne.responsable_sne rs3 ON rs3.id = imr3.responsable"
                "  WHERE imr3.id_ics = i.id_ics AND rs3.responsable ILIKE %s"
                ")"
            )
            params_data.append(f"%{responsables_filter}%")
            params_count.append(f"%{responsables_filter}%")

        if servicio:
            where_clauses.append("i.servicio ILIKE %s")
            params_data.append(f"%{servicio}%")
            params_count.append(f"%{servicio}%")

        if tabla:
            try:
                where_clauses.append("i.tabla = %s")
                params_data.append(int(tabla))
                params_count.append(int(tabla))
            except (ValueError, TypeError):
                pass

        if viaje_linea:
            try:
                where_clauses.append("i.viaje_linea = %s")
                params_data.append(int(viaje_linea))
                params_count.append(int(viaje_linea))
            except (ValueError, TypeError):
                pass

        if id_viaje:
            try:
                where_clauses.append("i.id_viaje = %s")
                params_data.append(int(id_viaje))
                params_count.append(int(id_viaje))
            except (ValueError, TypeError):
                pass

        if sentido:
            where_clauses.append("i.sentido ILIKE %s")
            params_data.append(f"%{sentido}%")
            params_count.append(f"%{sentido}%")

        if hora_ini_teorica:
            where_clauses.append("i.hora_ini_teorica::text ILIKE %s")
            params_data.append(f"%{hora_ini_teorica}%")
            params_count.append(f"%{hora_ini_teorica}%")

        if hora_ini_teorica_min:
            where_clauses.append("i.hora_ini_teorica >= %s::time")
            params_data.append(hora_ini_teorica_min)
            params_count.append(hora_ini_teorica_min)

        if hora_ini_teorica_max:
            where_clauses.append("i.hora_ini_teorica <= %s::time")
            params_data.append(hora_ini_teorica_max)
            params_count.append(hora_ini_teorica_max)

        if motivo_original:
            where_clauses.append("i.motivo ILIKE %s")
            params_data.append(f"%{motivo_original}%")
            params_count.append(f"%{motivo_original}%")

        if id_concesion:
            where_clauses.append(f"({concesion_expr} ILIKE %s OR i.id_concesion::text ILIKE %s)")
            params_data.extend([f"%{id_concesion}%", f"%{id_concesion}%"])
            params_count.extend([f"%{id_concesion}%", f"%{id_concesion}%"])

        if fecha_carga:
            where_clauses.append("i.fecha_carga::text ILIKE %s")
            params_data.append(f"%{fecha_carga}%")
            params_count.append(f"%{fecha_carga}%")

        if componente and comp_expr != "NULL::text":
            where_clauses.append(f"{comp_expr} ILIKE %s")
            params_data.append(f"%{componente}%")
            params_count.append(f"%{componente}%")

        if revisor_asignado:
            where_clauses.append(f"{reviewer_display_expr} ILIKE %s")
            params_data.append(f"%{revisor_asignado}%")
            params_count.append(f"%{revisor_asignado}%")

        if usuario_asigna_nombre:
            where_clauses.append(f"{executor_name_expr} ILIKE %s")
            params_data.append(f"%{usuario_asigna_nombre}%")
            params_count.append(f"%{usuario_asigna_nombre}%")

        if km_ranges:
            for col, (lo, hi) in km_ranges.items():
                if col not in self._KM_COLS:
                    continue
                if lo is not None:
                    where_clauses.append(f"i.{col} >= %s")
                    params_data.append(float(lo))
                    params_count.append(float(lo))
                if hi is not None:
                    where_clauses.append(f"i.{col} <= %s")
                    params_data.append(float(hi))
                    params_count.append(float(hi))

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        sql_select = f"""
            SELECT
                i.id_ics,
                i.fecha,
                i.id_linea,
                {ruta_expr}              AS ruta_comercial,
                i.servicio,
                i.tabla,
                i.viaje_linea,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                i.hora_ini_teorica::text   AS hora_ini_teorica,
                i.km_prog_ad,
                i.conductor,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                i.motivo                   AS motivo_original,
                {concesion_expr}          AS id_concesion,
                i.fecha_carga,
                {comp_expr}               AS componente,
                gs.estado_asignacion,
                gs.usuario_asigna,
                gs.fecha_hora_asignacion,
                gs.id_ejecucion,
                gs.fecha_cierre_dp,
                {reviewer_user_id_expr}   AS revisor_user_id,
                {reviewer_display_expr}   AS revisor_asignado,
                {reviewer_origin_expr}    AS revisor_origen,
                {executor_name_expr}      AS usuario_asigna_nombre,
                string_agg(DISTINCT me.motivo,      ' | ')  AS motivos_eliminacion,
                string_agg(DISTINCT rs.responsable, ' | ')  AS responsables
            FROM sne.ics i
            {concesion_join}
            {joins_cop}
            LEFT JOIN sne.gestion_sne gs ON gs.id_ics = i.id_ics
            LEFT JOIN sne.ics_revisor_asignacion a ON a.id_ics = i.id_ics
            LEFT JOIN public.usuarios ur ON ur.id = NULLIF(gs.revisor, 0)
            LEFT JOIN public.usuarios ua ON ua.id = gs.usuario_asigna
            LEFT JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            LEFT JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
            LEFT JOIN sne.responsable_sne rs ON rs.id = imr.responsable
            {where_sql}
            GROUP BY
                i.id_ics, i.fecha, i.id_linea, i.servicio, i.tabla,
                i.viaje_linea, i.id_viaje, i.sentido, i.vehiculo_real,
                i.hora_ini_teorica, i.km_prog_ad, i.conductor,
                i.km_elim_eic, i.km_ejecutado, i.offset_inicio, i.offset_fin,
                i.km_revision, i.motivo, i.id_concesion, i.fecha_carga,
                gs.estado_asignacion, gs.usuario_asigna, gs.fecha_hora_asignacion, gs.id_ejecucion, gs.fecha_cierre_dp,
                {reviewer_user_id_expr}, {reviewer_display_expr}, {reviewer_origin_expr}, {executor_name_expr}
                {group_by_component}{group_by_concesion}{group_by_ruta}
            ORDER BY i.km_revision DESC NULLS LAST, i.fecha DESC, i.id_ics DESC
        """

        if include_summary:
            resumen = self._fetchall(sql_select, params_count)
            filas_resumen = []
            for row in (resumen or []):
                fila = {k: _json_safe_value(v) for k, v in dict(row).items()}
                filas_resumen.append(fila)
            self._apply_dp_state_rows(filas_resumen)

            total = len(filas_resumen)
            filas = filas_resumen[offset:offset + tamano]
        else:
            fast_filter_joins = [
                "LEFT JOIN sne.gestion_sne gs ON gs.id_ics = i.id_ics",
            ]
            if concesion_join:
                fast_filter_joins.insert(0, concesion_join)
            if revisor_asignado:
                fast_filter_joins.append("LEFT JOIN sne.ics_revisor_asignacion a ON a.id_ics = i.id_ics")
                fast_filter_joins.append("LEFT JOIN public.usuarios ur ON ur.id = NULLIF(gs.revisor, 0)")
            if usuario_asigna_nombre:
                fast_filter_joins.append("LEFT JOIN public.usuarios ua ON ua.id = gs.usuario_asigna")
            if ruta_comercial or (componente and comp_expr != "NULL::text"):
                fast_filter_joins.insert(0, joins_cop)
            fast_filter_joins_sql = "\n                ".join(fast_filter_joins)
            sql_count = f"""
                SELECT COUNT(DISTINCT i.id_ics) AS total
                FROM sne.ics i
                {fast_filter_joins_sql}
                {where_sql}
            """
            sql_page = f"""
                WITH page_ids AS (
                    SELECT
                        i.id_ics,
                        i.km_revision AS sort_km_revision,
                        i.fecha AS sort_fecha
                    FROM sne.ics i
                    {fast_filter_joins_sql}
                    {where_sql}
                    GROUP BY i.id_ics, i.km_revision, i.fecha
                    ORDER BY i.km_revision DESC NULLS LAST, i.fecha DESC, i.id_ics DESC
                    LIMIT %s OFFSET %s
                )
                SELECT
                    i.id_ics,
                    i.fecha,
                    i.id_linea,
                    {ruta_expr}              AS ruta_comercial,
                    i.servicio,
                    i.tabla,
                    i.viaje_linea,
                    i.id_viaje,
                    i.sentido,
                    i.vehiculo_real,
                    i.hora_ini_teorica::text   AS hora_ini_teorica,
                    i.km_prog_ad,
                    i.conductor,
                    i.km_elim_eic,
                    i.km_ejecutado,
                    i.offset_inicio,
                    i.offset_fin,
                    i.km_revision,
                    i.motivo                   AS motivo_original,
                    {concesion_expr}          AS id_concesion,
                    i.fecha_carga,
                    {comp_expr}               AS componente,
                    gs.estado_asignacion,
                    gs.usuario_asigna,
                    gs.fecha_hora_asignacion,
                    gs.id_ejecucion,
                    gs.fecha_cierre_dp,
                    {reviewer_user_id_expr}   AS revisor_user_id,
                    {reviewer_display_expr}   AS revisor_asignado,
                    {reviewer_origin_expr}    AS revisor_origen,
                    {executor_name_expr}      AS usuario_asigna_nombre,
                    string_agg(DISTINCT me.motivo,      ' | ')  AS motivos_eliminacion,
                    string_agg(DISTINCT rs.responsable, ' | ')  AS responsables
                FROM page_ids pid
                JOIN sne.ics i ON i.id_ics = pid.id_ics
                {concesion_join}
                {joins_cop}
                LEFT JOIN sne.gestion_sne gs ON gs.id_ics = i.id_ics
                LEFT JOIN sne.ics_revisor_asignacion a ON a.id_ics = i.id_ics
                LEFT JOIN public.usuarios ur ON ur.id = NULLIF(gs.revisor, 0)
                LEFT JOIN public.usuarios ua ON ua.id = gs.usuario_asigna
                LEFT JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
                LEFT JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
                LEFT JOIN sne.responsable_sne rs ON rs.id = imr.responsable
                GROUP BY
                    i.id_ics, i.fecha, i.id_linea, i.servicio, i.tabla,
                    i.viaje_linea, i.id_viaje, i.sentido, i.vehiculo_real,
                    i.hora_ini_teorica, i.km_prog_ad, i.conductor,
                    i.km_elim_eic, i.km_ejecutado, i.offset_inicio, i.offset_fin,
                    i.km_revision, i.motivo, i.id_concesion, i.fecha_carga,
                    gs.estado_asignacion, gs.usuario_asigna, gs.fecha_hora_asignacion, gs.id_ejecucion, gs.fecha_cierre_dp,
                    {reviewer_user_id_expr}, {reviewer_display_expr}, {reviewer_origin_expr}, {executor_name_expr},
                    pid.sort_km_revision, pid.sort_fecha
                    {group_by_component}{group_by_concesion}{group_by_ruta}
                ORDER BY pid.sort_km_revision DESC NULLS LAST, pid.sort_fecha DESC, i.id_ics DESC
            """
            conteo = self._fetchone(sql_count, params_count)
            datos = self._fetchall(sql_page, params_data + [tamano, offset])
            total = int(conteo["total"] or 0) if conteo else 0
            filas = []
            for row in (datos or []):
                fila = {k: _json_safe_value(v) for k, v in dict(row).items()}
                filas.append(fila)
            self._apply_dp_state_rows(filas)
            filas_resumen = None

        return {
            "data": filas,
            "resumen_rows": filas_resumen,
            "total": total,
            "pagina": pagina,
            "tamano": tamano,
            "paginas": math.ceil(total / tamano) if tamano > 0 else 0,
            "vista": vista_norm,
            "id_ejecucion": latest_execution_id,
            "execution_summary": self._build_execution_summary(latest_execution_id)
                if vista_norm == "ultima_ejecucion" and include_summary else None,
        }

    # ---------------------------
    # Fechas disponibles en ICS
    # ---------------------------
    def fechas_disponibles(self) -> list[str]:
        rows = self._fetchall(
            "SELECT DISTINCT fecha::text AS fecha FROM sne.ics ORDER BY fecha DESC"
        )
        return [r["fecha"] for r in rows] if rows else []

    def clasificar_fecha_operativa(self, fecha_operativa: Any) -> dict[str, Any]:
        fecha = _coerce_date(fecha_operativa) or now_bogota().date()
        festivo = self._fetchone(
            """
            SELECT nombre
            FROM config.festivos
            WHERE fecha = %s
              AND activo = TRUE
            LIMIT 1
            """,
            [fecha],
        )

        dia_semana_iso = fecha.isoweekday()
        nombres_dia = {
            1: "Lunes",
            2: "Martes",
            3: "Miercoles",
            4: "Jueves",
            5: "Viernes",
            6: "Sabado",
            7: "Domingo",
        }

        if festivo:
            tipo_dia = "festivo"
            etiqueta = "Festivo"
        elif dia_semana_iso == 7:
            tipo_dia = "domingo"
            etiqueta = "Domingo"
        elif dia_semana_iso == 6:
            tipo_dia = "sabado"
            etiqueta = "Sabado"
        else:
            tipo_dia = "habil"
            etiqueta = "Habil"

        return {
            "fecha": fecha.isoformat(),
            "tipo_dia": tipo_dia,
            "etiqueta": etiqueta,
            "dia_semana_iso": dia_semana_iso,
            "dia_semana": nombres_dia.get(dia_semana_iso, ""),
            "es_festivo": bool(festivo),
            "nombre_festivo": str(festivo["nombre"]) if festivo else "",
            "capacidad_base": CAPACIDAD_POR_TIPO_DIA[tipo_dia],
        }

    # ---------------------------
    # Configuración de revisores
    # ---------------------------
    def _ensure_reviewer_config_table(self) -> None:
        if self._reviewer_config_ready:
            return
        self._execute("CREATE SCHEMA IF NOT EXISTS sne;")
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sne.revisores_config (
                usuario_id       INTEGER PRIMARY KEY REFERENCES public.usuarios(id) ON DELETE CASCADE,
                orden            INTEGER      NOT NULL DEFAULT 1,
                rol_asignacion   VARCHAR(60)  NOT NULL DEFAULT 'Revisor',
                componente       VARCHAR(30)  NOT NULL DEFAULT 'General',
                visible          BOOLEAN      NOT NULL DEFAULT TRUE,
                capacidad_habil  INTEGER      NOT NULL DEFAULT 70,
                capacidad_sab    INTEGER      NOT NULL DEFAULT 35,
                capacidad_dom_fest INTEGER    NOT NULL DEFAULT 25,
                es_revisor       BOOLEAN      NOT NULL DEFAULT TRUE,
                creado_por       VARCHAR(100),
                actualizado_por  VARCHAR(100),
                created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            );
            """
        )
        self._execute(
            """
            ALTER TABLE sne.revisores_config
            ADD COLUMN IF NOT EXISTS visible BOOLEAN NOT NULL DEFAULT TRUE,
            ADD COLUMN IF NOT EXISTS capacidad_dom_fest INTEGER NOT NULL DEFAULT 25;
            """
        )
        self._execute(
            """
            ALTER TABLE sne.revisores_config
            ALTER COLUMN capacidad_habil SET DEFAULT 70,
            ALTER COLUMN capacidad_sab SET DEFAULT 35,
            ALTER COLUMN capacidad_dom_fest SET DEFAULT 25;
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_revisores_config_orden
                ON sne.revisores_config (es_revisor, orden);
            """
        )
        self._reviewer_config_ready = True

    def _ensure_manual_reviewer_assignment_table(self) -> None:
        if self._manual_assignment_ready:
            return
        self._execute("CREATE SCHEMA IF NOT EXISTS sne;")
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sne.ics_revisor_asignacion (
                id_ics            BIGINT PRIMARY KEY,
                revisor_user_id   INTEGER      NOT NULL REFERENCES public.usuarios(id) ON DELETE RESTRICT,
                revisor_nombre    VARCHAR(160) NOT NULL,
                revisor_username  VARCHAR(100),
                origen            VARCHAR(40)  NOT NULL DEFAULT 'tabla_menu',
                creado_por        VARCHAR(100),
                actualizado_por   VARCHAR(100),
                created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            );
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ics_revisor_asignacion_revisor
                ON sne.ics_revisor_asignacion (revisor_user_id, updated_at DESC);
            """
        )
        self._manual_assignment_ready = True

    def listar_config_revisores(self) -> list[dict[str, Any]]:
        self._ensure_reviewer_config_table()
        rows = self._fetchall(
            """
            SELECT
                u.id AS user_id,
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ', u.nombres::text, u.apellidos::text)), ''), u.username::text) AS nombre,
                COALESCE(u.nombres::text, '') AS nombres,
                COALESCE(u.apellidos::text, '') AS apellidos,
                COALESCE(u.username::text, '') AS username,
                u.rol AS rol_id,
                COALESCE(r.nombre_rol::text, '') AS rol_nombre,
                COALESCE(u.estado, 0) AS estado_usuario,
                COALESCE(rc.es_revisor, FALSE) AS es_revisor,
                COALESCE(rc.orden, 9999 + u.id) AS orden,
                COALESCE(NULLIF(rc.rol_asignacion, ''), 'Revisor') AS rol_asignacion,
                COALESCE(NULLIF(rc.componente, ''), 'General') AS componente,
                COALESCE(rc.visible, TRUE) AS visible,
                LEAST(GREATEST(COALESCE(rc.capacidad_habil, 70), 0), 70) AS capacidad_habil,
                LEAST(GREATEST(COALESCE(rc.capacidad_sab, 35), 0), 35) AS capacidad_sab,
                LEAST(GREATEST(COALESCE(rc.capacidad_dom_fest, 25), 0), 25) AS capacidad_dom_fest
            FROM public.usuarios u
            LEFT JOIN public.roles r
              ON r.id_rol = u.rol
            LEFT JOIN sne.revisores_config rc
              ON rc.usuario_id = u.id
            ORDER BY
                COALESCE(rc.es_revisor, FALSE) DESC,
                COALESCE(rc.orden, 9999 + u.id) ASC,
                COALESCE(u.estado, 0) DESC,
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ', u.nombres::text, u.apellidos::text)), ''), u.username::text) ASC,
                u.id ASC
            """
        )
        return [{k: _json_safe_value(v) for k, v in dict(row).items()} for row in (rows or [])]

    def guardar_config_revisores(
        self,
        revisores: list[dict[str, Any]],
        usuario_actualiza: str,
    ) -> list[dict[str, Any]]:
        self._ensure_reviewer_config_table()

        payload = revisores or []
        user_ids: list[int] = []
        prepared: list[tuple[int, int, str, str, bool, int, int, int, bool, str, str]] = []

        for index, reviewer in enumerate(payload, start=1):
            try:
                user_id = int(reviewer.get("user_id"))
            except (TypeError, ValueError):
                raise ValueError("Cada revisor debe tener un user_id valido")

            if user_id in user_ids:
                raise ValueError("No se puede repetir un usuario en revisores activos")

            rol_asignacion = str(reviewer.get("rol_asignacion") or "Revisor").strip()[:60] or "Revisor"
            componente = str(reviewer.get("componente") or "General").strip()[:30] or "General"
            visible = not (reviewer.get("visible") in (False, 0, "0", "false", "False"))
            try:
                capacidad_habil = min(CAPACIDAD_HABIL_DEFAULT, max(0, int(reviewer.get("capacidad_habil", CAPACIDAD_HABIL_DEFAULT))))
            except (TypeError, ValueError):
                capacidad_habil = CAPACIDAD_HABIL_DEFAULT
            try:
                capacidad_sab = min(CAPACIDAD_SABADO_DEFAULT, max(0, int(reviewer.get("capacidad_sab", CAPACIDAD_SABADO_DEFAULT))))
            except (TypeError, ValueError):
                capacidad_sab = CAPACIDAD_SABADO_DEFAULT
            try:
                capacidad_dom_fest = min(CAPACIDAD_DOM_FEST_DEFAULT, max(0, int(reviewer.get("capacidad_dom_fest", CAPACIDAD_DOM_FEST_DEFAULT))))
            except (TypeError, ValueError):
                capacidad_dom_fest = CAPACIDAD_DOM_FEST_DEFAULT
            try:
                orden = max(1, int(reviewer.get("orden", index)))
            except (TypeError, ValueError):
                orden = index

            user_ids.append(user_id)
            prepared.append((
                user_id,
                orden,
                rol_asignacion,
                componente,
                visible,
                capacidad_habil,
                capacidad_sab,
                capacidad_dom_fest,
                True,
                usuario_actualiza,
                usuario_actualiza,
            ))

        if user_ids:
            existing = self._fetchall(
                "SELECT id FROM public.usuarios WHERE id = ANY(%s)",
                [user_ids],
            )
            existing_ids = {int(row["id"]) for row in (existing or [])}
            missing = sorted(set(user_ids) - existing_ids)
            if missing:
                raise ValueError(f"Usuarios no encontrados: {', '.join(str(x) for x in missing)}")

        # Evita escrituras concurrentes que puedan pisar configuraciones.
        self._execute("SELECT pg_advisory_xact_lock(%s)", [REVISORES_CONFIG_LOCK_KEY])

        if prepared:
            with self.conn.cursor() as c:
                c.executemany(
                    """
                    INSERT INTO sne.revisores_config (
                        usuario_id,
                        orden,
                        rol_asignacion,
                        componente,
                        visible,
                        capacidad_habil,
                        capacidad_sab,
                        capacidad_dom_fest,
                        es_revisor,
                        creado_por,
                        actualizado_por
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (usuario_id) DO UPDATE
                    SET
                        orden = EXCLUDED.orden,
                        rol_asignacion = EXCLUDED.rol_asignacion,
                        componente = EXCLUDED.componente,
                        visible = EXCLUDED.visible,
                        capacidad_habil = EXCLUDED.capacidad_habil,
                        capacidad_sab = EXCLUDED.capacidad_sab,
                        capacidad_dom_fest = EXCLUDED.capacidad_dom_fest,
                        es_revisor = EXCLUDED.es_revisor,
                        actualizado_por = EXCLUDED.actualizado_por,
                        updated_at = NOW()
                    """,
                    prepared,
                )

        if user_ids:
            self._execute(
                """
                DELETE FROM sne.revisores_config
                WHERE NOT (usuario_id = ANY(%s))
                """,
                [user_ids],
            )
        else:
            self._execute("DELETE FROM sne.revisores_config")

        return self.listar_config_revisores()

    def asignar_revisor_manual(
        self,
        ids_ics: list[int | str],
        revisor_user_id: int,
        usuario_actualiza: str,
        origen: str = "tabla_menu",
    ) -> dict[str, Any]:
        self._ensure_reviewer_config_table()
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()

        normalizados = self._normalize_ids(ids_ics, "Debes enviar al menos un id_ics para asignar")

        try:
            revisor_user_id = int(revisor_user_id)
        except (TypeError, ValueError):
            raise ValueError("El revisor seleccionado no es valido")

        revisor = self._fetchone(
            """
            SELECT
                u.id AS user_id,
                COALESCE(NULLIF(TRIM(CONCAT_WS(' ', u.nombres::text, u.apellidos::text)), ''), u.username::text) AS nombre,
                COALESCE(u.username::text, '') AS username,
                COALESCE(NULLIF(rc.rol_asignacion, ''), 'Revisor') AS rol_asignacion,
                COALESCE(NULLIF(rc.componente, ''), 'General') AS componente
            FROM sne.revisores_config rc
            JOIN public.usuarios u
              ON u.id = rc.usuario_id
            WHERE rc.usuario_id = %s
              AND rc.es_revisor = TRUE
              AND COALESCE(u.estado, 0) = 1
            LIMIT 1
            """,
            [revisor_user_id],
        )
        if not revisor:
            raise ValueError("El usuario seleccionado no esta habilitado como revisor activo")

        joins, comp_expr = self._cop_joins_and_exprs()
        existentes = self._fetchall(
            f"""
            SELECT
                i.id_ics,
                {comp_expr} AS componente,
                gs.fecha_cierre_dp
            FROM sne.ics i
            LEFT JOIN sne.gestion_sne gs
              ON gs.id_ics = i.id_ics
            {joins}
            WHERE i.id_ics = ANY(%s)
            """,
            [normalizados],
        )
        existentes_ids = {int(row["id_ics"]) for row in (existentes or [])}
        faltantes = sorted(set(normalizados) - existentes_ids)
        if faltantes:
            raise ValueError(f"ICS no encontrados: {', '.join(str(x) for x in faltantes[:10])}")

        fecha_validacion_dp = now_bogota().date()
        ids_vencidos_dp: list[int] = []
        ids_sin_fecha_cierre_dp: list[int] = []
        elegibles: list[dict[str, Any]] = []
        for row in existentes or []:
            current_id = int(row["id_ics"])
            fecha_cierre_dp = _coerce_date(row.get("fecha_cierre_dp"))
            if fecha_cierre_dp is None:
                ids_sin_fecha_cierre_dp.append(current_id)
                continue
            if fecha_cierre_dp < fecha_validacion_dp:
                ids_vencidos_dp.append(current_id)
                continue
            elegibles.append(row)

        reviewer_component = _normalize_component_key(revisor.get("componente")) or "GENERAL"
        if reviewer_component in {"ZONAL", "ALIMENTACION"}:
            incompatible_counts: dict[str, int] = {}
            for row in elegibles:
                row_component = _normalize_component_key(row.get("componente"))
                if row_component == reviewer_component:
                    continue
                label = _component_label(row.get("componente"))
                incompatible_counts[label] = incompatible_counts.get(label, 0) + 1

            if incompatible_counts:
                detail = ", ".join(
                    f"{label}: {count}" for label, count in sorted(incompatible_counts.items())
                )
                reviewer_label = _component_label(reviewer_component)
                raise ValueError(
                    f"El revisor {revisor['nombre']} esta configurado como {reviewer_label} "
                    f"y solo puede recibir registros {reviewer_label}. Seleccion incompatible: {detail}."
                )

        payload = [
            (
                int(row["id_ics"]),
                revisor_user_id,
                str(revisor["nombre"])[:160],
                str(revisor["username"] or "")[:100] or None,
                str(origen or "tabla_menu")[:40],
                usuario_actualiza,
                usuario_actualiza,
            )
            for row in elegibles
        ]

        if payload:
            with self.conn.cursor() as c:
                c.executemany(
                    """
                    INSERT INTO sne.ics_revisor_asignacion (
                        id_ics,
                        revisor_user_id,
                        revisor_nombre,
                        revisor_username,
                        origen,
                        creado_por,
                        actualizado_por
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_ics) DO UPDATE
                    SET
                        revisor_user_id = EXCLUDED.revisor_user_id,
                        revisor_nombre = EXCLUDED.revisor_nombre,
                        revisor_username = EXCLUDED.revisor_username,
                        origen = EXCLUDED.origen,
                        actualizado_por = EXCLUDED.actualizado_por,
                        updated_at = NOW()
                    """,
                    payload,
                )

        return {
            "ids_ics": [int(row["id_ics"]) for row in elegibles],
            "solicitados": len(normalizados),
            "preasignados": len(elegibles),
            "omitidos": len(ids_vencidos_dp) + len(ids_sin_fecha_cierre_dp),
            "ids_vencidos_dp": ids_vencidos_dp,
            "ids_sin_fecha_cierre_dp": ids_sin_fecha_cierre_dp,
            "revisor": {
                "user_id": int(revisor["user_id"]),
                "nombre": _json_safe_value(revisor["nombre"]),
                "username": _json_safe_value(revisor["username"]),
                "rol_asignacion": _json_safe_value(revisor["rol_asignacion"]),
                "componente": _json_safe_value(revisor["componente"]),
            },
            "origen": str(origen or "tabla_menu")[:40],
        }

    def desasignar_revisor_manual(
        self,
        ids_ics: list[int | str],
        usuario_actualiza: str,
        origen: str = "tabla_menu_clear",
    ) -> dict[str, Any]:
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()

        normalizados = self._normalize_ids(ids_ics, "Debes enviar al menos un id_ics para desasignar")

        with self.conn.cursor() as c:
            c.execute(
                """
                DELETE FROM sne.ics_revisor_asignacion
                WHERE id_ics = ANY(%s)
                RETURNING id_ics
                """,
                [normalizados],
            )
            eliminados = c.fetchall() or []

        ids_eliminados = [
            int(row["id_ics"])
            for row in eliminados
            if row and row.get("id_ics") is not None
        ]

        return {
            "ids_ics": ids_eliminados,
            "usuario_actualiza": str(usuario_actualiza or "sistema")[:100],
            "origen": str(origen or "tabla_menu_clear")[:40],
        }

    def ejecutar_asignacion(
        self,
        ids_ics: list[int | str],
        usuario_asigna_id: int,
        usuario_actualiza: str,
    ) -> dict[str, Any]:
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()

        normalizados = self._normalize_ids(ids_ics, "Debes enviar al menos un id_ics para ejecutar")

        try:
            usuario_asigna_id = int(usuario_asigna_id)
        except (TypeError, ValueError):
            raise ValueError("No fue posible identificar al usuario que ejecuta la asignacion")

        fecha_ejecucion = now_bogota()
        fecha_ejecucion_date = fecha_ejecucion.date()
        id_ejecucion = f"SNE-{uuid.uuid4().hex[:12].upper()}"
        joins_cop, comp_expr = self._cop_joins_and_exprs()
        component_key_expr = (
            "UPPER(TRIM(COALESCE("
            + (comp_expr if comp_expr != "NULL::text" else "''::text")
            + ", '')))"
        )

        with self.conn.cursor() as c:
            c.execute(
                """
                INSERT INTO sne.gestion_sne (
                    id_ics,
                    revisor,
                    estado_asignacion,
                    estado_objecion,
                    estado_transmitools
                )
                SELECT
                    i.id_ics,
                    0,
                    0,
                    0,
                    0
                FROM sne.ics i
                WHERE i.id_ics = ANY(%s)
                ON CONFLICT (id_ics) DO NOTHING
                """,
                [normalizados],
            )
            c.execute(
                f"""
                SELECT
                    gs.id_ics,
                    CASE
                        WHEN gs.fecha_cierre_dp IS NULL THEN 'SIN_FECHA_CIERRE_DP'
                        ELSE 'VENCIDO_DP'
                    END AS bloqueo
                FROM sne.gestion_sne gs
                JOIN sne.ics_revisor_asignacion a
                  ON a.id_ics = gs.id_ics
                JOIN sne.ics i
                  ON i.id_ics = gs.id_ics
                JOIN sne.revisores_config rc
                  ON rc.usuario_id = a.revisor_user_id
                 AND rc.es_revisor = TRUE
                JOIN public.usuarios u
                  ON u.id = a.revisor_user_id
                 AND COALESCE(u.estado, 0) = 1
                {joins_cop}
                WHERE gs.id_ics = ANY(%s)
                  AND COALESCE(gs.estado_asignacion, 0) IN (0, 2)
                  AND COALESCE(a.revisor_user_id, 0) > 0
                  AND (
                      UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'GENERAL'
                      OR (
                          UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'ZONAL'
                          AND {component_key_expr} = 'ZONAL'
                      )
                      OR (
                          UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'ALIMENTACION'
                          AND {component_key_expr} = 'ALIMENTACION'
                      )
                  )
                  AND (
                      gs.fecha_cierre_dp IS NULL
                      OR gs.fecha_cierre_dp::date < %s
                  )
                ORDER BY gs.id_ics
                """,
                [normalizados, fecha_ejecucion_date],
            )
            bloqueados_dp_rows = c.fetchall() or []
            c.execute(
                f"""
                WITH candidatos AS (
                    SELECT
                        gs.id_ics,
                        a.revisor_user_id
                    FROM sne.gestion_sne gs
                    JOIN sne.ics_revisor_asignacion a
                      ON a.id_ics = gs.id_ics
                    JOIN sne.ics i
                      ON i.id_ics = gs.id_ics
                    JOIN sne.revisores_config rc
                      ON rc.usuario_id = a.revisor_user_id
                     AND rc.es_revisor = TRUE
                    JOIN public.usuarios u
                      ON u.id = a.revisor_user_id
                     AND COALESCE(u.estado, 0) = 1
                    {joins_cop}
                    WHERE gs.id_ics = ANY(%s)
                      AND COALESCE(gs.estado_asignacion, 0) = 0
                      AND COALESCE(a.revisor_user_id, 0) > 0
                      AND gs.fecha_cierre_dp IS NOT NULL
                      AND gs.fecha_cierre_dp::date >= %s
                      AND (
                          UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'GENERAL'
                          OR (
                              UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'ZONAL'
                              AND {component_key_expr} = 'ZONAL'
                          )
                          OR (
                              UPPER(TRIM(COALESCE(rc.componente, 'GENERAL'))) = 'ALIMENTACION'
                              AND {component_key_expr} = 'ALIMENTACION'
                          )
                      )
                ),
                updated AS (
                    UPDATE sne.gestion_sne gs
                    SET
                        revisor = candidatos.revisor_user_id,
                        estado_asignacion = 1,
                        usuario_asigna = %s,
                        fecha_hora_asignacion = %s,
                        id_ejecucion = %s,
                        updated_at = NOW()
                    FROM candidatos
                    WHERE gs.id_ics = candidatos.id_ics
                    RETURNING gs.id_ics
                )
                SELECT id_ics
                FROM updated
                ORDER BY id_ics
                """,
                [normalizados, fecha_ejecucion_date, usuario_asigna_id, fecha_ejecucion, id_ejecucion],
            )
            ejecutados = c.fetchall() or []

        ids_ejecutados = [
            int(row["id_ics"])
            for row in ejecutados
            if row and row.get("id_ics") is not None
        ]
        ids_vencidos_dp = [
            int(row["id_ics"])
            for row in bloqueados_dp_rows
            if row and row.get("bloqueo") == "VENCIDO_DP" and row.get("id_ics") is not None
        ]
        ids_sin_fecha_cierre_dp = [
            int(row["id_ics"])
            for row in bloqueados_dp_rows
            if row and row.get("bloqueo") == "SIN_FECHA_CIERRE_DP" and row.get("id_ics") is not None
        ]

        if ids_ejecutados:
            self._execute(
                """
                DELETE FROM sne.ics_revisor_asignacion
                WHERE id_ics = ANY(%s)
                """,
                [ids_ejecutados],
            )

        return {
            "id_ejecucion": id_ejecucion if ids_ejecutados else None,
            "ids_ics": ids_ejecutados,
            "solicitados": len(normalizados),
            "ejecutados": len(ids_ejecutados),
            "bloqueados_dp": len(ids_vencidos_dp) + len(ids_sin_fecha_cierre_dp),
            "ids_vencidos_dp": ids_vencidos_dp,
            "ids_sin_fecha_cierre_dp": ids_sin_fecha_cierre_dp,
            "omitidos": max(0, len(normalizados) - len(ids_ejecutados)),
            "usuario_asigna": usuario_asigna_id,
            "usuario_actualiza": str(usuario_actualiza or "sistema")[:100],
            "fecha_hora_asignacion": _json_safe_value(fecha_ejecucion) if ids_ejecutados else None,
            "resumen": self._build_execution_summary(id_ejecucion) if ids_ejecutados else None,
        }

    def revertir_asignacion(
        self,
        ids_ics: list[int | str],
        usuario_revierte_id: int,
        usuario_revierte: str,
        motivo: str = "",
        ip_origen: Optional[str] = None,
    ) -> dict[str, Any]:
        self._ensure_gestion_sne_table()
        self._ensure_manual_reviewer_assignment_table()
        self._ensure_reversion_history_table()

        normalizados = self._normalize_ids(ids_ics, "Debes enviar al menos un id_ics para revertir")

        try:
            usuario_revierte_id = int(usuario_revierte_id)
        except (TypeError, ValueError):
            raise ValueError("No fue posible identificar al usuario que revierte la asignacion")

        usuario_revierte = str(usuario_revierte or "sistema")[:100]
        motivo = _validar_motivo_reversion(motivo)
        ip_origen = str(ip_origen or "")[:45] or None
        fecha_reversion = now_bogota()
        fecha_reversion_date = fecha_reversion.date()

        resultados: dict[str, list[int]] = {
            "ids_revertidos": [],
            "ids_vencidos_dp": [],
            "ids_sin_fecha_cierre_dp": [],
            "ids_gestion_posterior": [],
            "ids_objecion_en_proceso": [],
            "ids_objetados": [],
            "ids_no_asignados": [],
            "ids_no_encontrados": [],
        }

        with self.conn.cursor() as c:
            c.execute(
                """
                SELECT
                    gs.id_ics,
                    gs.revisor,
                    gs.estado_asignacion,
                    gs.usuario_asigna,
                    gs.fecha_hora_asignacion,
                    gs.id_ejecucion,
                    gs.estado_objecion,
                    gs.usuario_objeta,
                    gs.fecha_hora_objecion,
                    gs.estado_transmitools,
                    gs.fecha_hora_transmitools,
                    gs.fecha_cierre_dp
                FROM sne.gestion_sne gs
                WHERE gs.id_ics = ANY(%s)
                ORDER BY gs.id_ics
                FOR UPDATE
                """,
                [normalizados],
            )
            rows = c.fetchall() or []
            rows_by_id = {int(row["id_ics"]): row for row in rows if row and row.get("id_ics") is not None}

            for id_ics in normalizados:
                row = rows_by_id.get(int(id_ics))
                resultado = "REVERTIDO"
                detalle = None

                if not row:
                    resultado = "NO_ENCONTRADO"
                    detalle = "No existe gestion_sne para el registro."
                    resultados["ids_no_encontrados"].append(int(id_ics))
                else:
                    estado_asignacion = int(row.get("estado_asignacion") or 0)
                    revisor_actual = int(row.get("revisor") or 0)
                    fecha_asignacion = row.get("fecha_hora_asignacion")
                    fecha_cierre_dp = _coerce_date(row.get("fecha_cierre_dp"))
                    try:
                        estado_objecion = int(row.get("estado_objecion") or 0)
                    except (TypeError, ValueError):
                        estado_objecion = 0

                    tiene_asignacion_activa = revisor_actual > 0 and fecha_asignacion is not None
                    if not tiene_asignacion_activa:
                        resultado = "NO_ASIGNADO"
                        detalle = "El registro no tiene una asignacion ejecutada activa."
                        resultados["ids_no_asignados"].append(int(id_ics))
                    elif fecha_cierre_dp is None:
                        resultado = "SIN_FECHA_CIERRE_DP"
                        detalle = "No se puede validar la vigencia DP porque no hay fecha_cierre_dp."
                        resultados["ids_sin_fecha_cierre_dp"].append(int(id_ics))
                    elif estado_asignacion == 2 or fecha_reversion_date > fecha_cierre_dp:
                        resultado = "VENCIDO_DP"
                        detalle = "La fecha del sistema supera la fecha_cierre_dp."
                        resultados["ids_vencidos_dp"].append(int(id_ics))
                    elif estado_asignacion != 1:
                        resultado = "NO_ASIGNADO"
                        detalle = "El registro no tiene una asignacion ejecutada activa."
                        resultados["ids_no_asignados"].append(int(id_ics))
                    elif estado_objecion == 1:
                        resultado = "GESTION_POSTERIOR"
                        detalle = "No se puede revertir porque el registro ya esta en proceso de objecion (estado_objecion = 1)."
                        resultados["ids_gestion_posterior"].append(int(id_ics))
                        resultados["ids_objecion_en_proceso"].append(int(id_ics))
                    elif estado_objecion == 2:
                        resultado = "GESTION_POSTERIOR"
                        detalle = "No se puede revertir porque el registro ya esta objetado (estado_objecion = 2)."
                        resultados["ids_gestion_posterior"].append(int(id_ics))
                        resultados["ids_objetados"].append(int(id_ics))
                    else:
                        resultados["ids_revertidos"].append(int(id_ics))

                c.execute(
                    """
                    INSERT INTO sne.historial_reversion_asignacion (
                        id_ics,
                        id_ejecucion,
                        revisor_anterior,
                        usuario_asigna_anterior,
                        fecha_hora_asignacion_anterior,
                        estado_asignacion_anterior,
                        estado_objecion_anterior,
                        estado_transmitools_anterior,
                        fecha_cierre_dp,
                        usuario_revierte_id,
                        usuario_revierte,
                        motivo,
                        resultado,
                        detalle,
                        ip_origen
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    [
                        int(id_ics),
                        row.get("id_ejecucion") if row else None,
                        row.get("revisor") if row else None,
                        row.get("usuario_asigna") if row else None,
                        row.get("fecha_hora_asignacion") if row else None,
                        row.get("estado_asignacion") if row else None,
                        row.get("estado_objecion") if row else None,
                        row.get("estado_transmitools") if row else None,
                        row.get("fecha_cierre_dp") if row else None,
                        usuario_revierte_id,
                        usuario_revierte,
                        motivo or None,
                        resultado,
                        detalle,
                        ip_origen,
                    ],
                )

            ids_revertidos = resultados["ids_revertidos"]
            if ids_revertidos:
                c.execute(
                    """
                    INSERT INTO sne.ics_revisor_asignacion (
                        id_ics,
                        revisor_user_id,
                        revisor_nombre,
                        revisor_username,
                        origen,
                        creado_por,
                        actualizado_por,
                        created_at,
                        updated_at
                    )
                    SELECT
                        gs.id_ics,
                        gs.revisor,
                        COALESCE(
                            NULLIF(TRIM(CONCAT_WS(' ', u.nombres::text, u.apellidos::text)), ''),
                            NULLIF(u.username::text, ''),
                            'Revisor'
                        ) AS revisor_nombre,
                        NULLIF(u.username::text, '') AS revisor_username,
                        'reversion_ejecucion' AS origen,
                        %s AS creado_por,
                        %s AS actualizado_por,
                        NOW() AS created_at,
                        NOW() AS updated_at
                    FROM sne.gestion_sne gs
                    JOIN public.usuarios u
                      ON u.id = gs.revisor
                    WHERE gs.id_ics = ANY(%s)
                      AND COALESCE(gs.revisor, 0) > 0
                    ON CONFLICT (id_ics) DO UPDATE
                    SET
                        revisor_user_id = EXCLUDED.revisor_user_id,
                        revisor_nombre = EXCLUDED.revisor_nombre,
                        revisor_username = EXCLUDED.revisor_username,
                        origen = EXCLUDED.origen,
                        actualizado_por = EXCLUDED.actualizado_por,
                        updated_at = NOW()
                    """,
                    [usuario_revierte, usuario_revierte, ids_revertidos],
                )
                c.execute(
                    """
                    UPDATE sne.gestion_sne
                    SET
                        revisor = 0,
                        estado_asignacion = 0,
                        usuario_asigna = NULL,
                        fecha_hora_asignacion = NULL,
                        id_ejecucion = NULL,
                        updated_at = NOW()
                    WHERE id_ics = ANY(%s)
                    """,
                    [ids_revertidos],
                )

        bloqueados_dp = len(resultados["ids_vencidos_dp"]) + len(resultados["ids_sin_fecha_cierre_dp"])
        bloqueados_gestion = len(resultados["ids_gestion_posterior"])
        omitidos = (
            bloqueados_dp
            + bloqueados_gestion
            + len(resultados["ids_no_asignados"])
            + len(resultados["ids_no_encontrados"])
        )

        return {
            "solicitados": len(normalizados),
            "revertidos": len(resultados["ids_revertidos"]),
            "bloqueados_dp": bloqueados_dp,
            "bloqueados_gestion": bloqueados_gestion,
            "omitidos": omitidos,
            "fecha_hora_reversion": _json_safe_value(fecha_reversion),
            "usuario_revierte": usuario_revierte,
            **resultados,
        }
