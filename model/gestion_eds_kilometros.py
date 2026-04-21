import os
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple

from database.database_manager import _get_pool as get_db_pool

load_dotenv()
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")


def now_bogota() -> datetime:
    return datetime.now(TIMEZONE_BOGOTA)


class Gestion_kilometros:
    """
    Gestión de kilómetros recorridos por bus (diario).

    Tablas principales:
      - config.buses_cexp         → flota base
      - config.cop                → centros de operación (id_componente, id_zona)
      - config.componente         → componentes
      - config.zona               → zonas
      - config.km_recorrido_bus   → posicionamiento GPS (movil_bus, fecha, odometro)
      - eds.eds_registro          → registros EDS (id_bus, fecha_hora_registro,
                                     odometro_funcional bool, odometro numeric)
      - eds.km_diario             → km final editable (requiere creación previa):
            CREATE TABLE IF NOT EXISTS eds.km_diario (
                id                 BIGSERIAL PRIMARY KEY,
                id_bus             BIGINT      NOT NULL,
                fecha              DATE        NOT NULL,
                km_recorrido_final NUMERIC(12,2),
                updated_at         TIMESTAMPTZ DEFAULT NOW(),
                updated_by         TEXT,
                UNIQUE (id_bus, fecha)
            );
    """

    def __init__(self):
        self.conn = get_db_pool().getconn()
        if not self.conn.closed:
            self.conn.rollback()
        self.conn.cursor_factory = RealDictCursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            get_db_pool().putconn(self.conn)
            self.conn = None

    def close(self):
        if getattr(self, "conn", None):
            try:
                if not self.conn.closed:
                    self.conn.rollback()
                get_db_pool().putconn(self.conn)
            except Exception:
                pass
            self.conn = None

    def __del__(self):
        try:
            if getattr(self, "conn", None) and not self.conn.closed:
                self.close()
        except Exception:
            pass

    def _execute(self, sql: str, params: list = None) -> None:
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
        self.conn.commit()

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

    # =========================================================
    # FILTROS
    # =========================================================

    def filtros_tipologia(self) -> List[Dict]:
        sql = """
            SELECT DISTINCT tipologia
            FROM config.buses_cexp
            WHERE tipologia IS NOT NULL
            ORDER BY tipologia ASC;
        """
        return self._fetchall(sql, [])

    def filtros_combustible(self) -> List[Dict]:
        sql = """
            SELECT DISTINCT combustible
            FROM config.buses_cexp
            WHERE combustible IS NOT NULL
            ORDER BY combustible ASC;
        """
        return self._fetchall(sql, [])

    def filtros_componente(self) -> List[Dict]:
        """Componentes que tienen al menos un bus asignado."""
        sql = """
            SELECT DISTINCT comp.id, comp.componente
            FROM config.componente comp
            INNER JOIN config.cop c     ON c.id_componente = comp.id
            INNER JOIN config.buses_cexp b ON b.id_cop = c.id
            WHERE comp.estado = 1
            ORDER BY comp.componente ASC;
        """
        return self._fetchall(sql, [])

    def filtros_zona(self, id_componente: Optional[int] = None) -> List[Dict]:
        """Zonas activas; si id_componente, sólo las zonas de ese componente."""
        sql = """
            SELECT DISTINCT z.id, z.zona
            FROM config.zona z
            INNER JOIN config.cop c        ON c.id_zona = z.id
            INNER JOIN config.buses_cexp b ON b.id_cop  = c.id
            WHERE z.estado = 1
              AND (%s::bigint IS NULL OR c.id_componente = %s)
            ORDER BY z.zona ASC;
        """
        return self._fetchall(sql, [id_componente, id_componente])

    def filtros_cop(
        self,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
    ) -> List[Dict]:
        """COPs activos con buses; filtrables por componente y/o zona."""
        sql = """
            SELECT DISTINCT c.id, c.cop
            FROM config.cop c
            INNER JOIN config.buses_cexp b ON b.id_cop = c.id
            WHERE c.estado = 1
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
            ORDER BY c.cop ASC;
        """
        return self._fetchall(sql, [id_componente, id_componente, id_zona, id_zona])

    # =========================================================
    # FLOTA DÍA
    # =========================================================

    def flota_dia(
        self,
        fecha: date,
        pagina: int = 1,
        tamano: int = 200,
        placa: Optional[str] = None,
        no_interno: Optional[str] = None,
        tipologia: Optional[str] = None,
        combustible: Optional[str] = None,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        id_cop: Optional[int] = None,
        estado: Optional[int] = None,
    ) -> Tuple[List[Dict], int]:
        """
        Retorna la flota del día con km de cada fuente disponible.
        Columnas vacías (speed, gas_data, fms_comercial, vacio_planeado,
        km_acumulado, km_recorrido_calculado) se entregan como NULL para
        que el frontend las muestre en blanco.
        km_recorrido_final se trae de eds.km_diario si la tabla existe.
        """

        filter_params = [
            placa, placa,
            no_interno, no_interno,
            tipologia, tipologia,
            combustible, combustible,
            id_componente, id_componente,
            id_zona, id_zona,
            id_cop, id_cop,
            estado, estado,
        ]

        where = """
            WHERE 1=1
              AND (%s::text   IS NULL OR b.placa        ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.no_interno   ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.tipologia    = %s)
              AND (%s::text   IS NULL OR b.combustible  = %s)
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
              AND (%s::bigint IS NULL OR b.id_cop        = %s)
              AND (%s::int    IS NULL OR b.estado        = %s)
        """

        # ── COUNT ────────────────────────────────────────────
        sql_count = f"""
            SELECT COUNT(*)::int AS total
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id           = b.id_cop
            LEFT JOIN config.componente comp ON comp.id        = c.id_componente
            LEFT JOIN config.zona       z    ON z.id           = c.id_zona
            {where};
        """
        total = (self._fetchone(sql_count, filter_params) or {}).get("total", 0)

        # ── DATA ─────────────────────────────────────────────
        pag_sql, pag_params = self._paginacion(pagina, tamano)

        sql = f"""
            WITH eds_ultimo AS (
                -- Último registro EDS por bus para el día solicitado
                SELECT DISTINCT ON (id_bus)
                    id_bus,
                    odometro_funcional
                FROM eds.eds_registro
                WHERE DATE(fecha_hora_registro) = %s
                ORDER BY id_bus, fecha_hora_registro DESC
            ),
            eds_actual_max AS (
                -- Máximo odómetro del día consultado
                SELECT
                    id_bus,
                    MAX(odometro) AS odometro_actual
                FROM eds.eds_registro
                WHERE DATE(fecha_hora_registro) = %s
                GROUP BY id_bus
            ),
            eds_prev_fecha AS (
                -- Último día anterior con registro disponible
                SELECT
                    id_bus,
                    MAX(DATE(fecha_hora_registro)) AS fecha_previa
                FROM eds.eds_registro
                WHERE DATE(fecha_hora_registro) < %s
                GROUP BY id_bus
            ),
            eds_prev_max AS (
                -- Máximo odómetro del último día previo con datos
                SELECT
                    r.id_bus,
                    MAX(r.odometro) AS odometro_previo
                FROM eds.eds_registro r
                INNER JOIN eds_prev_fecha p
                    ON p.id_bus = r.id_bus
                   AND DATE(r.fecha_hora_registro) = p.fecha_previa
                GROUP BY r.id_bus
            ),
            pos_ultimo AS (
                -- Km recorridos por móvil según posicionamiento GPS
                SELECT DISTINCT ON (movil_bus)
                    movil_bus,
                    dist_final_km AS km_posicionamiento
                FROM config.km_recorrido_bus
                WHERE fecha = %s
                ORDER BY movil_bus
            )
            SELECT
                b.id,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.combustible,
                b.estado,
                c.id            AS id_cop,
                c.cop,
                c.id_componente,
                comp.componente,
                c.id_zona,
                z.zona,
                -- Odómetro funcional: booleano → 'SI' / 'NO'
                CASE
                    WHEN e.odometro_funcional IS TRUE  THEN 'SI'
                    WHEN e.odometro_funcional IS FALSE THEN 'NO'
                    ELSE NULL
                END                         AS odometro,
                NULL::numeric               AS km_acumulado,
                p.km_posicionamiento        AS posicionamiento,
                NULL::numeric               AS speed,
                NULL::numeric               AS gas_data,
                CASE
                    WHEN ea.odometro_actual IS NOT NULL AND ep.odometro_previo IS NOT NULL
                        THEN GREATEST(ea.odometro_actual - ep.odometro_previo, 0)
                    ELSE NULL
                END                         AS gestion_express,
                NULL::numeric               AS fms_comercial,
                NULL::numeric               AS vacio_planeado,
                NULL::numeric               AS km_recorrido_calculado,
                NULL::numeric               AS km_recorrido_final
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id           = b.id_cop
            LEFT JOIN config.componente comp ON comp.id        = c.id_componente
            LEFT JOIN config.zona       z    ON z.id           = c.id_zona
            LEFT JOIN eds_ultimo        e    ON e.id_bus       = b.id
            LEFT JOIN eds_actual_max    ea   ON ea.id_bus      = b.id
            LEFT JOIN eds_prev_max      ep   ON ep.id_bus      = b.id
            LEFT JOIN pos_ultimo        p    ON p.movil_bus    = b.no_interno
            {where}
            ORDER BY
                CASE
                    WHEN COALESCE(b.no_interno, '') ~ '^[0-9]+$' THEN 0
                    ELSE 1
                END,
                LENGTH(COALESCE(b.no_interno, '')),
                COALESCE(b.no_interno, '') ASC
        """

        data = self._fetchall(
            sql + pag_sql,
            [fecha, fecha, fecha, fecha] + filter_params + pag_params,
        )
        return data, total

    # =========================================================
    # ESTADÍSTICAS DEL DÍA
    # =========================================================

    def estadisticas_dia(self, fecha: date) -> Dict:
        sql = """
            WITH eds_ultimo AS (
                SELECT DISTINCT ON (id_bus)
                    id_bus,
                    odometro_funcional,
                    odometro
                FROM eds.eds_registro
                WHERE DATE(fecha_hora_registro) = %s
                ORDER BY id_bus, fecha_hora_registro DESC
            ),
            pos_ultimo AS (
                SELECT DISTINCT ON (movil_bus)
                    movil_bus,
                    dist_final_km
                FROM config.km_recorrido_bus
                WHERE fecha = %s
                ORDER BY movil_bus
            )
            SELECT
                COUNT(DISTINCT b.id)::int                                                  AS total_buses,
                COUNT(DISTINCT e.id_bus)::int                                              AS buses_con_eds,
                COUNT(DISTINCT CASE WHEN e.odometro_funcional IS TRUE THEN e.id_bus END)::int
                                                                                           AS odometro_si,
                COUNT(DISTINCT p.movil_bus)::int                                           AS buses_con_posicionamiento,
                COUNT(DISTINCT CASE WHEN e.odometro IS NOT NULL THEN e.id_bus END)::int   AS buses_gestion_express
            FROM config.buses_cexp b
            LEFT JOIN config.cop c ON c.id = b.id_cop
            LEFT JOIN eds_ultimo  e ON e.id_bus    = b.id
            LEFT JOIN pos_ultimo  p ON p.movil_bus = b.no_interno;
        """
        return dict(self._fetchone(sql, [fecha, fecha]) or {})

    # =========================================================
    # KM RECORRIDO FINAL  (UPSERT → eds.km_diario)
    # =========================================================

    def actualizar_km_final(
        self,
        id_bus: int,
        fecha: date,
        km_final: Optional[float],
        usuario: str,
    ) -> Dict:
        """
        Guarda o actualiza km_recorrido_final.
        Requiere que exista la tabla eds.km_diario (ver docstring de clase).
        """
        sql = """
            INSERT INTO eds.km_diario
                (id_bus, fecha, km_recorrido_final, updated_at, updated_by)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT (id_bus, fecha)
            DO UPDATE SET
                km_recorrido_final = EXCLUDED.km_recorrido_final,
                updated_at         = NOW(),
                updated_by         = EXCLUDED.updated_by
            RETURNING id, id_bus, fecha,
                      km_recorrido_final,
                      to_char(updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at,
                      updated_by;
        """
        return dict(self._fetchone(sql, [id_bus, fecha, km_final, usuario]) or {})
