import io, csv
from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from database.database_manager import get_db_connection

try:
    import openpyxl  # Para leer .xlsx
except Exception:
    openpyxl = None

TZ_BOGOTA = ZoneInfo("America/Bogota")

def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)

class GestionEstadoBuses:
    """
    Gestión de estado operativo de la flota por rango de fechas (ventana tipo Gantt).

    Tablas principales:
      - config.buses_cexp        → flota base
      - config.cop               → centros de operación (id_componente, id_zona)
      - config.componente        → componentes
      - config.zona              → zonas
      - config.km_recorrido_bus  → posicionamiento GPS (movil_bus, fecha, dist_final_km)
      - config.km_fms_bus        → km ejecutado FMS Comercial (vehiculo_real, fecha, km_ejecutado)

    Posicionamiento y FMS Comercial se agregan (SUM) sobre el rango [fecha_inicio, fecha_fin]
    solicitado y se entregan en 0 (no NULL) cuando el bus no tiene registros en el rango.

    El Gantt de estados (operativo/taller/inactivo/etc.) queda con el origen de datos y el
    catálogo de estados pendientes por definir; `_obtener_segmentos_estado` es el punto de
    extensión para cuando esa fuente se confirme.
    """

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        # Cache de _col_exists para evitar queries repetidas a information_schema
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

    # =========================================================
    # UTILIDADES
    # =========================================================

    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _fetchall(self, sql: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        self.cursor.execute(sql, params or [])
        return self.cursor.fetchall()

    def _fetchone(self, sql: str, params: Optional[list] = None) -> Optional[Dict[str, Any]]:
        self.cursor.execute(sql, params or [])
        return self.cursor.fetchone()

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
            INNER JOIN config.cop c        ON c.id_componente = comp.id
            INNER JOIN config.buses_cexp b ON b.id_cop        = c.id
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
    # FLOTA POR RANGO DE FECHAS (ventana Gantt)
    # =========================================================

    def flota_rango(
        self,
        fecha_inicio: date,
        fecha_fin: date,
        pagina: int = 1,
        tamano: int = 5000,
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
        Retorna la flota con Posicionamiento y FMS Comercial acumulados (SUM) en el
        rango [fecha_inicio, fecha_fin]. Buses sin registros en el rango quedan en 0.
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
            WITH pos_rango AS (
                -- Km recorridos por móvil según posicionamiento GPS, sumados en el rango
                SELECT
                    movil_bus,
                    SUM(dist_final_km) AS km_posicionamiento
                FROM config.km_recorrido_bus
                WHERE fecha BETWEEN %s AND %s
                GROUP BY movil_bus
            ),
            fms_rango AS (
                -- Km ejecutados según FMS Comercial, sumados en el rango
                SELECT
                    vehiculo_real,
                    SUM(km_ejecutado) AS km_fms_comercial
                FROM config.km_fms_bus
                WHERE fecha BETWEEN %s AND %s
                GROUP BY vehiculo_real
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
                COALESCE(p.km_posicionamiento, 0)  AS posicionamiento,
                COALESCE(fk.km_fms_comercial, 0)   AS fms_comercial
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id           = b.id_cop
            LEFT JOIN config.componente comp ON comp.id        = c.id_componente
            LEFT JOIN config.zona       z    ON z.id           = c.id_zona
            LEFT JOIN pos_rango         p    ON p.movil_bus    = b.no_interno
            LEFT JOIN fms_rango         fk   ON fk.vehiculo_real = b.no_interno
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
            [fecha_inicio, fecha_fin, fecha_inicio, fecha_fin] + filter_params + pag_params,
        )

        for row in data:
            row["gantt"] = self._obtener_segmentos_estado(row["id"], fecha_inicio, fecha_fin)

        return data, total

    # =========================================================
    # GANTT DE ESTADOS (pendiente por definir con el usuario)
    # =========================================================

    def _obtener_segmentos_estado(
        self,
        id_bus: int,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> List[Dict]:
        """
        Punto de extensión para los tramos de estado (operativo/taller/inactivo/etc.)
        que alimentan el Gantt. La tabla origen y el catálogo de estados están
        pendientes por confirmar; mientras tanto retorna [] y el frontend muestra
        la línea de tiempo como placeholder.
        """
        return []
