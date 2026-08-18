import os
from psycopg2.extras import RealDictCursor
from datetime import date as _date, datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from database.database_manager import get_db_connection

load_dotenv()
TZ_BOGOTA = ZoneInfo("America/Bogota")

# Tope de duración de las consultas de reportes (ver GestionSneMonitor._timeout_reporte).
# 200 s queda por debajo del corte de request de Azure App Service (~230 s): pasado ese
# punto el usuario ya perdió la respuesta, así que no se pierde ningún reporte que hoy
# funcione, pero la base de datos deja de quedar retenida.
REPORTE_TIMEOUT_MS = int(os.getenv("SNE_REPORTE_TIMEOUT_MS", "200000"))

class GestionSneMonitor:
    """
    Modelo para el Monitor SNE.

    Tablas principales:
        sne.gestion_sne   - estado de asignación y objeción por id_ics
        sne.ics           - registros ICS cargados
        config.rutas      - id_linea -> ruta_comercial
        config.cop        - id_cop -> cop / componente / zona
        public.usuarios   - id -> nombres / apellidos
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

    # ── Helpers ──────────────────────────────────────────────────────────────

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

    def _timeout_reporte(self):
        """
        Acota en la propia sesión de PostgreSQL cuánto puede durar una consulta de
        reporte. Cancelar el fetch desde el navegador no detiene la consulta en el
        servidor: sin este tope, una consulta abandonada sigue reteniendo locks
        sobre sne.ics y config.* hasta terminar.

        Se usa SET LOCAL a propósito: revierte solo al cerrar la transacción, así no
        se filtra a la siguiente petición que reutilice la conexión del pool (y es la
        forma admitida cuando PgBouncer trabaja en modo transaction).
        """
        self.cursor.execute("SET LOCAL statement_timeout = %s", (REPORTE_TIMEOUT_MS,))

    def _cop_joins_and_exprs(self):
        """Retorna (joins_sql, comp_expr, zona_expr) según columnas reales en config.cop."""
        has_id_comp = self._col_exists("config", "cop", "id_componente")
        has_id_zona = self._col_exists("config", "cop", "id_zona")
        joins, comp_expr, zona_expr = [], "NULL::text", "NULL::text"
        if has_id_comp:
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        if has_id_zona:
            joins.append("LEFT JOIN config.zona z ON z.id = c.id_zona")
            zona_expr = "z.zona"
        return " ".join(joins), comp_expr, zona_expr

    # ── Meta fechas ──────────────────────────────────────────────────────────

    def monitor_meta_fechas(self) -> dict:
        """Primer día del mes actual y última fecha con registros ICS."""
        hoy = datetime.now(TZ_BOGOTA).date()
        self.cursor.execute(
            """
            SELECT
                MAX(i.fecha)::text AS ultima_fecha
            FROM sne.ics i
            """
        )
        row = self.cursor.fetchone()
        primer_dia_mes = hoy.replace(day=1)
        if row and row.get("ultima_fecha"):
            ultima_fecha = datetime.strptime(row["ultima_fecha"], "%Y-%m-%d").date()
            if ultima_fecha < primer_dia_mes:
                ultima_fecha = primer_dia_mes
            return {
                "ultima_fecha": ultima_fecha.isoformat(),
                "primer_dia_mes": primer_dia_mes.isoformat(),
            }
        return {
            "ultima_fecha": primer_dia_mes.isoformat(),
            "primer_dia_mes": primer_dia_mes.isoformat(),
        }

    # ── Usuarios disponibles ─────────────────────────────────────────────────

    def monitor_usuarios_disponibles(self, fecha_ini=None, fecha_fin=None) -> list:
        """Usuarios (revisores) con asignaciones en el rango → para filtro de Resultados."""
        conds = ["gs.estado_asignacion = 1", "gs.revisor IS NOT NULL", "gs.revisor <> 0"]
        params = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT gs.revisor AS id,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.revisor::text) AS nombre_completo
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN public.usuarios u ON u.id = gs.revisor
            {where}
            ORDER BY nombre_completo
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_usuarios_revisores(self, fecha_ini=None, fecha_fin=None) -> list:
        """Revisores distintos (campo revisor de gestion_sne) para filtro de Asignaciones."""
        conds = ["gs.revisor IS NOT NULL", "gs.revisor <> 0"]
        params = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT gs.revisor AS id,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.revisor::text) AS nombre_completo
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN public.usuarios u ON u.id = gs.revisor
            {where}
            ORDER BY nombre_completo
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_usuarios_asignadores(self, fecha_ini=None, fecha_fin=None) -> list:
        """Usuarios que asignaron ICS (campo usuario_asigna) para filtro de Asignaciones."""
        conds = ["gs.usuario_asigna IS NOT NULL"]
        params = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT gs.usuario_asigna AS id,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.usuario_asigna::text) AS nombre_completo
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN public.usuarios u ON u.id = gs.usuario_asigna
            {where}
            ORDER BY nombre_completo
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_usuarios_objetores(self, fecha_ini=None, fecha_fin=None) -> list:
        """Usuarios que objetaron (campo usuario_objeta) para filtro de Asignaciones."""
        conds = ["gs.usuario_objeta IS NOT NULL"]
        params = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT gs.usuario_objeta AS id,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.usuario_objeta::text) AS nombre_completo
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN public.usuarios u ON u.id = gs.usuario_objeta
            {where}
            ORDER BY nombre_completo
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    # ── Filtros de catálogo ──────────────────────────────────────────────────

    def listar_componentes(self) -> list:
        joins, comp_expr, _ = self._cop_joins_and_exprs()
        if comp_expr == "NULL::text":
            return []
        self.cursor.execute(
            f"""
            SELECT DISTINCT {comp_expr} AS componente
            FROM config.cop c {joins}
            WHERE {comp_expr} IS NOT NULL
            ORDER BY componente
            """
        )
        return [r["componente"] for r in self.cursor.fetchall()]

    def listar_zonas(self, componente=None) -> list:
        joins, comp_expr, zona_expr = self._cop_joins_and_exprs()
        if zona_expr == "NULL::text":
            return []
        conds = [f"{zona_expr} IS NOT NULL"]
        params = []
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT {zona_expr} AS zona
            FROM config.cop c {joins}
            {where}
            ORDER BY zona
            """,
            params,
        )
        return [r["zona"] for r in self.cursor.fetchall()]

    def listar_cop(self, componente=None, zona=None) -> list:
        joins, comp_expr, zona_expr = self._cop_joins_and_exprs()
        conds = ["1=1"]
        params = []
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT c.id, c.cop, {comp_expr} AS componente, {zona_expr} AS zona
            FROM config.cop c {joins}
            {where}
            ORDER BY c.cop
            """,
            params,
        )
        return self.cursor.fetchall()

    def listar_rutas_disponibles(self, id_cop=None, zona=None,
                                  fecha_ini=None, fecha_fin=None,
                                  usuario_id=None) -> list:
        """Rutas disponibles según ICS en el rango y catálogo de rutas."""
        joins, _, zona_expr = self._cop_joins_and_exprs()
        conds = ["r.estado = 1"]
        params = []
        if id_cop:
            conds.append("r.id_cop = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        if fecha_ini:
            conds.append("i2.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i2.fecha <= %s"); params.append(fecha_fin)
        if usuario_id:
            conds.append("gs.revisor = %s"); params.append(int(usuario_id))

        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT DISTINCT r.id_linea, r.ruta_comercial
            FROM config.rutas r
            JOIN sne.ics i2 ON i2.id_linea = r.id_linea
            LEFT JOIN sne.gestion_sne gs ON gs.id_ics = i2.id_ics
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins}
            {where}
            ORDER BY r.ruta_comercial
            """,
            params,
        )
        return self.cursor.fetchall()

    # ── Dashboard Resultados ─────────────────────────────────────────────────

    def _mon_where(self, fecha_ini=None, fecha_fin=None,
                   id_linea=None, id_cop=None, usuario_id=None,
                   zona=None, componente=None,
                   estado_asignacion=None, usuario_asigna=None,
                   estado_objecion=None):
        """WHERE para el dashboard de Resultados. Sin usuario_id muestra todos los usuarios."""
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        where = " WHERE gs.estado_asignacion = 1 "
        params = []
        if usuario_id:
            where += " AND gs.revisor = %s "; params.append(int(usuario_id))
        if fecha_ini:
            where += " AND i.fecha >= %s "; params.append(fecha_ini)
        if fecha_fin:
            where += " AND i.fecha <= %s "; params.append(fecha_fin)
        if id_linea:
            where += " AND i.id_linea = %s "; params.append(int(id_linea))
        if id_cop:
            where += " AND r.id_cop = %s "; params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            where += f" AND {zona_expr} = %s "; params.append(zona)
        if componente and comp_expr != "NULL::text":
            where += f" AND {comp_expr} = %s "; params.append(componente)
        if estado_asignacion is not None:
            where += " AND gs.estado_asignacion = %s "; params.append(int(estado_asignacion))
        if usuario_asigna:
            where += " AND gs.usuario_asigna = %s "; params.append(int(usuario_asigna))
        if estado_objecion is not None:
            where += " AND gs.estado_objecion = %s "; params.append(int(estado_objecion))
        return joins_cop, where, params

    def monitor_comportamiento_registros(self, fecha_ini=None, fecha_fin=None,
                                          id_linea=None, id_cop=None, usuario_id=None,
                                          zona=None, componente=None,
                                          estado_asignacion=None, usuario_asigna=None,
                                          estado_objecion=None) -> list:
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text                                               AS fecha,
                COUNT(gs.id_ics)                                           AS ids_asignados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion = 1)    AS ids_revisados
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_comportamiento_km(self, fecha_ini=None, fecha_fin=None,
                                   id_linea=None, id_cop=None, usuario_id=None,
                                   zona=None, componente=None,
                                   estado_asignacion=None, usuario_asigna=None,
                                   estado_objecion=None) -> list:
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text                                                           AS fecha,
                COALESCE(SUM(i.km_revision), 0)                                        AS km_revision,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0)                 AS km_objetado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_distribucion_motivos(self, fecha_ini=None, fecha_fin=None,
                                      id_linea=None, id_cop=None, usuario_id=None,
                                      zona=None, componente=None,
                                      estado_asignacion=None, usuario_asigna=None,
                                      estado_objecion=None) -> dict:
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT me.motivo AS etiqueta, COALESCE(SUM(i.km_revision), 0) AS km_total
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            JOIN sne.motivos_eliminacion me ON me.id = imr.motivo
            {where}
            GROUP BY me.motivo ORDER BY km_total DESC LIMIT 15
            """,
            params,
        )
        por_motivo = [dict(r) for r in self.cursor.fetchall()]
        self.cursor.execute(
            f"""
            SELECT rs.responsable AS etiqueta, COALESCE(SUM(i.km_revision), 0) AS km_total
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            JOIN sne.ics_motivo_resp imr ON imr.id_ics = i.id_ics
            JOIN sne.responsable_sne rs ON rs.id = imr.responsable
            {where}
            GROUP BY rs.responsable ORDER BY km_total DESC LIMIT 15
            """,
            params,
        )
        por_responsable = [dict(r) for r in self.cursor.fetchall()]
        return {"por_motivo": por_motivo, "por_responsable": por_responsable}

    def monitor_por_ruta(self, fecha_ini=None, fecha_fin=None,
                          id_linea=None, id_cop=None, usuario_id=None,
                          zona=None, componente=None,
                          estado_asignacion=None, usuario_asigna=None,
                          estado_objecion=None) -> list:
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr = "COALESCE(SUM(gs.km_aceptado), 0)" if has_km_ac else "0::numeric"
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                EXTRACT(YEAR  FROM i.fecha)::int                                AS anio,
                EXTRACT(MONTH FROM i.fecha)::int                                AS mes,
                COALESCE(r.ruta_comercial::text, 'Sin Ruta')                   AS ruta_comercial,
                COUNT(gs.id_ics)                                               AS ids_asignados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion = 1)        AS ids_revisados,
                COALESCE(SUM(i.km_revision), 0)                                AS km_revision,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0)         AS km_objetado,
                {km_ac_expr}                                                   AS km_aceptado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
            GROUP BY EXTRACT(YEAR FROM i.fecha), EXTRACT(MONTH FROM i.fecha), r.ruta_comercial
            ORDER BY anio, mes, ruta_comercial
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_por_revisor(self, fecha_ini=None, fecha_fin=None,
                             id_linea=None, id_cop=None, usuario_id=None,
                             zona=None, componente=None,
                             estado_asignacion=None, usuario_asigna=None,
                             estado_objecion=None) -> list:
        """Datos por año/mes/revisor para tabla Comportamiento Revisor."""
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr = "COALESCE(SUM(gs.km_aceptado), 0)" if has_km_ac else "0::numeric"
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                EXTRACT(YEAR  FROM i.fecha)::int                                AS anio,
                EXTRACT(MONTH FROM i.fecha)::int                                AS mes,
                gs.revisor                                                      AS revisor_id,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.revisor::text) AS revisor_nombre,
                COUNT(gs.id_ics)                                               AS ids_asignados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion = 1)        AS ids_revisados,
                COUNT(gs.id_ics) FILTER (WHERE gs.estado_objecion IN (1, 3))  AS ids_obj_mi,
                COALESCE(SUM(i.km_revision), 0)                                AS km_revision,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0)         AS km_objetado,
                {km_ac_expr}                                                   AS km_aceptado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            LEFT JOIN public.usuarios u ON u.id = gs.revisor
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
              AND gs.revisor IS NOT NULL AND gs.revisor <> 0
            GROUP BY EXTRACT(YEAR FROM i.fecha), EXTRACT(MONTH FROM i.fecha),
                     gs.revisor, u.nombres, u.apellidos
            ORDER BY revisor_nombre, anio, mes
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_comportamiento_km_aceptado(self, fecha_ini=None, fecha_fin=None,
                                            id_linea=None, id_cop=None, usuario_id=None,
                                            zona=None, componente=None,
                                            estado_asignacion=None, usuario_asigna=None,
                                            estado_objecion=None) -> list:
        """Suma diaria de km_objetado vs km_aceptado."""
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr = "COALESCE(SUM(gs.km_aceptado), 0)" if has_km_ac else "0::numeric"
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text AS fecha,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0) AS km_objetado,
                {km_ac_expr}                                            AS km_aceptado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_exito_objecion(self, fecha_ini=None, fecha_fin=None,
                                id_linea=None, id_cop=None, usuario_id=None,
                                zona=None, componente=None,
                                estado_asignacion=None, usuario_asigna=None,
                                estado_objecion=None) -> list:
        """Suma diaria de km_revision vs km_aceptado para Éxito de Objeción Contundente."""
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr = "COALESCE(SUM(gs.km_aceptado), 0)" if has_km_ac else "0::numeric"
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text AS fecha,
                COALESCE(SUM(i.km_revision), 0) AS km_revision,
                {km_ac_expr}                     AS km_aceptado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_estimacion_ingresos(self, fecha_ini=None, fecha_fin=None,
                                     id_linea=None, id_cop=None, usuario_id=None,
                                     zona=None, componente=None,
                                     estado_asignacion=None, usuario_asigna=None,
                                     estado_objecion=None) -> list:
        """
        Suma diaria de ingresos estimados:
          ingresos_objetado = SUM(km_objetado × tarifa_por_bus_y_fecha)
          ingresos_aceptado = SUM(km_aceptado × tarifa_por_bus_y_fecha)
        Filtra a estado_objecion=1. Tarifa: config.tarifas con id_tipo_tarifa=1.
        """
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr = "COALESCE(gs.km_aceptado, 0)" if has_km_ac else "0::numeric"
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        where_ing = where + " AND gs.estado_objecion = 1 "
        self.cursor.execute(
            f"""
            SELECT
                i.fecha::text AS fecha,
                COALESCE(SUM(
                    COALESCE(og.km_objetado, 0) * COALESCE(t_tar.tarifa, 0)
                ), 0) AS ingresos_objetado,
                COALESCE(SUM(
                    {km_ac_expr} * COALESCE(t_tar.tarifa, 0)
                ), 0) AS ingresos_aceptado
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og ON true
            LEFT JOIN config.buses_cexp b
                ON UPPER(b.no_interno::text) = UPPER(i.vehiculo_real::text)
            LEFT JOIN config.tipologia_bus tb
                ON UPPER(COALESCE(tb.tipologia, '')) = UPPER(COALESCE(b.tipologia, ''))
            LEFT JOIN config.combustible cb
                ON UPPER(COALESCE(cb.combustible, '')) = UPPER(COALESCE(b.combustible, ''))
            LEFT JOIN LATERAL (
                SELECT tarifa FROM config.tarifas
                WHERE id_tipo_tarifa = 1
                  AND id_tipologia   = tb.id
                  AND id_combustible = cb.id
                  AND i.fecha >= desde
                  AND (hasta IS NULL OR i.fecha <= hasta)
                ORDER BY desde DESC LIMIT 1
            ) t_tar ON true
            {where_ing}
            GROUP BY i.fecha
            ORDER BY i.fecha
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    # ── Tabla Asignaciones ───────────────────────────────────────────────────

    def monitor_boxplot_rutas(self, fecha_ini=None, fecha_fin=None,
                               id_linea=None, id_cop=None, usuario_revisor=None,
                               zona=None, componente=None,
                               estado_asignacion=None, usuario_asigna=None,
                               estado_objecion=None, modo="sne") -> list:
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        conds = ["COALESCE(r.ruta_comercial::text, 'Sin Ruta') IS NOT NULL"]
        params = []
        extra_join = ""
        value_expr = "i.km_revision::numeric"

        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        if id_linea:
            conds.append("i.id_linea = %s"); params.append(int(id_linea))
        if id_cop:
            conds.append("r.id_cop = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        if usuario_revisor:
            conds.append("gs.revisor = %s"); params.append(int(usuario_revisor))
        if usuario_asigna:
            conds.append("gs.usuario_asigna = %s"); params.append(int(usuario_asigna))
        if estado_asignacion is not None:
            conds.append("gs.estado_asignacion = %s"); params.append(int(estado_asignacion))
        if estado_objecion is not None:
            conds.append("gs.estado_objecion = %s"); params.append(int(estado_objecion))

        modo = (modo or "sne").strip().lower()
        if modo == "asignado":
            conds.append("gs.estado_asignacion = 1")
            conds.append("i.km_revision IS NOT NULL")
        elif modo == "objetado":
            extra_join = """
            LEFT JOIN LATERAL (
                SELECT km_objetado
                FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics
                ORDER BY fecha_guardado DESC
                LIMIT 1
            ) og_box ON true
            """
            conds.append("gs.estado_objecion = 1")
            conds.append("og_box.km_objetado IS NOT NULL")
            value_expr = "og_box.km_objetado::numeric"
        else:
            conds.append("i.km_revision IS NOT NULL")

        where = " WHERE " + " AND ".join(conds)
        self.cursor.execute(
            f"""
            SELECT
                COALESCE(r.ruta_comercial::text, 'Sin Ruta') AS ruta_comercial,
                {value_expr}                                AS km_valor
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {extra_join}
            {where}
            ORDER BY ruta_comercial, km_valor
            """,
            params,
        )
        return [dict(r) for r in self.cursor.fetchall()]

    def monitor_por_objetor(self, fecha_ini=None, fecha_fin=None,
                             id_linea=None, id_cop=None, usuario_id=None,
                             zona=None, componente=None,
                             estado_asignacion=None, usuario_asigna=None,
                             estado_objecion=None) -> list:
        """Km objetado vs km asignado agrupado por objetor, ordenado por % cumplimiento."""
        joins_cop, where, params = self._mon_where(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_id,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
        where += " AND gs.usuario_objeta IS NOT NULL "
        self.cursor.execute(
            f"""
            SELECT
                gs.usuario_objeta                                                       AS usuario_id,
                COALESCE(u.nombres || ' ' || u.apellidos,
                         'ID ' || gs.usuario_objeta::text)                             AS objetor,
                COALESCE(SUM(i.km_revision), 0)                                        AS km_asignado,
                COALESCE(SUM(CASE WHEN gs.estado_objecion = 1
                               THEN og_lat.km_objetado ELSE 0 END), 0)                 AS km_objetado,
                COUNT(*)                                                               AS total_asignados,
                COUNT(*) FILTER (WHERE gs.estado_objecion = 1)                        AS total_objetados
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics ORDER BY fecha_guardado DESC LIMIT 1
            ) og_lat ON true
            LEFT JOIN public.usuarios u ON u.id = gs.usuario_objeta
            {where}
            GROUP BY gs.usuario_objeta, u.nombres, u.apellidos
            ORDER BY km_objetado DESC
            """,
            params,
        )
        result = []
        for r in self.cursor.fetchall():
            km_a = float(r["km_asignado"] or 0)
            km_o = float(r["km_objetado"] or 0)
            result.append({
                "objetor": r["objetor"],
                "km_asignado": round(km_a, 3),
                "km_objetado": round(km_o, 3),
                "km_pendiente": round(max(km_a - km_o, 0), 3),
                "pct_cumplimiento": round(km_o / km_a * 100, 1) if km_a else 0.0,
                "total_asignados": int(r["total_asignados"] or 0),
                "total_objetados": int(r["total_objetados"] or 0),
            })
        return result

    # ── Reportes ─────────────────────────────────────────────────────────────
    def reporte_sne_procesado(
        self,
        fecha_ini=None, fecha_fin=None,
        id_linea=None, id_cop=None,
        zona=None, componente=None,
    ) -> list:
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        conds: list[str] = []
        params: list = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        if id_linea:
            conds.append("i.id_linea = %s"); params.append(int(id_linea))
        if id_cop:
            conds.append("log.get_cop_at_date(i.id_linea, i.fecha::date) = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        self._timeout_reporte()
        self.cursor.execute(
            f"""
            SELECT
                i.id,
                i.fecha::text                  AS fecha,
                i.id_ics,
                i.id_linea,
                r.ruta_comercial,
                i.servicio,
                i.tabla,
                i.viaje_linea,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                i.hora_ini_teorica::text        AS hora_ini_teorica,
                i.km_prog_ad,
                i.conductor,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                i.motivo,
                i.fecha_carga::text             AS fecha_carga,
                {comp_expr}                     AS componente,
                {zona_expr}                     AS zona,
                c.cop
            FROM sne.ics i
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = log.get_cop_at_date(i.id_linea, i.fecha::date)
            {joins_cop}
            {where}
            ORDER BY i.fecha, i.id_ics
            """,
            params,
        )
        rows = []
        for r in self.cursor.fetchall():
            row = dict(r)
            for k in ("km_prog_ad", "km_elim_eic", "km_ejecutado",
                      "offset_inicio", "offset_fin", "km_revision"):
                v = row.get(k)
                row[k] = round(float(v), 3) if v is not None else None
            rows.append(row)
        return rows

    def reporte_sne_gestionado(
        self,
        fecha_ini=None, fecha_fin=None,
        id_linea=None, id_cop=None,
        zona=None, componente=None,
        id_revisor=None, revisor_id=None,   # id_revisor = nuevo; revisor_id = alias legacy
        id_asignador=None,
        estado_asignacion=None,
        estado_objecion=None,
    ) -> list:
        # soporte alias legacy
        id_revisor = id_revisor or revisor_id
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        has_fht   = self._col_exists("sne", "gestion_sne", "fecha_hora_transmitools")
        has_fini  = self._col_exists("sne", "gestion_sne", "fecha_inicio_dp")
        has_fcier = self._col_exists("sne", "gestion_sne", "fecha_cierre_dp")
        has_fase  = self._col_exists("sne", "gestion_sne", "fase_dp_actual")
        has_edo   = self._col_exists("sne", "gestion_sne", "estado_dp_actual")
        has_fcamb = self._col_exists("sne", "gestion_sne", "fecha_cambio_dp")
        has_km_ac = self._col_exists("sne", "gestion_sne", "km_aceptado")
        has_et    = self._col_exists("sne", "gestion_sne", "estado_transmitools")

        def _ts(col):
            return f"to_char({col} AT TIME ZONE 'America/Bogota', 'YYYY-MM-DD HH24:MI')"

        fht_expr   = _ts("gs.fecha_hora_transmitools") if has_fht   else "NULL::text"
        fini_expr  = _ts("gs.fecha_inicio_dp")          if has_fini  else "NULL::text"
        fcier_expr = _ts("gs.fecha_cierre_dp")           if has_fcier else "NULL::text"
        fase_expr  = "gs.fase_dp_actual::text"           if has_fase  else "NULL::text"
        edo_expr   = "gs.estado_dp_actual::text"         if has_edo   else "NULL::text"
        fcamb_expr = _ts("gs.fecha_cambio_dp")           if has_fcamb else "NULL::text"
        km_ac_expr = "ROUND(gs.km_aceptado::numeric, 3)" if has_km_ac else "NULL::numeric"
        et_expr    = (
            "CASE gs.estado_transmitools "
            "WHEN 0 THEN 'Sin Gestión' "
            "WHEN 1 THEN 'Envío Exitoso' "
            "WHEN 2 THEN 'Procesando' "
            "WHEN 3 THEN 'Error' "
            "ELSE gs.estado_transmitools::text END"
        ) if has_et else "NULL::text"

        conds: list[str] = []
        params: list = []
        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        if id_linea:
            conds.append("i.id_linea = %s"); params.append(int(id_linea))
        if id_cop:
            conds.append("log.get_cop_at_date(i.id_linea, i.fecha::date) = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        if id_revisor:
            conds.append("gs.revisor = %s");          params.append(int(id_revisor))
        if id_asignador:
            conds.append("gs.usuario_asigna = %s");   params.append(int(id_asignador))
        if estado_asignacion is not None:
            conds.append("gs.estado_asignacion = %s"); params.append(int(estado_asignacion))
        if estado_objecion is not None:
            conds.append("gs.estado_objecion = %s");  params.append(int(estado_objecion))
        where = (" WHERE " + " AND ".join(conds)) if conds else ""

        self._timeout_reporte()
        self.cursor.execute(
            f"""
            SELECT
                i.id,
                i.fecha::text                                               AS fecha,
                i.id_ics,
                i.id_linea,
                r.ruta_comercial,
                i.servicio,
                i.tabla,
                i.viaje_linea,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                i.hora_ini_teorica::text                                    AS hora_ini_teorica,
                i.km_prog_ad,
                i.conductor,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                i.motivo,
                i.fecha_carga::text                                         AS fecha_carga,
                {comp_expr}                                                 AS componente,
                {zona_expr}                                                 AS zona,
                c.cop,
                COALESCE(u_rev.nombres  || ' ' || u_rev.apellidos,  '')    AS revisor,
                CASE gs.estado_asignacion
                    WHEN 0 THEN 'No Asignado'
                    WHEN 1 THEN 'Asignado'
                    WHEN 2 THEN 'Vencido'
                    ELSE COALESCE(gs.estado_asignacion::text, '')
                END                                                         AS estado_asignacion,
                COALESCE(u_asig.nombres || ' ' || u_asig.apellidos, '')    AS usuario_asigna,
                to_char(gs.fecha_hora_asignacion AT TIME ZONE 'America/Bogota',
                        'YYYY-MM-DD HH24:MI')                              AS fecha_hora_asignacion,
                CASE gs.estado_objecion
                    WHEN 0 THEN 'No Objetado'
                    WHEN 1 THEN 'Objetado'
                    WHEN 2 THEN 'Vencido'
                    WHEN 3 THEN 'Manejo Interno'
                    ELSE COALESCE(gs.estado_objecion::text, '')
                END                                                         AS estado_objecion,
                COALESCE(u_obj.nombres  || ' ' || u_obj.apellidos,  '')    AS usuario_objeta,
                to_char(gs.fecha_hora_objecion AT TIME ZONE 'America/Bogota',
                        'YYYY-MM-DD HH24:MI')                              AS fecha_hora_objecion,
                ROUND(og.km_objetado::numeric,    3)                       AS km_objetado,
                ROUND(COALESCE(og.km_objetado, 0) * COALESCE(t_tar.tarifa, 0), 0) AS valor_km_objetado,
                ROUND(og.km_no_objetado::numeric, 3)                       AS km_no_objetado,
                rs.responsable                                              AS responsable_final,
                me.motivo                                                   AS motivo_final,
                ac.accion                                                   AS accion,
                ju.justificacion                                            AS justificacion,
                og.nota_objecion,
                to_char(og.fecha_guardado AT TIME ZONE 'America/Bogota',
                        'YYYY-MM-DD HH24:MI')                              AS fecha_guardado,
                og.tiempo_objecion_seg,
                og.reporte,
                {et_expr}                                                   AS estado_transmitools,
                {fht_expr}                                                  AS fecha_hora_transmitools,
                {fini_expr}                                                 AS fecha_inicio_dp,
                {fcier_expr}                                                AS fecha_cierre_dp,
                {fase_expr}                                                 AS fase_dp_actual,
                {edo_expr}                                                  AS estado_dp_actual,
                {fcamb_expr}                                                AS fecha_cambio_dp,
                {km_ac_expr}                                                AS km_aceptado,
                ROUND(COALESCE({km_ac_expr}, 0) * COALESCE(t_tar.tarifa, 0), 0) AS valor_km_aceptado
            FROM sne.ics i
            LEFT JOIN config.rutas r     ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c       ON c.id = log.get_cop_at_date(i.id_linea, i.fecha::date)
            {joins_cop}
            LEFT JOIN sne.gestion_sne gs ON gs.id_ics = i.id_ics
            LEFT JOIN public.usuarios u_rev  ON u_rev.id  = gs.revisor
            LEFT JOIN public.usuarios u_asig ON u_asig.id = gs.usuario_asigna
            LEFT JOIN public.usuarios u_obj  ON u_obj.id  = gs.usuario_objeta
            LEFT JOIN LATERAL (
                SELECT
                    km_objetado, km_no_objetado,
                    responsable_final, motivo_final, accion, justificacion,
                    nota_objecion, fecha_guardado, tiempo_objecion_seg, reporte
                FROM sne.objecion_guardada
                WHERE id_ics = i.id_ics
                ORDER BY fecha_guardado DESC
                LIMIT 1
            ) og ON true
            LEFT JOIN config.buses_cexp b
                ON UPPER(b.no_interno::text) = UPPER(i.vehiculo_real::text)
            LEFT JOIN config.tipologia_bus tb
                ON UPPER(COALESCE(tb.tipologia, '')) = UPPER(COALESCE(b.tipologia, ''))
            LEFT JOIN config.combustible cb
                ON UPPER(COALESCE(cb.combustible, '')) = UPPER(COALESCE(b.combustible, ''))
            LEFT JOIN LATERAL (
                SELECT tarifa FROM config.tarifas
                WHERE id_tipo_tarifa = 1
                  AND id_tipologia   = tb.id
                  AND id_combustible = cb.id
                  AND i.fecha >= desde
                  AND (hasta IS NULL OR i.fecha <= hasta)
                ORDER BY desde DESC LIMIT 1
            ) t_tar ON true
            LEFT JOIN sne.responsable_sne    rs ON rs.id               = og.responsable_final
            LEFT JOIN sne.motivos_eliminacion me ON me.id              = og.motivo_final
            LEFT JOIN sne.acciones           ac ON ac.id_acc           = og.accion
            LEFT JOIN sne.justificacion      ju ON ju.id_justificacion = og.justificacion
            {where}
            ORDER BY i.fecha, i.id_ics
            """,
            params,
        )
        rows = []
        for r in self.cursor.fetchall():
            row = dict(r)
            for k in ("km_prog_ad", "km_elim_eic", "km_ejecutado",
                      "offset_inicio", "offset_fin", "km_revision",
                      "km_objetado", "km_no_objetado", "km_aceptado"):
                v = row.get(k)
                row[k] = round(float(v), 3) if v is not None else None
            for k in ("valor_km_objetado", "valor_km_aceptado"):
                v = row.get(k)
                row[k] = round(float(v), 0) if v is not None else None
            rows.append(row)
        return rows

    def monitor_tabla_asignaciones(
        self,
        fecha_ini=None, fecha_fin=None,
        id_linea=None, id_cop=None,
        zona=None, componente=None,
        estado_asignacion=None, usuario_revisor=None, usuario_asigna=None,
        estado_objecion=None, usuario_objeta=None,
        fecha_inicio_dp=None, fecha_fin_dp=None,
    ) -> list:
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        has_creado_en    = self._col_exists("sne", "gestion_sne", "creado_en")
        has_fecha_cierre = self._col_exists("sne", "gestion_sne", "fecha_cierre_dp")
        has_fecha_ini_dp = self._col_exists("sne", "gestion_sne", "fecha_inicio_dp")
        has_km_ac        = self._col_exists("sne", "gestion_sne", "km_aceptado")
        km_ac_expr       = "COALESCE(SUM(gs.km_aceptado), 0)" if has_km_ac else "0::numeric"

        # ── WHERE conditions ────────────────────────────────────────────────
        conds: list[str] = []
        params: list = []

        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        if id_linea:
            conds.append("i.id_linea = %s"); params.append(int(id_linea))
        if id_cop:
            conds.append("r.id_cop = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"{zona_expr} = %s"); params.append(zona)
        if componente and comp_expr != "NULL::text":
            conds.append(f"{comp_expr} = %s"); params.append(componente)
        if estado_asignacion is not None:
            conds.append("gs.estado_asignacion = %s"); params.append(int(estado_asignacion))
        if usuario_revisor:
            conds.append("gs.revisor = %s"); params.append(int(usuario_revisor))
        if usuario_asigna:
            conds.append("gs.usuario_asigna = %s"); params.append(int(usuario_asigna))
        if estado_objecion is not None:
            conds.append("gs.estado_objecion = %s"); params.append(int(estado_objecion))
        if usuario_objeta:
            conds.append("gs.usuario_objeta = %s"); params.append(int(usuario_objeta))
        if fecha_inicio_dp and has_fecha_ini_dp:
            conds.append("gs.fecha_inicio_dp >= %s"); params.append(fecha_inicio_dp)
        if fecha_fin_dp and has_fecha_cierre:
            conds.append("gs.fecha_cierre_dp <= %s"); params.append(fecha_fin_dp)

        where = (" WHERE " + " AND ".join(conds)) if conds else ""

        # ── Expresiones condicionales ────────────────────────────────────────
        if has_fecha_cierre:
            por_objetar_cond = (
                "gs.estado_asignacion = 1 "
                "AND (gs.estado_objecion = 0 OR gs.estado_objecion IS NULL) "
                "AND (gs.fecha_cierre_dp IS NULL "
                "     OR gs.fecha_cierre_dp > NOW() AT TIME ZONE 'America/Bogota')"
            )
            vencidos_asig_cond = (
                "gs.estado_asignacion = 1 AND ("
                "  gs.estado_objecion = 2 "
                "  OR ((gs.estado_objecion = 0 OR gs.estado_objecion IS NULL) "
                "      AND gs.fecha_cierre_dp IS NOT NULL "
                "      AND gs.fecha_cierre_dp < NOW() AT TIME ZONE 'America/Bogota')"
                ")"
            )
        else:
            por_objetar_cond  = "gs.estado_asignacion = 1 AND (gs.estado_objecion = 0 OR gs.estado_objecion IS NULL)"
            vencidos_asig_cond = "gs.estado_asignacion = 1 AND gs.estado_objecion = 2"

        if has_creado_en:
            dias_carga_asig_expr = (
                "ROUND(AVG(CASE WHEN gs.estado_asignacion IN (1,2) "
                "               AND gs.fecha_hora_asignacion IS NOT NULL "
                "          THEN EXTRACT(EPOCH FROM (gs.fecha_hora_asignacion - gs.creado_en)) / 86400.0 "
                "          END)::numeric, 1)"
            )
        else:
            dias_carga_asig_expr = "NULL::numeric"

        dias_asig_obj_expr = (
            "ROUND(AVG(CASE WHEN gs.estado_objecion = 1 "
            "               AND gs.fecha_hora_objecion IS NOT NULL "
            "               AND gs.fecha_hora_asignacion IS NOT NULL "
            "          THEN EXTRACT(EPOCH FROM (gs.fecha_hora_objecion - gs.fecha_hora_asignacion)) / 86400.0 "
            "          END)::numeric, 1)"
        )

        self.cursor.execute(
            f"""
            SELECT
                EXTRACT(YEAR  FROM i.fecha)::int                                     AS anio,
                EXTRACT(MONTH FROM i.fecha)::int                                     AS mes,
                COALESCE(r.ruta_comercial::text, 'Sin Ruta')                        AS ruta_comercial,
                MIN(COALESCE({comp_expr}, 'Sin Componente'))                        AS componente,
                COALESCE(SUM(i.km_revision), 0)                                     AS total_km,
                COUNT(*)                                                              AS total_registros,
                COUNT(*) FILTER (WHERE gs.estado_asignacion = 0)                    AS por_asignar,
                COUNT(*) FILTER (WHERE gs.estado_asignacion = 1)                    AS asignados,
                COUNT(*) FILTER (WHERE gs.estado_asignacion = 2)                    AS vencidos_sin_asignar,
                COUNT(*) FILTER (WHERE {por_objetar_cond})                          AS por_objetar,
                COUNT(*) FILTER (WHERE gs.estado_objecion = 1)                      AS objetados,
                COUNT(*) FILTER (WHERE gs.estado_objecion = 3)                      AS manejo_interno,
                COUNT(*) FILTER (WHERE {vencidos_asig_cond})                        AS vencidos_asignados,
                COALESCE(SUM(CASE
                    WHEN gs.estado_asignacion = 0 THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_por_asignar,
                COALESCE(SUM(CASE
                    WHEN gs.estado_asignacion = 2 THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_vencidos_sin_asignar,
                COALESCE(SUM(CASE
                    WHEN gs.estado_asignacion = 1 THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_revision_asignados,
                COALESCE(SUM(CASE
                    WHEN {por_objetar_cond} THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_por_objetar,
                COALESCE(SUM(CASE
                    WHEN gs.estado_objecion = 1 THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_revision_objetados,
                COALESCE(SUM(CASE
                    WHEN {vencidos_asig_cond} THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_vencidos_asignados,
                COALESCE(SUM(CASE
                    WHEN gs.estado_objecion = 1 THEN og_lat.km_objetado
                    ELSE 0
                END), 0)                                                            AS km_objetados,
                COALESCE(SUM(CASE
                    WHEN gs.estado_objecion = 3 THEN i.km_revision
                    ELSE 0
                END), 0)                                                            AS km_manejo_interno,
                {km_ac_expr}                                                       AS km_aceptado,
                STRING_AGG(
                    DISTINCT CASE
                        WHEN gs.estado_asignacion = 1 AND u_obj.nombres IS NOT NULL
                        THEN (u_obj.nombres || ' ' || u_obj.apellidos)
                    END,
                    ', '
                )                                                                     AS usuarios_objetadores,
                {dias_carga_asig_expr}                                               AS dias_prom_carga_asig,
                {dias_asig_obj_expr}                                                 AS dias_prom_asig_obj
            FROM sne.gestion_sne gs
            JOIN sne.ics i            ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r  ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c    ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN LATERAL (
                SELECT km_objetado
                FROM sne.objecion_guardada
                WHERE id_ics = gs.id_ics
                ORDER BY fecha_guardado DESC
                LIMIT 1
            ) og_lat ON true
            LEFT JOIN public.usuarios u_obj ON u_obj.id = gs.usuario_objeta
            {where}
            GROUP BY
                EXTRACT(YEAR  FROM i.fecha),
                EXTRACT(MONTH FROM i.fecha),
                r.ruta_comercial
            ORDER BY anio, mes, ruta_comercial
            """,
            params,
        )
        rows = []
        for r in self.cursor.fetchall():
            row = dict(r)
            for key in (
                "total_km",
                "km_por_asignar",
                "km_vencidos_sin_asignar",
                "dias_prom_carga_asig",
                "dias_prom_asig_obj",
                "km_revision_asignados",
                "km_por_objetar",
                "km_revision_objetados",
                "km_vencidos_asignados",
                "km_objetados",
                "km_manejo_interno",
                "km_aceptado",
            ):
                v = row.get(key)
                row[key] = float(v) if v is not None else None
            total = int(row.get("total_registros") or 0)
            total_km = float(row.get("total_km") or 0)
            asignados = int(row.get("asignados") or 0)
            km_rev_asig = float(row.get("km_revision_asignados") or 0)
            km_obj = float(row.get("km_objetados") or 0)
            km_ac = float(row.get("km_aceptado") or 0)
            row["pct_asignacion"] = round(asignados / total * 100, 1) if total else 0.0
            row["pct_asig_km"] = round(km_rev_asig / total_km * 100, 1) if total_km else 0.0
            row["pct_cumplimiento"] = round(km_obj / km_rev_asig * 100, 1) if km_rev_asig else 0.0
            row["pct_exito"] = round(km_ac / km_obj * 100, 1) if km_obj else 0.0
            rows.append(row)
        return rows

    # ── Procesamientos ────────────────────────────────────────────────────────

    def monitor_procesamientos(self, fecha_ini=None, fecha_fin=None):
        """Logs de procesamientos de workflows para la pestaña Procesamientos."""

        def _fmt_dt(v):
            if v is None:
                return None
            if hasattr(v, "strftime"):
                return v.strftime("%d/%m/%Y %H:%M")
            return str(v)

        def _fmt_date(v):
            if v is None:
                return None
            if hasattr(v, "strftime"):
                return v.strftime("%d/%m/%Y")
            return str(v)

        # Condición de rango de fechas (se aplica a las dos tablas de log)
        date_conds = []
        date_params = []
        if fecha_ini:
            date_conds.append("fecha >= %s")
            date_params.append(fecha_ini)
        if fecha_fin:
            date_conds.append("fecha <= %s")
            date_params.append(fecha_fin)
        where_clause = ("WHERE " + " AND ".join(date_conds)) if date_conds else ""

        # Lista de reportes
        self.cursor.execute("""
            SELECT id_reporte, nombre_reporte
            FROM sne.reportes_sne
            ORDER BY id_reporte
        """)
        reportes = [dict(r) for r in self.cursor.fetchall()]

        # Todas las fechas (unión de ambas tablas filtradas) + datos de posicionamiento
        self.cursor.execute(f"""
            WITH fechas AS (
                SELECT DISTINCT fecha FROM log.procesa_posicionamientos {where_clause}
                UNION
                SELECT DISTINCT fecha FROM log.procesa_report_sne {where_clause}
            )
            SELECT
                f.fecha,
                p.estado,
                p.intentos,
                p.ultima_ejecucion,
                p.duracion_seg,
                p.archivos_total,
                p.archivos_ok,
                p.archivos_error,
                p.registros_pos,
                p.filas_km,
                p.actualizado_en
            FROM fechas f
            LEFT JOIN log.procesa_posicionamientos p ON p.fecha = f.fecha
            ORDER BY f.fecha DESC
        """, date_params + date_params)  # date_params se repite por el UNION
        all_fechas = []
        pos_by_fecha = {}
        for r in self.cursor.fetchall():
            fd = _fmt_date(r["fecha"])
            all_fechas.append(fd)
            pos_by_fecha[fd] = {
                "estado": r["estado"],
                "intentos": r["intentos"],
                "ultima_ejecucion": _fmt_dt(r["ultima_ejecucion"]),
                "duracion_seg": r["duracion_seg"],
                "archivos_total": r["archivos_total"],
                "archivos_ok": r["archivos_ok"],
                "archivos_error": r["archivos_error"],
                "registros_pos": r["registros_pos"],
                "filas_km": r["filas_km"],
                "actualizado_en": _fmt_dt(r["actualizado_en"]),
            } if r["estado"] is not None else None

        # Datos de reportes (mismos filtros de fecha)
        rep_where = where_clause  # misma cláusula, mismos params
        self.cursor.execute(f"""
            SELECT
                fecha,
                id_reporte,
                estado,
                ultima_ejecucion,
                duracion_seg,
                archivos_total,
                archivos_ok,
                archivos_error,
                registros_proce,
                fecha_actualizacion
            FROM log.procesa_report_sne
            {rep_where}
            ORDER BY fecha DESC, id_reporte
        """, date_params)
        rep_by_fecha = {}
        for r in self.cursor.fetchall():
            fd = _fmt_date(r["fecha"])
            if fd not in rep_by_fecha:
                rep_by_fecha[fd] = {}
            rep_by_fecha[fd][str(r["id_reporte"])] = {
                "estado": r["estado"],
                "ultima_ejecucion": _fmt_dt(r["ultima_ejecucion"]),
                "duracion_seg": r["duracion_seg"],
                "archivos_total": r["archivos_total"],
                "archivos_ok": r["archivos_ok"],
                "archivos_error": r["archivos_error"],
                "registros_proce": r["registros_proce"],
                "fecha_actualizacion": _fmt_dt(r["fecha_actualizacion"]),
            }

        rows = [
            {
                "fecha": fd,
                "posicionamiento": pos_by_fecha.get(fd),
                "reportes": rep_by_fecha.get(fd, {}),
            }
            for fd in all_fechas
        ]

        return {"reportes": reportes, "rows": rows}

    def monitor_transmitools(
        self,
        fecha_ini=None, fecha_fin=None,
        id_linea=None, id_cop=None,
        zona=None, componente=None,
        usuario_revisor=None,
    ) -> list:
        """
        Retorna todos los ICS con estado_asignacion=1 y estado_objecion=1
        con su estado_transmitools para el monitor global de envíos.
        No filtra por usuario; muestra todos los revisores.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
        has_fht = self._col_exists("sne", "gestion_sne", "fecha_hora_transmitools")
        fht_expr = (
            "to_char(gs.fecha_hora_transmitools AT TIME ZONE 'America/Bogota', 'YYYY-MM-DD HH24:MI')"
            if has_fht else "NULL::text"
        )

        conds: list = [
            "gs.estado_asignacion = 1",
            "gs.estado_objecion = 1",
        ]
        params: list = []

        if fecha_ini:
            conds.append("i.fecha >= %s"); params.append(fecha_ini)
        if fecha_fin:
            conds.append("i.fecha <= %s"); params.append(fecha_fin)
        if id_linea:
            conds.append("i.id_linea = %s"); params.append(int(id_linea))
        if id_cop:
            conds.append("r.id_cop = %s"); params.append(int(id_cop))
        if zona and zona_expr != "NULL::text":
            conds.append(f"UPPER({zona_expr}) = %s"); params.append(zona.strip().upper())
        if componente and comp_expr != "NULL::text":
            conds.append(f"UPPER({comp_expr}) = %s"); params.append(componente.strip().upper())
        if usuario_revisor:
            conds.append("gs.revisor = %s"); params.append(int(usuario_revisor))

        where = "WHERE " + " AND ".join(conds)

        self.cursor.execute(
            f"""
            SELECT
                gs.id_ics,
                gs.estado_transmitools,
                {fht_expr}                                               AS fecha_hora_transmitools,
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
                i.km_revision,
                i.motivo,
                r.ruta_comercial,
                c.cop                                                    AS cop_nombre,
                {comp_expr}                                              AS componente,
                {zona_expr}                                              AS zona,
                COALESCE(u.nombres || ' ' || u.apellidos, 'ID ' || gs.revisor::text) AS revisor_nombre
            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            {joins_cop}
            LEFT JOIN public.usuarios u ON u.id = gs.revisor
            {where}
            ORDER BY gs.estado_transmitools NULLS LAST, i.fecha DESC, gs.id_ics ASC
            """,
            params,
        )
        return self.cursor.fetchall()
