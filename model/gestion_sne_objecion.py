import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
TZ_BOGOTA = ZoneInfo("America/Bogota")

def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)

class GestionSneObjecion:
    """
    Modelo de gestion y objecion de kilometros SNE.

    Tablas involucradas:
        sne.ics              - registros ICS cargados
        sne.gestion_sne      - gestion/estado por revisor (PK = id_ics)
        sne.responsable_sne  - responsables asignables
        config.rutas         - id_linea -> ruta_comercial
        config.cop           - id_cop   -> cop / componente / zona
        config.componente    - id       -> componente
        config.zona          - id       -> zona
        public.usuarios      - id       -> revisor / usuario_asigna
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

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cerrar_conexion()

    # ── Helpers ──────────────────────────────────────────────────────────────────
    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        self.cursor.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return self.cursor.fetchone() is not None

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

    def listar_responsables(self):
        """Lista los responsables de sne.responsable_sne."""
        self.cursor.execute(
            "SELECT id, responsable FROM sne.responsable_sne ORDER BY responsable"
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
                "SELECT id_justificacion, id_acc, justificacion FROM sne.justificacion WHERE id_acc = %s ORDER BY id_justificacion",
                (id_acc,)
            )
        else:
            self.cursor.execute(
                "SELECT id_justificacion, id_acc, justificacion FROM sne.justificacion ORDER BY id_acc, id_justificacion"
            )
        return self.cursor.fetchall()

    def listar_motivos_por_responsable(self, id_responsable: int = None):
        """Lista motivos/observaciones desde sne.motivos_eliminacion filtrados por responsable."""
        if id_responsable:
            self.cursor.execute(
                """
                SELECT me.id, me.observacion, me.responsable
                FROM sne.motivos_eliminacion me
                WHERE me.responsable = %s
                ORDER BY me.observacion
                """,
                (id_responsable,)
            )
        else:
            self.cursor.execute(
                "SELECT id, observacion, responsable FROM sne.motivos_eliminacion ORDER BY responsable, observacion"
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
        fecha: str,
        hora_ini: str = '00:00:00',
        hora_fin: str = '23:59:59',
    ):
        """
        Filtra por rango de timestamps construido desde fecha + hora.
        Usa fecha_evento >= ts_ini AND < ts_fin para aprovechar índice.
        hora_ini / hora_fin en formato HH:MM:SS
        """
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
                p.vel_m_s
            FROM config.posicionamientos p
            WHERE p.movil_bus = %s
                AND p.fecha_evento >= (%s || ' ' || %s)::timestamptz
                AND p.fecha_evento <  (%s || ' ' || %s)::timestamptz
                AND p.latitud  IS NOT NULL
                AND p.longitud IS NOT NULL
            ORDER BY p.fecha_evento ASC
            LIMIT 10000
            """,
            (movil_bus, fecha, hora_ini, fecha, hora_fin),
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
        tab: str = "revisar",
        pagina: int = 1,
        tamano: int = 50,
    ):
        """
        Trae los registros ICS asignados al usuario logueado.

        Logica de tabs:
          - revisar   -> revisor > 0 y estado_asignacion = 1 (pendiente)
          - revisados -> estado_asignacion = 2 (ya revisado)
          - validar   -> estado_objecion = 1 (objecion pendiente)

        NOTA: sne.gestion_sne NO tiene columna 'id'; la PK es id_ics.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        where = " WHERE gs.revisor = %s "
        params: list = [usuario_id]

        if tab == "revisar":
            where += " AND gs.revisor > 0 AND (gs.estado_asignacion = 1 OR gs.estado_asignacion IS NULL) "
        elif tab == "revisados":
            where += " AND gs.estado_asignacion = 2 "
        elif tab == "validar":
            where += " AND gs.estado_objecion = 1 "

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
            where += " AND i.id_responsable = %s "
            params.append(int(id_responsable))

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
        sql = f"""
            SELECT
                gs.id_ics,
                gs.revisor,
                gs.estado_asignacion,
                gs.estado_objecion,
                gs.estado_transmitools,
                to_char(gs.fecha_hora_asignacion, 'YYYY-MM-DD HH24:MI') AS fecha_asignacion,
                to_char(gs.fecha_hora_objecion,   'YYYY-MM-DD HH24:MI') AS fecha_objecion,

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
                i.observacion,
                i.id_concesion,
                i.id_responsable,

                r.ruta_comercial,
                r.id_cop,
                c.cop                                                    AS cop_nombre,
                {comp_expr}                                              AS componente,
                {zona_expr}                                              AS zona,
                rs.responsable                                           AS responsable_nombre
            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            LEFT JOIN sne.responsable_sne rs ON rs.id = i.id_responsable
            {joins_cop}
            {where}
            ORDER BY i.fecha DESC, gs.id_ics ASC
            LIMIT %s OFFSET %s
        """
        self.cursor.execute(sql, params + [tamano, offset])
        registros = self.cursor.fetchall()
        return registros, total

    def obtener_detalle_ics(self, id_ics: int, usuario_id: int):
        """
        Detalle completo de un ICS para el modal de gestion.
        Solo retorna si el id_ics esta asignado al revisor dado.
        NOTA: sne.gestion_sne usa id_ics como PK, no tiene columna 'id'.
        """
        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()
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
                i.observacion,
                i.id_concesion,
                i.id_responsable,

                r.ruta_comercial,
                r.id_cop,
                c.cop                                                    AS cop_nombre,
                {comp_expr}                                              AS componente,
                {zona_expr}                                              AS zona,
                rs.responsable                                           AS responsable_nombre
            FROM sne.gestion_sne gs
            JOIN sne.ics i           ON i.id_ics = gs.id_ics
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            LEFT JOIN config.cop c   ON c.id = r.id_cop
            LEFT JOIN sne.responsable_sne rs ON rs.id = i.id_responsable
            {joins_cop}
            WHERE gs.id_ics = %s AND gs.revisor = %s
            """,
            (id_ics, usuario_id),
        )
        return self.cursor.fetchone()

    def estadisticas_usuario(self, usuario_id: int, fecha: str = None):
        """Estadisticas para los cards del header."""
        where, params = " WHERE gs.revisor = %s ", [usuario_id]
        if fecha:
            where += " AND i.fecha = %s "
            params.append(fecha)
        self.cursor.execute(
            f"""
            SELECT
                COUNT(*)                                            AS total_asignados,
                COUNT(*) FILTER (WHERE gs.estado_asignacion = 2)   AS total_revisados,
                COUNT(*) FILTER (
                    WHERE gs.estado_asignacion = 1
                    OR gs.estado_asignacion IS NULL
                )                                                   AS total_pendientes,
                COUNT(*) FILTER (WHERE gs.estado_objecion = 1)     AS total_objeciones,
                COALESCE(SUM(i.km_prog_ad), 0)                     AS km_totales_asignados,
                COALESCE(SUM(i.km_ejecutado), 0)                   AS km_ejecutados
            FROM sne.gestion_sne gs
            JOIN sne.ics i ON i.id_ics = gs.id_ics
            {where}
            """,
            params,
        )
        return self.cursor.fetchone()

    def actualizar_gestion(
        self,
        id_ics: int,
        usuario_id: int,
        estado_asignacion: int = None,
        estado_objecion: int = None,
        estado_transmitools: int = None,
        observacion: str = None,
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
        if id_responsable is not None:
            ics_sets.append("id_responsable = %s")
            ics_params.append(id_responsable)
        if observacion is not None:
            ics_sets.append("observacion = %s")
            ics_params.append(observacion)
        if km_objetado is not None:
            ics_sets.append("km_objetado = %s")
            ics_params.append(km_objetado)
        if ics_sets:
            ics_params.append(id_ics)
            self.cursor.execute(
                f"UPDATE sne.ics SET {', '.join(ics_sets)} WHERE id_ics = %s",
                ics_params,
            )

        # Actualizar gestion_sne con acción y justificación
        if id_accion is not None:
            sets.append("id_accion = %s")
            params.append(id_accion)
        if id_justificacion is not None:
            sets.append("id_justificacion = %s")
            params.append(id_justificacion)
        
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

        if observacion is not None:
            self.cursor.execute(
                "UPDATE sne.ics SET observacion = %s WHERE id_ics = %s",
                (observacion, id_ics),
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