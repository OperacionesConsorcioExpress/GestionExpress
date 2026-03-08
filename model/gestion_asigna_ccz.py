import time, psycopg2, openpyxl, csv, json, pytz
from datetime import datetime, date, timedelta
from io import StringIO, BytesIO
from typing import List, Optional
from fastapi import HTTPException
# ReportLab
from collections import OrderedDict
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
# Base de datos
from database.database_manager import get_db_connection

# Zona horaria Colombia
colombia_tz = pytz.timezone('America/Bogota')

# Turnos que no requieren control/ruta
_TURNOS_NO_APLICA = {"AUSENCIA", "DESCANSO", "VACACIONES", "OTRAS TAREAS"}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_fecha(v) -> str:
    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d")
    return str(v) if v is not None else ""

def _fmt_hora(v) -> str:
    if isinstance(v, datetime):
        return v.strftime("%H:%M:%S")
    if isinstance(v, timedelta):
        total = int(v.total_seconds())
        h, rem = divmod(total, 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    return str(v) if v is not None else ""

def _fmt_datetime(v) -> str:
    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v) if v is not None else ""

def _row_to_asignacion(row) -> dict:
    """
    Convierte una fila de la tabla 'asignaciones' a dict.
    Índices:
        0:fecha  1:cedula  2:nombre  3:turno  4:h_inicio  5:h_fin
        6:concesion  7:control  8:ruta  9:linea  10:cop
        11:observaciones  12:registrado_por  13:fecha_hora_registro
        14:cedula_enlace  15:nombre_supervisor_enlace
    """
    return {
        'fecha':                    _fmt_fecha(row[0]),
        'cedula':                   row[1],
        'nombre':                   row[2],
        'turno':                    row[3],
        'h_inicio':                 _fmt_hora(row[4]),
        'h_fin':                    _fmt_hora(row[5]),
        'concesion':                row[6],
        'control':                  row[7],
        'ruta':                     row[8],
        'linea':                    row[9],
        'cop':                      row[10],
        'observaciones':            row[11],
        'registrado_por':           row[12],
        'fecha_hora_registro':      _fmt_datetime(row[13]),
        'cedula_enlace':            row[14],
        'nombre_supervisor_enlace': row[15],
    }

# =====================================================================
# CLASE: Cargue_Controles
# =====================================================================
class Cargue_Controles:

    # ── Borrado de tablas ────────────────────────────────────────────
    def borrar_tablas(self, conn, cursor, tablas_a_borrar: list):
        """Elimina el contenido de las tablas seleccionadas en un solo commit."""
        try:
            orden = ['planta', 'supervisores', 'turnos', 'controles']
            for tabla in orden:
                if tabla in tablas_a_borrar:
                    cursor.execute(f'DELETE FROM {tabla}')
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error al borrar tablas: {e}")
            raise

    # ── Punto de entrada principal ───────────────────────────────────
    def cargar_datos(self, data: dict):
        """
        Carga las hojas seleccionadas en una SOLA conexión compartida.
        _cargar_controles() también usa esta misma conexión — ya no abre
        conexiones adicionales por lote.
        """
        tablas_a_borrar = [t for t in ['planta', 'supervisores', 'turnos', 'controles']
                            if t in data]

        print(f"[Cargue_Controles] Iniciando carga: {tablas_a_borrar}")

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                self.borrar_tablas(conn, cursor, tablas_a_borrar)

                if 'planta' in data:
                    self._cargar_planta(conn, cursor, data['planta'])
                if 'supervisores' in data:
                    self._cargar_supervisores(conn, cursor, data['supervisores'])
                if 'turnos' in data:
                    self._cargar_turnos(conn, cursor, data['turnos'])
                if 'controles' in data:
                    # Pasamos conn + cursor para NO abrir nueva conexión
                    self._cargar_controles(conn, cursor, data['controles'])

        print("[Cargue_Controles] Carga completada exitosamente.")

    # ── Carga planta ─────────────────────────────────────────────────
    def _cargar_planta(self, conn, cursor, planta_data: list):
        try:
            valores = [(r['cedula'], r['nombre']) for r in planta_data]
            cursor.executemany(
                """
                INSERT INTO planta (cedula, nombre) VALUES (%s, %s)
                ON CONFLICT (cedula) DO UPDATE SET nombre = EXCLUDED.nombre
                """,
                valores,
            )
            conn.commit()
            print(f"[Cargue_Controles] Planta: {len(valores)} registros cargados.")
        except Exception as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error en planta: {e}")
            raise

    # ── Carga supervisores ───────────────────────────────────────────
    def _cargar_supervisores(self, conn, cursor, supervisores_data: list):
        try:
            valores = [(r['cedula'], r['nombre']) for r in supervisores_data]
            cursor.executemany(
                """
                INSERT INTO supervisores (cedula, nombre) VALUES (%s, %s)
                ON CONFLICT (cedula) DO UPDATE SET nombre = EXCLUDED.nombre
                """,
                valores,
            )
            conn.commit()
            print(f"[Cargue_Controles] Supervisores: {len(valores)} registros cargados.")
        except Exception as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error en supervisores: {e}")
            raise

    # ── Carga turnos ─────────────────────────────────────────────────
    def _cargar_turnos(self, conn, cursor, turnos_data: list):
        try:
            valores = [
                (str(r['turno']), str(r['hora_inicio']),
                    str(r['hora_fin']),  str(r['detalles']))
                for r in turnos_data
            ]
            cursor.executemany(
                """
                INSERT INTO turnos (turno, hora_inicio, hora_fin, detalles)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (turno) DO UPDATE
                    SET hora_inicio = EXCLUDED.hora_inicio,
                        hora_fin    = EXCLUDED.hora_fin,
                        detalles    = EXCLUDED.detalles
                """,
                valores,
            )
            conn.commit()
            print(f"[Cargue_Controles] Turnos: {len(valores)} registros cargados.")
        except Exception as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error en turnos: {e}")
            raise

    # ── Carga controles (lotes, misma conexión) ──────────────────────
    def _cargar_controles(self, conn, cursor, controles_data: list,
                            batch_size: int = 450):
        """
        Inserta controles en lotes usando la conexión del contexto padre.
        ON CONFLICT ahora usa la clave natural (concesion, puestos, control, ruta)
        en lugar de (id) que no funciona para nuevas filas sin id previo.

        Ya NO abre una nueva conexión por lote — elimina el overhead de
        N conexiones al pool que existía en la versión anterior.
        """
        total = len(controles_data)
        cargados = 0

        try:
            for i in range(0, total, batch_size):
                batch = controles_data[i : i + batch_size]
                valores = [
                    (r['concesion'], r['puestos'], r['control'], r['ruta'],
                        r['linea'],     r['admin'],   r['cop'],     r['tablas'])
                    for r in batch
                ]
                cursor.executemany(
                    """
                    INSERT INTO controles
                        (concesion, puestos, control, ruta, linea, admin, cop, tablas)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (concesion, puestos, control, ruta)
                    DO UPDATE SET
                        linea  = EXCLUDED.linea,
                        admin  = EXCLUDED.admin,
                        cop    = EXCLUDED.cop,
                        tablas = EXCLUDED.tablas
                    """,
                    valores,
                )
                conn.commit()
                cargados += len(batch)
                print(f"[Cargue_Controles] Controles: {cargados}/{total} registros.")

        except psycopg2.OperationalError as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error operacional en controles: {e}")
            raise
        except Exception as e:
            conn.rollback()
            print(f"[Cargue_Controles] Error en controles: {e}")
            raise

# =====================================================================
# CLASE: Cargue_Asignaciones
# =====================================================================
class Cargue_Asignaciones:

    def procesar_asignaciones(self, assignments: list, user_session: dict) -> list:
        """
        Transforma la lista de asignaciones del frontend en registros listos
        para INSERT. Carga el mapa ruta→(linea,cop) en una sola query.
        """
        processed_data = []
        ahora = datetime.now(colombia_tz).strftime("%d-%m-%Y %H:%M:%S")
        registrado_por = f"{user_session['nombres']} {user_session['apellidos']}"
        usuario_registra = user_session['username']

        try:
            # Cargar todos los controles una sola vez
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT ruta, linea, cop FROM controles")
                    mapa_controles = {row[0]: (row[1], row[2])
                                        for row in cursor.fetchall()}

            for asignacion in assignments:
                turno = asignacion['turno']
                es_no_aplica = turno in _TURNOS_NO_APLICA

                rutas_raw = asignacion['rutas_asociadas'].split(',')

                for ruta_raw in rutas_raw:
                    ruta = ruta_raw.strip()

                    if es_no_aplica:
                        concesion_val = control_val = linea_val = cop_val = "NO APLICA"
                        ruta = "NO APLICA"
                    else:
                        linea_val, cop_val = mapa_controles.get(ruta, ("", ""))
                        concesion_val = asignacion['concesion']
                        control_val   = asignacion['control']

                    processed_data.append({
                        'fecha':                    asignacion['fecha'],
                        'cedula':                   asignacion['cedula'],
                        'nombre':                   asignacion['nombre'],
                        'turno':                    turno,
                        'h_inicio':                 asignacion['hora_inicio'],
                        'h_fin':                    asignacion['hora_fin'],
                        'concesion':                concesion_val,
                        'control':                  control_val,
                        'ruta':                     ruta,
                        'linea':                    linea_val,
                        'cop':                      cop_val,
                        'observaciones':            asignacion['observaciones'],
                        'usuario_registra':         usuario_registra,
                        'registrado_por':           registrado_por,
                        'fecha_hora_registro':      ahora,
                        'puestosSC':                int(asignacion.get('puestosSC', 0)),
                        'puestosUQ':                int(asignacion.get('puestosUQ', 0)),
                        'cedula_enlace':             asignacion['cedula_enlace'],
                        'nombre_supervisor_enlace': asignacion['nombre_supervisor_enlace'],
                    })

        except psycopg2.Error as e:
            print(f"[Cargue_Asignaciones] Error al procesar: {e}")

        return processed_data

    def cargar_asignaciones(self, processed_data: list) -> dict:
        if not processed_data:
            return {"status": "success", "message": "Sin datos para guardar."}

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:

                    # DELETE quirúrgico deduplicado: cada combinación única se borra 1 sola vez
                    delete_conditions = list({
                        (d['fecha'], d['cedula'], d['turno'],
                            d['h_inicio'], d['h_fin'], d['control'])
                        for d in processed_data
                    })
                    cursor.executemany(
                        """
                        DELETE FROM asignaciones
                        WHERE fecha = %s AND cedula = %s AND turno = %s
                            AND h_inicio = %s AND h_fin = %s AND control = %s
                        """,
                        delete_conditions,
                    )
                    print(f"[cargar_asignaciones] {len(delete_conditions)} combinaciones eliminadas.")

                    # INSERT — processed_data ya tiene 1 entrada por ruta (split hecho en procesar_asignaciones)
                    insert_values = [
                        (d['fecha'], d['cedula'], d['nombre'], d['turno'], d['h_inicio'], d['h_fin'],
                            d['concesion'], d['control'], d['ruta'], d['linea'], d['cop'], d['observaciones'],
                            d['usuario_registra'], d['registrado_por'], d['fecha_hora_registro'],
                            d['puestosSC'], d['puestosUQ'], d['cedula_enlace'], d['nombre_supervisor_enlace'])
                        for d in processed_data
                    ]
                    cursor.executemany(
                        """
                        INSERT INTO asignaciones
                            (fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control,
                                ruta, linea, cop, observaciones, usuario_registra, registrado_por,
                                fecha_hora_registro, puestosSC, puestosUQ, cedula_enlace, nombre_supervisor_enlace)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        insert_values,
                    )

                conn.commit()
            return {"status": "success", "message": "Asignaciones guardadas exitosamente."}

        except psycopg2.Error as e:
            print(f"[cargar_asignaciones] Error: {e}")
            raise HTTPException(status_code=500, detail=f"Error al guardar asignaciones: {e}")

# =====================================================================
# CLASE: Reporte_Asignaciones
# =====================================================================
class Reporte_Asignaciones:

    # ── Obtener asignaciones filtradas ───────────────────────────────
    def obtener_asignaciones(
        self,
        fecha_inicio,  fecha_fin,
        cedula=None,   nombre=None,      turno=None,
        concesion=None, control=None,    ruta=None,
        linea=None,    cop=None,
        registrado_por=None,             nombre_supervisor_enlace=None,
    ) -> list:

        query  = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin,
                    concesion, control, ruta, linea, cop, observaciones,
                    registrado_por, fecha_hora_registro,
                    cedula_enlace, nombre_supervisor_enlace
            FROM asignaciones
            WHERE fecha BETWEEN %s AND %s
        """
        params = [fecha_inicio, fecha_fin]

        filtros_opcionales = [
            (cedula,                    "cedula"),
            (nombre,                    "nombre"),
            (turno,                     "turno"),
            (concesion,                 "concesion"),
            (control,                   "control"),
            (ruta,                      "ruta"),
            (linea,                     "linea"),
            (cop,                       "cop"),
            (registrado_por,            "registrado_por"),
            (nombre_supervisor_enlace,  "nombre_supervisor_enlace"),
        ]
        for valor, columna in filtros_opcionales:
            if valor:
                query  += f" AND {columna} = %s"
                params.append(valor)

        query += " ORDER BY fecha ASC, nombre ASC"

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, tuple(params))
                    rows = cursor.fetchall()

            return [_row_to_asignacion(r) for r in rows]

        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_asignaciones: {e}")
            return []

    # ── Filtros únicos — OPTIMIZADO: 1 query en lugar de 10 ─────────
    def obtener_filtros_unicos(self) -> dict:
        """
        Obtiene todos los valores distintos para los filtros del dashboard
        en un SOLO round-trip usando UNION ALL con campo discriminador.

        Resultado: dict con 10 listas, igual que la versión anterior.
        """
        query = """
            SELECT 'cedulas'            AS tipo, cedula::text            AS valor FROM asignaciones
            UNION ALL
            SELECT 'nombres',                    nombre                           FROM asignaciones
            UNION ALL
            SELECT 'turnos',                     turno                            FROM asignaciones
            UNION ALL
            SELECT 'concesiones',                concesion                        FROM asignaciones
            UNION ALL
            SELECT 'controles',                  control                          FROM asignaciones
            UNION ALL
            SELECT 'rutas',                      ruta                             FROM asignaciones
            UNION ALL
            SELECT 'lineas',                     linea                            FROM asignaciones
            UNION ALL
            SELECT 'cops',                       cop                              FROM asignaciones
            UNION ALL
            SELECT 'registrado',                 registrado_por                   FROM asignaciones
            UNION ALL
            SELECT 'supervisores_enlace',        nombre_supervisor_enlace         FROM asignaciones
        """
        # Construimos sets para deduplicar en Python (más rápido que 10 DISTINCT)
        agrupado: dict = {
            k: set() for k in [
                "cedulas", "nombres", "turnos", "concesiones", "controles",
                "rutas", "lineas", "cops", "registrado", "supervisores_enlace",
            ]
        }

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    for tipo, valor in cursor.fetchall():
                        if valor is not None and tipo in agrupado:
                            agrupado[tipo].add(valor)
        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_filtros_unicos: {e}")

        # Ordenar y convertir sets a listas
        return {k: sorted(v) for k, v in agrupado.items()}

    # ── Consulta modal: concesiones + fechas_horas en 1 query ────────
    def obtener_datos_modal_consulta(self, fecha: str) -> dict:
        """
        NUEVO: retorna concesiones únicas Y sus fechas/horas de registro
        para una fecha dada, en una sola query.
        Reemplaza la cascada:
            1. obtener_concesiones_unicas_por_fecha(fecha)
            2. obtener_fechas_horas_registro(fecha, concesion)  ← por cada concesión
        El frontend ahora hace 1 solo POST /api/consulta_modal.
        """
        query = """
            SELECT DISTINCT concesion, fecha_hora_registro
            FROM asignaciones
            WHERE fecha = %s
            ORDER BY concesion, fecha_hora_registro DESC
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (fecha,))
                    rows = cursor.fetchall()

            # Agrupar: {concesion: [fecha_hora_1, fecha_hora_2, ...]}
            resultado: dict = {}
            for concesion_val, fhr in rows:
                fhr_str = _fmt_datetime(fhr)
                resultado.setdefault(concesion_val, []).append(fhr_str)

            return resultado

        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_datos_modal_consulta: {e}")
            return {}

    # ── Métodos de consulta originales (compatibilidad) ──────────────
    def obtener_asignacion_por_fecha(self, fecha: str, concesion: str,
                                        fecha_hora_registro: str) -> Optional[list]:
        """
        Devuelve una fila por técnico/control, agrupando todas sus rutas en un
        único registro. Esta corrección evita duplicados en frontend cuando en
        la base existe una fila por cada ruta asignada. También incluye datos
        de supervisor enlace y puestos para reconstrucción completa.
        """
        query = """
            SELECT
                fecha,
                cedula,
                nombre,
                turno,
                h_inicio,
                h_fin,
                concesion,
                control,
                STRING_AGG(ruta,  ', ' ORDER BY ruta) AS ruta,
                STRING_AGG(linea, ', ' ORDER BY ruta) AS linea,
                STRING_AGG(cop,   ', ' ORDER BY ruta) AS cop,
                MAX(observaciones) AS observaciones,
                MAX(puestosSC) AS puestosSC,
                MAX(puestosUQ) AS puestosUQ,
                fecha_hora_registro,
                MAX(cedula_enlace) AS cedula_enlace,
                MAX(nombre_supervisor_enlace) AS nombre_supervisor_enlace
            FROM asignaciones
            WHERE fecha = %s AND concesion = %s AND fecha_hora_registro = %s
            GROUP BY
                fecha, cedula, nombre, turno, h_inicio, h_fin,
                concesion, control, fecha_hora_registro
            ORDER BY nombre ASC, control ASC
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (fecha, concesion, fecha_hora_registro))
                    rows = cursor.fetchall()

            return [
                {
                    'fecha':               _fmt_fecha(r[0]),
                    'cedula':              r[1],
                    'nombre':              r[2],
                    'turno':               r[3],
                    'h_inicio':            _fmt_hora(r[4]),
                    'h_fin':               _fmt_hora(r[5]),
                    'concesion':           r[6],
                    'control':             r[7],
                    'ruta':                r[8],
                    'linea':               r[9],
                    'cop':                 r[10],
                    'observaciones':       r[11],
                    'puestosSC':           r[12],
                    'puestosUQ':           r[13],
                    'fecha_hora_registro': _fmt_datetime(r[14]),
                    'cedula_enlace':       r[15],
                    'nombre_supervisor_enlace': r[16],
                }
                for r in rows
            ] or None

        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_asignacion_por_fecha: {e}")
            return None

    def obtener_paquete_asignacion(self, fecha: str, concesion: str,
                                    fecha_hora_registro: str) -> Optional[dict]:
        """
        Consulta completa en un solo paquete: metadata de la versión
        seleccionada + filas de asignación ya agrupadas por técnico/control.
        """
        asignaciones = self.obtener_asignacion_por_fecha(fecha, concesion, fecha_hora_registro)
        if not asignaciones:
            return None

        primera = asignaciones[0]
        meta = {
            'fecha': _fmt_fecha(fecha),
            'concesion': concesion,
            'fecha_hora_registro': fecha_hora_registro,
            'puestosSC': primera.get('puestosSC') or 0,
            'puestosUQ': primera.get('puestosUQ') or 0,
            'cedula_enlace': primera.get('cedula_enlace') or '',
            'nombre_supervisor_enlace': primera.get('nombre_supervisor_enlace') or '',
            'total_tecnicos': len(asignaciones),
        }
        return {'meta': meta, 'asignaciones': asignaciones}

    def obtener_concesiones_unicas_por_fecha(self, fecha: str) -> Optional[list]:
        """Mantenido por compatibilidad con endpoints existentes."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT DISTINCT concesion FROM asignaciones "
                        "WHERE fecha = %s ORDER BY concesion",
                        (fecha,),
                    )
                    rows = cursor.fetchall()
            return [r[0] for r in rows] if rows else None
        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_concesiones: {e}")
            return None

    def obtener_fechas_horas_registro(self, fecha: str,
                                        concesion: str) -> Optional[list]:
        """Mantenido por compatibilidad con endpoints existentes."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT DISTINCT fecha_hora_registro FROM asignaciones "
                        "WHERE fecha = %s AND concesion = %s "
                        "ORDER BY fecha_hora_registro DESC",
                        (fecha, concesion),
                    )
                    rows = cursor.fetchall()
            return [r[0] for r in rows] if rows else None
        except psycopg2.Error as e:
            print(f"[Reporte_Asignaciones] Error en obtener_fechas_horas: {e}")
            return None

    def obtener_filtros_unicos_dashboard(self) -> dict:
        """Alias explícito para uso desde el router del dashboard."""
        return self.obtener_filtros_unicos()

    # ── Generación de reportes ────────────────────────────────────────
    def _filtros_a_kwargs(self, filtros: dict) -> dict:
        """Normaliza las claves del dict de filtros del frontend a kwargs."""
        return dict(
            fecha_inicio              = filtros.get('fechaInicio'),
            fecha_fin                 = filtros.get('fechaFin'),
            cedula                    = filtros.get('cedulaTecnico'),
            nombre                    = filtros.get('nombreTecnico'),
            turno                     = filtros.get('turno'),
            concesion                 = filtros.get('concesion'),
            control                   = filtros.get('control'),
            ruta                      = filtros.get('ruta'),
            linea                     = filtros.get('linea'),
            cop                       = filtros.get('cop'),
            registrado_por            = filtros.get('usuarioRegistra'),
            nombre_supervisor_enlace  = filtros.get('nombreSupervisorEnlace'),
        )

    _HEADERS_REPORTE = [
        "Fecha", "Cédula", "Nombre Técnico", "Turno",
        "Hora Inicio", "Hora Fin", "Concesión", "Control",
        "Ruta", "Línea", "COP", "Observaciones",
        "Usuario Registra", "Fecha de Registro",
        "Cedula Enlace", "Nombre Supervisor Enlace",
    ]

    def _asignacion_a_fila(self, a: dict) -> list:
        return [
            a['fecha'],        a['cedula'],     a['nombre'],
            a['turno'],        a['h_inicio'],   a['h_fin'],
            a['concesion'],    a['control'],    a['ruta'],
            a['linea'],        a['cop'],        a['observaciones'],
            a['registrado_por'], a['fecha_hora_registro'],
            a['cedula_enlace'], a['nombre_supervisor_enlace'],
        ]

    def generar_xlsx(self, filtros: dict) -> BytesIO:
        asignaciones = self.obtener_asignaciones(**self._filtros_a_kwargs(filtros))
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Asignaciones"
        ws.append(self._HEADERS_REPORTE)
        for a in asignaciones:
            ws.append(self._asignacion_a_fila(a))
        stream = BytesIO()
        wb.save(stream)
        stream.seek(0)
        return stream

    def generar_csv(self, filtros: dict) -> StringIO:
        asignaciones = self.obtener_asignaciones(**self._filtros_a_kwargs(filtros))
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(self._HEADERS_REPORTE)
        for a in asignaciones:
            writer.writerow(self._asignacion_a_fila(a))
        output.seek(0)
        return output

    def generar_json(self, filtros: dict) -> str:
        asignaciones = self.obtener_asignaciones(**self._filtros_a_kwargs(filtros))
        return json.dumps(asignaciones, ensure_ascii=False, indent=4)

    # ── Generación de PDF (mejorado visualmente y con ajuste de texto) ──
    def generar_pdf(self, asignaciones, fecha_asignacion, fecha_hora_registro, pdf_buffer):
        def split_detalle(valor):
            if valor is None:
                return []
            if isinstance(valor, list):
                return [str(x).strip() for x in valor if str(x).strip()]
            texto = str(valor).strip()
            if not texto:
                return []
            return [x.strip() for x in texto.split(',') if x.strip()]

        def obtener_attr(obj, *nombres, default=''):
            for nombre in nombres:
                if hasattr(obj, nombre):
                    valor = getattr(obj, nombre)
                    if valor:  # ignora None y strings vacíos para pasar al siguiente candidato
                        return valor
            return default

        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=A4,
            leftMargin=18,
            rightMargin=18,
            topMargin=22,
            bottomMargin=18
        )

        elements = []
        styles = getSampleStyleSheet()

        titulo_style = ParagraphStyle(
            'TituloAsignaciones',
            parent=styles['Title'],
            fontName='Helvetica-Bold',
            fontSize=15,
            leading=18,
            textColor=colors.HexColor('#123B70'),
            alignment=TA_LEFT,
            spaceAfter=4,
        )

        subtitulo_style = ParagraphStyle(
            'SubtituloAsignaciones',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8.5,
            leading=11,
            textColor=colors.HexColor('#475569'),
            alignment=TA_LEFT,
            spaceAfter=10,
        )

        info_style = ParagraphStyle(
            'InfoTecnico',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8.5,
            leading=11,
            textColor=colors.HexColor('#111827'),
            alignment=TA_LEFT,
        )

        header_style = ParagraphStyle(
            'HeaderTabla',
            parent=styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=8,
            leading=10,
            textColor=colors.white,
            alignment=TA_CENTER,
        )

        cell_style = ParagraphStyle(
            'CellTabla',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=7.7,
            leading=9,
            textColor=colors.HexColor('#111827'),
            alignment=TA_LEFT,
            wordWrap='CJK'
        )

        elements.append(Paragraph(
            f"Informe de Asignaciones Centro de Control para el {fecha_asignacion}",
            titulo_style
        ))
        elements.append(Paragraph(
            f"Actualización: {fecha_hora_registro}",
            subtitulo_style
        ))
        elements.append(Spacer(1, 8))

        asignaciones_por_tecnico = OrderedDict()

        for asignacion in asignaciones:
            nombre_tecnico = obtener_attr(asignacion, 'nombre', default='No informado') or 'No informado'
            cedula = obtener_attr(asignacion, 'cedula', default='') or ''
            control = obtener_attr(asignacion, 'control', default='') or ''
            clave = f"{cedula}||{nombre_tecnico}||{control}"

            if clave not in asignaciones_por_tecnico:
                asignaciones_por_tecnico[clave] = {
                    'datos_tecnico': asignacion,
                    'rutas': []
                }

            rutas_raw = obtener_attr(asignacion, 'rutas_detalle', 'ruta', default='') or ''
            cops_raw = obtener_attr(asignacion, 'cops_detalle', 'cop', default='') or ''
            lineas_raw = obtener_attr(asignacion, 'lineas_detalle', 'linea', default='') or ''

            rutas = split_detalle(rutas_raw)
            cops = split_detalle(cops_raw)
            lineas = split_detalle(lineas_raw)

            max_len = max(len(rutas), len(cops), len(lineas), 1)

            rutas += [''] * (max_len - len(rutas))
            cops += [''] * (max_len - len(cops))
            lineas += [''] * (max_len - len(lineas))

            for i in range(max_len):
                asignaciones_por_tecnico[clave]['rutas'].append(
                    (rutas[i], cops[i], lineas[i])
                )

        for _, datos_tecnico in asignaciones_por_tecnico.items():
            datos = datos_tecnico['datos_tecnico']
            rutas = datos_tecnico['rutas']

            nombre = obtener_attr(datos, 'nombre', default='No informado') or 'No informado'
            cedula = obtener_attr(datos, 'cedula', default='No informado') or 'No informado'
            control = obtener_attr(datos, 'control', default='No informado') or 'No informado'
            turno = obtener_attr(datos, 'turno', default='No informado') or 'No informado'
            h_inicio = obtener_attr(datos, 'h_inicio', 'hinicio', default='No informado') or 'No informado'
            h_fin = obtener_attr(datos, 'h_fin', 'hfin', default='No informado') or 'No informado'
            concesion = obtener_attr(datos, 'concesion', default='No informado') or 'No informado'
            observaciones = obtener_attr(datos, 'observaciones', default='Sin observaciones') or 'Sin observaciones'
            supervisor_enlace = obtener_attr(datos, 'nombre_supervisor_enlace', 'supervisor_enlace', default='No informado') or 'No informado'

            header_data = [
                [
                    Paragraph(f"<b>Técnico:</b> {nombre}", info_style),
                    Paragraph(f"<b>Cédula:</b> {cedula}", info_style),
                ],
                [
                    Paragraph(f"<b>Turno:</b> {turno}", info_style),
                    Paragraph(f"<b>Horario:</b> {h_inicio} - {h_fin}", info_style),
                ],
                [
                    Paragraph(f"<b>Concesión:</b> {concesion}", info_style),
                    Paragraph(f"<b>Control:</b> {control}", info_style),
                ],
                [
                    Paragraph(f"<b>Observaciones:</b> {observaciones}", info_style),
                    Paragraph(f"<b>Supervisor enlace:</b> {supervisor_enlace}", info_style),
                ],
            ]

            header_table = Table(header_data, colWidths=[3.85 * inch, 3.85 * inch])
            header_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#F8FAFC')),
                ('BOX', (0, 0), (-1, -1), 0.7, colors.HexColor('#CBD5E1')),
                ('INNERGRID', (0, 0), (-1, -1), 0.45, colors.HexColor('#CBD5E1')),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 6))

            data = [[
                Paragraph("Ruta", header_style),
                Paragraph("COP", header_style),
                Paragraph("Línea", header_style)
            ]]

            for ruta, cop, linea in rutas:
                data.append([
                    Paragraph(str(ruta or ''), cell_style),
                    Paragraph(str(cop or ''), cell_style),
                    Paragraph(str(linea or ''), cell_style),
                ])

            table = Table(data, colWidths=[3.6 * inch, 2.05 * inch, 2.25 * inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#123B70')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.55, colors.HexColor('#CBD5E1')),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 7),
                ('RIGHTPADDING', (0, 0), (-1, -1), 7),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ]))

            elements.append(table)
            elements.append(Spacer(1, 10))

        doc.build(elements)