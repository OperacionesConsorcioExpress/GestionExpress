import time
import psycopg2
import openpyxl
import csv
import json
from datetime import datetime, date
from io import StringIO, BytesIO
from typing import List
from fastapi import HTTPException
import pytz

# ReportLab para generación de PDF
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
# Importar la función de conexión a la base de datos
from database.database_manager import get_db_connection

# Zona horaria Colombia
colombia_tz = pytz.timezone('America/Bogota')

# =====================================================================
# CLASE: Cargue_Controles
# =====================================================================
class Cargue_Controles:
    def __init__(self):
        pass

    def borrar_tablas(self, conn, cursor, tablas_a_borrar):
        try:
            if 'planta' in tablas_a_borrar:
                cursor.execute('DELETE FROM planta')
            if 'supervisores' in tablas_a_borrar:
                cursor.execute('DELETE FROM supervisores')
            if 'turnos' in tablas_a_borrar:
                cursor.execute('DELETE FROM turnos')
            if 'controles' in tablas_a_borrar:
                cursor.execute('DELETE FROM controles')
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error al borrar las tablas: {str(e)}")
            raise

    def cargar_datos(self, data):
        # Crear una lista de las tablas a borrar según las hojas seleccionadas
        tablas_a_borrar = []
        if 'planta' in data:
            tablas_a_borrar.append('planta')
        if 'supervisores' in data:
            tablas_a_borrar.append('supervisores')
        if 'turnos' in data:
            tablas_a_borrar.append('turnos')
        if 'controles' in data:
            tablas_a_borrar.append('controles')

        print("Iniciando la carga de datos...")

        # Una sola conexión para toda la operación de carga
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Borrar solo las tablas seleccionadas
                self.borrar_tablas(conn, cursor, tablas_a_borrar)

                if 'planta' in data:
                    self._cargar_planta(conn, cursor, data['planta'])
                if 'supervisores' in data:
                    self._cargar_supervisores(conn, cursor, data['supervisores'])
                if 'turnos' in data:
                    self._cargar_turnos(conn, cursor, data['turnos'])
                if 'controles' in data:
                    self._cargar_controles(data['controles'])

        print("Datos cargados exitosamente.")

    def _cargar_planta(self, conn, cursor, planta_data):
        try:
            for row in planta_data:
                print(f"Insertando o actualizando en planta: {row}")
                cursor.execute('''
                    INSERT INTO planta (cedula, nombre) VALUES (%s, %s)
                    ON CONFLICT(cedula) DO UPDATE SET nombre=excluded.nombre
                ''', (row['cedula'], row['nombre']))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error al cargar los datos de planta: {str(e)}")
            raise

    def _cargar_supervisores(self, conn, cursor, supervisores_data):
        try:
            for row in supervisores_data:
                print(f"Insertando o actualizando en supervisores: {row}")
                cursor.execute('''
                    INSERT INTO supervisores (cedula, nombre) VALUES (%s, %s)
                    ON CONFLICT(cedula) DO UPDATE SET nombre=excluded.nombre
                ''', (row['cedula'], row['nombre']))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error al cargar los datos de supervisores: {str(e)}")
            raise

    def _cargar_turnos(self, conn, cursor, turnos_data):
        try:
            for row in turnos_data:
                print(f"Insertando o actualizando en turnos: {row}")
                cursor.execute('''
                    INSERT INTO turnos (turno, hora_inicio, hora_fin, detalles)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(turno) DO UPDATE SET hora_inicio = excluded.hora_inicio, hora_fin = excluded.hora_fin, detalles = excluded.detalles
                ''', (str(row['turno']), str(row['hora_inicio']), str(row['hora_fin']), str(row['detalles'])))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error al cargar los datos de turnos: {str(e)}")
            raise

    def _cargar_controles(self, controles_data):
        batch_size = 450  # Tamaño del lote
        retries = 10      # Número de reintentos
        delay = 1         # Retraso entre reintentos en segundos

        for i in range(0, len(controles_data), batch_size):
            batch = controles_data[i:i + batch_size]
            for _ in range(retries):
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            for row in batch:
                                print(f"Insertando o actualizando en controles: {row}")
                                cursor.execute('''
                                    INSERT INTO controles (concesion, puestos, control, ruta, linea, admin, cop, tablas)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT(id) DO UPDATE SET concesion = excluded.concesion, puestos = excluded.puestos, ruta = excluded.ruta, linea = excluded.linea, admin = excluded.admin, cop = excluded.cop, tablas = excluded.tablas
                                ''', (row['concesion'], row['puestos'], row['control'], row['ruta'], row['linea'], row['admin'], row['cop'], row['tablas']))
                        conn.commit()
                    break  # Salir del ciclo de reintentos si todo es exitoso

                except psycopg2.OperationalError as e:
                    print(f"Error de conexión: {str(e)}, reintentando...")
                    time.sleep(delay)  # Esperar antes de reintentar

                except Exception as e:
                    print(f"Error al cargar los datos de controles: {str(e)}")
                    raise

            else:
                raise psycopg2.OperationalError("No se pudo desbloquear la base de datos después de varios intentos")                 

# =====================================================================
# CLASE: Cargue_Asignaciones
# =====================================================================
class Cargue_Asignaciones:
    def __init__(self):
        pass

    def procesar_asignaciones(self, assignments, user_session):
        processed_data = []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Recupera todos los controles en una sola consulta y los guarda en un diccionario
                    cursor.execute("SELECT ruta, linea, cop FROM controles")
                    controles = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

            for asignacion in assignments:
                rutas = asignacion['rutas_asociadas'].split(',')
                for ruta in rutas:
                    if asignacion['turno'] in ["AUSENCIA", "DESCANSO", "VACACIONES", "OTRAS TAREAS"]:
                        concesion = control = linea = cop = "NO APLICA"
                        ruta = "NO APLICA"
                    else:
                        # Obtén la información del control de la ruta en el diccionario
                        linea, cop = controles.get(ruta.strip(), ("", ""))
                        concesion = asignacion['concesion']
                        control = asignacion['control']

                    processed_data.append({
                        'fecha': asignacion['fecha'],
                        'cedula': asignacion['cedula'],
                        'nombre': asignacion['nombre'],
                        'turno': asignacion['turno'],
                        'h_inicio': asignacion['hora_inicio'],
                        'h_fin': asignacion['hora_fin'],
                        'concesion': concesion,
                        'control': control,
                        'ruta': ruta.strip() if asignacion['turno'] not in ["AUSENCIA", "DESCANSO", "VACACIONES", "OTRAS TAREAS"] else "NO APLICA",
                        'linea': linea,
                        'cop': cop,
                        'observaciones': asignacion['observaciones'],
                        'usuario_registra': user_session['username'],
                        'registrado_por': f"{user_session['nombres']} {user_session['apellidos']}",
                        'fecha_hora_registro': datetime.now(colombia_tz).strftime("%d-%m-%Y %H:%M:%S"),
                        'puestosSC': int(asignacion.get('puestosSC', 0)),
                        'puestosUQ': int(asignacion.get('puestosUQ', 0)),
                        'cedula_enlace': asignacion['cedula_enlace'],
                        'nombre_supervisor_enlace': asignacion['nombre_supervisor_enlace']
                    })

        except psycopg2.Error as e:
            print(f"Error al procesar asignaciones: {e}")
        return processed_data

    def cargar_asignaciones(self, processed_data):
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Fase de eliminación en lote (en lugar de hacerlo uno por uno)
                    delete_conditions = [(data['fecha'], data['cedula'], data['turno'], data['h_inicio'], data['h_fin'], data['control'])
                                        for data in processed_data]
                    cursor.executemany('''
                        DELETE FROM asignaciones
                        WHERE fecha = %s AND cedula = %s AND turno = %s AND h_inicio = %s AND h_fin = %s AND control = %s
                    ''', delete_conditions)

                    print(f"Registros eliminados para las asignaciones.")

                    # Fase de inserción en lote
                    insert_values = [
                        (data['fecha'], data['cedula'], data['nombre'], data['turno'], data['h_inicio'], data['h_fin'],
                         data['concesion'], data['control'], ruta.strip(), data['linea'], data['cop'], data['observaciones'],
                         data['usuario_registra'], data['registrado_por'], data['fecha_hora_registro'], data['puestosSC'],
                         data['puestosUQ'], data['cedula_enlace'], data['nombre_supervisor_enlace'])
                        for data in processed_data for ruta in data['ruta'].split(',')
                    ]

                    cursor.executemany('''
                        INSERT INTO asignaciones (fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, ruta, linea, cop, observaciones, usuario_registra, registrado_por, fecha_hora_registro, puestosSC, puestosUQ, cedula_enlace, nombre_supervisor_enlace)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', insert_values)

                conn.commit()
            return {"status": "success", "message": "Asignaciones guardadas exitosamente."}

        except psycopg2.Error as e:
            print(f"Error al guardar asignaciones: {e}")
            raise HTTPException(status_code=500, detail=f"Error al guardar asignaciones: {e}")

# =====================================================================
# CLASE: Reporte_Asignaciones
# =====================================================================
class Reporte_Asignaciones:
    def __init__(self):
        pass

    def obtener_asignaciones(self, fecha_inicio, fecha_fin, cedula=None, nombre=None, turno=None, concesion=None, control=None, ruta=None, linea=None, cop=None, registrado_por=None, nombre_supervisor_enlace=None):
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, ruta, linea, cop, observaciones, registrado_por, fecha_hora_registro, cedula_enlace, nombre_supervisor_enlace
            FROM asignaciones
            WHERE fecha BETWEEN %s AND %s
        """
        params = [fecha_inicio, fecha_fin]

        if cedula:
            query += " AND cedula = %s"
            params.append(cedula)
        if nombre:
            query += " AND nombre = %s"
            params.append(nombre)
        if turno:
            query += " AND turno = %s"
            params.append(turno)
        if concesion:
            query += " AND concesion = %s"
            params.append(concesion)
        if control:
            query += " AND control = %s"
            params.append(control)
        if ruta:
            query += " AND ruta = %s"
            params.append(ruta)
        if linea:
            query += " AND linea = %s"
            params.append(linea)
        if cop:
            query += " AND cop = %s"
            params.append(cop)
        if registrado_por:
            query += " AND registrado_por = %s"
            params.append(registrado_por)
        if nombre_supervisor_enlace:
            query += " AND nombre_supervisor_enlace = %s"
            params.append(nombre_supervisor_enlace)

        query += " ORDER BY fecha ASC"

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, tuple(params))
                    resultados = cursor.fetchall()

            # Transformar las filas en diccionarios (fuera de la conexión)
            resultado = []
            for row in resultados:
                resultado.append({
                    'fecha': row[0].strftime("%Y-%m-%d") if isinstance(row[0], (date, datetime)) else row[0],
                    'cedula': row[1],
                    'nombre': row[2],
                    'turno': row[3],
                    'h_inicio': row[4].strftime("%H:%M:%S") if isinstance(row[4], (datetime,)) else row[4],
                    'h_fin': row[5].strftime("%H:%M:%S") if isinstance(row[5], (datetime,)) else row[5],
                    'concesion': row[6],
                    'control': row[7],
                    'ruta': row[8],
                    'linea': row[9],
                    'cop': row[10],
                    'observaciones': row[11],
                    'registrado_por': row[12],
                    'fecha_hora_registro': row[13].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[13], (datetime, date)) else row[13],
                    'cedula_enlace': row[14],
                    'nombre_supervisor_enlace': row[15],
                })

            return resultado

        except psycopg2.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return []

    def obtener_filtros_unicos(self):
        filtros = {
            "cedulas": [],
            "nombres": [],
            "turnos": [],
            "concesiones": [],
            "controles": [],
            "rutas": [],
            "lineas": [],
            "cops": [],
            "registrado": [],
            "supervisores_enlace": []
        }

        query_templates = {
            "cedulas": "SELECT DISTINCT cedula FROM asignaciones ORDER BY cedula",
            "nombres": "SELECT DISTINCT nombre FROM asignaciones ORDER BY nombre",
            "turnos": "SELECT DISTINCT turno FROM asignaciones ORDER BY turno",
            "concesiones": "SELECT DISTINCT concesion FROM asignaciones ORDER BY concesion",
            "controles": "SELECT DISTINCT control FROM asignaciones ORDER BY control",
            "rutas": "SELECT DISTINCT ruta FROM asignaciones ORDER BY ruta",
            "lineas": "SELECT DISTINCT linea FROM asignaciones ORDER BY linea",
            "cops": "SELECT DISTINCT cop FROM asignaciones ORDER BY cop",
            "registrado": "SELECT DISTINCT registrado_por FROM asignaciones ORDER BY registrado_por",
            "supervisores_enlace": "SELECT DISTINCT nombre_supervisor_enlace FROM asignaciones ORDER BY nombre_supervisor_enlace"
        }

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for key, query in query_templates.items():
                        cursor.execute(query)
                        filtros[key] = [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error al consultar los filtros únicos: {e}")

        return filtros

    def generar_xlsx(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Asignaciones"
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro", "Cedula Enlace", "Nombre Supervisor Enlace"]
        ws.append(headers)
        
        for asignacion in asignaciones:
            ws.append([
                asignacion['fecha'],
                asignacion['cedula'],
                asignacion['nombre'],
                asignacion['turno'],
                asignacion['h_inicio'],
                asignacion['h_fin'],
                asignacion['concesion'],
                asignacion['control'],
                asignacion['ruta'],
                asignacion['linea'],
                asignacion['cop'],
                asignacion['observaciones'],
                asignacion['registrado_por'],
                asignacion['fecha_hora_registro'],
                asignacion['cedula_enlace'],
                asignacion['nombre_supervisor_enlace']
            ])
        
        stream = BytesIO()
        wb.save(stream)
        stream.seek(0)
        
        return stream
    
    def generar_csv(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        output = StringIO()
        writer = csv.writer(output)
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro", "Cedula Enlace", "Nombre Supervisor Enlace"]
        writer.writerow(headers)
        
        for asignacion in asignaciones:
            writer.writerow([
                asignacion['fecha'],
                asignacion['cedula'],
                asignacion['nombre'],
                asignacion['turno'],
                asignacion['h_inicio'],
                asignacion['h_fin'],
                asignacion['concesion'],
                asignacion['control'],
                asignacion['ruta'],
                asignacion['linea'],
                asignacion['cop'],
                asignacion['observaciones'],
                asignacion['registrado_por'],
                asignacion['fecha_hora_registro'],
                asignacion['cedula_enlace'],
                asignacion['nombre_supervisor_enlace']
            ])
        
        output.seek(0)
        return output

    def generar_json(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        return json.dumps(asignaciones, ensure_ascii=False, indent=4)

    ###### FUNCIONES PARA CONSULTAR DE LA BASE DE DATOS Y TRAER LAS ASIGNACIONES AL FRONTEND ####### 
    def obtener_asignacion_por_fecha(self, fecha, concesion, fecha_hora_registro):
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, 
                STRING_AGG(ruta, ','), linea, cop, observaciones, puestosSC, puestosUQ, 
                fecha_hora_registro
            FROM asignaciones
            WHERE fecha = %s AND concesion = %s AND fecha_hora_registro = %s
            GROUP BY fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, linea, cop, 
                    observaciones, puestosSC, puestosUQ, fecha_hora_registro
            ORDER BY fecha_hora_registro DESC
        """
        params = (fecha, concesion, fecha_hora_registro)

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    resultados = cursor.fetchall()

            # Devolver una lista de asignaciones (fuera de la conexión)
            asignaciones = []
            for resultado in resultados:
                asignaciones.append({
                    'fecha': resultado[0].strftime("%Y-%m-%d") if isinstance(resultado[0], (date, datetime)) else resultado[0],
                    'cedula': resultado[1],
                    'nombre': resultado[2],
                    'turno': resultado[3],
                    'h_inicio': resultado[4].strftime("%H:%M:%S") if isinstance(resultado[4], (datetime,)) else resultado[4],
                    'h_fin': resultado[5].strftime("%H:%M:%S") if isinstance(resultado[5], (datetime,)) else resultado[5],
                    'concesion': resultado[6],
                    'control': resultado[7],
                    'ruta': resultado[8],
                    'linea': resultado[9],
                    'cop': resultado[10],
                    'observaciones': resultado[11],
                    'puestosSC': resultado[12],
                    'puestosUQ': resultado[13],
                    'fecha_hora_registro': resultado[14].strftime("%Y-%m-%d %H:%M:%S") if isinstance(resultado[14], (date, datetime)) else resultado[14]
                })

            return asignaciones
        except psycopg2.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

    def obtener_concesiones_unicas_por_fecha(self, fecha):
        query = """
            SELECT DISTINCT concesion
            FROM asignaciones
            WHERE fecha = %s
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (fecha,))
                    concesiones = cursor.fetchall()

            if concesiones:
                return [concesion[0] for concesion in concesiones]
            return None

        except psycopg2.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

    def obtener_fechas_horas_registro(self, fecha, concesion):
        query = """
            SELECT DISTINCT fecha_hora_registro
            FROM asignaciones
            WHERE fecha = %s AND concesion = %s
            ORDER BY fecha_hora_registro DESC
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (fecha, concesion))
                    resultados = cursor.fetchall()

            return [resultado[0] for resultado in resultados]

        except psycopg2.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

    ###### FUNCIONES PARA CONSULTAR DE LA BASE DE DATOS Y TRAER LAS ASIGNACIONES EN PDF ####### 
    def generar_pdf(self, asignaciones, fecha_asignacion, fecha_hora_registro, pdf_buffer):
        # Usar el buffer de memoria en vez de escribir en un archivo
        doc = SimpleDocTemplate(pdf_buffer, pagesize=A4)
        elements = []
        styles = getSampleStyleSheet()

        # Añadir título y subtítulo
        titulo = Paragraph(f"Informe de Asignaciones Centro de Control para el {fecha_asignacion}", styles['Title'])
        subtitulo = Paragraph(f"Actualización: {fecha_hora_registro}", styles['Normal'])
        elements.append(titulo)
        elements.append(subtitulo)
        elements.append(Spacer(1, 12))

        # Agrupar asignaciones por técnico
        asignaciones_por_tecnico = {}
        for asignacion in asignaciones:
            nombre_tecnico = asignacion.nombre
            if nombre_tecnico not in asignaciones_por_tecnico:
                asignaciones_por_tecnico[nombre_tecnico] = {
                    'datos_tecnico': asignacion,
                    'rutas': []
                }
            asignaciones_por_tecnico[nombre_tecnico]['rutas'].append((asignacion.ruta, asignacion.cop, asignacion.linea))

        # Dividir técnicos en grupos de 2 para hacer una distribución en columnas
        tecnicos = list(asignaciones_por_tecnico.items())
        for i in range(0, len(tecnicos), 2):
            fila = tecnicos[i:i+2]
            for nombre_tecnico, datos_tecnico in fila:
                # Información del técnico
                tecnico_info = Paragraph(
                    f"<b>Técnico:</b> {nombre_tecnico}<br/><b>Cédula:</b> {datos_tecnico['datos_tecnico'].cedula}<br/>"
                    f"<b>Puesto de Trabajo:</b> {datos_tecnico['datos_tecnico'].control}<br/><b>Turno:</b> {datos_tecnico['datos_tecnico'].turno}",
                    styles['Normal']
                )
                elements.append(tecnico_info)
                elements.append(Spacer(1, 6))

                # Crear una tabla con las rutas del técnico
                data = [['Ruta', 'COP', 'Línea']]
                for ruta, cop, linea in datos_tecnico['rutas']:
                    data.append([ruta, cop, linea])

                # Crear la tabla de rutas
                table = Table(data, colWidths=[0.7 * inch, 1 * inch, 0.7 * inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 2),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                elements.append(table)
                elements.append(Spacer(1, 12))

        # Generar el PDF en el buffer en vez de en un archivo
        doc.build(elements)