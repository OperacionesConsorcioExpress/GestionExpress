import sqlite3
from datetime import datetime
import openpyxl
from io import StringIO, BytesIO
import csv
import json

class Reporte_Asignaciones:
    def __init__(self, db_path="./centro_control.db"):
        self.db_path = db_path

    def obtener_asignaciones(self, fecha_inicio, fecha_fin, cedula=None, nombre=None, turno=None, concesion=None, control=None, ruta=None, linea=None, cop=None, registrado_por=None):
        print(f"Filtros recibidos: {fecha_inicio}, {fecha_fin}, {cedula}, {nombre}, {turno}, {concesion}, {control}, {ruta}, {linea}, {cop}, {registrado_por}")
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, ruta, linea, cop, observaciones, registrado_por, fecha_hora_registro
            FROM asignaciones
            WHERE fecha BETWEEN ? AND ?
        """
        params = [fecha_inicio, fecha_fin]

        if cedula:
            query += " AND cedula = ?"
            params.append(cedula)
        if nombre:
            query += " AND nombre = ?"
            params.append(nombre)
        if turno:
            query += " AND turno = ?"
            params.append(turno)
        if concesion:
            query += " AND concesion = ?"
            params.append(concesion)
        if control:
            query += " AND control = ?"
            params.append(control)
        if ruta:
            query += " AND ruta = ?"
            params.append(ruta)
        if linea:
            query += " AND linea = ?"
            params.append(linea)
        if cop:
            query += " AND cop = ?"
            params.append(cop)
        if registrado_por:
            query += " AND registrado_por = ?"
            params.append(registrado_por)

        query += " ORDER BY fecha ASC"

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            resultados = cursor.fetchall()
            conn.close()
            
            # Transformar las filas en diccionarios
            resultado = []
            for row in resultados:
                resultado.append({
                    'fecha': row[0],
                    'cedula': row[1],
                    'nombre': row[2],
                    'turno': row[3],
                    'h_inicio': row[4],
                    'h_fin': row[5],
                    'concesion': row[6],
                    'control': row[7],
                    'ruta': row[8],
                    'linea': row[9],
                    'cop': row[10],
                    'observaciones': row[11],
                    'registrado_por': row[12],
                    'fecha_hora_registro': row[13]
                })
            
            return resultado

        except sqlite3.Error as e:
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
            "registrado": []
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
            "registrado": "SELECT DISTINCT registrado_por FROM asignaciones ORDER BY registrado_por"
        }

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            for key, query in query_templates.items():
                cursor.execute(query)
                filtros[key] = [row[0] for row in cursor.fetchall()]
            conn.close()
        except sqlite3.Error as e:
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
            filtros.get('usuarioRegistra')
        )
        
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Asignaciones"
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro"]
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
                asignacion['fecha_hora_registro']
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
            filtros.get('usuarioRegistra')
        )
        
        output = StringIO()
        writer = csv.writer(output)
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro"]
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
                asignacion['fecha_hora_registro']
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
            filtros.get('usuarioRegistra')
        )
        
        return json.dumps(asignaciones, ensure_ascii=False, indent=4)
    
    def obtener_asignacion_por_fecha(self, cedula, fecha):
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, GROUP_CONCAT(ruta), linea, cop, observaciones, puestosSC, puestosUQ
            FROM asignaciones
            WHERE cedula = ? AND fecha = ?
            GROUP BY cedula, fecha, concesion, control
        """
        params = (cedula, fecha)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            conn.close()

            if resultados:
                return [{
                    'fecha': resultado[0],
                    'cedula': resultado[1],
                    'nombre': resultado[2],
                    'turno': resultado[3],
                    'h_inicio': resultado[4],
                    'h_fin': resultado[5],
                    'concesion': resultado[6],
                    'control': resultado[7],
                    'ruta': resultado[8],
                    'linea': resultado[9],
                    'cop': resultado[10],
                    'observaciones': resultado[11],
                    'puestosSC': resultado[12],
                    'puestosUQ': resultado[13]
                } for resultado in resultados]
            return None

        except sqlite3.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None
