import psycopg2
import json
import os
import re
import requests
import msal
import pandas as pd
from io import BytesIO
import calendar
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient, ContainerClient
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Cargar las variables de entorno desde .env
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "5000-juridica-y-riesgos-juridica-clausulas"
site_url = "https://grupoexpress.sharepoint.com/sites/PlataformaBICEXP"
username = os.getenv("USUARIO_JURIDICO")
password = os.getenv("CLAVE_JURIDICO")
remitente = os.getenv("USUARIO_CORREO_JURIDICO")
contrasena = os.getenv("CLAVE_CORREO_JURIDICO")

# Configuración del servidor SMTP Correos Automaticos
smtp_server = "smtp.office365.com"
smtp_port = 587

class GestionClausulas:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(DATABASE_PATH)
            #print("Conexión a la base de datos establecida.")
        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e
    
    def obtener_clausulas(self):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
            SELECT id, control, etapa, clausula, contrato_concesion, tema, descripcion_clausula, 
                    tipo_clausula, frecuencia
            FROM clausulas
            ORDER BY clausula ASC;;
            """
            cursor.execute(query)
            clausulas = cursor.fetchall()
        return clausulas
    
    def obtener_opciones_etapas(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT etapa FROM etapas_juridico;")
            etapas = cursor.fetchall()
        return etapas
    
    def obtener_opciones_clausulas(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT clausula FROM clausulas ORDER BY clausula;")
            clausulas = cursor.fetchall()
        return clausulas

    def obtener_opciones_concesion(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT concesion FROM concesion ORDER BY concesion ASC;")
            concesiones = cursor.fetchall()
        return concesiones
    
    def obtener_opciones_contrato(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT contrato FROM concesion;")
            contratos = cursor.fetchall()
        return contratos
    
    def obtener_contrato_por_concesion(self, concesion):
        query = "SELECT contrato FROM concesion WHERE concesion = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (concesion,))
            contrato = cursor.fetchone()
        return contrato[0] if contrato else None
    
    def obtener_opciones_tipo_clausula(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT tipo_clausula FROM tipo_clausulas ORDER BY tipo_clausula ASC;")
            tipos_clausula = cursor.fetchall()
        return tipos_clausula
    
    def obtener_opciones_procesos(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT proceso FROM procesos ORDER BY proceso;")
            procesos = cursor.fetchall()
        return procesos
    
    def obtener_opciones_subprocesos(self, proceso):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT subproceso FROM procesos WHERE proceso = %s ORDER BY subproceso;", (proceso,))
            subprocesos = cursor.fetchall()
        return subprocesos

    def obtener_opciones_frecuencias(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT frecuencia FROM frecuencia ORDER BY frecuencia ASC;")
            frecuencias = cursor.fetchall()
        return frecuencias
    
    def obtener_opciones_responsables(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT responsable FROM responsable ORDER BY responsable ASC;")
            responsables = cursor.fetchall()
        return responsables

    def obtener_opciones_responsables_clausulas(self):
        query = """
        SELECT DISTINCT TRIM(responsable_entrega) AS responsable_entrega
        FROM clausulas
        WHERE responsable_entrega IS NOT NULL
        ORDER BY responsable_entrega ASC;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            responsables = cursor.fetchall()
        
        return [fila[0] for fila in responsables]  # Extraer solo los valores únicos y ordenados

    def obtener_clausulas_filtradas(self, control=None, etapa=None, clausula=None, concesion=None, estado=None, responsable=None, proceso=None, subproceso=None):
        query = """
        SELECT c.id, c.control, c.etapa, c.clausula, c.contrato_concesion, c.tema, c.descripcion_clausula, 
            c.tipo_clausula, c.frecuencia, 
            COALESCE(g.estado, 'Sin Estado') AS estado_mas_reciente
        FROM clausulas c
        LEFT JOIN (
            SELECT DISTINCT ON (id_clausula) id_clausula, fecha_entrega, estado
            FROM clausulas_gestion
            ORDER BY id_clausula, fecha_entrega DESC
        ) g ON c.id = g.id_clausula
        WHERE (%s IS NULL OR c.control = %s)
        AND (%s IS NULL OR c.etapa = %s)
        AND (%s IS NULL OR c.clausula = %s)
        AND (%s IS NULL OR c.contrato_concesion = %s)
        AND (%s IS NULL OR g.estado = %s)
        AND (%s IS NULL OR TRIM(c.responsable_entrega) = TRIM(%s)) -- Coincidencia exacta
        -- Proceso (si viene)
        AND (
                %s IS NULL
            OR EXISTS (
                    SELECT 1
                    FROM public.clausula_proceso_subproceso cps
                    JOIN public.procesos p ON p.id_proceso = cps.id_proceso
                    WHERE cps.id_clausula = c.id
                    AND p.proceso = %s
                )
        )

        -- Subproceso (si viene)
        AND (
                %s IS NULL
            OR EXISTS (
                    SELECT 1
                    FROM public.clausula_proceso_subproceso cps2
                    JOIN public.procesos p2 ON p2.id_proceso = cps2.id_proceso
                    WHERE cps2.id_clausula = c.id
                    AND p2.proceso = %s
                    AND p2.subproceso = %s
                )
        )        
        
        ORDER BY c.clausula ASC;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (
                control, control,
                etapa, etapa,
                clausula, clausula,
                concesion, concesion,
                estado, estado,
                responsable, responsable,
                proceso, proceso,
                subproceso, proceso, subproceso # Asegura que el subproceso corresponde al proceso
            ))
            clausulas = cursor.fetchall()
        return clausulas

    def obtener_id_proceso(self, proceso, subproceso):
        query = """
        SELECT id_proceso FROM procesos
        WHERE proceso = %s AND subproceso = %s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (proceso, subproceso))
            result = cursor.fetchone()
            if result:
                return result[0]  # Retornar el id_proceso
            else:
                raise ValueError(f"No se encontró el id_proceso para proceso: {proceso} y subproceso: {subproceso}")

    def crear_clausula(self, clausula_data):
        query_clausula = """
        INSERT INTO clausulas (control, etapa, clausula, modificacion, contrato_concesion, tema, subtema, descripcion_clausula, 
                            tipo_clausula, norma_relacionada, consecuencia, frecuencia, inicio_cumplimiento, 
                            fin_cumplimiento, observacion, periodo_control, responsable_entrega, ruta_soporte)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query_clausula, (
                clausula_data['control'],
                clausula_data['etapa'],
                clausula_data['clausula'],
                clausula_data['modificacion'],
                clausula_data['contrato'],
                clausula_data['tema'],
                clausula_data['subtema'],
                clausula_data['descripcion'],
                clausula_data['tipo'],
                clausula_data['norma'],
                clausula_data['consecuencia'],
                clausula_data['frecuencia'],
                clausula_data['inicio_cumplimiento'],
                clausula_data['fin_cumplimiento'],
                clausula_data['observacion'],
                clausula_data['periodo_control'],
                clausula_data['responsable_entrega'],
                clausula_data['ruta_soporte']
            ))
            id_clausula = cursor.fetchone()[0]
            self.connection.commit()
            #print("Cláusula registrada con ID:", id_clausula)
            return id_clausula

    def registrar_ruta_soporte(self, id_clausula, ruta_soporte):
        """
        Actualiza la ruta soporte para una cláusula existente.
        """
        query = """
        UPDATE clausulas
        SET ruta_soporte = %s
        WHERE id = %s;
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (ruta_soporte, id_clausula))
                self.connection.commit()
                #print(f"Ruta soporte registrada para cláusula {id_clausula}: {ruta_soporte}")
        except Exception as e:
            self.connection.rollback()
            print(f"Error al registrar la ruta soporte: {e}")
            raise

    def registrar_clausula_proceso_subproceso(self, id_clausula, procesos_subprocesos):
        query = """
        INSERT INTO clausula_proceso_subproceso (id_clausula, id_proceso)
        VALUES (%s, %s);
        """
        try:
            with self.connection.cursor() as cursor:
                # Imprimir el id_clausula y procesos_subprocesos para verificar los datos recibidos
                #print("ID de la cláusula:", id_clausula)
                #print("Procesos a insertar:", procesos_subprocesos)

                # Iterar sobre cada proceso y registrar en la tabla
                for proceso in procesos_subprocesos:
                    id_proceso = proceso.get('id_proceso')
                    
                    if id_proceso:
                        # Ejecutar la inserción en la tabla
                        cursor.execute(query, (id_clausula, id_proceso))
                        #print(f"Registro insertado en clausula_proceso_subproceso: id_clausula={id_clausula}, id_proceso={id_proceso}")
                    else:
                        print("Error: id_proceso no encontrado en el objeto", proceso)
                self.connection.commit()  # Confirmar la transacción después de insertar todos los registros

        except Exception as e:
            self.connection.rollback()  # Revertir la transacción en caso de error
            #print(f"Error al insertar en clausula_proceso_subproceso: {e}")
            raise ValueError(f"Error al insertar en clausula_proceso_subproceso: {str(e)}")

    def obtener_clausula_por_id(self, id_clausula):
        query = """
        SELECT id, control, etapa, clausula, contrato_concesion, tema, subtema, descripcion_clausula, 
               tipo_clausula, modificacion, norma_relacionada, consecuencia, frecuencia, inicio_cumplimiento, 
               fin_cumplimiento, observacion, periodo_control, responsable_entrega, ruta_soporte
        FROM clausulas
        WHERE id = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_clausula,))
            clausula = cursor.fetchone()
        return clausula
    
    def obtener_procesos_subprocesos_por_clausula(self, id_clausula):
        query = """
        SELECT cps.id_proceso, p.proceso, p.subproceso
        FROM clausula_proceso_subproceso cps
        JOIN procesos p ON cps.id_proceso = p.id_proceso
        WHERE cps.id_clausula = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_clausula,))
            procesos_subprocesos = cursor.fetchall()
        return procesos_subprocesos
    
    def actualizar_clausula(self, id_clausula, clausula_data):
        query = """
        UPDATE clausulas
        SET control = %s, etapa = %s, clausula = %s, modificacion = %s, contrato_concesion = %s,
            tema = %s, subtema = %s, descripcion_clausula = %s, tipo_clausula = %s, norma_relacionada = %s,
            consecuencia = %s, frecuencia = %s, inicio_cumplimiento = %s, fin_cumplimiento = %s,
            observacion = %s, periodo_control = %s, responsable_entrega = %s, ruta_soporte = %s
        WHERE id = %s;
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    clausula_data.get('control'),
                    clausula_data.get('etapa'),
                    clausula_data.get('clausula'),
                    clausula_data.get('modificacion'),
                    clausula_data.get('contrato_concesion'),
                    clausula_data.get('tema'),
                    clausula_data.get('subtema'),
                    clausula_data.get('descripcion'),
                    clausula_data.get('tipo_clausula'),
                    clausula_data.get('norma_relacionada'),
                    clausula_data.get('consecuencia'),
                    clausula_data.get('frecuencia'),
                    clausula_data.get('inicio_cumplimiento'),
                    clausula_data.get('fin_cumplimiento'),
                    clausula_data.get('observacion'),
                    clausula_data.get('periodo_control'),
                    clausula_data.get('responsable_entrega'),
                    clausula_data.get('ruta_soporte'),
                    id_clausula
                ))
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Error al actualizar la cláusula: {e}")
            raise e
            
    def actualizar_procesos_subprocesos(self, id_clausula, procesos_subprocesos):
        # Eliminar duplicados en la lista de procesos usando un conjunto
        procesos_unicos = {proceso['id_proceso'] for proceso in procesos_subprocesos}
        
        query_delete = "DELETE FROM clausula_proceso_subproceso WHERE id_clausula = %s;"
        query_insert = """
        INSERT INTO clausula_proceso_subproceso (id_clausula, id_proceso)
        VALUES (%s, %s);
        """
        with self.connection.cursor() as cursor:
            # Eliminar procesos/subprocesos existentes
            cursor.execute(query_delete, (id_clausula,))
            # Insertar los nuevos procesos/subprocesos únicos
            for id_proceso in procesos_unicos:
                cursor.execute(query_insert, (id_clausula, id_proceso))
            self.connection.commit()

    def calcular_fechas_dinamicas(self, inicio, fin, frecuencia, periodo_control):
        # Asegurar que los valores sean del tipo `datetime`
        if isinstance(inicio, date):
            inicio = datetime.combine(inicio, datetime.min.time())
        elif isinstance(inicio, str):
            inicio = datetime.strptime(inicio, "%Y-%m-%d")

        if isinstance(fin, date):
            fin = datetime.combine(fin, datetime.min.time())
        elif isinstance(fin, str):
            fin = datetime.strptime(fin, "%Y-%m-%d")

        fechas = []
        fecha_actual = datetime.today()

        # Diccionario de frecuencia
        delta = {
            "Mensual": relativedelta(months=1),
            "Bimestral": relativedelta(months=2),
            "Trimestral": relativedelta(months=3),
            "Semestral": relativedelta(months=6),
            "Anual": relativedelta(years=1),
        }

        # Normaliza la frecuencia
        frecuencia_normalizada = frecuencia.capitalize()

        if frecuencia_normalizada not in delta and frecuencia_normalizada not in ["Personalizado", "No aplica", "Diario", "Quincenal"]:
            raise ValueError(f"Frecuencia no válida: {frecuencia}")

        incremento = delta.get(frecuencia_normalizada, None)

        # Manejo especial para 'No aplica'
        if frecuencia_normalizada == "No aplica":
            return fechas  # Retorna lista vacía

        # Manejo especial para 'PERSONALIZADO'
        if frecuencia_normalizada == "Personalizado":
            if isinstance(periodo_control, str):
                try:
                    fecha_unica = datetime.strptime(periodo_control, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Formato de periodo_control inválido para 'Personalizado': {periodo_control}")
                if inicio <= fecha_unica <= fin:
                    fechas.append({
                        "fecha": fecha_unica.strftime("%Y-%m-%d"),
                        "entrega": fecha_unica.strftime("%Y-%m-%d")
                    })
            return fechas

        # Manejo especial para 'Diario' (excluyendo fines de semana)
        if frecuencia_normalizada == "Diario":
            fecha_entrega = inicio  # Comienza desde la fecha de inicio de cumplimiento
            while fecha_entrega <= min(fecha_actual, fin):  # Hasta la fecha actual o la fecha fin, lo que sea menor
                if fecha_entrega.weekday() < 5:  # Lunes a Viernes
                    fechas.append({
                        "fecha": fecha_entrega.strftime("%Y-%m-%d"),
                        "entrega": fecha_entrega.strftime("%Y-%m-%d")  # La fecha de entrega coincide con el día hábil
                    })
                fecha_entrega += timedelta(days=1)  # Avanza al siguiente día
            return fechas

        # Manejo especial para 'Quincenal'
        if frecuencia_normalizada == "Quincenal":
            # Normalizar el día configurado (quitar espacios y convertir a minúsculas )
            #print(f"Período Control recibido: {periodo_control}")
            dia_configurado = periodo_control.strip().lower()  # Ejemplo: "MIERCOLES" -> "miercoles"

            # Diccionario para convertir nombres de días al formato de calendar
            dias_semana = {
                "lunes": "Monday", "martes": "Tuesday", "miercoles": "Wednesday",
                "jueves": "Thursday", "viernes": "Friday", "sabado": "Saturday", "domingo": "Sunday"
            }
            
            if dia_configurado not in dias_semana:
                raise ValueError(f"El día configurado en periodo_control no es válido: {dia_configurado}")

            dia_configurado_en_ingles = dias_semana[dia_configurado]  # Convertir al formato esperado
            #print(f"Día configurado: {dia_configurado}, en inglés: {dia_configurado_en_ingles}")
            #print(f"Días disponibles: {list(dias_semana.keys())}")

            fecha_entrega = inicio  # Comienza desde la fecha de inicio de cumplimiento
            dias_por_ciclo = 14  # Ciclo de quincena

            # Validar que inicio y fin sean correctos
            if inicio > fin:
                raise ValueError(f"La fecha de inicio ({inicio}) no puede ser mayor que la fecha de fin ({fin}).")
            #print(f"Fecha inicial: {inicio}, Fecha final: {fin}")

            # Avanzar hasta el primer día configurado después de la fecha de inicio
            while calendar.day_name[fecha_entrega.weekday()] != dia_configurado_en_ingles:
                #print(f"Avanzando desde {fecha_entrega.strftime('%Y-%m-%d')} - Día actual: {calendar.day_name[fecha_entrega.weekday()]}")
                fecha_entrega += timedelta(days=1)

            # Generar fechas dinámicas desde la fecha actual hasta la fecha fin
            fechas = []
            while fecha_entrega <= min(fecha_actual, fin):
                fechas.append({
                    "fecha": fecha_entrega.strftime("%Y-%m-%d"),
                    "entrega": fecha_entrega.strftime("%Y-%m-%d")
                })
                #print(f"Fecha programada: {fecha_entrega.strftime('%Y-%m-%d')}")
                fecha_entrega += timedelta(days=dias_por_ciclo)

            #print(f"Fechas calculadas dinámicamente: {fechas}")
            return fechas

        # Para frecuencias basadas en delta (Mensual, Bimestral, Trimestral, Semestral, Anual)
        while inicio <= fin:
            if inicio <= fecha_actual:
                # Ajusta la fecha de entrega exactamente al periodo_control
                if frecuencia_normalizada in ["Mensual", "Bimestral", "Trimestral", "Semestral"]:
                    dia_max = int(periodo_control)
                    fecha_entrega = inicio.replace(day=min(dia_max, calendar.monthrange(inicio.year, inicio.month)[1]))
                elif frecuencia_normalizada == "Anual":
                    dia, mes = map(int, periodo_control.split("-"))
                    fecha_entrega = inicio.replace(day=dia, month=mes)
                else:
                    fecha_entrega = inicio

                fechas.append({
                    "fecha": inicio.strftime("%Y-%m-%d"),
                    "entrega": fecha_entrega.strftime("%Y-%m-%d")
                })

            inicio += incremento

        return fechas

    def insertar_filas_gestion_nuevas(self, id_clausula, filas_gestion):
        """
        Inserta solo las filas de gestión nuevas si la fecha de entrega no existe ya en la base de datos.
        Si la cláusula tiene frecuencia "Diario", los campos se completan automáticamente.
        """
        # Obtener la frecuencia de la cláusula antes de la inserción
        query_frecuencia = """
        SELECT frecuencia FROM clausulas WHERE id = %s;
        """
        # Obtener ID de la feccha de entrega a gestionar
        query_check = """
        SELECT 1 FROM clausulas_gestion 
        WHERE id_clausula = %s AND fecha_entrega = %s;
        """
        # Insertar la fila de gestión
        query_insert = """
        INSERT INTO clausulas_gestion (id_clausula, fecha_entrega, estado, fecha_radicado, 
                                    numero_radicado, radicado_cexp, prorroga, fecha_prorroga,
                                    registrado_por, fecha_creacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id_gestion;
        """

        with self.connection.cursor() as cursor:
            # Obtener la frecuencia de la cláusula
            cursor.execute(query_frecuencia, (id_clausula,))
            frecuencia_result = cursor.fetchone()
            frecuencia = frecuencia_result[0] if frecuencia_result else None

            for fila in filas_gestion:
                # Convertir fecha al formato correcto
                fecha_entrega = datetime.strptime(fila['fecha_entrega'], "%d/%m/%Y").strftime("%Y-%m-%d")

                # Verificar si la fecha ya existe para esta cláusula
                cursor.execute(query_check, (id_clausula, fecha_entrega))
                existe = cursor.fetchone()

                if not existe:  # Solo insertar si no existe
                    # Si la frecuencia es "Diario", establecer valores automáticos
                    if frecuencia and frecuencia.lower() == "diario":
                        fecha_radicado = fecha_entrega
                        numero_radicado = "Automático"
                        radicado_cexp = "Automático"
                        registrado_por = "AUTOMÁTICO"
                    else:
                        fecha_radicado = None
                        numero_radicado = None
                        radicado_cexp = None
                        registrado_por = fila.get('registrado_por', 'Sin Gestionar')
                        
                    # Obtener valores de Prórroga y Fecha Prórroga
                    prorroga = fila.get("prorroga", "No")  # Valor por defecto "No"
                    fecha_prorroga = (
                        datetime.strptime(fila["fecha_prorroga"], "%d/%m/%Y").strftime("%Y-%m-%d")
                        if fila.get("fecha_prorroga") else None
                    )

                    cursor.execute(query_insert, (
                        id_clausula,
                        fecha_entrega,
                        fila['estado'],
                        fecha_radicado,
                        numero_radicado,
                        radicado_cexp,
                        prorroga,
                        fecha_prorroga,
                        registrado_por
                    ))
                    fila['id_gestion'] = cursor.fetchone()[0]

            self.connection.commit()
        
        return filas_gestion
    
    def actualizar_filas_gestion(self, filas_gestion):
        """
        Actualiza las filas de gestión existentes en la base de datos.
        Solo actualiza el campo `registrado_por` si hay cambios en los valores gestionados.
        """
        query_select = """
        SELECT fecha_radicado, numero_radicado, radicado_cexp, plan_accion, observacion, estado, registrado_por, prorroga, fecha_prorroga
        FROM clausulas_gestion WHERE id_gestion = %s;
        """
        query_update = """
        UPDATE clausulas_gestion
        SET fecha_radicado = %s, numero_radicado = %s, radicado_cexp = %s, plan_accion = %s, observacion = %s,
            estado = %s, prorroga = %s, fecha_prorroga = %s, registrado_por = CASE 
                WHEN %s THEN %s ELSE registrado_por END
        WHERE id_gestion = %s;
        """

        with self.connection.cursor() as cursor:
            for fila in filas_gestion:
                # Obtener valores actuales
                cursor.execute(query_select, (fila["id_gestion"],))
                current_values = cursor.fetchone()

                if not current_values:
                    continue  # Saltar si no se encontró el registro

                # Convertir fechas al mismo formato (YYYY-MM-DD)
                current_fecha_radicado = (
                    current_values[0].strftime("%Y-%m-%d") if current_values[0] else None
                )
                nueva_fecha_radicado = (
                    datetime.strptime(fila["fecha_radicado"], "%Y-%m-%d").strftime("%Y-%m-%d")
                    if fila["fecha_radicado"]
                    else None
                )
                
                # Convertir fecha prórroga al mismo formato
                current_fecha_prorroga = (
                    current_values[8].strftime("%Y-%m-%d") if current_values[8] else None
                )
                nueva_fecha_prorroga = (
                    datetime.strptime(fila["fecha_prorroga"], "%Y-%m-%d").strftime("%Y-%m-%d")
                    if fila.get("fecha_prorroga")
                    else None
                )

                # Comparar valores actuales con los enviados
                cambios_detectados = (
                    current_fecha_radicado != nueva_fecha_radicado or
                    current_values[1] != fila["numero_radicado"] or
                    current_values[2] != fila["radicado_cexp"] or  
                    current_values[3] != fila["plan_accion"] or
                    current_values[4] != fila["observacion"] or
                    current_values[5] != fila["estado"] or
                    current_values[7] != fila["prorroga"] or
                    current_fecha_prorroga != nueva_fecha_prorroga
                )

                registrado_por_cambio = fila["registrado_por"] if cambios_detectados else None

                # Ejecutar la actualización
                cursor.execute(query_update, (
                    nueva_fecha_radicado,
                    fila["numero_radicado"],
                    fila["radicado_cexp"],
                    fila["plan_accion"],
                    fila["observacion"],
                    fila["estado"],
                    fila["prorroga"],
                    nueva_fecha_prorroga,
                    cambios_detectados,  # True si hay cambios
                    registrado_por_cambio,  # Usuario actual si hay cambios
                    fila["id_gestion"],
                ))
            self.connection.commit()
        
    def obtener_filas_gestion_por_clausula(self, id_clausula):
        """
        Obtiene todas las filas de gestión asociadas a una cláusula específica.
        """
        query = """
        SELECT id_gestion, fecha_entrega, fecha_radicado, numero_radicado, radicado_cexp, plan_accion, observacion, 
            estado, registrado_por, fecha_creacion, prorroga, fecha_prorroga
        FROM clausulas_gestion
        WHERE id_clausula = %s
        ORDER BY fecha_entrega ASC;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_clausula,))
            filas = cursor.fetchall()

            # Convertir fechas a formato DD/MM/YYYY
            for fila in filas:
                if fila["fecha_entrega"]:
                    fila["fecha_entrega"] = fila["fecha_entrega"].strftime("%d/%m/%Y")
                if fila["fecha_radicado"]:
                    fila["fecha_radicado"] = fila["fecha_radicado"].strftime("%d/%m/%Y")
                if fila["fecha_prorroga"]:
                    fila["fecha_prorroga"] = fila["fecha_prorroga"].strftime("%d/%m/%Y")
                if fila.get("fecha_creacion"):
                    fila["fecha_creacion"] = fila["fecha_creacion"].strftime("%d/%m/%Y")
        return filas

    def obtener_clausula_nombre(self, id_clausula):
        query = "SELECT clausula FROM clausulas WHERE id = %s;"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (id_clausula,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"No se encontró el nombre de la cláusula con ID {id_clausula}")

    def obtener_clausula_contrato(self, id_clausula):
        query = "SELECT contrato_concesion FROM clausulas WHERE id = %s;"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (id_clausula,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"No se encontró el contrato asociado a la cláusula con ID {id_clausula}")

    def obtener_clausulas_con_entrega_estado(self):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
            SELECT 
                c.id, 
                subquery.fecha_entrega AS fecha_entrega_mas_reciente,
                subquery.estado AS estado_mas_reciente
            FROM clausulas c
            LEFT JOIN LATERAL (
                SELECT 
                    fecha_entrega, 
                    estado
                FROM clausulas_gestion
                WHERE id_clausula = c.id
                ORDER BY fecha_entrega DESC
                LIMIT 1
            ) subquery ON TRUE
            ORDER BY c.id ASC;
            """
            cursor.execute(query)
            clausulas = cursor.fetchall()

            # Convertir fechas a formato DD/MM/YYYY
            for clausula in clausulas:
                if clausula["fecha_entrega_mas_reciente"]:
                    fecha_original = clausula["fecha_entrega_mas_reciente"]
                    clausula["fecha_entrega_mas_reciente"] = fecha_original.strftime("%d/%m/%Y")

            #print("Resultados procesados:", clausulas)  # Verificar en consola
        return clausulas

    def obtener_opciones_estado(self):
        with self.connection.cursor() as cursor:
            query = """
            SELECT DISTINCT estado
            FROM clausulas_gestion
            WHERE estado IS NOT NULL
            ORDER BY estado;
            """
            cursor.execute(query)
            resultados = cursor.fetchall()
        return [fila[0] for fila in resultados]

    def crear_estructura_blob_storage(self, id_clausula, nombre_clausula, contrato, fechas_entrega):
        """
        Crea la estructura de carpetas en Blob Storage para una cláusula específica.
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)

            # Normalizar nombres para cumplir con los requisitos de Azure Storage
            def normalizar_nombre(nombre):
                # Reemplazar espacios por guiones, eliminar caracteres no permitidos
                return re.sub(r"[^a-zA-Z0-9\-]", "", nombre.replace(" ", "-"))

            carpeta_principal = f"{normalizar_nombre(str(id_clausula))}-{normalizar_nombre(nombre_clausula)}-{normalizar_nombre(contrato)}"
            
            for fecha_entrega in fechas_entrega:
                # Convertir fecha de entrega a objetos datetime
                fecha_obj = datetime.strptime(fecha_entrega, "%Y-%m-%d")
                anio = fecha_obj.year
                mes = f"{fecha_obj.month:02}"  # Mes en formato MM

                # Crear la ruta simulada para la carpeta
                ruta_carpeta = f"{carpeta_principal}/{anio}/{mes}/"

                # Subir un archivo vacío para simular el funcionamiento de la carpeta
                blob_client = container_client.get_blob_client(f"{ruta_carpeta}estructura.txt")
                blob_client.upload_blob(b"", overwrite=True)  # Archivo vacío para crear la estructura
        except Exception as e:
            print(f"Error creando la estructura en Blob Storage: {e}")
            raise

# Administrar los correos de copia para notificaciones de recordatorio e incumplimiento
    def obtener_responsables(self):
        query = """
        SELECT id_responsable, responsable, correo
        FROM responsable
        ORDER BY responsable;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def actualizar_copia_correos(self, id_clausula, responsables_copia):
        """
        Actualiza los responsables en copia asociados a una cláusula.
        - Elimina los responsables no incluidos en la lista.
        - Inserta los nuevos responsables.
        """
        query_delete = """
        DELETE FROM clausula_responsables_copia
        WHERE id_clausula = %s AND id_responsable NOT IN %s;
        """
        query_insert = """
        INSERT INTO clausula_responsables_copia (id_clausula, id_responsable)
        VALUES (%s, %s)
        ON CONFLICT (id_clausula, id_responsable) DO NOTHING;
        """
        try:
            with self.connection.cursor() as cursor:
                # Validar que la lista de responsables no esté vacía
                ids_responsables = tuple(r['id_responsable'] for r in responsables_copia if r.get('id_responsable'))
                if not ids_responsables:
                    # Si no hay responsables válidos, simplemente eliminamos todos los registros asociados
                    cursor.execute("DELETE FROM clausula_responsables_copia WHERE id_clausula = %s;", (id_clausula,))
                else:
                    # Eliminar responsables que ya no están en la lista
                    cursor.execute(query_delete, (id_clausula, ids_responsables))

                    # Insertar nuevos responsables
                    for responsable in responsables_copia:
                        if responsable.get('id_responsable'):
                            cursor.execute(query_insert, (id_clausula, responsable['id_responsable']))

                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise ValueError(f"Error al actualizar responsables en copia: {str(e)}")

    def obtener_copia_correos(self, id_clausula):
        """
        Obtiene los responsables en copia asociados a una cláusula.
        """
        query = """
        SELECT r.id_responsable, r.responsable, r.correo
        FROM clausula_responsables_copia crc
        JOIN responsable r ON crc.id_responsable = r.id_responsable
        WHERE crc.id_clausula = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_clausula,))
            return cursor.fetchall()

# JOBS DE ACTUALIZACIÓN ASINCRONICA PARA LAS FILAS DE GESTIÓN DINAMICAS Y EL ESTADO DE GESTIÓN DE CADA FILA
    def obtener_clausulas_job(self):
        """
        Retorna las cláusulas necesarias para calcular fechas dinámicas.
        """
        query = """
        SELECT id, inicio_cumplimiento, fin_cumplimiento, frecuencia, periodo_control
        FROM clausulas;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def actualizar_fechas_dinamicas(self, id_clausula, fechas_dinamicas):
        """
        Actualiza las fechas dinámicas de una cláusula en la base de datos sin eliminar filas existentes.
        """
        query_select = "SELECT fecha_entrega FROM clausulas_gestion WHERE id_clausula = %s;"
        query_insert = """
        INSERT INTO clausulas_gestion (id_clausula, fecha_entrega, estado)
        VALUES (%s, %s, 'A Tiempo');
        """
        with self.connection.cursor() as cursor:
            # Obtener fechas ya registradas
            cursor.execute(query_select, (id_clausula,))
            fechas_existentes = {row[0] for row in cursor.fetchall()}

            # Insertar solo las nuevas fechas
            for fecha in fechas_dinamicas:
                if fecha['entrega'] not in fechas_existentes:
                    cursor.execute(query_insert, (id_clausula, fecha['entrega']))

            self.connection.commit()

    def obtener_todas_filas_gestion(self):
        """
        Retorna todas las filas de gestión para sincronizar estados.
        """
        query = """
        SELECT id_gestion, fecha_entrega, fecha_radicado, estado
        FROM clausulas_gestion;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def actualizar_estado_fila(self, id_gestion, nuevo_estado):
        """
        Actualiza el estado de una fila de gestión en la base de datos.
        """
        query = """
        UPDATE clausulas_gestion
        SET estado = %s
        WHERE id_gestion = %s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (nuevo_estado, id_gestion))
            self.connection.commit()

    def calcular_estado(self, fecha_entrega, fecha_radicado, fecha_actual):
        """
        Calcula el estado de una fila según las fechas de entrega, radicado y actual.
        """
        if not fecha_radicado and fecha_entrega >= fecha_actual:
            return "A Tiempo"
        elif not fecha_radicado and fecha_entrega < fecha_actual:
            return "Incumplida"
        elif fecha_radicado > fecha_entrega:
            return "Extemporal"
        elif fecha_radicado <= fecha_entrega:
            return "Cumplida"
        else:
            return "Desconocido"

# REPORTES Y NOTIFICACIONES
    # Obtener token de Microsoft Graph
    def _get_graph_token(self) -> str:
        """Obtiene token usando Client Credentials (Entra ID / App Registration)."""
        tenant_id = os.getenv("TENANT_ID")
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")

        if not tenant_id or not client_id or not client_secret:
            raise RuntimeError("Faltan variables de entorno: TENANT_ID, CLIENT_ID, CLIENT_SECRET")

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority,
        )

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" not in result:
            raise RuntimeError(f"No se pudo obtener token Graph: {result}")

        return result["access_token"]
    # Enviar correo por Microsoft Graph
    def _send_mail_graph(self, sender_upn: str, to_list: list[str], cc_list: list[str], subject: str, html_body: str) -> None:
        """Envía correo por Microsoft Graph desde el buzón sender_upn."""
        token = self._get_graph_token()

        url = f"https://graph.microsoft.com/v1.0/users/{sender_upn}/sendMail"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": html_body},
                "toRecipients": [{"emailAddress": {"address": x}} for x in to_list],
                "ccRecipients": [{"emailAddress": {"address": x}} for x in cc_list] if cc_list else [],
            },
            "saveToSentItems": "false",
        }

        resp = requests.post(url, headers=headers, data=json.dumps(payload))

        # Graph devuelve 202 si lo aceptó.
        if resp.status_code != 202:
            raise RuntimeError(f"Graph sendMail falló. Status={resp.status_code} Body={resp.text[:2000]}")

# Recordatorio de Notificaciones
    def validar_conexion(self):
        """
        Verifica si la conexión a la base de datos sigue activa y la reestablece si está cerrada.
        """
        try:
            if self.connection.closed != 0:  # Si la conexión está cerrada (0 indica abierta)
                print("La conexión a la base de datos estaba cerrada. Reestableciendo conexión...")
                self.connection = psycopg2.connect(DATABASE_PATH)  # Reestablecer conexión
        except Exception as e:
            print(f"Error al validar o reestablecer la conexión: {e}")
            raise

    def calcular_proxima_fecha_cumplimiento(self, ultima_fecha, frecuencia, periodo_control, fin):
        """
        Calcula la próxima fecha de entrega basada en la última fecha registrada,
        generando la siguiente sin esperar a que llegue el periodo exacto.
        """

        # Asegurar que los valores sean del tipo `datetime`
        if isinstance(ultima_fecha, str):
            ultima_fecha = datetime.strptime(ultima_fecha, "%Y-%m-%d").date()
        if isinstance(fin, str):
            fin = datetime.strptime(fin, "%Y-%m-%d").date()

        fecha_actual = datetime.today().date()
        fechas = []

        # Diccionario de frecuencia
        delta = {
            "Mensual": relativedelta(months=1),
            "Bimestral": relativedelta(months=2),
            "Trimestral": relativedelta(months=3),
            "Semestral": relativedelta(months=6),
            "Anual": relativedelta(years=1),
        }

        # Normaliza la frecuencia
        frecuencia_normalizada = frecuencia.capitalize()

        # Si la frecuencia es "No aplica", finalizar sin calcular fechas
        if frecuencia_normalizada == "No aplica":
            return None  # Termina sin hacer nada

        if frecuencia_normalizada not in delta and frecuencia_normalizada not in ["Personalizado", "Diario", "Quincenal"]:
            raise ValueError(f"Frecuencia no válida: {frecuencia}")

        incremento = delta.get(frecuencia_normalizada, None)

        # Si la última fecha ya pasó, calcular la próxima fecha
        while ultima_fecha < fecha_actual:
            if incremento:
                ultima_fecha += incremento
            elif frecuencia_normalizada == "Diario":
                ultima_fecha += timedelta(days=1)
                while ultima_fecha.weekday() >= 5:  # Excluir fines de semana
                    ultima_fecha += timedelta(days=1)
            elif frecuencia_normalizada == "Quincenal":
                if not periodo_control:
                    raise ValueError(f"⚠️ Error en cláusula Quincenal: periodo_control es None.")

                dia_configurado = periodo_control.strip().lower()
                dias_semana = {
                    "lunes": "Monday", "martes": "Tuesday", "miercoles": "Wednesday",
                    "jueves": "Thursday", "viernes": "Friday", "sabado": "Saturday", "domingo": "Sunday"
                }
                if dia_configurado not in dias_semana:
                    raise ValueError(f"El día configurado en periodo_control no es válido: {dia_configurado}")

                dia_configurado_en_ingles = dias_semana[dia_configurado]

                # Avanzar hasta el primer día válido en la semana
                while calendar.day_name[ultima_fecha.weekday()] != dia_configurado_en_ingles:
                    ultima_fecha += timedelta(days=1)

                # Sumar ciclos quincenales hasta alcanzar la fecha actual
                while ultima_fecha < fecha_actual:
                    ultima_fecha += timedelta(days=14)

            elif frecuencia_normalizada == "Personalizado":
                if isinstance(periodo_control, str):
                    try:
                        fecha_unica = datetime.strptime(periodo_control, "%Y-%m-%d").date()
                    except ValueError:
                        raise ValueError(f"Formato de periodo_control inválido para 'Personalizado': {periodo_control}")
                    if fecha_unica > ultima_fecha and fecha_unica <= fin:
                        return fecha_unica.strftime("%Y-%m-%d")
                return None

        # Si la próxima fecha calculada supera la fecha de fin, retornar None
        if ultima_fecha > fin:
            return None

        return ultima_fecha.strftime("%Y-%m-%d")

    def generar_datos_recordatorio(self):
        """
        Obtiene los datos necesarios para generar el reporte de notificaciones por recordatorio,
        calculando la próxima fecha de cumplimiento sin depender de la BD.
        """
        self.validar_conexion()

        query_clausulas = """
        SELECT 
            c.id, c.clausula, c.contrato_concesion, c.frecuencia, c.responsable_entrega, r.correo,
            c.inicio_cumplimiento, c.fin_cumplimiento, c.periodo_control
        FROM clausulas c
        LEFT JOIN responsable r ON c.responsable_entrega = r.responsable;
        """

        query_gestion = """
        SELECT id_clausula, fecha_entrega, estado
        FROM clausulas_gestion
        WHERE id_clausula = %s
        ORDER BY fecha_entrega DESC
        LIMIT 1;
        """

        query_procesos = """
        SELECT p.proceso, p.subproceso
        FROM clausula_proceso_subproceso cps
        JOIN procesos p ON cps.id_proceso = p.id_proceso
        WHERE cps.id_clausula = %s;
        """

        query_cc_responsables = """
        SELECT crc.id_clausula, COALESCE(STRING_AGG(r.correo, ', '), '') AS correos_cc
        FROM clausula_responsables_copia crc
        JOIN responsable r ON crc.id_responsable = r.id_responsable
        GROUP BY crc.id_clausula;
        """

        try:
            data = []
            fecha_hoy = datetime.today().date()
            print(f"🔍 Fecha actual (HOY): {fecha_hoy}")

            cc_responsables = {}

            with self.connection.cursor() as cursor:
                cursor.execute(query_cc_responsables)
                for row in cursor.fetchall():
                    cc_responsables[row[0]] = row[1]

                cursor.execute(query_clausulas)
                clausulas = cursor.fetchall()

                for row in clausulas:
                    id_clausula = row[0]
                    inicio = row[6]
                    fin = row[7]
                    frecuencia = row[3]
                    periodo_control = row[8]

                    print(f"\n🆔 Procesando cláusula {id_clausula} ({row[1]})")

                    if isinstance(inicio, date):
                        inicio = inicio.strftime("%Y-%m-%d")
                    if isinstance(fin, date):
                        fin = fin.strftime("%Y-%m-%d")

                    cursor.execute(query_gestion, (id_clausula,))
                    gestion = cursor.fetchone()

                    if gestion:
                        ultima_fecha_entrega = gestion[1]
                        estado = gestion[2]
                    else:
                        ultima_fecha_entrega = inicio
                        estado = "Pendiente"

                    proxima_fecha = self.calcular_proxima_fecha_cumplimiento(ultima_fecha_entrega, frecuencia, periodo_control, fin)

                    if not proxima_fecha:
                        continue
                    #Parametrizar días de notificación
                    proxima_fecha_dt = datetime.strptime(proxima_fecha, "%Y-%m-%d").date()
                    dias_resta = 5 if frecuencia.lower() in ["mensual", "quincenal"] else 15
                    fecha_notificacion = proxima_fecha_dt - timedelta(days=dias_resta)

                    print(f"📅 Próxima fecha de entrega: {proxima_fecha_dt} - Fecha de notificación esperada: {fecha_notificacion}")

                    if fecha_notificacion == fecha_hoy:
                        cursor.execute(query_procesos, (id_clausula,))
                        procesos = cursor.fetchall()
                        procesos_texto = ", ".join([f"{proceso[0]} - {proceso[1]}" for proceso in procesos])

                        data.append({
                            "ID": row[0],
                            "Clausula": row[1],
                            "Contrato Concesion": row[2],
                            "Frecuencia": frecuencia,
                            "Responsable Entrega": row[4],
                            "Correo": row[5],
                            "CC_Correos": cc_responsables.get(id_clausula, ""),
                            "Proceso/Subproceso": procesos_texto,
                            "Fecha Entrega": proxima_fecha_dt.strftime("%d/%m/%Y"),
                            "Estado": estado,
                            "Fecha Notificacion": fecha_notificacion.strftime("%d/%m/%Y")
                        })
                        print(f"✅ Se enviará recordatorio para {row[1]}")
                    else:
                        print("❌ La fecha de notificación NO coincide con HOY.")

            return data if data else []

        except Exception as e:
            print(f"❌ Error al generar los datos del reporte: {e}")
            raise

    def enviar_correos_recordatorio(self):
        """
        Envía correos utilizando la información generada por la función `generar_datos_recordatorio`,
        pero usando Microsoft Graph (Entra ID).
        """
        remitente = os.getenv("USUARIO_CORREO_JURIDICO")  # UPN del buzón que envía (Graph)
        if not remitente:
            raise RuntimeError("Falta la variable de entorno USUARIO_CORREO_JURIDICO")

        try:
            datos_recordatorio = self.generar_datos_recordatorio()

            if not datos_recordatorio or all(d["ID"] is None for d in datos_recordatorio):
                print("No hay recordatorios para enviar hoy.")
                return

            for recordatorio in datos_recordatorio:
                if recordatorio["ID"] is None:
                    continue

                destinatario = recordatorio["Correo"]
                cc_destinatarios = [cc for cc in recordatorio["CC_Correos"].split(", ") if cc] if recordatorio["CC_Correos"] else []

                # Formatear la fecha de entrega
                try:
                    fecha_entrega = recordatorio["Fecha Entrega"]

                    if isinstance(fecha_entrega, str):
                        if "/" in fecha_entrega:
                            fecha_entrega = datetime.strptime(fecha_entrega, "%d/%m/%Y")
                        else:
                            fecha_entrega = datetime.strptime(fecha_entrega, "%Y-%m-%d")

                    fecha_entrega_formateada = fecha_entrega.strftime("%d/%m/%Y")

                except Exception as e:
                    print(f"⚠️ Error al formatear la fecha de entrega ({recordatorio['Fecha Entrega']}): {e}")
                    fecha_entrega_formateada = recordatorio["Fecha Entrega"]

                asunto = f"Recordatorio: {recordatorio['ID']} - {recordatorio['Clausula']} (Contrato: {recordatorio['Contrato Concesion']}) - Entrega Maxima {fecha_entrega_formateada}"

                cuerpo = (
                    f"<div style='background-color: #004080; color: white; padding: 10px; text-align: center; border-radius: 8px;'>"
                    f"<h2 style='margin: 0; font-size: 20px;'>Acreditación del cumplimiento de clausula {recordatorio['Clausula']} </h2>"
                    f"<p style='margin: 5px 0; font-size: 16px; font-style: italic; font-weight: bold;'>Consorcio Express S.A.S</p>"
                    f"</div>"
                    f"<div style='font-size: 14px; background-color: #f9f9f9; padding: 15px; border: 1px solid #ddd; border-radius: 8px; margin-top: 10px;'>"
                    f"<p>Estimado(a):<br><strong>{recordatorio['Responsable Entrega']}</strong>,</p>"
                    f"<p>Este es un recordatorio para la acreditación y cumplimiento de la cláusula:</p>"
                    f"<p style='font-weight: bold;'>"
                    f"{recordatorio['ID']} - {recordatorio['Clausula']} (Contrato: {recordatorio['Contrato Concesion']})</p>"
                    f"<p>Cuyo proceso y subproceso responsable son:</p>"
                    f"<p><strong>{recordatorio['Proceso/Subproceso']}</strong></p>"
                    f"<p>Frecuencia de cumplimiento: <strong>{recordatorio['Frecuencia']}</strong></p>"
                    f"<p>Fecha de entrega programada: <strong>{fecha_entrega_formateada}</strong></p>"
                    f"<p>Estado actual: <strong>{recordatorio['Estado']}</strong></p>"
                    f"<p style='margin-top: 20px;'>Por favor, recuerde actualizar el estado del cumplimiento con fecha, radicado y los soportes pertinentes a la plataforma de GestiónExpress.</p>"
                    f"<div style='text-align: center; margin-top: 20px;'>"
                    f"<a href='https://gestionconsorcioexpress.azurewebsites.net/' style='"
                    f"display: inline-block; background-color: #004080; color: white; padding: 10px 20px; text-decoration: none; "
                    f"border-radius: 5px; font-size: 16px;'>Ir a GestiónExpress</a>"
                    f"</div>"
                    f"</div>"
                    f"<div style='margin-top: 20px; font-size: 12px; color: #666; text-align: center;'>"
                    f"<p>Dirección Jurídica - Consorcio Express S.A.S</p>"
                    f"<p>Dirección: Av. El Dorado #69-63, Bogotá, Colombia | Tel: +57 123 456789</p>"
                    f"<p>Este correo es informativo y no requiere respuesta.</p>"
                    f"</div>"
                )

                # Envío por Graph
                self._send_mail_graph(
                    sender_upn=remitente,
                    to_list=[destinatario],
                    cc_list=cc_destinatarios,
                    subject=asunto,
                    html_body=cuerpo,
                )

                print(f"Correo enviado a: {destinatario}")

        except Exception as e:
            print(f"Error al enviar correos: {e}")
            raise

# Incumplimientos de Notificaciones
    def generar_datos_incumplimiento(self):
        """
        Obtiene los datos necesarios para generar el reporte de incumplimiento.
        """
        self.validar_conexion()  # Asegurar que la conexión esté activa
        query_clausulas = """
        SELECT 
            c.id, c.clausula, c.contrato_concesion, c.consecuencia, c.frecuencia, 
            c.responsable_entrega, r.correo
        FROM clausulas c
        LEFT JOIN responsable r ON c.responsable_entrega = r.responsable;
        """
        
        query_gestion = """
        SELECT id_clausula, fecha_entrega
        FROM clausulas_gestion
        WHERE id_clausula = %s AND estado = 'Incumplida' AND fecha_entrega < %s
        ORDER BY fecha_entrega ASC;
        """
        
        query_procesos = """
        SELECT p.proceso, p.subproceso
        FROM clausula_proceso_subproceso cps
        JOIN procesos p ON cps.id_proceso = p.id_proceso
        WHERE cps.id_clausula = %s;
        """
        
        query_cc_responsables = """
        SELECT crc.id_clausula, COALESCE(STRING_AGG(r.correo, ', '), '') AS correos_cc
        FROM clausula_responsables_copia crc
        JOIN responsable r ON crc.id_responsable = r.id_responsable
        GROUP BY crc.id_clausula;
        """
        
        try:
            data = []
            fecha_actual = datetime.today().date()
            cc_responsables = {}

            with self.connection.cursor() as cursor:
                # Obtener los responsables en copia
                cursor.execute(query_cc_responsables)
                for row in cursor.fetchall():
                    cc_responsables[row[0]] = row[1]  # Guardamos {id_clausula: lista_correos_cc}
                
                # Obtener los datos base de las cláusulas
                cursor.execute(query_clausulas)
                clausulas = cursor.fetchall()

                for row in clausulas:
                    id_clausula = row[0]

                    # Obtener las fechas incumplidas
                    cursor.execute(query_gestion, (id_clausula, fecha_actual))
                    fechas_incumplidas = cursor.fetchall()

                    if not fechas_incumplidas:
                        continue  # Si no hay fechas incumplidas, pasa a la siguiente cláusula

                    # Obtener los procesos y subprocesos asociados
                    cursor.execute(query_procesos, (id_clausula,))
                    procesos = cursor.fetchall()

                    procesos_texto = ", ".join(
                        [f"{proceso[0]} - {proceso[1]}" for proceso in procesos]
                    )

                    # Formatear las fechas incumplidas
                    fechas_formateadas = [
                        fecha[1].strftime("%d/%m/%Y") for fecha in fechas_incumplidas
                    ]

                    data.append({
                        "ID": row[0],
                        "Clausula": row[1],
                        "Contrato Concesion": row[2],
                        "Consecuencia": row[3],
                        "Frecuencia": row[4],
                        "Responsable Entrega": row[5],
                        "Correo": row[6],
                        "CC_Correos": cc_responsables.get(id_clausula, ""),  # Correos en copia
                        "Proceso/Subproceso": procesos_texto,
                        "Fechas Incumplidas": fechas_formateadas
                    })

            return data
        except Exception as e:
            print(f"Error al generar los datos de incumplimiento: {e}")
            raise

    def enviar_correos_incumplimiento(self):
        """
        Envía correos con las fechas de entrega incumplidas por responsable,
        usando Microsoft Graph (Entra ID) en lugar de SMTP.
        """
        remitente = os.getenv("USUARIO_CORREO_JURIDICO")
        if not remitente:
            raise RuntimeError("Falta la variable de entorno USUARIO_CORREO_JURIDICO")

        try:
            datos_incumplimiento = self.generar_datos_incumplimiento()

            if not datos_incumplimiento:
                print("No hay fechas de incumplimiento para enviar.")
                return

            # Agrupar por responsable
            responsables = {}
            for registro in datos_incumplimiento:
                responsable = registro["Responsable Entrega"]
                if responsable not in responsables:
                    responsables[responsable] = {"Correo": registro["Correo"], "Clausulas": []}
                responsables[responsable]["Clausulas"].append(registro)

            for responsable, datos in responsables.items():
                destinatario = datos["Correo"]
                cc_destinatarios = [cc for cc in datos["Clausulas"][0]["CC_Correos"].split(", ") if cc] if datos["Clausulas"][0]["CC_Correos"] else []

                cuerpo = (
                    f"<div style='background-color: #004080; color: white; padding: 10px; text-align: center; border-radius: 8px;'>"
                    f"<h2 style='margin: 0; font-size: 20px;'>Reporte de Incumplimientos de Cláusulas Jurídicas</h2>"
                    f"<p style='margin: 5px 0 0; font-size: 16px; font-style: italic; font-weight: bold;'>Consorcio Express S.A.S</p>"
                    f"</div>"
                    f"<div style='font-size: 14px; background-color: #f9f9f9; padding: 15px; border: 1px solid #ddd; border-radius: 8px;'>"
                    f"<p>Estimado(a):<br><strong>{responsable}</strong></p>"
                    f"<p>A continuación, se informa las cláusulas jurídicas en estado de 'INCUMPLIMIENTO'. "
                    f"Es importante dar gestión y subsanar esta obligación de forma inmediata:</p>"
                )

                cuerpo += (
                    f"<table style='width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 12px;'>"
                    f"<thead style='background-color: #f2f2f2;'>"
                    f"<tr>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Cláusula</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Frecuencia</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Consecuencia</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Fechas Incumplidas</th>"
                    f"</tr>"
                    f"</thead>"
                    f"<tbody>"
                )

                for clausula in datos["Clausulas"]:
                    fechas_incumplidas = " - ".join([f"[{fecha}]" for fecha in clausula["Fechas Incumplidas"]])
                    cuerpo += (
                        f"<tr>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'><b>{clausula['ID']} - {clausula['Clausula']}</b></td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{clausula['Frecuencia']}</td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{clausula['Consecuencia']}</td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{fechas_incumplidas}</td>"
                        f"</tr>"
                    )

                cuerpo += (
                    f"</tbody></table>"
                    f"<p>Por favor, recuerde actualizar el estado del cumplimiento con fecha, radicado y los soportes pertinentes "
                    f"a la plataforma de GestiónExpress.</p>"
                    f"<div style='text-align: center; margin-top: 20px;'>"
                    f"<a href='https://gestionconsorcioexpress.azurewebsites.net/' style='"
                    f"display: inline-block; background-color: #004080; color: white; padding: 10px 20px; text-decoration: none; "
                    f"border-radius: 5px; font-size: 16px;'>Ir a GestiónExpress</a>"
                    f"</div>"
                    f"</div>"
                    f"<div style='margin-top: 20px; font-size: 12px; color: #666; text-align: center;'>"
                    f"<p>Consorcio Express S.A.S</p>"
                    f"<p>Dirección: Av. El Dorado #69-63, Bogotá, Colombia | Tel: +57 123 456789</p>"
                    f"<p>Este correo es informativo y no requiere respuesta.</p>"
                    f"</div>"
                )

                self._send_mail_graph(
                    sender_upn=remitente,
                    to_list=[destinatario],
                    cc_list=cc_destinatarios,
                    subject="¡Importante! Reporte de Incumplimientos de Cláusulas Jurídicas",
                    html_body=cuerpo,
                )

                print(f"Correo enviado a: {destinatario}")

        except Exception as e:
            print(f"Error al enviar correos de incumplimiento: {e}")
            raise

    def enviar_correos_incumplimiento_direccion(self):
        """
        Envía un correo consolidado a una dirección específica con los incumplimientos de todos los responsables,
        usando Microsoft Graph (Entra ID) en lugar de SMTP.
        """
        remitente = os.getenv("USUARIO_CORREO_JURIDICO")
        if not remitente:
            raise RuntimeError("Falta la variable de entorno USUARIO_CORREO_JURIDICO")

        destinatario_fijo = [
            "patrick.barros@consorcioexpress.co",
            "natalia.morales@consorcioexpress.co"
        ]

        cc_fijo = [
            "laura.bonilla@consorcioexpress.co",
            "sergio.hincapie@consorcioexpress.co"
        ]

        try:
            datos_incumplimiento = self.generar_datos_incumplimiento()

            if not datos_incumplimiento:
                print("No hay fechas de incumplimiento para enviar.")
                return

            # Agrupar por responsable
            responsables = {}
            for registro in datos_incumplimiento:
                responsable = registro["Responsable Entrega"]
                if responsable not in responsables:
                    responsables[responsable] = {"Clausulas": []}
                responsables[responsable]["Clausulas"].append(registro)

            cuerpo = (
                f"<div style='background-color: #004080; color: white; padding: 10px; text-align: center; border-radius: 8px;'>"
                f"<h2 style='margin: 0; font-size: 20px;'>Consolidado de Incumplimientos de Cláusulas Jurídicas</h2>"
                f"<p style='margin: 5px 0 0; font-size: 16px; font-style: italic; font-weight: bold;'>Consorcio Express S.A.S</p>"
                f"</div>"
                f"<div style='font-size: 14px; background-color: #f9f9f9; padding: 15px; border: 1px solid #ddd; border-radius: 8px;'>"
                f"<p>Estimado(a),</p>"
                f"<p>A continuación, se presenta el reporte consolidado de cláusulas jurídicas en estado de 'INCUMPLIMIENTO'.</p>"
            )

            for responsable, datos in responsables.items():
                cuerpo += (
                    f"<h3 style='color: #004080; margin-top: 20px;'>Responsable: {responsable}</h3>"
                    f"<table style='width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 12px;'>"
                    f"<thead style='background-color: #f2f2f2;'>"
                    f"<tr>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Cláusula</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Frecuencia</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Consecuencia</th>"
                    f"<th style='border: 1px solid #ddd; padding: 8px; font-size: 14px;'>Fechas Incumplidas</th>"
                    f"</tr>"
                    f"</thead>"
                    f"<tbody>"
                )

                for clausula in datos["Clausulas"]:
                    fechas_incumplidas = " - ".join([f"[{fecha}]" for fecha in clausula["Fechas Incumplidas"]])
                    cuerpo += (
                        f"<tr>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'><b>{clausula['ID']} - {clausula['Clausula']}</b></td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{clausula['Frecuencia']}</td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{clausula['Consecuencia']}</td>"
                        f"<td style='border: 1px solid #ddd; padding: 8px; font-size: 12px;'>{fechas_incumplidas}</td>"
                        f"</tr>"
                    )

                cuerpo += "</tbody></table>"

            cuerpo += (
                f"<p>Por favor, recuerde actualizar el estado del cumplimiento con fecha, radicado y los soportes pertinentes "
                f"a la plataforma de GestiónExpress.</p>"
                f"<div style='text-align: center; margin-top: 20px;'>"
                f"<a href='https://gestionconsorcioexpress.azurewebsites.net/' style='"
                f"display: inline-block; background-color: #004080; color: white; padding: 10px 20px; text-decoration: none; "
                f"border-radius: 5px; font-size: 16px;'>Ir a GestiónExpress</a>"
                f"</div>"
                f"</div>"
                f"<div style='margin-top: 20px; font-size: 12px; color: #666; text-align: center;'>"
                f"<p>Consorcio Express S.A.S</p>"
                f"<p>Dirección: Av. El Dorado #69-63, Bogotá, Colombia | Tel: +57 123 456789</p>"
                f"<p>Este correo es informativo y no requiere respuesta.</p>"
                f"</div>"
            )

            self._send_mail_graph(
                sender_upn=remitente,
                to_list=destinatario_fijo,
                cc_list=cc_fijo,
                subject="¡Importante! Consolidado de Incumplimientos de Cláusulas Jurídicas",
                html_body=cuerpo,
            )

            print(f"Correo consolidado enviado a: {', '.join(destinatario_fijo)} con copia a: {', '.join(cc_fijo)}")

        except Exception as e:
            print(f"Error al enviar correos de incumplimiento a la dirección: {e}")
            raise

# Reporte descargable de la gestión de clausulas
    def conectar_db(self):
        """Asegura que la conexión a la base de datos esté abierta correctamente."""
        if self.connection is None or self.connection.closed:
            database_url = os.getenv("DATABASE_PATH")
            if not database_url:
                raise ValueError("DATABASE_PATH no está definido en las variables de entorno")
            
            self.connection = psycopg2.connect(database_url)

    def obtener_filtros_disponibles(self):
        """Obtiene los valores únicos de los filtros para el frontend."""
        self.conectar_db()
        query = """
        SELECT DISTINCT c.id, c.control, c.clausula, c.etapa, c.contrato_concesion, 
                        c.tipo_clausula, c.frecuencia, c.responsable_entrega, 
                        g.fecha_entrega, g.plan_accion, g.estado, g.registrado_por
        FROM clausulas c
        LEFT JOIN clausulas_gestion g ON c.id = g.id_clausula;
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()

            if not data:
                return {"error": "No hay datos en los filtros"}

            def limpiar(valores):
                return sorted(set(val for val in valores if val is not None))

            filtros = {
                "ids": limpiar([row[0] for row in data]),
                "controles": limpiar([row[1] for row in data]),
                "clausulas": limpiar([row[2] for row in data]),
                "etapas": limpiar([row[3] for row in data]),
                "contratos": limpiar([row[4] for row in data]),
                "tipos_clausula": limpiar([row[5] for row in data]),
                "frecuencias": limpiar([row[6] for row in data]),
                "responsables": limpiar([row[7] for row in data]),
                "plan_acciones": limpiar([row[9] for row in data]),
                "estados": limpiar([row[10] for row in data]),
                "registrados_por": limpiar([row[11] for row in data]),
            }
            return filtros
        except Exception as e:
            return {"error": str(e)}

    def obtener_reporte_clausulas(self, **filtros):
        """Obtiene los datos filtrados del reporte."""
        self.conectar_db()
        condiciones = []
        valores = []

        # Mapeo del filtro "responsable" a "responsable_entrega"
        filtro_mapeo = {
            "responsable": "c.responsable_entrega" 
        }

        for campo, valor in filtros.items():
            if valor:
                campo_bd = filtro_mapeo.get(campo, campo)  # Usa el mapeo si existe, sino deja el nombre original
                condiciones.append(f"{campo_bd} = %s")
                valores.append(valor)

        query = """
        SELECT
            c.id, c.control, c.clausula, c.etapa, c.contrato_concesion, c.tipo_clausula,
            c.tema, c.subtema, c.descripcion_clausula, c.modificacion, c.norma_relacionada,
            c.consecuencia,
            (SELECT STRING_AGG(p.proceso || ' - ' || p.subproceso, ', ')
            FROM clausula_proceso_subproceso cps
            JOIN procesos p ON cps.id_proceso = p.id_proceso
            WHERE cps.id_clausula = c.id) AS proceso_subproceso,
            c.frecuencia, c.inicio_cumplimiento, c.fin_cumplimiento, c.observacion,
            c.responsable_entrega,
            (SELECT STRING_AGG(r.correo, ', ')
            FROM clausula_responsables_copia crc
            JOIN responsable r ON crc.id_responsable = r.id_responsable
            WHERE crc.id_clausula = c.id) AS responsables_copia,
            c.ruta_soporte, g.fecha_entrega, g.fecha_radicado, g.numero_radicado,
            g.radicado_cexp, g.plan_accion, g.observacion, g.estado, g.registrado_por,
            g.prorroga, g.fecha_prorroga
        FROM clausulas c
        LEFT JOIN clausulas_gestion g ON c.id = g.id_clausula
        """

        if condiciones:
            query += " WHERE " + " AND ".join(condiciones)
            
        # Agregar orden ascendente por id y luego por fecha_entrega
        query += " ORDER BY c.id ASC, g.fecha_entrega ASC"

        #print(f"Consulta generada: {query}")  # 🔍 Log de la consulta SQL
        #print(f"Valores de la consulta: {valores}")  # 🔍 Log de los valores a insertar en SQL

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(valores))
                data = cursor.fetchall()

            return [
                {
                    "id": row[0], "control": row[1], "clausula": row[2], "etapa": row[3],
                    "contrato_concesion": row[4], "tipo_clausula": row[5], "tema": row[6], 
                    "subtema": row[7], "descripcion_clausula": row[8], "modificacion": row[9],
                    "norma_relacionada": row[10], "consecuencia": row[11], "proceso_subproceso": row[12],
                    "frecuencia": row[13], 
                    "inicio_cumplimiento": row[14].isoformat() if isinstance(row[14], date) else None,
                    "fin_cumplimiento": row[15].isoformat() if isinstance(row[15], date) else None,
                    "observacion": row[16], "responsable": row[17], "responsables_copia": row[18], 
                    "ruta_soporte": row[19], 
                    "fecha_entrega": row[20].isoformat() if isinstance(row[20], date) else None,
                    "fecha_radicado": row[21].isoformat() if isinstance(row[21], date) else None,
                    "numero_radicado": row[22], "radicado_cexp": row[23], 
                    "plan_accion": row[24], "observacion_gestion": row[25], "estado": row[26], 
                    "registrado_por": row[27], "prorroga": row[28], 
                    "fecha_prorroga": row[29].isoformat() if isinstance(row[29], date) else None
                } for row in data
            ]
        except Exception as e:
            self.connection.rollback()
            print(f"Error en obtener_reporte_clausulas: {str(e)}")  # 🔍 Log del error SQL
            return {"error": str(e)}
    
    def exportar_reporte(self, formato, **filtros):
        data = self.obtener_reporte_clausulas(**filtros)
        if not data:
            return None, None, None

        df = pd.DataFrame(data)
        
        # Renombrar la columna "numero_radicado" a "Radicado TMSA"
        df.rename(columns={"numero_radicado": "radicado_tmsa"}, inplace=True)

        # Obtener la carpeta de Descargas del usuario
        download_folder = os.path.expanduser("~/Downloads")
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)  # Asegurar que existe

        filename = f"Reporte_Clausulas.{formato}"

        # Crear archivo temporal en la carpeta de descargas
        file_path = os.path.join(download_folder, filename)

        if formato == "xlsx":
            df.to_excel(file_path, index=False)
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif formato == "csv":
            df.to_csv(file_path, index=False)
            content_type = "text/csv"
        elif formato == "json":
            df.to_json(file_path, orient="records")
            content_type = "application/json"
        else:
            return None, None, None

        return file_path, content_type, filename
    
    def close(self):
        self.connection.close()