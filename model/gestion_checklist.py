import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime, time
from typing import List, Tuple, Optional
import os

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")

class GestionChecklist:       
    def __init__(self):
        try:
            self.connection = psycopg2.connect(DATABASE_PATH)
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e

    def cerrar_conexion(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def obtener_procesos_para_select(self):
        """
        Retorna todas las filas de procesos para que el front arme el cascader.
        """
        query = """
            SELECT id_proceso, proceso, subproceso
            FROM public.procesos
            ORDER BY proceso ASC, subproceso ASC;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def obtener_tipos_vehiculos(self):
        """Obtiene la lista de tipos de vehículos."""
        query = "SELECT id_tipo_vehiculo, nombre FROM checklist_tipo_vehiculo ORDER BY nombre ASC;"
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            resultados = cursor.fetchall()
        return resultados

    def obtener_vehiculos(self):
        """Obtiene todos los vehículos registrados con el nombre del tipo de vehículo"""
        query = """
        SELECT v.placa, v.id_tipo_vehiculo, t.nombre AS tipo_vehiculo_nombre, v.marca, v.linea, v.modelo, v.estado, v.id_proceso, p.proceso, p.subproceso
        FROM vehiculos v
        JOIN checklist_tipo_vehiculo t ON v.id_tipo_vehiculo = t.id_tipo_vehiculo
        LEFT JOIN public.procesos p ON p.id_proceso = v.id_proceso
        ORDER BY v.placa ASC;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
        return resultados

    def crear_vehiculo(self, data):
        """Crea un nuevo vehículo en la base de datos"""
        query = """
        INSERT INTO vehiculos (placa, id_tipo_vehiculo, marca, linea, modelo, id_proceso)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING placa;
        """
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, (data["placa"], data["id_tipo_vehiculo"], data["marca"], data["linea"], data["modelo"], data.get("id_proceso"))) 
                self.connection.commit()
                return {"message": "Vehículo registrado exitosamente", "placa": cursor.fetchone()[0]}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al crear vehículo: {str(e)}"}

    def obtener_vehiculo_por_placa(self, placa):
        """Obtiene un vehículo específico por su placa"""
        query = """
        SELECT v.id AS id_vehiculo, v.placa, v.marca, v.linea, v.modelo, v.id_tipo_vehiculo, t.nombre AS tipo_vehiculo, v.id_proceso, p.proceso, p.subproceso
        FROM vehiculos v
        JOIN checklist_tipo_vehiculo t ON v.id_tipo_vehiculo = t.id_tipo_vehiculo
        LEFT JOIN public.procesos p ON p.id_proceso = v.id_proceso
        WHERE v.placa = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (placa,))
            vehiculo = cursor.fetchone()
        
        '''
        if vehiculo:
            print("Vehículo encontrado:", vehiculo)
        else:
            print("Vehículo NO encontrado.")
        '''
        
        return vehiculo

    def actualizar_vehiculo(self, placa, data):
        """Actualiza un vehículo por su placa"""
        query_update = """
        UPDATE vehiculos 
        SET id_tipo_vehiculo = %s, marca = %s, linea = %s, modelo = %s, id_proceso = %s
        WHERE placa = %s;
        """
        with self.connection.cursor() as cursor:
            try:
                # Ejecutar la actualización con el ID del tipo de vehículo
                cursor.execute(query_update, (data["id_tipo_vehiculo"], data["marca"], data["linea"], data["modelo"], data.get("id_proceso"), placa))
                self.connection.commit()
                return {"message": "Vehículo actualizado correctamente"}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al actualizar vehículo: {str(e)}"}

    def inactivar_vehiculo(self, placa, nuevo_estado):
        """Cambia el estado de un vehículo en la base de datos."""
        query = "UPDATE vehiculos SET estado = %s WHERE placa = %s;"
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, (nuevo_estado, placa))
                self.connection.commit()
                return {"message": f"Vehículo {'activado' if nuevo_estado == 1 else 'inactivado'} correctamente"}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al cambiar el estado del vehículo: {str(e)}"}

    def buscar_vehiculos(self, query, solo_moto: bool = False):
        """Busca vehículos activos que coincidan con la placa. 
        Si solo_moto=True, filtra id_tipo_vehiculo=2 (Moto)."""
        query_sql = """
            SELECT placa 
            FROM public.vehiculos 
            WHERE estado = 1 
            AND placa ILIKE %s
        """
        params = [f"%{query}%"]

        # ✅ filtro opcional para Motos
        if solo_moto:
            query_sql += " AND id_tipo_vehiculo = 2"

        query_sql += " ORDER BY placa ASC;"

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query_sql, params)
            resultados = cursor.fetchall()
        return resultados
    
    def obtener_tipos_documentos(self):
        """Consulta la lista de tipos de documentos en orden alfabético"""
        query = """
        SELECT id_tipo_documento, nombre 
        FROM checklist_tipo_documento
        ORDER BY nombre ASC
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            documentos = cursor.fetchall()
            return [{"id": doc["id_tipo_documento"], "nombre": doc["nombre"]} for doc in documentos]

    def obtener_componentes_por_tipo(self, id_tipo_vehiculo):
        """Obtiene los componentes para un tipo de vehículo"""
        query_sql = """
            SELECT id_componente, grupo, posicion, componente 
            FROM checklist_componentes 
            WHERE id_tipo_vehiculo = %s 
            ORDER BY grupo, posicion, componente;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query_sql, (id_tipo_vehiculo,))
            resultados = cursor.fetchall()
        return resultados

    def obtener_ultimo_checklist(self, id_vehiculo):
        """Obtiene el último checklist registrado para un vehículo"""
        query = """
        SELECT * FROM checklist_registro 
        WHERE id_vehiculo = %s 
        ORDER BY fecha_hora_registro DESC 
        LIMIT 1;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_vehiculo,))
            return cursor.fetchone()

    def obtener_documentos_checklist(self, id_checklist):
        """Obtiene los documentos del checklist registrado"""
        query = """
        SELECT id_tipo_documento, estado, observaciones
        FROM checklist_documentos
        WHERE id_checklist_registro = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_checklist,))
            documentos = cursor.fetchall()
        return documentos if documentos else []

    def obtener_detalles_checklist(self, id_checklist):
        """Obtiene los detalles del checklist registrado"""
        query = """
        SELECT id_componente, estado, observaciones
        FROM checklist_detalle
        WHERE id_checklist_registro = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_checklist,))
            detalles = cursor.fetchall()
        return detalles if detalles else []

    def guardar_checklist_registro(self, registro):
        """Inserta un nuevo checklist en checklist_registro y devuelve el ID generado."""
        #print("✅ Guardando checklist_registro...")
        # --- Validación previa ---
        km_anterior = self.obtener_km_anterior_por_vista(registro.id_vehiculo, registro.fecha_hora_registro)

        try:
            km_nuevo = int(registro.km_odometro)
        except Exception:
            raise ValueError("El km del odómetro es inválido.")

        # =========================================================
        # CONDICIONAL PARA "GESTIÓN CHECKLIST PREOPERACIONAL MOTOS" (AJUSTES)
        es_ajuste_gestion = (
            str(getattr(registro, "inicio_turno", "")).strip() == "07:00"
            and str(getattr(registro, "fin_turno", "")).strip() == "17:00"
            and str(getattr(registro, "observaciones_generales", "") or "").strip() == ""
        )

        if km_anterior is not None:
            if es_ajuste_gestion:
                # Si en ajustes llega 0 (por input vacío), tomar el último km registrado
                if km_nuevo <= 0:
                    km_nuevo = int(km_anterior)
                    registro.km_odometro = km_nuevo  # asegura que se inserte el km correcto

                # En ajustes: permitir igualdad, solo bloquear si es MENOR
                if km_nuevo < km_anterior:
                    raise ValueError(
                        f"El Km Odómetro ingresado ({km_nuevo}) no puede ser menor "
                        f"al último capturado ({km_anterior})."
                    )
            else:
                # Flujo normal: mantener regla actual (no menor NI igual)
                if km_nuevo <= km_anterior:
                    raise ValueError(
                        f"El Km Odómetro ingresado ({km_nuevo}) no puede ser menor o igual "
                        f"al último capturado ({km_anterior})."
                    )
        
        # --- Insertar si todo está OK ---
        query = """
        INSERT INTO checklist_registro 
        (id_vehiculo, fecha_hora_registro, inicio_turno, fin_turno, km_odometro, 
        observaciones_generales, usuario_registro, fecha_guardado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'America/Bogota')
        RETURNING id_checklist;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (
                registro.id_vehiculo, registro.fecha_hora_registro, registro.inicio_turno,
                registro.fin_turno, registro.km_odometro, registro.observaciones_generales,
                registro.usuario_registro
            ))
            id_checklist = cursor.fetchone()
            self.connection.commit()
            
        # REFRESH de la MV para que los cálculos de preop reflejen el odómetro nuevo
        try:
            self.refrescar_mv_odometro_por_dia(concurrently=True)  # usa False si tu PG no soporta concurrently
        except Exception as e:
            print(f"[WARN] Error refrescando MV post-checklist: {e}")
            
        if id_checklist:
            return id_checklist[0]
        return None

    def guardar_checklist_documento(self, id_checklist, documento):
        """Inserta un documento asociado a un checklist."""
        query = """
        INSERT INTO checklist_documentos (id_checklist_registro, id_tipo_documento, estado, observaciones)
        VALUES (%s, %s, %s::BOOLEAN, %s);
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (
                id_checklist,  
                documento.id_tipo_documento,
                documento.estado,
                documento.observaciones
            ))
            self.connection.commit()

    def guardar_checklist_detalle(self, id_checklist, detalle):
        """Inserta un detalle asociado a un checklist."""
        query = """
        INSERT INTO checklist_detalle (id_checklist_registro, id_componente, estado, observaciones)
        VALUES (%s, %s, %s::BOOLEAN, %s);
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (
                id_checklist,  
                detalle.id_componente,
                detalle.estado,
                detalle.observaciones
            ))
            self.connection.commit()

    def obtener_vehiculos_con_fallas(self, user_id: Optional[int] = None):
        """Devuelve vehículos con el estado del último checklist y su estado de vinculación, Si user_id viene, filtra por la flota (roles_flota_asis) del usuario."""
        query = """
        SELECT v.placa, t.nombre AS tipo_vehiculo, v.marca, v.linea, v.modelo,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM checklist_registro cr
                    LEFT JOIN checklist_documentos cd ON cr.id_checklist = cd.id_checklist_registro
                    LEFT JOIN checklist_detalle cdet ON cr.id_checklist = cdet.id_checklist_registro
                    WHERE cr.id_vehiculo = v.id
                    AND cr.fecha_hora_registro = (
                        SELECT MAX(fecha_hora_registro)
                        FROM checklist_registro
                        WHERE id_vehiculo = v.id
                    )
                    AND (
                        cd.estado = false OR cdet.estado = false
                    )
                )
                THEN 'Fallas'
                ELSE 'Correcto'
            END AS estado,
            CASE 
                WHEN v.estado = 1 THEN 'Activo'
                ELSE 'Inactivo'
            END AS vinculacion
        FROM vehiculos v
        JOIN checklist_tipo_vehiculo t ON v.id_tipo_vehiculo = t.id_tipo_vehiculo
        WHERE (%(user_id)s IS NULL OR EXISTS (
            SELECT 1 FROM public.roles_flota_asis rfa
            WHERE rfa.user_id = %(user_id)s
                AND rfa.id_proceso = v.id_proceso
        ))
        ORDER BY v.placa;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {"user_id": user_id})
            return cursor.fetchall()

    def obtener_detalle_falla_vehiculo(self, placa, user_id: Optional[int] = None):
        """Retorna datos del vehículo y su checklist con validación por flota (si aplica)."""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Vehículo validando pertenencia a la flota del usuario (si user_id viene)
            cursor.execute("""
                SELECT v.id, v.placa, t.nombre AS tipo_vehiculo, v.marca, v.linea, v.modelo
                FROM vehiculos v
                JOIN checklist_tipo_vehiculo t ON v.id_tipo_vehiculo = t.id_tipo_vehiculo
                WHERE v.placa = %s
                    AND (%s IS NULL OR EXISTS (
                        SELECT 1 FROM public.roles_flota_asis rfa
                        WHERE rfa.user_id = %s
                        AND rfa.id_proceso = v.id_proceso
                ))
            """, (placa, user_id, user_id))
            vehiculo = cursor.fetchone()
            if not vehiculo:
                return {"error": "Vehículo no encontrado o no pertenece a su flota"}

            # Observaciones generales
            cursor.execute("""
                SELECT fecha_hora_registro, observaciones_generales, usuario_registro
                FROM checklist_registro
                WHERE id_vehiculo = %s
                ORDER BY fecha_hora_registro DESC
            """, (vehiculo["id"],))

            observaciones_generales = []
            for row in cursor.fetchall():
                if row["observaciones_generales"] and row["observaciones_generales"].strip() != "":
                    row["fecha_hora_registro"] = row["fecha_hora_registro"].strftime('%Y-%m-%d %H:%M:%S')
                    observaciones_generales.append(row)

            # Consolidar fallas en documentos
            cursor.execute("""
                SELECT cd.id_tipo_documento, td.nombre AS tipo_documento,
                    cd.estado, cd.observaciones, cr.usuario_registro, cr.fecha_hora_registro
                FROM checklist_documentos cd
                JOIN checklist_registro cr ON cr.id_checklist = cd.id_checklist_registro
                JOIN checklist_tipo_documento td ON td.id_tipo_documento = cd.id_tipo_documento
                WHERE cr.id_vehiculo = %s
                ORDER BY cd.id_tipo_documento, cr.fecha_hora_registro DESC
            """, (vehiculo["id"],))
            docs = cursor.fetchall()

            # Consolidar historial de documentos
            documentos_dict = {}
            for doc in docs:
                key = doc["id_tipo_documento"]
                doc["fecha_hora_registro"] = doc["fecha_hora_registro"].strftime('%Y-%m-%d %H:%M:%S')
                if key not in documentos_dict:
                    documentos_dict[key] = {
                        "id_tipo_documento": key,
                        "tipo_documento": doc["tipo_documento"],
                        "estado_actual": doc["estado"],
                        "historial": []
                    }
                documentos_dict[key]["historial"].append({
                    "fecha": doc["fecha_hora_registro"],
                    "observacion": doc["observaciones"],
                    "usuario": doc["usuario_registro"],
                    "estado": doc["estado"]
                })
            documentos = list(documentos_dict.values())

            # Consolidar fallas en componentes
            cursor.execute("""
                SELECT cc.id_componente, cc.grupo, cc.posicion, cc.componente,
                    cd.estado, cd.observaciones, cr.usuario_registro, cr.fecha_hora_registro
                FROM checklist_detalle cd
                JOIN checklist_registro cr ON cr.id_checklist = cd.id_checklist_registro
                JOIN checklist_componentes cc ON cc.id_componente = cd.id_componente
                WHERE cr.id_vehiculo = %s
                ORDER BY cc.id_componente, cr.fecha_hora_registro DESC
            """, (vehiculo["id"],))
            comps = cursor.fetchall()

            # Consolidar historial de componentes
            componentes_dict = {}
            for comp in comps:
                key = comp["id_componente"]
                comp["fecha_hora_registro"] = comp["fecha_hora_registro"].strftime('%Y-%m-%d %H:%M:%S')
                if key not in componentes_dict:
                    componentes_dict[key] = {
                        "id_componente": key,
                        "grupo": comp["grupo"],
                        "posicion": comp["posicion"],
                        "componente": comp["componente"],
                        "estado_actual": comp["estado"],
                        "historial": []
                    }
                componentes_dict[key]["historial"].append({
                    "fecha": comp["fecha_hora_registro"],
                    "observacion": comp["observaciones"],
                    "usuario": comp["usuario_registro"],
                    "estado": comp["estado"]
                })
            componentes = list(componentes_dict.values())
            
            # Último checklist para obtener km_odometro
            cursor.execute("""
                SELECT km_odometro
                FROM checklist_registro
                WHERE id_vehiculo = %s
                ORDER BY fecha_hora_registro DESC
                LIMIT 1
            """, (vehiculo["id"],))

            checklist_info = cursor.fetchone()
            km_odometro = checklist_info["km_odometro"] if checklist_info else None

        return {
            "vehiculo": vehiculo,
            "observaciones_generales": observaciones_generales,
            "fallas_documentos": documentos,
            "fallas_componentes": componentes,
            "checklist": {
                    "km_odometro": km_odometro
            }
        }

    def obtener_filtros_reportes(self):
        """Devuelve valores únicos para filtros del reporte de fallas"""
        query = """
            SELECT DISTINCT
                v.placa,
                tv.nombre AS tipo_vehiculo,
                v.marca,
                cr.usuario_registro
            FROM checklist_registro cr
            JOIN vehiculos v ON cr.id_vehiculo = v.id
            JOIN checklist_tipo_vehiculo tv ON v.id_tipo_vehiculo = tv.id_tipo_vehiculo
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            resultados = cursor.fetchall()

        # Extraer columnas por índice
        placas = sorted(set(row[0] for row in resultados if row[0]))
        tipos = sorted(set(row[1] for row in resultados if row[1]))
        marcas = sorted(set(row[2] for row in resultados if row[2]))
        usuarios = sorted(set(row[3] for row in resultados if row[3]))

        return {
            "placas": placas,
            "tipos": tipos,
            "marcas": marcas,
            "usuarios": usuarios
        }
    # ---------- Reportes de Checklist ----------    
    def consultar_datos_reporte(
        self,
        fecha: Optional[str] = None,              # compatibilidad
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
        placa: Optional[str] = None,
        tipo_vehiculo: Optional[str] = None,
        marca: Optional[str] = None,
        estado: Optional[str] = None,
        usuario: Optional[str] = None
    ):
        condiciones_1, condiciones_2 = [], []
        params_1, params_2 = [], []

        # --- Fecha/rango ---
        if fecha_inicio and fecha_fin:
            condiciones_1.append("DATE(cr.fecha_hora_registro) BETWEEN %s AND %s")
            condiciones_2.append("DATE(cr.fecha_hora_registro) BETWEEN %s AND %s")
            params_1.extend([fecha_inicio, fecha_fin])
            params_2.extend([fecha_inicio, fecha_fin])
        elif fecha:
            condiciones_1.append("DATE(cr.fecha_hora_registro) = %s")
            condiciones_2.append("DATE(cr.fecha_hora_registro) = %s")
            params_1.append(fecha)
            params_2.append(fecha)

        # --- Otros filtros ---
        if placa:
            condiciones_1.append("v.placa = %s"); condiciones_2.append("v.placa = %s")
            params_1.append(placa);               params_2.append(placa)

        if tipo_vehiculo:
            condiciones_1.append("tv.nombre = %s"); condiciones_2.append("tv.nombre = %s")
            params_1.append(tipo_vehiculo);         params_2.append(tipo_vehiculo)

        if marca:
            condiciones_1.append("v.marca = %s"); condiciones_2.append("v.marca = %s")
            params_1.append(marca);               params_2.append(marca)

        if usuario:
            condiciones_1.append("cr.usuario_registro = %s"); condiciones_2.append("cr.usuario_registro = %s")
            params_1.append(usuario);                     params_2.append(usuario)

        if estado in ["Falla", "OK"]:
            estado_bool = (estado == "OK")
            condiciones_1.append("cd.estado = %s")
            condiciones_2.append("cdet.estado = %s")
            params_1.append(estado_bool)
            params_2.append(estado_bool)

        where_clause_1 = "WHERE " + " AND ".join(condiciones_1) if condiciones_1 else ""
        where_clause_2 = "WHERE " + " AND ".join(condiciones_2) if condiciones_2 else ""

        query = f"""
            SELECT * FROM (
                SELECT
                    cr.id_checklist,
                    cr.fecha_hora_registro AS fecha_reporte,
                    v.placa,
                    tv.nombre AS tipo_vehiculo,
                    v.marca,
                    v.linea,
                    v.modelo,
                    cr.inicio_turno,
                    cr.fin_turno,
                    cr.km_odometro,
                    cr.observaciones_generales,
                    'Documento' AS tipo,
                    td.nombre AS grupo,
                    NULL AS posicion,
                    td.nombre AS componente,
                    cd.observaciones,
                    cr.usuario_registro,
                    cd.estado AS estado_item,
                    cr.fecha_guardado
                FROM checklist_registro cr
                JOIN vehiculos v ON cr.id_vehiculo = v.id
                JOIN checklist_tipo_vehiculo tv ON v.id_tipo_vehiculo = tv.id_tipo_vehiculo
                JOIN checklist_documentos cd ON cr.id_checklist = cd.id_checklist_registro
                JOIN checklist_tipo_documento td ON cd.id_tipo_documento = td.id_tipo_documento
                {where_clause_1}

                UNION ALL

                SELECT
                    cr.id_checklist,
                    cr.fecha_hora_registro AS fecha_reporte,
                    v.placa,
                    tv.nombre AS tipo_vehiculo,
                    v.marca,
                    v.linea,
                    v.modelo,
                    cr.inicio_turno,
                    cr.fin_turno,
                    cr.km_odometro,
                    cr.observaciones_generales,
                    'Componente' AS tipo,
                    cc.grupo,
                    cc.posicion,
                    cc.componente,
                    cdet.observaciones,
                    cr.usuario_registro,
                    cdet.estado AS estado_item,
                    cr.fecha_guardado
                FROM checklist_registro cr
                JOIN vehiculos v ON cr.id_vehiculo = v.id
                JOIN checklist_tipo_vehiculo tv ON v.id_tipo_vehiculo = tv.id_tipo_vehiculo
                JOIN checklist_detalle cdet ON cr.id_checklist = cdet.id_checklist_registro
                JOIN checklist_componentes cc ON cdet.id_componente = cc.id_componente
                {where_clause_2}
            ) AS subconsulta
            ORDER BY fecha_guardado DESC;
        """

        params_final = tuple(params_1 + params_2)

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params_final)
            resultados = cursor.fetchall()

            for row in resultados:
                for campo in ["fecha_reporte", "fecha_guardado", "inicio_turno", "fin_turno"]:
                    if isinstance(row.get(campo), (datetime, time)):
                        row[campo] = (
                            row[campo].strftime("%Y-%m-%d %H:%M:%S")
                            if isinstance(row[campo], datetime)
                            else row[campo].strftime("%H:%M:%S")
                        )
            return resultados

    # ----------------------------------------------------------------------
    #Registros Kilómetros Base por Vehículo y Mantenimiento para los preoperacionales de motos
    # ----------------------------------------------------------------------
    def _get_id_vehiculo_por_placa(self, placa: str) -> int | None:
        with self.connection.cursor() as c:
            c.execute("SELECT id FROM public.vehiculos WHERE placa = %s", (placa,))
            row = c.fetchone()
            return int(row[0]) if row else None

    def obtener_km_base_por_placa(self, placa: str):
        """
        Devuelve mantenimientos ACTIVOS con su km_optimo y km_base/fecha_mtto (si existe) para la placa dada.
        items: [{ id_mtto, mantenimiento, km_optimo, km_base, fecha_mtto }]
        """
        idv = self._get_id_vehiculo_por_placa(placa)
        if not idv:
            raise ValueError("Vehículo no encontrado")

        sql = """
            SELECT 
                p.id AS id_mtto,
                p.mantenimiento,
                p.km_optimo,
                kb.km_base,
                kb.fecha_mtto
            FROM public.checklist_param_motos p
            LEFT JOIN public.checklist_mtto_km_base kb
              ON kb.id_mtto = p.id AND kb.id_vehiculo = %s
            WHERE p.estado = 1
            ORDER BY p.mantenimiento ASC
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(sql, (idv,))
            rows = c.fetchall()
        return rows

    def upsert_km_base_batch_por_placa(self, placa: str, items: list[dict]) -> dict:
        """
        items: [{ id_mtto:int, km_base:int|None, fecha_mtto:date|str|None }, ...]
        Inserta/actualiza km_base por (vehículo, mantenimiento).
        """
        idv = self._get_id_vehiculo_por_placa(placa)
        if not idv:
            raise ValueError("Vehículo no encontrado")

        q = """
            INSERT INTO public.checklist_mtto_km_base (id_vehiculo, id_mtto, km_base, fecha_mtto)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_vehiculo, id_mtto)
            DO UPDATE SET 
                km_base = EXCLUDED.km_base,
                fecha_mtto= EXCLUDED.fecha_mtto
        """
        updated = 0
        with self.connection.cursor() as c:
            try:
                for it in items:
                    id_mtto = int(it.get("id_mtto"))
                    km_base = it.get("km_base")
                    fecha_mtto = it.get("fecha_mtto") # date | str | None
                    
                    # Normaliza a int o None
                    if km_base is not None and str(km_base).strip() != "":
                        km_base = int(km_base)
                    else:
                        km_base = None

                    # psycopg soporta date y str ISO
                    c.execute(q, (idv, id_mtto, km_base, fecha_mtto))
                    updated += 1
                    
                self.connection.commit()
                return {"updated": updated}
            except Exception as e:
                self.connection.rollback()
                raise e

    # Refrescar MV mv_odometro_por_dia
    def refrescar_mv_odometro_por_dia(self, concurrently: bool = True) -> None:
        """
        Refresca la MV mv_odometro_por_dia para que vw_preop_historial_motos
        vea inmediatamente el último odómetro registrado.
        - CONCURRENTLY requiere índice único en la MV y ejecutarse fuera de transacción.
        """
        try:
            prev_autocommit = getattr(self.connection, "autocommit", False)
            self.connection.autocommit = True  # ejecutar fuera de transaction block
            with self.connection.cursor() as c:
                if concurrently:
                    c.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_odometro_por_dia;")
                else:
                    c.execute("REFRESH MATERIALIZED VIEW public.mv_odometro_por_dia;")
        except Exception as e:
            # No rompemos el flujo de guardado si falla el refresh
            print(f"[WARN] No se pudo refrescar MV: {e}")
        finally:
            # restaurar autocommit original
            try:
                self.connection.autocommit = prev_autocommit
            except Exception:
                pass

    # Obtener km anterior usando la vista vw_odometro_por_dia
    def obtener_km_anterior_por_vista(self, id_vehiculo: int, fecha_hora_registro: str):
        """
        Devuelve el último km odómetro registrado para el vehículo ANTES (o a la misma hora)
        de 'fecha_hora_registro'. Primero busca en checklist_registro (misma fecha incluida),
        si no hay, recurre a la vista diaria con fechas estrictamente anteriores.
        """
        # 1) Intentar con el último checklist (incluye mismo día)
        q_last_checklist = """
            SELECT cr.km_odometro
            FROM checklist_registro cr
            WHERE cr.id_vehiculo = %s
            AND cr.fecha_hora_registro <= %s
            ORDER BY cr.fecha_hora_registro DESC, cr.id_checklist DESC
            LIMIT 1
        """
        with self.connection.cursor() as cursor:
            cursor.execute(q_last_checklist, (id_vehiculo, fecha_hora_registro))
            row = cursor.fetchone()
            if row and row[0] is not None:
                try:
                    return int(row[0])
                except Exception:
                    pass  # si viene raro, seguimos con el fallback

        # 2) Fallback: vista por día (solo días estrictamente anteriores)
        q_prev_day = """
            SELECT km_dia
            FROM public.vw_odometro_por_dia
            WHERE id_vehiculo = %s
            AND fecha < DATE(%s)
            ORDER BY fecha DESC
            LIMIT 1
        """
        with self.connection.cursor() as cursor:
            cursor.execute(q_prev_day, (id_vehiculo, fecha_hora_registro))
            row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None

# ----------------------------------------------------------------------------------------------
# ------- Procesos - Subprocesos asignados al usuario para la flota de asistencia técnica -------
class Proceso_flota_asistencia:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = psycopg2.connect(dsn)

    def _cur(self):
        return self.connection.cursor(cursor_factory=RealDictCursor)

    # ---- Catálogo de procesos/subprocesos para UI ----
    def listar_procesos(self) -> List[Tuple[int, str, str]]:
        with self._cur() as c:
            c.execute("""
                SELECT p.id_proceso, p.proceso, p.subproceso
                FROM public.procesos p
                ORDER BY UPPER(p.proceso), UPPER(p.subproceso)
            """)
            rows = c.fetchall()
        # [(id_proceso, proceso, subproceso)]
        return [(r["id_proceso"], r["proceso"], r["subproceso"]) for r in rows]

    # ---- Devolver IDs de procesos asignados a un usuario ----
    def obtener_ids_procesos_usuario(self, user_id: int) -> List[int]:
        with self._cur() as c:
            c.execute("""
                SELECT id_proceso
                FROM public.roles_flota_asis
                WHERE user_id = %s
                ORDER BY id_proceso ASC
            """, (user_id,))
            rows = c.fetchall()
        return [r["id_proceso"] for r in rows]

    # ---- Reemplazar asignaciones (set completo) ----
    def reemplazar_asignaciones(self, user_id: int, procesos_ids: List[int]) -> None:
        with self.connection:
            with self._cur() as c:
                c.execute("DELETE FROM public.roles_flota_asis WHERE user_id = %s", (user_id,))
                if procesos_ids:
                    c.executemany("""
                        INSERT INTO public.roles_flota_asis (user_id, id_proceso)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, id_proceso) DO NOTHING
                    """, [(user_id, pid) for pid in procesos_ids])

    # ---- Utilidad: obtener id de usuario por username (si tu insert no retorna id) ----
    def obtener_id_usuario_por_username(self, username: str) -> Optional[int]:
        with self._cur() as c:
            c.execute("SELECT id FROM public.usuarios WHERE username = %s LIMIT 1", (username,))
            row = c.fetchone()
        return row["id"] if row else None

    # ---- cerrar conexión ----
    def close(self):
        try:
            self.connection.close()
        except:
            pass