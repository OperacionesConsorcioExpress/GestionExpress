"""
Sistema de Auditoría SGI
Versión: 1.0.0
Fecha: 30 de diciembre de 2025
Descripción: Gestión completa de auditoría para el módulo SGI
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from pathlib import Path
import json

# Cargar variables de entorno
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / ".env")

class AuditoriaSGI:
    """Clase para gestionar la auditoría completa del sistema SGI."""
    
    def __init__(self):
        """Inicializa la conexión a la base de datos."""
        # Obtener DATABASE_PATH del .env
        DATABASE_PATH = os.getenv("DATABASE_PATH", "").strip().strip('"')
        
        if not DATABASE_PATH:
            raise ValueError("DATABASE_PATH no configurado en .env")
        
        # Parsear la cadena de conexión
        try:
            # Formato: postgresql://usuario:password@host:puerto/database?opciones
            parts = DATABASE_PATH.replace("postgresql://", "").split("@")
            user_pass = parts[0].split(":")
            host_db = parts[1].split("/")
            host_port = host_db[0].split(":")
            db_options = host_db[1].split("?")
            
            self.db_config = {
                'host': host_port[0],
                'port': int(host_port[1]) if len(host_port) > 1 else 5432,
                'database': db_options[0],
                'user': user_pass[0],
                'password': user_pass[1] if len(user_pass) > 1 else "",
                'options': '-c search_path=sgi,public'
            }
            
            # Crear conexión
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            
            print("[OK] AuditoriaSGI inicializado correctamente")
            
        except Exception as e:
            print(f"[ERROR] Error al inicializar AuditoriaSGI: {e}")
            raise
    
    def registrar_log(
        self,
        usuario: str,
        accion: str,
        modulo: str,
        tipo_entidad: Optional[str] = None,
        codigo_registro: Optional[str] = None,
        detalle_registro: Optional[str] = None,
        datos_antes: Optional[Dict] = None,
        datos_despues: Optional[Dict] = None,
        motivo: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        duracion_ms: Optional[int] = None,
        resultado: str = 'exito',
        error_mensaje: Optional[str] = None
    ) -> Optional[int]:
        """
        Registra una acción en el log de auditoría.
        
        Args:
            usuario: Usuario que realiza la acción
            accion: Tipo de acción (CREAR, MODIFICAR, ELIMINAR, etc.)
            modulo: Módulo del sistema (PA, GC, RVD, etc.)
            tipo_entidad: Tipo de entidad afectada
            codigo_registro: Código del registro (ej: PA-2025-001)
            detalle_registro: Descripción detallada
            datos_antes: Estado anterior (dict)
            datos_despues: Estado posterior (dict)
            motivo: Motivo de la acción
            ip_address: Dirección IP del usuario
            user_agent: Navegador/cliente del usuario
            duracion_ms: Duración de la operación en milisegundos
            resultado: Resultado (exito, error, advertencia)
            error_mensaje: Mensaje de error si aplica
            
        Returns:
            ID del log creado o None si hay error
        """
        try:
            with self.connection.cursor() as cursor:
                # Convertir diccionarios a JSON
                datos_antes_json = json.dumps(datos_antes) if datos_antes else None
                datos_despues_json = json.dumps(datos_despues) if datos_despues else None
                
                query = """
                    SELECT sgi.registrar_auditoria(
                        %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, 
                        %s, %s, %s, %s, %s, %s
                    ) as log_id
                """
                
                params = (
                    usuario, accion, modulo, tipo_entidad, codigo_registro,
                    detalle_registro, datos_antes_json, datos_despues_json,
                    motivo, ip_address, user_agent, duracion_ms, resultado, error_mensaje
                )
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                log_id = result[0] if result else None
                
                self.connection.commit()
                
                print(f"✅ Auditoría registrada: {accion} {modulo} - ID: {log_id}")
                return log_id
                
        except Exception as e:
            print(f"❌ Error al registrar auditoría: {e}")
            self.connection.rollback()
            return None
    
    def log_accion_rapida(
        self,
        usuario: str,
        accion: str,
        modulo: str,
        codigo_registro: Optional[str] = None,
        detalle: Optional[str] = None,
        resultado: str = 'exito'
    ) -> Optional[int]:
        """
        Registra una acción simple de forma rápida.
        
        Args:
            usuario: Usuario que realiza la acción
            accion: Tipo de acción
            modulo: Módulo del sistema
            codigo_registro: Código del registro
            detalle: Descripción breve
            resultado: Resultado de la operación
            
        Returns:
            ID del log creado
        """
        return self.registrar_log(
            usuario=usuario,
            accion=accion,
            modulo=modulo,
            codigo_registro=codigo_registro,
            detalle_registro=detalle,
            resultado=resultado
        )
    
    def obtener_logs(
        self,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None,
        usuario: Optional[str] = None,
        accion: Optional[str] = None,
        modulo: Optional[str] = None,
        resultado: Optional[str] = None,
        codigo_registro: Optional[str] = None,
        page: int = 1,
        per_page: int = 25
    ) -> Dict[str, Any]:
        """
        Obtiene logs con filtros y paginación.
        
        Args:
            fecha_desde: Fecha inicial (formato: YYYY-MM-DD)
            fecha_hasta: Fecha final (formato: YYYY-MM-DD)
            usuario: Filtrar por usuario
            accion: Filtrar por acción
            modulo: Filtrar por módulo
            resultado: Filtrar por resultado
            codigo_registro: Filtrar por código
            page: Número de página
            per_page: Registros por página
            
        Returns:
            Dict con logs, total, páginas
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Construir query base
                where_clauses = []
                params = []
                
                if fecha_desde:
                    where_clauses.append("fecha_hora >= %s::timestamp")
                    params.append(fecha_desde)
                
                if fecha_hasta:
                    where_clauses.append("fecha_hora <= %s::timestamp + interval '1 day'")
                    params.append(fecha_hasta)
                
                if usuario:
                    where_clauses.append("usuario ILIKE %s")
                    params.append(f"%{usuario}%")
                
                if accion:
                    where_clauses.append("accion = %s")
                    params.append(accion)
                
                if modulo:
                    where_clauses.append("modulo = %s")
                    params.append(modulo)
                
                if resultado:
                    where_clauses.append("resultado = %s")
                    params.append(resultado)
                
                if codigo_registro:
                    where_clauses.append("codigo_registro ILIKE %s")
                    params.append(f"%{codigo_registro}%")
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # Contar total
                count_query = f"SELECT COUNT(*) as total FROM sgi.auditoria_log WHERE {where_sql}"
                cursor.execute(count_query, params)
                total = cursor.fetchone()['total']
                
                # Obtener logs paginados
                offset = (page - 1) * per_page
                query = f"""
                    SELECT 
                        id,
                        fecha_hora,
                        TO_CHAR(fecha_hora, 'DD/MM/YYYY HH24:MI:SS') as fecha_formateada,
                        usuario,
                        accion,
                        modulo,
                        tipo_entidad,
                        codigo_registro,
                        detalle_registro,
                        datos_antes,
                        datos_despues,
                        motivo,
                        ip_address,
                        user_agent,
                        duracion_ms,
                        resultado,
                        error_mensaje
                    FROM sgi.auditoria_log
                    WHERE {where_sql}
                    ORDER BY fecha_hora DESC
                    LIMIT %s OFFSET %s
                """
                
                params.extend([per_page, offset])
                cursor.execute(query, params)
                logs_raw = cursor.fetchall()
                
                # Convertir logs a diccionarios serializables (sin objetos datetime)
                logs = []
                for log in logs_raw:
                    log_dict = dict(log)
                    # Eliminar fecha_hora (ya tenemos fecha_formateada)
                    if 'fecha_hora' in log_dict:
                        del log_dict['fecha_hora']
                    logs.append(log_dict)
                
                return {
                    'success': True,
                    'logs': logs,
                    'total': total,
                    'page': page,
                    'per_page': per_page,
                    'total_pages': (total + per_page - 1) // per_page
                }
                
        except Exception as e:
            print(f"❌ Error al obtener logs: {e}")
            return {
                'success': False,
                'mensaje': str(e),
                'logs': [],
                'total': 0
            }
    
    def obtener_estadisticas(
        self,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Obtiene estadísticas de auditoría.
        
        Args:
            fecha_desde: Fecha inicial
            fecha_hasta: Fecha final
            
        Returns:
            Dict con estadísticas
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                where_clauses = ["1=1"]
                params = []
                
                if fecha_desde:
                    where_clauses.append("fecha_hora >= %s::timestamp")
                    params.append(fecha_desde)
                
                if fecha_hasta:
                    where_clauses.append("fecha_hora <= %s::timestamp + interval '1 day'")
                    params.append(fecha_hasta)
                
                where_sql = " AND ".join(where_clauses)
                
                # Estadísticas generales
                query = f"""
                    SELECT 
                        COUNT(*) as total_acciones,
                        COUNT(DISTINCT usuario) as total_usuarios,
                        COUNT(*) FILTER (WHERE resultado = 'exito') as acciones_exitosas,
                        COUNT(*) FILTER (WHERE resultado = 'error') as acciones_fallidas,
                        AVG(duracion_ms) FILTER (WHERE duracion_ms IS NOT NULL) as duracion_promedio,
                        MAX(fecha_hora) as ultima_actividad
                    FROM sgi.auditoria_log
                    WHERE {where_sql}
                """
                
                cursor.execute(query, params)
                stats = cursor.fetchone()
                
                # Acciones por módulo
                query_modulos = f"""
                    SELECT modulo, COUNT(*) as cantidad
                    FROM sgi.auditoria_log
                    WHERE {where_sql}
                    GROUP BY modulo
                    ORDER BY cantidad DESC
                """
                
                cursor.execute(query_modulos, params)
                por_modulo = cursor.fetchall()
                
                # Top usuarios
                query_usuarios = f"""
                    SELECT usuario, COUNT(*) as cantidad
                    FROM sgi.auditoria_log
                    WHERE {where_sql}
                    GROUP BY usuario
                    ORDER BY cantidad DESC
                    LIMIT 10
                """
                
                cursor.execute(query_usuarios, params)
                top_usuarios = cursor.fetchall()
                
                # Convertir estadísticas a formato JSON-serializable
                stats_dict = dict(stats)
                
                # Convertir datetime a string si existe
                if 'ultima_actividad' in stats_dict and stats_dict['ultima_actividad']:
                    stats_dict['ultima_actividad'] = stats_dict['ultima_actividad'].isoformat()
                
                # Convertir Decimal a float si existe
                if 'duracion_promedio' in stats_dict and stats_dict['duracion_promedio'] is not None:
                    stats_dict['duracion_promedio'] = float(stats_dict['duracion_promedio'])
                
                return {
                    'success': True,
                    'estadisticas': stats_dict,
                    'por_modulo': [dict(row) for row in por_modulo],
                    'top_usuarios': [dict(row) for row in top_usuarios]
                }
                
        except Exception as e:
            print(f"❌ Error al obtener estadísticas: {e}")
            return {
                'success': False,
                'mensaje': str(e)
            }
    
    def obtener_actividad_reciente(self, limite: int = 50) -> List[Dict]:
        """
        Obtiene las últimas acciones registradas.
        
        Args:
            limite: Número máximo de registros
            
        Returns:
            Lista de logs recientes
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        id,
                        TO_CHAR(fecha_hora, 'DD/MM/YYYY HH24:MI:SS') as fecha_formateada,
                        usuario,
                        accion,
                        modulo,
                        codigo_registro,
                        detalle_registro,
                        resultado,
                        duracion_ms
                    FROM sgi.auditoria_log
                    ORDER BY fecha_hora DESC
                    LIMIT %s
                """
                
                cursor.execute(query, (limite,))
                logs_raw = cursor.fetchall()
                
                # Convertir a lista de diccionarios simples
                return [dict(row) for row in logs_raw]
                
        except Exception as e:
            print(f"❌ Error al obtener actividad reciente: {e}")
            return []
    
    def obtener_logs_usuario(
        self,
        usuario: str,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None
    ) -> List[Dict]:
        """
        Obtiene todos los logs de un usuario específico.
        
        Args:
            usuario: Nombre del usuario
            fecha_desde: Fecha inicial
            fecha_hasta: Fecha final
            
        Returns:
            Lista de logs del usuario
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                where_clauses = ["usuario = %s"]
                params = [usuario]
                
                if fecha_desde:
                    where_clauses.append("fecha_hora >= %s::timestamp")
                    params.append(fecha_desde)
                
                if fecha_hasta:
                    where_clauses.append("fecha_hora <= %s::timestamp + interval '1 day'")
                    params.append(fecha_hasta)
                
                where_sql = " AND ".join(where_clauses)
                
                query = f"""
                    SELECT 
                        id,
                        TO_CHAR(fecha_hora, 'DD/MM/YYYY HH24:MI:SS') as fecha_formateada,
                        accion,
                        modulo,
                        codigo_registro,
                        resultado
                    FROM sgi.auditoria_log
                    WHERE {where_sql}
                    ORDER BY fecha_hora DESC
                """
                
                cursor.execute(query, params)
                logs_raw = cursor.fetchall()
                
                # Convertir a lista de diccionarios simples
                return [dict(row) for row in logs_raw]
                
        except Exception as e:
            print(f"❌ Error al obtener logs de usuario: {e}")
            return []
    
    def limpiar_logs_antiguos(self, meses: int = 12) -> int:
        """
        Elimina logs más antiguos que X meses.
        
        Args:
            meses: Número de meses de retención
            
        Returns:
            Número de registros eliminados
        """
        try:
            with self.connection.cursor() as cursor:
                query = "SELECT sgi.limpiar_auditoria_antigua(%s) as eliminados"
                cursor.execute(query, (meses,))
                result = cursor.fetchone()
                eliminados = result[0] if result else 0
                
                self.connection.commit()
                
                print(f"✅ Limpieza completada: {eliminados} registros eliminados")
                return eliminados
                
        except Exception as e:
            print(f"❌ Error al limpiar logs antiguos: {e}")
            self.connection.rollback()
            return 0
    
    def cerrar_conexion(self):
        """Cierra la conexión a la base de datos."""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("✅ Conexión de auditoría cerrada")


# Decorador para auditar funciones automáticamente
def auditar_accion(accion: str, modulo: str):
    """
    Decorador para auditar automáticamente una función.
    
    Uso:
        @auditar_accion('CREAR', 'PA')
        def crear_plan_accion(datos):
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            inicio = time.time()
            
            try:
                # Ejecutar función
                resultado = func(*args, **kwargs)
                
                # Calcular duración
                duracion_ms = int((time.time() - inicio) * 1000)
                
                # Registrar auditoría (si está disponible)
                try:
                    auditoria = AuditoriaSGI()
                    usuario = kwargs.get('usuario', 'system')
                    codigo = resultado.get('codigo') if isinstance(resultado, dict) else None
                    
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion=accion,
                        modulo=modulo,
                        codigo_registro=codigo,
                        duracion_ms=duracion_ms,
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                except Exception as e:
                    print(f"⚠️ No se pudo registrar auditoría: {e}")
                
                return resultado
                
            except Exception as e:
                # Registrar error
                duracion_ms = int((time.time() - inicio) * 1000)
                
                try:
                    auditoria = AuditoriaSGI()
                    usuario = kwargs.get('usuario', 'system')
                    
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion=accion,
                        modulo=modulo,
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass
                
                raise
        
        return wrapper
    return decorator
