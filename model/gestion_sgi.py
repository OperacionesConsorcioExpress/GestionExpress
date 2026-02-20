# VERSIÓN: 2.0 - Tabla actividades unificada: sgi_actividades (22-12-2025)
import os, re, time, uuid
import psycopg2
from psycopg2 import pool
import threading
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, date, timezone
from dotenv import load_dotenv
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# Importar sistema de cache ultra
try:
    from model.cache_ultra_sgi import cache_sgi_method, cache_invalidate_sgi, get_ultra_cache
    CACHE_AVAILABLE = True
    print("[OK] Ultra Cache SGI cargado correctamente")
except ImportError as e:
    print(f"[WARNING] Ultra Cache SGI no disponible: {e}")
    CACHE_AVAILABLE = False
    # Decorador dummy para cuando no hay cache
    def cache_sgi_method(query_type: str, ttl_seconds: Optional[int] = None):
        def decorator(func):
            return func
        return decorator

# Importar sistema de auditoría
try:
    from model.auditoria_sgi import AuditoriaSGI
    AUDITORIA_AVAILABLE = True
    print("[OK] Sistema de Auditoria SGI cargado correctamente")
except ImportError as e:
    print(f"[WARNING] Sistema de Auditoria SGI no disponible: {e}")
    AUDITORIA_AVAILABLE = False

# Cargar variables de entorno
_BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=_BASE_DIR / ".env")
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SGI_EVIDENCIAS_CONTAINER = os.getenv("SGI_EVIDENCIAS_CONTAINER", "")
SGI_EVIDENCIAS_PREFIX = os.getenv("SGI_EVIDENCIAS_PREFIX", "001-sgi")
SGI_EVIDENCIAS_SAS_MINUTES = int(os.getenv("SGI_EVIDENCIAS_SAS_MINUTES", "30"))
SGI_ANALISIS_CAUSAS_CONTAINER = os.getenv("SGI_ANALISIS_CAUSAS_CONTAINER", "001-sgi-analisis-causas")
SGI_ANALISIS_CAUSAS_PREFIX = os.getenv("SGI_ANALISIS_CAUSAS_PREFIX", "001-sgi/001-sgi-analisis-causas")
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

def _normalizar_segmento(texto: str) -> str:
    valor = (texto or "").strip()
    if not valor:
        return ""
    valor = valor.replace(" ", "-")
    valor = re.sub(r"[^A-Za-z0-9\\-_]", "", valor)
    valor = re.sub(r"-{2,}", "-", valor)
    return valor

def _normalizar_nombre_archivo(nombre: str) -> str:
    base = os.path.basename(nombre or "").strip()
    if not base:
        base = "archivo"
    base = base.replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9._-]", "", base)
    if not base:
        base = "archivo"
    return base

def _normalizar_codigo_ruta(codigo: str) -> str:
    valor = (codigo or "").strip()
    if not valor:
        return ""
    valor = valor.replace(" ", "-")
    valor = re.sub(r"[^A-Za-z0-9_-]", "", valor)
    valor = re.sub(r"-{2,}", "-", valor)
    return valor

def _normalizar_prefijo_ruta(prefijo: str) -> str:
    return (prefijo or "").strip().strip("/")

def _generar_nombre_archivo_evidencia(nombre: str, fecha: datetime) -> str:
    base = _normalizar_nombre_archivo(nombre)
    sufijo = uuid.uuid4().hex[:6]
    timestamp = fecha.strftime("%Y%m%dT%H%M%S")
    return f"{timestamp}_{sufijo}_{base}"

def _parse_storage_connection_string() -> Dict[str, str]:
    partes: Dict[str, str] = {}
    for item in (AZURE_STORAGE_CONNECTION_STRING or "").split(";"):
        if not item:
            continue
        if "=" in item:
            key, value = item.split("=", 1)
            partes[key] = value
    return partes

def _build_content_disposition(nombre_archivo: str) -> str:
    safe_name = (nombre_archivo or "archivo").replace('"', '')
    return f'inline; filename="{safe_name}"'

_POOL_LOCK = threading.Lock()
_DB_POOL = None

def _get_pool():
    global _DB_POOL
    if _DB_POOL is None:
        with _POOL_LOCK:
            if _DB_POOL is None:
                maxconn = int(os.getenv("DB_POOL_MAX", "10"))
                print(f"DB pool SGI configurado con maxconn={maxconn}")
                _DB_POOL = pool.ThreadedConnectionPool(
                    1,
                    maxconn,
                    dsn=DATABASE_PATH,
                    options='-c timezone=America/Bogota'
                )
    return _DB_POOL

class GestionSGI:
    """Gestión del Sistema Integral de Calidad - SGI."""

    def __init__(self):
        try:
            if getattr(self, "connection", None):
                self.cerrar_conexion()

            self.connection = None
            pool_con = _get_pool()
            ultimo_error = None
            for intento in range(5):
                try:
                    self.connection = pool_con.getconn()
                    break
                except pool.PoolError as error_pool:
                    ultimo_error = error_pool
                    time.sleep(0.2 * (intento + 1))
            if not self.connection:
                raise ultimo_error if ultimo_error else psycopg2.OperationalError("No se pudo obtener conexión del pool")
            self.cursor = None
            self.connection.autocommit = False

            # Fallback defensivo (por si el options no se respeta en algún entorno)
            with self.connection.cursor() as c:
                c.execute("SET TIME ZONE 'America/Bogota';")
            self.connection.commit()

        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e

    def cerrar_conexion(self):
        cursor = getattr(self, "cursor", None)
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
            self.cursor = None
        connection = getattr(self, "connection", None)
        if connection:
            try:
                if not connection.closed:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                if connection.closed:
                    _get_pool().putconn(connection, close=True)
                else:
                    _get_pool().putconn(connection)
            except Exception:
                pass
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cerrar_conexion()

    def __del__(self):
        try:
            self.cerrar_conexion()
        except Exception:
            pass

    @cache_sgi_method('sgi_procesos', ttl_seconds=300)
    def obtener_procesos_sgi(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene la lista de procesos SGI con filtros opcionales."""
        try:
            query = """
                SELECT
                    codigo,
                    tipo,
                    proceso,
                    subproceso,
                    actividad,
                    fecha_cierre,
                    estado,
                    responsable,
                    fecha_creacion,
                    usuario_creacion
                FROM sgi_procesos
                WHERE 1=1
            """
            params = []

            if filtros:
                if filtros.get('usuario'):
                    query += " AND LOWER(responsable) LIKE %s"
                    params.append(f"%{filtros['usuario'].lower()}%")

                if filtros.get('tipo_gestion'):
                    query += " AND tipo = %s"
                    params.append(filtros['tipo_gestion'])

                if filtros.get('proceso'):
                    query += " AND LOWER(proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('fecha_desde'):
                    query += " AND fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir a lista de diccionarios con fechas serializadas
                procesos = []
                for row in resultados:
                    proceso = dict(row)
                    # Serializar fechas para JSON
                    proceso = self._serializar_fechas(proceso)
                    # Formatear fecha para mostrar
                    if proceso.get('fecha_cierre'):
                        try:
                            from datetime import datetime
                            if isinstance(proceso['fecha_cierre'], str):
                                fecha_obj = datetime.strptime(proceso['fecha_cierre'], '%Y-%m-%d').date()
                            else:
                                fecha_obj = proceso['fecha_cierre']
                            proceso['fecha_cierre_formato'] = fecha_obj.strftime('%d/%m/%Y')
                        except:
                            proceso['fecha_cierre_formato'] = str(proceso['fecha_cierre'])
                    procesos.append(proceso)

                return procesos

        except Exception as e:
            print(f"Error al obtener procesos SGI: {e}")
            return []

    @cache_sgi_method('sgi_procesos_ultra', ttl_seconds=120)
    def obtener_procesos_sgi_ultra(self, filtros: Dict = None) -> List[Dict]:
        """🚀 VERSIÓN ULTRA-OPTIMIZADA: Consultas SGI <100ms usando vista materializada."""
        import time

        try:
            start_time = time.time()

            # Usar vista materializada ultra-optimizada
            query = """
                SELECT
                    codigo,
                    tipo,
                    proceso,
                    subproceso,
                    actividad,
                    fecha_cierre,
                    estado,
                    responsable,
                    fecha_creacion,
                    usuario_creacion,
                    urgencia
                FROM mv_sgi_procesos_ultra
                WHERE 1=1
            """
            params = []

            if filtros:
                # Filtros exactos (ultra-rápidos con índices)
                if filtros.get('tipo_gestion'):
                    query += " AND tipo = %s"
                    params.append(filtros['tipo_gestion'])

                if filtros.get('estado'):
                    query += " AND estado = %s"
                    params.append(filtros['estado'])

                # Búsquedas optimizadas con ILIKE (más rápido que LOWER + LIKE)
                if filtros.get('usuario'):
                    query += " AND responsable_search LIKE %s"
                    params.append(f"%{filtros['usuario'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND proceso_search LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND subproceso_search LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                # Rangos de fecha (con índices optimizados)
                if filtros.get('fecha_desde'):
                    query += " AND fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

                # Búsqueda full-text ultra-rápida (si se proporciona)
                if filtros.get('busqueda_general'):
                    query += " AND texto_busqueda_completo @@ plainto_tsquery('spanish', %s)"
                    params.append(filtros['busqueda_general'])

            # Ordenamiento optimizado usando índice
            query += " ORDER BY fecha_creacion DESC"

            # Limitar resultados para evitar timeout
            limite = 500  # Por defecto
            if filtros and filtros.get('limit'):
                limite = min(int(filtros['limit']), 1000)
            query += f" LIMIT {limite}"

            # Ejecutar consulta ultra-optimizada
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                query_time = (time.time() - start_time) * 1000
                print(f"⚡ Consulta SGI ULTRA ejecutada en {query_time:.2f}ms ({len(resultados)} registros)")

                # Procesamiento mínimo para máxima velocidad
                return self._procesar_resultados_ultra(resultados)

        except Exception as e:
            print(f"❌ Error en consulta ultra-optimizada: {e}")
            print("🔄 Fallback a consulta estándar...")
            # Si falla la vista materializada, usar consulta estándar
            return self.obtener_procesos_sgi(filtros)

    def _procesar_resultados_ultra(self, resultados) -> List[Dict]:
        """🚀 Procesamiento ultra-rápido de resultados SGI."""
        procesos = []

        for row in resultados:
            proceso = dict(row)

            # Formateo mínimo y eficiente de fechas
            if proceso.get('fecha_cierre'):
                try:
                    if isinstance(proceso['fecha_cierre'], str):
                        fecha_obj = datetime.strptime(proceso['fecha_cierre'], '%Y-%m-%d').date()
                    else:
                        fecha_obj = proceso['fecha_cierre']
                    proceso['fecha_cierre_formato'] = fecha_obj.strftime('%d/%m/%Y')
                except:
                    proceso['fecha_cierre_formato'] = str(proceso['fecha_cierre'])

            # Agregar indicadores útiles para frontend
            proceso['es_urgente'] = proceso.get('urgencia') in ['Vencido', 'Próximo']
            proceso['es_vencido'] = proceso.get('urgencia') == 'Vencido'

            # Serializar fechas para JSON
            proceso = self._serializar_fechas(proceso)

            procesos.append(proceso)

        return procesos

    def refresh_vista_materializada_sgi(self) -> Dict:
        """🔄 Actualiza la vista materializada para máxima performance."""
        try:
            start_time = time.time()

            with self.connection.cursor() as cursor:
                # Refresh concurrente (no bloquea otras consultas)
                cursor.execute("SELECT refresh_sgi_ultra_performance()")
                self.connection.commit()

                refresh_time = (time.time() - start_time) * 1000
                print(f"✅ Vista materializada SGI actualizada en {refresh_time:.2f}ms")

                return {
                    "success": True,
                    "mensaje": f"Vista actualizada en {refresh_time:.2f}ms",
                    "tiempo_ms": refresh_time
                }

        except Exception as e:
            print(f"❌ Error actualizando vista materializada: {e}")
            return {
                "success": False,
                "mensaje": f"Error: {str(e)}"
            }

    def busqueda_sgi_inteligente(self, texto_busqueda: str, limite: int = 100) -> List[Dict]:
        """🔍 Búsqueda inteligente ultra-rápida con ranking de relevancia."""
        try:
            start_time = time.time()

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Usar función PostgreSQL optimizada
                cursor.execute("""
                    SELECT * FROM busqueda_sgi_ultra(%s, NULL, NULL, %s)
                """, (texto_busqueda, limite))

                resultados = cursor.fetchall()

                search_time = (time.time() - start_time) * 1000
                print(f"🔍 Búsqueda inteligente ejecutada en {search_time:.2f}ms ({len(resultados)} resultados)")

                return self._procesar_resultados_ultra(resultados)

        except Exception as e:
            print(f"❌ Error en búsqueda inteligente: {e}")
            return []

    def crear_proceso_sgi(self, datos: Dict) -> Dict:
        """Crea un nuevo proceso SGI."""
        try:
            # Generar código único
            codigo = self._generar_codigo_proceso(datos['tipo'], datos['proceso'])

            # Query principal para insertar el proceso
            query = """
                INSERT INTO sgi_procesos (
                    codigo, tipo, proceso, subproceso, cop, tipo_accion,
                    fecha_apertura, fecha_cierre, fuente, hallazgo, causa_raiz,
                    estado, responsable, usuario_creacion, fecha_creacion
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
            """

            params = (
                codigo,
                datos['tipo'],
                datos['proceso'],
                datos.get('subproceso'),
                datos.get('cop'),
                datos.get('tipo_accion'),
                datos.get('fecha_apertura'),
                datos['fecha_cierre'],
                datos.get('fuente'),
                datos.get('hallazgo'),
                datos.get('causa_raiz'),
                datos.get('estado', 'Pendiente'),  # Usar el estado proporcionado o 'Pendiente' por defecto
                datos.get('responsable', 'sin_asignar'),
                datos.get('usuario_creacion', 'system'),
                now_bogota()
            )

            cursor = self.connection.cursor()
            cursor.execute(query, params)
            proceso_id = cursor.fetchone()[0]

            # Insertar actividades PHVA si las hay
            actividades_phva = datos.get('actividades_phva', [])
            if actividades_phva:
                query_actividad = """
                    INSERT INTO sgi_actividades_phva (
                        proceso_codigo, phva, descripcion, fecha_ejecucion, responsable
                    ) VALUES (%s, %s, %s, %s, %s)
                """

                for actividad in actividades_phva:
                    params_actividad = (
                        codigo,
                        actividad['phva'],
                        actividad['descripcion'],
                        actividad['fecha_ejecucion'],
                        actividad['responsable']
                    )
                    cursor.execute(query_actividad, params_actividad)

            self.connection.commit()

            return {
                'success': True,
                'mensaje': 'Proceso SGI creado exitosamente',
                'proceso_id': proceso_id,
                'codigo': codigo
            }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al crear proceso SGI: {e}")
            return {
                'success': False,
                'mensaje': f'Error al crear proceso: {str(e)}'
            }

    def actualizar_estado_proceso(self, codigo: str, nuevo_estado: str, observacion: str = None) -> Dict:
        """Actualiza el estado de un proceso SGI."""
        try:
            query = """
                UPDATE sgi_procesos
                SET estado = %s,
                    observacion_estado = %s,
                    fecha_actualizacion = %s
                WHERE codigo = %s
            """

            params = (nuevo_estado, observacion, now_bogota(), codigo)

            cursor = self.connection.cursor()
            cursor.execute(query, params)

            if cursor.rowcount > 0:
                self.connection.commit()
                return {
                    'success': True,
                    'mensaje': f'Estado actualizado a: {nuevo_estado}'
                }
            else:
                return {
                    'success': False,
                    'mensaje': 'Proceso no encontrado'
                }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al actualizar estado: {e}")
            return {
                'success': False,
                'mensaje': f'Error al actualizar estado: {str(e)}'
            }

    def obtener_estadisticas_sgi(self) -> Dict:
        """Obtiene estadísticas de los procesos SGI."""
        try:
            query = """
                SELECT
                    estado,
                    COUNT(*) as cantidad,
                    COUNT(CASE WHEN fecha_cierre <= CURRENT_DATE THEN 1 END) as vencidos,
                    COUNT(CASE WHEN fecha_cierre BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN 1 END) as proximo_vencer
                FROM sgi_procesos
                GROUP BY estado
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                resultados = cursor.fetchall()

                estadisticas = {
                    'total': 0,
                    'pendientes': 0,
                    'en_proceso': 0,
                    'cerrados': 0,
                    'vencidos': 0,
                    'proximo_vencer': 0
                }

                for row in resultados:
                    estado = row['estado']
                    cantidad = row['cantidad']

                    estadisticas['total'] += cantidad

                    if estado == 'Pendiente':
                        estadisticas['pendientes'] = cantidad
                    elif estado == 'En proceso':
                        estadisticas['en_proceso'] = cantidad
                    elif estado == 'Cerrado':
                        estadisticas['cerrados'] = cantidad

                    estadisticas['vencidos'] += row['vencidos']
                    estadisticas['proximo_vencer'] += row['proximo_vencer']

                return estadisticas

        except Exception as e:
            print(f"Error al obtener estadísticas: {e}")
            return {}

    def _generar_codigo_proceso(self, tipo: str, proceso: str) -> str:
        """Genera un código único para el proceso."""
        try:
            año_actual = datetime.now().year

            # Obtener el siguiente número secuencial
            query = """
                SELECT COALESCE(MAX(
                    CAST(SPLIT_PART(codigo, '-', 4) AS INTEGER)
                ), 0) + 1 as siguiente_numero
                FROM sgi_procesos
                WHERE codigo LIKE %s
            """

            patron = f"{tipo}-{año_actual}-{proceso}-%"

            cursor = self.connection.cursor()
            cursor.execute(query, (patron,))
            siguiente_numero = cursor.fetchone()[0]

            # Formatear código
            codigo = f"{tipo}-{año_actual}-{proceso}-{str(siguiente_numero).zfill(4)}"

            return codigo

        except Exception as e:
            print(f"Error al generar código: {e}")
            # Fallback con timestamp
            timestamp = int(datetime.now().timestamp())
            return f"{tipo}-{datetime.now().year}-{proceso}-{timestamp}"

    def obtener_proceso_por_codigo(self, codigo: str) -> Dict:
        """Obtiene un proceso específico por su código."""
        try:
            query = """
                SELECT * FROM sgi_procesos
                WHERE codigo = %s
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo,))
                resultado = cursor.fetchone()

                if resultado:
                    proceso = dict(resultado)
                    # Obtener actividades PHVA asociadas
                    proceso['actividades_phva'] = self.obtener_actividades_phva(codigo)
                    return proceso
                else:
                    return {}

        except Exception as e:
            print(f"Error al obtener proceso por código: {e}")
            return {}

    def obtener_actividades_phva(self, proceso_codigo: str) -> List[Dict]:
        """Obtiene las actividades PHVA de un proceso específico."""
        try:
            query = """
                SELECT id, phva, descripcion, fecha_ejecucion, responsable, estado, fecha_creacion
                FROM sgi_actividades_phva
                WHERE proceso_codigo = %s
                ORDER BY fecha_ejecucion ASC
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (proceso_codigo,))
                resultados = cursor.fetchall()

                actividades = []
                for row in resultados:
                    actividad = dict(row)
                    # Formatear fecha para mostrar
                    if actividad['fecha_ejecucion']:
                        actividad['fecha_ejecucion_formato'] = actividad['fecha_ejecucion'].strftime('%d/%m/%Y')
                    actividades.append(actividad)

                return actividades

        except Exception as e:
            print(f"Error al obtener actividades PHVA: {e}")
            return []

    def actualizar_estado_actividad_phva(self, actividad_id: int, nuevo_estado: str) -> Dict:
        """Actualiza el estado de una actividad PHVA."""
        try:
            query = """
                UPDATE sgi_actividades_phva
                SET estado = %s
                WHERE id = %s
            """

            cursor = self.connection.cursor()
            cursor.execute(query, (nuevo_estado, actividad_id))

            if cursor.rowcount > 0:
                self.connection.commit()
                return {
                    'success': True,
                    'mensaje': f'Estado de actividad actualizado a: {nuevo_estado}'
                }
            else:
                return {
                    'success': False,
                    'mensaje': 'Actividad no encontrada'
                }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al actualizar estado de actividad: {e}")
            return {
                'success': False,
                'mensaje': f'Error al actualizar estado: {str(e)}'
            }

    # ============================================================================
    # GESTIÓN DE PROCESOS Y SUBPROCESOS
    # ============================================================================

    def obtener_categorias_procesos(self) -> List[Dict]:
        """Obtiene todas las categorías de procesos activas."""
        try:
            query = """
                SELECT
                    id, codigo, nombre, descripcion, icono, orden
                FROM sgi.categorias_procesos
                WHERE activo = TRUE
                ORDER BY orden
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            print(f"Error al obtener categorías de procesos: {e}")
            return []

    def obtener_procesos(self, categoria_id: Optional[int] = None) -> List[Dict]:
        """Obtiene todos los procesos activos, opcionalmente filtrados por categoría."""
        try:
            query = """
                SELECT
                    p.id, p.codigo, p.nombre, p.descripcion, p.categoria_id, p.orden,
                    c.codigo as categoria_codigo, c.nombre as categoria_nombre, c.icono as categoria_icono
                FROM sgi.procesos p
                INNER JOIN sgi.categorias_procesos c ON p.categoria_id = c.id
                WHERE p.activo = TRUE AND c.activo = TRUE
            """

            params = []
            if categoria_id:
                query += " AND p.categoria_id = %s"
                params.append(categoria_id)

            query += " ORDER BY c.orden, p.orden"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            print(f"Error al obtener procesos: {e}")
            return []

    def obtener_subprocesos(self, proceso_id: Optional[int] = None) -> List[Dict]:
        """Obtiene todos los subprocesos activos, opcionalmente filtrados por proceso."""
        try:
            query = """
                SELECT
                    s.id, s.codigo, s.nombre, s.descripcion, s.proceso_id, s.orden,
                    p.codigo as proceso_codigo, p.nombre as proceso_nombre,
                    c.codigo as categoria_codigo, c.nombre as categoria_nombre
                FROM sgi.subprocesos s
                INNER JOIN sgi.procesos p ON s.proceso_id = p.id
                INNER JOIN sgi.categorias_procesos c ON p.categoria_id = c.id
                WHERE s.activo = TRUE AND p.activo = TRUE AND c.activo = TRUE
            """

            params = []
            if proceso_id:
                query += " AND s.proceso_id = %s"
                params.append(proceso_id)

            query += " ORDER BY c.orden, p.orden, s.orden"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            print(f"Error al obtener subprocesos: {e}")
            return []

    def obtener_proceso_por_codigo(self, codigo: str) -> Optional[Dict]:
        """Obtiene un proceso específico por su código."""
        try:
            query = """
                SELECT
                    p.id, p.codigo, p.nombre, p.descripcion, p.categoria_id,
                    c.codigo as categoria_codigo, c.nombre as categoria_nombre
                FROM sgi.procesos p
                INNER JOIN sgi.categorias_procesos c ON p.categoria_id = c.id
                WHERE p.codigo = %s AND p.activo = TRUE
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo,))
                row = cursor.fetchone()
                return dict(row) if row else None

        except Exception as e:
            print(f"Error al obtener proceso por código: {e}")
            return None

    def obtener_subproceso_por_codigo(self, codigo: str, proceso_id: Optional[int] = None) -> Optional[Dict]:
        """Obtiene un subproceso específico por su código y opcionalmente proceso."""
        try:
            query = """
                SELECT
                    s.id, s.codigo, s.nombre, s.descripcion, s.proceso_id,
                    p.codigo as proceso_codigo, p.nombre as proceso_nombre
                FROM sgi.subprocesos s
                INNER JOIN sgi.procesos p ON s.proceso_id = p.id
                WHERE s.codigo = %s AND s.activo = TRUE
            """

            params = [codigo]
            if proceso_id:
                query += " AND s.proceso_id = %s"
                params.append(proceso_id)

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                return dict(row) if row else None

        except Exception as e:
            print(f"Error al obtener subproceso por código: {e}")
            return None

    def obtener_estructura_completa_procesos(self) -> Dict:
        """Obtiene la estructura completa de categorías, procesos y subprocesos."""
        try:
            # Obtener toda la estructura de una vez
            query = """
                SELECT
                    c.id as categoria_id, c.codigo as categoria_codigo, c.nombre as categoria_nombre,
                    c.icono as categoria_icono, c.orden as categoria_orden,
                    p.id as proceso_id, p.codigo as proceso_codigo, p.nombre as proceso_nombre,
                    p.orden as proceso_orden,
                    s.id as subproceso_id, s.codigo as subproceso_codigo, s.nombre as subproceso_nombre,
                    s.orden as subproceso_orden
                FROM sgi.categorias_procesos c
                LEFT JOIN sgi.procesos p ON c.id = p.categoria_id AND p.activo = TRUE
                LEFT JOIN sgi.subprocesos s ON p.id = s.proceso_id AND s.activo = TRUE
                WHERE c.activo = TRUE
                ORDER BY c.orden, p.orden, s.orden
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                # Organizar la estructura jerárquica
                estructura = {}

                for row in rows:
                    cat_id = row['categoria_id']
                    proc_id = row['proceso_id']

                    # Inicializar categoría si no existe
                    if cat_id not in estructura:
                        estructura[cat_id] = {
                            'id': cat_id,
                            'codigo': row['categoria_codigo'],
                            'nombre': row['categoria_nombre'],
                            'icono': row['categoria_icono'],
                            'orden': row['categoria_orden'],
                            'procesos': {}
                        }

                    # Agregar proceso si existe
                    if proc_id and proc_id not in estructura[cat_id]['procesos']:
                        estructura[cat_id]['procesos'][proc_id] = {
                            'id': proc_id,
                            'codigo': row['proceso_codigo'],
                            'nombre': row['proceso_nombre'],
                            'orden': row['proceso_orden'],
                            'subprocesos': []
                        }

                    # Agregar subproceso si existe
                    if row['subproceso_id'] and proc_id:
                        estructura[cat_id]['procesos'][proc_id]['subprocesos'].append({
                            'id': row['subproceso_id'],
                            'codigo': row['subproceso_codigo'],
                            'nombre': row['subproceso_nombre'],
                            'orden': row['subproceso_orden']
                        })

                return estructura

        except Exception as e:
            print(f"Error al obtener estructura completa de procesos: {e}")
            return {}

    def obtener_cops_unicos(self) -> List[str]:
        """Obtiene todos los valores únicos de COP de la tabla config.cop."""
        try:
            query = """
                SELECT DISTINCT cop
                FROM config.cop
                WHERE cop IS NOT NULL AND cop != ''
                ORDER BY cop ASC
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                resultados = cursor.fetchall()

                # Extraer solo los valores de COP en una lista
                cops = [row['cop'] for row in resultados]
                return cops

        except Exception as e:
            print(f"Error al obtener COPs únicos: {e}")
            return []

    # ==================== GESTIÓN DE PLANES DE ACCIÓN ====================

    def crear_plan_accion(self, datos: Dict) -> Dict:
        """Crea un nuevo plan de acción con código automático."""
        inicio = time.time()
        codigo_generado = None

        try:
            print(f"Iniciando creación de PA con datos: {datos}")

            # Verificar conexión
            if self.connection.closed:
                print("Conexión cerrada, reconectando...")
                self.__init__()

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Generar código automático usando la función de PostgreSQL
                codigo_query = """
                    SELECT generar_codigo_pa(%s, %s) as codigo
                """

                print(f"Ejecutando query para generar código...")
                cursor.execute(codigo_query, (datos['proceso'], datos['subproceso']))
                codigo_result = cursor.fetchone()
                codigo_generado = codigo_result['codigo']
                print(f"Código generado: {codigo_generado}")

                # Query principal para insertar el plan de acción
                insert_query = """
                    INSERT INTO sgi.planes_accion (
                        codigo, proceso, subproceso, cop, tipo_accion, estado,
                        fecha_apertura, fecha_cierre, fuente, hallazgo, causa_raiz,
                        responsable, usuario_creacion
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """

                params = (
                    codigo_generado,
                    datos['proceso'],
                    datos['subproceso'],
                    datos['cop'],
                    datos['tipo_accion'],
                    datos.get('estado', 'Pendiente'),
                    datos['fecha_apertura'],
                    datos['fecha_cierre'],
                    datos.get('fuente'),
                    datos['hallazgo'],
                    datos['causa_raiz'],
                    datos.get('responsable') or 'Sin asignar',
                    datos.get('usuario_creacion', 'system')
                )

                print(f"Ejecutando insert de plan de acción...")
                cursor.execute(insert_query, params)
                plan_id = cursor.fetchone()['id']
                print(f"Plan de acción insertado con ID: {plan_id}")

                # Insertar actividades PHVA si las hay
                actividades_phva = datos.get('actividades_phva', [])
                if actividades_phva:
                    print(f"Insertando {len(actividades_phva)} actividades PHVA...")
                    # Usar la nueva tabla unificada sgi_actividades
                    actividad_query = """
                        INSERT INTO sgi.sgi_actividades (
                            codigo_referencia, tipo_origen, tipo_actividad, etapa_actividad,
                            nombre_actividad, descripcion, fecha_cierre, responsable, categoria, estado
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    for actividad in actividades_phva:
                        etapa_actividad = (actividad.get('phva') or '').strip().upper()
                        if etapa_actividad not in {'P', 'H', 'V', 'A'}:
                            raise ValueError('PHVA invalido en actividades del PA')
                        tipo_actividad = "CORRECTIVA" if (actividad.get('tipo') or '').strip().lower() == 'correctiva' else "PLAN"

                        # Mapear campos del frontend a la tabla unificada
                        actividad_params = (
                            codigo_generado,  # codigo_referencia
                            'PA',  # tipo_origen
                            tipo_actividad,  # tipo_actividad (CORRECTIVA/PLAN)
                            etapa_actividad,  # etapa_actividad (P, H, V, A)
                            actividad.get('nombre_actividad', ''),  # nombre_actividad
                            actividad.get('descripcion', ''),  # descripcion
                            actividad.get('fecha_ejecucion', actividad.get('fecha_cierre')),  # fecha_cierre
                            actividad.get('responsable', ''),  # responsable
                            'PHVA',  # categoria
                            actividad.get('estado') or datos.get('estado') or 'Pendiente'  # estado
                        )
                        cursor.execute(actividad_query, actividad_params)

                # Confirmar transacción
                self.connection.commit()
                print("Transacción confirmada exitosamente")

                # AUDITORÍA: Registrar creación exitosa
                if AUDITORIA_AVAILABLE:
                    try:
                        duracion_ms = int((time.time() - inicio) * 1000)
                        auditoria = AuditoriaSGI()
                        auditoria.registrar_log(
                            usuario=datos.get('usuario_creacion', 'system'),
                            accion='CREAR',
                            modulo='PA',
                            tipo_entidad='Plan de Acción',
                            codigo_registro=codigo_generado,
                            detalle_registro=f"Plan creado: {datos.get('hallazgo', '')[:100]}",
                            datos_despues={
                                'codigo': codigo_generado,
                                'proceso': datos['proceso'],
                                'subproceso': datos['subproceso'],
                                'tipo_accion': datos['tipo_accion'],
                                'responsable': datos.get('responsable'),
                                'fecha_cierre': str(datos['fecha_cierre']),
                                'num_actividades': len(actividades_phva)
                            },
                            motivo=f"Fuente: {datos.get('fuente', 'No especificada')}",
                            duracion_ms=duracion_ms,
                            resultado='exito'
                        )
                        auditoria.cerrar_conexion()
                        print(f"✅ Auditoría registrada para PA {codigo_generado}")
                    except Exception as e:
                        print(f"⚠️ No se pudo registrar auditoría: {e}")

                return {
                    'success': True,
                    'mensaje': 'Plan de Acción creado exitosamente',
                    'plan_id': plan_id,
                    'codigo': codigo_generado
                }

        except psycopg2.Error as db_error:
            print(f"Error de PostgreSQL: {db_error}")
            self.connection.rollback()

            # AUDITORÍA: Registrar error de base de datos
            if AUDITORIA_AVAILABLE and codigo_generado:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=datos.get('usuario_creacion', 'system'),
                        accion='CREAR',
                        modulo='PA',
                        tipo_entidad='Plan de Acción',
                        codigo_registro=codigo_generado,
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(db_error)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': f'Error de base de datos: {str(db_error)}'
            }
        except Exception as e:
            print(f"Error general al crear plan de acción: {e}")
            self.connection.rollback()

            # AUDITORÍA: Registrar error general
            if AUDITORIA_AVAILABLE:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=datos.get('usuario_creacion', 'system'),
                        accion='CREAR',
                        modulo='PA',
                        tipo_entidad='Plan de Acción',
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': f'Error al crear plan de acción: {str(e)}'
            }

    def obtener_codigos_planes_accion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene la lista de códigos de TODOS los tipos de gestión con filtros jerárquicos."""
        try:
            tipo_gestion = filtros.get('tipo_gestion') if filtros else None

            if tipo_gestion == 'PA':
                return self._obtener_codigos_pa(filtros)
            elif tipo_gestion == 'GC':
                return self._obtener_codigos_gc(filtros)
            elif tipo_gestion == 'RVD':
                return self._obtener_codigos_rvd(filtros)
            else:
                # Si no hay tipo específico, obtener códigos de todos los tipos
                return self._obtener_todos_los_codigos(filtros)

        except Exception as e:
            print(f"Error al obtener códigos: {e}")
            return []

    def _obtener_codigos_pa(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene códigos solo de Planes de Acción."""
        try:
            query = """
                SELECT DISTINCT
                    pa.codigo,
                    pa.proceso,
                    pa.subproceso,
                    'PA' as tipo_gestion,
                    pa.estado,
                    pa.fecha_creacion::date as fecha_creacion_date,
                    pa.tipo_accion,
                    pa.fecha_apertura,
                    pa.fecha_cierre,
                    COALESCE(pa.responsable, 'Sin asignar') as responsable
                FROM sgi.planes_accion pa
                WHERE 1=1
            """
            params = []

            if filtros:
                if filtros.get('proceso'):
                    query += " AND LOWER(pa.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(pa.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

            query += " ORDER BY fecha_creacion_date DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()
                return self._formatear_codigos(resultados)

        except Exception as e:
            print(f"Error al obtener códigos PA: {e}")
            return []

    def _obtener_codigos_gc(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene códigos solo de Gestión del Cambio."""
        try:
            query = """
                SELECT DISTINCT
                    gc.codigo,
                    gc.proceso,
                    gc.subproceso,
                    'GC' as tipo_gestion,
                    gc.estado,
                    gc.fecha_creacion::date as fecha_creacion_date,
                    gc.nombre_cambio,
                    gc.fecha_apertura,
                    gc.fecha_cierre,
                    COALESCE(gc.responsable, 'Sin asignar') as responsable
                FROM sgi.gestion_cambio gc
                WHERE gc.activo = TRUE
            """
            params = []

            if filtros:
                if filtros.get('proceso'):
                    query += " AND LOWER(gc.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(gc.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

            query += " ORDER BY fecha_creacion_date DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()
                return self._formatear_codigos(resultados)

        except Exception as e:
            print(f"Error al obtener códigos GC: {e}")
            return []

    def _obtener_codigos_rvd(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene códigos solo de Revisión por Dirección."""
        try:
            query = """
                SELECT
                    codigo,
                    proceso,
                    subproceso,
                    'RVD' as tipo_gestion,
                    estado,
                    fecha_creacion::date as fecha_creacion_date,
                    ano_rvd,
                    actividad as accion_compromiso,
                    responsable,
                    fecha_apertura,
                    fecha_cierre
                FROM sgi.revision_direccion
                WHERE activo = TRUE
            """
            params = []

            if filtros:
                if filtros.get('proceso'):
                    query += " AND LOWER(proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()
                return self._formatear_codigos(resultados)

        except Exception as e:
            print(f"Error al obtener códigos RVD: {e}")
            return []

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()
                return self._formatear_codigos(resultados)

        except Exception as e:
            print(f"Error al obtener códigos RVD: {e}")
            return []

    def _obtener_todos_los_codigos(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene códigos de todos los tipos de gestión combinados."""
        try:
            codigos_pa = self._obtener_codigos_pa(filtros)
            codigos_gc = self._obtener_codigos_gc(filtros)
            codigos_rvd = self._obtener_codigos_rvd(filtros)

            # Combinar todos los códigos
            todos_codigos = codigos_pa + codigos_gc + codigos_rvd

            # Ordenar por tipo y fecha
            todos_codigos.sort(key=lambda x: (x['tipo_gestion'], x['codigo']))

            return todos_codigos

        except Exception as e:
            print(f"Error al obtener todos los códigos: {e}")
            return []

    def _formatear_codigos(self, resultados) -> List[Dict]:
        """Formatea los resultados de códigos a la estructura esperada."""
        codigos = []
        for row in resultados:
            fecha_creacion_date = row.get('fecha_creacion_date')
            if hasattr(fecha_creacion_date, 'strftime'):
                fecha_creacion_formato = fecha_creacion_date.strftime('%d/%m/%Y')
            elif fecha_creacion_date:
                fecha_creacion_formato = str(fecha_creacion_date)
            else:
                fecha_creacion_formato = ''

            row_serializado = self._serializar_fechas(dict(row))
            codigo_info = {
                'codigo': row_serializado.get('codigo'),
                'proceso': row_serializado.get('proceso'),
                'subproceso': row_serializado.get('subproceso'),
                'tipo_gestion': row_serializado.get('tipo_gestion'),
                'estado': row_serializado.get('estado'),
                'fecha_creacion_formato': fecha_creacion_formato,
                'responsable': row_serializado.get('responsable', None),  # Agregar responsable
                'tipo_accion': row_serializado.get('tipo_accion', None),  # Para PA
                'nombre_cambio': row_serializado.get('nombre_cambio', None),  # Para GC
                'accion_compromiso': row_serializado.get('accion_compromiso', None),  # Para RVD
                'fecha_apertura': row_serializado.get('fecha_apertura', None),
                'fecha_cierre': row_serializado.get('fecha_cierre', None)
            }
            codigos.append(codigo_info)
        return codigos

    def obtener_planes_accion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene la lista de planes de acción con filtros opcionales."""
        try:
            query = """
                SELECT
                    pa.codigo,
                    'PA' as tipo,
                    pa.proceso,
                    pa.subproceso,
                    pa.cop,
                    pa.tipo_accion,
                    pa.estado,
                    pa.fecha_apertura,
                    pa.fecha_cierre,
                    pa.fuente,
                    pa.hallazgo,
                    pa.causa_raiz,
                    COALESCE(pa.responsable, 'Sin asignar') as responsable,
                    pa.usuario_creacion,
                    pa.fecha_creacion
                FROM sgi.planes_accion pa
                WHERE 1=1
            """
            params = []

            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(pa.codigo) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(pa.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(pa.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND pa.estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('responsable'):
                    query += " AND LOWER(COALESCE(pa.responsable, '')) LIKE %s"
                    params.append(f"%{filtros['responsable'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND pa.fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND pa.fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY pa.fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir a lista de diccionarios y formatear fechas
                planes = []
                for row in resultados:
                    plan = dict(row)
                    # Formatear fechas para mostrar
                    if plan['fecha_cierre']:
                        plan['fecha_cierre_formato'] = plan['fecha_cierre'].strftime('%d/%m/%Y')
                    if plan['fecha_apertura']:
                        plan['fecha_apertura_formato'] = plan['fecha_apertura'].strftime('%d/%m/%Y')
                    planes.append(plan)

                return planes

        except Exception as e:
            print(f"Error al obtener planes de acción: {e}")
            return []

    def obtener_plan_accion_por_codigo(self, codigo: str) -> Optional[Dict]:
        """Obtiene un plan de acción específico por su código."""
        try:
            query = """
                SELECT
                    pa.*
                FROM sgi.planes_accion pa
                WHERE pa.codigo = %s
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo,))
                row = cursor.fetchone()

                if row:
                    plan = dict(row)

                    # Obtener actividades de la tabla unificada
                    actividades_query = """
                        SELECT
                            id,
                            codigo_referencia,
                            tipo_origen,
                            etapa_actividad,
                            nombre_actividad,
                            descripcion,
                            fecha_cierre,
                            responsable,
                            estado,
                            categoria,
                            fecha_creacion,
                            created_at
                        FROM sgi.sgi_actividades
                        WHERE codigo_referencia = %s AND tipo_origen = 'PA'
                        ORDER BY fecha_cierre
                    """
                    cursor.execute(actividades_query, (codigo,))
                    actividades = [dict(act) for act in cursor.fetchall()]

                    plan['actividades_phva'] = actividades
                    return plan

                return None

        except Exception as e:
            print(f"Error al obtener plan de acción por código: {e}")
            return None

    # ==================== GESTIÓN DEL CAMBIO (GC) ====================

    def crear_gestion_cambio(self, datos: Dict) -> Dict:
        """Crea una nueva gestión del cambio con actividades."""
        inicio = time.time()
        codigo_generado = None

        try:
            print(f"Iniciando creación de GC con datos: {datos}")

            # Verificar conexión
            if self.connection.closed:
                print("Conexión cerrada, reconectando...")
                self.__init__()

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Generar código automático usando la función de PostgreSQL
                codigo_query = """
                    SELECT generar_codigo_gc(%s, %s) as codigo
                """

                print(f"Ejecutando query para generar código GC...")
                cursor.execute(codigo_query, (datos['proceso'], datos['subproceso']))
                codigo_result = cursor.fetchone()
                codigo_generado = codigo_result['codigo']
                print(f"Código GC generado: {codigo_generado}")

                # Query optimizado - SOLO columnas que existen en la tabla (incluyendo categoria)
                insert_query = """
                    INSERT INTO sgi.gestion_cambio (
                        codigo, proceso, subproceso, cop, estado, etapa,
                        fecha_apertura, fecha_cierre, nombre_cambio,
                        responsable, usuario_creacion, categoria
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """

                # Preparar datos usando solo campos que existen
                params = (
                    codigo_generado,
                    datos['proceso'],
                    datos['subproceso'],
                    datos['cop'],
                    datos.get('estado', 'Abierto'),
                    datos.get('etapa', 'Antes'),  # Cambiado default a 'Antes'
                    datos['fecha_apertura'],
                    datos.get('fecha_cierre'),
                    datos['nombre_cambio'],
                    datos.get('responsable') or 'Sin asignar',
                    datos.get('usuario_creacion', 'system'),
                    datos.get('categoria', 'CAMBIO')  # Agregar categoria con valor por defecto
                )

                print(f"Ejecutando INSERT de Gestión de Cambio...")
                cursor.execute(insert_query, params)

                # Obtener el ID insertado
                gc_result = cursor.fetchone()
                gc_id = gc_result['id']
                print(f"Insert completado: registro GC creado con ID {gc_id}")

                actividades = datos.get('actividades', [])
                if actividades:
                    print(f"Insertando {len(actividades)} actividades de GC en sgi_actividades...")
                    actividad_query = """
                        INSERT INTO sgi.sgi_actividades (
                            codigo_referencia, tipo_origen, etapa_actividad, nombre_actividad,
                            descripcion, fecha_cierre, responsable, categoria, estado
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    for actividad in actividades:
                        etapa = (actividad.get('etapa') or 'Antes').strip()
                        etapa_actividad = etapa[:10]

                        actividad_params = (
                            codigo_generado,  # codigo_referencia
                            'GC',  # tipo_origen
                            etapa_actividad,  # etapa_actividad
                            actividad.get('nombre_actividad', ''),  # nombre_actividad
                            actividad.get('descripcion', ''),  # descripcion
                            actividad.get('fecha_ejecucion'),  # fecha_cierre
                            actividad.get('responsable', ''),  # responsable
                            datos.get('categoria', 'CAMBIO'),  # categoria
                            datos.get('estado', 'Abierto')  # estado
                        )
                        cursor.execute(actividad_query, actividad_params)

                # Confirmar transacción
                self.connection.commit()
                print(f"Transacción GC confirmada exitosamente. ID: {gc_id}")

                # AUDITORÍA: Registrar creación exitosa de GC
                if AUDITORIA_AVAILABLE:
                    try:
                        duracion_ms = int((time.time() - inicio) * 1000)
                        auditoria = AuditoriaSGI()
                        auditoria.registrar_log(
                            usuario=datos.get('usuario_creacion', 'system'),
                            accion='CREAR',
                            modulo='GC',
                            tipo_entidad='Gestión de Cambio',
                            codigo_registro=codigo_generado,
                            detalle_registro=f"GC creada: {datos.get('nombre_cambio', '')[:100]}",
                            datos_despues={
                                'codigo': codigo_generado,
                                'proceso': datos['proceso'],
                                'subproceso': datos['subproceso'],
                                'etapa': datos.get('etapa', 'Antes'),
                                'responsable': datos.get('responsable'),
                                'categoria': datos.get('categoria', 'CAMBIO'),
                                'num_actividades': len(actividades)
                            },
                            duracion_ms=duracion_ms,
                            resultado='exito'
                        )
                        auditoria.cerrar_conexion()
                        print(f"✅ Auditoría registrada para GC {codigo_generado}")
                    except Exception as e:
                        print(f"⚠️ No se pudo registrar auditoría: {e}")

                return {
                    'success': True,
                    'mensaje': f'Gestión del Cambio creada exitosamente',
                    'gc_id': gc_id,
                    'codigo': codigo_generado
                }

        except psycopg2.Error as db_error:
            print(f"Error de PostgreSQL en GC: {db_error}")
            self.connection.rollback()

            # AUDITORÍA: Registrar error de base de datos
            if AUDITORIA_AVAILABLE and codigo_generado:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=datos.get('usuario_creacion', 'system'),
                        accion='CREAR',
                        modulo='GC',
                        tipo_entidad='Gestión de Cambio',
                        codigo_registro=codigo_generado,
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(db_error)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': f'Error de base de datos: {str(db_error)}'
            }
        except Exception as e:
            print(f"Error general al crear GC: {e}")
            self.connection.rollback()

            # AUDITORÍA: Registrar error general
            if AUDITORIA_AVAILABLE:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=datos.get('usuario_creacion', 'system'),
                        accion='CREAR',
                        modulo='GC',
                        tipo_entidad='Gestión de Cambio',
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': f'Error inesperado: {str(e)}'
            }

    def obtener_gestiones_cambio(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene lista de gestiones del cambio con filtros opcionales."""
        try:
            query = """
                SELECT
                    id, codigo, proceso, subproceso, cop, estado, etapa,
                    fecha_apertura, fecha_cierre, nombre_cambio, responsable,
                    actividad, descripcion, usuario_creacion, fecha_creacion,
                    CASE
                        WHEN fecha_cierre IS NULL THEN
                            CURRENT_DATE - fecha_apertura
                        ELSE
                            fecha_cierre - fecha_apertura
                    END as dias_transcurridos
                FROM sgi.gestion_cambio
                WHERE activo = true
            """

            params = []

            if filtros:
                if filtros.get('proceso'):
                    query += " AND LOWER(proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('etapa'):
                    query += " AND etapa = %s"
                    params.append(filtros['etapa'])

                if filtros.get('responsable'):
                    query += " AND LOWER(responsable) LIKE %s"
                    params.append(f"%{filtros['responsable'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND fecha_apertura >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND fecha_apertura <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir a lista de diccionarios
                gestiones = []
                for row in resultados:
                    gc = dict(row)

                    # Formatear fechas
                    if gc['fecha_apertura']:
                        gc['fecha_apertura'] = gc['fecha_apertura'].strftime('%Y-%m-%d')
                    if gc['fecha_cierre']:
                        gc['fecha_cierre'] = gc['fecha_cierre'].strftime('%Y-%m-%d')
                    if gc['fecha_creacion']:
                        gc['fecha_creacion'] = gc['fecha_creacion'].strftime('%Y-%m-%d %H:%M:%S')

                    # Calcular estado del tiempo
                    if gc['estado'] == 'Cerrado':
                        gc['estado_tiempo'] = 'Completado'
                    elif gc['fecha_cierre'] and now_bogota().date() > gc['fecha_cierre']:
                        gc['estado_tiempo'] = 'Vencido'
                    elif gc['fecha_cierre'] and now_bogota().date() == gc['fecha_cierre']:
                        gc['estado_tiempo'] = 'Vence hoy'
                    elif gc['fecha_cierre'] and (now_bogota().date() + timedelta(days=7)) >= gc['fecha_cierre']:
                        gc['estado_tiempo'] = 'Próximo a vencer'
                    else:
                        gc['estado_tiempo'] = 'En tiempo'

                    gestiones.append(gc)

                return gestiones

        except Exception as e:
            print(f"Error al obtener gestiones del cambio: {e}")
            return []

    def obtener_gestion_cambio_por_codigo(self, codigo: str) -> Optional[Dict]:
        """Obtiene una gestión del cambio específica por su código."""
        try:
            query = """
                SELECT
                    id, codigo, proceso, subproceso, cop, estado, etapa,
                    fecha_apertura, fecha_cierre, nombre_cambio, responsable,
                    actividad, descripcion, usuario_creacion, fecha_creacion,
                    fecha_actualizacion, observaciones
                FROM sgi.gestion_cambio
                WHERE codigo = %s AND activo = true
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo,))
                resultado = cursor.fetchone()

                if resultado:
                    gc = dict(resultado)

                    # Formatear fechas
                    if gc['fecha_apertura']:
                        gc['fecha_apertura'] = gc['fecha_apertura'].strftime('%Y-%m-%d')
                    if gc['fecha_cierre']:
                        gc['fecha_cierre'] = gc['fecha_cierre'].strftime('%Y-%m-%d')
                    if gc['fecha_creacion']:
                        gc['fecha_creacion'] = gc['fecha_creacion'].strftime('%Y-%m-%d %H:%M:%S')
                    if gc['fecha_actualizacion']:
                        gc['fecha_actualizacion'] = gc['fecha_actualizacion'].strftime('%Y-%m-%d %H:%M:%S')

                    return gc

                return None

        except Exception as e:
            print(f"Error al obtener gestión del cambio por código: {e}")
            return None

    def obtener_estadisticas_gc(self) -> Dict:
        """Obtiene estadísticas de las gestiones del cambio."""
        try:
            query = """
                SELECT
                    COUNT(*) as total_gc,
                    COUNT(*) FILTER (WHERE estado = 'Pendiente') as pendientes,
                    COUNT(*) FILTER (WHERE estado = 'En proceso') as en_proceso,
                    COUNT(*) FILTER (WHERE estado = 'Abierta') as abiertas,
                    COUNT(*) FILTER (WHERE estado = 'Cerrado') as cerradas
                FROM sgi.gestion_cambio
                WHERE activo = true
            """

            query_etapas = """
                SELECT etapa, COUNT(*) as cantidad
                FROM sgi.gestion_cambio
                WHERE activo = true
                GROUP BY etapa
                ORDER BY etapa
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Estadísticas generales
                cursor.execute(query)
                stats = dict(cursor.fetchone())

                # Estadísticas por etapa
                cursor.execute(query_etapas)
                etapas = cursor.fetchall()
                stats['por_etapa'] = {row['etapa']: row['cantidad'] for row in etapas}

                return stats

        except Exception as e:
            print(f"Error al obtener estadísticas GC: {e}")
            return {
                'total_gc': 0,
                'pendientes': 0,
                'en_proceso': 0,
                'abiertas': 0,
                'cerradas': 0,
                'por_etapa': {}
            }

    # ==================== MÉTODOS PARA GENERAR CÓDIGOS AUTOMÁTICOS ====================

    def generar_codigo_pa(self, proceso: str, subproceso: str) -> str:
        """Genera código automático para Plan de Acción."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                codigo_query = "SELECT generar_codigo_pa(%s, %s) as codigo"
                cursor.execute(codigo_query, (proceso, subproceso))
                resultado = cursor.fetchone()
                return resultado['codigo']
        except Exception as e:
            print(f"Error al generar código PA: {e}")
            return f"PA-{proceso[:2].upper()}-{subproceso[:2].upper()}-0001"

    def generar_codigo_gc(self, proceso: str, subproceso: str) -> str:
        """Genera código automático para Gestión del Cambio."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                codigo_query = "SELECT generar_codigo_gc(%s, %s) as codigo"
                cursor.execute(codigo_query, (proceso, subproceso))
                resultado = cursor.fetchone()
                return resultado['codigo']
        except Exception as e:
            print(f"Error al generar código GC: {e}")
            return f"GC-{proceso[:2].upper()}-{subproceso[:2].upper()}-0001"

    def generar_codigo_rvd(self, proceso: str, subproceso: str) -> str:
        """Genera código automático para Revisión por la Dirección."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                codigo_query = "SELECT generar_codigo_rvd(%s, %s) as codigo"
                cursor.execute(codigo_query, (proceso, subproceso))
                resultado = cursor.fetchone()
                return resultado['codigo']
        except Exception as e:
            print(f"Error al generar código RVD: {e}")
            return f"RVD-{proceso[:2].upper()}-{subproceso[:2].upper()}-0001"

    # ==================== REVISIÓN POR LA DIRECCIÓN (RVD) ====================

    def crear_revision_direccion(self, datos: Dict[str, Any], usuario_sesion: str = "system") -> Dict[str, Any]:
        """
        Crea una nueva revisión por la dirección.

        Args:
            datos: Diccionario con los datos de la RVD
            usuario_sesion: Usuario que crea la RVD

        Returns:
            Diccionario con el resultado de la operación
        """
        inicio = time.time()
        codigo_generado = None

        try:
            print(f"Iniciando creación de RVD con datos: {datos}")

            # Verificar conexión
            if self.connection.closed:
                print("Conexión cerrada, reconectando...")
                self.__init__()

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Generar código automático usando la función de PostgreSQL
                codigo_query = """
                    SELECT generar_codigo_rvd(%s, %s) as codigo
                """

                print(f"Ejecutando query para generar código RVD...")
                cursor.execute(codigo_query, (datos['proceso'], datos['subproceso']))
                codigo_result = cursor.fetchone()
                codigo_generado = codigo_result['codigo']
                print(f"Código RVD generado: {codigo_generado}")

                # Query principal - INCLUIR ano_rvd que es obligatorio
                insert_query = """
                    INSERT INTO sgi.revision_direccion (
                        codigo, proceso, subproceso, cop, ano_rvd,
                        actividad, responsable, estado, fecha_apertura,
                        fecha_cierre, usuario_creacion, categoria
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """

                actividad = datos.get('actividad') or datos.get('accion_compromiso') or ''

                params = (
                    codigo_generado,
                    datos['proceso'],
                    datos['subproceso'],
                    datos['cop'],
                    datos.get('ano_rvd', datetime.now().year),  # Año RVD (obligatorio)
                    actividad,  # Acción/Compromiso
                    datos.get('responsable', ''),
                    datos.get('estado', 'Abierta'),
                    datos['fecha_apertura'],
                    datos.get('fecha_cierre'),
                    usuario_sesion,
                    datos.get('categoria', 'RVD')  # Categoría
                )

                print(f"Ejecutando insert de revisión por la dirección...")
                cursor.execute(insert_query, params)
                rvd_id = cursor.fetchone()['id']

                print(f"Insertando actividad de RVD en sgi_actividades...")
                actividad_query = """
                    INSERT INTO sgi.sgi_actividades (
                        codigo_referencia, tipo_origen, etapa_actividad, nombre_actividad,
                        descripcion, fecha_cierre, responsable, categoria, estado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                actividad_params = (
                    codigo_generado,  # codigo_referencia
                    'RVD',  # tipo_origen
                    'RVD',  # etapa_actividad
                    'Acción/Compromiso',  # nombre_actividad
                    actividad,  # descripcion
                    datos.get('fecha_cierre'),
                    datos.get('responsable', ''),
                    datos.get('categoria', 'RVD'),
                    datos.get('estado', 'Abierta')
                )
                cursor.execute(actividad_query, actividad_params)

                # Confirmar transacción
                self.connection.commit()

                print(f"✅ RVD creada exitosamente con ID: {rvd_id} y código: {codigo_generado}")

                # AUDITORÍA: Registrar creación exitosa de RVD
                if AUDITORIA_AVAILABLE:
                    try:
                        duracion_ms = int((time.time() - inicio) * 1000)
                        auditoria = AuditoriaSGI()
                        auditoria.registrar_log(
                            usuario=usuario_sesion,
                            accion='CREAR',
                            modulo='RVD',
                            tipo_entidad='Revisión por la Dirección',
                            codigo_registro=codigo_generado,
                            detalle_registro=f"RVD creada: {actividad[:100] if actividad else 'Sin detalle'}",
                            datos_despues={
                                'codigo': codigo_generado,
                                'proceso': datos['proceso'],
                                'subproceso': datos['subproceso'],
                                'ano_rvd': datos.get('ano_rvd', datetime.now().year),
                                'responsable': datos.get('responsable'),
                                'categoria': datos.get('categoria', 'RVD')
                            },
                            duracion_ms=duracion_ms,
                            resultado='exito'
                        )
                        auditoria.cerrar_conexion()
                        print(f"✅ Auditoría registrada para RVD {codigo_generado}")
                    except Exception as e:
                        print(f"⚠️ No se pudo registrar auditoría: {e}")

                return {
                    'success': True,
                    'mensaje': f'Revisión por la Dirección creada exitosamente',
                    'id': rvd_id,
                    'codigo': codigo_generado,
                    'datos': {
                        'proceso': datos['proceso'],
                        'subproceso': datos['subproceso'],
                        'cop': datos['cop'],
                        'ano_rvd': datos['ano_rvd'],
                        'responsable': datos['responsable'],
                        'estado': datos.get('estado', 'Pendiente')
                    }
                }

        except psycopg2.Error as e:
            self.connection.rollback()
            error_msg = f"Error de base de datos al crear RVD: {e}"
            print(error_msg)

            # AUDITORÍA: Registrar error de base de datos
            if AUDITORIA_AVAILABLE and codigo_generado:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario_sesion,
                        accion='CREAR',
                        modulo='RVD',
                        tipo_entidad='Revisión por la Dirección',
                        codigo_registro=codigo_generado,
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': error_msg,
                'error': str(e)
            }

        except Exception as e:
            self.connection.rollback()
            error_msg = f"Error inesperado al crear RVD: {e}"
            print(error_msg)

            # AUDITORÍA: Registrar error general
            if AUDITORIA_AVAILABLE:
                try:
                    duracion_ms = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario_sesion,
                        accion='CREAR',
                        modulo='RVD',
                        tipo_entidad='Revisión por la Dirección',
                        duracion_ms=duracion_ms,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except:
                    pass

            return {
                'success': False,
                'mensaje': error_msg,
                'error': str(e)
            }

    # =====================================================
    # FUNCIONES PARA OBTENER DATOS POR TIPO DE GESTIÓN
    # =====================================================

    def obtener_datos_por_tipo_gestion(self, tipo_gestion: str, filtros: Dict = None) -> List[Dict]:
        """Obtiene datos de la tabla correspondiente según el tipo de gestión seleccionado."""
        try:
            estado_filtro = ''
            if filtros and filtros.get('estado') is not None:
                estado_filtro = str(filtros.get('estado')).strip().lower()

            if estado_filtro == 'cerrado':
                if tipo_gestion == "PA":
                    return self._obtener_cierres_planes_accion(filtros)
                elif tipo_gestion == "GC":
                    return self._obtener_cierres_gestion_cambio(filtros)
                elif tipo_gestion == "RVD":
                    return self._obtener_cierres_revision_direccion(filtros)

            if tipo_gestion == "PA":
                return self._obtener_planes_accion(filtros)
            elif tipo_gestion == "GC":
                return self._obtener_gestion_cambio(filtros)
            elif tipo_gestion == "RVD":
                return self._obtener_revision_direccion(filtros)
            elif not tipo_gestion or tipo_gestion.strip() == "":
                # Si no hay tipo de gestión seleccionado, retornar todos los datos combinados
                return self._obtener_todos_los_datos_combinados(filtros)
            else:
                return []
        except Exception as e:
            print(f"Error al obtener datos por tipo de gestión {tipo_gestion}: {e}")
            return []

    def _obtener_planes_accion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene todos los planes de acción de la tabla sgi.planes_accion."""
        try:
            query = """
                SELECT
                    pa.id,
                    pa.codigo,
                    pa.proceso,
                    pa.subproceso,
                    pa.cop,
                    pa.tipo_accion,
                    pa.estado,
                    pa.fecha_apertura,
                    TO_CHAR(pa.fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    pa.fecha_cierre,
                    TO_CHAR(pa.fecha_cierre, 'DD/MM/YYYY') as fecha_cierre_formato,
                    pa.fuente,
                    pa.hallazgo,
                    pa.causa_raiz,
                    COALESCE(pa.responsable, 'Sin asignar') as responsable,
                    pa.usuario_creacion,
                    pa.fecha_creacion,
                    TO_CHAR(pa.fecha_creacion, 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato
                FROM sgi.planes_accion pa
                WHERE 1=1
            """
            params = []

            # Aplicar filtros adicionales si existen
            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(pa.codigo) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(pa.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(pa.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND pa.estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('responsable'):
                    query += " AND LOWER(COALESCE(pa.responsable, '')) LIKE %s"
                    params.append(f"%{filtros['responsable'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND pa.fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND pa.fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY pa.fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir fechas a strings para serialización JSON
                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener planes de acción: {e}")
            return []

    def _obtener_cierres_planes_accion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene cierres desde sgi_cierres para PA."""
        try:
            query = """
                SELECT
                    c.codigo_referencia as codigo,
                    COALESCE(pa.proceso, '-') as proceso,
                    COALESCE(pa.subproceso, '-') as subproceso,
                    COALESCE(pa.cop, '-') as cop,
                    COALESCE(pa.tipo_accion, ('Cierre ' || c.tipo_cierre)) as tipo_accion,
                    'Cerrado' as estado,
                    pa.fecha_apertura,
                    TO_CHAR(pa.fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    COALESCE(pa.fecha_cierre, c.fecha_cierre) as fecha_cierre,
                    TO_CHAR(COALESCE(pa.fecha_cierre, c.fecha_cierre), 'DD/MM/YYYY') as fecha_cierre_formato,
                    pa.fuente,
                    pa.hallazgo,
                    pa.causa_raiz,
                    COALESCE(pa.responsable, c.responsable_cierre, 'Sin asignar') as responsable,
                    pa.usuario_creacion,
                    COALESCE(pa.fecha_creacion, c.fecha_registro) as fecha_creacion,
                    TO_CHAR(COALESCE(pa.fecha_creacion, c.fecha_registro), 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato
                FROM sgi.sgi_cierres c
                LEFT JOIN sgi.planes_accion pa ON pa.codigo = c.codigo_referencia
                WHERE c.tipo_origen = 'PA'
            """
            params = []

            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(c.codigo_referencia) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(pa.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(pa.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND c.fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND c.fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY c.fecha_registro DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener cierres PA: {e}")
            return []

    def _obtener_gestion_cambio(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene todos los registros de gestión del cambio de la tabla sgi.gestion_cambio."""
        try:
            print("  🔍 Ejecutando consulta SQL para gestion_cambio...")
            # Consulta SIMPLIFICADA - solo columnas que SEGURO existen
            query = """
                SELECT
                    id,
                    codigo,
                    proceso,
                    subproceso,
                    cop,
                    estado,
                    etapa,
                    fecha_apertura,
                    TO_CHAR(fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    fecha_cierre,
                    TO_CHAR(fecha_cierre, 'DD/MM/YYYY') as fecha_cierre_formato,
                    nombre_cambio,
                    responsable,
                    usuario_creacion,
                    fecha_creacion,
                    TO_CHAR(fecha_creacion, 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato
                FROM sgi.gestion_cambio
                WHERE activo = TRUE
            """
            params = []

            # Aplicar filtros adicionales si existen
            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(codigo) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('responsable'):
                    query += " AND LOWER(responsable) LIKE %s"
                    params.append(f"%{filtros['responsable'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir fechas a strings para serialización JSON
                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener gestión del cambio: {e}")
            return []

    def _obtener_cierres_gestion_cambio(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene cierres desde sgi_cierres para GC."""
        try:
            query = """
                SELECT
                    c.codigo_referencia as codigo,
                    COALESCE(gc.proceso, '-') as proceso,
                    COALESCE(gc.subproceso, '-') as subproceso,
                    COALESCE(gc.cop, '-') as cop,
                    'Cerrado' as estado,
                    gc.etapa,
                    gc.fecha_apertura,
                    TO_CHAR(gc.fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    COALESCE(gc.fecha_cierre, c.fecha_cierre) as fecha_cierre,
                    TO_CHAR(COALESCE(gc.fecha_cierre, c.fecha_cierre), 'DD/MM/YYYY') as fecha_cierre_formato,
                    COALESCE(gc.nombre_cambio, ('Cierre ' || c.tipo_cierre)) as nombre_cambio,
                    COALESCE(gc.responsable, c.responsable_cierre, 'Sin asignar') as responsable,
                    gc.usuario_creacion,
                    COALESCE(gc.fecha_creacion, c.fecha_registro) as fecha_creacion,
                    TO_CHAR(COALESCE(gc.fecha_creacion, c.fecha_registro), 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato
                FROM sgi.sgi_cierres c
                LEFT JOIN sgi.gestion_cambio gc ON gc.codigo = c.codigo_referencia
                WHERE c.tipo_origen = 'GC'
            """
            params = []

            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(c.codigo_referencia) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(gc.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(gc.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND c.fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND c.fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY c.fecha_registro DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener cierres GC: {e}")
            return []

    def _obtener_revision_direccion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene todos los registros de revisión por la dirección de la tabla sgi.revision_direccion."""
        try:
            # Consulta MÍNIMA - solo columnas básicas que SEGURO existen
            query = """
                SELECT
                    id,
                    codigo,
                    proceso,
                    subproceso,
                    cop,
                    ano_rvd,
                    actividad,
                    (
                        SELECT STRING_AGG(DISTINCT act.responsable, ', ')
                        FROM sgi.sgi_actividades act
                        WHERE act.codigo_referencia = rvd.codigo
                        AND act.tipo_origen = 'RVD'
                        AND act.responsable IS NOT NULL
                        AND act.responsable != ''
                    ) as responsable,
                    estado,
                    fecha_apertura,
                    TO_CHAR(fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    fecha_cierre,
                    TO_CHAR(fecha_cierre, 'DD/MM/YYYY') as fecha_cierre_formato,
                    usuario_creacion,
                    fecha_creacion,
                    TO_CHAR(fecha_creacion, 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato,
                    categoria
                FROM sgi.revision_direccion rvd
                WHERE activo = TRUE
            """
            params = []

            # Aplicar filtros adicionales si existen
            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(codigo) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('estado'):
                    query += " AND estado = %s"
                    params.append(filtros['estado'])

                if filtros.get('responsable'):
                    query += """
                        AND EXISTS (
                            SELECT 1
                            FROM sgi.sgi_actividades act
                            WHERE act.codigo_referencia = rvd.codigo
                            AND act.tipo_origen = 'RVD'
                            AND act.responsable IS NOT NULL
                            AND act.responsable != ''
                            AND LOWER(act.responsable) LIKE %s
                        )
                    """
                    params.append(f"%{filtros['responsable'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY fecha_creacion DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                # Convertir fechas a strings para serialización JSON
                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener revisión por la dirección: {e}")
            return []

    def _obtener_cierres_revision_direccion(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene cierres desde sgi_cierres para RVD."""
        try:
            query = """
                SELECT
                    c.codigo_referencia as codigo,
                    COALESCE(rvd.proceso, '-') as proceso,
                    COALESCE(rvd.subproceso, '-') as subproceso,
                    COALESCE(rvd.cop, '-') as cop,
                    rvd.ano_rvd,
                    COALESCE(rvd.actividad, ('Cierre ' || c.tipo_cierre)) as actividad,
                    COALESCE(
                        (
                            SELECT STRING_AGG(DISTINCT act.responsable, ', ')
                            FROM sgi.sgi_actividades act
                            WHERE act.codigo_referencia = c.codigo_referencia
                            AND act.tipo_origen = 'RVD'
                            AND act.responsable IS NOT NULL
                            AND act.responsable != ''
                        ),
                        rvd.responsable,
                        c.responsable_cierre,
                        'Sin asignar'
                    ) as responsable,
                    'Cerrado' as estado,
                    rvd.fecha_apertura,
                    TO_CHAR(rvd.fecha_apertura, 'DD/MM/YYYY') as fecha_apertura_formato,
                    COALESCE(rvd.fecha_cierre, c.fecha_cierre) as fecha_cierre,
                    TO_CHAR(COALESCE(rvd.fecha_cierre, c.fecha_cierre), 'DD/MM/YYYY') as fecha_cierre_formato,
                    rvd.usuario_creacion,
                    COALESCE(rvd.fecha_creacion, c.fecha_registro) as fecha_creacion,
                    TO_CHAR(COALESCE(rvd.fecha_creacion, c.fecha_registro), 'DD/MM/YYYY HH24:MI') as fecha_creacion_formato,
                    rvd.categoria
                FROM sgi.sgi_cierres c
                LEFT JOIN sgi.revision_direccion rvd ON rvd.codigo = c.codigo_referencia
                WHERE c.tipo_origen = 'RVD'
            """
            params = []

            if filtros:
                if filtros.get('codigo'):
                    query += " AND LOWER(c.codigo_referencia) LIKE %s"
                    params.append(f"%{filtros['codigo'].lower()}%")

                if filtros.get('proceso'):
                    query += " AND LOWER(rvd.proceso) LIKE %s"
                    params.append(f"%{filtros['proceso'].lower()}%")

                if filtros.get('subproceso'):
                    query += " AND LOWER(rvd.subproceso) LIKE %s"
                    params.append(f"%{filtros['subproceso'].lower()}%")

                if filtros.get('fecha_desde'):
                    query += " AND c.fecha_cierre >= %s"
                    params.append(filtros['fecha_desde'])

                if filtros.get('fecha_hasta'):
                    query += " AND c.fecha_cierre <= %s"
                    params.append(filtros['fecha_hasta'])

            query += " ORDER BY c.fecha_registro DESC"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                datos_serializables = []
                for row in resultados:
                    registro = dict(row)
                    registro = self._serializar_fechas(registro)
                    datos_serializables.append(registro)

                return datos_serializables

        except Exception as e:
            print(f"Error al obtener cierres RVD: {e}")
            return []

    def _obtener_todos_los_datos_combinados(self, filtros: Dict = None) -> List[Dict]:
        """Obtiene todos los datos combinados de las tres tablas: PA, GC y RVD."""
        try:
            estado_filtro = ''
            if filtros and filtros.get('estado') is not None:
                estado_filtro = str(filtros.get('estado')).strip().lower()

            if estado_filtro == 'cerrado':
                planes_accion = self._obtener_cierres_planes_accion(filtros)
                gestion_cambio = self._obtener_cierres_gestion_cambio(filtros)
                revision_direccion = self._obtener_cierres_revision_direccion(filtros)

                for registro in planes_accion:
                    registro['tipo'] = 'PA'
                    if not registro.get('actividad'):
                        registro['actividad'] = registro.get('tipo_accion', 'Plan de Accion')

                for registro in gestion_cambio:
                    registro['tipo'] = 'GC'
                    if not registro.get('actividad'):
                        registro['actividad'] = registro.get('nombre_cambio', 'Gestion del Cambio')

                for registro in revision_direccion:
                    registro['tipo'] = 'RVD'
                    if not registro.get('actividad'):
                        registro['actividad'] = f"Revision {registro.get('codigo', 'RVD')}"

                todos_los_datos = planes_accion + gestion_cambio + revision_direccion
                todos_los_datos.sort(key=lambda x: x.get('fecha_creacion', ''), reverse=True)
                return todos_los_datos
            print("🔄 Obteniendo todos los datos combinados de PA, GC y RVD...")
            print(f"🔍 Filtros recibidos: {filtros}")

            # Obtener datos de cada tabla
            print("📊 Consultando planes_accion...")
            planes_accion = self._obtener_planes_accion(filtros)
            print(f"✅ Planes de acción obtenidos: {len(planes_accion)}")

            print("📊 Consultando gestion_cambio...")
            gestion_cambio = self._obtener_gestion_cambio(filtros)
            print(f"✅ Gestión de cambio obtenidos: {len(gestion_cambio)}")

            print("📊 Consultando revision_direccion...")
            revision_direccion = self._obtener_revision_direccion(filtros)
            print(f"✅ Revisión por dirección obtenidos: {len(revision_direccion)}")

            # Agregar campo 'tipo' a cada registro para identificar su origen
            for registro in planes_accion:
                registro['tipo'] = 'PA'
                # Asegurar que tenga los campos necesarios para la vista
                if not registro.get('actividad'):
                    registro['actividad'] = registro.get('tipo_accion', 'Plan de Acción')

            for registro in gestion_cambio:
                registro['tipo'] = 'GC'
                # Asegurar que tenga los campos necesarios para la vista
                # Usar el campo 'actividad' (que sí existe en la tabla) o nombre_cambio como fallback
                if not registro.get('actividad'):
                    registro['actividad'] = registro.get('nombre_cambio', 'Gestión del Cambio')

            for registro in revision_direccion:
                registro['tipo'] = 'RVD'
                # Asegurar que tenga los campos necesarios para la vista
                # Usar el código como actividad si no hay otro campo disponible
                if not registro.get('actividad'):
                    registro['actividad'] = f"Revisión {registro.get('codigo', 'RVD')}"

            # Combinar todos los datos
            todos_los_datos = planes_accion + gestion_cambio + revision_direccion

            # Ordenar por fecha de creación (más reciente primero)
            todos_los_datos.sort(key=lambda x: x.get('fecha_creacion', ''), reverse=True)

            print(f"✅ Datos combinados obtenidos: {len(planes_accion)} PA + {len(gestion_cambio)} GC + {len(revision_direccion)} RVD = {len(todos_los_datos)} total")
            print(f"📋 Tipos en el resultado final: {[d.get('tipo') for d in todos_los_datos]}")

            return todos_los_datos

        except Exception as e:
            print(f"❌ Error al obtener todos los datos combinados: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _serializar_fechas(self, registro: Dict) -> Dict:
        """Convierte objetos date/datetime a strings para serialización JSON."""
        import datetime

        for key, value in registro.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                if isinstance(value, datetime.datetime):
                    registro[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    registro[key] = value.strftime('%Y-%m-%d')
            elif value is None:
                registro[key] = None

        return registro

    def obtener_actividades_por_referencia(self, codigo: str, tipo_origen: str) -> List[Dict]:
        """Obtiene actividades de sgi_actividades por codigo y tipo."""
        try:
            query = """
                SELECT
                    act.id,
                    act.codigo_referencia,
                    act.tipo_origen,
                    act.tipo_actividad,
                    act.etapa_actividad,
                    act.nombre_actividad,
                    act.descripcion,
                    act.fecha_cierre,
                    act.responsable,
                    act.estado,
                    act.categoria,
                    act.fecha_creacion,
                    (
                        SELECT COUNT(*)
                        FROM sgi.sgi_evidencias ev
                        WHERE ev.actividad_id = act.id
                    ) AS evidencias_total,
                    (
                        SELECT MAX(ev.fecha_subida)
                        FROM sgi.sgi_evidencias ev
                        WHERE ev.actividad_id = act.id
                    ) AS evidencia_ultima
                FROM sgi.sgi_actividades act
                WHERE UPPER(TRIM(act.codigo_referencia)) = UPPER(TRIM(%s))
                AND UPPER(TRIM(act.tipo_origen)) = UPPER(TRIM(%s))
                ORDER BY act.fecha_cierre, act.id
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo, tipo_origen))
                actividades = [dict(row) for row in cursor.fetchall()]

                if not actividades:
                    fallback_query = """
                        SELECT
                            act.id,
                            act.codigo_referencia,
                            act.tipo_origen,
                            act.tipo_actividad,
                            act.etapa_actividad,
                            act.nombre_actividad,
                            act.descripcion,
                            act.fecha_cierre,
                            act.responsable,
                            act.estado,
                            act.categoria,
                            act.fecha_creacion,
                            (
                                SELECT COUNT(*)
                                FROM sgi.sgi_evidencias ev
                                WHERE ev.actividad_id = act.id
                            ) AS evidencias_total,
                            (
                                SELECT MAX(ev.fecha_subida)
                                FROM sgi.sgi_evidencias ev
                                WHERE ev.actividad_id = act.id
                            ) AS evidencia_ultima
                        FROM sgi.sgi_actividades act
                        WHERE regexp_replace(UPPER(act.codigo_referencia), '[^A-Z0-9]', '', 'g')
                            = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                        AND UPPER(TRIM(act.tipo_origen)) = UPPER(TRIM(%s))
                        ORDER BY act.fecha_cierre, act.id
                    """
                    cursor.execute(fallback_query, (codigo, tipo_origen))
                    actividades = [dict(row) for row in cursor.fetchall()]

                for actividad in actividades:
                    if actividad.get("fecha_cierre"):
                        actividad["fecha_cierre_formato"] = actividad["fecha_cierre"].strftime("%d/%m/%Y")
                    if actividad.get("fecha_creacion"):
                        actividad["fecha_creacion_formato"] = actividad["fecha_creacion"].strftime("%d/%m/%Y %H:%M")
                    if actividad.get("evidencia_ultima"):
                        actividad["evidencia_ultima_formato"] = actividad["evidencia_ultima"].strftime("%d/%m/%Y %H:%M")
                    self._serializar_fechas(actividad)
                return actividades

        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener actividades por referencia: {e}")
            return []

    def obtener_actividad_por_id(self, actividad_id: int) -> Optional[Dict]:
        """Obtiene una actividad especifica por id."""
        try:
            query = """
                SELECT
                    id,
                    codigo_referencia,
                    tipo_origen,
                    tipo_actividad,
                    etapa_actividad,
                    nombre_actividad,
                    descripcion,
                    fecha_cierre,
                    responsable,
                    estado,
                    categoria
                FROM sgi.sgi_actividades
                WHERE id = %s
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (actividad_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener actividad por id: {e}")
            return None

    def obtener_config_storage(self, clave: str) -> Optional[str]:
        """Obtiene un valor de configuracion de storage en BD."""
        try:
            query = "SELECT valor FROM sgi.sgi_storage_config WHERE clave = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(query, (clave,))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener configuracion de storage: {e}")
            return None

    def obtener_prefijo_evidencias(self, tipo_origen: Optional[str] = None) -> Tuple[Optional[str], bool]:
        """Obtiene el prefijo para evidencias. Devuelve (prefijo, es_especifico)."""
        prefijo_especifico = None
        if tipo_origen:
            clave_especifica = f"sgi_evidencias_prefix_{tipo_origen}"
            prefijo_especifico = self.obtener_config_storage(clave_especifica)

        if prefijo_especifico:
            prefijo = _normalizar_prefijo_ruta(prefijo_especifico)
            return (prefijo or None), True

        prefix = self.obtener_config_storage("sgi_evidencias_prefix")
        if not prefix:
            prefix = SGI_EVIDENCIAS_PREFIX
        prefix = _normalizar_prefijo_ruta(prefix)
        return (prefix or None), False

    def obtener_container_evidencias(self, tipo_origen: Optional[str] = None) -> Optional[str]:
        """Obtiene el contenedor activo para evidencias SGI."""
        clave_especifica = None
        if tipo_origen:
            clave_especifica = f"sgi_evidencias_container_{tipo_origen}"

        container = None
        if clave_especifica:
            container = self.obtener_config_storage(clave_especifica)

        if not container:
            container = self.obtener_config_storage("sgi_evidencias_container")

        if not container:
            container = SGI_EVIDENCIAS_CONTAINER

        if container:
            return container.strip()
        return None

    def obtener_container_analisis_causas(self) -> Optional[str]:
        """Obtiene el contenedor activo para adjuntos de analisis de causas."""
        container = self.obtener_config_storage("sgi_analisis_causas_container")
        if not container:
            container = self.obtener_container_evidencias()
        if not container:
            container = SGI_ANALISIS_CAUSAS_CONTAINER
        if container:
            return container.strip()
        return None

    def obtener_prefijo_analisis_causas(self) -> Optional[str]:
        """Obtiene el prefijo de carpeta para adjuntos de analisis de causas."""
        prefix = self.obtener_config_storage("sgi_analisis_causas_prefix")
        if not prefix:
            prefix = SGI_ANALISIS_CAUSAS_PREFIX
        prefix = _normalizar_prefijo_ruta(prefix)
        return prefix or None

    def _get_container_client(self, container_name: str):
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en .env")
        if not container_name:
            raise RuntimeError("No se encontro el contenedor de evidencias configurado")
        blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        return blob_service.get_container_client(container_name)

    def _eliminar_directorio_datalake(self, container: str, directorio: str) -> List[str]:
        """Elimina un directorio en ADLS Gen2 (HNS) de forma recursiva."""
        errores = []
        if not AZURE_STORAGE_CONNECTION_STRING or not container or not directorio:
            return errores
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            from azure.core.exceptions import ResourceNotFoundError
        except Exception as e:
            errores.append(f"{container}:{directorio}: datalake no disponible ({e})")
            return errores

        try:
            service = DataLakeServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
            file_system = service.get_file_system_client(container)
            dirs = []
            for path in file_system.get_paths(path=directorio, recursive=True):
                nombre = getattr(path, "name", None)
                if not nombre:
                    continue
                if getattr(path, "is_directory", False):
                    dirs.append(nombre)
                else:
                    try:
                        file_system.delete_file(nombre)
                    except ResourceNotFoundError:
                        pass
                    except Exception as e:
                        errores.append(f"{container}:{nombre}: {str(e)}")

            for nombre in sorted(set(dirs), key=len, reverse=True):
                try:
                    file_system.delete_directory(nombre)
                except ResourceNotFoundError:
                    continue
                except Exception as e:
                    errores.append(f"{container}:{nombre}: {str(e)}")

            try:
                file_system.delete_directory(directorio)
            except ResourceNotFoundError:
                return errores
        except ResourceNotFoundError:
            return errores
        except Exception as e:
            errores.append(f"{container}:{directorio}: {str(e)}")
        return errores

    def _eliminar_blobs_evidencias(self, evidencias: List[Dict]) -> List[str]:
        """Elimina archivos en blob storage para un listado de evidencias."""
        errores = []
        if not evidencias:
            return errores

        clientes = {}
        for evidencia in evidencias:
            evidencia_id = evidencia.get("id")
            container = evidencia.get("container")
            blob_path = evidencia.get("blob_path")
            if not container or not blob_path:
                errores.append(f"Evidencia {evidencia_id}: falta container o blob_path")
                continue
            try:
                client = clientes.get(container)
                if not client:
                    client = self._get_container_client(container)
                    clientes[container] = client
                client.delete_blob(blob_path)
            except Exception as e:
                errores.append(f"Evidencia {evidencia_id}: {str(e)}")

        return errores

    def _eliminar_blobs_adjuntos(self, adjuntos: List[Dict]) -> List[str]:
        """Elimina archivos en blob storage para un listado de adjuntos."""
        errores = []
        if not adjuntos:
            return errores

        clientes = {}
        for adjunto in adjuntos:
            adjunto_id = adjunto.get("id")
            container = adjunto.get("container")
            blob_path = adjunto.get("blob_path")
            if not container or not blob_path:
                errores.append(f"Adjunto {adjunto_id}: falta container o blob_path")
                continue
            try:
                client = clientes.get(container)
                if not client:
                    client = self._get_container_client(container)
                    clientes[container] = client
                client.delete_blob(blob_path)
            except Exception as e:
                errores.append(f"Adjunto {adjunto_id}: {str(e)}")

        return errores

    def _eliminar_blobs_por_prefijo(self, container: str, prefijo: str) -> List[str]:
        """Elimina todos los blobs que cuelgan de un prefijo."""
        errores = []
        if not container or not prefijo:
            return errores
        try:
            client = self._get_container_client(container)
            for blob in client.list_blobs(name_starts_with=prefijo):
                blob_name = getattr(blob, "name", None)
                if not blob_name:
                    continue
                try:
                    client.delete_blob(blob_name)
                except Exception as e:
                    errores.append(f"{container}:{prefijo}: {str(e)}")
        except Exception as e:
            errores.append(f"{container}:{prefijo}: {str(e)}")
        return errores

    def _obtener_prefijo_codigo_evidencias(self, codigo: str, tipo_origen: str) -> Optional[str]:
        codigo_seg = _normalizar_codigo_ruta(codigo)
        if not codigo_seg:
            return None
        tipo_seg = _normalizar_segmento(tipo_origen).lower()
        prefijo, prefijo_especifico = self.obtener_prefijo_evidencias(tipo_origen)
        prefijo = _normalizar_prefijo_ruta(prefijo or "001-sgi")
        carpeta_tipo = f"{prefijo}-{tipo_seg}" if prefijo else tipo_seg
        if prefijo_especifico:
            base = prefijo
        else:
            base = f"{prefijo}/{carpeta_tipo}" if prefijo else carpeta_tipo
        base = _normalizar_prefijo_ruta(base)
        if base:
            return f"{base}/{codigo_seg}/"
        return f"{codigo_seg}/"

    def _obtener_prefijo_codigo_analisis(self, codigo: str) -> Optional[str]:
        codigo_seg = _normalizar_codigo_ruta(codigo)
        if not codigo_seg:
            return None
        prefijo = self.obtener_prefijo_analisis_causas() or "001-sgi/001-sgi-analisis-causas"
        prefijo = _normalizar_prefijo_ruta(prefijo)
        if prefijo:
            return f"{prefijo}/{codigo_seg}/"
        return f"{codigo_seg}/"

    def _eliminar_prefijos_storage_por_codigo(self, codigo: str, tipo_origen: str) -> List[str]:
        """Elimina cualquier blob restante bajo el prefijo del codigo."""
        errores = []
        container_evid = self.obtener_container_evidencias(tipo_origen)
        prefijo_evid = self._obtener_prefijo_codigo_evidencias(codigo, tipo_origen)
        if container_evid and prefijo_evid:
            errores += self._eliminar_blobs_por_prefijo(container_evid, prefijo_evid)
            dir_evid = prefijo_evid.rstrip("/")
            if dir_evid:
                errores += self._eliminar_directorio_datalake(container_evid, dir_evid)

        container_analisis = self.obtener_container_analisis_causas()
        prefijo_analisis = self._obtener_prefijo_codigo_analisis(codigo)
        if container_analisis and prefijo_analisis:
            errores += self._eliminar_blobs_por_prefijo(container_analisis, prefijo_analisis)
            dir_analisis = prefijo_analisis.rstrip("/")
            if dir_analisis:
                errores += self._eliminar_directorio_datalake(container_analisis, dir_analisis)

        return errores

    def _extraer_directorio_codigo(self, blob_path: Optional[str], codigo_seg: str) -> Optional[str]:
        if not blob_path or not codigo_seg:
            return None
        path_lower = blob_path.lower()
        codigo_lower = codigo_seg.lower()
        needle = f"/{codigo_lower}/"
        idx = path_lower.find(needle)
        if idx >= 0:
            end = idx + 1 + len(codigo_lower)
            return blob_path[:end]
        if path_lower.startswith(codigo_lower + "/"):
            return blob_path[:len(codigo_seg)]
        return None

    def registrar_evidencia_actividad(
        self,
        actividad_id: int,
        codigo_referencia: str,
        tipo_origen: str,
        container: str,
        blob_path: str,
        url_publica: Optional[str],
        nombre_archivo: str,
        content_type: Optional[str],
        tamano_bytes: int,
        usuario: str
    ) -> Dict:
        """Inserta una evidencia relacionada a una actividad."""
        inicio = time.time()

        bloqueo = self._validar_registro_modificable(codigo_referencia, tipo_origen)
        if bloqueo:
            return {"ok": False, "error": bloqueo}

        query = """
            INSERT INTO sgi.sgi_evidencias (
                actividad_id,
                codigo_referencia,
                tipo_origen,
                container,
                blob_path,
                url_publica,
                nombre_archivo,
                content_type,
                tamano_bytes,
                usuario_subida,
                estado_validacion,
                motivo_rechazo,
                fecha_validacion,
                usuario_validador
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        try:
            # Obtener información de la actividad
            nombre_actividad = None
            etapa_actividad = None
            try:
                with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT nombre_actividad, etapa_actividad FROM sgi.sgi_actividades WHERE id = %s",
                        (actividad_id,)
                    )
                    actividad_info = cursor.fetchone()
                    if actividad_info:
                        actividad_dict = dict(actividad_info)
                        nombre_actividad = actividad_dict.get('nombre_actividad', 'Sin nombre')
                        etapa_actividad = actividad_dict.get('etapa_actividad', '')
            except Exception:
                pass

            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    actividad_id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario,
                    "pendiente",
                    None,
                    None,
                    None
                ))
                row = cursor.fetchone()
                evidencia_id = row[0] if row else None
                self.connection.commit()

                # Registrar auditoría de subida de evidencia
                if AUDITORIA_AVAILABLE and evidencia_id:
                    duracion = int((time.time() - inicio) * 1000)

                    detalle = f"Evidencia subida: '{nombre_archivo}'"
                    if nombre_actividad:
                        detalle += f" para actividad '{nombre_actividad}'"
                        if etapa_actividad:
                            detalle += f" (Etapa: {etapa_actividad})"

                    # Convertir tamaño a formato legible
                    if tamano_bytes < 1024:
                        tamano_str = f"{tamano_bytes} bytes"
                    elif tamano_bytes < 1024 * 1024:
                        tamano_str = f"{round(tamano_bytes / 1024, 2)} KB"
                    else:
                        tamano_str = f"{round(tamano_bytes / (1024 * 1024), 2)} MB"

                    datos_despues = {
                        'evidencia_id': evidencia_id,
                        'nombre_archivo': nombre_archivo,
                        'tamano': tamano_str,
                        'content_type': content_type,
                        'actividad': nombre_actividad,
                        'etapa': etapa_actividad,
                        'estado_inicial': 'pendiente'
                    }

                    try:
                        auditoria = AuditoriaSGI()
                        auditoria.registrar_log(
                            usuario=usuario,
                            accion='CREAR',
                            modulo=tipo_origen,
                            tipo_entidad='Evidencia',
                            codigo_registro=codigo_referencia,
                            detalle_registro=detalle,
                            datos_despues=datos_despues,
                            duracion_ms=duracion,
                            resultado='exito'
                        )
                        auditoria.cerrar_conexion()
                        print(f"✅ Auditoría registrada: CREAR evidencia '{nombre_archivo}' en {tipo_origen} {codigo_referencia}")
                    except Exception as e_audit:
                        print(f"⚠️ Error al registrar auditoría de subida de evidencia: {e_audit}")

                return {"ok": True, "id": evidencia_id}
        except Exception as e:
            self.connection.rollback()

            # Registrar error en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='CREAR',
                        modulo=tipo_origen,
                        tipo_entidad='Evidencia',
                        codigo_registro=codigo_referencia,
                        detalle_registro=f"Error al subir evidencia '{nombre_archivo}'",
                        duracion_ms=duracion,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de error: {e_audit}")

            return {"ok": False, "error": str(e)}

    def obtener_evidencias_actividad(self, actividad_id: int) -> List[Dict]:
        """Obtiene evidencias de una actividad."""
        try:
            query = """
                SELECT
                    id,
                    actividad_id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    fecha_subida,
                    estado_validacion,
                    motivo_rechazo,
                    fecha_validacion,
                    usuario_validador
                FROM sgi.sgi_evidencias
                WHERE actividad_id = %s
                ORDER BY fecha_subida DESC, id DESC
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (actividad_id,))
                evidencias = [dict(row) for row in cursor.fetchall()]
                for evidencia in evidencias:
                    if evidencia.get("fecha_subida"):
                        evidencia["fecha_subida_formato"] = evidencia["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                    if evidencia.get("fecha_validacion"):
                        evidencia["fecha_validacion_formato"] = evidencia["fecha_validacion"].strftime("%d/%m/%Y %H:%M")
                    url_descarga = self._generar_url_firmada_evidencia(evidencia)
                    if url_descarga:
                        evidencia["url_descarga"] = url_descarga
                    self._serializar_fechas(evidencia)
                return evidencias
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener evidencias: {e}")
            return []

    def obtener_evidencias_por_ids(self, actividad_id: int, evidencias_ids: List[int]) -> List[Dict]:
        """Obtiene evidencias filtradas por IDs."""
        if not evidencias_ids:
            return []
        try:
            query = """
                SELECT
                    id,
                    actividad_id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    fecha_subida,
                    estado_validacion,
                    motivo_rechazo,
                    fecha_validacion,
                    usuario_validador
                FROM sgi.sgi_evidencias
                WHERE actividad_id = %s
                AND id = ANY(%s)
                ORDER BY fecha_subida DESC, id DESC
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (actividad_id, evidencias_ids))
                evidencias = [dict(row) for row in cursor.fetchall()]
                for evidencia in evidencias:
                    if evidencia.get("fecha_subida"):
                        evidencia["fecha_subida_formato"] = evidencia["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                    if evidencia.get("fecha_validacion"):
                        evidencia["fecha_validacion_formato"] = evidencia["fecha_validacion"].strftime("%d/%m/%Y %H:%M")
                    url_descarga = self._generar_url_firmada_evidencia(evidencia)
                    if url_descarga:
                        evidencia["url_descarga"] = url_descarga
                    self._serializar_fechas(evidencia)
                return evidencias
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener evidencias por ids: {e}")
            return []

    def obtener_evidencia_por_id(self, evidencia_id: int) -> Optional[Dict]:
        """Obtiene una evidencia por id."""
        try:
            query = """
                SELECT
                    id,
                    actividad_id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    fecha_subida,
                    estado_validacion,
                    motivo_rechazo,
                    fecha_validacion,
                    usuario_validador
                FROM sgi.sgi_evidencias
                WHERE id = %s
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (evidencia_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                evidencia = dict(row)
                if evidencia.get("fecha_subida"):
                    evidencia["fecha_subida_formato"] = evidencia["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                if evidencia.get("fecha_validacion"):
                    evidencia["fecha_validacion_formato"] = evidencia["fecha_validacion"].strftime("%d/%m/%Y %H:%M")
                url_descarga = self._generar_url_firmada_evidencia(evidencia)
                if url_descarga:
                    evidencia["url_descarga"] = url_descarga
                self._serializar_fechas(evidencia)
                return evidencia
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener evidencia por id: {e}")
            return None

    def obtener_evidencias_por_codigo(self, codigo: str, tipo_origen: str) -> List[Dict]:
        """Obtiene evidencias asociadas a un codigo y tipo."""
        try:
            query = """
                SELECT
                    ev.id,
                    ev.actividad_id,
                    ev.codigo_referencia,
                    ev.tipo_origen,
                    ev.container,
                    ev.blob_path,
                    ev.url_publica,
                    ev.nombre_archivo,
                    ev.content_type,
                    ev.tamano_bytes,
                    ev.usuario_subida,
                    ev.fecha_subida,
                    ev.estado_validacion,
                    ev.motivo_rechazo,
                    ev.fecha_validacion,
                    ev.usuario_validador,
                    act.etapa_actividad,
                    act.nombre_actividad
                FROM sgi.sgi_evidencias ev
                LEFT JOIN sgi.sgi_actividades act ON act.id = ev.actividad_id
                WHERE ev.codigo_referencia = %s
                AND UPPER(TRIM(ev.tipo_origen)) = UPPER(TRIM(%s))
                ORDER BY ev.fecha_subida DESC, ev.id DESC
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo, tipo_origen))
                evidencias = [dict(row) for row in cursor.fetchall()]
                for evidencia in evidencias:
                    if evidencia.get("fecha_subida"):
                        evidencia["fecha_subida_formato"] = evidencia["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                    if evidencia.get("fecha_validacion"):
                        evidencia["fecha_validacion_formato"] = evidencia["fecha_validacion"].strftime("%d/%m/%Y %H:%M")
                    url_descarga = self._generar_url_firmada_evidencia(evidencia)
                    if url_descarga:
                        evidencia["url_descarga"] = url_descarga
                    self._serializar_fechas(evidencia)
                return evidencias
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener evidencias por codigo: {e}")
            return []

    def _obtener_info_evidencia_para_notificacion(self, evidencia_id: int) -> Dict:
        """Obtiene información de una evidencia para crear notificaciones."""
        try:
            query = """
                SELECT
                    ev.codigo_referencia,
                    ev.tipo_origen,
                    ev.nombre_archivo,
                    ev.actividad_id,
                    act.nombre_actividad,
                    CASE
                        WHEN UPPER(ev.tipo_origen) = 'PA' THEN pa.responsable
                        WHEN UPPER(ev.tipo_origen) = 'GC' THEN gc.responsable
                        WHEN UPPER(ev.tipo_origen) = 'RVD' THEN rvd.responsable
                        ELSE 'unknown'
                    END as responsable
                FROM sgi.sgi_evidencias ev
                LEFT JOIN sgi.sgi_actividades act ON ev.actividad_id = act.id
                LEFT JOIN sgi.planes_accion pa ON ev.codigo_referencia = pa.codigo AND ev.tipo_origen = 'PA'
                LEFT JOIN sgi.gestion_cambio gc ON ev.codigo_referencia = gc.codigo AND ev.tipo_origen = 'GC'
                LEFT JOIN sgi.revision_direccion rvd ON ev.codigo_referencia = rvd.codigo AND ev.tipo_origen = 'RVD'
                WHERE ev.id = %s
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (evidencia_id,))
                resultado = cursor.fetchone()
                return dict(resultado) if resultado else {}
        except Exception as e:
            print(f"Error al obtener info evidencia para notificación: {e}")
            return {}

    def actualizar_estado_evidencia(
        self,
        evidencia_id: int,
        estado: str,
        usuario_validador: str,
        motivo_rechazo: Optional[str] = None
    ) -> Dict:
        """Actualiza el estado de validacion de una evidencia."""
        inicio = time.time()
        estado_normalizado = (estado or "").strip().lower()
        if estado_normalizado not in ("pendiente", "aprobada", "rechazada"):
            return {"ok": False, "mensaje": "Estado invalido"}

        if estado_normalizado == "rechazada" and not motivo_rechazo:
            return {"ok": False, "mensaje": "Debe indicar el motivo de rechazo"}

        referencia = self._obtener_referencia_por_evidencia(evidencia_id)
        if not referencia:
            return {"ok": False, "mensaje": "Evidencia no encontrada"}

        bloqueo = self._validar_registro_modificable(
            referencia.get("codigo_referencia"),
            referencia.get("tipo_origen")
        )
        if bloqueo:
            return {"ok": False, "mensaje": bloqueo}

        try:
            # Obtener información de la evidencia antes de actualizar
            query_evidencia = """
                SELECT
                    ev.codigo_referencia,
                    ev.tipo_origen,
                    ev.nombre_archivo,
                    ev.estado_validacion as estado_anterior,
                    act.nombre_actividad,
                    act.etapa_actividad
                FROM sgi.sgi_evidencias ev
                LEFT JOIN sgi.sgi_actividades act ON ev.actividad_id = act.id
                WHERE ev.id = %s
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query_evidencia, (evidencia_id,))
                evidencia_info = cursor.fetchone()

                if not evidencia_info:
                    return {"ok": False, "mensaje": "Evidencia no encontrada"}

                evidencia_dict = dict(evidencia_info)
                codigo_referencia = evidencia_dict.get('codigo_referencia', '')
                tipo_origen = evidencia_dict.get('tipo_origen', '')
                nombre_archivo = evidencia_dict.get('nombre_archivo', 'archivo')
                estado_anterior = evidencia_dict.get('estado_anterior', 'pendiente')
                nombre_actividad = evidencia_dict.get('nombre_actividad', 'Sin nombre')
                etapa_actividad = evidencia_dict.get('etapa_actividad', '')

            query = """
                UPDATE sgi.sgi_evidencias
                SET estado_validacion = %s,
                    motivo_rechazo = %s,
                    fecha_validacion = %s,
                    usuario_validador = %s
                WHERE id = %s
                RETURNING id
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    estado_normalizado,
                    motivo_rechazo,
                    now_bogota(),
                    usuario_validador,
                    evidencia_id
                ))
                row = cursor.fetchone()
                self.connection.commit()

                # Registrar auditoría específica para aprobación/rechazo de evidencias
                if AUDITORIA_AVAILABLE and row:
                    duracion = int((time.time() - inicio) * 1000)

                    # Determinar la acción y detalle según el estado
                    if estado_normalizado == 'aprobada':
                        accion_auditoria = 'APROBAR'
                        detalle = f"Evidencia aprobada: '{nombre_archivo}' de actividad '{nombre_actividad}'"
                        if etapa_actividad:
                            detalle += f" (Etapa: {etapa_actividad})"
                    elif estado_normalizado == 'rechazada':
                        accion_auditoria = 'RECHAZAR'
                        detalle = f"Evidencia rechazada: '{nombre_archivo}' de actividad '{nombre_actividad}'"
                        if etapa_actividad:
                            detalle += f" (Etapa: {etapa_actividad})"
                        if motivo_rechazo:
                            detalle += f" - Motivo: {motivo_rechazo}"
                    else:
                        accion_auditoria = 'MODIFICAR'
                        detalle = f"Evidencia modificada a pendiente: '{nombre_archivo}'"

                    datos_antes = {
                        'evidencia_id': evidencia_id,
                        'estado_anterior': estado_anterior,
                        'nombre_archivo': nombre_archivo
                    }

                    datos_despues = {
                        'evidencia_id': evidencia_id,
                        'estado_nuevo': estado_normalizado,
                        'nombre_archivo': nombre_archivo,
                        'actividad': nombre_actividad,
                        'etapa': etapa_actividad
                    }

                    if motivo_rechazo:
                        datos_despues['motivo_rechazo'] = motivo_rechazo

                    try:
                        auditoria = AuditoriaSGI()
                        auditoria.registrar_log(
                            usuario=usuario_validador,
                            accion=accion_auditoria,
                            modulo=tipo_origen,  # PA, GC, o RVD
                            tipo_entidad='Evidencia',
                            codigo_registro=codigo_referencia,
                            detalle_registro=detalle,
                            datos_antes=datos_antes,
                            datos_despues=datos_despues,
                            duracion_ms=duracion,
                            resultado='exito'
                        )
                        auditoria.cerrar_conexion()
                        print(f"✅ Auditoría registrada: {accion_auditoria} evidencia en {tipo_origen} {codigo_referencia}")
                    except Exception as e_audit:
                        print(f"⚠️ Error al registrar auditoría de evidencia: {e_audit}")

                return {"ok": bool(row), "id": row[0] if row else None}
        except Exception as e:
            self.connection.rollback()

            # Registrar error en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario_validador,
                        accion='MODIFICAR',
                        modulo='EVIDENCIA',
                        tipo_entidad='Evidencia',
                        codigo_registro=str(evidencia_id),
                        detalle_registro=f"Error al actualizar estado de evidencia",
                        duracion_ms=duracion,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de error: {e_audit}")

            return {"ok": False, "mensaje": str(e)}

    def reemplazar_archivo_evidencia(
        self,
        evidencia_id: int,
        nombre_archivo: str,
        content_type: Optional[str],
        tamano_bytes: int,
        usuario_subida: str
    ) -> Dict:
        """Actualiza metadatos al reemplazar una evidencia."""
        try:
            referencia = self._obtener_referencia_por_evidencia(evidencia_id)
            if not referencia:
                return {"ok": False, "mensaje": "Evidencia no encontrada"}

            bloqueo = self._validar_registro_modificable(
                referencia.get("codigo_referencia"),
                referencia.get("tipo_origen")
            )
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            query = """
                UPDATE sgi.sgi_evidencias
                SET nombre_archivo = %s,
                    content_type = %s,
                    tamano_bytes = %s,
                    usuario_subida = %s,
                    fecha_subida = %s,
                    estado_validacion = %s,
                    motivo_rechazo = NULL,
                    fecha_validacion = NULL,
                    usuario_validador = NULL
                WHERE id = %s
                RETURNING id
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    now_bogota(),
                    "pendiente",
                    evidencia_id
                ))
                row = cursor.fetchone()
                self.connection.commit()
                return {"ok": bool(row), "id": row[0] if row else None}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def eliminar_evidencia(self, evidencia_id: int, usuario: str = "unknown") -> Dict:
        """Elimina una evidencia y su blob asociado."""
        inicio = time.time()

        try:
            # Obtener información de la evidencia antes de eliminar
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        ev.id,
                        ev.container,
                        ev.blob_path,
                        ev.codigo_referencia,
                        ev.tipo_origen,
                        ev.nombre_archivo,
                        ev.estado_validacion,
                        act.nombre_actividad,
                        act.etapa_actividad
                    FROM sgi.sgi_evidencias ev
                    LEFT JOIN sgi.sgi_actividades act ON ev.actividad_id = act.id
                    WHERE ev.id = %s
                    """,
                    (evidencia_id,)
                )
                evidencia = cursor.fetchone()

            if not evidencia:
                return {"ok": False, "mensaje": "Evidencia no encontrada"}

            evidencia_dict = dict(evidencia)
            codigo_referencia = evidencia_dict.get('codigo_referencia', '')
            tipo_origen = evidencia_dict.get('tipo_origen', '')
            nombre_archivo = evidencia_dict.get('nombre_archivo', 'archivo')
            estado_validacion = evidencia_dict.get('estado_validacion', 'pendiente')
            nombre_actividad = evidencia_dict.get('nombre_actividad', 'Sin nombre')
            etapa_actividad = evidencia_dict.get('etapa_actividad', '')

            bloqueo = self._validar_registro_modificable(codigo_referencia, tipo_origen)
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            # Eliminar blob de Azure
            errores_blobs = self._eliminar_blobs_evidencias([evidencia_dict])

            # Eliminar de base de datos
            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM sgi.sgi_evidencias WHERE id = %s", (evidencia_id,))
                filas = cursor.rowcount
                self.connection.commit()

            # Registrar auditoría de eliminación
            if AUDITORIA_AVAILABLE and filas > 0:
                duracion = int((time.time() - inicio) * 1000)

                detalle = f"Evidencia eliminada: '{nombre_archivo}' de actividad '{nombre_actividad}'"
                if etapa_actividad:
                    detalle += f" (Etapa: {etapa_actividad})"

                datos_antes = {
                    'evidencia_id': evidencia_id,
                    'nombre_archivo': nombre_archivo,
                    'estado_validacion': estado_validacion,
                    'actividad': nombre_actividad,
                    'etapa': etapa_actividad
                }

                try:
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='ELIMINAR',
                        modulo=tipo_origen,
                        tipo_entidad='Evidencia',
                        codigo_registro=codigo_referencia,
                        detalle_registro=detalle,
                        datos_antes=datos_antes,
                        duracion_ms=duracion,
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                    print(f"✅ Auditoría registrada: ELIMINAR evidencia '{nombre_archivo}' en {tipo_origen} {codigo_referencia}")
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de eliminación de evidencia: {e_audit}")

            respuesta = {"ok": True, "filas": filas}
            if errores_blobs:
                respuesta["advertencias"] = errores_blobs
                respuesta["mensaje"] = "Evidencia eliminada con advertencias al borrar blobs"
            return respuesta
        except Exception as e:
            self.connection.rollback()

            # Registrar error en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='ELIMINAR',
                        modulo='EVIDENCIA',
                        tipo_entidad='Evidencia',
                        codigo_registro=str(evidencia_id),
                        detalle_registro=f"Error al eliminar evidencia ID {evidencia_id}",
                        duracion_ms=duracion,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de error: {e_audit}")

            return {"ok": False, "mensaje": str(e)}

    def registrar_adjunto_analisis_causas(
        self,
        codigo_referencia: str,
        tipo_origen: str,
        container: str,
        blob_path: str,
        url_publica: Optional[str],
        nombre_archivo: str,
        content_type: Optional[str],
        tamano_bytes: int,
        usuario: str
    ) -> Dict:
        """Inserta un adjunto de analisis de causas asociado a un registro."""
        bloqueo = self._validar_registro_modificable(codigo_referencia, tipo_origen)
        if bloqueo:
            return {"ok": False, "error": bloqueo}

        query = """
            INSERT INTO sgi.sgi_analisis_causas_adjuntos (
                codigo_referencia,
                tipo_origen,
                container,
                blob_path,
                url_publica,
                nombre_archivo,
                content_type,
                tamano_bytes,
                usuario_subida,
                fecha_subida,
                reemplazos
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario,
                    now_bogota(),
                    0
                ))
                row = cursor.fetchone()
                self.connection.commit()
                return {"ok": True, "id": row[0] if row else None}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "error": str(e)}

    def obtener_adjuntos_analisis_causas(self, codigo: str, tipo_origen: str) -> List[Dict]:
        """Obtiene adjuntos de analisis de causas por codigo y tipo."""
        try:
            query = """
                SELECT
                    id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    fecha_subida,
                    reemplazos
                FROM sgi.sgi_analisis_causas_adjuntos
                WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                    = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                ORDER BY fecha_subida DESC, id DESC
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo, tipo_origen))
                adjuntos = [dict(row) for row in cursor.fetchall()]
                for adjunto in adjuntos:
                    if adjunto.get("fecha_subida"):
                        adjunto["fecha_subida_formato"] = adjunto["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                    url_descarga = self._generar_url_firmada_evidencia(adjunto)
                    if url_descarga:
                        adjunto["url_descarga"] = url_descarga
                    adjunto["puede_reemplazar"] = int(adjunto.get("reemplazos") or 0) < 1
                    self._serializar_fechas(adjunto)
                return adjuntos
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener adjuntos de analisis de causas: {e}")
            return []

    def obtener_adjunto_analisis_causas_por_id(self, adjunto_id: int) -> Optional[Dict]:
        """Obtiene un adjunto de analisis de causas por id."""
        try:
            query = """
                SELECT
                    id,
                    codigo_referencia,
                    tipo_origen,
                    container,
                    blob_path,
                    url_publica,
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    fecha_subida,
                    reemplazos
                FROM sgi.sgi_analisis_causas_adjuntos
                WHERE id = %s
            """
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (adjunto_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                adjunto = dict(row)
                if adjunto.get("fecha_subida"):
                    adjunto["fecha_subida_formato"] = adjunto["fecha_subida"].strftime("%d/%m/%Y %H:%M")
                url_descarga = self._generar_url_firmada_evidencia(adjunto)
                if url_descarga:
                    adjunto["url_descarga"] = url_descarga
                adjunto["puede_reemplazar"] = int(adjunto.get("reemplazos") or 0) < 1
                self._serializar_fechas(adjunto)
                return adjunto
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener adjunto de analisis de causas: {e}")
            return None

    def reemplazar_adjunto_analisis_causas(
        self,
        adjunto_id: int,
        nombre_archivo: str,
        content_type: Optional[str],
        tamano_bytes: int,
        usuario_subida: str
    ) -> Dict:
        """Actualiza metadatos al reemplazar un adjunto de analisis de causas."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, reemplazos, codigo_referencia, tipo_origen
                    FROM sgi.sgi_analisis_causas_adjuntos
                    WHERE id = %s
                    """,
                    (adjunto_id,)
                )
                row = cursor.fetchone()
                if not row:
                    return {"ok": False, "mensaje": "Adjunto no encontrado"}
                reemplazos = int(row.get("reemplazos") or 0)
                if reemplazos >= 1:
                    return {"ok": False, "mensaje": "El adjunto ya fue reemplazado"}

                bloqueo = self._validar_registro_modificable(
                    row.get("codigo_referencia"),
                    row.get("tipo_origen")
                )
                if bloqueo:
                    return {"ok": False, "mensaje": bloqueo}

            query = """
                UPDATE sgi.sgi_analisis_causas_adjuntos
                SET nombre_archivo = %s,
                    content_type = %s,
                    tamano_bytes = %s,
                    usuario_subida = %s,
                    fecha_subida = %s,
                    reemplazos = reemplazos + 1
                WHERE id = %s
                RETURNING reemplazos
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    nombre_archivo,
                    content_type,
                    tamano_bytes,
                    usuario_subida,
                    now_bogota(),
                    adjunto_id
                ))
                row = cursor.fetchone()
                self.connection.commit()
                return {"ok": bool(row), "reemplazos": row[0] if row else None}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def eliminar_adjunto_analisis_causas(self, adjunto_id: int) -> Dict:
        """Elimina un adjunto de analisis de causas y su blob asociado."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, container, blob_path, nombre_archivo
                    FROM sgi.sgi_analisis_causas_adjuntos
                    WHERE id = %s
                    """,
                    (adjunto_id,)
                )
                adjunto = cursor.fetchone()

            if not adjunto:
                return {"ok": False, "mensaje": "Adjunto no encontrado"}

            adjunto_dict = dict(adjunto)
            errores_blobs = self._eliminar_blobs_adjuntos([adjunto_dict])

            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM sgi.sgi_analisis_causas_adjuntos WHERE id = %s", (adjunto_id,))
                filas = cursor.rowcount
                self.connection.commit()

            respuesta = {"ok": True, "filas": filas}
            if errores_blobs:
                respuesta["advertencias"] = errores_blobs
                respuesta["mensaje"] = "Adjunto eliminado con advertencias al borrar blobs"
            return respuesta
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def obtener_registro_por_codigo(self, codigo: str, tipo_origen: str) -> Optional[Dict]:
        """Obtiene un registro principal segun tipo y codigo."""
        tipo = (tipo_origen or "").strip().upper()
        try:
            if tipo == "PA":
                query = """
                    SELECT
                        codigo,
                        proceso,
                        subproceso,
                        cop,
                        tipo_accion,
                        estado,
                        fecha_apertura,
                        fecha_cierre,
                        fuente,
                        hallazgo,
                        causa_raiz,
                        responsable
                    FROM sgi.planes_accion
                    WHERE codigo = %s
                """
            elif tipo == "GC":
                query = """
                    SELECT
                        codigo,
                        proceso,
                        subproceso,
                        cop,
                        estado,
                        fecha_apertura,
                        fecha_cierre,
                        nombre_cambio,
                        descripcion_cambio,
                        responsable,
                        categoria,
                        activo
                    FROM sgi.gestion_cambio
                    WHERE codigo = %s
                """
            elif tipo == "RVD":
                query = """
                    SELECT
                        codigo,
                        proceso,
                        subproceso,
                        cop,
                        ano_rvd,
                        actividad,
                        responsable,
                        estado,
                        fecha_apertura,
                        fecha_cierre,
                        categoria,
                        activo
                    FROM sgi.revision_direccion
                    WHERE codigo = %s
                """
            else:
                return None

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (codigo,))
                row = cursor.fetchone()
                if not row:
                    return None
                registro = dict(row)
                self._serializar_fechas(registro)
                return registro
        except Exception as e:
            self.connection.rollback()
            print(f"Error al obtener registro por codigo: {e}")
            return None

    def actualizar_registro_por_codigo(self, codigo: str, tipo_origen: str, datos: Dict, usuario: str = "unknown") -> Dict:
        """Actualiza un registro principal segun tipo y codigo, con auditoría detallada de campos modificados."""
        import time
        inicio = time.time()

        tipo = (tipo_origen or "").strip().upper()
        try:
            if tipo == "PA":
                campos = [
                    "proceso", "subproceso", "cop", "tipo_accion", "estado",
                    "fecha_apertura", "fecha_cierre", "fuente", "hallazgo", "causa_raiz",
                    "responsable"
                ]
                tabla = "sgi.planes_accion"
                nombres_amigables = {
                    "proceso": "Proceso",
                    "subproceso": "Subproceso",
                    "cop": "COP",
                    "tipo_accion": "Tipo de Acción",
                    "estado": "Estado",
                    "fecha_apertura": "Fecha de Apertura",
                    "fecha_cierre": "Fecha de Cierre",
                    "fuente": "Fuente",
                    "hallazgo": "Hallazgo",
                    "causa_raiz": "Causa Raíz",
                    "responsable": "Responsable"
                }
            elif tipo == "GC":
                campos = [
                    "proceso", "subproceso", "cop", "estado", "fecha_apertura",
                    "fecha_cierre", "nombre_cambio", "descripcion_cambio", "categoria",
                    "responsable"
                ]
                tabla = "sgi.gestion_cambio"
                nombres_amigables = {
                    "proceso": "Proceso",
                    "subproceso": "Subproceso",
                    "cop": "COP",
                    "estado": "Estado",
                    "fecha_apertura": "Fecha de Apertura",
                    "fecha_cierre": "Fecha de Cierre",
                    "nombre_cambio": "Nombre del Cambio",
                    "descripcion_cambio": "Descripción del Cambio",
                    "categoria": "Categoría",
                    "responsable": "Responsable"
                }
            elif tipo == "RVD":
                campos = [
                    "proceso", "subproceso", "cop", "ano_rvd", "actividad",
                    "responsable", "estado", "fecha_apertura", "fecha_cierre", "categoria"
                ]
                tabla = "sgi.revision_direccion"
                nombres_amigables = {
                    "proceso": "Proceso",
                    "subproceso": "Subproceso",
                    "cop": "COP",
                    "ano_rvd": "Año RVD",
                    "actividad": "Actividad",
                    "responsable": "Responsable",
                    "estado": "Estado",
                    "fecha_apertura": "Fecha de Apertura",
                    "fecha_cierre": "Fecha de Cierre",
                    "categoria": "Categoría"
                }
            else:
                return {"ok": False, "mensaje": "Tipo invalido"}

            bloqueo = self._validar_registro_modificable(codigo, tipo)
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            def _normalizar_valor(valor):
                if isinstance(valor, str):
                    return valor.strip()
                if isinstance(valor, (datetime, date)):
                    return valor.isoformat()
                return valor

            def _valores_equivalentes(nuevo, anterior):
                nuevo_norm = _normalizar_valor(nuevo)
                anterior_norm = _normalizar_valor(anterior)
                if (nuevo_norm is None or nuevo_norm == "") and (anterior_norm is None or anterior_norm == ""):
                    return True
                if isinstance(nuevo_norm, str) and isinstance(anterior_norm, (int, float)):
                    try:
                        nuevo_norm = type(anterior_norm)(nuevo_norm)
                    except Exception:
                        pass
                if isinstance(anterior_norm, str) and isinstance(nuevo_norm, (int, float)):
                    try:
                        anterior_norm = type(nuevo_norm)(anterior_norm)
                    except Exception:
                        pass
                return nuevo_norm == anterior_norm

            # 1. CONSULTAR VALORES ACTUALES ANTES DE ACTUALIZAR
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Construir query SELECT con los campos que se van a actualizar
                campos_select = ", ".join(campos)
                query_select = f"SELECT {campos_select} FROM {tabla} WHERE codigo = %s"
                cursor.execute(query_select, (codigo,))
                registro_anterior = cursor.fetchone()

                if not registro_anterior:
                    return {"ok": False, "mensaje": "Registro no encontrado"}

                # 2. PREPARAR UPDATE
                valores = []
                sets = []
                for campo in campos:
                    if campo in datos:
                        valor_nuevo = datos.get(campo)
                        valor_anterior = registro_anterior.get(campo)
                        if not _valores_equivalentes(valor_nuevo, valor_anterior):
                            sets.append(f"{campo} = %s")
                            valores.append(valor_nuevo)

                if not sets:
                    return {"ok": True, "mensaje": "Sin cambios", "actualizados": 0, "sin_cambios": True}

                sets.append("fecha_actualizacion = %s")
                valores.append(now_bogota())
                valores.append(codigo)

                # 3. EJECUTAR UPDATE
                query_update = f"UPDATE {tabla} SET {', '.join(sets)} WHERE codigo = %s"
                cursor.execute(query_update, valores)
                self.connection.commit()

                filas_actualizadas = cursor.rowcount

                # 4. COMPARAR Y REGISTRAR AUDITORÍA CON CAMPOS MODIFICADOS
                if AUDITORIA_AVAILABLE and filas_actualizadas > 0:
                    try:
                        auditoria = AuditoriaSGI()

                        # Identificar campos que cambiaron
                        cambios_detallados = []  # Lista con formato "Campo: 'valor_antes' → 'valor_nuevo'"
                        datos_antes = {}
                        datos_despues = {}

                        for campo in campos:
                            valor_anterior = registro_anterior.get(campo)
                            valor_nuevo = datos.get(campo, valor_anterior)

                            # Convertir fechas y None a string para comparación y display
                            valor_anterior_str = valor_anterior
                            valor_nuevo_str = valor_nuevo

                            if hasattr(valor_anterior, 'isoformat'):
                                valor_anterior_str = valor_anterior.isoformat()
                            if hasattr(valor_nuevo, 'isoformat'):
                                valor_nuevo_str = valor_nuevo.isoformat()
                            datos_antes[campo] = valor_anterior_str
                            datos_despues[campo] = valor_nuevo_str


                            # Solo registrar si cambió
                            if str(valor_anterior) != str(valor_nuevo):
                                nombre_campo = nombres_amigables.get(campo, campo)

                                # Formatear valores para el detalle (limitar longitud si es muy largo)
                                antes_display = str(valor_anterior_str) if valor_anterior_str is not None else "null"
                                despues_display = str(valor_nuevo_str) if valor_nuevo_str is not None else "null"

                                # Truncar si es muy largo (más de 50 caracteres)
                                if len(antes_display) > 50:
                                    antes_display = antes_display[:47] + "..."
                                if len(despues_display) > 50:
                                    despues_display = despues_display[:47] + "..."

                                # Crear el texto del cambio: "Campo: 'valor_antes' → 'valor_nuevo'"
                                cambio_texto = f"{nombre_campo}: '{antes_display}' → '{despues_display}'"
                                cambios_detallados.append(cambio_texto)

                        # Generar detalle legible con VALORES
                        if cambios_detallados:
                            if len(cambios_detallados) == 1:
                                # 1 campo: mostrar el cambio completo
                                detalle = cambios_detallados[0]
                            elif len(cambios_detallados) == 2:
                                # 2 campos: mostrar ambos cambios
                                detalle = f"{cambios_detallados[0]} | {cambios_detallados[1]}"
                            elif len(cambios_detallados) == 3:
                                # 3 campos: mostrar los 3 cambios separados por " | "
                                detalle = " | ".join(cambios_detallados)
                            else:
                                # 4+ campos: mostrar primeros 2 cambios + resumen
                                primeros_cambios = " | ".join(cambios_detallados[:2])
                                detalle = f"{primeros_cambios} | (+{len(cambios_detallados) - 2} campos más)"
                        else:
                            # Sin cambios en campos - verificar si hubo acciones recientes
                            detalle = "Guardado sin modificaciones en campos (revisar acciones recientes: evidencias, comentarios, etc.)"

                        duracion = int((time.time() - inicio) * 1000)

                        auditoria.registrar_log(
                            usuario=usuario,
                            accion='MODIFICAR',
                            modulo=tipo,
                            tipo_entidad=f"Registro {tipo}",
                            codigo_registro=codigo,
                            detalle_registro=detalle,
                            datos_antes=datos_antes,
                            datos_despues=datos_despues,
                            duracion_ms=duracion,
                            resultado='exito'
                        )
                    except Exception as e:
                        print(f"⚠️ Error al registrar auditoría de actualización: {e}")

                return {"ok": True, "actualizados": filas_actualizadas}

        except Exception as e:
            self.connection.rollback()

            # Registrar error en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    auditoria = AuditoriaSGI()
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='MODIFICAR',
                        modulo=tipo,
                        tipo_entidad=f"Registro {tipo}",
                        codigo_registro=codigo,
                        detalle_registro=f"Error al actualizar registro",
                        error_mensaje=str(e),
                        duracion_ms=duracion,
                        resultado='error'
                    )
                except Exception as ae:
                    print(f"⚠️ Error al registrar auditoría de error: {ae}")

            return {"ok": False, "mensaje": str(e)}

    def obtener_historial_registro_admin(self, codigo: str, tipo_origen: str) -> Dict:
        """Obtiene el historial de cambios del registro desde auditoria."""
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()

        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}

        try:
            tipo_entidad = f"Registro {tipo}"
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        usuario,
                        accion,
                        modulo,
                        tipo_entidad,
                        codigo_registro,
                        detalle_registro,
                        datos_antes,
                        datos_despues,
                        fecha_hora,
                        TO_CHAR(fecha_hora, 'DD/MM/YYYY HH24:MI') as fecha_formato
                    FROM sgi.auditoria_log
                    WHERE codigo_registro = %s
                      AND modulo = %s
                      AND accion = 'MODIFICAR'
                      AND tipo_entidad = %s
                      AND resultado = 'exito'
                      AND (
                          (datos_antes IS NOT NULL AND datos_antes <> '{}'::jsonb)
                          OR (datos_despues IS NOT NULL AND datos_despues <> '{}'::jsonb)
                      )
                    ORDER BY fecha_hora DESC, id DESC
                    """,
                    (codigo, tipo, tipo_entidad)
                )
                rows = [dict(row) for row in cursor.fetchall()]
                for row in rows:
                    self._serializar_fechas(row)
                return {"ok": True, "historial": rows}

        except Exception as e:
            return {"ok": False, "mensaje": str(e)}

    def _obtener_tabla_registro(self, tipo: str) -> Optional[str]:
        tipo_norm = (tipo or "").strip().upper()
        if tipo_norm == "PA":
            return "sgi.planes_accion"
        if tipo_norm == "GC":
            return "sgi.gestion_cambio"
        if tipo_norm == "RVD":
            return "sgi.revision_direccion"
        return None

    def _validar_registro_modificable(self, codigo: str, tipo_origen: str) -> Optional[str]:
        """Retorna un mensaje si el registro no permite modificaciones."""
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()
        tabla = self._obtener_tabla_registro(tipo)
        if not tabla:
            return "Tipo invalido"
        if not codigo:
            return "Codigo requerido"

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT estado FROM {tabla} WHERE codigo = %s", (codigo,))
                registro = cursor.fetchone()
                if not registro:
                    return "Registro no encontrado"

                estado_actual = str(registro.get("estado") or "").strip().lower()
                if estado_actual == "cerrado":
                    return "El registro esta cerrado y no permite cambios"

                cursor.execute(
                    "SELECT 1 FROM sgi.sgi_cierres WHERE codigo_referencia = %s AND tipo_origen = %s",
                    (codigo, tipo)
                )
                if cursor.fetchone():
                    return "El registro ya tiene cierre y no permite cambios"

        except Exception as e:
            return f"Error al validar estado del registro: {e}"

        return None

    def _normalizar_tipo_actividad(self, tipo_origen: str, tipo_actividad: Optional[str]) -> Optional[str]:
        tipo_origen_norm = (tipo_origen or "").strip().upper()
        if tipo_origen_norm != "PA":
            return None
        valor = (tipo_actividad or "").strip().upper()
        if not valor:
            return "PLAN"
        if valor not in {"CORRECTIVA", "PLAN"}:
            raise ValueError("Tipo de actividad invalido")
        return valor

    def _obtener_referencia_por_actividad(self, actividad_id: int) -> Optional[Dict]:
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT codigo_referencia, tipo_origen FROM sgi.sgi_actividades WHERE id = %s",
                    (actividad_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def _obtener_referencia_por_evidencia(self, evidencia_id: int) -> Optional[Dict]:
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT codigo_referencia, tipo_origen FROM sgi.sgi_evidencias WHERE id = %s",
                    (evidencia_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def validar_cierre_registro(self, codigo: str, tipo_origen: str) -> Dict:
        """Valida si un registro cumple condiciones para cierre estricto."""
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()
        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}

        tabla = self._obtener_tabla_registro(tipo)
        if not tabla:
            return {"ok": False, "mensaje": "Tipo invalido"}

        detalle = {
            "estado_registro": None,
            "total_actividades": 0,
            "actividades_cerradas": 0,
            "actividades_no_ejecutadas": 0,
            "actividades_otro_estado": 0,
            "actividades_sin_evidencias": 0,
            "actividades_con_evidencias_pendientes": 0,
            "actividades_con_evidencias_rechazadas": 0,
            "actividades_con_evidencias_no_aprobadas": 0,
            "evidencias_total": 0,
            "evidencias_aprobadas": 0,
            "evidencias_pendientes": 0,
            "evidencias_rechazadas": 0,
            "evidencias_sin_estado": 0
        }

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT estado FROM {tabla} WHERE codigo = %s", (codigo,))
                registro = cursor.fetchone()
                if not registro:
                    return {"ok": False, "mensaje": "Registro no encontrado", "puede_cerrar": False, "detalle": detalle}

                estado_actual = str(registro.get("estado") or "").strip()
                detalle["estado_registro"] = estado_actual

                cursor.execute(
                    "SELECT 1 FROM sgi.sgi_cierres WHERE codigo_referencia = %s AND tipo_origen = %s",
                    (codigo, tipo)
                )
                if cursor.fetchone():
                    return {
                        "ok": True,
                        "puede_cerrar": False,
                        "mensaje": "Este registro ya tiene un cierre",
                        "detalle": detalle,
                        "bloqueos": ["El registro ya tiene cierre registrado"]
                    }

                if estado_actual.lower() == "cerrado":
                    return {
                        "ok": True,
                        "puede_cerrar": False,
                        "mensaje": "Registro ya cerrado",
                        "detalle": detalle,
                        "bloqueos": ["El registro ya se encuentra cerrado"]
                    }

                cursor.execute(
                    """
                    SELECT
                        act.id,
                        act.estado,
                        COUNT(ev.id) AS evidencias_total,
                        SUM(CASE WHEN ev.estado_validacion = 'aprobada' THEN 1 ELSE 0 END) AS evidencias_aprobadas,
                        SUM(CASE WHEN ev.estado_validacion = 'pendiente' THEN 1 ELSE 0 END) AS evidencias_pendientes,
                        SUM(CASE WHEN ev.estado_validacion = 'rechazada' THEN 1 ELSE 0 END) AS evidencias_rechazadas
                    FROM sgi.sgi_actividades act
                    LEFT JOIN sgi.sgi_evidencias ev ON ev.actividad_id = act.id
                    WHERE regexp_replace(UPPER(act.codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(act.tipo_origen)) = UPPER(TRIM(%s))
                    GROUP BY act.id, act.estado
                    ORDER BY act.id
                    """,
                    (codigo, tipo)
                )
                actividades = [dict(row) for row in cursor.fetchall()]

            if not actividades:
                detalle["total_actividades"] = 0
                return {
                    "ok": True,
                    "puede_cerrar": False,
                    "mensaje": "No hay actividades registradas para cerrar",
                    "detalle": detalle,
                    "bloqueos": ["No hay actividades registradas"]
                }

            bloqueos = []
            total_evidencias_sin_estado = 0

            for actividad in actividades:
                detalle["total_actividades"] += 1

                estado_norm = str(actividad.get("estado") or "").strip().lower()
                if estado_norm == "cerrado":
                    detalle["actividades_cerradas"] += 1
                elif estado_norm in ("no ejecutado", "no ejecutada"):
                    detalle["actividades_no_ejecutadas"] += 1
                else:
                    detalle["actividades_otro_estado"] += 1

                evid_total = int(actividad.get("evidencias_total") or 0)
                evid_aprobadas = int(actividad.get("evidencias_aprobadas") or 0)
                evid_pendientes = int(actividad.get("evidencias_pendientes") or 0)
                evid_rechazadas = int(actividad.get("evidencias_rechazadas") or 0)

                detalle["evidencias_total"] += evid_total
                detalle["evidencias_aprobadas"] += evid_aprobadas
                detalle["evidencias_pendientes"] += evid_pendientes
                detalle["evidencias_rechazadas"] += evid_rechazadas

                if evid_total == 0:
                    detalle["actividades_sin_evidencias"] += 1
                if evid_pendientes > 0:
                    detalle["actividades_con_evidencias_pendientes"] += 1
                if evid_rechazadas > 0:
                    detalle["actividades_con_evidencias_rechazadas"] += 1
                if evid_total > evid_aprobadas:
                    detalle["actividades_con_evidencias_no_aprobadas"] += 1

                total_evidencias_sin_estado += max(
                    evid_total - (evid_aprobadas + evid_pendientes + evid_rechazadas),
                    0
                )

            detalle["evidencias_sin_estado"] = total_evidencias_sin_estado

            if detalle["actividades_otro_estado"] > 0:
                bloqueos.append("Hay actividades con estado diferente a Cerrado/No ejecutado")
            if detalle["actividades_sin_evidencias"] > 0:
                bloqueos.append("Hay actividades sin evidencias cargadas")
            if detalle["actividades_con_evidencias_no_aprobadas"] > 0:
                bloqueos.append("Hay evidencias pendientes o rechazadas")
            if detalle["evidencias_sin_estado"] > 0:
                bloqueos.append("Hay evidencias sin estado de validacion")

            puede_cerrar = (
                detalle["total_actividades"] > 0
                and detalle["actividades_otro_estado"] == 0
                and detalle["actividades_sin_evidencias"] == 0
                and detalle["actividades_con_evidencias_no_aprobadas"] == 0
                and detalle["evidencias_sin_estado"] == 0
            )

            mensaje = "Cierre habilitado" if puede_cerrar else "Cierre bloqueado"
            return {
                "ok": True,
                "puede_cerrar": puede_cerrar,
                "mensaje": mensaje,
                "detalle": detalle,
                "bloqueos": bloqueos
            }

        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def registrar_cierre_registro(
        self,
        codigo: str,
        tipo_origen: str,
        responsable_cierre: str,
        fecha_cierre: str,
        tipo_cierre: str,
        descripcion_cierre: str,
        usuario: str = "unknown"
    ) -> Dict:
        # Registra el cierre de un registro y actualiza su estado.
        inicio = time.time()
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()
        responsable_cierre = (responsable_cierre or "").strip()
        tipo_cierre = (tipo_cierre or "").strip()
        descripcion_cierre = (descripcion_cierre or "").strip()

        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}
        if not responsable_cierre:
            return {"ok": False, "mensaje": "Responsable de cierre requerido"}
        if not fecha_cierre:
            return {"ok": False, "mensaje": "Fecha de cierre requerida"}
        if not tipo_cierre:
            return {"ok": False, "mensaje": "Tipo de cierre requerido"}
        if not descripcion_cierre:
            return {"ok": False, "mensaje": "Descripcion de cierre requerida"}
        if len(responsable_cierre) > 150:
            return {"ok": False, "mensaje": "El responsable de cierre no puede exceder 150 caracteres"}

        tipos_validos = {"Eficaz", "No eficaz", "No ejecutado"}
        if tipo_cierre not in tipos_validos:
            return {"ok": False, "mensaje": "Tipo de cierre invalido"}

        try:
            fecha_obj = date.fromisoformat(fecha_cierre)
        except Exception:
            return {"ok": False, "mensaje": "Fecha de cierre invalida"}

        validacion = self.validar_cierre_registro(codigo, tipo)
        if not validacion.get("ok"):
            return {"ok": False, "mensaje": validacion.get("mensaje", "No se pudo validar el cierre")}
        if not validacion.get("puede_cerrar"):
            bloqueos = validacion.get("bloqueos") or []
            detalle_bloqueo = "; ".join(bloqueos)
            mensaje = validacion.get("mensaje", "Cierre bloqueado")
            if detalle_bloqueo:
                mensaje = f"{mensaje}: {detalle_bloqueo}"
            return {"ok": False, "mensaje": mensaje}

        if tipo == "PA":
            tabla = "sgi.planes_accion"
        elif tipo == "GC":
            tabla = "sgi.gestion_cambio"
        else:
            tabla = "sgi.revision_direccion"

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT estado FROM {tabla} WHERE codigo = %s", (codigo,))
                registro = cursor.fetchone()
                if not registro:
                    return {"ok": False, "mensaje": "Registro no encontrado"}

                estado_actual = str(registro.get("estado") or "").strip()
                if estado_actual.lower() == "cerrado":
                    return {"ok": False, "mensaje": "Registro ya cerrado"}

                cursor.execute(
                    "SELECT 1 FROM sgi.sgi_cierres WHERE codigo_referencia = %s AND tipo_origen = %s",
                    (codigo, tipo)
                )
                if cursor.fetchone():
                    return {"ok": False, "mensaje": "Este registro ya tiene un cierre"}

                cursor.execute(
                    "INSERT INTO sgi.sgi_cierres (codigo_referencia, tipo_origen, responsable_cierre, fecha_cierre, tipo_cierre, descripcion_cierre, usuario_cierre) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        codigo,
                        tipo,
                        responsable_cierre,
                        fecha_obj,
                        tipo_cierre,
                        descripcion_cierre,
                        usuario
                    )
                )

                cursor.execute(
                    f"UPDATE {tabla} SET estado = %s, fecha_actualizacion = %s WHERE codigo = %s",
                    ("Cerrado", now_bogota(), codigo)
                )

                self.connection.commit()

            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='CERRAR',
                        modulo=tipo,
                        tipo_entidad=f"Registro {tipo}",
                        codigo_registro=codigo,
                        detalle_registro=f"Cierre: {tipo_cierre}",
                        datos_antes={"estado": estado_actual},
                        datos_despues={
                            "estado": "Cerrado",
                            "responsable_cierre": responsable_cierre,
                            "fecha_cierre": fecha_cierre,
                            "tipo_cierre": tipo_cierre,
                            "descripcion_cierre": descripcion_cierre
                        },
                        duracion_ms=duracion,
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                except Exception as e:
                    print(f"?? Error al registrar auditoria de cierre: {e}")

            return {"ok": True, "mensaje": "Registro cerrado"}

        except Exception as e:
            self.connection.rollback()

            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='CERRAR',
                        modulo=tipo,
                        tipo_entidad=f"Registro {tipo}",
                        codigo_registro=codigo,
                        detalle_registro="Error al cerrar registro",
                        error_mensaje=str(e),
                        duracion_ms=duracion,
                        resultado='error'
                    )
                    auditoria.cerrar_conexion()
                except Exception as ae:
                    print(f"?? Error al registrar auditoria de cierre: {ae}")

            return {"ok": False, "mensaje": str(e)}

    def obtener_detalle_cierre_registro(self, codigo: str, tipo_origen: str) -> Dict:
        """Obtiene el detalle del cierre registrado en sgi_cierres."""
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()

        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        codigo_referencia as codigo,
                        tipo_origen as tipo,
                        responsable_cierre,
                        fecha_cierre,
                        TO_CHAR(fecha_cierre, 'DD/MM/YYYY') as fecha_cierre_formato,
                        tipo_cierre,
                        descripcion_cierre,
                        usuario_cierre
                    FROM sgi.sgi_cierres
                    WHERE codigo_referencia = %s AND tipo_origen = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (codigo, tipo)
                )
                row = cursor.fetchone()
                if not row:
                    return {"ok": False, "mensaje": "No hay cierre registrado"}

                detalle = dict(row)
                detalle = self._serializar_fechas(detalle)
                return {"ok": True, "detalle": detalle}

        except Exception as e:
            return {"ok": False, "mensaje": str(e)}

    def obtener_historial_cierre_registro(self, codigo: str, tipo_origen: str) -> Dict:
        """Obtiene el historial de cambios de un cierre."""
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()

        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        codigo_referencia as codigo,
                        tipo_origen as tipo,
                        responsable_cierre_anterior,
                        fecha_cierre_anterior,
                        TO_CHAR(fecha_cierre_anterior, 'DD/MM/YYYY') as fecha_cierre_anterior_formato,
                        tipo_cierre_anterior,
                        descripcion_cierre_anterior,
                        responsable_cierre_nuevo,
                        fecha_cierre_nuevo,
                        TO_CHAR(fecha_cierre_nuevo, 'DD/MM/YYYY') as fecha_cierre_nuevo_formato,
                        tipo_cierre_nuevo,
                        descripcion_cierre_nuevo,
                        usuario_modificacion,
                        fecha_modificacion,
                        TO_CHAR(fecha_modificacion, 'DD/MM/YYYY HH24:MI') as fecha_modificacion_formato
                    FROM sgi.sgi_cierres_historial
                    WHERE codigo_referencia = %s AND tipo_origen = %s
                    ORDER BY fecha_modificacion DESC, id DESC
                    """,
                    (codigo, tipo)
                )
                rows = [dict(row) for row in cursor.fetchall()]
                for row in rows:
                    self._serializar_fechas(row)
                return {"ok": True, "historial": rows}

        except Exception as e:
            return {"ok": False, "mensaje": str(e)}

    def actualizar_cierre_registro(
        self,
        codigo: str,
        tipo_origen: str,
        responsable_cierre: str,
        fecha_cierre: str,
        tipo_cierre: str,
        descripcion_cierre: str,
        usuario: str = "unknown"
    ) -> Dict:
        # Actualiza los datos de cierre registrados.
        inicio = time.time()
        tipo = (tipo_origen or "").strip().upper()
        codigo = (codigo or "").strip()
        responsable_cierre = (responsable_cierre or "").strip()
        tipo_cierre = (tipo_cierre or "").strip()
        descripcion_cierre = (descripcion_cierre or "").strip()

        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}
        if not codigo:
            return {"ok": False, "mensaje": "Codigo requerido"}
        if not responsable_cierre:
            return {"ok": False, "mensaje": "Responsable de cierre requerido"}
        if not fecha_cierre:
            return {"ok": False, "mensaje": "Fecha de cierre requerida"}
        if not tipo_cierre:
            return {"ok": False, "mensaje": "Tipo de cierre requerido"}
        if not descripcion_cierre:
            return {"ok": False, "mensaje": "Descripcion de cierre requerida"}
        if len(responsable_cierre) > 150:
            return {"ok": False, "mensaje": "El responsable de cierre no puede exceder 150 caracteres"}

        tipos_validos = {"Eficaz", "No eficaz", "No ejecutado"}
        if tipo_cierre not in tipos_validos:
            return {"ok": False, "mensaje": "Tipo de cierre invalido"}

        try:
            fecha_obj = date.fromisoformat(fecha_cierre)
        except Exception:
            return {"ok": False, "mensaje": "Fecha de cierre invalida"}

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        responsable_cierre,
                        fecha_cierre,
                        tipo_cierre,
                        descripcion_cierre
                    FROM sgi.sgi_cierres
                    WHERE codigo_referencia = %s AND tipo_origen = %s
                    """,
                    (codigo, tipo)
                )
                anterior = cursor.fetchone()
                if not anterior:
                    return {"ok": False, "mensaje": "No hay cierre registrado", "no_encontrado": True}

                responsable_anterior = (anterior.get("responsable_cierre") or "").strip()
                tipo_anterior = (anterior.get("tipo_cierre") or "").strip()
                descripcion_anterior = (anterior.get("descripcion_cierre") or "").strip()
                fecha_anterior = anterior.get("fecha_cierre")
                hay_cambios = (
                    responsable_anterior != responsable_cierre
                    or tipo_anterior != tipo_cierre
                    or descripcion_anterior != descripcion_cierre
                    or fecha_anterior != fecha_obj
                )

                cursor.execute(
                    """
                    UPDATE sgi.sgi_cierres
                    SET responsable_cierre = %s,
                        fecha_cierre = %s,
                        tipo_cierre = %s,
                        descripcion_cierre = %s
                    WHERE codigo_referencia = %s AND tipo_origen = %s
                    RETURNING
                        codigo_referencia as codigo,
                        tipo_origen as tipo,
                        responsable_cierre,
                        fecha_cierre,
                        TO_CHAR(fecha_cierre, 'DD/MM/YYYY') as fecha_cierre_formato,
                        tipo_cierre,
                        descripcion_cierre,
                        usuario_cierre
                    """,
                    (
                        responsable_cierre,
                        fecha_obj,
                        tipo_cierre,
                        descripcion_cierre,
                        codigo,
                        tipo
                    )
                )
                actualizado = cursor.fetchone()

                if hay_cambios:
                    cursor.execute(
                        """
                        INSERT INTO sgi.sgi_cierres_historial (
                            codigo_referencia,
                            tipo_origen,
                            responsable_cierre_anterior,
                            fecha_cierre_anterior,
                            tipo_cierre_anterior,
                            descripcion_cierre_anterior,
                            responsable_cierre_nuevo,
                            fecha_cierre_nuevo,
                            tipo_cierre_nuevo,
                            descripcion_cierre_nuevo,
                            usuario_modificacion,
                            fecha_modificacion
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            codigo,
                            tipo,
                            responsable_anterior or None,
                            fecha_anterior,
                            tipo_anterior or None,
                            descripcion_anterior or None,
                            responsable_cierre,
                            fecha_obj,
                            tipo_cierre,
                            descripcion_cierre,
                            usuario,
                            now_bogota()
                        )
                    )
                self.connection.commit()

            detalle = dict(actualizado) if actualizado else {}
            detalle = self._serializar_fechas(detalle)

            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='MODIFICAR',
                        modulo=tipo,
                        tipo_entidad='Cierre',
                        codigo_registro=codigo,
                        detalle_registro='Actualizacion de cierre',
                        datos_antes={
                            "responsable_cierre": anterior.get("responsable_cierre"),
                            "fecha_cierre": str(anterior.get("fecha_cierre")) if anterior.get("fecha_cierre") else None,
                            "tipo_cierre": anterior.get("tipo_cierre"),
                            "descripcion_cierre": anterior.get("descripcion_cierre")
                        },
                        datos_despues={
                            "responsable_cierre": responsable_cierre,
                            "fecha_cierre": fecha_cierre,
                            "tipo_cierre": tipo_cierre,
                            "descripcion_cierre": descripcion_cierre
                        },
                        duracion_ms=duracion,
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                except Exception as e:
                    print(f"?? Error al registrar auditoria de cierre editado: {e}")

            return {"ok": True, "mensaje": "Cierre actualizado", "detalle": detalle}

        except Exception as e:
            self.connection.rollback()

            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='MODIFICAR',
                        modulo=tipo,
                        tipo_entidad='Cierre',
                        codigo_registro=codigo,
                        detalle_registro='Error al actualizar cierre',
                        duracion_ms=duracion,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except Exception as ae:
                    print(f"?? Error al registrar auditoria de cierre editado: {ae}")

            return {"ok": False, "mensaje": str(e)}

    def eliminar_registro_por_codigo(self, codigo: str, tipo_origen: str) -> Dict:
        """Elimina o desactiva un registro principal y limpia actividades/evidencias."""
        tipo = (tipo_origen or "").strip().upper()
        try:
            if tipo not in ("PA", "GC", "RVD"):
                return {"ok": False, "mensaje": "Tipo invalido"}

            bloqueo = self._validar_registro_modificable(codigo, tipo)
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            actividades_ids = []
            evidencias = []
            adjuntos = []
            codigo_seg = _normalizar_codigo_ruta(codigo)

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM sgi.sgi_actividades
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                actividades_ids = [row["id"] for row in cursor.fetchall()]

                cursor.execute(
                    """
                    SELECT id, container, blob_path
                    FROM sgi.sgi_evidencias
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                evidencias = [dict(row) for row in cursor.fetchall()]

                cursor.execute(
                    """
                    SELECT id, container, blob_path
                    FROM sgi.sgi_analisis_causas_adjuntos
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                adjuntos = [dict(row) for row in cursor.fetchall()]

            errores_blobs = self._eliminar_blobs_evidencias(evidencias)
            errores_adjuntos = self._eliminar_blobs_adjuntos(adjuntos)
            errores_directorios = []
            prefijos_por_container = {}
            for item in evidencias + adjuntos:
                container = item.get("container")
                blob_path = item.get("blob_path")
                directorio = self._extraer_directorio_codigo(blob_path, codigo_seg)
                if not container or not directorio:
                    continue
                prefijos_por_container.setdefault(container, set()).add(directorio)

            for container, directorios in prefijos_por_container.items():
                for directorio in directorios:
                    errores_directorios += self._eliminar_directorio_datalake(container, directorio)

            errores_prefijos = self._eliminar_prefijos_storage_por_codigo(codigo, tipo)

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM sgi.sgi_evidencias
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                if actividades_ids:
                    cursor.execute("DELETE FROM sgi.sgi_actividades WHERE id = ANY(%s)", (actividades_ids,))

                cursor.execute(
                    """
                    DELETE FROM sgi.sgi_analisis_causas_adjuntos
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                cursor.execute(
                    """
                    DELETE FROM sgi.sgi_notificaciones
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_entidad)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )

                if tipo == "PA":
                    cursor.execute("DELETE FROM sgi.planes_accion WHERE codigo = %s", (codigo,))
                elif tipo == "GC":
                    cursor.execute("DELETE FROM sgi.gestion_cambio WHERE codigo = %s", (codigo,))
                elif tipo == "RVD":
                    cursor.execute("DELETE FROM sgi.revision_direccion WHERE codigo = %s", (codigo,))

                self.connection.commit()
                respuesta = {"ok": True, "filas": cursor.rowcount}
                advertencias = errores_blobs + errores_adjuntos + errores_directorios + errores_prefijos
                if advertencias:
                    respuesta["advertencias"] = advertencias
                    respuesta["mensaje"] = "Registro eliminado con advertencias al borrar blobs"
                return respuesta
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def eliminar_actividades_por_codigo(self, codigo: str, tipo_origen: str) -> Dict:
        """Elimina todas las actividades (y evidencias) asociadas a un codigo."""
        tipo = (tipo_origen or "").strip().upper()
        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}

        bloqueo = self._validar_registro_modificable(codigo, tipo)
        if bloqueo:
            return {"ok": False, "mensaje": bloqueo}

        try:
            actividades_ids = []
            evidencias = []

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM sgi.sgi_actividades
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                actividades_ids = [row["id"] for row in cursor.fetchall()]

                if actividades_ids:
                    cursor.execute(
                        """
                        SELECT id, container, blob_path
                        FROM sgi.sgi_evidencias
                        WHERE actividad_id = ANY(%s)
                        """,
                        (actividades_ids,)
                    )
                    evidencias = [dict(row) for row in cursor.fetchall()]

            if not actividades_ids:
                return {"ok": True, "filas": 0, "mensaje": "No hay actividades para eliminar"}

            errores_blobs = self._eliminar_blobs_evidencias(evidencias)

            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM sgi.sgi_evidencias WHERE actividad_id = ANY(%s)", (actividades_ids,))
                cursor.execute("DELETE FROM sgi.sgi_actividades WHERE id = ANY(%s)", (actividades_ids,))
                self.connection.commit()

            respuesta = {"ok": True, "filas": len(actividades_ids)}
            if errores_blobs:
                respuesta["advertencias"] = errores_blobs
                respuesta["mensaje"] = "Actividades eliminadas con advertencias al borrar blobs"
            return respuesta
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def eliminar_evidencias_por_codigo(self, codigo: str, tipo_origen: str, usuario: str = "unknown") -> Dict:
        """Elimina todas las evidencias asociadas a un codigo sin borrar actividades."""
        inicio = time.time()
        tipo = (tipo_origen or "").strip().upper()
        if tipo not in ("PA", "GC", "RVD"):
            return {"ok": False, "mensaje": "Tipo invalido"}

        bloqueo = self._validar_registro_modificable(codigo, tipo)
        if bloqueo:
            return {"ok": False, "mensaje": bloqueo}

        try:
            evidencias = []

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        ev.id,
                        ev.container,
                        ev.blob_path,
                        ev.nombre_archivo,
                        act.nombre_actividad
                    FROM sgi.sgi_evidencias ev
                    LEFT JOIN sgi.sgi_actividades act ON ev.actividad_id = act.id
                    WHERE regexp_replace(UPPER(ev.codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(ev.tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                evidencias = [dict(row) for row in cursor.fetchall()]

            if not evidencias:
                return {"ok": True, "filas": 0, "mensaje": "No hay evidencias para eliminar"}

            cantidad_evidencias = len(evidencias)
            nombres_archivos = [ev.get('nombre_archivo', 'archivo') for ev in evidencias[:3]]

            errores_blobs = self._eliminar_blobs_evidencias(evidencias)

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM sgi.sgi_evidencias
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo)
                )
                self.connection.commit()

            # Registrar auditoría de eliminación masiva
            if AUDITORIA_AVAILABLE and cantidad_evidencias > 0:
                duracion = int((time.time() - inicio) * 1000)

                if cantidad_evidencias == 1:
                    detalle = f"Evidencia eliminada: '{nombres_archivos[0]}'"
                elif cantidad_evidencias <= 3:
                    detalle = f"{cantidad_evidencias} evidencias eliminadas: {', '.join(nombres_archivos)}"
                else:
                    detalle = f"{cantidad_evidencias} evidencias eliminadas (primeras 3: {', '.join(nombres_archivos)}, ...)"

                datos_antes = {
                    'cantidad_evidencias': cantidad_evidencias,
                    'codigo_registro': codigo,
                    'tipo_origen': tipo
                }

                try:
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='ELIMINAR',
                        modulo=tipo,
                        tipo_entidad='Evidencias (múltiples)',
                        codigo_registro=codigo,
                        detalle_registro=detalle,
                        datos_antes=datos_antes,
                        duracion_ms=duracion,
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                    print(f"✅ Auditoría registrada: ELIMINAR {cantidad_evidencias} evidencias de {tipo} {codigo}")
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de eliminación masiva: {e_audit}")

            respuesta = {"ok": True, "filas": len(evidencias)}
            if errores_blobs:
                respuesta["advertencias"] = errores_blobs
                respuesta["mensaje"] = "Evidencias eliminadas con advertencias al borrar blobs"
            return respuesta
        except Exception as e:
            self.connection.rollback()

            # Registrar error en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    duracion = int((time.time() - inicio) * 1000)
                    auditoria = AuditoriaSGI()
                    auditoria.registrar_log(
                        usuario=usuario,
                        accion='ELIMINAR',
                        modulo=tipo,
                        tipo_entidad='Evidencias',
                        codigo_registro=codigo,
                        detalle_registro=f"Error al eliminar evidencias de {tipo} {codigo}",
                        duracion_ms=duracion,
                        resultado='error',
                        error_mensaje=str(e)
                    )
                    auditoria.cerrar_conexion()
                except Exception as e_audit:
                    print(f"⚠️ Error al registrar auditoría de error: {e_audit}")

            return {"ok": False, "mensaje": str(e)}
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def crear_actividad(self, datos: Dict) -> Dict:
        """Crea una actividad en sgi_actividades."""
        try:
            bloqueo = self._validar_registro_modificable(
                datos.get("codigo_referencia"),
                datos.get("tipo_origen")
            )
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            tipo_origen = (datos.get("tipo_origen") or "").strip().upper()
            tipo_actividad = self._normalizar_tipo_actividad(
                tipo_origen,
                datos.get("tipo_actividad")
            )

            query = """
                INSERT INTO sgi.sgi_actividades (
                    codigo_referencia,
                    tipo_origen,
                    tipo_actividad,
                    etapa_actividad,
                    nombre_actividad,
                    descripcion,
                    fecha_cierre,
                    responsable,
                    estado,
                    categoria,
                    fecha_creacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    datos.get("codigo_referencia"),
                    tipo_origen,
                    tipo_actividad,
                    datos.get("etapa_actividad"),
                    datos.get("nombre_actividad"),
                    datos.get("descripcion"),
                    datos.get("fecha_cierre"),
                    datos.get("responsable"),
                    datos.get("estado"),
                    datos.get("categoria"),
                    now_bogota()
                ))
                row = cursor.fetchone()
                self.connection.commit()
                return {"ok": True, "id": row[0] if row else None}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def actualizar_actividad(self, actividad_id: int, datos: Dict) -> Dict:
        """Actualiza una actividad."""
        campos = [
            "etapa_actividad", "nombre_actividad", "descripcion",
            "fecha_cierre", "responsable", "estado", "categoria"
        ]
        try:
            referencia = self._obtener_referencia_por_actividad(actividad_id)
            if not referencia:
                return {"ok": False, "mensaje": "Actividad no encontrada"}

            bloqueo = self._validar_registro_modificable(
                referencia.get("codigo_referencia"),
                referencia.get("tipo_origen")
            )
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            tipo_origen = (referencia.get("tipo_origen") or "").strip().upper()
            if "tipo_actividad" in datos:
                if tipo_origen != "PA":
                    datos.pop("tipo_actividad", None)
                else:
                    try:
                        datos["tipo_actividad"] = self._normalizar_tipo_actividad(
                            tipo_origen,
                            datos.get("tipo_actividad")
                        )
                    except ValueError as e:
                        return {"ok": False, "mensaje": str(e)}
                    campos.append("tipo_actividad")

            sets = []
            valores = []
            for campo in campos:
                if campo in datos:
                    sets.append(f"{campo} = %s")
                    valores.append(datos.get(campo))
            if not sets:
                return {"ok": False, "mensaje": "Sin cambios"}

            valores.append(actividad_id)
            query = f"UPDATE sgi.sgi_actividades SET {', '.join(sets)} WHERE id = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(query, valores)
                self.connection.commit()
                return {"ok": True, "filas": cursor.rowcount}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def eliminar_actividad(self, actividad_id: int) -> Dict:
        """Elimina una actividad y sus evidencias."""
        try:
            referencia = self._obtener_referencia_por_actividad(actividad_id)
            if not referencia:
                return {"ok": False, "mensaje": "Actividad no encontrada"}

            bloqueo = self._validar_registro_modificable(
                referencia.get("codigo_referencia"),
                referencia.get("tipo_origen")
            )
            if bloqueo:
                return {"ok": False, "mensaje": bloqueo}

            evidencias = []
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, container, blob_path FROM sgi.sgi_evidencias WHERE actividad_id = %s",
                    (actividad_id,)
                )
                evidencias = [dict(row) for row in cursor.fetchall()]

            errores_blobs = self._eliminar_blobs_evidencias(evidencias)

            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM sgi.sgi_evidencias WHERE actividad_id = %s", (actividad_id,))
                cursor.execute("DELETE FROM sgi.sgi_actividades WHERE id = %s", (actividad_id,))
                self.connection.commit()
                respuesta = {"ok": True, "filas": cursor.rowcount}
                if errores_blobs:
                    respuesta["advertencias"] = errores_blobs
                    respuesta["mensaje"] = "Actividad eliminada con advertencias al borrar blobs"
                return respuesta
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "mensaje": str(e)}

    def construir_ruta_blob_evidencia(
        self,
        tipo_origen: str,
        codigo_referencia: str,
        actividad_id: int,
        nombre_archivo: str
    ) -> str:
        fecha = now_bogota()
        tipo_seg = _normalizar_segmento(tipo_origen).lower()
        codigo_seg = _normalizar_codigo_ruta(codigo_referencia)
        prefijo, prefijo_especifico = self.obtener_prefijo_evidencias(tipo_origen)
        prefijo = _normalizar_prefijo_ruta(prefijo or "001-sgi")
        carpeta_tipo = f"{prefijo}-{tipo_seg}" if prefijo else tipo_seg
        archivo_seg = _generar_nombre_archivo_evidencia(nombre_archivo, fecha)
        if prefijo_especifico:
            base = prefijo
        else:
            if prefijo:
                base = f"{prefijo}/{carpeta_tipo}"
            else:
                base = carpeta_tipo
        ruta = (
            f"{base}/{codigo_seg}/act-{actividad_id}/"
            f"{fecha.year}/{fecha.month:02d}/{fecha.day:02d}/{archivo_seg}"
        )
        return ruta

    def construir_ruta_blob_analisis_causas(
        self,
        tipo_origen: str,
        codigo_referencia: str,
        nombre_archivo: str
    ) -> str:
        fecha = now_bogota()
        codigo_seg = _normalizar_codigo_ruta(codigo_referencia)
        prefijo = self.obtener_prefijo_analisis_causas() or "001-sgi/001-sgi-analisis-causas"
        archivo_seg = _generar_nombre_archivo_evidencia(nombre_archivo, fecha)
        ruta = (
            f"{prefijo}/{codigo_seg}/"
            f"{fecha.year}/{fecha.month:02d}/{fecha.day:02d}/{archivo_seg}"
        )
        return ruta

    def diagnostico_actividades(self, codigo: str, tipo_origen: str) -> Dict:
        """Devuelve informacion de diagnostico para actividades."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user")
                db, user = cursor.fetchone()

                cursor.execute(
                    "SELECT COUNT(*) FROM sgi.sgi_actividades WHERE codigo_referencia = %s",
                    (codigo,)
                )
                count_exact = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*) FROM sgi.sgi_actividades
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                    """,
                    (codigo,)
                )
                count_norm = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*) FROM sgi.sgi_actividades
                    WHERE regexp_replace(UPPER(codigo_referencia), '[^A-Z0-9]', '', 'g')
                        = regexp_replace(UPPER(%s), '[^A-Z0-9]', '', 'g')
                        AND UPPER(TRIM(tipo_origen)) = UPPER(TRIM(%s))
                    """,
                    (codigo, tipo_origen)
                )
                count_tipo = cursor.fetchone()[0]

                return {
                    "database": db,
                    "db_user": user,
                    "count_exact": count_exact,
                    "count_normalizado": count_norm,
                    "count_tipo": count_tipo
                }
        except Exception as e:
            self.connection.rollback()
            return {"error": str(e)}

    def _generar_url_firmada_evidencia(self, evidencia: Dict) -> Optional[str]:
        """Genera una URL firmada (SAS) para visualizar una evidencia."""
        try:
            if not evidencia.get("container") or not evidencia.get("blob_path"):
                return None

            partes = _parse_storage_connection_string()
            account_name = partes.get("AccountName")
            account_key = partes.get("AccountKey")
            blob_endpoint = partes.get("BlobEndpoint")

            if not account_name or not account_key:
                return None

            now_utc = datetime.now(timezone.utc)
            start_time = now_utc - timedelta(minutes=5)
            expiry = now_utc + timedelta(minutes=SGI_EVIDENCIAS_SAS_MINUTES)
            sas = generate_blob_sas(
                account_name=account_name,
                container_name=evidencia["container"],
                blob_name=evidencia["blob_path"],
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                start=start_time,
                expiry=expiry,
                content_disposition=_build_content_disposition(evidencia.get("nombre_archivo", "archivo")),
                content_type=evidencia.get("content_type")
            )

            base_url = blob_endpoint.rstrip("/") if blob_endpoint else f"https://{account_name}.blob.core.windows.net"
            return f"{base_url}/{evidencia['container']}/{evidencia['blob_path']}?{sas}"
        except Exception as e:
            print(f"Error generando URL firmada: {e}")
            return None
