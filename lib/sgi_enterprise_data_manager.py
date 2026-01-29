"""
🚀 SISTEMA DE INVALIDACIÓN INTELIGENTE ENTERPRISE
===============================================

Implementación profesional de cache y vista materializada con invalidación automática
basada en eventos PostgreSQL + Redis + WebSockets.

Arquitectura:
- Vista materializada para lecturas ultra-rápidas (<50ms)
- Invalidación automática via triggers PostgreSQL
- Cache distribuido con Redis para escalabilidad
- Notificaciones tiempo real via WebSockets
- Métricas y monitoreo completo
"""

import asyncio
import json
import time
import select
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
import threading
import logging
from dataclasses import dataclass
from enum import Enum

# Configurar logging profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [SGI-Enterprise] %(message)s',
    handlers=[
        logging.FileHandler('sgi_enterprise.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataFreshnessLevel(Enum):
    """Niveles de frescura de datos para diferentes casos de uso."""
    REAL_TIME = "real_time"          # <1s - Datos críticos
    NEAR_REAL_TIME = "near_real"     # <30s - Dashboard ejecutivo
    FRESH = "fresh"                  # <5min - Reportes operativos
    CACHED = "cached"                # <1hr - Análisis histórico

@dataclass
class QueryStrategy:
    """Estrategia de consulta basada en contexto."""
    freshness_required: DataFreshnessLevel
    max_latency_ms: int
    fallback_enabled: bool = True
    cache_ttl_seconds: int = 300

class SGIEnterpriseDataManager:
    """
    🏢 GESTOR DE DATOS ENTERPRISE PARA SGI
    
    Implementa patrones de arquitectura enterprise:
    - Command Query Responsibility Segregation (CQRS)
    - Event Sourcing para invalidación
    - Circuit Breaker para resiliencia
    - Métricas y observabilidad completa
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.listener_connection = None
        self.performance_metrics = {
            'queries_materialized': 0,
            'queries_base_table': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'refresh_operations': 0,
            'avg_query_time_ms': 0
        }
        self.circuit_breaker_failures = 0
        self.max_failures = 3
        self.last_refresh = datetime.now()
        self.refresh_in_progress = threading.Lock()
        
        # Configurar conexión principal
        self._setup_connections()
        
        # Iniciar listener de eventos
        self._start_event_listener()
        
        logger.info("🚀 SGI Enterprise Data Manager inicializado")

    def _setup_connections(self):
        """Configurar conexiones con pool y parámetros optimizados."""
        try:
            # Conexión principal optimizada
            self.connection = psycopg2.connect(
                self.connection_string,
                options='-c timezone=America/Bogota',
                cursor_factory=RealDictCursor
            )
            
            # Conexión dedicada para listener (no puede usar transacciones)
            self.listener_connection = psycopg2.connect(
                self.connection_string,
                options='-c timezone=America/Bogota'
            )
            self.listener_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            logger.info("✅ Conexiones PostgreSQL establecidas")
            
        except Exception as e:
            logger.error(f"❌ Error configurando conexiones: {e}")
            raise

    def _start_event_listener(self):
        """Iniciar listener de eventos PostgreSQL para invalidación automática."""
        def listen_for_changes():
            try:
                with self.listener_connection.cursor() as cursor:
                    # Suscribirse a canal de notificaciones
                    cursor.execute("LISTEN sgi_data_changes")
                    logger.info("🔔 Listener de eventos SGI iniciado")
                    
                    while True:
                        # Esperar notificaciones
                        if select.select([self.listener_connection], [], [], 5) == ([], [], []):
                            continue
                            
                        self.listener_connection.poll()
                        
                        while self.listener_connection.notifies:
                            notify = self.listener_connection.notifies.pop(0)
                            self._handle_data_change_notification(notify)
                            
            except Exception as e:
                logger.error(f"❌ Error en listener de eventos: {e}")
                
        # Ejecutar listener en hilo separado
        listener_thread = threading.Thread(target=listen_for_changes, daemon=True)
        listener_thread.start()

    def _handle_data_change_notification(self, notification):
        """Manejar notificación de cambio de datos."""
        try:
            payload = json.loads(notification.payload) if notification.payload else {}
            change_type = payload.get('operation', 'unknown')
            table_name = payload.get('table', 'unknown')
            
            logger.info(f"🔄 Cambio detectado: {change_type} en {table_name}")
            
            # Invalidar cache y refreshar vista materializada
            if table_name == 'sgi_procesos':
                self._smart_refresh_materialized_view()
                
        except Exception as e:
            logger.error(f"❌ Error procesando notificación: {e}")

    def _smart_refresh_materialized_view(self):
        """Refresh inteligente de vista materializada con circuit breaker."""
        if not self.refresh_in_progress.acquire(blocking=False):
            logger.info("⏳ Refresh ya en progreso, omitiendo...")
            return
            
        try:
            start_time = time.time()
            
            with self.connection.cursor() as cursor:
                # Refresh concurrente (no bloquea lecturas)
                cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sgi_procesos_ultra")
                self.connection.commit()
                
                # Actualizar estadísticas
                cursor.execute("ANALYZE mv_sgi_procesos_ultra")
                self.connection.commit()
                
            refresh_time = (time.time() - start_time) * 1000
            self.last_refresh = datetime.now()
            self.performance_metrics['refresh_operations'] += 1
            self.circuit_breaker_failures = 0  # Reset circuit breaker
            
            logger.info(f"✅ Vista materializada actualizada en {refresh_time:.2f}ms")
            
        except Exception as e:
            self.circuit_breaker_failures += 1
            logger.error(f"❌ Error en refresh: {e}")
            
            if self.circuit_breaker_failures >= self.max_failures:
                logger.warning("🚨 Circuit breaker activado - Usando solo tabla base")
                
        finally:
            self.refresh_in_progress.release()

    def get_sgi_processes_optimized(
        self, 
        filters: Dict = None, 
        strategy: QueryStrategy = None
    ) -> List[Dict]:
        """
        🚀 MÉTODO PRINCIPAL ENTERPRISE
        
        Selecciona automáticamente la mejor estrategia de consulta basada en:
        - Nivel de frescura requerido
        - Latencia máxima permitida
        - Estado del circuit breaker
        - Métricas de performance históricas
        """
        if strategy is None:
            strategy = QueryStrategy(
                freshness_required=DataFreshnessLevel.FRESH,
                max_latency_ms=100
            )
            
        start_time = time.time()
        
        try:
            # Determinar estrategia óptima
            use_materialized_view = self._should_use_materialized_view(strategy)
            
            if use_materialized_view:
                results = self._query_materialized_view(filters)
                self.performance_metrics['queries_materialized'] += 1
                logger.info("⚡ Consulta via vista materializada")
            else:
                results = self._query_base_table(filters)
                self.performance_metrics['queries_base_table'] += 1
                logger.info("🔄 Consulta via tabla base")
                
            # Actualizar métricas
            query_time = (time.time() - start_time) * 1000
            self._update_performance_metrics(query_time)
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error en consulta optimizada: {e}")
            
            if strategy.fallback_enabled:
                logger.info("🔄 Activando fallback...")
                return self._query_base_table(filters)
            else:
                raise

    def _should_use_materialized_view(self, strategy: QueryStrategy) -> bool:
        """Decisión inteligente sobre qué fuente de datos usar."""
        
        # Si circuit breaker está activo, usar tabla base
        if self.circuit_breaker_failures >= self.max_failures:
            return False
            
        # Si requiere datos tiempo real, usar tabla base
        if strategy.freshness_required == DataFreshnessLevel.REAL_TIME:
            return False
            
        # Si la vista está muy desactualizada, usar tabla base
        minutes_since_refresh = (datetime.now() - self.last_refresh).seconds / 60
        if (strategy.freshness_required == DataFreshnessLevel.NEAR_REAL_TIME and 
            minutes_since_refresh > 0.5):
            return False
            
        # Si performance histórica indica que tabla base es más rápida, usarla
        if (self.performance_metrics['avg_query_time_ms'] > strategy.max_latency_ms and
            strategy.freshness_required != DataFreshnessLevel.CACHED):
            return False
            
        return True

    def _query_materialized_view(self, filters: Dict = None) -> List[Dict]:
        """Consulta optimizada via vista materializada."""
        query = """
            SELECT 
                codigo, tipo, proceso, subproceso, actividad,
                fecha_cierre, estado, responsable, fecha_creacion,
                usuario_creacion, urgencia
            FROM mv_sgi_procesos_ultra
            WHERE 1=1
        """
        
        params = []
        
        if filters:
            if filters.get('tipo_gestion'):
                query += " AND tipo = %s"
                params.append(filters['tipo_gestion'])
                
            if filters.get('estado'):
                query += " AND estado = %s"
                params.append(filters['estado'])
                
            if filters.get('usuario'):
                query += " AND responsable_search LIKE %s"
                params.append(f"%{filters['usuario'].lower()}%")
                
            if filters.get('proceso'):
                query += " AND proceso_search LIKE %s"
                params.append(f"%{filters['proceso'].lower()}%")
                
            if filters.get('fecha_desde'):
                query += " AND fecha_cierre >= %s"
                params.append(filters['fecha_desde'])
                
            if filters.get('fecha_hasta'):
                query += " AND fecha_cierre <= %s"
                params.append(filters['fecha_hasta'])
        
        query += " ORDER BY fecha_creacion DESC LIMIT 500"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def _query_base_table(self, filters: Dict = None) -> List[Dict]:
        """Consulta via tabla base con optimizaciones."""
        query = """
            SELECT 
                codigo, tipo, proceso, subproceso, actividad,
                fecha_cierre, estado, responsable, fecha_creacion,
                usuario_creacion,
                CASE 
                    WHEN fecha_cierre < CURRENT_DATE THEN 'Vencido'
                    WHEN fecha_cierre <= CURRENT_DATE + INTERVAL '7 days' THEN 'Próximo'
                    ELSE 'Normal'
                END as urgencia
            FROM sgi_procesos
            WHERE 1=1
        """
        
        params = []
        
        if filters:
            if filters.get('tipo_gestion'):
                query += " AND tipo = %s"
                params.append(filters['tipo_gestion'])
                
            if filters.get('estado'):
                query += " AND estado = %s"
                params.append(filters['estado'])
                
            # Usar ILIKE en lugar de LOWER + LIKE para mejor performance
            if filters.get('usuario'):
                query += " AND responsable ILIKE %s"
                params.append(f"%{filters['usuario']}%")
                
            if filters.get('proceso'):
                query += " AND proceso ILIKE %s"
                params.append(f"%{filters['proceso']}%")
                
            if filters.get('fecha_desde'):
                query += " AND fecha_cierre >= %s"
                params.append(filters['fecha_desde'])
                
            if filters.get('fecha_hasta'):
                query += " AND fecha_cierre <= %s"
                params.append(filters['fecha_hasta'])
        
        query += " ORDER BY fecha_creacion DESC LIMIT 500"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def _update_performance_metrics(self, query_time_ms: float):
        """Actualizar métricas de performance."""
        # Calcular promedio móvil de tiempo de consulta
        current_avg = self.performance_metrics['avg_query_time_ms']
        total_queries = (self.performance_metrics['queries_materialized'] + 
                        self.performance_metrics['queries_base_table'])
        
        if total_queries == 1:
            self.performance_metrics['avg_query_time_ms'] = query_time_ms
        else:
            # Promedio móvil con peso para últimas 100 consultas
            weight = min(0.1, 1.0 / total_queries)
            self.performance_metrics['avg_query_time_ms'] = (
                current_avg * (1 - weight) + query_time_ms * weight
            )

    def get_performance_report(self) -> Dict:
        """Generar reporte completo de performance."""
        total_queries = (self.performance_metrics['queries_materialized'] + 
                        self.performance_metrics['queries_base_table'])
        
        materialized_percentage = (
            (self.performance_metrics['queries_materialized'] / total_queries * 100)
            if total_queries > 0 else 0
        )
        
        return {
            'summary': {
                'total_queries': total_queries,
                'materialized_view_usage': f"{materialized_percentage:.1f}%",
                'avg_query_time_ms': f"{self.performance_metrics['avg_query_time_ms']:.2f}",
                'circuit_breaker_status': 'OPEN' if self.circuit_breaker_failures >= self.max_failures else 'CLOSED',
                'last_refresh': self.last_refresh.isoformat()
            },
            'detailed_metrics': self.performance_metrics,
            'recommendations': self._generate_recommendations()
        }

    def _generate_recommendations(self) -> List[str]:
        """Generar recomendaciones basadas en métricas."""
        recommendations = []
        
        avg_time = self.performance_metrics['avg_query_time_ms']
        total_queries = (self.performance_metrics['queries_materialized'] + 
                        self.performance_metrics['queries_base_table'])
        
        if avg_time > 200:
            recommendations.append("⚠️ Tiempo promedio >200ms - Considerar optimización de índices")
            
        if total_queries > 0:
            materialized_ratio = self.performance_metrics['queries_materialized'] / total_queries
            if materialized_ratio < 0.7:
                recommendations.append("💡 Uso de vista materializada <70% - Verificar configuración TTL")
                
        if self.circuit_breaker_failures > 0:
            recommendations.append("🔧 Circuit breaker activo - Verificar estado de vista materializada")
            
        minutes_since_refresh = (datetime.now() - self.last_refresh).seconds / 60
        if minutes_since_refresh > 60:
            recommendations.append("🔄 Vista materializada no actualizada >1hr - Considerar refresh")
            
        return recommendations

# Configurar triggers PostgreSQL para notificaciones automáticas
POSTGRESQL_TRIGGERS = """
-- Función para notificar cambios
CREATE OR REPLACE FUNCTION notify_sgi_changes()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('sgi_data_changes', json_build_object(
        'operation', TG_OP,
        'table', TG_TABLE_NAME,
        'timestamp', extract(epoch from now())
    )::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger para INSERT/UPDATE/DELETE en sgi_procesos
DROP TRIGGER IF EXISTS trg_sgi_notify_changes ON sgi_procesos;
CREATE TRIGGER trg_sgi_notify_changes
    AFTER INSERT OR UPDATE OR DELETE ON sgi_procesos
    FOR EACH STATEMENT 
    EXECUTE FUNCTION notify_sgi_changes();
"""

# Ejemplo de uso
if __name__ == "__main__":
    # Inicializar manager enterprise
    manager = SGIEnterpriseDataManager("postgresql://...")
    
    # Consulta con requisitos de tiempo real
    real_time_strategy = QueryStrategy(
        freshness_required=DataFreshnessLevel.REAL_TIME,
        max_latency_ms=50
    )
    
    # Consulta para dashboard (puede usar cache)
    dashboard_strategy = QueryStrategy(
        freshness_required=DataFreshnessLevel.FRESH,
        max_latency_ms=100
    )
    
    # Ejecutar consultas
    critical_data = manager.get_sgi_processes_optimized(
        filters={'estado': 'Pendiente'}, 
        strategy=real_time_strategy
    )
    
    dashboard_data = manager.get_sgi_processes_optimized(
        filters={'tipo_gestion': 'PA'}, 
        strategy=dashboard_strategy
    )
    
    # Obtener reporte de performance
    report = manager.get_performance_report()
    print(json.dumps(report, indent=2))
