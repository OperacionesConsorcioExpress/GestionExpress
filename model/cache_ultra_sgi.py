"""
ULTRA CACHE SYSTEM SGI - Sistema avanzado de cache inteligente
Implementa cache multinivel, invalidación automática y optimización de consultas
Objetivo: Reducir tiempo de consultas de 1000ms+ a <50ms
Autor: Sistema de Optimización SGI
Fecha: Noviembre 2025
"""

import os
import json
import hashlib
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable, Tuple
from dataclasses import dataclass, asdict
from collections import OrderedDict
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")

@dataclass
class CacheEntry:
    """Entrada de cache con metadatos"""
    data: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int
    ttl_seconds: int
    tags: List[str]
    size_bytes: int
    
    def is_expired(self) -> bool:
        """Verifica si la entrada ha expirado"""
        return datetime.now() > self.created_at + timedelta(seconds=self.ttl_seconds)
    
    def is_stale(self, max_age_seconds: int = 300) -> bool:
        """Verifica si la entrada está obsoleta"""
        return (datetime.now() - self.last_accessed).total_seconds() > max_age_seconds

class UltraCacheSGI:
    """Sistema de cache ultra avanzado para SGI"""
    
    def __init__(self, max_size_mb: int = 50, debug: bool = False):
        self.max_size_bytes = max_size_mb * 1024 * 1024  # Convertir MB a bytes
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.current_size = 0
        self.debug = debug
        
        # Locks para thread safety
        self.cache_lock = threading.RLock()
        self.stats_lock = threading.Lock()
        
        # Estadísticas de performance
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'invalidations': 0,
            'total_queries': 0,
            'avg_query_time_ms': 0,
            'cache_size_mb': 0,
            'hit_rate': 0.0
        }
        
        # Configuración de TTL por tipo de consulta
        self.ttl_config = {
            'sgi_procesos': 300,          # 5 minutos
            'sgi_filtros': 600,           # 10 minutos  
            'sgi_indicadores': 900,       # 15 minutos
            'sgi_estadisticas': 180,      # 3 minutos
            'sgi_usuarios': 1800,         # 30 minutos
            'default': 300                # 5 minutos por defecto
        }
        
        # Patrones de invalidación
        self.invalidation_patterns = {
            'sgi_procesos': ['procesos', 'estadisticas', 'dashboard'],
            'sgi_indicadores': ['indicadores', 'metricas', 'reportes'],
            'sgi_usuarios': ['usuarios', 'permisos', 'sesiones']
        }
        
        self.log("🚀 Ultra Cache SGI inicializado", {
            'max_size_mb': max_size_mb,
            'ttl_config': self.ttl_config
        })

    def get(self, key: str, default: Any = None) -> Any:
        """Obtiene un valor del cache"""
        with self.cache_lock:
            entry = self.cache.get(key)
            
            if entry is None:
                self._record_miss()
                return default
            
            # Verificar expiración
            if entry.is_expired():
                self.log(f"🕐 Cache expirado para key: {key}")
                del self.cache[key]
                self.current_size -= entry.size_bytes
                self._record_miss()
                return default
            
            # Actualizar estadísticas de acceso
            entry.last_accessed = datetime.now()
            entry.access_count += 1
            
            # Mover al final (LRU)
            self.cache.move_to_end(key)
            
            self._record_hit()
            self.log(f"✅ Cache hit para key: {key}")
            return entry.data

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, tags: List[str] = None) -> None:
        """Almacena un valor en el cache"""
        if tags is None:
            tags = []
            
        # Determinar TTL basado en el tipo de consulta
        if ttl_seconds is None:
            ttl_seconds = self._get_ttl_for_key(key)
        
        # Calcular tamaño aproximado
        size_bytes = self._calculate_size(value)
        
        with self.cache_lock:
            # Eliminar entrada existente si existe
            if key in self.cache:
                old_entry = self.cache[key]
                self.current_size -= old_entry.size_bytes
                del self.cache[key]
            
            # Verificar espacio disponible
            self._ensure_space(size_bytes)
            
            # Crear nueva entrada
            entry = CacheEntry(
                data=value,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                access_count=1,
                ttl_seconds=ttl_seconds,
                tags=tags,
                size_bytes=size_bytes
            )
            
            self.cache[key] = entry
            self.current_size += size_bytes
            
            self.log(f"💾 Valor almacenado en cache: {key} ({size_bytes} bytes)")

    def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalida entradas del cache por tags"""
        with self.cache_lock:
            keys_to_remove = []
            
            for key, entry in self.cache.items():
                if any(tag in entry.tags for tag in tags):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                entry = self.cache[key]
                self.current_size -= entry.size_bytes
                del self.cache[key]
            
            count = len(keys_to_remove)
            with self.stats_lock:
                self.stats['invalidations'] += count
            
            self.log(f"🗑️ Invalidadas {count} entradas por tags: {tags}")
            return count

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalida entradas que coincidan con un patrón"""
        with self.cache_lock:
            keys_to_remove = [key for key in self.cache.keys() if pattern in key]
            
            for key in keys_to_remove:
                entry = self.cache[key]
                self.current_size -= entry.size_bytes
                del self.cache[key]
            
            count = len(keys_to_remove)
            with self.stats_lock:
                self.stats['invalidations'] += count
            
            self.log(f"🗑️ Invalidadas {count} entradas por patrón: {pattern}")
            return count

    def clear_expired(self) -> int:
        """Limpia entradas expiradas del cache"""
        with self.cache_lock:
            keys_to_remove = []
            
            for key, entry in self.cache.items():
                if entry.is_expired():
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                entry = self.cache[key]
                self.current_size -= entry.size_bytes
                del self.cache[key]
            
            count = len(keys_to_remove)
            self.log(f"🧹 Limpiadas {count} entradas expiradas")
            return count

    def get_or_set(self, key: str, factory: Callable[[], Any], ttl_seconds: Optional[int] = None, tags: List[str] = None) -> Any:
        """Obtiene del cache o ejecuta factory function si no existe"""
        value = self.get(key)
        
        if value is not None:
            return value
        
        # Ejecutar factory function
        start_time = datetime.now()
        value = factory()
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Almacenar en cache
        self.set(key, value, ttl_seconds, tags)
        
        # Actualizar estadísticas
        with self.stats_lock:
            self.stats['total_queries'] += 1
            if self.stats['total_queries'] == 1:
                self.stats['avg_query_time_ms'] = execution_time
            else:
                self.stats['avg_query_time_ms'] = (
                    (self.stats['avg_query_time_ms'] * (self.stats['total_queries'] - 1)) + execution_time
                ) / self.stats['total_queries']
        
        self.log(f"🏭 Factory ejecutada para {key}: {execution_time:.2f}ms")
        return value

    def cache_sgi_query(self, query_type: str, params: Dict[str, Any], query_func: Callable[[], Any]) -> Any:
        """Cache específico para consultas SGI"""
        # Generar key único basado en consulta y parámetros
        cache_key = self._generate_cache_key(query_type, params)
        
        # Tags para invalidación inteligente
        tags = self._get_tags_for_query_type(query_type)
        
        # TTL específico para el tipo de consulta
        ttl = self.ttl_config.get(query_type, self.ttl_config['default'])
        
        return self.get_or_set(cache_key, query_func, ttl, tags)

    def invalidate_sgi_data(self, data_type: str) -> int:
        """Invalida datos SGI específicos cuando hay cambios"""
        if data_type in self.invalidation_patterns:
            tags_to_invalidate = self.invalidation_patterns[data_type]
            return self.invalidate_by_tags(tags_to_invalidate)
        
        # Invalidación por patrón si no hay tags específicos
        return self.invalidate_pattern(data_type)

    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del cache"""
        with self.stats_lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
            
            stats = {
                **self.stats,
                'hit_rate': hit_rate,
                'cache_size_mb': self.current_size / (1024 * 1024),
                'entry_count': len(self.cache),
                'memory_usage_percent': (self.current_size / self.max_size_bytes) * 100
            }
            
            return stats

    def optimize(self) -> Dict[str, int]:
        """Optimiza el cache eliminando entradas obsoletas"""
        with self.cache_lock:
            expired_count = self.clear_expired()
            
            # Eliminar entradas obsoletas (no accedidas recientemente)
            stale_keys = []
            for key, entry in self.cache.items():
                if entry.is_stale(max_age_seconds=1800):  # 30 minutos
                    stale_keys.append(key)
            
            for key in stale_keys:
                entry = self.cache[key]
                self.current_size -= entry.size_bytes
                del self.cache[key]
            
            stale_count = len(stale_keys)
            
            self.log(f"🔧 Optimización completada: {expired_count} expiradas, {stale_count} obsoletas")
            
            return {
                'expired_removed': expired_count,
                'stale_removed': stale_count,
                'total_removed': expired_count + stale_count
            }

    def _ensure_space(self, required_bytes: int) -> None:
        """Asegura espacio suficiente en el cache"""
        while (self.current_size + required_bytes) > self.max_size_bytes and self.cache:
            # Eliminar el elemento menos recientemente usado (LRU)
            oldest_key, oldest_entry = self.cache.popitem(last=False)
            self.current_size -= oldest_entry.size_bytes
            
            with self.stats_lock:
                self.stats['evictions'] += 1
            
            self.log(f"🗑️ Evicted LRU entry: {oldest_key}")

    def _generate_cache_key(self, query_type: str, params: Dict[str, Any]) -> str:
        """Genera una clave única para la consulta"""
        # Ordenar parámetros para consistencia
        sorted_params = json.dumps(params, sort_keys=True, default=str)
        key_string = f"{query_type}:{sorted_params}"
        
        # Hash para evitar claves muy largas
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_ttl_for_key(self, key: str) -> int:
        """Determina TTL basado en el tipo de clave"""
        for query_type, ttl in self.ttl_config.items():
            if query_type in key:
                return ttl
        return self.ttl_config['default']

    def _get_tags_for_query_type(self, query_type: str) -> List[str]:
        """Obtiene tags para un tipo de consulta"""
        base_tags = [query_type]
        
        if 'sgi_procesos' in query_type:
            base_tags.extend(['procesos', 'sgi'])
        elif 'sgi_indicadores' in query_type:
            base_tags.extend(['indicadores', 'metricas', 'sgi'])
        elif 'sgi_usuarios' in query_type:
            base_tags.extend(['usuarios', 'auth', 'sgi'])
        
        return base_tags

    def _calculate_size(self, obj: Any) -> int:
        """Calcula tamaño aproximado del objeto"""
        try:
            if isinstance(obj, (str, bytes)):
                return len(obj)
            elif isinstance(obj, (list, tuple)):
                return sum(self._calculate_size(item) for item in obj)
            elif isinstance(obj, dict):
                return sum(self._calculate_size(k) + self._calculate_size(v) for k, v in obj.items())
            else:
                # Estimación aproximada para otros tipos
                return len(str(obj)) * 2
        except:
            return 1024  # Tamaño por defecto si hay error

    def _record_hit(self) -> None:
        """Registra un cache hit"""
        with self.stats_lock:
            self.stats['hits'] += 1

    def _record_miss(self) -> None:
        """Registra un cache miss"""
        with self.stats_lock:
            self.stats['misses'] += 1

    def log(self, message: str, data: Any = None) -> None:
        """Log de debug"""
        if self.debug:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{timestamp}] [UltraCacheSGI] {message}")
            if data:
                print(f"  Data: {data}")

# Instancia global del cache
_ultra_cache_instance = None
_cache_lock = threading.Lock()

def get_ultra_cache() -> UltraCacheSGI:
    """Obtiene la instancia singleton del cache"""
    global _ultra_cache_instance
    
    if _ultra_cache_instance is None:
        with _cache_lock:
            if _ultra_cache_instance is None:
                _ultra_cache_instance = UltraCacheSGI(debug=True)
    
    return _ultra_cache_instance

# Decorador para cache automático
def cache_sgi_method(query_type: str, ttl_seconds: Optional[int] = None):
    """Decorador para cache automático de métodos SGI"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            cache = get_ultra_cache()
            
            # Generar key basado en función y argumentos
            func_name = f"{func.__module__}.{func.__name__}"
            params = {'args': args[1:], 'kwargs': kwargs}  # Excluir 'self'
            
            def query_func():
                return func(*args, **kwargs)
            
            return cache.cache_sgi_query(
                f"{query_type}_{func_name}",
                params,
                query_func
            )
        
        return wrapper
    return decorator

# Funciones helper para integración fácil
def cache_get(key: str, default: Any = None) -> Any:
    """Helper para obtener del cache"""
    return get_ultra_cache().get(key, default)

def cache_set(key: str, value: Any, ttl_seconds: Optional[int] = None, tags: List[str] = None) -> None:
    """Helper para almacenar en cache"""
    get_ultra_cache().set(key, value, ttl_seconds, tags)

def cache_invalidate_sgi(data_type: str) -> int:
    """Helper para invalidar datos SGI"""
    return get_ultra_cache().invalidate_sgi_data(data_type)

def cache_stats() -> Dict[str, Any]:
    """Helper para obtener estadísticas"""
    return get_ultra_cache().get_stats()

def cache_optimize() -> Dict[str, int]:
    """Helper para optimizar cache"""
    return get_ultra_cache().optimize()
