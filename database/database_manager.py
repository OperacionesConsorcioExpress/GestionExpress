import os
import threading
import logging
import psycopg2
from psycopg2 import pool, extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
DB_POOL_MIN   = int(os.getenv("DB_POOL_MIN", "2"))
DB_POOL_MAX   = int(os.getenv("DB_POOL_MAX", "8"))
logger = logging.getLogger("database_manager")

# ─────────────────────────────────────────────
# Pool Singleton — UNA sola instancia por proceso
# ─────────────────────────────────────────────
_POOL_LOCK = threading.Lock()
_DB_POOL: pool.ThreadedConnectionPool = None

def _get_pool() -> pool.ThreadedConnectionPool:
    """
    Retorna el pool de conexiones singleton.
    Se inicializa una sola vez por proceso (worker de gunicorn).
    Thread-safe gracias al doble check con Lock.
    """
    global _DB_POOL

    if _DB_POOL is None:
        with _POOL_LOCK:
            if _DB_POOL is None:
                if not DATABASE_PATH:
                    raise EnvironmentError(
                        "❌ Variable de entorno DATABASE_PATH no está configurada."
                    )
                _DB_POOL = pool.ThreadedConnectionPool(
                    DB_POOL_MIN,
                    DB_POOL_MAX,
                    dsn=DATABASE_PATH,
                    options="-c timezone=America/Bogota",
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                logger.info(
                    f"✅ Pool DB centralizado inicializado — "
                    f"min={DB_POOL_MIN}, max={DB_POOL_MAX}"
                )
                print(
                    f"✅ [database_manager] Pool DB inicializado — "
                    f"min={DB_POOL_MIN}, max={DB_POOL_MAX}"
                )
    return _DB_POOL

# ─────────────────────────────────────────────
# Context Manager principal — usar en todo el proyecto
# ─────────────────────────────────────────────
@contextmanager
def get_db_connection():
    """
    Entrega una conexión del pool y la devuelve automáticamente
    al terminar, incluso si ocurre una excepción.

    IMPORTANTE — Por qué NO se toca conn.autocommit aquí:
    -------------------------------------------------------
    psycopg2 maneja autocommit=False por defecto en todas las conexiones.
    Intentar cambiar autocommit mientras hay una transacción abierta
    genera: "set_session cannot be used inside a transaction".
    Las conexiones del pool pueden venir con transacciones pendientes,
    por eso se hace rollback() PRIMERO para limpiarlas, sin tocar autocommit.
    """
    conn = None
    try:
        conn = _get_pool().getconn()

        # Limpiar cualquier transacción residual antes de usar la conexión.
        # Esto resuelve el problema "idle in transaction" y el error
        # "set_session cannot be used inside a transaction".
        if not conn.closed:
            conn.rollback()

        yield conn

    except psycopg2.pool.PoolError as e:
        # Pool agotado — no hay conn que devolver
        logger.error(f"❌ Pool de conexiones agotado: {e}")
        raise

    except psycopg2.OperationalError as e:
        logger.error(f"❌ Error operacional de base de datos: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    except Exception as e:
        logger.error(f"❌ Error inesperado en conexión DB: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        if conn:
            try:
                # Rollback final defensivo: limpia transacciones pendientes
                # antes de devolver al pool para que la próxima solicitud
                # reciba una conexión en estado limpio.
                if not conn.closed:
                    conn.rollback()
                    # Reset cursor_factory al default (tuplas estándar).
                    # Evita que conexiones usadas con RealDictCursor contaminen
                    # el pool y rompan código que espera row[0] en lugar de row["col"].
                    conn.cursor_factory = pg_extensions.cursor
                _get_pool().putconn(conn)
            except Exception as e:
                logger.warning(f"⚠️ Error al devolver conexión al pool: {e}")

# ─────────────────────────────────────────────
# Conexión dedicada — SOLO para LISTEN/NOTIFY
# ─────────────────────────────────────────────
def get_dedicated_connection() -> psycopg2.extensions.connection:
    """
    Crea una conexión DIRECTA fuera del pool.
    USAR ÚNICAMENTE para conexiones persistentes que no pueden
    devolverse al pool entre operaciones:
    - LISTEN/NOTIFY de PostgreSQL (sgi_enterprise_data_manager.py)
    El llamador es responsable de cerrar con conn.close().
    """
    if not DATABASE_PATH:
        raise EnvironmentError(
            "❌ Variable de entorno DATABASE_PATH no está configurada."
        )
    conn = psycopg2.connect(
        dsn=DATABASE_PATH,
        options="-c timezone=America/Bogota",
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    logger.info("🔌 Conexión dedicada creada (LISTEN/NOTIFY)")
    return conn

# ─────────────────────────────────────────────
# Utilidad: verificar estado del pool
# ─────────────────────────────────────────────
def get_pool_status() -> dict:
    """
    Retorna información del estado actual del pool.
    Usado por el endpoint GET /health en main.py.
    """
    try:
        p = _get_pool()
        return {
            "status": "ok",
            "min_connections": DB_POOL_MIN,
            "max_connections": DB_POOL_MAX,
            "pool_activo": p.closed == 0,
        }
    except Exception as e:
        return {
            "status": "error",
            "detalle": str(e),
        }

# ─────────────────────────────────────────────
# Utilidad: cerrar el pool (shutdown graceful)
# ─────────────────────────────────────────────
def close_pool():
    """
    Cierra todas las conexiones del pool.
    Llamado automáticamente por el lifespan de FastAPI en main.py.
    """
    global _DB_POOL
    if _DB_POOL and not _DB_POOL.closed:
        _DB_POOL.closeall()
        _DB_POOL = None
        logger.info("🔴 Pool DB cerrado correctamente.")
        print("🔴 [database_manager] Pool DB cerrado.")