import os, time, threading, logging, psycopg2
from psycopg2 import pool, extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuración — valores leídos desde .env o # variables de entorno del App Service de Azure.
# Si no están definidas, se usan los defaults.
# ─────────────────────────────────────────────
load_dotenv()
DATABASE_PATH           = os.getenv("DATABASE_PATH")            # Puerto 6432 — PgBouncer (producción Azure)
DATABASE_PATH_DEDICATED = os.getenv("DATABASE_PATH_DEDICATED")  # Puerto 5432 — directo (LISTEN/NOTIFY)
DB_POOL_MIN             = int(os.getenv("DB_POOL_MIN", "2"))    # Conexiones mínimas por worker
DB_POOL_MAX             = int(os.getenv("DB_POOL_MAX", "8"))    # Conexiones máximas por worker

logger = logging.getLogger("database_manager") # Logger específico para este módulo

# ─────────────────────────────────────────────
# Pool Singleton — UNA sola instancia por proceso # Cada worker de gunicorn tiene su propio proceso y pool independiente.
# ─────────────────────────────────────────────
_POOL_LOCK = threading.Lock()
_DB_POOL: pool.ThreadedConnectionPool = None

def _get_pool() -> pool.ThreadedConnectionPool:
    """
    Retorna el pool de conexiones singleton.
    Se inicializa una sola vez por proceso (worker de gunicorn).
    Thread-safe gracias al doble check con Lock.

    options explicadas:
    ─────────────────────────────────────────────────────────
    timezone=America/Bogota  → todas las queries devuelven timestamps en hora Colombia
    statement_timeout=30000  → corta cualquier query que tarde más de 30 segundos.
                                Evita que una query colgada bloquee una conexión del pool
                                indefinidamente y agote los slots disponibles.
    ─────────────────────────────────────────────────────────
    keepalives → mantiene vivas las conexiones idle a través del firewall de Azure,
                que cierra conexiones TCP inactivas después de ~4 minutos.
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
                    options="-c timezone=America/Bogota -c statement_timeout=30000",
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
# Validación interna — detecta conexiones muertas
# ─────────────────────────────────────────────
def _conexion_viva(conn) -> bool:
    """
    Verifica que la conexión sigue activa antes de usarla.

    ¿Por qué es necesario?
    Azure PostgreSQL cierra conexiones idle después de un tiempo
    (firewall, mantenimiento, reinicios del servidor).
    Sin esta validación, el pool puede entregar una conexión
    "muerta" y el request falla con:
        'SSL connection has been closed unexpectedly'

    Usa una query mínima (SELECT 1) que no toca datos ni genera
    carga en la base de datos.
    """
    try:
        if conn.closed:
            return False
        with conn.cursor() as c:
            c.execute("SELECT 1")
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────
# Context Manager principal — usar en todo el proyecto
# ─────────────────────────────────────────────
@contextmanager
def get_db_connection():
    """
    Entrega una conexión del pool y la devuelve automáticamente
    al terminar, incluso si ocurre una excepción.

    Flujo interno:
    ──────────────────────────────────────────────────────────
    1. getconn()        → toma una conexión del pool
    2. rollback()       → limpia transacciones residuales
    3. _conexion_viva() → si está muerta, pide una nueva
    4. yield conn       → entrega la conexión al llamador
    5. finally          → rollback + reset cursor_factory + putconn()

    IMPORTANTE — Por qué NO se toca conn.autocommit aquí:
    ──────────────────────────────────────────────────────────
    psycopg2 maneja autocommit=False por defecto.
    Cambiar autocommit con una transacción abierta genera:
        'set_session cannot be used inside a transaction'
    Las conexiones del pool pueden venir con transacciones
    pendientes → se hace rollback() PRIMERO para limpiarlas.
    """
    conn = None
    try:
        conn = _get_pool().getconn()

        # Limpiar transacción residual antes de usar la conexión.
        # Resuelve "idle in transaction" y el error de autocommit.
        if not conn.closed:
            conn.rollback()

        # ── Reconexión automática ──────────────────────────────
        # Si Azure cerró la conexión mientras estaba idle en el pool,
        # se detecta aquí y se reemplaza por una nueva en lugar de
        # fallar con SSL error en el request del usuario.
        if not _conexion_viva(conn):
            logger.warning("⚠️ Conexión muerta detectada — solicitando nueva al pool")
            try:
                _get_pool().putconn(conn)
            except Exception:
                pass
            conn = _get_pool().getconn()
            if not conn.closed:
                conn.rollback()

        yield conn

    except psycopg2.pool.PoolError as e:
        # Pool agotado — todos los slots están en uso.
        # Aumentar DB_POOL_MAX o reducir requests concurrentes.
        logger.error(f"❌ Pool de conexiones agotado: {e}")
        raise

    except psycopg2.OperationalError as e:
        # Error de red, servidor caído, timeout de conexión.
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
                # Rollback final defensivo: limpia cualquier transacción
                # pendiente antes de devolver al pool.
                # La próxima solicitud recibe la conexión en estado limpio.
                if not conn.closed:
                    conn.rollback()
                    # Reset cursor_factory al default (tuplas estándar).
                    # Evita que conexiones usadas con RealDictCursor contaminen
                    # el pool y rompan código que espera row[0] en vez de row["col"].
                    conn.cursor_factory = pg_extensions.cursor
                _get_pool().putconn(conn)
            except Exception as e:
                logger.warning(f"⚠️ Error al devolver conexión al pool: {e}")

# ─────────────────────────────────────────────
# Conexión dedicada — SOLO para LISTEN/NOTIFY
# ─────────────────────────────────────────────
def get_dedicated_connection() -> psycopg2.extensions.connection:
    """
    Crea una conexión DIRECTA fuera del pool — bypassa PgBouncer.
    ¿Por qué fuera del pool y por el puerto 5432?
    ──────────────────────────────────────────────────────────
    PgBouncer en transaction mode NO soporta LISTEN/NOTIFY.
    LISTEN requiere una conexión persistente que mantenga el canal
    abierto entre operaciones — incompatible con el multiplexeo
    de PgBouncer que devuelve la conexión al pool tras cada query.

    Usa DATABASE_PATH_DEDICATED (puerto 5432 directo) definido en .env.
    Si no está configurado, cae en DATABASE_PATH como fallback seguro
    (útil en entornos locales donde PgBouncer no aplica).

    IMPORTANTE: El llamador es responsable de cerrar con conn.close().
    """
    dsn = DATABASE_PATH_DEDICATED or DATABASE_PATH
    if not dsn:
        raise EnvironmentError(
            "❌ Variable de entorno DATABASE_PATH_DEDICATED (o DATABASE_PATH) no configurada."
        )
    conn = psycopg2.connect(
        dsn=dsn,
        options="-c timezone=America/Bogota",
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    logger.info("🔌 Conexión dedicada creada (LISTEN/NOTIFY) — puerto 5432 directo")
    return conn

# ─────────────────────────────────────────────
# Utilidad: estado del pool + métricas de DB
# Usado por GET /health en main.py
# ─────────────────────────────────────────────
def get_pool_status() -> dict:
    """
    Retorna el estado del pool local y las métricas reales
    de conexiones en PostgreSQL via pg_stat_activity.

    Campos retornados:
    ──────────────────────────────────────────────────────────
    status          → "ok" o "error"
    min_connections → DB_POOL_MIN configurado
    max_connections → DB_POOL_MAX configurado
    pool_activo     → True si el pool está abierto
    db_ping         → "ok" si la DB responde, o el error
    db_active       → conexiones ejecutando SQL ahora mismo
    db_idle         → conexiones en pool esperando trabajo (normal)
    db_idle_tx      → conexiones con transacción abierta sin cerrar (PROBLEMA si > 0)
    db_total        → total conexiones abiertas en PostgreSQL
    db_tiempo_ms    → latencia del ping a la DB en milisegundos
    """
    resultado = {
        "status":          "ok",
        "min_connections": DB_POOL_MIN,
        "max_connections": DB_POOL_MAX,
        "pool_activo":     False,
        "db_ping":         "sin verificar",
        "db_active":       None,
        "db_idle":         None,
        "db_idle_tx":      None,
        "db_total":        None,
        "db_tiempo_ms":    None,
    }

    # ── Estado del pool local ──────────────────────────────
    try:
        p = _get_pool()
        resultado["pool_activo"] = (p.closed == 0)
    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"pool no disponible: {e}"
        return resultado

    # ── Ping + métricas reales de PostgreSQL ──────────────
    try:
        t_inicio = time.monotonic()
        with get_db_connection() as conn:
            with conn.cursor() as c:
                # Latencia del ping
                c.execute("SELECT 1")
                resultado["db_tiempo_ms"] = round((time.monotonic() - t_inicio) * 1000, 1)

                # Métricas de pg_stat_activity — estado de todas las conexiones
                c.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'active')              AS active,
                        COUNT(*) FILTER (WHERE state = 'idle')                AS idle,
                        COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_tx,
                        COUNT(*)                                               AS total
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                """)
                row = c.fetchone()
                resultado["db_active"]  = row[0]
                resultado["db_idle"]    = row[1]
                resultado["db_idle_tx"] = row[2]
                resultado["db_total"]   = row[3]
                resultado["db_ping"]    = "ok"

                # Alerta automática si hay conexiones idle in transaction
                if row[2] and row[2] > 0:
                    logger.warning(
                        f"⚠️ {row[2]} conexiones 'idle in transaction' detectadas"
                    )

    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"error: {e}"

    return resultado

# ─────────────────────────────────────────────
# Utilidad: cerrar el pool (shutdown graceful)
# Llamado por el lifespan de FastAPI en main.py
# ─────────────────────────────────────────────
def close_pool():
    """
    Cierra todas las conexiones del pool limpiamente.
    Se ejecuta cuando gunicorn hace shutdown del worker,
    asegurando que PostgreSQL no quede con conexiones huérfanas.
    """
    global _DB_POOL
    if _DB_POOL and not _DB_POOL.closed:
        _DB_POOL.closeall()
        _DB_POOL = None
        logger.info("🔴 Pool DB cerrado correctamente.")
        print("🔴 [database_manager] Pool DB cerrado.")