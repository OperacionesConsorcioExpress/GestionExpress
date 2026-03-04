import os, time, queue, threading, logging, psycopg2
from psycopg2 import pool, extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv
from fastapi import HTTPException

# ─────────────────────────────────────────────
# Configuración — valores leídos desde .env o variables de entorno del App Service de Azure.
# ─────────────────────────────────────────────
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")            # Puerto 6432 — PgBouncer (producción Azure)
DATABASE_PATH_DEDICATED = os.getenv("DATABASE_PATH_DEDICATED")  # Puerto 5432 — directo (LISTEN/NOTIFY)
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))    # Conexiones mínimas por worker
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "8"))    # Conexiones máximas por worker

# ─────────────────────────────────────────────
# CAPA 1 — Colchón de conexiones
# ─────────────────────────────────────────────
# Reserva 1 slot del pool para operaciones críticas (health check, admin).
# La app normal solo puede usar hasta DB_POOL_MAX - DB_POOL_RESERVADO slots.

DB_POOL_RESERVADO = int(os.getenv("DB_POOL_RESERVADO", "1")) # Slots reservados para operaciones críticas
DB_POOL_DISPONIBLE = DB_POOL_MAX - DB_POOL_RESERVADO # Slots disponibles para requests normales

# ─────────────────────────────────────────────
# CAPA 2 — Cola de espera con timeout
# ─────────────────────────────────────────────
# Cuando el pool de slots normales está lleno, el request espera en cola por DB_POOL_TIMEOUT_SEG segundos antes de responder al usuario. Evita el error inmediato "pool exhausted" en picos de tráfico momentáneos.

DB_POOL_TIMEOUT_SEG = float(os.getenv("DB_POOL_TIMEOUT_SEG", "3.0"))  # Segundos máx de espera en cola
# Semaforo para controlar el acceso a los slots disponibles del pool.
_pool_semaforo = threading.Semaphore(DB_POOL_DISPONIBLE)

# ─────────────────────────────────────────────
# CAPA 3 — Circuit Breaker - Evita caidas en cascada
# ─────────────────────────────────────────────
# Si la DB falla repetidamente, el circuit breaker se "abre" y bloquea nuevas conexiones por CB_RECOVERY_SEG segundos para dar tiempo de recuperación.Esto evita que todos los requests sigan golpeando DB 

# Estados:
#   CLOSED   → funcionando normal, todas las conexiones pasan
#   OPEN     → DB con problemas, rechaza conexiones inmediatamente
#   HALF     → período de prueba, deja pasar 1 conexión para verificar recuperación

CB_FALLAS_MAX    = int(os.getenv("CB_FALLAS_MAX",    "5"))    # Fallos consecutivos para abrir el circuit breaker
CB_RECOVERY_SEG  = float(os.getenv("CB_RECOVERY_SEG", "30.0")) # Segundos que espera antes de intentar recuperación

_cb_lock          = threading.Lock()
_cb_fallas        = 0         # Contador de fallos consecutivos
_cb_ultimo_fallo  = 0.0       # Timestamp del último fallo
_cb_estado        = "CLOSED"  # Estado actual: CLOSED | OPEN | HALF

logger = logging.getLogger("database_manager")

# ─────────────────────────────────────────────
# Pool Singleton — UNA sola instancia por proceso cada worker de gunicorn tiene su propio proceso y pool.
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
                # Fallback: si DATABASE_PATH (puerto 6432 PgBouncer) no está definida,
                # usa DATABASE_PATH_DEDICATED (puerto 5432 directo).
                # Permite correr local sin PgBouncer usando solo DATABASE_PATH_DEDICATED.
                _dsn_pool = DATABASE_PATH or DATABASE_PATH_DEDICATED
                if not _dsn_pool:
                    raise EnvironmentError(
                        "❌ Ninguna variable DATABASE_PATH ni DATABASE_PATH_DEDICATED está configurada."
                    )
                _DB_POOL = pool.ThreadedConnectionPool(
                    DB_POOL_MIN,
                    DB_POOL_MAX,
                    dsn=_dsn_pool,
                    options="-c timezone=America/Bogota -c statement_timeout=30000",
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                _modo = "PgBouncer :6432" if DATABASE_PATH else "Directo :5432 (fallback)"
                logger.info(
                    f"✅ Pool DB inicializado — min={DB_POOL_MIN}, max={DB_POOL_MAX}, "
                    f"disponibles={DB_POOL_DISPONIBLE}, reservados={DB_POOL_RESERVADO} — modo={_modo}"
                )
                print(
                    f"✅ [database_manager] Pool DB inicializado — min={DB_POOL_MIN}, max={DB_POOL_MAX}, "
                    f"disponibles={DB_POOL_DISPONIBLE}, reservados={DB_POOL_RESERVADO} — modo={_modo}"
                )
    return _DB_POOL

# ─────────────────────────────────────────────
# CAPA 3 — Lógica interna del Circuit Breaker
# ─────────────────────────────────────────────
def _cb_registrar_exito():
    """Resetea el contador de fallos cuando una conexión es exitosa."""
    global _cb_fallas, _cb_estado
    with _cb_lock:
        if _cb_fallas > 0 or _cb_estado != "CLOSED":
            logger.info(f"✅ Circuit Breaker: conexión exitosa — estado CLOSED")
        _cb_fallas = 0
        _cb_estado = "CLOSED"

def _cb_registrar_fallo():
    """Incrementa el contador de fallos y abre el circuit breaker si supera el límite."""
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado
    with _cb_lock:
        _cb_fallas       += 1
        _cb_ultimo_fallo  = time.monotonic()
        if _cb_fallas >= CB_FALLAS_MAX:
            _cb_estado = "OPEN"
            logger.error(
                f"🔴 Circuit Breaker ABIERTO — {_cb_fallas} fallos consecutivos. "
                f"Bloqueando conexiones por {CB_RECOVERY_SEG}s"
            )

def _cb_verificar() -> str:
    """
    Verifica el estado actual del circuit breaker.
    Retorna: 'CLOSED' | 'OPEN' | 'HALF'
    """
    global _cb_estado
    with _cb_lock:
        if _cb_estado == "OPEN":
            tiempo_desde_fallo = time.monotonic() - _cb_ultimo_fallo
            if tiempo_desde_fallo >= CB_RECOVERY_SEG:
                # Pasó el tiempo de recuperación — permitir 1 conexión de prueba
                _cb_estado = "HALF"
                logger.info("🟡 Circuit Breaker SEMI-ABIERTO — probando recuperación de DB")
        return _cb_estado

# ─────────────────────────────────────────────
# Validación interna — detecta conexiones muertas
# ─────────────────────────────────────────────
def _conexion_viva(conn) -> bool:
    """
    Verifica que la conexión sigue activa antes de usarla.

    Azure PostgreSQL cierra conexiones idle después de un tiempo (firewall, mantenimiento, reinicios del servidor).Sin esta validación, el pool puede entregar una conexión muerta y el request falla con:'SSL connection has been closed unexpectedly'
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
    Entrega una conexión del pool con las 3 capas de resiliencia activas.

    Flujo completo:
    ──────────────────────────────────────────────────────────────────
    CAPA 3 — Circuit Breaker:
        Si la DB tuvo 5+ fallos consecutivos, rechaza inmediatamentecon HTTP 503 en lugar de intentar conectar.

    CAPA 2 — Cola con timeout:
        Si los DB_POOL_DISPONIBLE slots están ocupados, espera hasta DB_POOL_TIMEOUT_SEG segundos. Si en ese tiempo se libera un slot,continúa. Si no, responde HTTP 503 con mensaje claro al usuario.

    CAPA 1 — Colchón:
        El semáforo garantiza que siempre quede DB_POOL_RESERVADO slots libres para health check y operaciones críticas de admin.

    ──────────────────────────────────────────────────────────────────
    IMPORTANTE — Por qué NO se toca conn.autocommit:
        psycopg2 maneja autocommit=False por defecto.Cambiar autocommit con una transacción abierta genera:
            'set_session cannot be used inside a transaction'        Se hace rollback() PRIMERO para limpia transacciones pendientes.
    """
    # ── CAPA 3: Verificar Circuit Breaker ─────────────────────────
    estado_cb = _cb_verificar()
    if estado_cb == "OPEN":
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Base de datos temporalmente no disponible",
                "mensaje": "El sistema detectó problemas con la base de datos. Intente en unos segundos.",
                "codigo":  "CIRCUIT_BREAKER_OPEN"
            }
        )

    # ── CAPA 2: Cola de espera con timeout ────────────────────────
    # Intenta adquirir un slot del semáforo (conexión disponible).
    # Si no hay slots libres, espera hasta DB_POOL_TIMEOUT_SEG segundos.
    slot_adquirido = _pool_semaforo.acquire(timeout=DB_POOL_TIMEOUT_SEG)
    if not slot_adquirido:
        logger.warning(
            f"⚠️ Pool ocupado — request esperó {DB_POOL_TIMEOUT_SEG}s sin obtener conexión"
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Servidor ocupado",
                "mensaje": f"El sistema está procesando muchas solicitudes. Intente nuevamente en unos segundos.",
                "codigo":  "POOL_TIMEOUT"
            }
        )

    conn = None
    try:
        conn = _get_pool().getconn()

        # Limpiar transacción residual antes de usar la conexión.
        if not conn.closed:
            conn.rollback()

        # ── Reconexión automática ──────────────────────────────────
        # Si Azure cerró la conexión mientras estaba idle en el pool,
        # se detecta y reemplaza automáticamente.
        if not _conexion_viva(conn):
            logger.warning("⚠️ Conexión muerta detectada — solicitando nueva al pool")
            try:
                _get_pool().putconn(conn)
            except Exception:
                pass
            conn = _get_pool().getconn()
            if not conn.closed:
                conn.rollback()

        # Conexión exitosa — resetear circuit breaker
        _cb_registrar_exito()

        yield conn

    except HTTPException:
        # Re-lanzar HTTPExceptions sin modificar (son respuestas controladas)
        raise

    except psycopg2.pool.PoolError as e:
        _cb_registrar_fallo()
        logger.error(f"❌ Pool de conexiones agotado: {e}")
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Sin conexiones disponibles",
                "mensaje": "El sistema está bajo alta carga. Intente en unos segundos.",
                "codigo":  "POOL_EXHAUSTED"
            }
        )

    except psycopg2.OperationalError as e:
        _cb_registrar_fallo()
        logger.error(f"❌ Error operacional de base de datos: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Error de conexión a la base de datos",
                "mensaje": "No se pudo establecer conexión. Intente nuevamente.",
                "codigo":  "DB_OPERATIONAL_ERROR"
            }
        )

    except Exception as e:
        _cb_registrar_fallo()
        logger.error(f"❌ Error inesperado en conexión DB: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        # ── CAPA 1: Liberar slot del semáforo ─────────────────────
        # SIEMPRE liberar el slot, incluso si hubo error.
        # Sin esto, el semáforo se agota progresivamente y la app se congela.
        if slot_adquirido:
            _pool_semaforo.release()

        # Devolver conexión al pool limpia
        if conn:
            try:
                if not conn.closed:
                    conn.rollback()
                    # Reset cursor_factory al default (tuplas estándar).
                    # Evita contaminación de RealDictCursor entre módulos.
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

    PgBouncer en transaction mode NO soporta LISTEN/NOTIFY.
    LISTEN requiere una conexión persistente que mantenga el canal abierto entre operaciones — incompatible con el multiplexeo de PgBouncer.

    Usa DATABASE_PATH_DEDICATED (puerto 5432 directo).Si no está configurado, cae en DATABASE_PATH como fallback
    (útil en entornos locales donde PgBouncer no aplica).

    IMPORTANTE: El llamador es responsable de cerrar con conn.close().
    """
    dsn = DATABASE_PATH_DEDICATED or DATABASE_PATH
    if not dsn:
        raise EnvironmentError(
            "❌ Variable DATABASE_PATH_DEDICATED (o DATABASE_PATH) no configurada."
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
# Utilidad: estado del pool + métricas de DB Usado por GET /health en main.py
# ─────────────────────────────────────────────
def get_pool_status() -> dict:
    """
    Retorna el estado completo del sistema de conexiones:
    ──────────────────────────────────────────────────────────
    pool_activo     → True si el pool está inicializado
    min_connections → DB_POOL_MIN configurado
    max_connections → DB_POOL_MAX configurado
    pool_disponible → slots para requests normales (max - reservado)
    pool_reservado  → slots reservados para operaciones críticas
    circuit_breaker → estado: CLOSED (ok) | OPEN (bloqueado) | HALF (recuperando)
    cb_fallas       → fallos consecutivos actuales
    db_ping         → "ok" o descripción del error
    db_active       → conexiones ejecutando SQL ahora mismo
    db_idle         → conexiones en pool esperando (normal)
    db_idle_tx      → transacciones abiertas sin cerrar (PROBLEMA si > 0)
    db_total        → total conexiones en PostgreSQL
    db_tiempo_ms    → latencia del ping en milisegundos
    """
    resultado = {
        "status":          "ok",
        "min_connections": DB_POOL_MIN,
        "max_connections": DB_POOL_MAX,
        "pool_disponible": DB_POOL_DISPONIBLE,
        "pool_reservado":  DB_POOL_RESERVADO,
        "pool_activo":     False,
        "circuit_breaker": _cb_estado,
        "cb_fallas":       _cb_fallas,
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

    # ── Ping + métricas via conexión dedicada ─────────────
    # Usa conexión directa fuera del pool para no competir con
    # requests normales cuando el sistema está bajo carga máxima.
    try:
        t_inicio    = time.monotonic()
        _dsn_health = DATABASE_PATH_DEDICATED or DATABASE_PATH
        conn_health = psycopg2.connect(
            dsn=_dsn_health,
            options="-c timezone=America/Bogota",
            connect_timeout=5,
        )
        try:
            with conn_health.cursor() as c:
                c.execute("SELECT 1")
                resultado["db_tiempo_ms"] = round((time.monotonic() - t_inicio) * 1000, 1)

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

                if row[2] and row[2] > 0:
                    logger.warning(f"⚠️ {row[2]} conexiones 'idle in transaction' detectadas")
        finally:
            conn_health.close()

    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"error: {e}"

    return resultado

# ─────────────────────────────────────────────
# Utilidad: cerrar el pool (shutdown graceful) Llamado por el lifespan de FastAPI en main.py
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