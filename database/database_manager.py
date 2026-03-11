import os, time, threading, logging, psycopg2
from psycopg2 import pool, extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv
from fastapi import HTTPException

# ─────────────────────────────────────────────
# Variables de entorno
# ─────────────────────────────────────────────
load_dotenv()
DATABASE_PATH           = os.getenv("DATABASE_PATH")           # pgbouncer:6432  (producción)
DATABASE_PATH_DEDICATED = os.getenv("DATABASE_PATH_DEDICATED") # postgres:5432   (directo)

# ── Por qué DB_POOL_MAX debe ser alto (≥ 50) ──────────────────────────
# PgBouncer en transaction mode multiplexa N conexiones Python a M conexiones
# reales en PostgreSQL (M << N). Python puede tener 50 conexiones "virtuales"
# a PgBouncer; PgBouncer las sirve con solo 15 conexiones reales a Postgres.
#
# Regla práctica:
#   DB_POOL_MAX ≤ PgBouncer MAX_CLIENT_CONN  (100 en docker-compose)
#   PgBouncer DEFAULT_POOL_SIZE ≤ PostgreSQL max_connections - reservas
# ─────────────────────────────────────────────
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))   # bajo: PgBouncer gestiona la concurrencia real
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "50"))  # alto: conexiones virtuales Python → PgBouncer

# ─────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────
CB_FALLAS_MAX   = int(os.getenv("CB_FALLAS_MAX",    "10"))
CB_RECOVERY_SEG = float(os.getenv("CB_RECOVERY_SEG", "15.0"))

_cb_lock         = threading.Lock()
_cb_fallas       = 0
_cb_ultimo_fallo = 0.0
_cb_estado       = "CLOSED"   # CLOSED | OPEN | HALF

logger = logging.getLogger("database_manager")

# ─────────────────────────────────────────────
# Pool Singleton
# ─────────────────────────────────────────────
_POOL_LOCK = threading.Lock()
_DB_POOL: pool.ThreadedConnectionPool = None

def _get_pool() -> pool.ThreadedConnectionPool:
    """
    Retorna el pool psycopg2 singleton (uno por proceso/worker Gunicorn).

    Conecta a DATABASE_PATH (PgBouncer :6432).
    Fallback a DATABASE_PATH_DEDICATED si DATABASE_PATH no está configurado.

    Sin 'options': PgBouncer en transaction mode ignora los parámetros de
    startup (IGNORE_STARTUP_PARAMETERS). Configurar timezone a nivel de DB.

    application_name: visible en pg_stat_activity para diagnóstico.

    keepalives: mantiene vivas las conexiones cliente→PgBouncer a través
    del firewall de Azure (cierra TCP idle tras ~4 min sin keepalives).
    """
    global _DB_POOL

    if _DB_POOL is None:
        with _POOL_LOCK:
            if _DB_POOL is None:
                _dsn_pool = DATABASE_PATH or DATABASE_PATH_DEDICATED
                if not _dsn_pool:
                    raise EnvironmentError(
                        "❌ Ninguna variable DATABASE_PATH ni DATABASE_PATH_DEDICATED configurada."
                    )

                _DB_POOL = pool.ThreadedConnectionPool(
                    DB_POOL_MIN,
                    DB_POOL_MAX,
                    dsn=_dsn_pool,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                    application_name="gestion_express",
                )

                _modo = "PgBouncer :6432" if DATABASE_PATH else "Directo :5432 (local fallback)"
                _msg  = (
                    f"✅ Pool DB inicializado — min={DB_POOL_MIN}, "
                    f"max={DB_POOL_MAX} — modo={_modo}"
                )
                logger.info(_msg)
                print(f"✅ [database_manager] {_msg}")

    return _DB_POOL

# ─────────────────────────────────────────────
# Circuit Breaker — lógica interna
# ─────────────────────────────────────────────
def _cb_registrar_exito():
    global _cb_fallas, _cb_estado
    with _cb_lock:
        if _cb_fallas > 0 or _cb_estado != "CLOSED":
            logger.info("✅ Circuit Breaker: conexión exitosa — estado CLOSED")
        _cb_fallas = 0
        _cb_estado = "CLOSED"

def _cb_registrar_fallo():
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado
    with _cb_lock:
        _cb_fallas      += 1
        _cb_ultimo_fallo = time.monotonic()
        if _cb_fallas >= CB_FALLAS_MAX:
            _cb_estado = "OPEN"
            logger.error(
                f"🔴 Circuit Breaker ABIERTO — {_cb_fallas} fallos consecutivos. "
                f"Reintento en {CB_RECOVERY_SEG}s o via GET /reset-circuit-breaker"
            )

def _cb_verificar() -> str:
    global _cb_estado
    with _cb_lock:
        if _cb_estado == "OPEN":
            if (time.monotonic() - _cb_ultimo_fallo) >= CB_RECOVERY_SEG:
                _cb_estado = "HALF"
                logger.info("🟡 Circuit Breaker SEMI-ABIERTO — probando recuperación")
        return _cb_estado

# ─────────────────────────────────────────────
# Context Manager principal
# ─────────────────────────────────────────────
@contextmanager
def get_db_connection():
    """
    Entrega una conexión del pool con circuit breaker activo.

    Flujo:
    ──────────────────────────────────────────────────────────────────
    1. Circuit Breaker:
        Si hay 10+ fallos consecutivos → HTTP 503 inmediato (no llega a DB).
        Se recupera automáticamente tras CB_RECOVERY_SEG seg o con
        GET /reset-circuit-breaker.

    2. Pool Python → PgBouncer:
        getconn() toma un slot del pool (Python→PgBouncer).
        PoolError si todos los DB_POOL_MAX slots están ocupados.
        ⚠ Si ves POOL_EXHAUSTED frecuente → aumentar DB_POOL_MAX.

    3. Reconexión automática (sin SELECT 1):
        Si psycopg2 detectó que la conexión está cerrada (conn.closed),
        la descarta y solicita una nueva. Sin SELECT 1 en el hot path —
        los keepalives TCP manejan la detección de conexiones muertas.
        Si la propia consulta falla → OperationalError → circuit breaker.

    4. Cleanup en finally:
        Rollback + putconn. Si la conexión quedó en error (STATUS_INERROR
        o STATUS_UNKNOWN), se elimina del pool (close=True) en lugar de
        reciclarse.

    ⚠  Nota de concurrencia:
        Los endpoints async def que llaman a get_db_connection() bloquean
        el event loop de uvicorn. Para alta concurrencia, usar:
            from fastapi.concurrency import run_in_threadpool
            result = await run_in_threadpool(funcion_sincrona_con_db)
    """
    # ── 1. Circuit Breaker ─────────────────────────────────────────
    estado_cb = _cb_verificar()
    if estado_cb == "OPEN":
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Base de datos temporalmente no disponible",
                "mensaje": "El sistema detectó problemas de conectividad. Intente en unos segundos.",
                "codigo":  "CIRCUIT_BREAKER_OPEN"
            }
        )

    conn = None

    try:
        # ── 2. Obtener conexión del pool ───────────────────────────
        conn = _get_pool().getconn()

        # ── 3. Limpiar estado previo de la conexión ────────────────
        if not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass

        # ── 3b. Reconexión si psycopg2 detectó conexión cerrada ───
        # (no hacemos SELECT 1 — los keepalives TCP manejan esto)
        if conn.closed:
            logger.warning("⚠️ Conexión cerrada detectada en pool — descartando y solicitando nueva")
            try:
                _get_pool().putconn(conn, close=True)
            except Exception as e:
                logger.warning(f"⚠️ Error al descartar conexión cerrada: {e}")
            conn = None

            conn = _get_pool().getconn()
            if not conn.closed:
                conn.rollback()
            else:
                raise psycopg2.OperationalError(
                    "La conexión de reemplazo obtenida del pool también está cerrada."
                )

        # Conexión lista — resetear circuit breaker si estaba en HALF
        _cb_registrar_exito()

        yield conn

    except pool.PoolError as e:
        # Pool Python agotado: todos los DB_POOL_MAX slots en uso simultáneo.
        # NO dispara circuit breaker (es carga, no fallo de conectividad).
        # Solución: aumentar DB_POOL_MAX en variables de entorno del App Service.
        logger.error(
            f"❌ Pool psycopg2 agotado — {DB_POOL_MAX} slots en uso simultáneo: {e}"
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Sin conexiones disponibles",
                "mensaje": "El sistema está procesando muchas solicitudes simultáneas. Intente en unos segundos.",
                "codigo":  "POOL_EXHAUSTED"
            }
        )

    except HTTPException:
        raise

    except psycopg2.OperationalError as e:
        # Error real de conectividad: DB caída, PgBouncer caído, query_wait_timeout, etc.
        _cb_registrar_fallo()
        logger.error(f"❌ Error operacional DB: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Error de conexión a la base de datos",
                "mensaje": "No se pudo establecer conexión con la base de datos. Intente nuevamente.",
                "codigo":  "DB_OPERATIONAL_ERROR"
            }
        )

    except Exception as e:
        _cb_registrar_fallo()
        logger.error(f"❌ Error inesperado en get_db_connection: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        if conn:
            try:
                # Detectar si la conexión quedó en estado de error
                try:
                    _tx_status = conn.info.transaction_status
                    _en_error  = conn.closed or _tx_status in (
                        pg_extensions.TRANSACTION_STATUS_INERROR,   # 3
                        pg_extensions.TRANSACTION_STATUS_UNKNOWN,   # 4
                    )
                except Exception:
                    _en_error = conn.closed

                # Rollback antes de devolver al pool
                if not conn.closed:
                    conn.rollback()

                # close=True → elimina del pool (no recicla una conexión errónea)
                _get_pool().putconn(conn, close=_en_error)

                if _en_error:
                    logger.debug("🟥 Conexión errónea descartada del pool (no reciclada)")

            except Exception as e:
                logger.warning(f"⚠️ Error al devolver conexión al pool: {e}")
                try:
                    _get_pool().putconn(conn, close=True)
                except Exception:
                    pass

# ─────────────────────────────────────────────
# Conexión dedicada — SOLO para LISTEN/NOTIFY
# ─────────────────────────────────────────────
def get_dedicated_connection() -> psycopg2.extensions.connection:
    """
    Conexión DIRECTA a Postgres fuera del pool — bypassa PgBouncer.
    PgBouncer en transaction mode NO soporta LISTEN/NOTIFY.
    El llamador es responsable de cerrar con conn.close().
    """
    dsn = DATABASE_PATH_DEDICATED or DATABASE_PATH
    if not dsn:
        raise EnvironmentError(
            "❌ Variable DATABASE_PATH_DEDICATED (o DATABASE_PATH) no configurada."
        )
    conn = psycopg2.connect(
        dsn=dsn,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        application_name="gestion_express_listen",
    )
    logger.info("🔌 Conexión dedicada creada (LISTEN/NOTIFY) — puerto 5432 directo")
    return conn

# ─────────────────────────────────────────────
# Estado del pool + métricas de DB
# Usado por GET /health en main.py
# ─────────────────────────────────────────────
def get_pool_status() -> dict:
    """
    Estado del sistema de conexiones para diagnóstico.

    Campos clave:
    ──────────────────────────────────────────────────────────────────
    pool_min / pool_max     → configuración del pool Python
    pool_disponibles        → slots libres en el pool Python en este momento
    pool_en_uso             → slots actualmente prestados a requests
    circuit_breaker         → CLOSED=ok | OPEN=bloqueado | HALF=recuperando
    db_ping                 → "ok" o descripción del error
    db_active               → conexiones PostgreSQL ejecutando SQL
    db_idle                 → conexiones PostgreSQL en espera
    db_idle_tx              → DEBE ser 0 — indica transacciones huérfanas
    db_total_pg             → total conexiones server-side (≤ PgBouncer DEFAULT_POOL_SIZE)
    db_tiempo_ms            → latencia del ping a Postgres

    NOTA — Con PgBouncer:
    db_total_pg refleja las conexiones server-side de PgBouncer (≤ 15),
    NO el total de clientes Python. Es normal verlo bajo (2-15) incluso
    con mucha carga.

    Lectura de pool_en_uso:
    Si pool_en_uso ≈ pool_max frecuentemente → aumentar DB_POOL_MAX.
    """
    resultado = {
        "status":            "ok",
        "pool_min":          DB_POOL_MIN,
        "pool_max":          DB_POOL_MAX,
        "pool_activo":       False,
        "pool_disponibles":  None,
        "pool_en_uso":       None,
        "circuit_breaker":   _cb_estado,
        "cb_fallas":         _cb_fallas,
        "db_ping":           "sin verificar",
        "db_active":         None,
        "db_idle":           None,
        "db_idle_tx":        None,
        "db_total_pg":       None,
        "db_tiempo_ms":      None,
    }

    # ── Estado del pool Python ─────────────────────────────────────
    try:
        p = _get_pool()
        resultado["pool_activo"] = (p.closed == 0)
        # Acceso a internos de ThreadedConnectionPool para diagnóstico
        try:
            resultado["pool_disponibles"] = len(p._pool)
            resultado["pool_en_uso"]      = len(p._used)
        except Exception:
            pass  # internals pueden cambiar entre versiones de psycopg2
    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"pool no disponible: {e}"
        return resultado

    # ── Ping + métricas vía conexión directa a Postgres ───────────
    # Usa DATABASE_PATH_DEDICATED (puerto 5432) para medir la salud
    # real de Postgres independientemente del estado de PgBouncer.
    try:
        _dsn_health = DATABASE_PATH_DEDICATED or DATABASE_PATH
        t_inicio    = time.monotonic()

        conn_health = psycopg2.connect(
            dsn=_dsn_health,
            connect_timeout=5,
            application_name="gestion_express_health",
        )

        try:
            with conn_health.cursor() as c:
                c.execute("SELECT 1")
                resultado["db_tiempo_ms"] = round(
                    (time.monotonic() - t_inicio) * 1000, 1
                )

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

                resultado["db_active"]   = row[0]
                resultado["db_idle"]     = row[1]
                resultado["db_idle_tx"]  = row[2]
                resultado["db_total_pg"] = row[3]
                resultado["db_ping"]     = "ok"

                if row[2] and row[2] > 0:
                    logger.warning(
                        f"⚠️ {row[2]} conexiones 'idle in transaction' detectadas — "
                        "posibles transacciones huérfanas"
                    )
        finally:
            conn_health.close()

    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"error: {e}"

    return resultado

# ─────────────────────────────────────────────
# Shutdown graceful
# ─────────────────────────────────────────────
def close_pool():
    """
    Cierra todas las conexiones del pool limpiamente.
    Llamado por el lifespan de FastAPI en main.py al detener Gunicorn.
    """
    global _DB_POOL
    if _DB_POOL and not _DB_POOL.closed:
        try:
            _DB_POOL.closeall()
            logger.info("🔴 Pool DB cerrado correctamente.")
            print("🔴 [database_manager] Pool DB cerrado.")
        finally:
            _DB_POOL = None

# ─────────────────────────────────────────────
# Reset de emergencia
# ─────────────────────────────────────────────
def reset_circuit_breaker() -> dict:
    """
    Reset completo: Circuit Breaker + Pool psycopg2.

    Cuándo usarlo:
        DB o PgBouncer estaban caídos y ya se recuperaron pero la app
        sigue rechazando requests. El pool puede tener conexiones muertas
        acumuladas.

    Flujo recomendado:
        1. GET  /health                 → verificar db_ping = "ok"
        2. GET  /reset-circuit-breaker  → reset completo
        3. GET  /health                 → confirmar circuit_breaker = "CLOSED"
    """
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado, _DB_POOL

    # ── 1. Reset Circuit Breaker ───────────────────────────────────
    with _cb_lock:
        fallas_previas = _cb_fallas
        estado_previo  = _cb_estado
        _cb_fallas       = 0
        _cb_ultimo_fallo = 0.0
        _cb_estado       = "CLOSED"

    # ── 2. Reset Pool ──────────────────────────────────────────────
    pool_reseteado = False
    with _POOL_LOCK:
        if _DB_POOL is not None:
            try:
                _DB_POOL.closeall()
                logger.info("🔄 Pool cerrado durante reset completo")
            except Exception as e:
                logger.warning(f"⚠️ Error al cerrar pool en reset: {e}")
            finally:
                _DB_POOL = None
                pool_reseteado = True

    logger.info(
        f"🔄 Reset completo — CB: {estado_previo}→CLOSED, "
        f"fallos previos: {fallas_previas}, pool recreado: {pool_reseteado}"
    )

    return {
        "estado_anterior":   estado_previo,
        "fallas_anteriores": fallas_previas,
        "estado_actual":     "CLOSED",
        "pool_reseteado":    pool_reseteado,
        "mensaje":           "✅ Reset completo — Pool + Circuit Breaker reiniciados"
    }
