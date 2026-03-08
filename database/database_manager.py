import os, time, queue, threading, logging, psycopg2
from psycopg2 import pool, extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv
from fastapi import HTTPException

# ─────────────────────────────────────────────
# Configuración — valores leídos desde .env o variables de entorno del App Service de Azure.
# ─────────────────────────────────────────────
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")  # Puerto 6432 — PgBouncer (producción Azure)
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

CB_FALLAS_MAX    = int(os.getenv("CB_FALLAS_MAX",    "10"))   # Fallos consecutivos para abrir el circuit breaker 10 tolera reinicios normales de gunicorn sin dispararse
CB_RECOVERY_SEG  = float(os.getenv("CB_RECOVERY_SEG", "15.0")) # Segundos de espera antes de intentar recuperación 15s suficiente para reinicio normal del App Service

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
                # AJUSTE:
                # Se mantiene tu fallback actual: primero PgBouncer (:6432),
                # si no existe entonces conexión directa (:5432).
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

    Azure PostgreSQL cierra conexiones idle después de un tiempo
    (firewall, mantenimiento, reinicios del servidor).
    Sin esta validación, el pool puede entregar una conexión muerta
    y el request falla con:
        'SSL connection has been closed unexpectedly'
    """
    try:
        if conn is None:
            return False

        if conn.closed:
            return False

        # AJUSTE:
        # Se mantiene la validación liviana con SELECT 1.
        # No se cambia tu lógica, solo se deja explícito el caso None/cerrada.
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
        Si la DB tuvo 5+ fallos consecutivos, rechaza inmediatamente
        con HTTP 503 en lugar de intentar conectar.

    CAPA 2 — Cola con timeout:
        Si los DB_POOL_DISPONIBLE slots están ocupados, espera hasta
        DB_POOL_TIMEOUT_SEG segundos. Si en ese tiempo se libera un slot,
        continúa. Si no, responde HTTP 503 con mensaje claro al usuario.

    CAPA 1 — Colchón:
        El semáforo garantiza que siempre quede DB_POOL_RESERVADO slots
        libres para health check y operaciones críticas de admin.

    IMPORTANTE — Por qué NO se toca conn.autocommit:
        psycopg2 maneja autocommit=False por defecto.
        Cambiar autocommit con una transacción abierta genera:
            'set_session cannot be used inside a transaction'
        Se hace rollback() PRIMERO para limpiar transacciones pendientes.
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
    #  Si no hay slot disponible en el tiempo configurado, se responde con POOL_TIMEOUT.
    slot_adquirido = _pool_semaforo.acquire(timeout=DB_POOL_TIMEOUT_SEG)
    if not slot_adquirido:
        logger.warning(
            f"⚠️ Pool ocupado — request esperó {DB_POOL_TIMEOUT_SEG}s sin obtener conexión"
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Servidor ocupado",
                "mensaje": "El sistema está procesando muchas solicitudes. Intente nuevamente en unos segundos.",
                "codigo":  "POOL_TIMEOUT"
            }
        )

    conn = None

    try:
        # Trazabilidad para identificar si el fallo ocurre al momento de pedir conexión al pool.
        logger.debug("🟦 Intentando obtener conexión del pool")
        conn = _get_pool().getconn()
        logger.debug("🟩 Conexión obtenida del pool")

        # Limpiar transacción residual antes de usar la conexión.
        if not conn.closed:
            conn.rollback()

        # ── Reconexión automática ──────────────────────────────────
        # Si Azure cerró la conexión mientras estaba idle en el pool,
        # se detecta y reemplaza automáticamente.
        if not _conexion_viva(conn):
            logger.warning("⚠️ Conexión muerta detectada — cerrando y solicitando nueva")

            try:
                # close=True elimina la conexión del pool en lugar de reciclarla.
                # Sin esto, la conexión muerta regresa al pool y vuelve a entregarse.
                _get_pool().putconn(conn, close=True)
            except Exception as e:
                logger.warning(f"⚠️ No se pudo cerrar conexión muerta del pool: {e}")

            conn = None

            logger.debug("🟦 Solicitando nueva conexión tras detectar conexión muerta")
            conn = _get_pool().getconn()
            logger.debug("🟩 Nueva conexión obtenida del pool")

            if not conn.closed:
                conn.rollback()

            # La nueva conexión sale muerta, disparamos error operacional.
            if not _conexion_viva(conn):
                raise psycopg2.OperationalError(
                    "La nueva conexión obtenida del pool también está inactiva."
                )

        # Conexión exitosa — resetear circuit breaker
        _cb_registrar_exito()

        yield conn

    except HTTPException:
        # Re-lanzar HTTPExceptions sin modificar (son respuestas controladas)
        raise

    except psycopg2.pool.PoolError as e:
        # AJUSTE:
        # Este error normalmente aparece cuando el semáforo permitió avanzar,
        # pero la conexión aún no había sido devuelta físicamente al pool,
        # o bien el pool real ya estaba sin conexiones disponibles.
        logger.error(f"❌ Pool de conexiones agotado: {e}")
        raise HTTPException(
            status_code=503,
            detail={
                "error":   "Sin conexiones disponibles",
                "mensaje": "El sistema está procesando muchas solicitudes. Intente en unos segundos.",
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
        # Devolver la conexión al pool y DESPUÉS liberar el semáforo.
        # Esto evita la carrera donde otro request toma el slot del semáforo
        # pero la conexión todavía no ha sido devuelta al pool real.
        if conn:
            try:
                # Detectar si la conexión quedó en estado de error durante la operación.
                # Se usa transaction_status de libpq (conn.info.transaction_status) porque
                # psycopg2.extensions NO expone STATUS_IN_ERROR ni STATUS_INTRANS_INERROR.
                # Los valores correctos son del namespace TRANSACTION_STATUS_*:
                #   TRANSACTION_STATUS_INERROR  (3) → error activo en la transacción
                #   TRANSACTION_STATUS_UNKNOWN  (4) → conexión perdida / estado desconocido
                # Referencia: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.connection.info
                try:
                    _tx_status = conn.info.transaction_status
                    _en_error = conn.closed or _tx_status in (
                        pg_extensions.TRANSACTION_STATUS_INERROR,   # 3 — error en tx activa
                        pg_extensions.TRANSACTION_STATUS_UNKNOWN,   # 4 — conexión perdida
                    )
                except Exception:
                    # Si conn.info no está disponible (conexión ya cerrada), tratar como error.
                    _en_error = conn.closed

                if not conn.closed:
                    conn.rollback()
                    conn.cursor_factory = pg_extensions.cursor

                logger.debug("🟨 Devolviendo conexión al pool")
                # close=True cuando está en error: elimina del pool en lugar de reciclar.
                _get_pool().putconn(conn, close=_en_error)
                logger.debug(f"🟩 Conexión {'cerrada' if _en_error else 'devuelta'} al pool")

            except Exception as e:
                logger.warning(f"⚠️ Error al devolver conexión al pool: {e}")
                try:
                    _get_pool().putconn(conn, close=True)
                except Exception:
                    pass

        # El release del semáforo queda al final a propósito — no cambiar este orden.
        if slot_adquirido:
            _pool_semaforo.release()

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
    pool_activo            → True si el pool está inicializado
    min_connections        → DB_POOL_MIN configurado
    max_connections        → DB_POOL_MAX configurado
    pool_capacidad_normal  → slots configurados para requests normales (max - reservado)
    pool_reservado         → slots reservados para operaciones críticas
    circuit_breaker        → estado: CLOSED | OPEN | HALF
    cb_fallas              → fallos consecutivos actuales
    db_ping                → "ok" o descripción del error
    db_active              → conexiones ejecutando SQL ahora mismo
    db_idle                → conexiones en espera
    db_idle_tx             → conexiones idle in transaction
    db_total               → total conexiones vistas por PostgreSQL
    db_tiempo_ms           → latencia del ping en milisegundos

    IMPORTANTE:
    ──────────────────────────────────────────────────────────
    pool_capacidad_normal NO es disponibilidad real en tiempo real.
    Es la capacidad configurada para tráfico normal:
        DB_POOL_MAX - DB_POOL_RESERVADO

    El ping y métricas de DB se toman con conexión dedicada
    fuera del pool para no competir con requests normales.
    """
    resultado = {
        "status":                "ok",
        "min_connections":       DB_POOL_MIN,
        "max_connections":       DB_POOL_MAX,
        "pool_capacidad_normal": DB_POOL_DISPONIBLE,
        "pool_reservado":        DB_POOL_RESERVADO,
        "pool_activo":           False,
        "circuit_breaker":       _cb_estado,
        "cb_fallas":             _cb_fallas,
        "db_ping":               "sin verificar",
        "db_active":             None,
        "db_idle":               None,
        "db_idle_tx":            None,
        "db_total":              None,
        "db_tiempo_ms":          None,
    }

    # ── Estado del pool local ──────────────────────────────
    try:
        p = _get_pool()
        resultado["pool_activo"] = (p.closed == 0)
    except Exception as e:
        resultado["status"]  = "error"
        resultado["db_ping"] = f"pool no disponible: {e}"
        return resultado

    # ── Ping + métricas vía conexión dedicada ──────────────
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
    Cierra todas las conexiones del pool limpiamente se ejecuta cuando gunicorn hace shutdown del worker, asegurando que PostgreSQL no quede con conexiones huérfanas.
    """
    global _DB_POOL

    if _DB_POOL and not _DB_POOL.closed:
        try:
            _DB_POOL.closeall()
            logger.info("🔴 Pool DB cerrado correctamente.")
            print("🔴 [database_manager] Pool DB cerrado.")
        finally:
            # AJUSTE:
            # Asegurar que la referencia global quede en None incluso si
            # closeall() lanza algún comportamiento no esperado.
            _DB_POOL = None

# ─────────────────────────────────────────────
# Reset Circuit Breaker — recuperación manual
# ─────────────────────────────────────────────
def reset_circuit_breaker() -> dict:
    """
    Reset completo del sistema de conexiones:
        1. Circuit Breaker → CLOSED
        2. Pool psycopg2   → cierra todas las conexiones y recrea el pool
        3. Semáforo        → recrea con capacidad original DB_POOL_DISPONIBLE

    ¿Cuándo usarlo?
    ──────────────────────────────────────────────────────────
    - DB estaba caída y ya se recuperó pero la app sigue mostrando errores.
    - Pool agotado (POOL_EXHAUSTED) con DB funcionando — indica slots perdidos.
    - Después de redeploy con fallos en cascada.

    Flujo recomendado:
        1. GET  /health                → verificar db_ping = "ok"
        2. GET  /reset-circuit-breaker → reset completo
        3. GET  /health                → confirmar circuit_breaker = "CLOSED"
    """
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado, _DB_POOL, _pool_semaforo

    # ── 1. Reset Circuit Breaker ───────────────────────────
    with _cb_lock:
        fallas_previas = _cb_fallas
        estado_previo  = _cb_estado
        _cb_fallas       = 0
        _cb_ultimo_fallo = 0.0
        _cb_estado       = "CLOSED"

    # ── 2. Reset Pool — cierra y recrea ───────────────────
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

    # ── 3. Reset Semáforo — recrea con capacidad original ─
    # threading.Semaphore no permite resetear el contador interno,
    # se recrea el objeto. Seguro porque el reset implica que no hay
    # requests activos (el pool acaba de cerrarse).
    _pool_semaforo = threading.Semaphore(DB_POOL_DISPONIBLE)

    logger.info(
        f"🔄 Reset completo — CB: {estado_previo}→CLOSED, "
        f"fallos previos: {fallas_previas}, pool recreado: {pool_reseteado}, "
        f"semáforo restaurado a: {DB_POOL_DISPONIBLE}"
    )

    return {
        "estado_anterior":    estado_previo,
        "fallas_anteriores":  fallas_previas,
        "estado_actual":      "CLOSED",
        "pool_reseteado":     pool_reseteado,
        "semaforo_restaurado": DB_POOL_DISPONIBLE,
        "mensaje":            "✅ Reset completo — Pool + Circuit Breaker + Semáforo reiniciados"
    }