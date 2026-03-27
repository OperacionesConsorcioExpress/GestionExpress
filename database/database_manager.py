import os, time, threading, logging, psycopg2
from psycopg2 import extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
DATABASE_PATH_DEDICATED = os.getenv("DATABASE_PATH_DEDICATED")

CB_FALLAS_MAX = int(os.getenv("CB_FALLAS_MAX", "10"))
CB_RECOVERY_SEG = float(os.getenv("CB_RECOVERY_SEG", "15.0"))

# ── Configuración del pool ──
POOL_MIN_CONN = int(os.getenv("POOL_MIN_CONN", "5"))
POOL_MAX_CONN = int(os.getenv("POOL_MAX_CONN", "100"))
POOL_TIMEOUT_SEC = float(os.getenv("POOL_TIMEOUT_SEC", "30"))

_cb_lock = threading.Lock()
_cb_fallas = 0
_cb_ultimo_fallo = 0.0
_cb_estado = "CLOSED"

logger = logging.getLogger("database_manager")

_MANAGER_LOCK = threading.Lock()
_DB_POOL = None

_CONNECT_KWARGS = {
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

def _resolve_primary_dsn() -> str:
    dsn = DATABASE_PATH or DATABASE_PATH_DEDICATED
    if not dsn:
        raise EnvironmentError(
            "DATABASE_PATH o DATABASE_PATH_DEDICATED debe estar configurada."
        )
    return dsn

def _resolve_dedicated_dsn() -> str:
    dsn = DATABASE_PATH_DEDICATED or DATABASE_PATH
    if not dsn:
        raise EnvironmentError(
            "DATABASE_PATH_DEDICATED o DATABASE_PATH debe estar configurada."
        )
    return dsn

def _create_connection(dsn: str, application_name: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dsn=dsn,
        application_name=application_name,
        **_CONNECT_KWARGS,
    )

class _PythonConnectionPool:
    """
    Pool de conexiones Python thread-safe con control de min/max y limpieza de idle.

    - Recicla conexiones en lugar de abrir/cerrar por request.
    - Espera hasta POOL_TIMEOUT_SEC cuando el pool está al máximo antes de fallar.
    - Un hilo daemon libera conexiones idle por encima de POOL_MIN_CONN cada 60s.
    - Interfaz compatible con el adaptador anterior (getconn/putconn/closeall).
    """

    _IDLE_CHECK_INTERVAL = 60   # segundos entre limpiezas
    _CONN_MAX_AGE_SEC    = 1800 # 30 min; conexiones más viejas se renuevan al devolver

    def __init__(
        self,
        dsn: str,
        app_name: str,
        min_conn: int,
        max_conn: int,
        timeout_sec: float,
    ):
        self._dsn         = dsn
        self._app_name    = app_name
        self._min_conn    = min_conn
        self._max_conn    = max_conn
        self._timeout_sec = timeout_sec

        # _cond protege _idle y _in_use_count; también sirve para wait/notify
        self._cond         = threading.Condition()
        # lista de (created_at, conn) — las más recientes al final
        self._idle: list   = []
        self._in_use_count = 0
        self.closed        = 0

        # Pre-calentar hasta min_conn (fallos no son fatales)
        for _ in range(min_conn):
            try:
                conn = _create_connection(dsn, app_name)
                self._idle.append((time.monotonic(), conn))
            except Exception as e:
                logger.warning(f"Pool init: fallo al pre-crear conexión: {e}")

        logger.info(
            f"Pool Python iniciado — min={min_conn} max={max_conn} "
            f"timeout={timeout_sec}s  pre-creadas={len(self._idle)}"
        )

        # Hilo daemon de limpieza idle
        self._stop_event = threading.Event()
        t = threading.Thread(
            target=self._idle_cleanup_loop,
            daemon=True,
            name="db-pool-idle-cleanup",
        )
        t.start()

    # ── propiedades internas ──────────────────────────────────────────────────

    def _total(self) -> int:
        """Total de conexiones vivas (idle + en uso). Debe llamarse con _cond adquirido."""
        return len(self._idle) + self._in_use_count

    # ── ciclo de limpieza ─────────────────────────────────────────────────────

    def _idle_cleanup_loop(self):
        while not self._stop_event.wait(self._IDLE_CHECK_INTERVAL):
            self._purge_excess_idle()

    def _purge_excess_idle(self):
        """Cierra conexiones idle por encima de min_conn (las más viejas primero)."""
        to_close = []
        with self._cond:
            # _idle está ordenado más nuevo al final; pop(0) = más viejo
            while len(self._idle) > self._min_conn:
                _ts, conn = self._idle.pop(0)
                to_close.append(conn)

        for conn in to_close:
            try:
                if not conn.closed:
                    conn.close()
                logger.debug("Pool: conexión idle eliminada por limpieza periódica")
            except Exception as e:
                logger.warning(f"Pool cleanup: {e}")

    # ── interfaz pública ──────────────────────────────────────────────────────

    def getconn(self) -> psycopg2.extensions.connection:
        deadline = time.monotonic() + self._timeout_sec

        with self._cond:
            while True:
                # 1. Reutilizar conexión idle (más reciente primero)
                while self._idle:
                    created_at, conn = self._idle.pop()
                    age = time.monotonic() - created_at
                    if conn.closed or age > self._CONN_MAX_AGE_SEC:
                        # demasiado vieja o muerta → descartar silenciosamente
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue
                    try:
                        conn.rollback()
                        # Garantizar cursor_factory limpio sin importar qué módulo
                        # haya modificado la conexión antes de devolverla al pool
                        conn.cursor_factory = pg_extensions.cursor
                        self._in_use_count += 1
                        return conn
                    except Exception:
                        pass  # conexión rota, descartar

                # 2. Crear nueva si hay cupo
                if self._total() < self._max_conn:
                    self._in_use_count += 1  # reservar slot antes de salir del lock
                    break

                # 3. Esperar hasta que haya cupo o venza el timeout
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise psycopg2.OperationalError(
                        f"Pool agotado: ninguna conexión disponible tras {self._timeout_sec}s "
                        f"(en_uso={self._in_use_count} idle={len(self._idle)} max={self._max_conn})"
                    )
                self._cond.wait(timeout=remaining)

        # Crear la conexión fuera del lock para no bloquear otros hilos
        try:
            conn = _create_connection(self._dsn, self._app_name)
            return conn
        except Exception:
            with self._cond:
                self._in_use_count -= 1
                self._cond.notify()
            raise

    def putconn(self, conn, close: bool = False):
        if conn is None:
            return

        returned_to_pool = False

        if not close and not conn.closed:
            try:
                conn.rollback()
                # Limpiar cursor_factory para que el próximo usuario siempre
                # reciba el cursor por defecto (tuplas), independientemente de
                # lo que haya seteado el módulo anterior (p. ej. RealDictCursor)
                conn.cursor_factory = pg_extensions.cursor
                with self._cond:
                    self._in_use_count -= 1
                    self._idle.append((time.monotonic(), conn))
                    returned_to_pool = True
                    self._cond.notify()
            except Exception as e:
                logger.warning(f"Pool putconn: conexión descartada por error: {e}")

        if not returned_to_pool:
            with self._cond:
                self._in_use_count -= 1
                self._cond.notify()
            try:
                if not conn.closed:
                    conn.close()
            except Exception as e:
                logger.warning(f"Pool putconn: error cerrando conexión: {e}")

    def closeall(self):
        self._stop_event.set()
        with self._cond:
            idle_conns = [c for _, c in self._idle]
            self._idle.clear()
            self.closed = 1
            self._cond.notify_all()

        for conn in idle_conns:
            try:
                if not conn.closed:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    conn.close()
            except Exception as e:
                logger.warning(f"Pool closeall: error cerrando conexión idle: {e}")

        logger.info("Pool de conexiones cerrado correctamente.")

    @property
    def active_connections(self) -> int:
        with self._cond:
            return self._in_use_count

    @property
    def idle_connections(self) -> int:
        with self._cond:
            return len(self._idle)

def _get_pool() -> _PythonConnectionPool:
    global _DB_POOL

    if _DB_POOL is None:
        with _MANAGER_LOCK:
            if _DB_POOL is None:
                dsn  = _resolve_primary_dsn()
                modo = "PgBouncer :6432" if DATABASE_PATH else "Directo :5432 (fallback)"
                _DB_POOL = _PythonConnectionPool(
                    dsn=dsn,
                    app_name="gestion_express",
                    min_conn=POOL_MIN_CONN,
                    max_conn=POOL_MAX_CONN,
                    timeout_sec=POOL_TIMEOUT_SEC,
                )
                logger.info(
                    f"Pool Python activo — modo={modo} "
                    f"min={POOL_MIN_CONN} max={POOL_MAX_CONN} timeout={POOL_TIMEOUT_SEC}s"
                )
                print(
                    f"[database_manager] Pool Python activo — modo={modo} "
                    f"min={POOL_MIN_CONN} max={POOL_MAX_CONN} timeout={POOL_TIMEOUT_SEC}s"
                )

    return _DB_POOL

def _cb_registrar_exito():
    global _cb_fallas, _cb_estado
    with _cb_lock:
        if _cb_fallas > 0 or _cb_estado != "CLOSED":
            logger.info("Circuit Breaker: conexion exitosa, estado CLOSED")
        _cb_fallas = 0
        _cb_estado = "CLOSED"

def _cb_registrar_fallo():
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado
    with _cb_lock:
        _cb_fallas += 1
        _cb_ultimo_fallo = time.monotonic()
        if _cb_fallas >= CB_FALLAS_MAX:
            _cb_estado = "OPEN"
            logger.error(
                f"Circuit Breaker abierto tras {_cb_fallas} fallos consecutivos. "
                f"Reintento en {CB_RECOVERY_SEG}s o via GET /reset-circuit-breaker"
            )

def _cb_verificar() -> str:
    global _cb_estado
    with _cb_lock:
        if _cb_estado == "OPEN":
            if (time.monotonic() - _cb_ultimo_fallo) >= CB_RECOVERY_SEG:
                _cb_estado = "HALF"
                logger.info("Circuit Breaker semi-abierto: probando recuperacion")
        return _cb_estado

@contextmanager
def get_db_connection():
    """
    Entrega una conexion nueva al DSN principal.

    - Usa DATABASE_PATH si esta disponible (PgBouncer).
    - Hace fallback a DATABASE_PATH_DEDICATED si no existe DATABASE_PATH.
    - No hay pool Python; PgBouncer administra el pooling real.
    """
    if _cb_verificar() == "OPEN":
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Base de datos temporalmente no disponible",
                "mensaje": "El sistema detecto problemas de conectividad. Intente en unos segundos.",
                "codigo": "CIRCUIT_BREAKER_OPEN",
            },
        )

    conn = None

    try:
        conn = _get_pool().getconn()

        if conn.closed:
            raise psycopg2.OperationalError("La conexion recien creada llego cerrada.")

        try:
            conn.rollback()
        except Exception:
            pass

        _cb_registrar_exito()
        yield conn

    except HTTPException:
        raise

    except psycopg2.OperationalError as e:
        _cb_registrar_fallo()
        logger.error(f"Error operacional DB: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Error de conexion a la base de datos",
                "mensaje": "No se pudo establecer conexion con la base de datos. Intente nuevamente.",
                "codigo": "DB_OPERATIONAL_ERROR",
            },
        )

    finally:
        if conn:
            try:
                try:
                    tx_status = conn.info.transaction_status
                    en_error = conn.closed or tx_status in (
                        pg_extensions.TRANSACTION_STATUS_INERROR,
                        pg_extensions.TRANSACTION_STATUS_UNKNOWN,
                    )
                except Exception:
                    en_error = conn.closed

                if not conn.closed:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                _get_pool().putconn(conn, close=en_error)
            except Exception as e:
                logger.warning(f"Error cerrando conexion DB: {e}")
                try:
                    _get_pool().putconn(conn, close=True)
                except Exception:
                    pass

def get_dedicated_connection() -> psycopg2.extensions.connection:
    """
    Conexion directa a PostgreSQL por 5432.

    Se mantiene para flujos que no deben pasar por PgBouncer, por ejemplo
    LISTEN/NOTIFY o jobs/entornos externos.
    """
    conn = _create_connection(
        _resolve_dedicated_dsn(),
        "gestion_express_dedicated",
    )
    logger.info("Conexion dedicada creada por 5432 directo")
    return conn

def get_pool_status() -> dict:
    """
    Estado del pool Python y métricas de conexión PostgreSQL.
    """
    resultado = {
        "status": "ok",
        "pool_min": POOL_MIN_CONN,
        "pool_max": POOL_MAX_CONN,
        "pool_timeout_sec": POOL_TIMEOUT_SEC,
        "pool_activo": False,
        "pool_disponibles": None,
        "pool_en_uso": None,
        "pool_idle": None,
        "pooling_backend": "PgBouncer externo" if DATABASE_PATH else "Directo sin PgBouncer",
        "connection_mode": "DATABASE_PATH" if DATABASE_PATH else "DATABASE_PATH_DEDICATED",
        "circuit_breaker": _cb_estado,
        "cb_fallas": _cb_fallas,
        "db_ping": "sin verificar",
        "db_active": None,
        "db_idle": None,
        "db_idle_tx": None,
        "db_total_pg": None,
        "db_tiempo_ms": None,
    }

    try:
        manager = _get_pool()
        resultado["pool_activo"]      = (manager.closed == 0)
        resultado["pool_en_uso"]      = manager.active_connections
        resultado["pool_idle"]        = manager.idle_connections
        resultado["pool_disponibles"] = POOL_MAX_CONN - manager.active_connections
    except Exception as e:
        resultado["status"] = "error"
        resultado["db_ping"] = f"pool no disponible: {e}"
        return resultado

    try:
        dsn_health = _resolve_dedicated_dsn()
        t_inicio = time.monotonic()
        conn_health = psycopg2.connect(
            dsn=dsn_health,
            connect_timeout=5,
            application_name="gestion_express_health",
        )

        try:
            with conn_health.cursor() as c:
                c.execute("SELECT 1")
                resultado["db_tiempo_ms"] = round((time.monotonic() - t_inicio) * 1000, 1)

                c.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'active') AS active,
                        COUNT(*) FILTER (WHERE state = 'idle') AS idle,
                        COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_tx,
                        COUNT(*) AS total
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    """
                )
                row = c.fetchone()

                resultado["db_active"] = row[0]
                resultado["db_idle"] = row[1]
                resultado["db_idle_tx"] = row[2]
                resultado["db_total_pg"] = row[3]
                resultado["db_ping"] = "ok"

                if row[2] and row[2] > 0:
                    logger.warning(
                        f"{row[2]} conexiones 'idle in transaction' detectadas"
                    )
        finally:
            conn_health.close()

    except Exception as e:
        resultado["status"] = "error"
        resultado["db_ping"] = f"error: {e}"

    return resultado

def close_pool():
    """
    Cierra conexiones cliente que sigan abiertas al apagar la app.

    No existe pool Python que reciclar; este cierre es solo defensivo.
    """
    global _DB_POOL
    if _DB_POOL is not None:
        try:
            _DB_POOL.closeall()
            logger.info("Conexiones DB cerradas correctamente.")
            print("[database_manager] Conexiones DB cerradas.")
        finally:
            _DB_POOL = None

def reset_circuit_breaker() -> dict:
    """
    Reinicia el circuit breaker.

    Ya no hay pool Python local que resetear: las siguientes solicitudes
    abriran conexiones nuevas y PgBouncer manejara el pooling real.
    """
    global _cb_fallas, _cb_ultimo_fallo, _cb_estado

    with _cb_lock:
        fallas_previas = _cb_fallas
        estado_previo = _cb_estado
        _cb_fallas = 0
        _cb_ultimo_fallo = 0.0
        _cb_estado = "CLOSED"

    logger.info(
        f"Reset de circuit breaker: {estado_previo}->CLOSED, "
        f"fallos previos={fallas_previas}"
    )

    return {
        "estado_anterior": estado_previo,
        "fallas_anteriores": fallas_previas,
        "estado_actual": "CLOSED",
        "pool_reseteado": False,
        "mensaje": "Circuit Breaker reiniciado. El pool Python reanuda la entrega de conexiones.",
    }