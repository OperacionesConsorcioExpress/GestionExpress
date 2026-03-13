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

class _PgBouncerConnectionAdapter:
    """
    Adaptador de compatibilidad para codigo legado.

    No reutiliza conexiones ni administra un pool Python.
    Cada getconn() abre una conexion nueva al DSN principal y cada putconn()
    la cierra. El pooling real queda delegado a PgBouncer.
    """

    def __init__(self):
        self.closed = 0
        self._lock = threading.Lock()
        self._pool = []
        self._used = {}

    def getconn(self) -> psycopg2.extensions.connection:
        conn = _create_connection(_resolve_primary_dsn(), "gestion_express")
        with self._lock:
            self.closed = 0
            self._used[id(conn)] = conn
        return conn

    def putconn(self, conn, close: bool = False):
        if conn is None:
            return

        with self._lock:
            self._used.pop(id(conn), None)

        try:
            if not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn.close()
        except Exception as e:
            logger.warning(f"Error cerrando conexion cliente: {e}")

    def closeall(self):
        with self._lock:
            conexiones = list(self._used.values())
            self._used.clear()
            self.closed = 1

        for conn in conexiones:
            try:
                if not conn.closed:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    conn.close()
            except Exception as e:
                logger.warning(f"Error cerrando conexion durante shutdown: {e}")

    @property
    def active_connections(self) -> int:
        with self._lock:
            return len(self._used)

def _get_pool() -> _PgBouncerConnectionAdapter:
    global _DB_POOL

    if _DB_POOL is None:
        with _MANAGER_LOCK:
            if _DB_POOL is None:
                _DB_POOL = _PgBouncerConnectionAdapter()
                modo = "PgBouncer :6432" if DATABASE_PATH else "Directo :5432 (fallback)"
                logger.info(
                    f"Conexiones DB inicializadas sin pool local de Python. modo={modo}"
                )
                print(
                    f"[database_manager] Conexiones DB sin pool local de Python. modo={modo}"
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
    Estado del sistema de conexion.

    Se conservan algunas llaves historicas para compatibilidad, pero ya no
    representan un pool Python real.
    """
    resultado = {
        "status": "ok",
        "pool_min": None,
        "pool_max": None,
        "pool_activo": False,
        "pool_disponibles": None,
        "pool_en_uso": None,
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
        resultado["pool_activo"] = (manager.closed == 0)
        resultado["pool_en_uso"] = manager.active_connections
    except Exception as e:
        resultado["status"] = "error"
        resultado["db_ping"] = f"adapter no disponible: {e}"
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
        "mensaje": "Circuit Breaker reiniciado. Las nuevas solicitudes abriran conexiones nuevas.",
    }
