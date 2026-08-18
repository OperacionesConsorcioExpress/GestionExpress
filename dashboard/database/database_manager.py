import os, time, threading, logging, psycopg2
from psycopg2 import extensions as pg_extensions
from contextlib import contextmanager
from dotenv import load_dotenv
from fastapi import HTTPException

from .infisical_client import (
    InfisicalError,
    estado_infisical,
    infisical_configurado,
    invalidar_cache_secretos,
    proveedor as _infisical,
)

load_dotenv()

# ── Conexiones propias del Dashboard (BI) ──
# Independientes de DATABASE_PATH / DATABASE_PATH_DEDICATED (esas son del software).
# "Plata" y "Oro" son las capas de datos consumidas por las visualizaciones.
# Conexión heredada conservada durante la etapa de transición.
# No eliminar hasta validar Infisical en todos los entornos.
_MODOS_CONEXION_VALIDOS = {"legacy", "infisical", "auto"}
_CAMPOS_CAPA_REQUERIDOS = ("HOST", "DATA_BASE", "USER", "PASSWORD")
_PUERTO_POSTGRES_DEFAULT = 5432

CB_FALLAS_MAX = int(os.getenv("CB_FALLAS_MAX", "10"))
CB_RECOVERY_SEG = float(os.getenv("CB_RECOVERY_SEG", "15.0"))

# ── Configuración del pool ──
POOL_MIN_CONN = int(os.getenv("POOL_MIN_CONN_BI", "2"))
POOL_MAX_CONN = int(os.getenv("POOL_MAX_CONN_BI", "20"))
POOL_TIMEOUT_SEC = float(os.getenv("POOL_TIMEOUT_SEC_BI", "30"))

logger = logging.getLogger("dashboard.database_manager")

_CONNECT_KWARGS = {
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

def _normalizar_dsn(dsn: str) -> str:
    """
    Normaliza formatos heredados de URL PostgreSQL para que sean
    compatibles con psycopg2.
    """
    if not dsn:
        return dsn

    dsn = dsn.strip()

    if dsn.startswith("postgresql+psycopg2://"):
        return dsn.replace("postgresql+psycopg2://", "postgresql://", 1)

    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql://", 1)

    return dsn

def _valor_vacio(valor) -> bool:
    return valor is None or str(valor).strip() == ""

def _modo_conexion() -> str:
    modo = os.getenv("BI_DATABASE_CONNECTION_MODE", "auto").strip().lower()
    if modo not in _MODOS_CONEXION_VALIDOS:
        permitidos = ", ".join(sorted(_MODOS_CONEXION_VALIDOS))
        raise EnvironmentError(
            f"BI_DATABASE_CONNECTION_MODE inválido. Valores permitidos: {permitidos}."
        )
    return modo

def _validar_puerto(nombre_variable: str, valor: str | None) -> int:
    if _valor_vacio(valor):
        return _PUERTO_POSTGRES_DEFAULT

    try:
        puerto = int(str(valor).strip())
    except ValueError:
        raise EnvironmentError(f"{nombre_variable} debe ser un entero válido.") from None

    if puerto < 1 or puerto > 65535:
        raise EnvironmentError(f"{nombre_variable} debe estar entre 1 y 65535.")

    return puerto

def _cargar_secretos(modo: str) -> dict:
    """
    Secretos de Infisical para resolver la configuración.

    En modo 'infisical' la falla es fatal: es una decisión explícita de operar
    contra el gestor de secretos. En 'auto' y 'legacy' se degrada al entorno
    local, de modo que una caída de Infisical no deja el Dashboard sin datos.
    """
    if modo == "legacy" or not infisical_configurado():
        return {}

    try:
        return _infisical.obtener_secretos()
    except InfisicalError as e:
        if modo == "infisical":
            raise EnvironmentError(
                f"BI_DATABASE_CONNECTION_MODE=infisical pero no se pudieron obtener "
                f"los secretos: {e}"
            ) from None
        logger.warning("Infisical no disponible (%s). Se continúa con el entorno local.", e)
        return {}

def _leer_valor(nombre: str, secretos: dict) -> str | None:
    """
    Valor de una variable. Infisical manda; el entorno local (.env) es respaldo.
    Así un secreto rotado en el gestor no queda opacado por un .env viejo.
    """
    valor = secretos.get(nombre)
    if not _valor_vacio(valor):
        return valor
    return os.getenv(nombre)

def _leer_configuracion_capa(capa: str, secretos: dict) -> tuple[dict, list[str]]:
    prefijo = f"CAPA_{capa.upper()}"
    valores = {
        campo: _leer_valor(f"{prefijo}_{campo}", secretos)
        for campo in _CAMPOS_CAPA_REQUERIDOS
    }
    faltantes = [
        f"{prefijo}_{campo}"
        for campo, valor in valores.items()
        if _valor_vacio(valor)
    ]

    if faltantes:
        return {}, faltantes

    puerto_var = f"{prefijo}_PORT"
    return {
        "host": valores["HOST"].strip(),
        "port": _validar_puerto(puerto_var, _leer_valor(puerto_var, secretos)),
        "dbname": valores["DATA_BASE"].strip(),
        "user": valores["USER"].strip(),
        "password": valores["PASSWORD"],
    }, []

def _leer_dsn_heredado(capa: str, secretos: dict) -> str:
    variable = f"DATABASE_{capa.upper()}_CEINF"
    dsn = _leer_valor(variable, secretos)
    if _valor_vacio(dsn):
        raise EnvironmentError(f"{variable} debe estar configurada.")
    return _normalizar_dsn(dsn.strip())

def _describir_configuracion(capa: str, origen: str, configuracion: dict, modo: str):
    if origen == "heredada":
        logger.info("Configuración BI '%s' seleccionada: modo=%s origen=heredada", capa, modo)
        return

    logger.info(
        "Configuración BI '%s' seleccionada: modo=%s origen=%s host=%s puerto=%s base=%s",
        capa,
        modo,
        origen,
        configuracion.get("host"),
        configuracion.get("port"),
        configuracion.get("dbname"),
    )

def _origen_capa(capa: str, secretos: dict) -> str:
    """Distingue si las credenciales vinieron de Infisical o del entorno local."""
    if not secretos:
        return "entorno"
    clave_testigo = f"CAPA_{capa.upper()}_HOST"
    return "infisical" if not _valor_vacio(secretos.get(clave_testigo)) else "entorno"

def _resolve_capa_config(capa: str) -> dict:
    modo = _modo_conexion()
    secretos = _cargar_secretos(modo)

    if modo == "legacy":
        configuracion = {"dsn": _leer_dsn_heredado(capa, secretos)}
        _describir_configuracion(capa, "heredada", configuracion, modo)
        return configuracion

    config_capa, faltantes = _leer_configuracion_capa(capa, secretos)

    if not faltantes:
        _describir_configuracion(capa, _origen_capa(capa, secretos), config_capa, modo)
        return config_capa

    if modo == "infisical":
        raise EnvironmentError(
            f"Configuración CAPA_{capa.upper()} incompleta. Variables faltantes: "
            f"{', '.join(faltantes)}. Defínalas en Infisical (proyecto/entorno "
            f"configurado) o en el entorno local, o cambie "
            f"BI_DATABASE_CONNECTION_MODE a 'auto'."
        )

    logger.info(
        "Configuración CAPA_%s incompleta (%s). Se utilizará la conexión heredada.",
        capa.upper(),
        ", ".join(faltantes),
    )
    try:
        configuracion = {"dsn": _leer_dsn_heredado(capa, secretos)}
    except EnvironmentError:
        variable_heredada = f"DATABASE_{capa.upper()}_CEINF"
        raise EnvironmentError(
            f"Configuración de base de datos '{capa}' incompleta. Configure "
            f"{', '.join(faltantes)} o {variable_heredada}."
        ) from None
    _describir_configuracion(capa, "heredada", configuracion, modo)
    return configuracion

def _resolve_plata_config() -> dict:
    return _resolve_capa_config("plata")

# ── Credenciales de mantenimiento (escritura) ─────────────────────────────────
# El rol con el que consulta el tablero es de solo lectura a propósito. Para las
# tareas que sí necesitan escribir —refrescar una vista materializada— existe un
# rol aparte, publicado en Infisical como CAPA_{CAPA}_USER_RW / _PASSWORD_RW.
# Se usa de forma ocasional y fuera del pool: el tablero nunca lee con él.
_CAMPOS_ESCRITURA = ("USER_RW", "PASSWORD_RW")

def _resolve_capa_config_rw(capa: str) -> dict | None:
    """
    Configuración de escritura de una capa, o None si no está publicada.

    Reutiliza host, puerto y base de la configuración de lectura: solo cambian
    las credenciales, porque es el mismo servidor.
    """
    modo = _modo_conexion()
    secretos = _cargar_secretos(modo)
    prefijo = f"CAPA_{capa.upper()}"

    usuario = _leer_valor(f"{prefijo}_USER_RW", secretos)
    clave = _leer_valor(f"{prefijo}_PASSWORD_RW", secretos)
    if _valor_vacio(usuario) or _valor_vacio(clave):
        return None

    conexion, faltantes = _leer_configuracion_capa(capa, secretos)
    if faltantes:
        return None

    return {**conexion, "user": usuario.strip(), "password": clave}

@contextmanager
def get_plata_connection_rw():
    """
    Conexión de escritura a la capa Plata, para mantenimiento.

    Deliberadamente fuera del pool: se usa de forma esporádica (refrescos) y no
    debe mezclarse con las conexiones de solo lectura que sirven el tablero. Se
    abre y se cierra en cada uso.
    """
    configuracion = _resolve_capa_config_rw("plata")
    if configuracion is None:
        raise EnvironmentError(
            "No hay credenciales de escritura para la capa Plata. Publique "
            "CAPA_PLATA_USER_RW y CAPA_PLATA_PASSWORD_RW en el gestor de secretos."
        )
    conexion = _create_connection(configuracion, "gestion_express_bi_mantenimiento")
    try:
        yield conexion
    finally:
        try:
            conexion.close()
        except Exception:
            logger.warning("No se pudo cerrar la conexión de mantenimiento.", exc_info=True)

def hay_credenciales_escritura(capa: str = "plata") -> bool:
    """True si el gestor de secretos publica credenciales de escritura para la capa."""
    try:
        return _resolve_capa_config_rw(capa) is not None
    except Exception:
        return False

def _resolve_oro_config() -> dict:
    return _resolve_capa_config("oro")

def _create_connection(configuracion, application_name: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        **configuracion,
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
        configuracion,
        app_name: str,
        min_conn: int,
        max_conn: int,
        timeout_sec: float,
    ):
        self._configuracion = configuracion
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
                conn = _create_connection(configuracion, app_name)
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
            name="bi-db-pool-idle-cleanup",
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
            conn = _create_connection(self._configuracion, self._app_name)
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

class _CapaDatos:
    """
    Encapsula pool + circuit breaker de una capa de datos del Dashboard (plata/oro).
    Cada capa es una base de datos independiente: un fallo en una no afecta a la otra.
    """

    def __init__(self, nombre: str, resolver_config, app_name: str):
        self.nombre = nombre
        self._resolver_config = resolver_config
        self._app_name = app_name

        self._manager_lock = threading.Lock()
        self._pool: _PythonConnectionPool | None = None
        self._config: dict | None = None

        self._cb_lock = threading.Lock()
        self._cb_fallas = 0
        self._cb_ultimo_fallo = 0.0
        self._cb_estado = "CLOSED"

    def _obtener_config(self) -> dict:
        """
        Configuración resuelta una sola vez por capa.

        Evita repetir la resolución (y, con Infisical, el tráfico HTTP) cada vez
        que se consulta el estado del pool o se recrea una conexión.
        """
        if self._config is None:
            self._config = self._resolver_config()
        return self._config

    def _get_pool(self) -> _PythonConnectionPool:
        if self._pool is None:
            with self._manager_lock:
                if self._pool is None:
                    configuracion = self._obtener_config()
                    self._pool = _PythonConnectionPool(
                        configuracion=configuracion,
                        app_name=self._app_name,
                        min_conn=POOL_MIN_CONN,
                        max_conn=POOL_MAX_CONN,
                        timeout_sec=POOL_TIMEOUT_SEC,
                    )
                    logger.info(
                        f"Pool BI '{self.nombre}' activo — "
                        f"min={POOL_MIN_CONN} max={POOL_MAX_CONN} timeout={POOL_TIMEOUT_SEC}s"
                    )
        return self._pool

    def _cb_registrar_exito(self):
        with self._cb_lock:
            if self._cb_fallas > 0 or self._cb_estado != "CLOSED":
                logger.info(f"Circuit Breaker '{self.nombre}': conexion exitosa, estado CLOSED")
            self._cb_fallas = 0
            self._cb_estado = "CLOSED"

    def _cb_registrar_fallo(self):
        with self._cb_lock:
            self._cb_fallas += 1
            self._cb_ultimo_fallo = time.monotonic()
            if self._cb_fallas >= CB_FALLAS_MAX:
                self._cb_estado = "OPEN"
                logger.error(
                    f"Circuit Breaker '{self.nombre}' abierto tras {self._cb_fallas} fallos consecutivos. "
                    f"Reintento en {CB_RECOVERY_SEG}s"
                )

    def _cb_verificar(self) -> str:
        with self._cb_lock:
            if self._cb_estado == "OPEN":
                if (time.monotonic() - self._cb_ultimo_fallo) >= CB_RECOVERY_SEG:
                    self._cb_estado = "HALF"
                    logger.info(f"Circuit Breaker '{self.nombre}' semi-abierto: probando recuperacion")
            return self._cb_estado

    @contextmanager
    def get_connection(self):
        if self._cb_verificar() == "OPEN":
            raise HTTPException(
                status_code=503,
                detail={
                    "error": f"Base de datos '{self.nombre}' temporalmente no disponible",
                    "mensaje": "El sistema detecto problemas de conectividad. Intente en unos segundos.",
                    "codigo": "CIRCUIT_BREAKER_OPEN",
                },
            )

        conn = None
        pool = None

        try:
            # Se guarda la referencia al pool: devolver la conexión en el finally
            # no debe depender de volver a resolver la configuración.
            pool = self._get_pool()
            conn = pool.getconn()

            if conn.closed:
                raise psycopg2.OperationalError("La conexion recien creada llego cerrada.")

            # getconn() ya entrega la conexión con rollback aplicado y
            # cursor_factory limpio; repetirlo aquí solo agrega latencia.
            self._cb_registrar_exito()
            yield conn

        except HTTPException:
            raise

        except EnvironmentError as e:
            # Configuración ausente o inválida: no es un fallo de conectividad,
            # así que no debe contar contra el circuit breaker.
            logger.error(f"Configuración DB '{self.nombre}' inválida: {e}")
            raise HTTPException(
                status_code=503,
                detail={
                    "error": f"Configuración de la base de datos '{self.nombre}' incompleta",
                    "mensaje": str(e),
                    "codigo": "DB_CONFIG_ERROR",
                },
            )

        except InfisicalError as e:
            logger.error(f"Secretos no disponibles para DB '{self.nombre}': {e}")
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "Gestor de secretos no disponible",
                    "mensaje": str(e),
                    "codigo": "SECRETS_UNAVAILABLE",
                },
            )

        except psycopg2.OperationalError as e:
            self._cb_registrar_fallo()
            logger.error(f"Error operacional DB '{self.nombre}': {e}")
            raise HTTPException(
                status_code=503,
                detail={
                    "error": f"Error de conexion a la base de datos '{self.nombre}'",
                    "mensaje": "No se pudo establecer conexion con la base de datos. Intente nuevamente.",
                    "codigo": "DB_OPERATIONAL_ERROR",
                },
            )

        finally:
            if conn is not None and pool is not None:
                try:
                    tx_status = conn.info.transaction_status
                    en_error = conn.closed or tx_status in (
                        pg_extensions.TRANSACTION_STATUS_INERROR,
                        pg_extensions.TRANSACTION_STATUS_UNKNOWN,
                    )
                except Exception:
                    en_error = True

                try:
                    # putconn() hace el rollback y limpia cursor_factory antes de
                    # devolver la conexión al pool: no hace falta anticiparlo.
                    pool.putconn(conn, close=en_error)
                except Exception as e:
                    logger.warning(f"Error devolviendo conexion DB '{self.nombre}': {e}")

    def esta_inicializada(self) -> bool:
        """True si el pool ya fue creado (la capa recibió al menos una consulta)."""
        return self._pool is not None

    def pool_status(self) -> dict:
        with self._cb_lock:
            cb_estado = self._cb_estado
            cb_fallas = self._cb_fallas

        resultado = {
            "status": "ok",
            "pool_min": POOL_MIN_CONN,
            "pool_max": POOL_MAX_CONN,
            "pool_timeout_sec": POOL_TIMEOUT_SEC,
            "pool_activo": False,
            "pool_disponibles": None,
            "pool_en_uso": None,
            "pool_idle": None,
            "circuit_breaker": cb_estado,
            "cb_fallas": cb_fallas,
            "origen_config": None,
            "db_ping": "sin verificar",
        }

        try:
            manager = self._get_pool()
            resultado["pool_activo"]      = (manager.closed == 0)
            resultado["pool_en_uso"]      = manager.active_connections
            resultado["pool_idle"]        = manager.idle_connections
            resultado["pool_disponibles"] = POOL_MAX_CONN - manager.active_connections
            resultado["origen_config"]    = "heredada" if "dsn" in self._obtener_config() else "capa"
        except Exception as e:
            resultado["status"] = "error"
            resultado["db_ping"] = f"pool no disponible: {e}"
            return resultado

        # El ping viaja por el pool: mide la ruta real que usan las consultas y
        # evita abrir (y descartar) una conexión extra en cada chequeo de salud.
        conn_health = None
        try:
            t_inicio = time.monotonic()
            conn_health = manager.getconn()
            with conn_health.cursor() as c:
                c.execute("SELECT 1")
            resultado["db_tiempo_ms"] = round((time.monotonic() - t_inicio) * 1000, 1)
            resultado["db_ping"] = "ok"
        except Exception as e:
            resultado["status"] = "error"
            resultado["db_ping"] = f"error: {e}"
        finally:
            if conn_health is not None:
                try:
                    manager.putconn(conn_health, close=conn_health.closed)
                except Exception:
                    pass

        return resultado

    def close(self):
        # Se descarta la configuración cacheada para que un reinicio del pool
        # vuelva a leer secretos (p. ej. tras una rotación de credenciales).
        self._config = None
        if self._pool is not None:
            try:
                self._pool.closeall()
                logger.info(f"Conexiones DB '{self.nombre}' cerradas correctamente.")
            finally:
                self._pool = None

    def reset_circuit_breaker(self) -> dict:
        with self._cb_lock:
            fallas_previas = self._cb_fallas
            estado_previo = self._cb_estado
            self._cb_fallas = 0
            self._cb_ultimo_fallo = 0.0
            self._cb_estado = "CLOSED"

        logger.info(
            f"Reset de circuit breaker '{self.nombre}': {estado_previo}->CLOSED, "
            f"fallos previos={fallas_previas}"
        )

        return {
            "capa": self.nombre,
            "estado_anterior": estado_previo,
            "fallas_anteriores": fallas_previas,
            "estado_actual": "CLOSED",
        }

# ── Capas de datos del Dashboard ──
_CAPA_PLATA = _CapaDatos("plata", _resolve_plata_config, "gestion_express_bi_plata")
_CAPA_ORO   = _CapaDatos("oro", _resolve_oro_config, "gestion_express_bi_oro")

_CAPAS = {"plata": _CAPA_PLATA, "oro": _CAPA_ORO}

@contextmanager
def get_plata_connection():
    """
    Entrega una conexion del pool de la capa Plata segun
    BI_DATABASE_CONNECTION_MODE.
    """
    with _CAPA_PLATA.get_connection() as conn:
        yield conn

@contextmanager
def get_oro_connection():
    """
    Entrega una conexion del pool de la capa Oro segun
    BI_DATABASE_CONNECTION_MODE.
    """
    with _CAPA_ORO.get_connection() as conn:
        yield conn

def get_pool_status(forzar_inicio: bool = False) -> dict:
    """
    Estado del pool y métricas de conexión de cada capa (plata/oro), más el
    diagnóstico del gestor de secretos.

    Los pools se crean bajo demanda: por defecto una capa que todavía no se ha
    usado se reporta como 'no inicializado' en lugar de abrirse solo para
    responder el chequeo de salud. Use forzar_inicio=True para verificarlas todas.
    """
    capas = {}
    for nombre, capa in _CAPAS.items():
        if capa.esta_inicializada() or forzar_inicio:
            capas[nombre] = capa.pool_status()
        else:
            capas[nombre] = {
                "status": "ok",
                "pool_activo": False,
                "circuit_breaker": "CLOSED",
                "db_ping": "no inicializado (sin uso)",
            }

    capas["secretos"] = estado_infisical()
    capas["modo_conexion"] = os.getenv("BI_DATABASE_CONNECTION_MODE", "auto").strip().lower()
    return capas

def recargar_secretos() -> dict:
    """
    Descarta secretos cacheados y la configuración resuelta, y cierra los pools
    para que la próxima consulta reconstruya la conexión con credenciales
    frescas. Útil tras rotar credenciales en Infisical sin reiniciar la app.
    """
    invalidar_cache_secretos()
    for capa in _CAPAS.values():
        capa.close()
    logger.info("Secretos y pools BI reiniciados; se recargarán en la próxima consulta.")
    return {"recargado": True, "capas": list(_CAPAS)}

def close_pool():
    """Cierra conexiones cliente que sigan abiertas al apagar la app."""
    for capa in _CAPAS.values():
        capa.close()

def reset_circuit_breaker(capa: str = "all") -> dict:
    """
    Reinicia el circuit breaker de una capa ('plata' u 'oro'), o de ambas si
    se llama sin argumentos / con 'all'.
    """
    if capa == "all":
        return {nombre: c.reset_circuit_breaker() for nombre, c in _CAPAS.items()}

    objetivo = _CAPAS.get(capa)
    if objetivo is None:
        raise ValueError(f"Capa desconocida: '{capa}'. Use 'plata', 'oro' o 'all'.")
    return objetivo.reset_circuit_breaker()
