"""
infisical_client.py — Proveedor de secretos Infisical para el Dashboard (BI)
════════════════════════════════════════════════════════════════════════════
Resuelve las credenciales de las capas de datos (Plata / Oro) contra el
servidor Infisical mediante autenticación *Universal Auth* (Machine Identity).

Diseño:
  · Una sola descarga de secretos por proceso, reutilizada durante TTL segundos.
  · Token de acceso cacheado hasta su expiración real (con margen de seguridad).
  · Thread-safe: el pool de conexiones se construye desde hilos de FastAPI.
  · Nunca escribe en os.environ ni registra valores de secretos en el log.

Variables de entorno que consume (todas del .env de la raíz):
  INFISICAL_URL             — p. ej. http://20.237.248.212:8080
  INFISICAL_CLIENT_ID       — identidad de máquina (Universal Auth)
  INFISICAL_CLIENT_SECRET   — secreto de la identidad
  INFISICAL_PROJECT_ID      — identificador del proyecto (workspaceId)
  INFISICAL_ENVIRONMENT     — entorno del proyecto (por defecto: prod)
  INFISICAL_SECRET_PATH     — carpeta de secretos (por defecto: /)
  INFISICAL_CACHE_TTL_SEG   — vigencia del caché en segundos (por defecto: 900)
  INFISICAL_TIMEOUT_SEG     — timeout HTTP en segundos (por defecto: 10)
  INFISICAL_RECURSIVE       — incluir subcarpetas: true/false (por defecto: false)
"""

import os
import time
import logging
import threading

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("dashboard.infisical")

# Margen que se resta a la expiración del token para renovarlo antes de tiempo.
_MARGEN_RENOVACION_SEG = 60
_VIGENCIA_TOKEN_DEFECTO_SEG = 3600

_VALORES_VERDADEROS = {"1", "true", "t", "yes", "y", "si", "sí"}

class InfisicalError(RuntimeError):
    """Fallo al autenticar o descargar secretos desde Infisical."""

def _valor_vacio(valor) -> bool:
    return valor is None or str(valor).strip() == ""

def _entero_env(nombre: str, defecto: int) -> int:
    bruto = os.getenv(nombre)
    if _valor_vacio(bruto):
        return defecto
    try:
        return int(str(bruto).strip())
    except ValueError:
        logger.warning("%s no es un entero válido; se usa %s.", nombre, defecto)
        return defecto


class _ProveedorInfisical:
    """
    Cliente perezoso y cacheado del API de Infisical.

    No se conecta al construirse: la primera lectura de un secreto dispara el
    login y la descarga. A partir de ahí sirve desde memoria hasta que vence
    el TTL, de modo que resolver la configuración de una capa no genera
    tráfico HTTP en cada consulta del Dashboard.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._secretos: dict[str, str] | None = None
        self._secretos_vencen_en = 0.0
        self._token: str | None = None
        self._token_vence_en = 0.0
        self._sesion: requests.Session | None = None

    # ── parámetros de entorno (releídos en cada uso: el .env puede cambiar) ──

    @property
    def _url(self) -> str:
        return (os.getenv("INFISICAL_URL") or "").strip().rstrip("/")

    @property
    def _client_id(self) -> str:
        return (os.getenv("INFISICAL_CLIENT_ID") or "").strip()

    @property
    def _client_secret(self) -> str:
        return (os.getenv("INFISICAL_CLIENT_SECRET") or "").strip()

    @property
    def _project_id(self) -> str:
        return (os.getenv("INFISICAL_PROJECT_ID") or "").strip()

    @property
    def _environment(self) -> str:
        valor = (os.getenv("INFISICAL_ENVIRONMENT") or "").strip()
        return valor or "prod"

    @property
    def _secret_path(self) -> str:
        valor = (os.getenv("INFISICAL_SECRET_PATH") or "").strip()
        return valor or "/"

    @property
    def _timeout(self) -> int:
        return _entero_env("INFISICAL_TIMEOUT_SEG", 10)

    @property
    def _ttl(self) -> int:
        return _entero_env("INFISICAL_CACHE_TTL_SEG", 900)

    @property
    def _recursivo(self) -> bool:
        return (os.getenv("INFISICAL_RECURSIVE") or "").strip().lower() in _VALORES_VERDADEROS

    # ── estado de configuración ──────────────────────────────────────────────

    def variables_faltantes(self) -> list[str]:
        """Variables de entorno obligatorias que no están definidas."""
        requeridas = {
            "INFISICAL_URL": self._url,
            "INFISICAL_CLIENT_ID": self._client_id,
            "INFISICAL_CLIENT_SECRET": self._client_secret,
            "INFISICAL_PROJECT_ID": self._project_id,
        }
        return [nombre for nombre, valor in requeridas.items() if _valor_vacio(valor)]

    def esta_configurado(self) -> bool:
        """True si hay datos suficientes en el entorno para intentar la conexión."""
        return not self.variables_faltantes()

    # ── acceso HTTP ──────────────────────────────────────────────────────────

    def _obtener_sesion(self) -> requests.Session:
        """Sesión HTTP reutilizada: evita renegociar la conexión en cada llamada."""
        if self._sesion is None:
            self._sesion = requests.Session()
        return self._sesion

    def _obtener_token(self) -> str:
        """Devuelve un token válido, renovándolo solo cuando está por vencer."""
        if self._token and time.monotonic() < self._token_vence_en:
            return self._token

        destino = f"{self._url}/api/v1/auth/universal-auth/login"
        try:
            respuesta = self._obtener_sesion().post(
                destino,
                json={"clientId": self._client_id, "clientSecret": self._client_secret},
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise InfisicalError(
                f"No se pudo contactar el servidor Infisical en {self._url}: {e}"
            ) from e

        if respuesta.status_code != 200:
            raise InfisicalError(
                f"Autenticación Infisical rechazada (HTTP {respuesta.status_code}). "
                f"Verifique INFISICAL_CLIENT_ID / INFISICAL_CLIENT_SECRET."
            )

        try:
            cuerpo = respuesta.json()
            token = cuerpo["accessToken"]
        except (ValueError, KeyError) as e:
            raise InfisicalError(
                "Respuesta de autenticación Infisical con formato inesperado."
            ) from e

        vigencia = cuerpo.get("expiresIn") or _VIGENCIA_TOKEN_DEFECTO_SEG
        try:
            vigencia = int(vigencia)
        except (TypeError, ValueError):
            vigencia = _VIGENCIA_TOKEN_DEFECTO_SEG

        self._token = token
        self._token_vence_en = time.monotonic() + max(vigencia - _MARGEN_RENOVACION_SEG, 30)

        logger.info(
            "Infisical: autenticación exitosa (entorno=%s, vigencia=%ss).",
            self._environment,
            vigencia,
        )
        return token

    def _descargar_secretos(self) -> dict[str, str]:
        """Descarga todos los secretos del proyecto/entorno/ruta configurados."""
        token = self._obtener_token()
        destino = f"{self._url}/api/v3/secrets/raw"
        parametros = {
            "workspaceId": self._project_id,
            "environment": self._environment,
            "secretPath": self._secret_path,
            "include_imports": "true",
            "recursive": "true" if self._recursivo else "false",
        }

        try:
            respuesta = self._obtener_sesion().get(
                destino,
                headers={"Authorization": f"Bearer {token}"},
                params=parametros,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise InfisicalError(f"Error descargando secretos desde Infisical: {e}") from e

        # Token revocado o expirado antes de tiempo: reintentar una vez con token nuevo.
        if respuesta.status_code in (401, 403):
            logger.info("Infisical: token rechazado, renovando y reintentando.")
            self._token = None
            self._token_vence_en = 0.0
            token = self._obtener_token()
            try:
                respuesta = self._obtener_sesion().get(
                    destino,
                    headers={"Authorization": f"Bearer {token}"},
                    params=parametros,
                    timeout=self._timeout,
                )
            except requests.RequestException as e:
                raise InfisicalError(f"Error descargando secretos desde Infisical: {e}") from e

        if respuesta.status_code != 200:
            raise InfisicalError(
                f"Infisical respondió HTTP {respuesta.status_code} al listar secretos "
                f"(proyecto={self._project_id}, entorno={self._environment}, "
                f"ruta={self._secret_path})."
            )

        try:
            cuerpo = respuesta.json()
        except ValueError as e:
            raise InfisicalError("Respuesta de secretos Infisical no es JSON válido.") from e

        secretos: dict[str, str] = {}

        # Los secretos importados desde otras rutas se aplican primero para que
        # los definidos directamente en la ruta actual tengan prioridad.
        for importado in cuerpo.get("imports") or []:
            for secreto in importado.get("secrets") or []:
                clave = secreto.get("secretKey")
                if clave:
                    secretos[clave] = secreto.get("secretValue") or ""

        for secreto in cuerpo.get("secrets") or []:
            clave = secreto.get("secretKey")
            if clave:
                secretos[clave] = secreto.get("secretValue") or ""

        logger.info(
            "Infisical: %s secretos cargados (proyecto=%s, entorno=%s, ruta=%s).",
            len(secretos),
            self._project_id,
            self._environment,
            self._secret_path,
        )
        return secretos

    # ── interfaz pública ─────────────────────────────────────────────────────

    def obtener_secretos(self, forzar: bool = False) -> dict[str, str]:
        """
        Diccionario de secretos, servido desde caché mientras esté vigente.
        Una sola descarga por TTL aunque varios hilos lo pidan a la vez.
        """
        if not forzar and self._secretos is not None and time.monotonic() < self._secretos_vencen_en:
            return self._secretos

        with self._lock:
            # Otro hilo pudo refrescar el caché mientras esperábamos el lock.
            if not forzar and self._secretos is not None and time.monotonic() < self._secretos_vencen_en:
                return self._secretos

            faltantes = self.variables_faltantes()
            if faltantes:
                raise InfisicalError(
                    "Configuración de Infisical incompleta. Variables faltantes: "
                    f"{', '.join(faltantes)}."
                )

            secretos = self._descargar_secretos()
            self._secretos = secretos
            self._secretos_vencen_en = time.monotonic() + self._ttl
            return secretos

    def obtener(self, nombre: str, defecto: str | None = None) -> str | None:
        """Valor de un secreto puntual. Devuelve `defecto` si no existe."""
        valor = self.obtener_secretos().get(nombre)
        return defecto if _valor_vacio(valor) else valor

    def invalidar_cache(self):
        """Descarta secretos y token cacheados; la próxima lectura recarga."""
        with self._lock:
            self._secretos = None
            self._secretos_vencen_en = 0.0
            self._token = None
            self._token_vence_en = 0.0
        logger.info("Infisical: caché de secretos invalidado.")

    def estado(self) -> dict:
        """Diagnóstico para el endpoint de salud. No expone valores de secretos."""
        faltantes = self.variables_faltantes()
        vigente = self._secretos is not None and time.monotonic() < self._secretos_vencen_en
        return {
            "configurado": not faltantes,
            "variables_faltantes": faltantes,
            "url": self._url or None,
            "entorno": self._environment,
            "ruta": self._secret_path,
            "cache_vigente": vigente,
            "secretos_en_cache": len(self._secretos) if self._secretos else 0,
            "cache_ttl_seg": self._ttl,
            "segundos_para_expirar": (
                round(self._secretos_vencen_en - time.monotonic(), 1) if vigente else 0
            ),
        }


# Instancia única compartida por todo el Dashboard.
proveedor = _ProveedorInfisical()


def obtener_secreto(nombre: str, defecto: str | None = None) -> str | None:
    """Atajo para leer un secreto individual desde el proveedor compartido."""
    return proveedor.obtener(nombre, defecto)


def infisical_configurado() -> bool:
    """True si el entorno tiene lo necesario para consultar Infisical."""
    return proveedor.esta_configurado()


def invalidar_cache_secretos():
    """Fuerza la recarga de secretos en la próxima lectura."""
    proveedor.invalidar_cache()


def estado_infisical() -> dict:
    """Diagnóstico del proveedor, apto para exponer en /admin/db-status."""
    return proveedor.estado()
