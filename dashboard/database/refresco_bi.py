"""
refresco_bi.py — Mantiene al día las vistas materializadas del Dashboard BI
═══════════════════════════════════════════════════════════════════════════
Una vista materializada no se actualiza sola. Si nadie la refresca, el tablero
responde rápido pero con datos congelados, que es la peor forma de fallar
porque no avisa. Los procesos que cargan las tablas de origen corren fuera de
este repositorio y el servidor no tiene pg_cron, así que el refresco lo gobierna
la propia aplicación.

Este módulo es genérico: sirve a cualquier visualización del dashboard, no solo
a Kilómetros. Cada modelo declara su materializada una vez y el vigilante se
encarga del resto.

── Cómo lo usa una visualización nueva ──────────────────────────────────────

En el modelo (dashboard/model/{área}/bi_{nombre}.py), al final del archivo:

    from dashboard.database.refresco_bi import registrar_materializada

    registrar_materializada(
        nombre="bi_mi_indicador",
        objeto="pl_x01_esquema.bi_mi_indicador",   # la materializada
        origen="pl_x01_esquema.tabla_origen",      # tabla que llena el ETL
        columna_sello="fecha_hora_carga",          # marca de tiempo de carga
        al_refrescar=limpiar_cache,                # opcional: invalidar cachés
    )

El vigilante compara el sello de la tabla de origen con el de la materializada
y solo refresca cuando hay datos nuevos. El chequeo son dos MAX() indexados
(milisegundos); el refresco completo depende del volumen.

── Requisitos de la materializada ───────────────────────────────────────────

Debe tener un índice ÚNICO para admitir REFRESH ... CONCURRENTLY, que es lo que
evita bloquear las lecturas mientras se recarga:

    CREATE UNIQUE INDEX <nombre>_pk ON <objeto> (<clave>);

── Variables de entorno ─────────────────────────────────────────────────────

  BI_REFRESCO_AUTOMATICO   true/false (por defecto: true)
  BI_REFRESCO_INTERVALO_SEG  cada cuánto comprobar (por defecto: 900)
"""

import os
import time
import logging
import threading
from typing import Callable

import psycopg2

from .database_manager import get_plata_connection, get_plata_connection_rw, hay_credenciales_escritura

logger = logging.getLogger("dashboard.refresco_bi")

INTERVALO_CHEQUEO_SEG = int(os.getenv("BI_REFRESCO_INTERVALO_SEG", "900"))
REFRESCO_HABILITADO = (os.getenv("BI_REFRESCO_AUTOMATICO", "true").strip().lower()
                       not in {"0", "false", "no", "n"})

_registro: dict[str, dict] = {}
_registro_lock = threading.Lock()
_vigilante_iniciado = False


def registrar_materializada(
    nombre: str,
    objeto: str,
    origen: str,
    columna_sello: str = "fecha_hora_carga",
    al_refrescar: Callable[[], None] | None = None,
    funcion_refresco: str | None = None,
) -> None:
    """
    Inscribe una materializada en el vigilante. Idempotente: volver a registrar
    el mismo nombre actualiza la definición en lugar de duplicarla.
    """
    with _registro_lock:
        _registro[nombre] = {
            "objeto": objeto,
            "origen": origen,
            "columna_sello": columna_sello,
            "al_refrescar": al_refrescar,
            "funcion_refresco": funcion_refresco,
            "activo": True,
            "motivo": None,
            "lock": _registro.get(nombre, {}).get("lock") or threading.Lock(),
            "estado": _registro.get(nombre, {}).get("estado", "sin ejecutar"),
            "ultimo_refresco": _registro.get(nombre, {}).get("ultimo_refresco"),
            "duracion_seg": _registro.get(nombre, {}).get("duracion_seg"),
        }
    logger.info("BI: materializada '%s' registrada para refresco automático.", nombre)

def materializadas_registradas() -> list[str]:
    with _registro_lock:
        return sorted(_registro)

def _sellos(entrada: dict) -> tuple:
    """(sello del origen, sello de la materializada). None si están vacíos."""
    columna = entrada["columna_sello"]
    with get_plata_connection() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                f"SELECT (SELECT MAX({columna}) FROM {entrada['origen']}),"
                f"       (SELECT MAX({columna}) FROM {entrada['objeto']})"
            )
            return cursor.fetchone()

def _desactivar(nombre: str, motivo: str) -> None:
    """
    Marca una materializada como no refrescable desde la aplicación.

    La falta de permisos no se arregla reintentando: se avisa una sola vez, con
    la indicación de qué hacer, y el vigilante deja de intentarlo. El tablero
    sigue funcionando —solo deja de auto-actualizarse— y `estado_refresco()`
    permite ver la situación en cualquier momento.
    """
    with _registro_lock:
        ya_avisado = _registro.get(nombre, {}).get("estado") == "sin permisos"
        if nombre in _registro:
            _registro[nombre].update({"estado": "sin permisos", "motivo": motivo, "activo": False})
    if not ya_avisado:
        objeto = _registro.get(nombre, {}).get("objeto", nombre)
        logger.error(
            "BI: no se puede refrescar '%s' (%s).\n"
            "     El refresco usa el rol de mantenimiento (CAPA_PLATA_USER_RW), que debe ser\n"
            "     dueño de la materializada: REFRESH MATERIALIZED VIEW no admite otro permiso.\n"
            "     Qué revisar:\n"
            "       1) Que CAPA_PLATA_USER_RW y CAPA_PLATA_PASSWORD_RW estén publicados y sean\n"
            "          del rol dueño de %s.\n"
            "       2) Si no, que el proceso de carga ejecute al terminar:\n"
            "          REFRESH MATERIALIZED VIEW CONCURRENTLY %s;\n"
            "     Mientras tanto el tablero funciona, pero con los datos de la última carga.",
            nombre, motivo, objeto, objeto,
        )

def _habilitar_escritura(cursor, nombre: str) -> bool:
    """
    Levanta el modo solo lectura para esta sesión, si el rol lo permite.

    Devuelve False —sin lanzar— cuando la política del servidor no deja
    levantarlo: en ese caso el refresco no es posible desde la aplicación y debe
    hacerlo un rol con escritura o el proceso que carga los datos.
    """
    try:
        cursor.execute("SELECT current_setting('default_transaction_read_only')")
        if (cursor.fetchone() or ["off"])[0] != "on":
            return True   # el rol ya puede escribir
        cursor.execute("SET SESSION default_transaction_read_only = off")
        cursor.execute("SELECT current_setting('default_transaction_read_only')")
        if (cursor.fetchone() or ["on"])[0] == "on":
            return False
        logger.info("BI: escritura habilitada en la sesión para refrescar '%s'.", nombre)
        return True
    except Exception as error:
        logger.warning("BI: no se pudo habilitar escritura para '%s': %s", nombre, error)
        return False

def refrescar(nombre: str, forzar: bool = False) -> dict:
    """Refresca una materializada concreta si su origen tiene datos más nuevos."""
    with _registro_lock:
        entrada = _registro.get(nombre)
    if entrada is None:
        return {"estado": "desconocida", "mensaje": f"'{nombre}' no está registrada."}

    if not entrada.get("activo", True) and not forzar:
        return {"estado": "sin permisos", "mensaje": entrada.get("motivo") or "desactivado"}

    if not entrada["lock"].acquire(blocking=False):
        return {"estado": "en curso", "mensaje": "Ya hay un refresco ejecutándose."}

    try:
        origen, materializada = _sellos(entrada)
        if origen is None:
            return {"estado": "omitido", "mensaje": f"{entrada['origen']} está vacía."}
        if not forzar and materializada is not None and origen <= materializada:
            return {
                "estado": "al día",
                "sello_datos": str(materializada),
                "mensaje": "La materializada ya refleja la última carga.",
            }

        inicio = time.perf_counter()
        # El refresco escribe, así que va por el rol de mantenimiento; las
        # lecturas del tablero siguen usando el rol de solo lectura.
        with get_plata_connection_rw() as conexion:
            # REFRESH ... CONCURRENTLY no admite ejecutarse dentro de una transacción.
            previo = conexion.autocommit
            conexion.autocommit = True
            escritura_habilitada = False
            try:
                with conexion.cursor() as cursor:
                    # El rol del dashboard es de solo lectura por política
                    # (default_transaction_read_only = on sobre el rol y la base).
                    # El refresco es la única operación que necesita escribir, así
                    # que se pide el permiso acotado a esta sesión y se devuelve al
                    # terminar: la conexión vuelve al pool tal como estaba.
                    escritura_habilitada = _habilitar_escritura(cursor, nombre)
                    if not escritura_habilitada:
                        _desactivar(nombre, "el rol no permite escribir en esta base")
                        return {
                            "estado": "sin permiso de escritura",
                            "mensaje": (
                                "El rol de base de datos es de solo lectura y no permite "
                                "levantar la restricción en la sesión."
                            ),
                        }
                    # Si el administrador publicó una función SECURITY DEFINER para
                    # el refresco, se usa: es la vía que no exige ser dueño.
                    if entrada.get("funcion_refresco"):
                        cursor.execute(f"SELECT {entrada['funcion_refresco']}(%s)", [entrada["objeto"]])
                    else:
                        cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {entrada['objeto']}")
                    cursor.execute(f"ANALYZE {entrada['objeto']}")
            finally:
                # Esta conexión no vuelve a ningún pool (se cierra al salir del
                # contexto), así que no hay estado de sesión que restaurar.
                conexion.autocommit = previo
        duracion = round(time.perf_counter() - inicio, 1)

        # Los cachés del modelo suelen llevar el sello de carga en la clave:
        # al entrar datos nuevos hay que descartar lo calculado con el anterior.
        if entrada["al_refrescar"]:
            try:
                entrada["al_refrescar"]()
            except Exception:
                logger.warning("BI: fallo al invalidar cachés de '%s'.", nombre, exc_info=True)

        with _registro_lock:
            _registro[nombre].update({
                "estado": "refrescado",
                "ultimo_refresco": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duracion_seg": duracion,
            })
        logger.info("BI: '%s' refrescada en %.1f s (datos hasta %s).", nombre, duracion, origen)
        return {
            "estado": "refrescado",
            "sello_anterior": str(materializada) if materializada else None,
            "sello_datos": str(origen),
            "duracion_seg": duracion,
        }

    except (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.ReadOnlySqlTransaction) as error:
        # Falta de permisos: es una condición de configuración, no un fallo
        # transitorio. Reintentar cada pocos minutos solo llenaría la terminal
        # con el mismo traceback, así que se avisa una vez y se deja de intentar.
        _desactivar(nombre, str(error).strip().splitlines()[0] if str(error).strip() else "permisos insuficientes")
        return {"estado": "sin permisos", "mensaje": str(error).strip()}

    except Exception as error:
        logger.exception("BI: falló el refresco de '%s'", nombre)
        with _registro_lock:
            _registro[nombre].update({"estado": "error"})
        return {"estado": "error", "mensaje": str(error)}
    finally:
        entrada["lock"].release()

def refrescar_todas(forzar: bool = False) -> dict[str, dict]:
    """Recorre todas las materializadas registradas."""
    return {nombre: refrescar(nombre, forzar) for nombre in materializadas_registradas()}

def _bucle():
    """Comprueba periódicamente si entró carga nueva. Nunca propaga excepciones."""
    while True:
        try:
            refrescar_todas()
        except Exception:
            logger.exception("BI: error no controlado en el ciclo de refresco.")
        time.sleep(INTERVALO_CHEQUEO_SEG)

def iniciar_refresco_automatico() -> None:
    """Arranca el vigilante en un hilo daemon. Seguro llamarlo más de una vez."""
    global _vigilante_iniciado
    if not REFRESCO_HABILITADO:
        logger.info("BI: refresco automático deshabilitado por BI_REFRESCO_AUTOMATICO.")
        return
    with _registro_lock:
        if _vigilante_iniciado:
            return
        _vigilante_iniciado = True
    threading.Thread(target=_bucle, name="bi-refresco-materializadas", daemon=True).start()
    logger.info(
        "BI: vigilante activo cada %s s sobre %s materializada(s).",
        INTERVALO_CHEQUEO_SEG,
        len(_registro),
    )

def estado_refresco() -> dict:
    """Diagnóstico de todas las materializadas, apto para un endpoint de estado."""
    resultado = {
        "habilitado": REFRESCO_HABILITADO,
        "intervalo_seg": INTERVALO_CHEQUEO_SEG,
        "materializadas": {},
    }
    with _registro_lock:
        nombres = list(_registro)
    for nombre in nombres:
        with _registro_lock:
            entrada = dict(_registro[nombre])
        detalle = {
            "objeto": entrada["objeto"],
            "origen": entrada["origen"],
            "estado": entrada["estado"],
            "activo": entrada.get("activo", True),
            "motivo": entrada.get("motivo"),
            "ultimo_refresco": entrada["ultimo_refresco"],
            "duracion_seg": entrada["duracion_seg"],
        }
        try:
            origen, materializada = _sellos(entrada)
            detalle.update({
                "sello_origen": str(origen) if origen else None,
                "sello_materializada": str(materializada) if materializada else None,
                "al_dia": bool(origen and materializada and origen <= materializada),
            })
        except Exception as error:
            detalle["error"] = str(error)
        resultado["materializadas"][nombre] = detalle
    return resultado
