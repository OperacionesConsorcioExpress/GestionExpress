import time
from datetime import datetime
from fastapi import HTTPException
from database.database_manager import get_db_connection
# ─────────────────────────────────────────────────────────────────────────────
# CACHÉ EN MEMORIA  (TTL = 5 minutos)
# Se invalida automáticamente al vencer. Para forzar la invalidación tras un
# cargue masivo, llama a  invalidar_cache_parametrizacion()  desde el endpoint
# de confirmar_cargue.
# ─────────────────────────────────────────────────────────────────────────────
_CACHE_TTL = 300          # segundos
_cache_parametrizacion = {"data": None, "ts": 0.0}

def invalidar_cache_parametrizacion():
    """Fuerza la recarga de datos la próxima vez que se consulten."""
    _cache_parametrizacion["data"] = None
    _cache_parametrizacion["ts"] = 0.0

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL CONSOLIDADA
# ─────────────────────────────────────────────────────────────────────────────
def get_datos_parametrizacion() -> dict:
    """
    Retorna en una sola conexión:
    {
    "puestos_SC":  [int, ...],
    "puestos_UQ":  [int, ...],
    "concesiones": [str, ...],
    "turnos":      [{"turno": str, "hora_inicio": str, "hora_fin": str, "detalles": str}, ...]
    }

    Resultado cacheado 5 minutos. Compatible con el endpoint
    GET /api/datos_iniciales del router.
    """
    ahora = time.time()
    if _cache_parametrizacion["data"] and (ahora - _cache_parametrizacion["ts"]) < _CACHE_TTL:
        return _cache_parametrizacion["data"]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:

            # 1. Puestos SAN CRISTÓBAL
            cursor.execute(
                "SELECT DISTINCT puestos FROM controles "
                "WHERE concesion = 'SAN CRISTÓBAL' ORDER BY puestos"
            )
            p_sc = [row[0] for row in cursor.fetchall()]

            # 2. Puestos USAQUÉN
            cursor.execute(
                "SELECT DISTINCT puestos FROM controles "
                "WHERE concesion = 'USAQUÉN' ORDER BY puestos"
            )
            p_uq = [row[0] for row in cursor.fetchall()]

            # 3. Concesiones únicas
            cursor.execute(
                "SELECT DISTINCT concesion FROM controles ORDER BY concesion"
            )
            concesiones = [row[0] for row in cursor.fetchall()]

            # 4. Turnos completos (turno + horario + detalles)
            cursor.execute(
                "SELECT DISTINCT turno, hora_inicio, hora_fin, detalles "
                "FROM turnos ORDER BY turno"
            )
            turnos_list = [
                {
                    "turno":       row[0],
                    "hora_inicio": str(row[1]) if row[1] else "",
                    "hora_fin":    str(row[2]) if row[2] else "",
                    "detalles":    row[3] or "",
                }
                for row in cursor.fetchall()
            ]

    resultado = {
        "puestos_SC":  p_sc,
        "puestos_UQ":  p_uq,
        "concesiones": concesiones,
        "turnos":      turnos_list,
    }

    _cache_parametrizacion["data"] = resultado
    _cache_parametrizacion["ts"]   = ahora
    return resultado

# ─────────────────────────────────────────────────────────────────────────────
# CONSULTAS CON PARÁMETROS DE USUARIO (no cacheables globalmente)
# ─────────────────────────────────────────────────────────────────────────────
def control(concesion_seleccionada: str, puestos_seleccionado: str) -> list:
    """Lista de controles disponibles para una concesión + número de puestos."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT control
                FROM controles
                WHERE concesion = %s AND puestos = %s
                ORDER BY control
                """,
                (concesion_seleccionada, puestos_seleccionado),
            )
            return [row[0] for row in cursor.fetchall()]

def rutas(concesion_param: str, puestos_param: str, control_param: str) -> str:
    """
    Rutas asociadas a una combinación concesión + puestos + control.
    Retorna cadena separada por comas, ordenada alfabéticamente.
    El ORDER BY en la query evita el sort en Python.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT ruta
                FROM controles
                WHERE concesion = %s AND puestos = %s AND control = %s
                ORDER BY ruta
                """,
                (concesion_param, puestos_param, control_param),
            )
            rutas_list = [row[0] for row in cursor.fetchall()]

    return ", ".join(rutas_list) if rutas_list else ""

def get_turno_horario(turno_param: str) -> dict:
    """
    Retorna {"inicio": "HH:MM:SS", "fin": "HH:MM:SS"} en 1 sola conexión.
    Reemplaza hora_inicio() + hora_fin() (eran 2 conexiones separadas).
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT hora_inicio, hora_fin FROM turnos WHERE turno = %s LIMIT 1",
                (turno_param,),
            )
            row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Turno '{turno_param}' no encontrado.")

    return {
        "inicio": str(row[0]) if row[0] else "",
        "fin":    str(row[1]) if row[1] else "",
    }

def turno_descripcion(turno_param: str):
    """Descripción de un turno. Sin cambios respecto a la versión anterior."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT detalles FROM turnos WHERE turno = %s LIMIT 1",
                (turno_param,),
            )
            row = cursor.fetchone()
    return row[0] if row else None

def fecha_asignacion(fecha_str: str) -> str:
    """Valida que la fecha sea actual o futura. Sin cambios."""
    try:
        fecha = datetime.strptime(fecha_str, "%d/%m/%Y")
        if fecha < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
            raise HTTPException(
                status_code=400,
                detail="La fecha debe ser el día actual o una fecha futura.",
            )
        return fecha_str
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Formato de fecha inválido. Use DD/MM/YYYY.",
        )

# ─────────────────────────────────────────────────────────────────────────────
# ALIASES DE COMPATIBILIDAD  (mantienen los nombres anteriores como wrappers)
# Los endpoints del router que todavía usan los nombres viejos siguen
# funcionando sin tocar route_asigna_ccz.py en esta primera iteración.
# ─────────────────────────────────────────────────────────────────────────────
def puestos_SC() -> list:
    """Wrapper de compatibilidad → get_datos_parametrizacion()."""
    return get_datos_parametrizacion()["puestos_SC"]

def puestos_UQ() -> list:
    """Wrapper de compatibilidad → get_datos_parametrizacion()."""
    return get_datos_parametrizacion()["puestos_UQ"]

def concesion() -> list:
    """Wrapper de compatibilidad → get_datos_parametrizacion()."""
    return get_datos_parametrizacion()["concesiones"]

def turnos() -> list:
    """
    Wrapper de compatibilidad.
    Retorna solo los nombres de turno (igual que antes).
    """
    return [t["turno"] for t in get_datos_parametrizacion()["turnos"]]

def hora_inicio(turno_param: str) -> str:
    """Wrapper de compatibilidad → get_turno_horario()."""
    return get_turno_horario(turno_param)["inicio"]

def hora_fin(turno_param: str) -> str:
    """Wrapper de compatibilidad → get_turno_horario()."""
    return get_turno_horario(turno_param)["fin"]