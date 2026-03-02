from datetime import datetime
from fastapi import HTTPException
from model.database_manager import get_db_connection

def fecha_asignacion(fecha_str):
    try:
        fecha = datetime.strptime(fecha_str, "%d/%m/%Y")
        if fecha < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
            raise HTTPException(status_code=400, detail="La fecha debe ser el día actual o una fecha futura.")
        return fecha_str
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use DD/MM/YYYY.")

def puestos_SC():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT puestos FROM controles WHERE concesion = 'SAN CRISTÓBAL'")
            return [row[0] for row in cursor.fetchall()]

def puestos_UQ():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT puestos FROM controles WHERE concesion = 'USAQUÉN'")
            return [row[0] for row in cursor.fetchall()]

def concesion():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT concesion FROM controles")
            return [row[0] for row in cursor.fetchall()]

def control(concesion_seleccionada, puestos_seleccionado):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT control FROM controles
                WHERE concesion = %s AND puestos = %s
            """, (concesion_seleccionada, puestos_seleccionado))
            return [row[0] for row in cursor.fetchall()]

def rutas(concesion, puestos, control):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT ruta FROM controles
                WHERE concesion = %s AND puestos = %s AND control = %s
            """, (concesion, puestos, control))
            rutas_list = [row[0] for row in cursor.fetchall()]

    # Ordenar y unir en cadena separada por comas (procesamiento fuera de la conexión)
    return ", ".join(sorted(rutas_list)) if rutas_list else ""

def turnos():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT turno FROM turnos")
            return [row[0] for row in cursor.fetchall()]

def turno_descripcion(turno):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT descripcion FROM turnos WHERE turno = %s", (turno,))
            descripcion = cursor.fetchone()
            return descripcion[0] if descripcion else None

def hora_inicio(turno):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT hora_inicio FROM turnos WHERE turno = %s", (turno,))
            return cursor.fetchone()[0]

def hora_fin(turno):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT hora_fin FROM turnos WHERE turno = %s", (turno,))
            return cursor.fetchone()[0]