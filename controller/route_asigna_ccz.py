from database.database_manager import get_db_connection
from fastapi import APIRouter, Request, Form, Depends, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List
from io import BytesIO
# Importar la función para asignar controles y generar reportes
from lib.verifcar_clave import check_user
from lib.asignar_controles import (
    fecha_asignacion, puestos_SC, puestos_UQ, concesion,
    control, rutas, turnos, hora_inicio, hora_fin
)
from lib.cargues import ProcesarCargueControles
from model.gestion_asigna_ccz import Cargue_Controles, Cargue_Asignaciones, Reporte_Asignaciones

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
router_asigna_ccz = APIRouter(tags=["asignaciones_ccz"])
templates = Jinja2Templates(directory="./view")
# ─────────────────────────────────────────────
# Obtenemos la sesión del usuario desde la cookie
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────
# Instancias de las clases de gestión de asignaciones y reportes
# ─────────────────────────────────────────────
cargue_asignaciones = Cargue_Asignaciones()
reporte_asignaciones = Reporte_Asignaciones()

# Caché en memoria como diccionario — igual que en main.py
cache = {}

# =====================================================================
# PANTALLA DE ASIGNACIÓN
# =====================================================================
@router_asigna_ccz.get("/asignacion", response_class=HTMLResponse)
def asignacion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("asignacion.html", {"request": req, "user_session": user_session})

@router_asigna_ccz.post("/asignacion", response_class=HTMLResponse)
def asignacion_post(req: Request, username: str = Form(...), password_user: str = Form(...)):
    verify = check_user(username, password_user)
    if verify:
        return templates.TemplateResponse("asignacion.html", {"request": req, "data_user": verify})
    else:
        error_message = "Por favor valide sus credenciales y vuelva a intentar."
        return templates.TemplateResponse("index.html", {"request": req, "error_message": error_message})

# =====================================================================
# CARGUES MASIVOS DE PLANTA Y CONTROLES
# =====================================================================
# Clase para manejar el request de confirmación de cargue
class ConfirmarCargueRequest(BaseModel):
    session_id: str

@router_asigna_ccz.post("/cargar_archivo/")
async def cargar_archivo(file: UploadFile = File(...)):
    procesador = ProcesarCargueControles(file)
    preliminar = procesador.leer_archivo()

    # Generar una clave única para el usuario/sesión (simulación de UUID)
    session_id = str(len(cache) + 1)
    cache[session_id] = preliminar

    print(f"Archivo cargado correctamente. Session ID: {session_id}")
    return {"session_id": session_id, "preliminar": preliminar}

@router_asigna_ccz.post("/confirmar_cargue/")
async def confirmar_cargue(data: dict):
    session_id = data.get("session_id")
    if session_id not in cache:
        raise HTTPException(status_code=404, detail="Sesión no encontrada")

    # Obtener datos preliminares desde la caché
    preliminar = cache[session_id]

    # Filtrar datos según lo seleccionado por el usuario
    hojas_a_cargar = {}

    if data.get("tcz"):
        hojas_a_cargar['planta'] = preliminar.get('planta')

    if data.get("supervisores"):
        hojas_a_cargar['supervisores'] = preliminar.get('supervisores')

    if data.get("turnos"):
        hojas_a_cargar['turnos'] = preliminar.get('turnos')

    if data.get("controles"):
        hojas_a_cargar['controles'] = preliminar.get('controles')

    if not hojas_a_cargar:
        raise HTTPException(status_code=400, detail="Debe seleccionar al menos una hoja para cargar.")

    # Enviar las hojas seleccionadas a la base de datos
    try:
        cargador = Cargue_Controles()
        cargador.cargar_datos(hojas_a_cargar)
        return {"message": "Datos cargados exitosamente."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al cargar los datos: {str(e)}")

# Plantilla de cargue de planta activa y controles
@router_asigna_ccz.get("/plantilla_cargue")
async def descargar_plantilla():
    file_path = "./cargues/asignaciones_tecnicos.xlsx"
    return FileResponse(path=file_path, filename="asignaciones_tecnicos.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# =====================================================================
# FUNCIONALIDADES PARA GESTIONAR LAS ASIGNACIONES "asignar_controles.py"
# =====================================================================
def _row(r, *keys):
    """Extrae valores de una fila: soporta tupla (índice) o RealDictRow (clave)."""
    if isinstance(r, dict):
        return [r[k] for k in keys]
    return [r[i] for i in range(len(keys))]

def get_planta_data():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT cedula, nombre FROM planta")
            rows = cursor.fetchall()
    return [{"cedula": v[0], "nombre": v[1]} for r in rows for v in [_row(r, "cedula", "nombre")]]

def get_supervisores_data():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT cedula, nombre FROM supervisores")
            rows = cursor.fetchall()
    return [{"cedula": v[0], "nombre": v[1]} for r in rows for v in [_row(r, "cedula", "nombre")]]

def get_turnos_data():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT turno, hora_inicio, hora_fin, detalles FROM turnos")
            rows = cursor.fetchall()
    return [{"turno": v[0], "hora_inicio": v[1], "hora_fin": v[2], "detalles": v[3]}
            for r in rows for v in [_row(r, "turno", "hora_inicio", "hora_fin", "detalles")]]

@router_asigna_ccz.get("/api/planta", response_class=JSONResponse)
def api_planta():
    data = get_planta_data()
    return data

@router_asigna_ccz.get("/api/supervisores", response_class=JSONResponse)
def api_supervisores():
    data = get_supervisores_data()
    return data

@router_asigna_ccz.get("/api/fecha_asignacion")
async def api_fecha_asignacion(fecha: str):
    return fecha_asignacion(fecha)

@router_asigna_ccz.get("/api/puestos_SC")
async def api_puestos_SC():
    return puestos_SC()

@router_asigna_ccz.get("/api/puestos_UQ")
async def api_puestos_UQ():
    return puestos_UQ()

@router_asigna_ccz.get("/api/concesion")
async def api_concesion():
    return concesion()

@router_asigna_ccz.get("/api/control")
async def get_control(concesion: str, puestos: str):
    controles = control(concesion, puestos)
    return JSONResponse(content=controles)

@router_asigna_ccz.get("/api/rutas")
async def get_rutas(concesion: str, puestos: str, control: str):
    rutas_asociadas = rutas(concesion, puestos, control)
    return {"rutas": rutas_asociadas}

@router_asigna_ccz.get("/api/turnos")
async def get_turnos():
    return turnos()

@router_asigna_ccz.get("/api/turnos", response_class=JSONResponse)
def api_turnos():
    data = get_turnos_data()
    return data

@router_asigna_ccz.get("/api/turno_descripcion")
def turno_descripcion(turno: str):
    return {"descripcion": turno_descripcion(turno)}

@router_asigna_ccz.get("/api/hora_inicio")
async def get_hora_inicio(turno: str):
    return {"inicio": hora_inicio(turno)}

@router_asigna_ccz.get("/api/hora_fin")
async def get_hora_fin(turno: str):
    return {"fin": hora_fin(turno)}

# =====================================================================
# GUARDAR ASIGNACIONES EN GRILLA
# =====================================================================
class AsignacionRequest(BaseModel):
    fecha: str
    cedula: str
    nombre: str
    turno: str
    hora_inicio: str
    hora_fin: str
    concesion: str
    control: str
    rutas_asociadas: str
    observaciones: str

@router_asigna_ccz.post("/api/guardar_asignaciones")
async def guardar_asignaciones(request: Request, user_session: dict = Depends(get_user_session)):
    try:
        data = await request.json() # Obtener datos del cuerpo de la solicitud
        processed_data = cargue_asignaciones.procesar_asignaciones(data, user_session) # Procesar, cargar asignaciones y Pasar user_session
        cargue_asignaciones.cargar_asignaciones(processed_data)
        return {"message": "Asignaciones guardadas exitosamente."} # Retornar mensaje de éxito
    except Exception as e:
        error_message = f"Error al guardar asignaciones: {str(e)}" # Manejar errores y retornar mensaje de error
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================================
# DASHBOARD Y REPORTES PARA CENTRO DE CONTROL
# =====================================================================
# Ruta para el dashboard principal
@router_asigna_ccz.get("/dashboard", response_class=HTMLResponse)
async def dashboard(req: Request, modal: bool = False, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    filtros = reporte_asignaciones.obtener_filtros_unicos()
    return templates.TemplateResponse("dashboard.html", {
        "request": req,
        "user_session": user_session,
        "modal": modal,
        **filtros
    })

@router_asigna_ccz.get("/filtrar_asignaciones", response_class=HTMLResponse)
async def filtrar_asignaciones(request: Request, fechaInicio: str, fechaFin: str, cedulaTecnico: str = None, nombreTecnico: str = None, turno: str = None, concesion: str = None, control: str = None, ruta: str = None, linea: str = None, cop: str = None, usuarioRegistra: str = None, nombreSupervisorEnlace: str = None):
    filtros = {
        "fecha_inicio": fechaInicio,
        "fecha_fin": fechaFin,
        "cedula": cedulaTecnico,
        "nombre": nombreTecnico,
        "turno": turno,
        "concesion": concesion,
        "control": control,
        "ruta": ruta,
        "linea": linea,
        "cop": cop,
        "registrado_por": usuarioRegistra,
        "nombre_supervisor_enlace": nombreSupervisorEnlace
    }
    
    asignaciones = reporte_asignaciones.obtener_asignaciones(**filtros)
    return templates.TemplateResponse("dashboard.html", {"request": request, "asignaciones": asignaciones, **filtros})

@router_asigna_ccz.post("/buscar_asignaciones")
async def buscar_asignaciones(request: Request):
    # Recibir los datos de los filtros desde el frontend
    filtros = await request.json()
    
    # Crear una instancia de Reporte_Asignaciones
    reporte = Reporte_Asignaciones()

    # Obtener las asignaciones utilizando los filtros
    asignaciones = reporte.obtener_asignaciones(
        fecha_inicio=filtros.get('fechaInicio'),
        fecha_fin=filtros.get('fechaFin'),
        cedula=filtros.get('cedulaTecnico'),
        nombre=filtros.get('nombreTecnico'),
        turno=filtros.get('turno'),
        concesion=filtros.get('concesion'),
        control=filtros.get('control'),
        ruta=filtros.get('ruta'),
        linea=filtros.get('linea'),
        cop=filtros.get('cop'),
        registrado_por=filtros.get('usuarioRegistra'),
        nombre_supervisor_enlace=filtros.get('nombreSupervisorEnlace')
    )
    # Depuración para verificar la salida
    #print(asignaciones)
    # Devolver las asignaciones como JSON para que el frontend las maneje
    return JSONResponse(content=asignaciones)

@router_asigna_ccz.post("/descargar_xlsx")
async def descargar_xlsx(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    xlsx_file = reporte.generar_xlsx(filtros)
    # Usar StreamingResponse para devolver el archivo
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.xlsx"'
    }
    return StreamingResponse(xlsx_file, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

@router_asigna_ccz.post("/descargar_csv")
async def descargar_csv(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    csv_file = reporte.generar_csv(filtros)
    
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.csv"'
    }
    return StreamingResponse(csv_file, media_type="text/csv", headers=headers)

@router_asigna_ccz.post("/descargar_json")
async def descargar_json(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    json_data = reporte.generar_json(filtros)
    
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.json"'
    }
    return JSONResponse(content=json_data, headers=headers)

# =====================================================================
# CONSULTA DE ASIGNACIONES AYUDA
# =====================================================================
@router_asigna_ccz.post("/api/obtener_asignaciones_ayuda")
async def obtener_asignaciones_ayuda(request: Request):
    data = await request.json()
    fecha = data['fecha']
    concesion = data['concesion']
    fecha_hora_registro = data.get('fecha_hora_registro')

    reporte = Reporte_Asignaciones()
    asignaciones = reporte.obtener_asignacion_por_fecha(fecha, concesion, fecha_hora_registro)

    if not asignaciones:
        return JSONResponse(content={"message": "No se encontraron asignaciones para la fecha, concesión y fecha/hora seleccionadas."}, status_code=404)
    
    return JSONResponse(content={"asignaciones": asignaciones}, status_code=200)

@router_asigna_ccz.post("/api/obtener_concesiones_por_fecha")
async def obtener_concesiones_por_fecha(request: Request):
    data = await request.json()
    fecha = data['fecha']

    reporte = Reporte_Asignaciones()
    concesiones = reporte.obtener_concesiones_unicas_por_fecha(fecha)

    if not concesiones:
        return JSONResponse(content={"message": "No se encontraron concesiones para la fecha seleccionada."}, status_code=404)

    return JSONResponse(content=concesiones)

@router_asigna_ccz.post("/api/obtener_fechas_horas_registro")
async def obtener_fechas_horas_registro(request: Request):
    data = await request.json()
    fecha = data['fecha']
    concesion = data['concesion']

    reporte = Reporte_Asignaciones()
    fechas_horas = reporte.obtener_fechas_horas_registro(fecha, concesion)

    if not fechas_horas:
        return JSONResponse(content={"message": "No se encontraron registros para la fecha y concesión seleccionada."}, status_code=404)
    
    return JSONResponse(content={"fechas_horas": fechas_horas}, status_code=200)

# =====================================================================
# GENERAR PDF DE ASIGNACIONES
# =====================================================================
# Define el modelo para las asignaciones individuales para el PDF
class Asignacion(BaseModel):
    fecha: str
    cedula: str
    nombre: str
    turno: str
    h_inicio: str
    h_fin: str
    concesion: str
    control: str
    ruta: str
    linea: str
    cop: str
    observaciones: str
    puestosSC: int
    puestosUQ: int
    fecha_hora_registro: str

# Define el modelo para la solicitud de PDF
class PDFRequest(BaseModel):
    asignaciones: List[Asignacion]
    fecha_asignacion: str
    fecha_hora_registro: str

@router_asigna_ccz.post("/generar_pdf/")
def generar_pdf_asignaciones(request: PDFRequest):
    try:
        # Crear un buffer de memoria
        pdf_buffer = BytesIO()

        # Instanciar la clase Reporte_Asignaciones
        reporte_asignaciones = Reporte_Asignaciones()

        # Generar el PDF usando el buffer
        reporte_asignaciones.generar_pdf(
            request.asignaciones,  # Lista de asignaciones
            request.fecha_asignacion,  # Fecha de asignación
            request.fecha_hora_registro,  # Fecha de última modificación
            pdf_buffer  # Buffer de memoria para escribir el PDF
        )

        # Asegurarse de que el buffer esté al inicio antes de enviarlo
        pdf_buffer.seek(0)

        # Devolver el PDF generado directamente al cliente sin guardarlo en disco
        return StreamingResponse(pdf_buffer, media_type='application/pdf', headers={
            "Content-Disposition": "attachment; filename=asignaciones_tecnicos.pdf"
        })

    except Exception as e:
        return {"error": str(e)}