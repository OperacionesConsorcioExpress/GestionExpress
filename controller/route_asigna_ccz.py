import time , uuid
from datetime import date
from database.database_manager import get_db_connection
from fastapi import APIRouter, Request, Form, Depends, File, UploadFile, HTTPException
from fastapi.responses import (
    HTMLResponse, RedirectResponse, JSONResponse,
    StreamingResponse, FileResponse,
)
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional
from io import BytesIO

from lib.verifcar_clave import check_user
from lib.asignar_controles import (
    # Funciones individuales — mantenidas para endpoints legacy
    fecha_asignacion, puestos_SC, puestos_UQ, concesion,
    control, rutas, turnos, hora_inicio, hora_fin,
    # Nueva función consolidada + invalidación de caché
    get_datos_parametrizacion,
    invalidar_cache_parametrizacion,
    get_turno_horario,
)
from lib.cargues import ProcesarCargueControles
from model.gestion_asigna_ccz import (
    Cargue_Controles,
    Cargue_Asignaciones,
    Reporte_Asignaciones,
)
# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────
router_asigna_ccz = APIRouter(tags=["asignaciones_ccz"])
templates = Jinja2Templates(directory="./view")
# ─────────────────────────────────────────────────────────────────────────────
# Instancias globales — se crean UNA sola vez (stateless, thread-safe)
# ─────────────────────────────────────────────────────────────────────────────
cargue_asignaciones  = Cargue_Asignaciones()
reporte_asignaciones = Reporte_Asignaciones()
# ─────────────────────────────────────────────────────────────────────────────
# Caché de cargue con TTL  (30 minutos)
# Estructura: { session_id: {"data": preliminar, "ts": float} }
# ─────────────────────────────────────────────────────────────────────────────
_CACHE_CARGUE_TTL = 1800   # segundos
_cache_cargue: dict = {}
_CACHE_DASHBOARD_FILTROS_TTL = 300
_cache_dashboard_filtros: dict = {"data": None, "ts": 0.0}

def _limpiar_cache_cargue():
    """Elimina entradas vencidas del caché de cargue."""
    ahora = time.time()
    vencidos = [k for k, v in _cache_cargue.items()
                if ahora - v["ts"] > _CACHE_CARGUE_TTL]
    for k in vencidos:
        del _cache_cargue[k]

# ─────────────────────────────────────────────────────────────────────────────
# Dependencia de sesión
# ─────────────────────────────────────────────────────────────────────────────
def _obtener_dashboard_filtros_cache():
    data = _cache_dashboard_filtros.get("data")
    ts = _cache_dashboard_filtros.get("ts", 0.0)
    if data is not None and (time.time() - ts) <= _CACHE_DASHBOARD_FILTROS_TTL:
        return data
    return None

def _guardar_dashboard_filtros_cache(data: dict):
    _cache_dashboard_filtros["data"] = data
    _cache_dashboard_filtros["ts"] = time.time()

def _invalidar_dashboard_filtros_cache():
    _cache_dashboard_filtros["data"] = None
    _cache_dashboard_filtros["ts"] = 0.0


def _validar_rango_dashboard(fecha_inicio: str, fecha_fin: str) -> Optional[str]:
    try:
        inicio = date.fromisoformat(fecha_inicio)
        fin = date.fromisoformat(fecha_fin)
    except (TypeError, ValueError):
        return "El rango de fechas no tiene un formato valido."

    if inicio > fin:
        return "La fecha inicio no puede ser mayor que la fecha fin."

    if (fin - inicio).days > 30:
        return "El rango de consulta no puede superar 30 dias."

    return None

def get_user_session(req: Request):
    return req.session.get("user")
# =====================================================================
# PANTALLA DE ASIGNACIÓN
# =====================================================================
@router_asigna_ccz.get("/asignacion", response_class=HTMLResponse)
def asignacion(req: Request,
                user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "asignacion.html", {"request": req, "user_session": user_session}
    )

@router_asigna_ccz.post("/asignacion", response_class=HTMLResponse)
def asignacion_post(req: Request,
                    username: str = Form(...),
                    password_user: str = Form(...)):
    verify = check_user(username, password_user)
    if verify:
        return templates.TemplateResponse(
            "asignacion.html", {"request": req, "data_user": verify}
        )
    return templates.TemplateResponse(
        "index.html",
        {"request": req,
            "error_message": "Por favor valide sus credenciales y vuelva a intentar."},
    )

# =====================================================================
# CARGUES MASIVOS DE PLANTA Y CONTROLES
# =====================================================================
@router_asigna_ccz.post("/cargar_archivo/")
async def cargar_archivo(file: UploadFile = File(...)):
    """
    Procesa el archivo Excel preliminarmente y guarda los datos en caché.
    Retorna session_id para confirmar el cargue posteriormente.
    """
    _limpiar_cache_cargue()   # Aprovechar para limpiar entradas vencidas

    procesador = ProcesarCargueControles(file)
    preliminar = procesador.leer_archivo()

    session_id = str(uuid.uuid4())   # UUID único — sin colisiones
    _cache_cargue[session_id] = {"data": preliminar, "ts": time.time()}

    print(f"[cargar_archivo] Archivo procesado. Session ID: {session_id}")
    return {"session_id": session_id, "preliminar": preliminar}

@router_asigna_ccz.post("/confirmar_cargue/")
async def confirmar_cargue(data: dict):
    """
    Persiste en BD las hojas seleccionadas por el usuario.
    Invalida el caché de parametrización si se cargaron datos que lo afectan.
    """
    session_id = data.get("session_id")

    if session_id not in _cache_cargue:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    entrada    = _cache_cargue[session_id]
    preliminar = entrada["data"]

    hojas_a_cargar = {}
    if data.get("tcz"):
        hojas_a_cargar["planta"]       = preliminar.get("planta")
    if data.get("supervisores"):
        hojas_a_cargar["supervisores"] = preliminar.get("supervisores")
    if data.get("turnos"):
        hojas_a_cargar["turnos"]       = preliminar.get("turnos")
    if data.get("controles"):
        hojas_a_cargar["controles"]    = preliminar.get("controles")

    if not hojas_a_cargar:
        raise HTTPException(
            status_code=400,
            detail="Debe seleccionar al menos una hoja para cargar.",
        )

    try:
        cargador = Cargue_Controles()
        cargador.cargar_datos(hojas_a_cargar)

        # Invalida caché de parametrización si cambiaron turnos o controles
        if any(k in hojas_a_cargar for k in ("planta", "turnos", "controles")):
            invalidar_cache_parametrizacion()
            print("[confirmar_cargue] Caché de parametrización invalidado.")

        # Limpiar esta entrada del caché de cargue
        del _cache_cargue[session_id]

        return {"message": "Datos cargados exitosamente."}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al cargar los datos: {str(e)}"
        )

@router_asigna_ccz.get("/plantilla_cargue")
async def descargar_plantilla():
    file_path = "./cargues/asignaciones_tecnicos.xlsx"
    return FileResponse(
        path=file_path,
        filename="asignaciones_tecnicos.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# =====================================================================
# NUEVO: ENDPOINT CONSOLIDADO DE DATOS INICIALES
# Reemplaza 4-6 requests separados al cargar la página
# GET /api/datos_iniciales
# Retorna: { planta, supervisores, turnos, puestos_SC, puestos_UQ, concesiones }
# =====================================================================
@router_asigna_ccz.get("/api/datos_iniciales", response_class=JSONResponse)
def api_datos_iniciales():
    """
    Endpoint principal de inicialización del frontend.
    Consolida: planta + supervisores + turnos + puestos_SC/UQ + concesiones
    en una sola respuesta. Resultado de parametrización cacheado 5 min.
    """
    try:
        # Parametrización cacheada (puestos, concesiones, turnos)
        params = get_datos_parametrizacion()

        # Planta y supervisores — datos operativos, sin caché
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT cedula, nombre FROM planta ORDER BY nombre"
                )
                planta = [{"cedula": r[0], "nombre": r[1]}
                           for r in cursor.fetchall()]

                cursor.execute(
                    "SELECT cedula, nombre FROM supervisores ORDER BY nombre"
                )
                supervisores = [{"cedula": r[0], "nombre": r[1]}
                                 for r in cursor.fetchall()]

        return JSONResponse(content={
            "planta":       planta,
            "supervisores": supervisores,
            "turnos":       params["turnos"],
            "puestos_SC":   params["puestos_SC"],
            "puestos_UQ":   params["puestos_UQ"],
            "concesiones":  params["concesiones"],
        })

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos iniciales: {str(e)}",
        )

# =====================================================================
# NUEVO: ENDPOINT CONSOLIDADO PARA MODAL CONSULTAR
# Reemplaza cascada: concesiones → fechas_horas (2-3 requests → 1)
# POST /api/consulta_modal  body: { "fecha": "YYYY-MM-DD" }
# Retorna: { "concesion_nombre": ["2024-01-15 08:30:00", ...], ... }
# =====================================================================
@router_asigna_ccz.post("/api/consulta_modal", response_class=JSONResponse)
async def api_consulta_modal(request: Request):
    """
    Retorna concesiones únicas con sus fechas/horas de registro para
    una fecha dada, en un solo round-trip a la BD.
    El frontend construye el select de concesiones y el select de
    fechas/horas sin requests adicionales.
    """
    data  = await request.json()
    fecha = data.get("fecha")

    if not fecha:
        raise HTTPException(status_code=400, detail="Campo 'fecha' requerido.")

    resultado = reporte_asignaciones.obtener_datos_modal_consulta(fecha)

    if not resultado:
        return JSONResponse(
            content={"message": "No se encontraron asignaciones para la fecha seleccionada."},
            status_code=404,
        )

    return JSONResponse(content=resultado)

# =====================================================================
# ENDPOINTS INDIVIDUALES (compatibilidad con frontend actual)
# Estos siguen funcionando; el frontend nuevo preferirá /api/datos_iniciales
# =====================================================================
@router_asigna_ccz.get("/api/planta", response_class=JSONResponse)
def api_planta():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT cedula, nombre FROM planta ORDER BY nombre")
            return [{"cedula": r[0], "nombre": r[1]} for r in cursor.fetchall()]

@router_asigna_ccz.get("/api/supervisores", response_class=JSONResponse)
def api_supervisores():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT cedula, nombre FROM supervisores ORDER BY nombre"
            )
            return [{"cedula": r[0], "nombre": r[1]} for r in cursor.fetchall()]

@router_asigna_ccz.get("/api/fecha_asignacion")
async def api_fecha_asignacion(fecha: str):
    return fecha_asignacion(fecha)

@router_asigna_ccz.get("/api/puestos_SC")
async def api_puestos_SC():
    return puestos_SC()   # wrapper → get_datos_parametrizacion() cacheado

@router_asigna_ccz.get("/api/puestos_UQ")
async def api_puestos_UQ():
    return puestos_UQ()   # wrapper → get_datos_parametrizacion() cacheado

@router_asigna_ccz.get("/api/concesion")
async def api_concesion():
    return concesion()    # wrapper → get_datos_parametrizacion() cacheado

@router_asigna_ccz.get("/api/control")
async def get_control(concesion: str, puestos: str):
    controles = control(concesion, puestos)
    return JSONResponse(content=controles)

@router_asigna_ccz.get("/api/rutas")
async def get_rutas(concesion: str, puestos: str, control: str):
    rutas_asociadas = rutas(concesion, puestos, control)
    return {"rutas": rutas_asociadas}

# ── /api/turnos — ruta duplicada eliminada, queda solo esta ──────────
@router_asigna_ccz.get("/api/turnos")
async def get_turnos():
    """Lista de nombres de turno. Usa caché de parametrización."""
    return turnos()   # wrapper → get_datos_parametrizacion() cacheado

@router_asigna_ccz.get("/api/turno_descripcion")
def api_turno_descripcion(turno: str):
    from lib.asignar_controles import turno_descripcion as _td
    return {"descripcion": _td(turno)}

# ── Hora inicio + fin en 1 solo endpoint (optimizado) ────────────────
@router_asigna_ccz.get("/api/turno_horario")
async def api_turno_horario(turno: str):
    """
    NUEVO: retorna {inicio, fin} en 1 query en lugar de 2 endpoints.
    El frontend puede seguir usando /api/hora_inicio y /api/hora_fin
    (compatibilidad) pero preferir este endpoint.
    """
    return get_turno_horario(turno)

@router_asigna_ccz.get("/api/hora_inicio")
async def get_hora_inicio(turno: str):
    return {"inicio": hora_inicio(turno)}

@router_asigna_ccz.get("/api/hora_fin")
async def get_hora_fin(turno: str):
    return {"fin": hora_fin(turno)}

# =====================================================================
# GUARDAR ASIGNACIONES
# =====================================================================
class AsignacionRequest(BaseModel):
    fecha:           str
    cedula:          str
    nombre:          str
    turno:           str
    hora_inicio:     str
    hora_fin:        str
    concesion:       str
    control:         str
    rutas_asociadas: str
    observaciones:   str

@router_asigna_ccz.post("/api/guardar_asignaciones")
async def guardar_asignaciones(
    request: Request,
    user_session: dict = Depends(get_user_session),
):
    try:
        data           = await request.json()
        processed_data = cargue_asignaciones.procesar_asignaciones(data, user_session)
        cargue_asignaciones.cargar_asignaciones(processed_data)
        _invalidar_dashboard_filtros_cache()
        return {"message": "Asignaciones guardadas exitosamente."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================================
# DASHBOARD Y REPORTES
# =====================================================================
@router_asigna_ccz.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    req: Request,
    modal: bool = False,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": req, "user_session": user_session, "modal": modal},
    )

@router_asigna_ccz.get("/dashboard/filtros", response_class=JSONResponse)
async def dashboard_filtros(
    refresh: bool = False,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse(content={"message": "Sesion no valida."}, status_code=401)

    filtros = None if refresh else _obtener_dashboard_filtros_cache()
    if filtros is None:
        filtros = reporte_asignaciones.obtener_filtros_unicos_dashboard()
        _guardar_dashboard_filtros_cache(filtros)

    return JSONResponse(content=filtros)

@router_asigna_ccz.get("/filtrar_asignaciones", response_class=HTMLResponse)
async def filtrar_asignaciones(
    request: Request,
    fechaInicio: str, fechaFin: str,
    cedulaTecnico: Optional[str] = None,
    nombreTecnico: Optional[str] = None,
    turno: Optional[str] = None,
    concesion: Optional[str] = None,
    control: Optional[str] = None,
    ruta: Optional[str] = None,
    linea: Optional[str] = None,
    cop: Optional[str] = None,
    usuarioRegistra: Optional[str] = None,
    nombreSupervisorEnlace: Optional[str] = None,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    rango_error = _validar_rango_dashboard(fechaInicio, fechaFin)
    if rango_error:
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user_session": user_session,
                "modal": False,
                "asignaciones": [],
                "fechaInicio": fechaInicio,
                "fechaFin": fechaFin,
                "dashboard_error": rango_error,
            },
            status_code=400,
        )

    filtros_kwargs = dict(
        fecha_inicio             = fechaInicio,
        fecha_fin                = fechaFin,
        cedula                   = cedulaTecnico,
        nombre                   = nombreTecnico,
        turno                    = turno,
        concesion                = concesion,
        control                  = control,
        ruta                     = ruta,
        linea                    = linea,
        cop                      = cop,
        registrado_por           = usuarioRegistra,
        nombre_supervisor_enlace = nombreSupervisorEnlace,
    )
    asignaciones = reporte_asignaciones.obtener_asignaciones(**filtros_kwargs)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user_session": user_session,
            "modal": False,
            "asignaciones": asignaciones,
            "fechaInicio": fechaInicio,
            "fechaFin": fechaFin,
            "cedulaTecnico": cedulaTecnico,
            "nombreTecnico": nombreTecnico,
            "turno": turno,
            "concesion": concesion,
            "control": control,
            "ruta": ruta,
            "linea": linea,
            "cop": cop,
            "usuarioRegistra": usuarioRegistra,
            "nombreSupervisorEnlace": nombreSupervisorEnlace,
        },
    )

@router_asigna_ccz.post("/buscar_asignaciones")
async def buscar_asignaciones(request: Request):
    filtros      = await request.json()
    if not filtros.get("fechaInicio") or not filtros.get("fechaFin"):
        return JSONResponse(
            content={"message": "Debe seleccionar fecha inicial y fecha final para consultar."},
            status_code=400,
        )

    rango_error = _validar_rango_dashboard(
        filtros.get("fechaInicio"),
        filtros.get("fechaFin"),
    )
    if rango_error:
        return JSONResponse(content={"message": rango_error}, status_code=400)

    asignaciones = reporte_asignaciones.obtener_asignaciones(
        fecha_inicio             = filtros.get("fechaInicio"),
        fecha_fin                = filtros.get("fechaFin"),
        cedula                   = filtros.get("cedulaTecnico"),
        nombre                   = filtros.get("nombreTecnico"),
        turno                    = filtros.get("turno"),
        concesion                = filtros.get("concesion"),
        control                  = filtros.get("control"),
        ruta                     = filtros.get("ruta"),
        linea                    = filtros.get("linea"),
        cop                      = filtros.get("cop"),
        registrado_por           = filtros.get("usuarioRegistra"),
        nombre_supervisor_enlace = filtros.get("nombreSupervisorEnlace"),
    )
    return JSONResponse(content=asignaciones)

@router_asigna_ccz.post("/descargar_xlsx")
async def descargar_xlsx(request: Request):
    filtros = await request.json()
    rango_error = _validar_rango_dashboard(
        filtros.get("fechaInicio"),
        filtros.get("fechaFin"),
    )
    if rango_error:
        return JSONResponse(content={"message": rango_error}, status_code=400)
    return StreamingResponse(
        reporte_asignaciones.generar_xlsx(filtros),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="asignaciones.xlsx"'},
    )

@router_asigna_ccz.post("/descargar_csv")
async def descargar_csv(request: Request):
    filtros = await request.json()
    rango_error = _validar_rango_dashboard(
        filtros.get("fechaInicio"),
        filtros.get("fechaFin"),
    )
    if rango_error:
        return JSONResponse(content={"message": rango_error}, status_code=400)
    return StreamingResponse(
        reporte_asignaciones.generar_csv(filtros),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="asignaciones.csv"'},
    )

@router_asigna_ccz.post("/descargar_json")
async def descargar_json(request: Request):
    filtros  = await request.json()
    rango_error = _validar_rango_dashboard(
        filtros.get("fechaInicio"),
        filtros.get("fechaFin"),
    )
    if rango_error:
        return JSONResponse(content={"message": rango_error}, status_code=400)
    json_str = reporte_asignaciones.generar_json(filtros)
    return StreamingResponse(
        BytesIO(json_str.encode("utf-8")),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="asignaciones.json"'},
    )

# =====================================================================
# CONSULTA MODAL AYUDA — endpoints legacy (compatibilidad)
# El frontend nuevo usa /api/consulta_modal en su lugar
# =====================================================================
@router_asigna_ccz.post("/api/obtener_asignaciones_ayuda")
async def obtener_asignaciones_ayuda(request: Request):
    data                = await request.json()
    fecha               = data["fecha"]
    concesion_val       = data["concesion"]
    fecha_hora_registro = data.get("fecha_hora_registro")

    paquete = reporte_asignaciones.obtener_paquete_asignacion(
        fecha, concesion_val, fecha_hora_registro
    )

    if not paquete:
        return JSONResponse(
            content={"message": "No se encontraron asignaciones para los parámetros indicados."},
            status_code=404,
        )
    return JSONResponse(content=paquete)

@router_asigna_ccz.post("/api/obtener_concesiones_por_fecha")
async def obtener_concesiones_por_fecha(request: Request):
    data       = await request.json()
    fecha      = data["fecha"]
    concesiones = reporte_asignaciones.obtener_concesiones_unicas_por_fecha(fecha)

    if not concesiones:
        return JSONResponse(
            content={"message": "No se encontraron concesiones para la fecha seleccionada."},
            status_code=404,
        )
    return JSONResponse(content=concesiones)

@router_asigna_ccz.post("/api/obtener_fechas_horas_registro")
async def obtener_fechas_horas_registro(request: Request):
    data          = await request.json()
    fecha         = data["fecha"]
    concesion_val = data["concesion"]

    fechas_horas = reporte_asignaciones.obtener_fechas_horas_registro(
        fecha, concesion_val
    )

    if not fechas_horas:
        return JSONResponse(
            content={"message": "No se encontraron registros para los parámetros indicados."},
            status_code=404,
        )
    return JSONResponse(content={"fechas_horas": fechas_horas})

# =====================================================================
# GENERAR PDF DE ASIGNACIONES
# =====================================================================
class AsignacionPDF(BaseModel):
    fecha: Optional[str] = ""
    cedula: Optional[str] = ""
    nombre: Optional[str] = ""
    turno: Optional[str] = ""
    h_inicio: Optional[str] = ""
    h_fin: Optional[str] = ""
    hinicio: Optional[str] = ""
    hfin: Optional[str] = ""
    concesion: Optional[str] = ""
    control: Optional[str] = ""

    ruta: Optional[str] = ""
    cop: Optional[str] = ""
    linea: Optional[str] = ""

    rutas_detalle: Optional[str] = ""
    cops_detalle: Optional[str] = ""
    lineas_detalle: Optional[str] = ""

    observaciones: Optional[str] = ""
    puestosSC: Optional[int] = 0
    puestosUQ: Optional[int] = 0
    fecha_hora_registro: Optional[str] = ""
    supervisor_enlace: Optional[str] = ""
    nombre_supervisor_enlace: Optional[str] = ""

class PDFRequest(BaseModel):
    asignaciones: List[AsignacionPDF]
    fecha_asignacion: str
    fecha_hora_registro: str

@router_asigna_ccz.post("/generar_pdf/")
def generar_pdf_asignaciones(request: PDFRequest):
    try:
        pdf_buffer = BytesIO()

        reporte_asignaciones = Reporte_Asignaciones()

        reporte_asignaciones.generar_pdf(
            request.asignaciones,
            request.fecha_asignacion,
            request.fecha_hora_registro,
            pdf_buffer
        )

        pdf_buffer.seek(0)

        return StreamingResponse(
            pdf_buffer,
            media_type='application/pdf',
            headers={
                "Content-Disposition": f"attachment; filename=asignaciones_tecnicos_{request.fecha_asignacion}.pdf"
            }
        )

    except Exception as e:
        return {"error": str(e)}
