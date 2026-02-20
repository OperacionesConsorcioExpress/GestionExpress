from fastapi import APIRouter, Request, HTTPException, UploadFile, Depends, Body, Query,  File, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from model.gestion_checklist import GestionChecklist
from pydantic import BaseModel
from datetime import datetime, time
from typing import List, Optional, Dict, Any
import io, json
import pandas as pd
from model.gestion_eds import GestionEDS, Eds_config, RegistroEDS

# Crear router
router_eds = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Función localmente para validar usuario
def get_user_session(req: Request):
    return req.session.get('user')

# --- RUTA PRINCIPAL SISTEMA ESTACIÓN DE SERVICIO ---
# ----------------------------------------------------------------------
@router_eds.get("/eds_config", response_class=HTMLResponse)
def eds_config(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # Si ya tienes el nuevo template, cambia por "EDS_surtidores.html"
    return templates.TemplateResponse(
        "eds_config.html",
        {"request": req, "user_session": user_session, "componentes": {}}
    )

# ============================================================
#                  MODELOS Pydantic (API)
# ============================================================
class UbicacionIn(BaseModel):
    nombre: str
    tipo: str  # INTERNA / EXTERNA
    ciudad: Optional[str] = None
    direccion: Optional[str] = None
    observacion: Optional[str] = None
    estado: int = 1
    
    componente: Optional[str] = None
    zona: Optional[str] = None
    cop_ids: List[int] = []

class SurtidorIn(BaseModel):
    nombre: str
    tipo_combustible: str  # ACPM / GNV / ELECTRICO
    estado: int = 1

class EDSIn(BaseModel):
    codigo: str
    nombre: str
    id_ubicacion: int
    surtidores_ids: List[int] = []
    estado: int = 1

# ============================================================
#                         EDS
# ============================================================
@router_eds.get("/api/config/eds")
def api_listar_eds(
    req: Request,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(10, ge=1, le=100),
    q: Optional[str] = Query(None),
    estado: Optional[int] = Query(None),
    id_ubicacion: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        data, total = g.eds_listar(pagina, tamano, q=q, estado=estado, id_ubicacion=id_ubicacion)
    return {"data": data, "total": total}

@router_eds.post("/api/config/eds")
def api_crear_eds(
    req: Request,
    body: EDSIn = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        row = g.eds_crear(
            codigo=body.codigo,
            nombre=body.nombre,
            id_ubicacion=body.id_ubicacion,
            surtidores_ids=body.surtidores_ids,
            estado=body.estado
        )
    return row

@router_eds.put("/api/config/eds/{id_eds}")
def api_actualizar_eds(
    req: Request,
    id_eds: int,
    body: EDSIn = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        row = g.eds_actualizar(
            id_eds=id_eds,
            codigo=body.codigo,
            nombre=body.nombre,
            id_ubicacion=body.id_ubicacion,
            surtidores_ids=body.surtidores_ids,
            estado=body.estado
        )
    return row

@router_eds.patch("/api/config/eds/{id_eds}/estado")
def api_estado_eds(
    req: Request,
    id_eds: int,
    estado: dict = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    nuevo = int(estado.get("estado", 1))
    with Eds_config() as g:
        row = g.eds_cambiar_estado(id_eds=id_eds, estado=nuevo)
    return row

# ============================================================
#                       SURTIDORES
# ============================================================
@router_eds.get("/api/config/eds/surtidores")
def api_listar_surtidores(
    req: Request,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(10, ge=1, le=100),
    q: Optional[str] = Query(None),
    estado: Optional[int] = Query(None),
    tipo_combustible: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        data, total = g.surtidores_listar(pagina, tamano, q=q, estado=estado, tipo_combustible=tipo_combustible)
    return {"data": data, "total": total}

@router_eds.get("/api/config/eds/surtidores/activos")
def api_listar_surtidores_activos(
    req: Request,
    estado: int = Query(1),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        data = g.surtidores_activos(estado=estado)
    return {"data": data}

@router_eds.post("/api/config/eds/surtidores")
def api_crear_surtidor(
    req: Request,
    body: SurtidorIn = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        row = g.surtidor_crear(body.nombre, body.tipo_combustible, body.estado)
    return row

@router_eds.put("/api/config/eds/surtidores/{id_surtidor}")
def api_actualizar_surtidor(
    req: Request,
    id_surtidor: int,
    body: SurtidorIn = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        row = g.surtidor_actualizar(id_surtidor, body.nombre, body.tipo_combustible, body.estado)
    return row

@router_eds.patch("/api/config/eds/surtidores/{id_surtidor}/estado")
def api_estado_surtidor(
    req: Request,
    id_surtidor: int,
    estado: dict = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    nuevo = int(estado.get("estado", 1))
    with Eds_config() as g:
        row = g.surtidor_cambiar_estado(id_surtidor, nuevo)
    return row

# ============================================================
#                    FILTROS (DEPENDIENTES)
# ============================================================
@router_eds.get("/api/config/eds/filtros/componentes")
def api_componentes(req: Request, user=Depends(get_user_session)):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    with Eds_config() as g:
        data = g.filtros_componentes()
    return {"data": data}

@router_eds.get("/api/config/eds/filtros/zonas")
def api_zonas(
    req: Request,
    id_componente: Optional[int] = Query(None),
    user=Depends(get_user_session)
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    with Eds_config() as g:
        data = g.filtros_zonas(id_componente=id_componente)
    return {"data": data}

@router_eds.get("/api/config/eds/filtros/cop")
def api_cops(
    req: Request,
    id_zona: Optional[int] = Query(None),
    id_componente: Optional[int] = Query(None),
    user=Depends(get_user_session)
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    with Eds_config() as g:
        data = g.filtros_cop(id_zona=id_zona, id_componente=id_componente)
    return {"data": data}

# ============================================================
#                      UBICACIONES
# ============================================================
@router_eds.get("/api/config/eds/ubicaciones")
def api_listar_ubicaciones(
    req: Request,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(10, ge=1, le=100),
    q: Optional[str] = Query(None),
    estado: Optional[int] = Query(None),
    tipo: Optional[str] = Query(None),
    user=Depends(get_user_session),
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        data, total = g.ubicaciones_listar(pagina, tamano, q=q, estado=estado, tipo=tipo)
    return {"data": data, "total": total}

@router_eds.get("/api/config/eds/ubicaciones/activas")
def api_listar_ubicaciones_activas(
    req: Request,
    estado: int = Query(1),
    user=Depends(get_user_session),
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with Eds_config() as g:
        data = g.ubicaciones_activas(estado=estado)
    return {"data": data}

@router_eds.post("/api/config/eds/ubicaciones")
def api_crear_ubicacion(
    req: Request,
    body: dict = Body(...),
    user=Depends(get_user_session),
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    # esperado del frontend: cops_ids (array)
    cops_ids = body.get("cops_ids") or []
    with Eds_config() as g:
        try:
            row = g.ubicacion_crear(
                nombre=body["nombre"],
                tipo=body["tipo"],
                ciudad=body.get("ciudad"),
                direccion=body.get("direccion"),
                observacion=body.get("observacion"),
                estado=int(body.get("estado", 1)),
                cops_ids=[int(x) for x in cops_ids],
            )
        except ValueError as e:
            return JSONResponse({"detail": str(e)}, status_code=400)
    return row

@router_eds.put("/api/config/eds/ubicaciones/{id_ubicacion}")
def api_actualizar_ubicacion(
    req: Request,
    id_ubicacion: int,
    body: dict = Body(...),
    user=Depends(get_user_session),
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    cops_ids = body.get("cops_ids") or []
    with Eds_config() as g:
        try:
            row = g.ubicacion_actualizar(
                id_ubicacion=id_ubicacion,
                nombre=body["nombre"],
                tipo=body["tipo"],
                ciudad=body.get("ciudad"),
                direccion=body.get("direccion"),
                observacion=body.get("observacion"),
                estado=int(body.get("estado", 1)),
                cops_ids=[int(x) for x in cops_ids],
            )
        except ValueError as e:
            return JSONResponse({"detail": str(e)}, status_code=400)
    return row

@router_eds.patch("/api/config/eds/ubicaciones/{id_ubicacion}/estado")
def api_estado_ubicacion(
    req: Request,
    id_ubicacion: int,
    payload: dict = Body(...),
    user=Depends(get_user_session),
):
    if not user:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    nuevo = int(payload.get("estado", 1))
    with Eds_config() as g:
        row = g.ubicacion_cambiar_estado(id_ubicacion=id_ubicacion, estado=nuevo)
    return row

# ============================================================
# ===================== EDS REGISTRO ==========================
@router_eds.get("/eds_registro", response_class=HTMLResponse)
def eds_registro(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    
    # nombre completo defensivo
    nombres = user_session.get("nombres") or user_session.get("nombre") or ""
    apellidos = user_session.get("apellidos") or ""
    full_name = (f"{nombres} {apellidos}").strip() or user_session.get("usuario") or "Usuario"
    
    return templates.TemplateResponse(
        "eds_registro.html",
        {"request": req, "user_session": user_session, "usuario_full_name": full_name}
    )
    
@router_eds.get("/api/eds/registro/init")
def api_registro_init(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    nombres = user_session.get("nombres") or user_session.get("nombre") or ""
    apellidos = user_session.get("apellidos") or ""
    full_name = (f"{nombres} {apellidos}").strip() or user_session.get("usuario") or "Usuario"

    with RegistroEDS() as r:
        eds = r.eds_activas()

    hoy = datetime.now().date().isoformat()
    return {"usuario_full_name": full_name, "hoy": hoy, "eds": eds}

@router_eds.get("/api/eds/registro/eds/{id_eds}/detalle")
def api_registro_eds_detalle(id_eds: int, req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with RegistroEDS() as r:
            data = r.eds_detalle(id_eds)
        return data
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@router_eds.get("/api/eds/registro/pendientes")
def api_registro_pendientes(
    req: Request,
    id_eds: int = Query(..., ge=1),
    fecha: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with RegistroEDS() as r:
        data = r.pendientes_hoy(id_eds=id_eds, fecha=fecha)
    return {"data": data}

@router_eds.get("/api/eds/registro/ya_registrados")
def api_registro_ya_registrados(
    req: Request,
    id_eds: int = Query(..., ge=1),
    fecha: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with RegistroEDS() as r:
        data = r.ya_registrados(id_eds=id_eds, fecha=fecha)
    return {"data": data}

@router_eds.get("/api/eds/registro/alertas")
def api_registro_alertas(
    req: Request,
    id_eds: int = Query(..., ge=1),
    fecha: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with RegistroEDS() as r:
        data = r.alertas_dia(id_eds=id_eds, fecha=fecha)
    return {"data": data}

@router_eds.get("/api/eds/registro/bus_lookup")
def api_registro_bus_lookup(
    req: Request,
    q: str = Query(...),
    id_eds: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with RegistroEDS() as r:
        bus = r.bus_lookup(q)
        if not bus:
            return {"bus": None}

        asignado = None
        if id_eds:
            asignado = r._es_bus_asignado_a_eds(int(id_eds), int(bus["id"]))

    return {"bus": bus, "asignado_a_eds": asignado}

@router_eds.get("/api/eds/registro/bus_suggest")
def api_registro_bus_suggest(
    req: Request,
    q: str = Query(...),
    id_eds: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    with RegistroEDS() as r:
        data = r.bus_sugerencias(q=q, id_eds=id_eds, limit=10)
    return {"data": data}

@router_eds.post("/api/eds/registro/guardar")
async def api_registro_guardar(
    req: Request,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    # -----------------------
    # Usuario
    # -----------------------
    nombres = user_session.get("nombres") or user_session.get("nombre") or ""
    apellidos = user_session.get("apellidos") or ""
    full_name = (f"{nombres} {apellidos}").strip() or user_session.get("usuario") or "Usuario"

    try:
        # =======================
        # Parseo MANUAL del form
        # =======================
        form = await req.form()

        # -----------------------
        # Campos obligatorios
        # -----------------------
        id_bus = int(form.get("id_bus"))
        id_eds = int(form.get("id_eds"))
        id_surtidor = int(form.get("id_surtidor"))

        odometro = int(form.get("odometro"))
        fecha_operativa = form.get("fecha_operativa")

        if not fecha_operativa:
            raise ValueError("La fecha operativa es obligatoria")

        # -----------------------
        # Campos opcionales
        # -----------------------
        observacion = form.get("observacion")

        volumen_raw = form.get("volumen")
        volumen = None
        if volumen_raw not in (None, ""):
            volumen = float(str(volumen_raw).replace(",", "."))

        odo_func_raw = (form.get("odometro_funcional") or "true").strip().lower()
        odometro_funcional = odo_func_raw in ("true", "1", "si", "sí", "on", "yes")

        # -----------------------
        # Evidencias (archivos)
        # -----------------------
        evidencia_files = []
        files = form.getlist("evidencias")

        for f in files:
            if not f:
                continue

            content = await f.read()
            if not content:
                continue

            evidencia_files.append((
                content,
                f.filename or "evidencia",
                f.content_type or "application/octet-stream"
            ))

        # -----------------------
        # Guardar en BD + Blob
        # -----------------------
        with RegistroEDS() as r:
            row = r.registrar(
                id_bus=id_bus,
                id_eds=id_eds,
                id_surtidor=id_surtidor,
                odometro=odometro,
                volumen=volumen,
                odometro_funcional=odometro_funcional,
                observacion=observacion,
                registrado_por=full_name,
                fecha_operativa=fecha_operativa,
                evidencias=evidencia_files or None
            )

        return {"ok": True, "registro": row}

    except ValueError as e:
        # Errores de validación controlados
        return JSONResponse({"detail": str(e)}, status_code=400)

    except Exception as e:
        # Errores inesperados
        return JSONResponse(
            {"detail": f"Error guardando registro: {str(e)}"},
            status_code=500
        )
