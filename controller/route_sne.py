from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from typing import Optional
from pydantic import BaseModel
from model.gestion_sne_objecion import GestionSneObjecion

router_sne = APIRouter(prefix="/sne", tags=["sne_objecion"])
templates = Jinja2Templates(directory="./view")

def get_user_session(req: Request):
    return req.session.get("user")

def require_session(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesion no valida")
    return user

# =====================================================================
# HTML: VISTA PRINCIPAL
# =====================================================================
@router_sne.get("/objecion", response_class=HTMLResponse)
def sne_objecion(
    req: Request,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "sne_objecion.html",
        {"request": req, "user_session": user_session},
    )

# =====================================================================
# API: FILTROS
# =====================================================================
@router_sne.get("/api/filtros/ultima-fecha")
def filtros_ultima_fecha(user_session: dict = Depends(require_session)):
    """
    Retorna la ultima fecha con registros en sne.ics.

    Response:
    {
        "ok": true,
        "data": "2026-02-10"
    }
    """
    with GestionSneObjecion() as db:
        fecha = db.ultima_fecha_ics()
    return {"ok": True, "data": fecha}

@router_sne.get("/api/filtros/ics")
def filtros_ics(
    fecha: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna lista de id_ics unicos para la fecha dada,
    filtrados por los que tiene asignados el usuario logueado.

    Response:
    {
        "ok": true,
        "data": [103338422, 103338426, ...]
    }
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        if not fecha:
            fecha = db.ultima_fecha_ics()
        data = db.listar_id_ics_por_fecha(fecha=fecha, usuario_id=usuario_id) if fecha else []
    return {"ok": True, "data": data}

@router_sne.get("/api/filtros/componentes")
def filtros_componentes(user_session: dict = Depends(require_session)):
    """
    Retorna lista de componentes unicos desde config.cop -> config.componente.

    Response:
    {
        "ok": true,
        "data": ["COMPONENTE A", "COMPONENTE B", ...]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_componentes()
    return {"ok": True, "data": data}

@router_sne.get("/api/filtros/zonas")
def filtros_zonas(
    componente: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna zonas filtradas por componente.

    Response:
    {
        "ok": true,
        "data": ["ZONA 1", "ZONA 2", ...]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_zonas(componente=componente)
    return {"ok": True, "data": data}

@router_sne.get("/api/filtros/cop")
def filtros_cop(
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna COPs filtrados por componente y/o zona.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "cop": "COP NORTE", "componente": "...", "zona": "..."},
            ...
        ]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_cop(componente=componente, zona=zona)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne.get("/api/filtros/rutas")
def filtros_rutas(
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna rutas comerciales filtradas por COP.
    Muestra el campo ruta_comercial en la lista desplegable.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "id_linea": 10177, "ruta_comercial": "RUTA 177", "id_cop": 3},
            ...
        ]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_rutas_por_cop(id_cop=id_cop, zona=zona)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne.get("/api/filtros/responsables")
def filtros_responsables(user_session: dict = Depends(require_session)):
    """
    Retorna lista de responsables desde sne.responsable_sne.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "responsable": "JUAN PEREZ"},
            ...
        ]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_responsables()
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne.get("/api/filtros/acciones")
def filtros_acciones(user_session: dict = Depends(require_session)):
    """Retorna lista de acciones desde sne.acciones."""
    with GestionSneObjecion() as db:
        data = db.listar_acciones()
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne.get("/api/filtros/justificaciones")
def filtros_justificaciones(
    id_acc: Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Retorna justificaciones filtradas por id_acc."""
    with GestionSneObjecion() as db:
        data = db.listar_justificaciones(id_acc=id_acc)
    return {"ok": True, "data": [{"id": r["id_justificacion"], "id_acc": r["id_acc"], "justificacion": r["justificacion"]} for r in data]}

@router_sne.get("/api/filtros/motivos")
def filtros_motivos(
    id_responsable: Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Retorna motivos/observaciones filtrados por responsable."""
    with GestionSneObjecion() as db:
        data = db.listar_motivos_por_responsable(id_responsable=id_responsable)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne.get("/api/estadisticas")
def estadisticas(
    fecha: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Estadisticas de los ICS asignados al usuario logueado.
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        stats = db.estadisticas_usuario(usuario_id=usuario_id, fecha=fecha)
    return {"ok": True, "data": dict(stats) if stats else {}}

# =====================================================================
# API: MAPA - POSICIONAMIENTOS POR BUS Y FECHA
# =====================================================================
@router_sne.get("/api/mapa/posicionamientos")
def mapa_posicionamientos(
    bus: str,
    fecha: Optional[str] = None,
    hora_ini: str = "00:00:00",
    hora_fin: str = "23:59:59",
    user_session: dict = Depends(require_session),
):
    """
    Retorna posicionamientos GPS para un bus en una fecha y rango de horas.
    Se usa en el mapa del modal de gestión.
    """
    # Asegurar segundos en las horas recibidas desde el front (HH:MM -> HH:MM:SS)
    def _pad_hora(h: str, fin: bool = False) -> str:
        parts = h.split(':')
        if len(parts) == 2:
            return h + (':59' if fin else ':00')
        return h
    
    with GestionSneObjecion() as db:
        if not fecha:
            fecha = db.ultima_fecha_ics()
        data = db.listar_posicionamientos(
            movil_bus=bus.strip().upper(),
            fecha=fecha,
            hora_ini=_pad_hora(hora_ini, fin=False),
            hora_fin=_pad_hora(hora_fin, fin=True),
        )
        
    return {
        "ok": True,
        "bus": bus,
        "fecha": fecha,
        "total": len(data),
        "data": [dict(r) for r in data],
    }

# =====================================================================
# API: LISTADO DE REGISTROS ICS POR TAB
# =====================================================================
@router_sne.get("/api/registros")
def listar_registros(
    tab: str = Query("revisar", regex="^(revisar|revisados|validar)$"),
    fecha: Optional[str] = None,
    id_ics: Optional[int] = None,
    id_linea: Optional[int] = None,
    id_concesion: Optional[int] = None,
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    id_responsable: Optional[int] = None,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(50, ge=1, le=500),
    user_session: dict = Depends(require_session),
):
    """
    Retorna los registros ICS asignados al usuario logueado segun el tab activo.

    Tabs:
        - revisar   -> asignados pendientes de revision (revisor > 0, estado_asignacion=1)
        - revisados -> ya revisados (estado_asignacion=2)
        - validar   -> con objecion pendiente de validar (estado_objecion=1)
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        registros, total = db.listar_ics_por_usuario(
            usuario_id=usuario_id,
            fecha=fecha,
            id_ics=id_ics,
            id_linea=id_linea,
            id_concesion=id_concesion,
            id_cop=id_cop,
            zona=zona,
            componente=componente,
            id_responsable=id_responsable,
            tab=tab,
            pagina=pagina,
            tamano=tamano,
        )
    return {
        "ok": True,
        "tab": tab,
        "total": total,
        "pagina": pagina,
        "tamano": tamano,
        "data": [dict(r) for r in registros],
    }

@router_sne.get("/api/registros/{id_ics}")
def detalle_registro(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """
    Retorna el detalle completo de un ICS para el modal de revision.
    Solo accesible si el ICS esta asignado al usuario logueado.
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        detalle = db.obtener_detalle_ics(id_ics=id_ics, usuario_id=usuario_id)
    if not detalle:
        raise HTTPException(
            status_code=404,
            detail=f"ICS {id_ics} no encontrado o no asignado a este usuario",
        )
    return {"ok": True, "data": dict(detalle)}

# =====================================================================
# API: ACTUALIZAR GESTION DE UN ICS
# PATCH /sne/api/registros/{id_ics}/gestion
# =====================================================================
class GestionPayload(BaseModel):
    estado_asignacion: Optional[int] = None
    estado_objecion: Optional[int] = None
    estado_transmitools: Optional[int] = None
    observacion: Optional[str] = None
    id_responsable: Optional[int] = None
    id_accion: Optional[int] = None
    id_justificacion: Optional[int] = None
    km_objetado: Optional[float] = None

@router_sne.patch("/api/registros/{id_ics}/gestion")
def actualizar_gestion(
    id_ics: int,
    payload: GestionPayload,
    user_session: dict = Depends(require_session),
):
    """
    Actualiza el estado de gestion de un ICS.
    Solo el revisor asignado puede modificarlo.
    """
    usuario_id = user_session.get("id")
    try:
        with GestionSneObjecion() as db:
            db.actualizar_gestion(
                id_ics=id_ics,
                usuario_id=usuario_id,
                estado_asignacion=payload.estado_asignacion,
                estado_objecion=payload.estado_objecion,
                estado_transmitools=payload.estado_transmitools,
                observacion=payload.observacion,
                id_responsable=payload.id_responsable,
                id_accion=payload.id_accion,
                id_justificacion=payload.id_justificacion,
                km_objetado=payload.km_objetado,
            )
        return {"ok": True, "msg": "Gestion actualizada correctamente"}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))