from fastapi import APIRouter, Request, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from datetime import date
from typing import Optional

from model.gestion_estado_buses import GestionEstadoBuses

# Router del módulo
router_estado_buses = APIRouter()

# Plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Sesión de usuario (validación local)
def obtener_sesion_usuario(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────────────────────────────────────
#  PÁGINA PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_buses.get("/estado_buses", response_class=HTMLResponse)
def estado_buses(req: Request, user_session: dict = Depends(obtener_sesion_usuario)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "estado_buses.html",
        {"request": req, "user_session": user_session},
    )

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS ESTÁTICOS  (tipología, combustible, componente)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_buses.get("/api/estado_buses/filtros")
def api_filtros(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBuses() as g:
            return {
                "tipologia":   [r["tipologia"]   for r in g.filtros_tipologia()],
                "combustible": [r["combustible"] for r in g.filtros_combustible()],
                "componente":  g.filtros_componente(),
            }
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS DEPENDIENTES: ZONAS
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_buses.get("/api/estado_buses/filtros/zonas")
def api_filtros_zonas(
    req: Request,
    id_componente: Optional[int] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBuses() as g:
            return g.filtros_zona(id_componente)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS DEPENDIENTES: COPs
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_buses.get("/api/estado_buses/filtros/cops")
def api_filtros_cops(
    req: Request,
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBuses() as g:
            return g.filtros_cop(id_componente, id_zona)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  FLOTA POR RANGO DE FECHAS  (tabla principal + ventana Gantt)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_buses.get("/api/estado_buses/flota")
def api_flota(
    req: Request,
    fecha_inicio: date = Query(...),
    fecha_fin: date       = Query(...),
    pagina: int           = Query(1, ge=1),
    tamano: int           = Query(5000, ge=1, le=5000),
    placa: Optional[str]  = Query(None),
    no_interno: Optional[str]   = Query(None),
    tipologia: Optional[str]    = Query(None),
    combustible: Optional[str]  = Query(None),
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int]       = Query(None),
    id_cop: Optional[int]        = Query(None),
    estado: Optional[int]        = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    if fecha_fin < fecha_inicio:
        return JSONResponse({"detail": "fecha_fin no puede ser anterior a fecha_inicio"}, status_code=400)

    # Limpiar cadenas vacías → None para que el SQL las trate como sin filtro
    placa       = placa.strip()       or None if placa       else None
    no_interno  = no_interno.strip()  or None if no_interno  else None
    tipologia   = tipologia.strip()   or None if tipologia   else None
    combustible = combustible.strip() or None if combustible else None

    try:
        with GestionEstadoBuses() as g:
            data, total = g.flota_rango(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                pagina=pagina,
                tamano=tamano,
                placa=placa,
                no_interno=no_interno,
                tipologia=tipologia,
                combustible=combustible,
                id_componente=id_componente,
                id_zona=id_zona,
                id_cop=id_cop,
                estado=estado,
            )
        return {"data": [dict(r) for r in data], "total": total}
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)
