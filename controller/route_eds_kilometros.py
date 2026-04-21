from fastapi import APIRouter, Request, Depends, Body, Query
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from datetime import date
from typing import Optional

from model.gestion_eds_kilometros import Gestion_kilometros

# ── Router ────────────────────────────────────────────────────────────────────
router_eds_kilometros = APIRouter()
templates = Jinja2Templates(directory="./view")


def get_user_session(req: Request):
    return req.session.get("user")


# ─────────────────────────────────────────────────────────────────────────────
#  PÁGINA PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

@router_eds_kilometros.get("/eds_kilometros", response_class=HTMLResponse)
def eds_kilometros(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "eds_kilometros.html",
        {"request": req, "user_session": user_session},
    )


# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS ESTÁTICOS  (tipología, combustible, componente)
# ─────────────────────────────────────────────────────────────────────────────

@router_eds_kilometros.get("/api/eds/km/filtros")
def api_filtros(
    req: Request,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with Gestion_kilometros() as g:
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

@router_eds_kilometros.get("/api/eds/km/filtros/zonas")
def api_filtros_zonas(
    req: Request,
    id_componente: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with Gestion_kilometros() as g:
            return g.filtros_zona(id_componente)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)


# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS DEPENDIENTES: COPs
# ─────────────────────────────────────────────────────────────────────────────

@router_eds_kilometros.get("/api/eds/km/filtros/cops")
def api_filtros_cops(
    req: Request,
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with Gestion_kilometros() as g:
            return g.filtros_cop(id_componente, id_zona)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)


# ─────────────────────────────────────────────────────────────────────────────
#  ESTADÍSTICAS DEL DÍA
# ─────────────────────────────────────────────────────────────────────────────

@router_eds_kilometros.get("/api/eds/km/estadisticas")
def api_estadisticas(
    req: Request,
    fecha: date = Query(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with Gestion_kilometros() as g:
            return g.estadisticas_dia(fecha)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)


# ─────────────────────────────────────────────────────────────────────────────
#  FLOTA DEL DÍA  (tabla principal)
# ─────────────────────────────────────────────────────────────────────────────

@router_eds_kilometros.get("/api/eds/km/flota")
def api_flota(
    req: Request,
    fecha: date       = Query(...),
    pagina: int       = Query(1, ge=1),
    tamano: int       = Query(5000, ge=1, le=5000),
    placa: Optional[str]  = Query(None),
    no_interno: Optional[str] = Query(None),
    tipologia: Optional[str]  = Query(None),
    combustible: Optional[str] = Query(None),
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int]       = Query(None),
    id_cop: Optional[int]        = Query(None),
    estado: Optional[int]        = Query(None),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    # Limpiar cadenas vacías → None para que el SQL las trate como sin filtro
    placa       = placa.strip()       or None if placa       else None
    no_interno  = no_interno.strip()  or None if no_interno  else None
    tipologia   = tipologia.strip()   or None if tipologia   else None
    combustible = combustible.strip() or None if combustible else None

    try:
        with Gestion_kilometros() as g:
            data, total = g.flota_dia(
                fecha=fecha,
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


# ─────────────────────────────────────────────────────────────────────────────
#  KM RECORRIDO FINAL  (celda editable)
# ─────────────────────────────────────────────────────────────────────────────

class KmFinalIn(BaseModel):
    id_bus: int
    fecha: date
    km_recorrido_final: Optional[float] = None


@router_eds_kilometros.patch("/api/eds/km/km_final")
def api_actualizar_km_final(
    req: Request,
    body: KmFinalIn = Body(...),
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        usuario = user_session.get("username", "sistema")
        with Gestion_kilometros() as g:
            row = g.actualizar_km_final(
                id_bus=body.id_bus,
                fecha=body.fecha,
                km_final=body.km_recorrido_final,
                usuario=usuario,
            )
        return {"ok": True, "data": row}
    except Exception as exc:
        # La tabla eds.km_diario puede no existir todavía
        return JSONResponse(
            {"ok": False, "detail": str(exc),
             "hint": "Verifique que exista la tabla eds.km_diario (ver docstring del modelo)."},
            status_code=500,
        )
