from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field
from model.gestion_tarifas import (
    GestionTarifas,
    GestionTipoTarifa,
    GestionTipologiaBus,
    GestionCombustible,
)

router_tarifas = APIRouter()
templates = Jinja2Templates(directory="./view")


def obtener_sesion_usuario(req: Request):
    return req.session.get("user")


def parametros_paginacion(
    pagina: int = Query(1, ge=1, alias="pagina"),
    tamano: int = Query(10, ge=1, le=500, alias="tamano"),
):
    return pagina, tamano


# ── Esquemas comunes ──────────────────────────────────────────────────────────
class EstadoIn(BaseModel):
    estado: bool


# ── Vista principal ───────────────────────────────────────────────────────────
@router_tarifas.get("/tarifas_cexp", response_class=HTMLResponse)
def vista_tarifas(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "tarifas_cexp.html",
        {"request": req, "user_session": user_session},
    )


# ══════════════════════════════════════════════════════════════════════════════
# API TARIFAS  (config.tarifas)
# ══════════════════════════════════════════════════════════════════════════════
class TarifaIn(BaseModel):
    id_tipo_tarifa: int = Field(..., ge=1)
    id_tipologia: int = Field(..., ge=1)
    id_combustible: int = Field(..., ge=1)
    desde: str = Field(..., min_length=10, max_length=10)
    hasta: Optional[str] = None
    tarifa: float = Field(..., gt=0)


class TarifaValorIn(BaseModel):
    tarifa: float = Field(..., gt=0)


class CerrarPeriodoIn(BaseModel):
    hasta: str = Field(..., min_length=10, max_length=10)


@router_tarifas.get("/api/config/tarifas")
def listar_tarifas(
    q: Optional[str] = None,
    id_tipo_tarifa: Optional[int] = None,
    id_tipologia: Optional[int] = None,
    id_combustible: Optional[int] = None,
    solo_vigentes: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionTarifas() as db:
        data, total = db.listar(
            q=q,
            id_tipo_tarifa=id_tipo_tarifa,
            id_tipologia=id_tipologia,
            id_combustible=id_combustible,
            solo_vigentes=solo_vigentes,
            pagina=pagina,
            tamano=tamano,
        )
    return {"data": data, "total": total, "page": pagina, "size": tamano}


@router_tarifas.get("/api/config/tarifas/consultar")
def consultar_tarifa_por_fecha(
    fecha: str,
    id_tipo_tarifa: Optional[int] = None,
    id_tipologia: Optional[int] = None,
    id_combustible: Optional[int] = None,
):
    with GestionTarifas() as db:
        data = db.consultar_por_fecha(
            fecha=fecha,
            id_tipo_tarifa=id_tipo_tarifa,
            id_tipologia=id_tipologia,
            id_combustible=id_combustible,
        )
    return {"data": data, "total": len(data)}


@router_tarifas.get("/api/config/tarifas/apoyo")
def apoyo_tarifas():
    with GestionTarifas() as db:
        return {
            "tipos": db.listar_tipos(),
            "tipologias": db.listar_tipologias(),
            "combustibles": db.listar_combustibles(),
        }


@router_tarifas.post("/api/config/tarifas", status_code=201)
def crear_tarifa(payload: TarifaIn):
    with GestionTarifas() as db:
        try:
            return db.crear(
                id_tipo_tarifa=payload.id_tipo_tarifa,
                id_tipologia=payload.id_tipologia,
                id_combustible=payload.id_combustible,
                desde=payload.desde,
                hasta=payload.hasta,
                tarifa=payload.tarifa,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.put("/api/config/tarifas/{id}/valor")
def actualizar_valor_tarifa(id: int, payload: TarifaValorIn):
    with GestionTarifas() as db:
        try:
            return db.actualizar_valor(id=id, tarifa=payload.tarifa)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.patch("/api/config/tarifas/{id}/cerrar")
def cerrar_periodo_tarifa(id: int, payload: CerrarPeriodoIn):
    with GestionTarifas() as db:
        try:
            return db.cerrar_periodo(id=id, hasta=payload.hasta)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# API TIPO TARIFA  (config.tipo_tarifa)
# ══════════════════════════════════════════════════════════════════════════════
class TipoTarifaIn(BaseModel):
    tipo: str = Field(..., min_length=1, max_length=20)


@router_tarifas.get("/api/config/tipo_tarifa")
def listar_tipo_tarifa(
    q: Optional[str] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionTipoTarifa() as db:
        data, total = db.listar(q=q, estado=estado, pagina=pagina, tamano=tamano)
    return {"data": data, "total": total, "page": pagina, "size": tamano}


@router_tarifas.post("/api/config/tipo_tarifa", status_code=201)
def crear_tipo_tarifa(payload: TipoTarifaIn):
    with GestionTipoTarifa() as db:
        try:
            return db.crear(tipo=payload.tipo)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.put("/api/config/tipo_tarifa/{id}")
def actualizar_tipo_tarifa(id: int, payload: TipoTarifaIn):
    with GestionTipoTarifa() as db:
        try:
            return db.actualizar(id=id, tipo=payload.tipo)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.patch("/api/config/tipo_tarifa/{id}/estado")
def cambiar_estado_tipo_tarifa(id: int, payload: EstadoIn):
    with GestionTipoTarifa() as db:
        try:
            return db.cambiar_estado(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# API TIPOLOGÍA BUS  (config.tipologia_bus)
# ══════════════════════════════════════════════════════════════════════════════
class TipologiaIn(BaseModel):
    tipologia: str = Field(..., min_length=1, max_length=60)


@router_tarifas.get("/api/config/tipologia_bus")
def listar_tipologia_bus(
    q: Optional[str] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionTipologiaBus() as db:
        data, total = db.listar(q=q, estado=estado, pagina=pagina, tamano=tamano)
    return {"data": data, "total": total, "page": pagina, "size": tamano}


@router_tarifas.post("/api/config/tipologia_bus", status_code=201)
def crear_tipologia_bus(payload: TipologiaIn):
    with GestionTipologiaBus() as db:
        try:
            return db.crear(tipologia=payload.tipologia)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.put("/api/config/tipologia_bus/{id}")
def actualizar_tipologia_bus(id: int, payload: TipologiaIn):
    with GestionTipologiaBus() as db:
        try:
            return db.actualizar(id=id, tipologia=payload.tipologia)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.patch("/api/config/tipologia_bus/{id}/estado")
def cambiar_estado_tipologia_bus(id: int, payload: EstadoIn):
    with GestionTipologiaBus() as db:
        try:
            return db.cambiar_estado(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# API COMBUSTIBLE  (config.combustible)
# ══════════════════════════════════════════════════════════════════════════════
class CombustibleIn(BaseModel):
    combustible: str = Field(..., min_length=1, max_length=20)


@router_tarifas.get("/api/config/combustible")
def listar_combustible(
    q: Optional[str] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionCombustible() as db:
        data, total = db.listar(q=q, estado=estado, pagina=pagina, tamano=tamano)
    return {"data": data, "total": total, "page": pagina, "size": tamano}


@router_tarifas.post("/api/config/combustible", status_code=201)
def crear_combustible(payload: CombustibleIn):
    with GestionCombustible() as db:
        try:
            return db.crear(combustible=payload.combustible)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.put("/api/config/combustible/{id}")
def actualizar_combustible(id: int, payload: CombustibleIn):
    with GestionCombustible() as db:
        try:
            return db.actualizar(id=id, combustible=payload.combustible)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@router_tarifas.patch("/api/config/combustible/{id}/estado")
def cambiar_estado_combustible(id: int, payload: EstadoIn):
    with GestionCombustible() as db:
        try:
            return db.cambiar_estado(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
