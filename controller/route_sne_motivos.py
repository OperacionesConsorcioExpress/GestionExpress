from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field
from model.gestion_sne_motivos import (
    GestionSneMotivos,
    GestionMotivosNotas,
    GestionResponsableSne,
    GestionJustificacion,
)

router_sne_motivos = APIRouter()
templates = Jinja2Templates(directory="./view")

def obtener_sesion_usuario(req: Request):
    return req.session.get("user")

# ── Esquemas comunes ──────────────────────────────────────────────────────────
class EstadoIn(BaseModel):
    estado: bool

def parametros_paginacion(
    pagina: int = Query(1, ge=1, alias="pagina"),
    tamano: int = Query(10, ge=1, le=500, alias="tamano"),
):
    return pagina, tamano

# ── Vista principal ───────────────────────────────────────────────────────────
@router_sne_motivos.get("/sne_motivos", response_class=HTMLResponse)
def vista_sne_motivos(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "sne_motivos.html",
        {"request": req, "user_session": user_session},
    )

# ══════════════════════════════════════════════════════════════════════════════
# API Motivos SNE  (sne.motivos_eliminacion)
# ══════════════════════════════════════════════════════════════════════════════
class MotivoIn(BaseModel):
    motivo: str = Field(..., min_length=2, max_length=255)
    id_responsable: int = Field(..., ge=1)


@router_sne_motivos.get("/api/sne/motivos")
def listar_motivos(
    q: Optional[str] = None,
    id_responsable: Optional[int] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionSneMotivos() as db:
        data, total = db.listar_motivos(
            q=q, id_responsable=id_responsable, estado=estado,
            pagina=pagina, tamano=tamano,
        )
    return {"data": data, "total": total, "page": pagina, "size": tamano}

@router_sne_motivos.post("/api/sne/motivos", status_code=201)
def crear_motivo(payload: MotivoIn):
    with GestionSneMotivos() as db:
        try:
            return db.crear_motivo(
                motivo=payload.motivo,
                id_responsable=payload.id_responsable,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.put("/api/sne/motivos/{id}")
def actualizar_motivo(id: int, payload: MotivoIn):
    with GestionSneMotivos() as db:
        try:
            return db.actualizar_motivo(
                id=id,
                motivo=payload.motivo,
                id_responsable=payload.id_responsable,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.patch("/api/sne/motivos/{id}/estado")
def cambiar_estado_motivo(id: int, payload: EstadoIn):
    with GestionSneMotivos() as db:
        try:
            return db.cambiar_estado_motivo(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.get("/api/sne/motivos/responsables")
def listar_responsables():
    with GestionSneMotivos() as db:
        return {"data": db.listar_responsables()}

@router_sne_motivos.post("/api/sne/motivos/sincronizar")
def sincronizar_desde_ics():
    with GestionSneMotivos() as db:
        try:
            return db.sincronizar_desde_ics()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════════════════
# API Motivos Notas  (sne.motivos_notas)
# ══════════════════════════════════════════════════════════════════════════════
class MotivoNotaIn(BaseModel):
    motivo_nota: str = Field(..., min_length=2, max_length=255)

@router_sne_motivos.get("/api/sne/motivos-notas")
def listar_motivos_notas(
    q: Optional[str] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionMotivosNotas() as db:
        data, total = db.listar(q=q, estado=estado, pagina=pagina, tamano=tamano)
    return {"data": data, "total": total, "page": pagina, "size": tamano}

@router_sne_motivos.post("/api/sne/motivos-notas", status_code=201)
def crear_motivo_nota(payload: MotivoNotaIn):
    with GestionMotivosNotas() as db:
        try:
            return db.crear(motivo_nota=payload.motivo_nota)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.put("/api/sne/motivos-notas/{id}")
def actualizar_motivo_nota(id: int, payload: MotivoNotaIn):
    with GestionMotivosNotas() as db:
        try:
            return db.actualizar(id=id, motivo_nota=payload.motivo_nota)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.patch("/api/sne/motivos-notas/{id}/estado")
def cambiar_estado_nota(id: int, payload: EstadoIn):
    with GestionMotivosNotas() as db:
        try:
            return db.cambiar_estado(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

# ══════════════════════════════════════════════════════════════════════════════
# API Responsable  (sne.responsable_sne)
# ══════════════════════════════════════════════════════════════════════════════
class ResponsableIn(BaseModel):
    responsable: str = Field(..., min_length=2, max_length=255)

@router_sne_motivos.get("/api/sne/responsable")
def listar_responsable_sne(
    q: Optional[str] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionResponsableSne() as db:
        data, total = db.listar(q=q, estado=estado, pagina=pagina, tamano=tamano)
    return {"data": data, "total": total, "page": pagina, "size": tamano}

@router_sne_motivos.post("/api/sne/responsable", status_code=201)
def crear_responsable_sne(payload: ResponsableIn):
    with GestionResponsableSne() as db:
        try:
            return db.crear(responsable=payload.responsable)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.put("/api/sne/responsable/{id}")
def actualizar_responsable_sne(id: int, payload: ResponsableIn):
    with GestionResponsableSne() as db:
        try:
            return db.actualizar(id=id, responsable=payload.responsable)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.patch("/api/sne/responsable/{id}/estado")
def cambiar_estado_responsable(id: int, payload: EstadoIn):
    with GestionResponsableSne() as db:
        try:
            return db.cambiar_estado(id=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

# ══════════════════════════════════════════════════════════════════════════════
# API Justificación  (sne.justificacion  ←→  sne.acciones)
# ══════════════════════════════════════════════════════════════════════════════
class JustificacionIn(BaseModel):
    justificacion: str = Field(..., min_length=2, max_length=500)
    id_acc: int = Field(..., ge=1)

@router_sne_motivos.get("/api/sne/justificacion")
def listar_justificacion(
    q: Optional[str] = None,
    id_acc: Optional[int] = None,
    estado: Optional[bool] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    with GestionJustificacion() as db:
        data, total = db.listar(
            q=q, id_acc=id_acc, estado=estado, pagina=pagina, tamano=tamano
        )
    return {"data": data, "total": total, "page": pagina, "size": tamano}

@router_sne_motivos.post("/api/sne/justificacion", status_code=201)
def crear_justificacion(payload: JustificacionIn):
    with GestionJustificacion() as db:
        try:
            return db.crear(justificacion=payload.justificacion, id_acc=payload.id_acc)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.put("/api/sne/justificacion/{id}")
def actualizar_justificacion(id: int, payload: JustificacionIn):
    with GestionJustificacion() as db:
        try:
            return db.actualizar(
                id_justificacion=id,
                justificacion=payload.justificacion,
                id_acc=payload.id_acc,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.patch("/api/sne/justificacion/{id}/estado")
def cambiar_estado_justificacion(id: int, payload: EstadoIn):
    with GestionJustificacion() as db:
        try:
            return db.cambiar_estado(id_justificacion=id, estado=payload.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

@router_sne_motivos.get("/api/sne/acciones")
def listar_acciones():
    with GestionJustificacion() as db:
        return {"data": db.listar_acciones()}
