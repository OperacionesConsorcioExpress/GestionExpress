from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field
from model.gestion_sne_motivos import GestionSneMotivos

router_sne_motivos = APIRouter()
templates = Jinja2Templates(directory="./view")

def obtener_sesion_usuario(req: Request):
    return req.session.get("user")

# ── Esquemas ──────────────────────────────────────────────────────────────────
class MotivoIn(BaseModel):
    observacion: str = Field(..., min_length=2, max_length=255)
    id_responsable: int = Field(..., ge=1)

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

# ── API Motivos ───────────────────────────────────────────────────────────────
@router_sne_motivos.get("/api/sne/motivos")
def listar_motivos(
    q: Optional[str] = None,
    id_responsable: Optional[int] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    db = GestionSneMotivos()
    try:
        data, total = db.listar_motivos(
            q=q, id_responsable=id_responsable, pagina=pagina, tamano=tamano
        )
        return {"data": data, "total": total, "page": pagina, "size": tamano}
    finally:
        db.cerrar_conexion()

@router_sne_motivos.post("/api/sne/motivos", status_code=201)
def crear_motivo(payload: MotivoIn):
    db = GestionSneMotivos()
    try:
        return db.crear_motivo(
            observacion=payload.observacion,
            id_responsable=payload.id_responsable,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_sne_motivos.put("/api/sne/motivos/{id}")
def actualizar_motivo(id: int, payload: MotivoIn):
    db = GestionSneMotivos()
    try:
        return db.actualizar_motivo(
            id=id,
            observacion=payload.observacion,
            id_responsable=payload.id_responsable,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_sne_motivos.delete("/api/sne/motivos/{id}")
def eliminar_motivo(id: int):
    db = GestionSneMotivos()
    try:
        return db.eliminar_motivo(id=id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

# ── Datos de apoyo ────────────────────────────────────────────────────────────
@router_sne_motivos.get("/api/sne/motivos/responsables")
def listar_responsables():
    db = GestionSneMotivos()
    try:
        return {"data": db.listar_responsables()}
    finally:
        db.cerrar_conexion()

# ── Sincronización desde sne.ics ──────────────────────────────────────────────
@router_sne_motivos.post("/api/sne/motivos/sincronizar")
def sincronizar_desde_ics():
    """
    Detecta observaciones nuevas en sne.ics que no estén en
    sne.motivos_eliminacion e inserta las faltantes con responsable = NULL.
    """
    db = GestionSneMotivos()
    try:
        resultado = db.sincronizar_desde_ics()
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.cerrar_conexion()