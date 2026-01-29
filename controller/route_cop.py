from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field
from model.gestion_cop import GestionCOP

# Crear router
router_cop = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Función localmente para validar usuario
def get_user_session(req: Request):
    return req.session.get('user')

# --- RUTA PRINCIPAL CENTRO DE OPERACIÓN ---
# ----------------------------------------------------------------------
@router_cop.get("/centros_operacion", response_class=HTMLResponse)
def cop(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "centro_operacion.html",
        {
            "request": req,
            "user_session": user_session,
            "componentes": {}
        }
    )

# --------- Schemas Pydantic ---------
class EstadoIn(BaseModel):
    estado: int = Field(..., ge=0, le=1)

class ComponenteIn(BaseModel):
    componente: str = Field(..., min_length=1)
    estado: int = Field(1, ge=0, le=1)

class ZonaIn(BaseModel):
    zona: str = Field(..., min_length=1)
    estado: int = Field(1, ge=0, le=1)

class COPIn(BaseModel):
    cop: str = Field(..., min_length=1)
    id_componente: int
    id_zona: int
    estado: int = Field(1, ge=0, le=1)

# ---------- Helpers ----------
def paginate_params(page: int = Query(1, ge=1), size: int = Query(10, ge=1, le=500)):
    return page, size

# ---------- Endpoints COMPONENTE ----------
@router_cop.get("/api/config/componentes")
def list_componentes(
    q: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    page_size = Depends(paginate_params)
):
    page, size = page_size
    db = GestionCOP()
    try:
        data, total = db.list_componentes(q=q, estado=estado, page=page, size=size)
        return {"data": data, "total": total, "page": page, "size": size}
    finally:
        db.cerrar_conexion()

@router_cop.post("/api/config/componentes", status_code=201)
def create_componente(payload: ComponenteIn):
    db = GestionCOP()
    try:
        row = db.create_componente(componente=payload.componente, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.put("/api/config/componentes/{id}")
def update_componente(id: int, payload: ComponenteIn):
    db = GestionCOP()
    try:
        row = db.update_componente(id=id, componente=payload.componente, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.patch("/api/config/componentes/{id}/estado")
def toggle_componente(id: int, payload: EstadoIn):
    db = GestionCOP()
    try:
        return db.toggle_estado(table="componente", id=id, estado=payload.estado)
    finally:
        db.cerrar_conexion()

# ---------- Endpoints ZONA ----------
@router_cop.get("/api/config/zonas")
def list_zonas(
    q: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    page_size = Depends(paginate_params)
):
    page, size = page_size
    db = GestionCOP()
    try:
        data, total = db.list_zonas(q=q, estado=estado, page=page, size=size)
        return {"data": data, "total": total, "page": page, "size": size}
    finally:
        db.cerrar_conexion()

@router_cop.post("/api/config/zonas", status_code=201)
def create_zona(payload: ZonaIn):
    db = GestionCOP()
    try:
        row = db.create_zona(zona=payload.zona, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.put("/api/config/zonas/{id}")
def update_zona(id: int, payload: ZonaIn):
    db = GestionCOP()
    try:
        row = db.update_zona(id=id, zona=payload.zona, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.patch("/api/config/zonas/{id}/estado")
def toggle_zona(id: int, payload: EstadoIn):
    db = GestionCOP()
    try:
        return db.toggle_estado(table="zona", id=id, estado=payload.estado)
    finally:
        db.cerrar_conexion()

# ---------- Endpoints COP ----------
@router_cop.get("/api/config/cop")
def list_cop(
    q: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    id_componente: Optional[int] = None,
    id_zona: Optional[int] = None,
    page_size = Depends(paginate_params)
):
    page, size = page_size
    db = GestionCOP()
    try:
        data, total = db.list_cop(q=q, estado=estado, id_componente=id_componente, id_zona=id_zona, page=page, size=size)
        return {"data": data, "total": total, "page": page, "size": size}
    finally:
        db.cerrar_conexion()

@router_cop.post("/api/config/cop", status_code=201)
def create_cop(payload: COPIn):
    db = GestionCOP()
    try:
        row = db.create_cop(cop=payload.cop, id_componente=payload.id_componente, id_zona=payload.id_zona, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.put("/api/config/cop/{id}")
def update_cop(id: int, payload: COPIn):
    db = GestionCOP()
    try:
        row = db.update_cop(id=id, cop=payload.cop, id_componente=payload.id_componente, id_zona=payload.id_zona, estado=payload.estado)
        return row
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_cop.patch("/api/config/cop/{id}/estado")
def toggle_cop(id: int, payload: EstadoIn):
    db = GestionCOP()
    try:
        return db.toggle_estado(table="cop", id=id, estado=payload.estado)
    finally:
        db.cerrar_conexion()
        