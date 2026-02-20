from fastapi import APIRouter, Request, HTTPException, Depends, Query, UploadFile, File
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from pydantic import BaseModel, Field
from model.gestion_rutas import GestionRutas

# Router del módulo
router_rutas = APIRouter()

# Plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Sesión de usuario (validación local)
def obtener_sesion_usuario(req: Request):
    return req.session.get("user")

# ── Esquemas ─────────────────────────────────────────────────────────────────
class EstadoIn(BaseModel):
    estado: int = Field(..., ge=0, le=1)

class RutaIn(BaseModel):
    id_linea: int = Field(..., ge=1)
    ruta_comercial: str = Field(..., min_length=1, max_length=100)
    id_cop: int
    estado: int = Field(1, ge=0, le=1)

def parametros_paginacion(
    pagina: int = Query(1, ge=1, alias="pagina"),
    tamano: int = Query(10, ge=1, le=500, alias="tamano"),
):
    return pagina, tamano

# ── Vista principal ───────────────────────────────────────────────────────────
@router_rutas.get("/rutas", response_class=HTMLResponse)
def vista_rutas(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "rutas.html",
        {
            "request": req,
            "user_session": user_session,
            "componentes": {},
        },
    )

# ── API Rutas ─────────────────────────────────────────────────────────────────
@router_rutas.get("/api/config/rutas")
def listar_rutas(
    q: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    id_cop: Optional[int] = None,
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    pagina_tamano=Depends(parametros_paginacion),
):
    pagina, tamano = pagina_tamano
    db = GestionRutas()
    try:
        data, total = db.listar_rutas(
            q=q,
            estado=estado,
            id_cop=id_cop,
            componente=componente,
            zona=zona,
            pagina=pagina,
            tamano=tamano,
        )
        return {"data": data, "total": total, "page": pagina, "size": tamano}
    finally:
        db.cerrar_conexion()

@router_rutas.post("/api/config/rutas", status_code=201)
def crear_ruta(payload: RutaIn):
    db = GestionRutas()
    try:
        fila = db.crear_ruta(**payload.model_dump())
        return fila
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_rutas.put("/api/config/rutas/{id}")
def actualizar_ruta(id: int, payload: RutaIn):
    db = GestionRutas()
    try:
        fila = db.actualizar_ruta(id=id, **payload.model_dump())
        return fila
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_rutas.patch("/api/config/rutas/{id}/estado")
def cambiar_estado_ruta(id: int, payload: EstadoIn):
    db = GestionRutas()
    try:
        return db.cambiar_estado(id=id, estado=payload.estado)
    finally:
        db.cerrar_conexion()

# ── API de apoyo (COP / filtros) ──────────────────────────────────────────────
@router_rutas.get("/api/config/rutas/cop")
def listar_cop(
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    estado: Optional[int] = 1,
):
    db = GestionRutas()
    try:
        data = db.listar_cop(componente=componente, zona=zona, estado=estado)
        return {"data": data}
    finally:
        db.cerrar_conexion()

@router_rutas.get("/api/config/rutas/filtros/componentes")
def filtros_componentes():
    db = GestionRutas()
    try:
        return {"data": db.listar_componentes()}
    finally:
        db.cerrar_conexion()

@router_rutas.get("/api/config/rutas/filtros/zonas")
def filtros_zonas(componente: Optional[str] = None):
    db = GestionRutas()
    try:
        return {"data": db.listar_zonas(componente=componente)}
    finally:
        db.cerrar_conexion()

# ── Carga Masiva (CSV o XLSX) ─────────────────────────────────────────────────
@router_rutas.post("/api/config/rutas/upload")
async def cargar_rutas_masivo(file: UploadFile = File(...)):
    nombre = file.filename.lower()
    contenido = await file.read()
    db = GestionRutas()
    try:
        if nombre.endswith(".csv"):
            reporte = db.carga_masiva_csv_bytes(contenido)
        elif nombre.endswith(".xlsx"):
            reporte = db.carga_masiva_xlsx_bytes(contenido)
        else:
            raise HTTPException(
                status_code=400, detail="Formato inválido. Usa .csv o .xlsx"
            )
        return reporte
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

# ── Previsualización (ensayo en seco) ─────────────────────────────────────────
@router_rutas.post("/api/config/rutas/upload/preview")
async def previsualizar_carga(file: UploadFile = File(...)):
    nombre = file.filename.lower()
    contenido = await file.read()
    db = GestionRutas()
    try:
        if nombre.endswith(".csv"):
            reporte = db.previsualizar_csv_bytes(contenido)
        elif nombre.endswith(".xlsx"):
            reporte = db.previsualizar_xlsx_bytes(contenido)
        else:
            raise HTTPException(
                status_code=400, detail="Formato inválido. Usa .csv o .xlsx"
            )
        return reporte
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()