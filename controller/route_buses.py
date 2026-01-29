from fastapi import APIRouter, Request, HTTPException, Depends, Query, UploadFile, File
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from model.gestion_buses import GestionBuses

# Router del módulo
router_buses = APIRouter()

# Plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Sesión de usuario (validación local)
def obtener_sesion_usuario(req: Request):
    return req.session.get('user')

# ---------- Esquemas ----------
class EstadoIn(BaseModel):
    estado: int = Field(..., ge=0, le=1)

class BusIn(BaseModel):
    placa: str = Field(..., min_length=5, max_length=10)
    no_interno: Optional[str] = Field(None, max_length=20)
    tipologia: Optional[str] = Field(None, max_length=30)
    modelo: Optional[int] = Field(None, ge=1990, le=2100)
    marca: Optional[str] = Field(None, max_length=40)
    linea: Optional[str] = Field(None, max_length=40)
    carroceria: Optional[str] = Field(None, max_length=40)
    combustible: str = Field(..., pattern="^(ACPM|GNV|ELECTRICO)$")
    tecnologia: Optional[str] = Field(None, max_length=20)
    id_cop: int
    estado: int = Field(1, ge=0, le=1)

def parametros_paginacion(
    pagina: int = Query(1, ge=1, alias="pagina"),
    tamano: int = Query(10, ge=1, le=500, alias="tamano")
):
    # Acepta también 'page' y 'size' como alias opcionales
    pagina = Query(1, ge=1, alias="page") if pagina is None else pagina
    tamano = Query(10, ge=1, le=500, alias="size") if tamano is None else tamano
    return pagina, tamano

# ---------- Vista principal ----------
@router_buses.get("/buses_cexp", response_class=HTMLResponse)
def vista_buses_cexp(req: Request, user_session: dict = Depends(obtener_sesion_usuario)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "buses_cexp.html",
        {
            "request": req,
            "user_session": user_session,
            "componentes": {}
        }
    )

# ---------- API Buses ----------
@router_buses.get("/api/config/buses_cexp")
def listar_buses_cexp(
    q: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    id_cop: Optional[int] = None,
    combustible: Optional[str] = None,
    tipologia: Optional[str] = None,
    modelo_desde: Optional[int] = Query(None, ge=1990, le=2100),
    modelo_hasta: Optional[int] = Query(None, ge=1990, le=2100),
    pagina_tamano = Depends(parametros_paginacion)
):
    pagina, tamano = pagina_tamano
    db = GestionBuses()
    try:
        data, total = db.listar_buses(
            q=q, estado=estado, id_cop=id_cop, combustible=combustible,
            tipologia=tipologia, modelo_desde=modelo_desde, modelo_hasta=modelo_hasta,
            pagina=pagina, tamano=tamano
        )
        return {"data": data, "total": total, "page": pagina, "size": tamano}
    finally:
        db.cerrar_conexion()

@router_buses.post("/api/config/buses_cexp", status_code=201)
def crear_bus(payload: BusIn):
    db = GestionBuses()
    try:
        fila = db.crear_bus(**payload.model_dump())
        return fila
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_buses.put("/api/config/buses_cexp/{id}")
def actualizar_bus(id: int, payload: BusIn):
    db = GestionBuses()
    try:
        fila = db.actualizar_bus(id=id, **payload.model_dump())
        return fila
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

@router_buses.patch("/api/config/buses_cexp/{id}/estado")
def cambiar_estado_bus(id: int, payload: EstadoIn):
    db = GestionBuses()
    try:
        return db.cambiar_estado(id=id, estado=payload.estado)
    finally:
        db.cerrar_conexion()

# ---------- API de apoyo (COP / filtros) ----------
@router_buses.get("/api/config/buses_cexp/cop")
def listar_cop(componente: Optional[str] = None, zona: Optional[str] = None, estado: Optional[int] = 1):
    db = GestionBuses()
    try:
        data = db.listar_cop(componente=componente, zona=zona, estado=estado)
        return {"data": data}
    finally:
        db.cerrar_conexion()

@router_buses.get("/api/config/buses_cexp/filtros/componentes")
def filtros_componentes():
    db = GestionBuses()
    try:
        return {"data": db.listar_componentes()}
    finally:
        db.cerrar_conexion()

@router_buses.get("/api/config/buses_cexp/filtros/zonas")
def filtros_zonas(componente: Optional[str] = None):
    db = GestionBuses()
    try:
        return {"data": db.listar_zonas(componente=componente)}
    finally:
        db.cerrar_conexion()

# ---------- Carga Masiva (CSV o XLSX) ----------
# IMPORTANTE: el nombre del parámetro DEBE ser 'file' porque el front envía 'file' en el FormData.
@router_buses.post("/api/config/buses_cexp/upload")
async def cargar_buses_masivo(file: UploadFile = File(...)):
    nombre = file.filename.lower()
    contenido = await file.read()
    db = GestionBuses()
    try:
        if nombre.endswith(".csv"):
            reporte = db.carga_masiva_csv_bytes(contenido)
        elif nombre.endswith(".xlsx"):
            reporte = db.carga_masiva_xlsx_bytes(contenido)
        else:
            raise HTTPException(status_code=400, detail="Formato inválido. Usa .csv o .xlsx")
        return reporte
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()

# ---------- Previsualización (ensayo en seco) ----------
@router_buses.post("/api/config/buses_cexp/upload/preview")
async def previsualizar_carga(file: UploadFile = File(...)):
    nombre = file.filename.lower()
    contenido = await file.read()
    db = GestionBuses()
    try:
        if nombre.endswith(".csv"):
            reporte = db.previsualizar_csv_bytes(contenido)
        elif nombre.endswith(".xlsx"):
            reporte = db.previsualizar_xlsx_bytes(contenido)
        else:
            raise HTTPException(status_code=400, detail="Formato inválido. Usa .csv o .xlsx")
        return reporte
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.cerrar_conexion()
