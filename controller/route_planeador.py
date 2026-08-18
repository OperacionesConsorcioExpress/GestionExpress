import os
from datetime import date, datetime, timedelta
from fastapi import APIRouter, Request, Depends, Query, HTTPException, File, UploadFile
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from typing import Optional
from pydantic import BaseModel
from model.gestion_usuarios import HandleDB
from model.gestion_planeador import Gestionplaneador
from model.gestion_blobstorage import ContainerModel

router_planeador = APIRouter(prefix="/planeador", tags=["planeador"])
templates = Jinja2Templates(directory="./view")

CONTAINER_PLANEADOR = "b01-gestion-express"
container_model = ContainerModel()

NIVELES = {"lector": 1, "editor": 2, "admin": 3}

def get_user_session(req: Request):
    return req.session.get("user")

def require_session(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesion no valida")
    return user

def _check_permiso(db: Gestionplaneador, equipo_id: int, user: dict, minimo: str):
    rol = db.obtener_rol_usuario(equipo_id, user["id"])
    if rol is None or NIVELES.get(rol, 0) < NIVELES[minimo]:
        raise HTTPException(status_code=403, detail="No tiene permisos suficientes en este equipo")
    return rol

def _ensure_container():
    pass  # b01-gestion-express ya existe en Azure

@router_planeador.get("/planeador", response_class=HTMLResponse)
def planeador(
    req: Request,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "planeador.html",
        {"request": req, "user_session": user_session},
    )

# ─────────────────────────────────────────────
# Equipos / miembros
# ─────────────────────────────────────────────
class EquipoIn(BaseModel):
    nombre: str
    area: Optional[str] = None
    descripcion: Optional[str] = None
    color: Optional[str] = None

class MiembroIn(BaseModel):
    usuario_id: int
    rol_equipo: str

@router_planeador.get("/usuarios")
def listar_usuarios(user=Depends(require_session)):
    """Lista usuarios activos del sistema para el selector de miembros del equipo."""
    rows = HandleDB().get_all_users()
    return [
        {"id": r[0], "nombres": r[1], "apellidos": r[2], "username": r[3], "estado": r[5]}
        for r in rows if r[5] == 1
    ]

@router_planeador.get("/equipos")
def listar_equipos(user=Depends(require_session)):
    with Gestionplaneador() as db:
        return db.listar_equipos(user["id"])

@router_planeador.post("/equipos")
def crear_equipo(data: EquipoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        return db.crear_equipo(data.nombre, data.area, data.descripcion, data.color, user["id"])

@router_planeador.put("/equipos/{equipo_id}")
def actualizar_equipo(equipo_id: int, data: EquipoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        return db.actualizar_equipo(equipo_id, data.nombre, data.area, data.descripcion, data.color)

@router_planeador.delete("/equipos/{equipo_id}")
def eliminar_equipo(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_equipo(equipo_id)
        return {"message": "Equipo eliminado"}

@router_planeador.get("/equipos/{equipo_id}/miembros")
def listar_miembros(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_miembros(equipo_id)

@router_planeador.post("/equipos/{equipo_id}/miembros")
def agregar_miembro(equipo_id: int, data: MiembroIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        return db.agregar_miembro(equipo_id, data.usuario_id, data.rol_equipo)

@router_planeador.put("/equipos/{equipo_id}/miembros/{usuario_id}")
def actualizar_rol_miembro(equipo_id: int, usuario_id: int, data: MiembroIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        return db.actualizar_rol_miembro(equipo_id, usuario_id, data.rol_equipo)

@router_planeador.delete("/equipos/{equipo_id}/miembros/{usuario_id}")
def eliminar_miembro(equipo_id: int, usuario_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_miembro(equipo_id, usuario_id)
        return {"message": "Miembro eliminado"}

# ─────────────────────────────────────────────
# Proyectos
# ─────────────────────────────────────────────
class ProyectoIn(BaseModel):
    nombre: str
    descripcion: Optional[str] = None
    estado: Optional[str] = "activo"
    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    color: Optional[str] = None
    responsable_id: Optional[int] = None

@router_planeador.get("/equipos/{equipo_id}/proyectos")
def listar_proyectos(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_proyectos(equipo_id)

@router_planeador.post("/equipos/{equipo_id}/proyectos")
def crear_proyecto(equipo_id: int, data: ProyectoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_proyecto(equipo_id, data.nombre, data.descripcion, data.estado,
                                  data.fecha_inicio, data.fecha_fin, data.color, data.responsable_id, user["id"])

@router_planeador.put("/proyectos/{proyecto_id}")
def actualizar_proyecto(proyecto_id: int, data: ProyectoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_proyecto(proyecto_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.actualizar_proyecto(proyecto_id, data.nombre, data.descripcion, data.estado,
                                       data.fecha_inicio, data.fecha_fin, data.color, data.responsable_id)

@router_planeador.delete("/proyectos/{proyecto_id}")
def eliminar_proyecto(proyecto_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_proyecto(proyecto_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_proyecto(proyecto_id)
        return {"message": "Proyecto eliminado"}

# ─────────────────────────────────────────────
# Tablero / columnas / tarjetas
# ─────────────────────────────────────────────
class ColumnaIn(BaseModel):
    nombre: str
    orden: Optional[int] = None
    wip_limit: Optional[int] = None
    color: Optional[str] = None

class TarjetaIn(BaseModel):
    columna_id: Optional[int] = None
    proyecto_id: Optional[int] = None
    titulo: Optional[str] = None
    descripcion: Optional[str] = None
    asignado_a: Optional[int] = None
    prioridad: Optional[str] = None
    etiquetas: Optional[list] = None
    fecha_limite: Optional[date] = None
    fecha_inicio: Optional[date] = None
    predecesora_id: Optional[int] = None
    orden: Optional[int] = None
    archivado: Optional[bool] = None

class TarjetaCrearIn(BaseModel):
    columna_id: int
    proyecto_id: Optional[int] = None
    titulo: str
    descripcion: Optional[str] = None
    asignado_a: Optional[int] = None
    prioridad: Optional[str] = "media"
    etiquetas: Optional[list] = None
    fecha_limite: Optional[date] = None
    fecha_inicio: Optional[date] = None
    predecesora_id: Optional[int] = None

class ChecklistIn(BaseModel):
    texto: Optional[str] = None
    completado: Optional[bool] = None

class ComentarioIn(BaseModel):
    texto: str

@router_planeador.get("/equipos/{equipo_id}/tablero")
def obtener_tablero(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.obtener_tablero(equipo_id)

@router_planeador.get("/equipos/{equipo_id}/tablero/resumen-personas")
def resumen_tablero_por_persona(equipo_id: int, anio: Optional[int] = None, mes: Optional[int] = None, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.resumen_tablero_por_persona(equipo_id, anio, mes)

@router_planeador.get("/equipos/{equipo_id}/tablero/fechas-inicio")
def fechas_inicio_tablero(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.fechas_inicio_disponibles(equipo_id)

@router_planeador.post("/equipos/{equipo_id}/columnas")
def crear_columna(equipo_id: int, data: ColumnaIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_columna(equipo_id, data.nombre, data.wip_limit, data.color)

@router_planeador.put("/columnas/{columna_id}")
def actualizar_columna(columna_id: int, data: ColumnaIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_columna(columna_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.actualizar_columna(columna_id, data.nombre, data.orden, data.wip_limit, data.color)

@router_planeador.delete("/columnas/{columna_id}")
def eliminar_columna(columna_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_columna(columna_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_columna(columna_id)
        return {"message": "Columna eliminada"}

@router_planeador.post("/tarjetas")
def crear_tarjeta(data: TarjetaCrearIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_columna(data.columna_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_tarjeta(data.columna_id, data.proyecto_id, data.titulo, data.descripcion,
                                 data.asignado_a, data.prioridad, data.etiquetas, data.fecha_limite,
                                 user["id"], data.fecha_inicio, data.predecesora_id)

@router_planeador.put("/tarjetas/{tarjeta_id}")
def actualizar_tarjeta(tarjeta_id: int, data: TarjetaIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        _check_permiso(db, equipo_id, user, "editor")
        if data.columna_id:
            destino_equipo = db.obtener_equipo_de_columna(data.columna_id)
            if destino_equipo != equipo_id:
                raise HTTPException(status_code=400, detail="La columna destino no pertenece al mismo equipo")
        campos = data.dict(exclude_unset=True)
        return db.actualizar_tarjeta(tarjeta_id, **campos)

@router_planeador.delete("/tarjetas/{tarjeta_id}")
def eliminar_tarjeta(tarjeta_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_tarjeta(tarjeta_id)
        return {"message": "Tarjeta eliminada"}

@router_planeador.post("/tarjetas/{tarjeta_id}/checklist")
def crear_checklist_item(tarjeta_id: int, data: ChecklistIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_checklist_item(tarjeta_id, data.texto)

@router_planeador.put("/checklist/{item_id}")
def actualizar_checklist_item(item_id: int, data: ChecklistIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_checklist_item(item_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.actualizar_checklist_item(item_id, data.texto, data.completado)

@router_planeador.delete("/checklist/{item_id}")
def eliminar_checklist_item(item_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_checklist_item(item_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_checklist_item(item_id)
        return {"message": "Item eliminado"}

@router_planeador.get("/tarjetas/{tarjeta_id}/comentarios")
def listar_comentarios(tarjeta_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_comentarios(tarjeta_id)

@router_planeador.post("/tarjetas/{tarjeta_id}/comentarios")
def crear_comentario(tarjeta_id: int, data: ComentarioIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_comentario(tarjeta_id, user["id"], data.texto)

# ─────────────────────────────────────────────
# Recurrentes
# ─────────────────────────────────────────────
class RecurrenteIn(BaseModel):
    titulo: str
    descripcion: Optional[str] = None
    frecuencia: str
    dias_semana: Optional[list] = None
    dia_mes: Optional[int] = None
    mes_inicio: Optional[int] = None
    hora: Optional[str] = None
    responsable_id: int
    proyecto_id: Optional[int] = None
    duracion_minutos: Optional[int] = 30

class RecurrenteUpdateIn(BaseModel):
    titulo: Optional[str] = None
    descripcion: Optional[str] = None
    frecuencia: Optional[str] = None
    dias_semana: Optional[list] = None
    dia_mes: Optional[int] = None
    mes_inicio: Optional[int] = None
    hora: Optional[str] = None
    responsable_id: Optional[int] = None
    proyecto_id: Optional[int] = None
    activo: Optional[bool] = None
    duracion_minutos: Optional[int] = None
    recalcular: Optional[bool] = False

class EjecucionIn(BaseModel):
    estado: str
    observaciones: Optional[str] = None

@router_planeador.get("/equipos/{equipo_id}/recurrentes")
def listar_recurrentes(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_recurrentes(equipo_id)

@router_planeador.get("/equipos/{equipo_id}/recurrentes/resumen")
def listar_recurrentes_resumen(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_recurrentes_resumen(equipo_id)

@router_planeador.get("/equipos/{equipo_id}/recurrentes/carga")
def resumen_carga_equipo(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.resumen_carga_equipo(equipo_id)

@router_planeador.get("/recurrentes/{actividad_id}/ejecuciones")
def historial_ejecuciones(actividad_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_recurrente(actividad_id)
        _check_permiso(db, equipo_id, user, "lector")
        return db.historial_ejecuciones(actividad_id)

@router_planeador.post("/equipos/{equipo_id}/recurrentes")
def crear_recurrente(equipo_id: int, data: RecurrenteIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        try:
            return db.crear_recurrente(equipo_id, data.titulo, data.descripcion, data.frecuencia,
                                        data.dias_semana, data.dia_mes, data.mes_inicio, data.hora,
                                        data.responsable_id, data.proyecto_id, data.duracion_minutos, user["id"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

@router_planeador.put("/recurrentes/{actividad_id}")
def actualizar_recurrente(actividad_id: int, data: RecurrenteUpdateIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_recurrente(actividad_id)
        _check_permiso(db, equipo_id, user, "editor")
        campos = data.dict(exclude_unset=True)
        recalcular = campos.pop("recalcular", False)
        row = db.actualizar_recurrente(actividad_id, **campos)
        if recalcular:
            db.recalcular_ejecuciones_actividad(actividad_id)
        return row

@router_planeador.delete("/recurrentes/{actividad_id}")
def eliminar_recurrente(actividad_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_recurrente(actividad_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_recurrente(actividad_id)
        return {"message": "Actividad recurrente eliminada"}

@router_planeador.post("/equipos/{equipo_id}/recurrentes/migrar-dias-habiles")
def migrar_dias_habiles(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "admin")
        actualizadas = db.migrar_ejecuciones_a_dias_habiles(equipo_id)
        return {"message": f"{actualizadas} ejecuciones movidas al siguiente día hábil", "actualizadas": actualizadas}

@router_planeador.get("/equipos/{equipo_id}/recurrentes/ejecuciones")
def obtener_ejecuciones(
    equipo_id: int,
    desde: Optional[date] = Query(None),
    hasta: Optional[date] = Query(None),
    user=Depends(require_session),
):
    hoy = date.today()
    desde = desde or (hoy - timedelta(days=7))
    hasta = hasta or hoy
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.obtener_ejecuciones(equipo_id, desde, hasta)

@router_planeador.post("/ejecuciones/{ejecucion_id}/completar")
def completar_ejecucion(ejecucion_id: int, data: EjecucionIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_ejecucion(ejecucion_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.marcar_ejecucion(ejecucion_id, data.estado, user["id"], data.observaciones)

# ─────────────────────────────────────────────
# Notas / archivos
# ─────────────────────────────────────────────
class NotaIn(BaseModel):
    titulo: str
    contenido: Optional[str] = None
    color: Optional[str] = None

class NotaUpdateIn(BaseModel):
    titulo: Optional[str] = None
    contenido: Optional[str] = None
    fijado: Optional[bool] = None
    color: Optional[str] = None

@router_planeador.get("/equipos/{equipo_id}/notas")
def listar_notas(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_notas(equipo_id)

@router_planeador.post("/equipos/{equipo_id}/notas")
def crear_nota(equipo_id: int, data: NotaIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_nota(equipo_id, data.titulo, data.contenido, user["id"], data.color)

@router_planeador.put("/notas/{nota_id}")
def actualizar_nota(nota_id: int, data: NotaUpdateIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_nota(nota_id)
        _check_permiso(db, equipo_id, user, "editor")
        return db.actualizar_nota(nota_id, user["id"], data.titulo, data.contenido, data.fijado, data.color)

@router_planeador.get("/notas/{nota_id}/historial")
def historial_nota(nota_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_nota(nota_id)
        _check_permiso(db, equipo_id, user, "lector")
        return db.obtener_historial_nota(nota_id)

@router_planeador.delete("/notas/{nota_id}")
def eliminar_nota(nota_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_nota(nota_id)
        _check_permiso(db, equipo_id, user, "admin")
        db.eliminar_nota(nota_id)
        return {"message": "Nota eliminada"}

@router_planeador.get("/equipos/{equipo_id}/archivos")
def listar_archivos(equipo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_archivos(equipo_id)

import re as _re

def _sanitizar_path(texto, fallback):
    return _re.sub(r"[^\w\-]", "_", texto).strip("_") or fallback

@router_planeador.post("/equipos/{equipo_id}/archivos")
async def subir_archivo(
    equipo_id: int,
    nota_id: Optional[int] = None,
    file: UploadFile = File(...),
    user=Depends(require_session),
):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        _ensure_container()
        nombre_equipo = _sanitizar_path(db.obtener_nombre_equipo(equipo_id), f"equipo_{equipo_id}")
        titulo_nota = _sanitizar_path(db.obtener_titulo_nota(nota_id) if nota_id else "sin_nota", f"nota_{nota_id}")
        fecha_carga = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_limpio = _re.sub(r"[^\w\-\.]", "_", file.filename)
        blob_path = f"planeador/notas/{nombre_equipo}/{titulo_nota}/{fecha_carga}_{nombre_limpio}"
        contents = await file.read()
        await container_model.upload_file(CONTAINER_PLANEADOR, blob_path, contents)
        return db.registrar_archivo(equipo_id, nota_id, file.filename, blob_path,
                                     len(contents), file.content_type, user["id"])

@router_planeador.get("/tarjetas/{tarjeta_id}/archivos")
def listar_archivos_tarjeta(tarjeta_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        if not equipo_id:
            raise HTTPException(status_code=404, detail="Tarjeta no encontrada")
        _check_permiso(db, equipo_id, user, "lector")
        return db.listar_archivos_tarjeta(tarjeta_id)

@router_planeador.post("/tarjetas/{tarjeta_id}/archivos")
async def subir_archivo_tarjeta(
    tarjeta_id: int,
    file: UploadFile = File(...),
    user=Depends(require_session),
):
    with Gestionplaneador() as db:
        equipo_id = db.obtener_equipo_de_tarjeta(tarjeta_id)
        if not equipo_id:
            raise HTTPException(status_code=404, detail="Tarjeta no encontrada")
        _check_permiso(db, equipo_id, user, "editor")
        _ensure_container()
        nombre_equipo = _sanitizar_path(db.obtener_nombre_equipo(equipo_id), f"equipo_{equipo_id}")
        titulo_tarjeta = _sanitizar_path(db.obtener_titulo_tarjeta(tarjeta_id), f"tarjeta_{tarjeta_id}")
        fecha_carga = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_limpio = _re.sub(r"[^\w\-\.]", "_", file.filename)
        blob_path = f"planeador/scrumban/{nombre_equipo}/{titulo_tarjeta}/{fecha_carga}_{nombre_limpio}"
        contents = await file.read()
        await container_model.upload_file(CONTAINER_PLANEADOR, blob_path, contents)
        return db.registrar_archivo_tarjeta(equipo_id, tarjeta_id, file.filename, blob_path,
                                             len(contents), file.content_type, user["id"])

@router_planeador.get("/archivos/{archivo_id}/download")
def descargar_archivo(archivo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        archivo = db.obtener_archivo(archivo_id)
        if not archivo:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        _check_permiso(db, archivo["equipo_id"], user, "lector")
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{archivo['nombre_archivo']}",
            "Cache-Control": "no-store",
        }
        return StreamingResponse(
            container_model.stream_file(CONTAINER_PLANEADOR, archivo["blob_path"]),
            media_type=archivo["content_type"] or "application/octet-stream",
            headers=headers,
        )

@router_planeador.delete("/archivos/{archivo_id}")
def eliminar_archivo(archivo_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        archivo = db.obtener_archivo(archivo_id)
        if not archivo:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        _check_permiso(db, archivo["equipo_id"], user, "admin")
        try:
            container_model.delete_file(CONTAINER_PLANEADOR, archivo["blob_path"])
        except Exception:
            pass
        db.eliminar_archivo(archivo_id)
        return {"message": "Archivo eliminado"}

# ── Calendario ──────────────────────────────────────────────────────────────

class EventoIn(BaseModel):
    titulo: str
    observacion: Optional[str] = None
    fecha: str

@router_planeador.get("/equipos/{equipo_id}/calendario")
def obtener_calendario(equipo_id: int, desde: str, hasta: str, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "lector")
        return db.obtener_datos_calendario(equipo_id, desde, hasta)

@router_planeador.post("/equipos/{equipo_id}/eventos")
def crear_evento(equipo_id: int, data: EventoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        _check_permiso(db, equipo_id, user, "editor")
        return db.crear_evento_calendario(equipo_id, data.titulo, data.observacion, data.fecha, user["id"])

@router_planeador.put("/eventos/{evento_id}")
def actualizar_evento(evento_id: int, data: EventoIn, user=Depends(require_session)):
    with Gestionplaneador() as db:
        db.cursor.execute("SELECT equipo_id FROM planer.eventos_calendario WHERE id=%s", (evento_id,))
        row = db.cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Evento no encontrado")
        _check_permiso(db, row["equipo_id"], user, "editor")
        return db.actualizar_evento_calendario(evento_id, data.titulo, data.observacion, data.fecha)

@router_planeador.delete("/eventos/{evento_id}")
def eliminar_evento(evento_id: int, user=Depends(require_session)):
    with Gestionplaneador() as db:
        db.cursor.execute("SELECT equipo_id FROM planer.eventos_calendario WHERE id=%s", (evento_id,))
        row = db.cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Evento no encontrado")
        _check_permiso(db, row["equipo_id"], user, "admin")
        db.eliminar_evento_calendario(evento_id)
        return {"message": "Evento eliminado"}
