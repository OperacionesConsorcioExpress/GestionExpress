import json
import urllib.parse
from typing import List, Optional

from fastapi import APIRouter, Request, Depends, HTTPException, Query, Form, File, UploadFile
from starlette.responses import StreamingResponse

from model.gestion_sne_ficha_rutas import GestionSneFichaRutas

router_sne_ficha_rutas = APIRouter(prefix="/monitor", tags=["sne_ficha_rutas"])


def _require_sesion(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesión no válida")
    return user


def _nombre_usuario(user_session: dict) -> str:
    nombres = user_session.get("nombres") or user_session.get("nombre") or ""
    apellidos = user_session.get("apellidos") or ""
    nombre = f"{nombres} {apellidos}".strip()
    return nombre or user_session.get("usuario") or "Usuario"


# ══════════════════════════════════════════════════════════════════════════════
# RUTAS ACTIVAS (config.rutas) PARA LA PESTAÑA FICHA DE RUTAS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_ficha_rutas.get("/api/ficha-rutas/rutas")
def ficha_rutas_listar_rutas(
    q: Optional[str] = None,
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    id_cop: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneFichaRutas() as db:
        data = db.listar_rutas_activas(q=q, componente=componente, zona=zona, id_cop=id_cop)
    return {"ok": True, "data": data}


# ══════════════════════════════════════════════════════════════════════════════
# FICHAS / HISTORIAS POR RUTA
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_ficha_rutas.get("/api/ficha-rutas")
def ficha_rutas_listar(
    id_linea: Optional[int] = None,
    ruta_comercial: Optional[str] = None,
    estado: Optional[int] = Query(None, ge=0, le=1),
    q: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneFichaRutas() as db:
        data = db.listar_fichas(id_linea=id_linea, ruta_comercial=ruta_comercial, estado=estado, q=q)
    return {"ok": True, "data": data}


@router_sne_ficha_rutas.post("/api/ficha-rutas", status_code=201)
async def ficha_rutas_crear(
    id_linea: int = Form(...),
    ruta_comercial: str = Form(...),
    asunto: str = Form(...),
    observacion: str = Form(...),
    estado: int = Form(1),
    files: List[UploadFile] = File(default=[]),
    user_session: dict = Depends(_require_sesion),
):
    try:
        with GestionSneFichaRutas() as db:
            adjuntos = []
            for f in files:
                if not f or not f.filename:
                    continue
                contenido = await f.read()
                adjuntos.append(db.subir_adjunto_ficha(contenido, ruta_comercial, f.filename))

            fila = db.crear_ficha(
                id_linea=id_linea,
                ruta_comercial=ruta_comercial,
                asunto=asunto,
                observacion=observacion,
                estado=estado,
                usuario_publico=_nombre_usuario(user_session),
                adjuntos=adjuntos,
            )
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router_sne_ficha_rutas.put("/api/ficha-rutas/{id}")
async def ficha_rutas_actualizar(
    id: int,
    ruta_comercial: str = Form(...),
    asunto: str = Form(...),
    observacion: str = Form(...),
    estado: int = Form(...),
    adjuntos_existentes: str = Form("[]"),
    files: List[UploadFile] = File(default=[]),
    user_session: dict = Depends(_require_sesion),
):
    try:
        try:
            existentes = json.loads(adjuntos_existentes) or []
        except Exception:
            existentes = []

        with GestionSneFichaRutas() as db:
            nuevos = []
            for f in files:
                if not f or not f.filename:
                    continue
                contenido = await f.read()
                nuevos.append(db.subir_adjunto_ficha(contenido, ruta_comercial, f.filename))

            adjuntos = existentes + nuevos
            fila = db.actualizar_ficha(
                id=id,
                asunto=asunto,
                observacion=observacion,
                estado=estado,
                adjuntos=adjuntos,
            )
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router_sne_ficha_rutas.patch("/api/ficha-rutas/{id}/estado")
async def ficha_rutas_cambiar_estado(
    id: int,
    payload: dict,
    user_session: dict = Depends(_require_sesion),
):
    estado = int(payload.get("estado"))
    try:
        with GestionSneFichaRutas() as db:
            fila = db.cambiar_estado_ficha(id=id, estado=estado)
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# ADJUNTOS (blob storage)
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_ficha_rutas.get("/api/ficha-rutas/adjunto")
def ficha_rutas_descargar_adjunto(
    ruta: str = Query(..., description="Ruta del blob dentro del contenedor sne-fichas-rutas"),
    user_session: dict = Depends(_require_sesion),
):
    try:
        with GestionSneFichaRutas() as db:
            contenido, content_type, nombre_archivo = db.descargar_adjunto_ficha(ruta)
        return StreamingResponse(
            iter([contenido]),
            media_type=content_type,
            headers={"Content-Disposition": "inline; filename*=UTF-8''" + urllib.parse.quote(nombre_archivo, safe='')},
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Adjunto no encontrado: {str(e)}")
