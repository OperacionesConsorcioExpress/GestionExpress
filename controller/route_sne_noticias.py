import json
import urllib.parse
from typing import List, Optional

from fastapi import APIRouter, Request, Depends, HTTPException, Query, Form, File, UploadFile
from starlette.responses import StreamingResponse

from model.gestion_sne_noticias import GestionSneNoticias

router_sne_noticias = APIRouter(prefix="/monitor", tags=["sne_noticias"])


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
# CONTADOR DE NOTICIAS ACTIVAS (para badge en sne_objecion)
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_noticias.get("/api/noticias/contador")
def noticias_contador(user_session: dict = Depends(_require_sesion)):
    with GestionSneNoticias() as db:
        total = db.contar_activas()
    return {"ok": True, "total": total}


# ══════════════════════════════════════════════════════════════════════════════
# CRUD NOTICIAS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_noticias.get("/api/noticias")
def noticias_listar(
    estado: Optional[int] = Query(None, ge=0, le=1),
    q: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneNoticias() as db:
        data = db.listar_noticias(estado=estado, q=q)
    return {"ok": True, "data": data}


@router_sne_noticias.post("/api/noticias", status_code=201)
async def noticias_crear(
    asunto: str = Form(...),
    observacion: str = Form(...),
    estado: int = Form(1),
    files: List[UploadFile] = File(default=[]),
    user_session: dict = Depends(_require_sesion),
):
    try:
        with GestionSneNoticias() as db:
            adjuntos = []
            for f in files:
                if not f or not f.filename:
                    continue
                contenido = await f.read()
                adjuntos.append(db.subir_adjunto_noticia(contenido, f.filename))
            fila = db.crear_noticia(
                asunto=asunto,
                observacion=observacion,
                estado=estado,
                usuario_publico=_nombre_usuario(user_session),
                adjuntos=adjuntos,
            )
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router_sne_noticias.put("/api/noticias/{id}")
async def noticias_actualizar(
    id: int,
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
        with GestionSneNoticias() as db:
            nuevos = []
            for f in files:
                if not f or not f.filename:
                    continue
                contenido = await f.read()
                nuevos.append(db.subir_adjunto_noticia(contenido, f.filename))
            adjuntos = existentes + nuevos
            fila = db.actualizar_noticia(
                id=id,
                asunto=asunto,
                observacion=observacion,
                estado=estado,
                adjuntos=adjuntos,
            )
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router_sne_noticias.patch("/api/noticias/{id}/estado")
async def noticias_cambiar_estado(
    id: int,
    payload: dict,
    user_session: dict = Depends(_require_sesion),
):
    estado = int(payload.get("estado"))
    try:
        with GestionSneNoticias() as db:
            fila = db.cambiar_estado_noticia(id=id, estado=estado)
        return {"ok": True, "data": fila}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# ADJUNTOS (blob storage sne-noticias)
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_noticias.get("/api/noticias/adjunto")
def noticias_descargar_adjunto(
    ruta: str = Query(..., description="Ruta del blob dentro del contenedor sne-noticias"),
    user_session: dict = Depends(_require_sesion),
):
    try:
        with GestionSneNoticias() as db:
            contenido, content_type, nombre_archivo = db.descargar_adjunto_noticia(ruta)
        return StreamingResponse(
            iter([contenido]),
            media_type=content_type,
            headers={"Content-Disposition": "inline; filename*=UTF-8''" + urllib.parse.quote(nombre_archivo, safe='')},
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Adjunto no encontrado: {str(e)}")
