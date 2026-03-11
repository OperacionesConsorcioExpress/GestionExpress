import os
from io import BytesIO
from urllib.parse import unquote
from fastapi import APIRouter, Request, Depends, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from model.gestion_blobstorage import ContainerModel

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
router_blobstorage = APIRouter(tags=["blobstorage"])
templates          = Jinja2Templates(directory="./view")

# Instancia compartida — el caché vive aquí
container_model = ContainerModel()
# ─────────────────────────────────────────────
# Helper de sesión
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# =====================================================================
# ENDPOINTS
# =====================================================================
@router_blobstorage.get("/containers", response_class=HTMLResponse)
def get_containers(req: Request, user_session: dict = Depends(get_user_session)):
    """Renderiza la página principal de Blob Storage."""
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    user_rol_storage = user_session.get("rol_storage")
    if user_rol_storage is None:
        return HTMLResponse(
            "Error: No se encontró rol_storage en la sesión del usuario",
            status_code=400
        )

    allowed_containers = container_model.get_allowed_containers(user_rol_storage)
    context = {
        "request":      req,
        "user_session": user_session,
        "containers":   allowed_containers
    }
    return templates.TemplateResponse("containers.html", context)

@router_blobstorage.post("/containers")
async def create_container(data: dict):
    """Crea un nuevo contenedor en Azure Blob Storage."""
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="El nombre del contenedor es requerido")
    try:
        container_model.create_container(name)
        return {"message": f"Contenedor '{name}' creado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/files")
def get_files(container_name: str):
    """
    Retorna el árbol jerárquico del contenedor.
    Internamente llama a get_files_with_metadata() y aprovecha el caché —
    si el frontend ya pidió /files-metadata, esta respuesta es inmediata.
    """
    try:
        result = container_model.get_files_with_metadata(container_name)
        return {"files_tree": result["tree"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/files-metadata")
def get_files_with_metadata(container_name: str):
    """
    Retorna árbol + metadata + stats del contenedor.
    Primera llamada: consulta Azure (puede tardar con muchos archivos).
    Llamadas siguientes dentro del TTL: respuesta inmediata desde caché.
    """
    try:
        result = container_model.get_files_with_metadata(container_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/blobs/{blob_path:path}/properties")
def get_blob_properties(container_name: str, blob_path: str):
    """Retorna las propiedades detalladas de un blob específico."""
    try:
        decoded_path = unquote(blob_path)
        props = container_model.get_blob_properties(container_name, decoded_path)
        return props
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router_blobstorage.post("/containers/{container_name}/files")
async def upload_file(container_name: str, path: str = "", file: UploadFile = File(...)):
    """Sube un archivo al contenedor. Invalida el caché automáticamente."""
    try:
        full_path = os.path.join(path, file.filename) if path else file.filename
        contents  = await file.read()
        await container_model.upload_file(container_name, full_path, contents)
        return JSONResponse(
            status_code=200,
            content={"message": f"Archivo '{file.filename}' subido exitosamente en '{full_path}'"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/files/{file_path:path}/download")
def download_file(container_name: str, file_path: str):
    """
    Descarga con streaming directo Azure -> cliente.
    - stream_file(): max_concurrency=4, nunca carga el archivo en RAM
    - Content-Length: obtenido del caché si está disponible, sino de properties
    - Access-Control-Expose-Headers: permite que el frontend lea Content-Length
        para mostrar la barra de progreso real
    """
    try:
        filename  = os.path.basename(file_path)
        blob_size = container_model.get_blob_size(container_name, file_path)

        headers = {
            "Content-Disposition":        f"attachment; filename*=UTF-8''{filename}",
            "Content-Length":             str(blob_size),
            "Access-Control-Expose-Headers": "Content-Length",
            "Cache-Control":              "no-store",
        }

        return StreamingResponse(
            container_model.stream_file(container_name, file_path),
            media_type="application/octet-stream",
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router_blobstorage.delete("/containers/{container_name}/files/{file_name:path}")
def delete_file(container_name: str, file_name: str):
    """Elimina un archivo del contenedor. Invalida el caché automáticamente."""
    try:
        decoded_file_name = unquote(file_name)
        container_model.delete_file(container_name, decoded_file_name)
        return {"message": f"Archivo '{decoded_file_name}' eliminado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/blobs/{blob_path:path}/preview")
def preview_excel(
    container_name: str,
    blob_path:      str,
    sheet: int = 0,
    page:  int = 1,
    limit: int = 300,
):
    """
    Preview paginado de un archivo Excel.
    Retorna JSON con las filas de la página solicitada.
    El browser NUNCA descarga el binario completo — solo recibe JSON.
    """
    try:
        decoded_path = unquote(blob_path)
        result = container_model.preview_excel(container_name, decoded_path, sheet, page, limit)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router_blobstorage.post("/containers/{container_name}/cache/invalidate")
def invalidate_cache(container_name: str):
    """
    Endpoint para invalidar el caché manualmente desde el frontend
    (botón Refrescar). Fuerza reconsulta a Azure en la próxima petición.
    """
    container_model._cache_invalidate(container_name)
    return {"message": f"Caché del contenedor '{container_name}' invalidado"}