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

# Instancia compartida — igual que en main.py
container_model = ContainerModel()

# ─────────────────────────────────────────────
# Helper de sesión — igual que en main.py
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# =====================================================================
# ENDPOINTS
# =====================================================================
@router_blobstorage.get("/containers", response_class=HTMLResponse)
def get_containers(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # Obtener rol_storage del usuario logueado
    user_rol_storage = user_session.get("rol_storage")
    if user_rol_storage is None:
        return HTMLResponse("Error: No se encontró rol_storage en la sesión del usuario", status_code=400)

    # Obtener contenedores permitidos según el rol_storage del usuario
    allowed_containers = container_model.get_allowed_containers(user_rol_storage)

    context = {"request": req, "user_session": user_session, "containers": allowed_containers}
    return templates.TemplateResponse("containers.html", context)

@router_blobstorage.post("/containers")
async def create_container(data: dict):
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
    try:
        files_tree = container_model.get_files(container_name)
        return {"files_tree": files_tree}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router_blobstorage.post("/containers/{container_name}/files")
async def upload_file(container_name: str, path: str = "", file: UploadFile = File(...)):
    try:
        # Construir la ruta completa en el contenedor
        full_path = os.path.join(path, file.filename) if path else file.filename
        contents = await file.read()
        await container_model.upload_file(container_name, full_path, contents)
        return JSONResponse(status_code=200, content={"message": f"Archivo {file.filename} subido exitosamente en {full_path}"})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router_blobstorage.get("/containers/{container_name}/files/{file_path:path}/download")
def download_file(container_name: str, file_path: str):
    try:
        file_content = container_model.download_file(container_name, file_path)
        return StreamingResponse(BytesIO(file_content), media_type="application/octet-stream",
                                headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"})
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router_blobstorage.delete("/containers/{container_name}/files/{file_name:path}")
def delete_file(container_name: str, file_name: str):
    try:
        # Decodificar el nombre del archivo para manejar caracteres especiales y rutas
        decoded_file_name = unquote(file_name)
        container_model.delete_file(container_name, decoded_file_name)
        return {"message": f"Archivo '{decoded_file_name}' eliminado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))