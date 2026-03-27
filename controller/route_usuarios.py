import os
from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import List, Optional
from azure.storage.blob import BlobServiceClient
from werkzeug.security import generate_password_hash
# Importar modelos y controladores necesarios
from lib.cambiar_contrasena import cambiar_contrasena_post
from lib.pantallas_menu import get_pantallas_menu
from model.gestion_usuarios import GestionUsuarios, HandleDB, CargueLicenciasBI, Cargue_Roles_Blob_Storage
from model.gestion_roles_powerbi import ModeloRolesPowerBI
from model.gestion_checklist import Proceso_flota_asistencia

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
router_usuarios = APIRouter(tags=["usuarios"])
templates = Jinja2Templates(directory="./view")

# Instancias compartidas — igual que en main.py
db = HandleDB()
storage_db = Cargue_Roles_Blob_Storage()
# ─────────────────────────────────────────────
# Helper de sesión — igual que en main.py
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────
# Helper Blob Storage — igual que en main.py
# ─────────────────────────────────────────────
def obtener_contenedores_blob_storage():
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("No se encontró la cadena de conexión de Azure Storage en las variables de entorno")
        
        # Crear el cliente de Blob Storage utilizando la cadena de conexión
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        contenedores = blob_service_client.list_containers()
        return [container.name for container in contenedores]
    except Exception as e:
        # Loguear el error y retornar una lista vacía
        print(f"Error al obtener los contenedores de Blob Storage: {str(e)}")
        return []

# =====================================================================
# GESTIÓN DE USUARIOS
# =====================================================================
@router_usuarios.get("/registrarse", response_class=HTMLResponse)
def registrarse(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # >>> : listar roles activos de BlobStorage (id, nombre)
    roles = db.get_all_roles()  # Obtiene los roles desde la base de datos
    roles_storage = storage_db.get_all_roles_storage()  # Obtiene los roles storage
    usuarios = db.get_all_users()  # Obtiene los usuarios desde la base de datos
    
    # >>> : listar roles Power BI activos (id, nombre)
    modelo_pbi = ModeloRolesPowerBI()
    roles_powerbi_raw = modelo_pbi.obtener_todos_roles()  # [(id, nombre, ids_csv, estado)]
    roles_powerbi = [(r[0], r[1]) for r in roles_powerbi_raw if r[3] == 1]

    # >>> : listar procesos y subprocesos asignados a la flota de asistencia técnica (Checklist y Preoperacionales)
    gestor_flota = Proceso_flota_asistencia()
    procesos = gestor_flota.listar_procesos()   # [(id, proceso, subproceso)]
    gestor_flota.close()

    return templates.TemplateResponse("registrarse.html", {
        "request": req,
        "user_session": user_session,
        "roles": roles,
        "roles_storage": roles_storage,
        "usuarios": usuarios,
        "roles_powerbi": roles_powerbi,
        "procesos": procesos,  
    })

@router_usuarios.get("/registrarse/{user_id}/datos")
async def get_user_data(user_id: int):
    user = db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # >>> : listar procesos y subprocesos asignados a la flota de asistencia técnica (Checklist y Preoperacionales)
    gestor_flota = Proceso_flota_asistencia()
    procesos_ids = gestor_flota.obtener_ids_procesos_usuario(user_id)  # 👈
    gestor_flota.close()
    
    return JSONResponse(content={
        "id": user["id"],
        "nombres": user["nombres"],
        "apellidos": user["apellidos"],
        "username": user["username"],
        "rol": user["rol"],
        "estado": user["estado"],
        "rol_storage": user["rol_storage"],
        "rol_powerbi": user.get("rol_powerbi", 0),
        "procesos_asignados": procesos_ids, # Flota de asistencia técnica (Checklist y Preoperacionales)
    })

@router_usuarios.post("/registrarse", response_class=HTMLResponse)
def registrarse_post(req: Request, nombres: str = Form(...), apellidos: str = Form(...),
                    username: str = Form(...), rol: int = Form(...),
                    rol_storage: int = Form(...), rol_powerbi: int = Form(0), password_user: str = Form(...), 
                    procesos_asignados: List[int] = Form(default=[]), user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    
    # Si no se selecciona un rol de storage o powerbi, guardar "0"
    rol_storage = rol_storage if rol_storage != 0 else 0
    rol_powerbi = rol_powerbi if rol_powerbi != 0 else 0

    data_user = {
        "nombres": nombres,
        "apellidos": apellidos,
        "username": username,
        "rol": rol,
        "rol_storage": rol_storage,
        "rol_powerbi": rol_powerbi,
        "password_user": password_user,
        "estado": 1
    }
    
    user = GestionUsuarios(data_user)
    result = user.create_user()

    if result.get("success"):
        # Obtener id de usuario y guardar asignaciones
        gestor_flota = Proceso_flota_asistencia()
        user_id = gestor_flota.obtener_id_usuario_por_username(username)
        if user_id:
            gestor_flota.reemplazar_asignaciones(user_id, procesos_asignados or [])
        gestor_flota.close()
        # Establecer una cookie con el mensaje de éxito
        response = RedirectResponse(url="/registrarse", status_code=303)
        response.set_cookie(key="success_message", value="Usuario creado correctamente.", max_age=5)
        return response
    else:
        # Establecer una cookie con el mensaje de error
        error_message = result.get("message", "Error desconocido al crear usuario.")
        response = RedirectResponse(url="/registrarse", status_code=303)
        response.set_cookie(key="error_message", value=error_message, max_age=5)
        return response

@router_usuarios.post("/registrarse/{id}/editar")
async def editar_usuario(id: int, request: Request, user_data: dict = Depends(get_user_session)):
    try:
        form_data = await request.form()
        # Convertimos FormData en un diccionario
        form_data_dict = dict(form_data)

        # Verificamos si se proporcionó una nueva contraseña
        password_user = form_data_dict.get("password_user")
        if not password_user:
            # Si no se proporcionó una nueva contraseña, quitamos ese campo del formulario
            form_data_dict.pop("password_user", None)
        else:
            # Si se proporciona una nueva contraseña, la encriptamos
            form_data_dict["password_user"] = generate_password_hash(password_user)
            
        # Verificamos si se seleccionó un rol de storage
        rol_storage = form_data_dict.get("rol_storage")
        form_data_dict["rol_storage"] = rol_storage if rol_storage != "0" else 0
        
        # Verificamos si se seleccionó un rol de powerbi
        rol_powerbi = form_data_dict.get("rol_powerbi", "0")
        form_data_dict["rol_powerbi"] = int(rol_powerbi) if rol_powerbi != "0" else 0
        
        # procesos_asignados[] (puede venir vacío = No Asignar)
        procesos_asignados = form_data.getlist("procesos_asignados[]") if hasattr(form_data, "getlist") else []
        procesos_asignados = [int(x) for x in procesos_asignados] if procesos_asignados else []

        # Llama a la función para actualizar el usuario en la base de datos
        db.update_user(id, form_data_dict)
        
        # Guardar las asignaciones de procesos para la flota de asistencia técnica
        gestor_flota = Proceso_flota_asistencia()
        gestor_flota.reemplazar_asignaciones(id, procesos_asignados)
        gestor_flota.close()
        
        # Crear respuesta de redirección con una cookie que contenga el mensaje de éxito
        response = RedirectResponse(url="/registrarse", status_code=303)
        response.set_cookie(key="success_message", value="Usuario actualizado correctamente.", max_age=5)
        return response

    except Exception as e:
        return templates.TemplateResponse("registrarse.html", {
            "request": request,
            "user_session": user_data,
            "error_message": f"Error al actualizar el usuario: {str(e)}"
        })

@router_usuarios.post("/registrarse/{id}/inactivar")
async def inactivate_user(id: int, request: Request, user_data: dict = Depends(get_user_session)):
    try:
        db.inactivate_user(id)

        # Crear respuesta de redirección con una cookie que contenga el mensaje de éxito
        response = RedirectResponse(url="/registrarse", status_code=303)
        response.set_cookie(key="success_message", value="Usuario inactivado correctamente.", max_age=5)
        return response
        
    except Exception as e:
        return templates.TemplateResponse("registrarse.html", {
            "request": request,
            "user_session": user_data,
            "error_message": f"Error al inactivar el usuario: {str(e)}"
        })

@router_usuarios.get("/pantallas_permitidas", response_class=JSONResponse)
def obtener_pantallas_permitidas(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return JSONResponse({"error": "Usuario no autenticado"}, status_code=401)

    role_id = user_session.get("rol")
    if not role_id:
        return JSONResponse({"error": "Rol no encontrado para el usuario"}, status_code=404)

    # Consultar las pantallas asignadas al rol del usuario
    pantallas_permitidas = db.get_pantallas_by_role(role_id)
    if not pantallas_permitidas:
        return JSONResponse({"error": "No hay pantallas asignadas para el rol"}, status_code=404)

    return JSONResponse({"pantallas": pantallas_permitidas}, status_code=200)

# =====================================================================
# CAMBIO DE CONTRASEÑA POR EL USUARIO LOGUEADO
# =====================================================================
@router_usuarios.post("/cambiar-contrasena")
async def cambiar_contrasena_post_route(request: Request, user_session: dict = Depends(get_user_session)):
    return await cambiar_contrasena_post(request, user_session)

# =====================================================================
# GESTIÓN DE ROLES DEL SISTEMA
# =====================================================================
@router_usuarios.get("/roles", response_class=HTMLResponse)
async def get_roles(request: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)  # Redirigir si no hay sesión iniciada.

    pantallas_disponibles = get_pantallas_menu()
    roles = db.get_all_roles()

    # Verifica si existe el parámetro de éxito en la URL
    success_message = None
    success_param = request.query_params.get('success', None)

    if success_param == '1':
        success_message = "Rol creado correctamente."
    elif success_param == '2':
        success_message = "Rol actualizado correctamente."
    elif success_param == '3':
        success_message = "Rol eliminado correctamente."

    return templates.TemplateResponse("roles.html", {
        "request": request,
        "roles": roles,
        "pantallas": pantallas_disponibles,
        "success_message": success_message,
        "user_session": user_session  # Pasa `user_session` al contexto de la plantilla.
    })

@router_usuarios.get("/roles/{id_rol}/datos")
async def obtener_datos_rol(id_rol: int):
    # Obtener los datos del rol desde la base de datos
    rol = db.get_role_by_id(id_rol)

    if not rol:
        raise HTTPException(status_code=404, detail="Rol no encontrado")

    id_rol, nombre_rol, pantallas_asignadas = rol

    return JSONResponse(content={
        "id_rol": id_rol,
        "nombre_rol": nombre_rol,
        "pantallas_asignadas": pantallas_asignadas.split(',')  # Convertimos las pantallas a lista
    })

@router_usuarios.post("/roles", response_class=HTMLResponse)
async def add_role(request: Request, role_name: str = Form(...), permissions: List[str] = Form(...), role_id: Optional[int] = Form(None)):
    if role_id:
        # Si hay un role_id, entonces es una actualización
        return await update_role(request, role_id, role_name, permissions)

    # Validaciones
    if not role_name.strip():
        return templates.TemplateResponse("roles.html", {
            "request": request,
            "roles": db.get_all_roles(),
            "pantallas": get_pantallas_menu(),
            "error_message": "Debe ingresar un nombre para el rol."
        })

    if not permissions or len(permissions) == 0:
        return templates.TemplateResponse("roles.html", {
            "request": request,
            "roles": db.get_all_roles(),
            "pantallas": get_pantallas_menu(),
            "error_message": "Debe seleccionar al menos una pantalla para asignar al rol."
        })

    # Inserta el nuevo rol en la base de datos
    permisos_string = ','.join(permissions)
    db.insert_role({
        "nombre_rol": role_name,
        "pantallas_asignadas": permisos_string
    })

    return RedirectResponse(url="/roles?success=1", status_code=303)

@router_usuarios.post("/roles/{role_id}/editar", response_class=HTMLResponse)
async def update_role(request: Request, role_id: int, role_name: str = Form(...), permissions: List[str] = Form(...)):
    # Validaciones
    if not role_name.strip():
        return templates.TemplateResponse("roles.html", {
            "request": request,
            "roles": db.get_all_roles(),
            "pantallas": get_pantallas_menu(),
            "error_message": "Debe ingresar un nombre para el rol."
        })

    if not permissions or len(permissions) == 0:
        return templates.TemplateResponse("roles.html", {
            "request": request,
            "roles": db.get_all_roles(),
            "pantallas": get_pantallas_menu(),
            "error_message": "Debe seleccionar al menos una pantalla para asignar al rol."
        })

    # Actualizar el rol
    permisos_string = ','.join(permissions)
    db.update_role(role_id, role_name, permisos_string)

    return RedirectResponse(url="/roles?success=2", status_code=303)

@router_usuarios.post("/roles/{role_id}/eliminar")
async def eliminar_rol(role_id: int):
    try:
        db.delete_role(role_id)
        return RedirectResponse(url="/roles?success=3", status_code=303)
    except Exception as e:
        return templates.TemplateResponse("roles.html", {
            "roles": db.get_all_roles(),
            "pantallas": get_pantallas_menu(),
            "error_message": f"Ocurrió un error al intentar eliminar el rol: {e}"
        })

# =====================================================================
# GESTIÓN DE ROLES DE BLOB STORAGE
# =====================================================================
# Instancia de la clase Cargue_Roles_Blob_Storage — igual que en main.py
storage_db = Cargue_Roles_Blob_Storage()

# Pantalla principal de roles_storage
@router_usuarios.get("/roles_storage", response_class=HTMLResponse)
def get_roles_storage(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # Intentar obtener los contenedores de Azure Blob Storage
    contenedores_disponibles = obtener_contenedores_blob_storage()

    # Mostrar un mensaje de advertencia si no se encuentran contenedores
    error_message = None
    if not contenedores_disponibles:
        error_message = "No se pudieron obtener los contenedores de Blob Storage. Verifique la conexión o Variable de Entorno."

    # Obtener roles storage desde la base de datos
    roles_storage = storage_db.get_all_roles_storage()

    # Verificar si hay un mensaje de éxito o error en la URL
    success_message = None
    if req.query_params.get('success') == '1':
        success_message = "Rol creado correctamente."
    elif req.query_params.get('success') == '2':
        success_message = "Rol actualizado correctamente."
    elif req.query_params.get('success') == '3':
        success_message = "Rol eliminado correctamente."

    # Renderizar la plantilla con los datos y mensajes correspondientes
    return templates.TemplateResponse("roles_storage.html", {
        "request": req,
        "user_session": user_session,
        "containers": contenedores_disponibles,
        "roles_storage": roles_storage,
        "success_message": success_message,
        "error_message": error_message
    })

# Crear nuevo rol de storage
@router_usuarios.post("/roles_storage", response_class=HTMLResponse)
async def add_role_storage(
    req: Request,
    role_storage_name: str = Form(...),
    containers: list = Form(...),
    accion_ver: Optional[str] = Form(None),
    accion_editar: Optional[str] = Form(None),
    accion_descargar: Optional[str] = Form(None),
    accion_eliminar: Optional[str] = Form(None),
    accion_cargar: Optional[str] = Form(None),
):
    try:
        # Validaciones
        if not role_storage_name.strip():
            return RedirectResponse(url="/roles_storage?error=Debe ingresar un nombre para el rol", status_code=303)

        if not containers or len(containers) == 0:
            return RedirectResponse(url="/roles_storage?error=Debe seleccionar al menos un contenedor", status_code=303)

        # Inserta el nuevo rol en la base de datos
        storage_db.insert_roles_storage({
            "nombre_rol_storage": role_storage_name,
            "contenedores_asignados": containers,
            "accion_ver": accion_ver == "on",
            "accion_editar": accion_editar == "on",
            "accion_descargar": accion_descargar == "on",
            "accion_eliminar": accion_eliminar == "on",
            "accion_cargar": accion_cargar == "on",
        })

        return RedirectResponse(url="/roles_storage?success=1", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error: {str(e)}", status_code=303)

# Obtener los datos de un rol específico
@router_usuarios.get("/roles_storage/{role_storage_id}/datos", response_class=JSONResponse)
async def obtener_datos_role_storage(role_storage_id: int):
    # Obtener los datos del rol desde la base de datos
    role_storage = storage_db.get_role_storage_by_id(role_storage_id)

    if not role_storage:
        raise HTTPException(status_code=404, detail="Rol no encontrado")

    return JSONResponse(content=role_storage)

# Editar un rol existente
@router_usuarios.post("/roles_storage/{role_storage_id}/editar", response_class=HTMLResponse)
async def update_role_storage(
    req: Request,
    role_storage_id: int,
    role_storage_name: str = Form(...),
    containers: list = Form(...),
    accion_ver: Optional[str] = Form(None),
    accion_editar: Optional[str] = Form(None),
    accion_descargar: Optional[str] = Form(None),
    accion_eliminar: Optional[str] = Form(None),
    accion_cargar: Optional[str] = Form(None),
):
    try:
        # Validaciones
        if not role_storage_name.strip():
            return RedirectResponse(url="/roles_storage?error=Debe ingresar un nombre para el rol", status_code=303)

        if not containers or len(containers) == 0:
            return RedirectResponse(url="/roles_storage?error=Debe seleccionar al menos un contenedor", status_code=303)

        # Actualizar el rol en la base de datos
        storage_db.update_role_storage(
            role_storage_id, role_storage_name, containers,
            accion_ver=accion_ver == "on",
            accion_editar=accion_editar == "on",
            accion_descargar=accion_descargar == "on",
            accion_eliminar=accion_eliminar == "on",
            accion_cargar=accion_cargar == "on",
        )

        return RedirectResponse(url="/roles_storage?success=2", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error: {str(e)}", status_code=303)

# Eliminar un rol
@router_usuarios.post("/roles_storage/{role_storage_id}/eliminar")
async def eliminar_role_storage(role_storage_id: int):
    try:
        storage_db.delete_role_storage(role_storage_id)
        return RedirectResponse(url="/roles_storage?success=3", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error al intentar eliminar el rol: {str(e)}", status_code=303)