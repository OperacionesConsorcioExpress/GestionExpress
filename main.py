######################### Importar librerías necesarias #################################
from fastapi import FastAPI, Request, Form, Depends, File, UploadFile, HTTPException, Query, Response, APIRouter, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import FileResponse
from bs4 import BeautifulSoup 
from pydantic import BaseModel
from urllib.parse import unquote
from datetime import datetime, date
import psycopg2
import json
from typing import List, Optional
from io import BytesIO 
import shutil
import os
import re
import msal
import requests
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash
from azure.storage.blob import BlobServiceClient, BlobClient

################## Importar Backend de Herramientas ###########################
from lib.pantallas_menu import get_pantallas_menu
from lib.verifcar_clave import check_user
from lib.asignar_controles import fecha_asignacion, puestos_SC, puestos_UQ, concesion, control, rutas, turnos, hora_inicio, hora_fin

################### Importar Controladores Endpoint ##########################
from controller.user import User
from controller.cargues import ProcesarCargueControles
#from controller.route_chatbot import chatbot_router # Comentar
#from controller.route_NPL_chatbot import npl_router # Comentar
from controller.route_checklist import checklist_router
from controller.cambiar_contrasena import cambiar_contrasena_post
from controller.route_roles_powerbi import router_roles_powerbi
from controller.route_sgi import router_sgi
from controller.route_cop import router_cop
from controller.route_buses import router_buses
from controller.route_rutas import router_rutas
from controller.route_eds import router_eds
from controller.route_sne_motivos import router_sne_motivos
from controller.route_sne import router_sne

##################### Importar Modelos Backend ##########################
from model.gestionar_db import Cargue_Controles
from model.gestionar_db import Cargue_Asignaciones
from model.gestionar_db import CargueLicenciasBI
from model.gestionar_db import HandleDB
from model.gestionar_db import Cargue_Roles_Blob_Storage
from model.consultas_db import Reporte_Asignaciones
from model.gestion_clausulas import GestionClausulas
from model.job import TareasProgramadasJuridico
from model.containerModel import ContainerModel
from model.gestion_reportbi import ReportBIGestion
from model.roles_powerbi import ModeloRolesPowerBI
from model.gestion_checklist import Proceso_flota_asistencia

############################### Carga de Variables de Entorno ###########################
load_dotenv()

####################### Iniciar la aplicación FastAPI y configurar middleware ##########################
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="!secret_key", max_age=1800) # Expira en 30 minutos
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="./view")
db = HandleDB()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Habilitar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todas las solicitudes de origen cruzado
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables de entorno
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "5000-juridica-y-riesgos-juridica-clausulas"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

############################### RUTAS INICIALES DEL SISTEMA ###########################
# Ruta de diagnóstico para Azure/App Service
@app.get("/health")
def health_check():
    return {"status": "🟢 Gestión Express activo"}

# Función para verificar si el usuario ha iniciado sesión
def get_user_session(req: Request):
    return req.session.get('user')

# Ruta principal
@app.get("/", response_class=HTMLResponse)
def root(req: Request, user_session: dict = Depends(get_user_session)):
    return templates.TemplateResponse("index.html", {"request": req, "user_session": user_session}, title="Centro de Control")

# Ruta Login
@app.post("/", response_class=HTMLResponse)
def login(req: Request, username: str = Form(...), password_user: str = Form(...)):
    # Verifica las credenciales del usuario
    verify, nombres, apellidos = check_user(username, password_user)
    
    if verify:
        # Obtén la información del usuario desde la base de datos, incluyendo el rol
        user_data = db.get_only(username)  # Esto debe retornar la fila completa del usuario

        if user_data:
            estado = user_data[6]  # campo de estado del usuario
            rol = user_data[4]  # es el id_rol del usuario
            rol_storage = user_data[7]  # es el id_rol_storage del usuario
            rol_powerbi  = user_data[8] # es el id_rol_powerbi del usuario
            
            # Verificamos si el estado del usuario es activo (1)
            if estado == 1:
                # Guardar la sesión del usuario, incluyendo el rol
                req.session['user'] = {
                    "id": user_data[0], 
                    "username": username,
                    "nombres": nombres,
                    "apellidos": apellidos,
                    "rol": rol,
                    "rol_storage": rol_storage,
                    "rol_powerbi": int(rol_powerbi or 0)
                }

                #print("Sesión del usuario:", req.session['user']) 
                
                # Redirigir al usuario a la página de inicio después del login
                return RedirectResponse(url="/inicio", status_code=302)
            else:
                # Si el usuario está inactivo (estado == 0), muestra un mensaje de error
                error_message = "El usuario está inactivo. No puede iniciar sesión."
                return templates.TemplateResponse("index.html", {"request": req, "error_message": error_message})
    else:
        # Si las credenciales no son válidas, muestra un mensaje de error
        error_message = "Por favor valide sus credenciales y vuelva a intentar."
        return templates.TemplateResponse("index.html", {"request": req, "error_message": error_message})

# Ruta de cierre de sesión
@app.get("/logout", response_class=HTMLResponse)
async def logout(request: Request): # Limpiar cualquier estado de sesión
    request.session.clear()  # Limpia la sesión del usuario
    response = RedirectResponse(url="/", status_code=302) # Crear una respuesta de redirección
    response.delete_cookie("access_token") # Eliminar la cookie de sesión o token de acceso
    return response

###################### CREAR Y EDITAR USUARIO EN EL SISTEMA ##########################

@app.get("/inicio", response_class=HTMLResponse)
def registrarse(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)    
    return templates.TemplateResponse("inicio.html", {"request": req, "user_session": user_session})

@app.get("/registrarse", response_class=HTMLResponse)
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
    gestor_flota = Proceso_flota_asistencia(DATABASE_PATH)
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

@app.get("/registrarse/{user_id}/datos" )
async def get_user_data(user_id: int):
    user = db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # >>> : listar procesos y subprocesos asignados a la flota de asistencia técnica (Checklist y Preoperacionales)
    gestor_flota = Proceso_flota_asistencia(DATABASE_PATH)
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

@app.post("/registrarse", response_class=HTMLResponse)
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
    
    user = User(data_user)
    result = user.create_user()

    if result.get("success"):
        # Obtener id de usuario y guardar asignaciones
        gestor_flota = Proceso_flota_asistencia(DATABASE_PATH)
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

@app.post("/registrarse/{id}/editar")
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
        gestor_flota = Proceso_flota_asistencia(DATABASE_PATH)
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

@app.post("/registrarse/{id}/inactivar")
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

@app.get("/roles", response_class=HTMLResponse)
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

@app.get("/roles/{id_rol}/datos")
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

@app.post("/roles", response_class=HTMLResponse)
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

@app.post("/roles/{role_id}/editar", response_class=HTMLResponse)
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

@app.post("/roles/{role_id}/eliminar")
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

@app.get("/pantallas_permitidas", response_class=JSONResponse)
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

################### CAMBIO DE CONTRASEÑA POR EL USUARIO LOGUEADO ########################
@app.post("/cambiar-contrasena")
async def cambiar_contrasena_post_route(request: Request, user_session: dict = Depends(get_user_session)):
    return await cambiar_contrasena_post(request, user_session)

######################## RUTAS PARA ASIGNACIONES Y CARGUES MASIVOS ############################
@app.get("/asignacion", response_class=HTMLResponse)
def asignacion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("asignacion.html", {"request": req, "user_session": user_session})

@app.post("/asignacion", response_class=HTMLResponse)
def asignacion_post(req: Request, username: str = Form(...), password_user: str = Form(...)):
    verify = check_user(username, password_user)
    if verify:
        return templates.TemplateResponse("asignacion.html", {"request": req, "data_user": verify})
    else:
        error_message = "Por favor valide sus credenciales y vuelva a intentar."
        return templates.TemplateResponse("index.html", {"request": req, "error_message": error_message})

# Clase para manejar el request de confirmación de cargue
class ConfirmarCargueRequest(BaseModel):
    session_id: str

# FUNCIONALIDADES PARA CARGUES MASIVOS DE PLANTA Y CONTROLES "gestionar_db.py"
# Cargues Archivos de Planta y Parametrización de Controles
# Caché en memoria como diccionario
cache = {}

@app.post("/cargar_archivo/")
async def cargar_archivo(file: UploadFile = File(...)):
    procesador = ProcesarCargueControles(file)
    preliminar = procesador.leer_archivo()

    # Generar una clave única para el usuario/sesión (simulación de UUID)
    session_id = str(len(cache) + 1)
    cache[session_id] = preliminar

    print(f"Archivo cargado correctamente. Session ID: {session_id}")
    return {"session_id": session_id, "preliminar": preliminar}

@app.post("/confirmar_cargue/")
async def confirmar_cargue(data: dict):
    session_id = data.get("session_id")
    if session_id not in cache:
        raise HTTPException(status_code=404, detail="Sesión no encontrada")

    # Obtener datos preliminares desde la caché
    preliminar = cache[session_id]

    # Filtrar datos según lo seleccionado por el usuario
    hojas_a_cargar = {}

    if data.get("tcz"):
        hojas_a_cargar['planta'] = preliminar.get('planta')

    if data.get("supervisores"):
        hojas_a_cargar['supervisores'] = preliminar.get('supervisores')

    if data.get("turnos"):
        hojas_a_cargar['turnos'] = preliminar.get('turnos')

    if data.get("controles"):
        hojas_a_cargar['controles'] = preliminar.get('controles')

    if not hojas_a_cargar:
        raise HTTPException(status_code=400, detail="Debe seleccionar al menos una hoja para cargar.")

    # Enviar las hojas seleccionadas a la base de datos
    try:
        cargador = Cargue_Controles()
        cargador.cargar_datos(hojas_a_cargar)
        return {"message": "Datos cargados exitosamente."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al cargar los datos: {str(e)}")
    
# Plantilla de cargue de planta activa y controles
@app.get("/plantilla_cargue")
async def descargar_plantilla():
    file_path = "./cargues/asignaciones_tecnicos.xlsx"
    return FileResponse(path=file_path, filename="asignaciones_tecnicos.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

#########################################################################################
# FUNCIONALIDADES PARA GESTIONAR LAS ASIGNACIONES "asignar_controles.py"
def get_planta_data():
    conn = psycopg2.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT cedula, nombre FROM planta")
    rows = cursor.fetchall()
    conn.close()
    return [{"cedula": row[0], "nombre": row[1]} for row in rows]

def get_supervisores_data():
    conn = psycopg2.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT cedula, nombre FROM supervisores")
    rows = cursor.fetchall()
    conn.close()
    return [{"cedula": row[0], "nombre": row[1]} for row in rows]

@app.get("/api/planta", response_class=JSONResponse)
def api_planta():
    data = get_planta_data()
    return data

@app.get("/api/supervisores", response_class=JSONResponse)
def api_supervisores():
    data = get_supervisores_data()
    return data

@app.get("/api/fecha_asignacion")
async def api_fecha_asignacion(fecha: str):
    return fecha_asignacion(fecha)

@app.get("/api/puestos_SC")
async def api_puestos_SC():
    return puestos_SC()

@app.get("/api/puestos_UQ")
async def api_puestos_UQ():
    return puestos_UQ()

@app.get("/api/concesion")
async def api_concesion():
    return concesion()

@app.get("/api/control")
async def get_control(concesion: str, puestos: str):
    controles = control(concesion, puestos)
    return JSONResponse(content=controles)

@app.get("/api/rutas")
async def get_rutas(concesion: str, puestos: str, control: str):
    rutas_asociadas = rutas(concesion, puestos, control)
    return {"rutas": rutas_asociadas}

@app.get("/api/turnos")
async def get_turnos():
    return turnos()

def get_turnos_data():
    conn = psycopg2.connect(DATABASE_PATH) 
    cursor = conn.cursor()
    cursor.execute("SELECT turno, hora_inicio, hora_fin, detalles FROM turnos")
    rows = cursor.fetchall()
    conn.close()
    return [{"turno": row[0], "hora_inicio": row[1], "hora_fin": row[2], "detalles": row[3]} for row in rows]

@app.get("/api/turnos", response_class=JSONResponse)
def api_turnos():
    data = get_turnos_data()
    return data

@app.get("/api/turno_descripcion")
def turno_descripcion(turno: str):
    return {"descripcion": turno_descripcion(turno)}

@app.get("/api/hora_inicio")
async def get_hora_inicio(turno: str):
    return {"inicio": hora_inicio(turno)}

@app.get("/api/hora_fin")
async def get_hora_fin(turno: str):
    return {"fin": hora_fin(turno)}

#######################################################################
# FUNCIONALIDADES PARA GUARDAR LO REGISTRADO EN LA GRILLA DE ASIGNACIÓN"
cargue_asignaciones = Cargue_Asignaciones()

class AsignacionRequest(BaseModel):
    fecha: str
    cedula: str
    nombre: str
    turno: str
    hora_inicio: str
    hora_fin: str
    concesion: str
    control: str
    rutas_asociadas: str
    observaciones: str

# Función para obtener datos del cuerpo de la solicitud y guardar las asignaciones
@app.post("/api/guardar_asignaciones")
async def guardar_asignaciones(request: Request, user_session: dict = Depends(get_user_session)):
    try:
        data = await request.json() # Obtener datos del cuerpo de la solicitud
        processed_data = cargue_asignaciones.procesar_asignaciones(data, user_session) # Procesar, cargar asignaciones y Pasar user_session
        cargue_asignaciones.cargar_asignaciones(processed_data)
        return {"message": "Asignaciones guardadas exitosamente."} # Retornar mensaje de éxito
    except Exception as e:
        error_message = f"Error al guardar asignaciones: {str(e)}" # Manejar errores y retornar mensaje de error
        raise HTTPException(status_code=500, detail=str(e))

#################################################################
# FUNCIONALIDADES DE CONSULTA Y REPORTES PARA CENTRO DE CONTROL"
# Instancia de la clase para manejar reportes
reporte_asignaciones = Reporte_Asignaciones()

# Ruta para el dashboard principal
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(req: Request,  modal: bool = False, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    filtros = reporte_asignaciones.obtener_filtros_unicos()
    return templates.TemplateResponse("dashboard.html", {
        "request": req,
        "user_session": user_session,
        "modal": modal,
        **filtros
    })

@app.get("/filtrar_asignaciones", response_class=HTMLResponse)
async def filtrar_asignaciones(request: Request, fechaInicio: str, fechaFin: str, cedulaTecnico: str = None, nombreTecnico: str = None, turno: str = None, concesion: str = None, control: str = None, ruta: str = None, linea: str = None, cop: str = None, usuarioRegistra: str = None, nombreSupervisorEnlace: str = None):
    filtros = {
        "fecha_inicio": fechaInicio,
        "fecha_fin": fechaFin,
        "cedula": cedulaTecnico,
        "nombre": nombreTecnico,
        "turno": turno,
        "concesion": concesion,
        "control": control,
        "ruta": ruta,
        "linea": linea,
        "cop": cop,
        "registrado_por": usuarioRegistra,
        "nombre_supervisor_enlace": nombreSupervisorEnlace
    }
    
    asignaciones = reporte_asignaciones.obtener_asignaciones(**filtros)
    return templates.TemplateResponse("dashboard.html", {"request": request, "asignaciones": asignaciones, **filtros})

@app.post("/buscar_asignaciones")
async def buscar_asignaciones(request: Request):
    # Recibir los datos de los filtros desde el frontend
    filtros = await request.json()
    
    # Crear una instancia de Reporte_Asignaciones
    reporte = Reporte_Asignaciones()

    # Obtener las asignaciones utilizando los filtros
    asignaciones = reporte.obtener_asignaciones(
        fecha_inicio=filtros.get('fechaInicio'),
        fecha_fin=filtros.get('fechaFin'),
        cedula=filtros.get('cedulaTecnico'),
        nombre=filtros.get('nombreTecnico'),
        turno=filtros.get('turno'),
        concesion=filtros.get('concesion'),
        control=filtros.get('control'),
        ruta=filtros.get('ruta'),
        linea=filtros.get('linea'),
        cop=filtros.get('cop'),
        registrado_por=filtros.get('usuarioRegistra'),
        nombre_supervisor_enlace=filtros.get('nombreSupervisorEnlace')
    )
    # Depuración para verificar la salida
    #print(asignaciones)
    # Devolver las asignaciones como JSON para que el frontend las maneje
    return JSONResponse(content=asignaciones)

@app.post("/descargar_xlsx")
async def descargar_xlsx(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    xlsx_file = reporte.generar_xlsx(filtros)
    
    # Usar StreamingResponse para devolver el archivo
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.xlsx"'
    }
    return StreamingResponse(xlsx_file, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

@app.post("/descargar_csv")
async def descargar_csv(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    csv_file = reporte.generar_csv(filtros)
    
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.csv"'
    }
    return StreamingResponse(csv_file, media_type="text/csv", headers=headers)

@app.post("/descargar_json")
async def descargar_json(request: Request):
    filtros = await request.json()
    
    reporte = Reporte_Asignaciones()
    json_data = reporte.generar_json(filtros)
    
    headers = {
        'Content-Disposition': 'attachment; filename="asignaciones.json"'
    }
    return JSONResponse(content=json_data, headers=headers)

##################################################################
# FUNCIONALIDADES DE CONSULTA DE ASIGNACIONES Y TRAERLO EN GRILLA"
# Instancia de la clase para manejar consultas en la base de datos

@app.post("/api/obtener_asignaciones_ayuda")
async def obtener_asignaciones_ayuda(request: Request):
    data = await request.json()
    fecha = data['fecha']
    concesion = data['concesion']
    fecha_hora_registro = data.get('fecha_hora_registro')

    reporte = Reporte_Asignaciones()
    asignaciones = reporte.obtener_asignacion_por_fecha(fecha, concesion, fecha_hora_registro)

    if not asignaciones:
        return JSONResponse(content={"message": "No se encontraron asignaciones para la fecha, concesión y fecha/hora seleccionadas."}, status_code=404)
    
    return JSONResponse(content={"asignaciones": asignaciones}, status_code=200)

@app.post("/api/obtener_concesiones_por_fecha")
async def obtener_concesiones_por_fecha(request: Request):
    data = await request.json()
    fecha = data['fecha']

    reporte = Reporte_Asignaciones()
    concesiones = reporte.obtener_concesiones_unicas_por_fecha(fecha)

    if not concesiones:
        return JSONResponse(content={"message": "No se encontraron concesiones para la fecha seleccionada."}, status_code=404)

    return JSONResponse(content=concesiones)

@app.post("/api/obtener_fechas_horas_registro")
async def obtener_fechas_horas_registro(request: Request):
    data = await request.json()
    fecha = data['fecha']
    concesion = data['concesion']

    reporte = Reporte_Asignaciones()
    fechas_horas = reporte.obtener_fechas_horas_registro(fecha, concesion)

    if not fechas_horas:
        return JSONResponse(content={"message": "No se encontraron registros para la fecha y concesión seleccionada."}, status_code=404)
    
    return JSONResponse(content={"fechas_horas": fechas_horas}, status_code=200)

# Define el modelo para las asignaciones individuales para el PDF
class Asignacion(BaseModel):
    fecha: str
    cedula: str
    nombre: str
    turno: str
    h_inicio: str
    h_fin: str
    concesion: str
    control: str
    ruta: str
    linea: str
    cop: str
    observaciones: str
    puestosSC: int
    puestosUQ: int
    fecha_hora_registro: str

# Define el modelo para la solicitud de PDF
class PDFRequest(BaseModel):
    asignaciones: List[Asignacion]
    fecha_asignacion: str
    fecha_hora_registro: str

@app.post("/generar_pdf/")
def generar_pdf_asignaciones(request: PDFRequest):
    try:
        # Crear un buffer de memoria
        pdf_buffer = BytesIO()

        # Instanciar la clase Reporte_Asignaciones
        reporte_asignaciones = Reporte_Asignaciones()

        # Generar el PDF usando el buffer
        reporte_asignaciones.generar_pdf(
            request.asignaciones,  # Lista de asignaciones
            request.fecha_asignacion,  # Fecha de asignación
            request.fecha_hora_registro,  # Fecha de última modificación
            pdf_buffer  # Buffer de memoria para escribir el PDF
        )

        # Asegurarse de que el buffer esté al inicio antes de enviarlo
        pdf_buffer.seek(0)

        # Devolver el PDF generado directamente al cliente sin guardarlo en disco
        return StreamingResponse(pdf_buffer, media_type='application/pdf', headers={
            "Content-Disposition": "attachment; filename=asignaciones_tecnicos.pdf"
        })

    except Exception as e:
        return {"error": str(e)}

############### SECCIÓN POWER_BI EMBEBIDO #################
AUTHORITY_URL = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
report_handler = ReportBIGestion()# Instanciar el manejador de reportes

# Función para obtener el token de acceso
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY_URL, client_credential=CLIENT_SECRET
    )
    token_response = app.acquire_token_for_client(scopes=SCOPE)
    access_token = token_response.get('access_token')
    #print("Token de acceso:", access_token)
    return access_token

# Función para obtener la lista de informes o aplicaciones disponibles en Power BI
def get_available_reports(access_token):
    url = "https://api.powerbi.com/v1.0/myorg/reports"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    # Verificar si la respuesta es exitosa
    if response.status_code == 200:
        try:
            reports_data = response.json()
            return reports_data.get('value', [])  # Devuelve la lista de informes si existe
        except ValueError:
            # Error al decodificar el JSON
            print("Error al decodificar la respuesta JSON.")
            print("Contenido de la respuesta:", response.text)
            return []
    else:
        # Manejar errores de respuesta no exitosa
        print(f"Error en la API de Power BI: {response.status_code}")
        print("Contenido de la respuesta:", response.text)
        return []

@app.get("/powerbi", response_class=HTMLResponse)
def get_powerbi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # ---- 1) licencias por cédula
    cedula = user_session.get('username')
    licencias_bi_query = """SELECT licencia_bi, contraseña_licencia FROM licencias_bi WHERE cedula = %s"""
    result = db.fetch_one(query=licencias_bi_query, values=(cedula,))

    # ---- 2) rol_powerbi desde sesión (con fallback a BD si no viene)
    id_rol_bi = int(user_session.get("rol_powerbi") or 0)
    if id_rol_bi <= 0:
        row = db.fetch_one("SELECT rol_powerbi FROM usuarios WHERE id = %s", (user_session["id"],))
        id_rol_bi = int(row[0]) if (row and row[0]) else 0
        req.session["user"]["rol_powerbi"] = id_rol_bi  # sincroniza sesión

    # ---- 3) arma report_data solo si el rol está ACTIVO
    modelo_roles_bi = ModeloRolesPowerBI()
    ids_permitidos = []
    if id_rol_bi > 0:
        rol = modelo_roles_bi.obtener_rol_por_id(id_rol_bi)
        if rol and rol["estado"] == 1:
            ids_permitidos = rol["id_powerbi"]  # List[int]

    from model.gestion_reportbi import ReportBIGestion
    report_handler = ReportBIGestion()
    report_data = report_handler.obtener_reportes_por_ids(ids_permitidos) if ids_permitidos else {}

    default_report_id = ids_permitidos[0] if ids_permitidos else None

    return templates.TemplateResponse("powerbi.html", {
        "request": req,
        "user_session": user_session,
        "licencia_bi": result[0] if result else None,
        "contraseña_licencia": result[1] if result else None,
        "report_data": report_data,
        "default_report_id": default_report_id,
        "error_message": None if result else "No se encontraron licencias para el usuario."
    })

@app.get("/api/get_report_url")
def get_report_url(
    report_id: int = Query(..., title="Report ID"),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    id_rol_bi = int(user_session.get("rol_powerbi") or 0)
    if id_rol_bi <= 0:
        return JSONResponse({"error": "Sin rol Power BI asignado"}, status_code=403)

    modelo = ModeloRolesPowerBI()
    rol = modelo.obtener_rol_por_id(id_rol_bi)
    if not rol or rol["estado"] != 1:
        return JSONResponse({"error": "Rol Power BI inactivo o inexistente"}, status_code=403)

    ids_permitidos = set(rol["id_powerbi"])
    if report_id not in ids_permitidos:
        return JSONResponse({"error": "No autorizado para este informe"}, status_code=403)

    report_handler = ReportBIGestion()
    report_url = report_handler.obtener_url_bi(report_id)
    if report_url and report_url.strip() and report_url != "NaN":
        return JSONResponse({"url": report_url})
    return JSONResponse({"error": "No existe enlace para este informe"}, status_code=404)

@app.post("/cargar_licencias")
async def cargar_licencias(file: UploadFile = File(...)):
    try:
        # Guardar el archivo temporalmente
        file_path = f"temp_{file.filename}"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Instanciar el cargador de licencias y cargar el archivo
        db_conn = psycopg2.connect(DATABASE_PATH)
        cargador = CargueLicenciasBI(db_conn)
        result = cargador.cargar_licencias_excel(file_path)

        # Eliminar el archivo temporal
        os.remove(file_path)

        # Retornar el resultado en formato JSON
        return result
    except Exception as e:
        return {"error": str(e)}

############### SECCIÓN ROLES DE CONTENEDORES BLOB STORAGE #################
# Instancia de la clase Cargue_Roles_Blob_Storage
storage_db = Cargue_Roles_Blob_Storage()

# Obtener contenedores de Blob Storage
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

# Pantalla principal de roles_storage
@app.get("/roles_storage", response_class=HTMLResponse)
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
@app.post("/roles_storage", response_class=HTMLResponse)
async def add_role_storage(req: Request, role_storage_name: str = Form(...), containers: list = Form(...)):
    try:
        # Validaciones
        if not role_storage_name.strip():
            return RedirectResponse(url="/roles_storage?error=Debe ingresar un nombre para el rol", status_code=303)

        if not containers or len(containers) == 0:
            return RedirectResponse(url="/roles_storage?error=Debe seleccionar al menos un contenedor", status_code=303)

        # Inserta el nuevo rol en la base de datos
        storage_db.insert_roles_storage({
            "nombre_rol_storage": role_storage_name,
            "contenedores_asignados": containers
        })

        return RedirectResponse(url="/roles_storage?success=1", status_code=303)
    
    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error: {str(e)}", status_code=303)

# Obtener los datos de un rol específico
@app.get("/roles_storage/{role_storage_id}/datos", response_class=JSONResponse)
async def obtener_datos_role_storage(role_storage_id: int):
    # Obtener los datos del rol desde la base de datos
    role_storage = storage_db.get_role_storage_by_id(role_storage_id)

    if not role_storage:
        raise HTTPException(status_code=404, detail="Rol no encontrado")

    return JSONResponse(content=role_storage)

# Editar un rol existente
@app.post("/roles_storage/{role_storage_id}/editar", response_class=HTMLResponse)
async def update_role_storage(req: Request, role_storage_id: int, role_storage_name: str = Form(...), containers: list = Form(...)):
    try:
        # Validaciones
        if not role_storage_name.strip():
            return RedirectResponse(url="/roles_storage?error=Debe ingresar un nombre para el rol", status_code=303)

        if not containers or len(containers) == 0:
            return RedirectResponse(url="/roles_storage?error=Debe seleccionar al menos un contenedor", status_code=303)

        # Actualizar el rol en la base de datos
        storage_db.update_role_storage(role_storage_id, role_storage_name, containers)

        return RedirectResponse(url="/roles_storage?success=2", status_code=303)
    
    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error: {str(e)}", status_code=303)

# Eliminar un rol
@app.post("/roles_storage/{role_storage_id}/eliminar")
async def eliminar_role_storage(role_storage_id: int):
    try:
        storage_db.delete_role_storage(role_storage_id)
        return RedirectResponse(url="/roles_storage?success=3", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_storage?error=Ocurrió un error al intentar eliminar el rol: {str(e)}", status_code=303)

################## TRANSFERENCIA DE DATOS EN BLOB STORAGE ####################
container_model = ContainerModel()

@app.get("/containers", response_class=HTMLResponse)
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

@app.post("/containers")
async def create_container(data: dict):
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="El nombre del contenedor es requerido")
    try:
        container_model.create_container(name)
        return {"message": f"Contenedor '{name}' creado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/containers/{container_name}/files")
def get_files(container_name: str):
    try:
        files_tree = container_model.get_files(container_name)
        return {"files_tree": files_tree}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/containers/{container_name}/files")
async def upload_file(container_name: str, path: str = "", file: UploadFile = File(...)):
    try:
        # Construir la ruta completa en el contenedor
        full_path = os.path.join(path, file.filename) if path else file.filename
        contents = await file.read()
        await container_model.upload_file(container_name, full_path, contents)
        return JSONResponse(status_code=200, content={"message": f"Archivo {file.filename} subido exitosamente en {full_path}"})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/containers/{container_name}/files/{file_path:path}/download")
def download_file(container_name: str, file_path: str):
    try:
        file_content = container_model.download_file(container_name, file_path)
        return StreamingResponse(BytesIO(file_content), media_type="application/octet-stream",
                                headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"})
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.delete("/containers/{container_name}/files/{file_name:path}")
def delete_file(container_name: str, file_name: str):
    try:
        # Decodificar el nombre del archivo para manejar caracteres especiales y rutas
        decoded_file_name = unquote(file_name)
        container_model.delete_file(container_name, decoded_file_name)
        return {"message": f"Archivo '{decoded_file_name}' eliminado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

################## SECCIÓN JURIDICO ####################
# Plantilla de cargue de planta activa y controles
@app.get("/plantilla_cargue_juridico")
async def descargar_plantilla_juridico():
    file_path = "./cargues/parametros_clausulas.xlsx"
    return FileResponse(path=file_path, filename="parametros_clausulas.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.get("/juridico", response_class=HTMLResponse)
def control_clausulas(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    
    # Obteniendo los datos desde la base de datos
    gestion = GestionClausulas()
    clausulas = gestion.obtener_clausulas()
    clausulas_con_estado = gestion.obtener_clausulas_con_entrega_estado()
    # Solo ajustamos las columnas "Entrega" y "Estado"
    clausulas_dict = {clausula["id"]: clausula for clausula in clausulas_con_estado}
    for clausula in clausulas:
        clausula["fecha_entrega_mas_reciente"] = clausulas_dict.get(clausula["id"], {}).get("fecha_entrega_mas_reciente", "Sin Fecha")
        clausula["estado_mas_reciente"] = clausulas_dict.get(clausula["id"], {}).get("estado_mas_reciente", "Sin Estado")
    etapas = gestion.obtener_opciones_etapas()
    clausulas_lista = gestion.obtener_opciones_clausulas()
    concesiones = gestion.obtener_opciones_concesion()
    contratos = gestion.obtener_opciones_contrato()
    tipos_clausula = gestion.obtener_opciones_tipo_clausula()
    procesos = gestion.obtener_opciones_procesos()
    frecuencias = gestion.obtener_opciones_frecuencias()
    responsables = gestion.obtener_opciones_responsables()
    responsable_entrega = gestion.obtener_opciones_responsables_clausulas()
    estados = gestion.obtener_opciones_estado()
    gestion.close()
    
    # Permisos de edición de la parametrización Juridica solo para el Rol " 1- Administrador y 3- Jurídico"
    es_editable = user_session.get("rol") in [1, 3]

    return templates.TemplateResponse("juridico.html", {
        "request": req, 
        "user_session": user_session,
        "clausulas": clausulas,
        "etapas": etapas,
        "clausulas_lista": clausulas_lista,
        "concesiones": concesiones,
        "contratos": contratos,
        "tipos_clausula": tipos_clausula,
        "procesos": procesos,
        "frecuencias": frecuencias,
        "responsables": responsables,
        "responsable_entrega": responsable_entrega,
        "estados": estados,
        "es_editable": es_editable
    })
    
@app.get("/obtener_subprocesos/{proceso}", response_class=JSONResponse)
def obtener_subprocesos(proceso: str):
    gestion = GestionClausulas()
    subprocesos = gestion.obtener_opciones_subprocesos(proceso)
    gestion.close()
    return subprocesos

@app.get("/filtrar_clausulas", response_class=HTMLResponse)
def filtrar_clausulas(req: Request, control: str = None, etapa: str = None, clausula: str = None, 
                        concesion: str = None, estado: str = None, responsable: str = None, proceso: str = None, subproceso: str = None,
                        user_session: dict = Depends(get_user_session)):

    gestion = GestionClausulas()
    
    # Mapear concesión a contrato antes de aplicar el filtro
    contrato = None
    if concesion and concesion != "Seleccionar...":
        contrato = gestion.obtener_contrato_por_concesion(concesion)
    
    clausulas_filtradas = gestion.obtener_clausulas_filtradas(
        control if control != "Seleccionar..." else None,
        etapa if etapa != "Seleccionar..." else None, 
        clausula if clausula != "Seleccionar..." else None, 
        contrato,
        estado if estado != "Seleccionar..." else None,
        responsable if responsable != "Seleccionar..." else None ,
        proceso if proceso != "Seleccionar..." else None,
        subproceso if subproceso != "Seleccionar..." else None
    )
    gestion.close()

    return templates.TemplateResponse("juridico.html", {
        "request": req, 
        "clausulas": clausulas_filtradas,
        "user_session": user_session
    })

@app.get("/obtener_id_proceso")
async def obtener_id_proceso(proceso: str, subproceso: str):
    try:
        gestion_clausulas = GestionClausulas()
        id_proceso = gestion_clausulas.obtener_id_proceso(proceso, subproceso)
        return JSONResponse(content={"success": True, "id_proceso": id_proceso})
    except Exception as e:
        return JSONResponse(content={"success": False, "message": str(e)}, status_code=400)

@app.post("/clausulas/nueva")
async def crear_clausula(req: Request, control: str = Form(...), etapa: str = Form(...), 
                        clausula: str = Form(...), modificaciones: str = Form(None), 
                        contrato: str = Form(...), tema: str = Form(...), subtema: str = Form(...), 
                        descripcion: str = Form(...), tipo: str = Form(...), norma: str = Form(None), 
                        consecuencia: str = Form(None), frecuencia: str = Form(...), 
                        periodo_control: str = Form(...), inicio_cumplimiento: str = Form(...), 
                        fin_cumplimiento: str = Form(...), observacion: str = Form(None), 
                        procesos_subprocesos: str = Form(...), 
                        responsable_entrega: str = Form(...), ruta_soporte: str = Form(None)):
    try:       
        # Validar campos obligatorios
        #print(f"Datos del formulario recibidos: {locals()}")
        campos_faltantes = [campo for campo, valor in locals().items() if valor == ""]
        if campos_faltantes:
            raise ValueError(f"Faltan los siguientes campos: {', '.join(campos_faltantes)}")

        # Crear la cláusula en la base de datos
        nueva_clausula = {
            "control": control,
            "etapa": etapa,
            "clausula": clausula,
            "modificacion": modificaciones,
            "contrato": contrato,
            "tema": tema,
            "subtema": subtema,
            "descripcion": descripcion,
            "tipo": tipo,
            "norma": norma,
            "consecuencia": consecuencia,
            "frecuencia": frecuencia,
            "periodo_control": periodo_control,
            "inicio_cumplimiento": inicio_cumplimiento,
            "fin_cumplimiento": fin_cumplimiento,
            "observacion": observacion,
            "responsable_entrega": responsable_entrega,
            "ruta_soporte": None  
        }
        
        gestion_clausulas = GestionClausulas()
        clausula_id = gestion_clausulas.crear_clausula(nueva_clausula)
        #print("ID de la cláusula creada:", clausula_id)

        # Generar y registrar la ruta soporte
        ruta_soporte = f"5000-juridica-y-riesgos-juridica-clausulas/{clausula_id}-{clausula.replace(' ', '-')}-{contrato.replace(' ', '-')}"
        gestion_clausulas.registrar_ruta_soporte(clausula_id, ruta_soporte)
        #print(f"Ruta soporte registrada: {ruta_soporte}")

        # Procesar procesos_subprocesos como lista JSON
        procesos_subprocesos = json.loads(procesos_subprocesos)
        #print("Procesos y subprocesos recibidos:", procesos_subprocesos)

        # Registrar cada id_proceso en la tabla auxiliar
        gestion_clausulas.registrar_clausula_proceso_subproceso(clausula_id, procesos_subprocesos)

        # Calcular fechas dinámicas y crear la estructura en Blob Storage
        #print(f"Llamando a calcular_fechas_dinamicas con inicio: {inicio_cumplimiento}, fin: {fin_cumplimiento}, frecuencia: {frecuencia}, periodo_control: {periodo_control}")
        fechas_entrega = gestion_clausulas.calcular_fechas_dinamicas(
            inicio_cumplimiento, fin_cumplimiento, frecuencia, periodo_control
        )
        #print(f"Fechas generadas para la cláusula: {fechas_entrega}")
        #print(f"Creando estructura en Blob Storage con fechas: {[f['entrega'] for f in fechas_entrega]}")
        gestion_clausulas.crear_estructura_blob_storage(
            clausula_id, clausula, contrato, [f["entrega"] for f in fechas_entrega]
        )

        return JSONResponse(content={"success": True, "message": "Cláusula creada exitosamente", "id_clausula": clausula_id})
    
    except Exception as e:
        print(f"Error al crear la cláusula: {e}")
        return JSONResponse(content={"success": False, "message": f"Error al crear la cláusula: {str(e)}"}, status_code=400)
    
@app.get("/clausula/{id}", response_class=JSONResponse)
async def obtener_clausula(id: int):
    gestion_clausulas = GestionClausulas()
    try:
        clausula = gestion_clausulas.obtener_clausula_por_id(id)
        if not clausula:
            raise HTTPException(status_code=404, detail="Cláusula no encontrada")
        
        # Obtener procesos y subprocesos asociados
        procesos_subprocesos = gestion_clausulas.obtener_procesos_subprocesos_por_clausula(id)
        clausula["procesos_subprocesos"] = procesos_subprocesos
        
        return clausula
    finally:
        gestion_clausulas.close()
        
@app.post("/clausula/{id}/actualizar", response_class=JSONResponse)
async def actualizar_clausula(id: int, request: Request):
    form_data = await request.form()
    clausula_data = {key: form_data.get(key) for key in form_data.keys()}

    # Extraer procesos y subprocesos del formulario
    procesos_subprocesos = clausula_data.pop("procesos_subprocesos", None)
    if procesos_subprocesos:
        procesos_subprocesos = json.loads(procesos_subprocesos)

    gestion_clausulas = GestionClausulas()
    try:
        # Actualizar la cláusula principal en la tabla "clausulas"
        gestion_clausulas.actualizar_clausula(id, clausula_data)

        # Si se envían procesos/subprocesos, actualizarlos en la tabla "clausula_proceso_subproceso"
        if procesos_subprocesos:
            gestion_clausulas.actualizar_procesos_subprocesos(id, procesos_subprocesos)

        return {"message": "Cláusula actualizada correctamente"}
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        gestion_clausulas.close()

@app.get("/api/fechas_dinamicas/{clausula_id}")
def obtener_fechas_dinamicas(clausula_id: int):
    gestion_clausulas = GestionClausulas()
    clausula = gestion_clausulas.obtener_clausula_por_id(clausula_id)
    if not clausula:
        raise HTTPException(status_code=404, detail="Cláusula no encontrada")

    fechas = gestion_clausulas.calcular_fechas_dinamicas(
        clausula["inicio_cumplimiento"],
        clausula["fin_cumplimiento"],
        clausula["frecuencia"],
        clausula["periodo_control"], 
    )
    return JSONResponse(content=fechas)

@app.post("/gestion_clausula/{id_clausula}")
async def gestionar_filas_clausula(id_clausula: int, request: Request):
    data = await request.json()
    nuevas_filas = [fila for fila in data.get("filas", []) if not fila.get("id_gestion")]
    filas_existentes = [fila for fila in data.get("filas", []) if fila.get("id_gestion")]

    gestion = GestionClausulas()
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    try:
        # Obtener ruta ya existente
        nombre_clausula = gestion.obtener_clausula_nombre(id_clausula)
        contrato = gestion.obtener_clausula_contrato(id_clausula)

        def normalizar_nombre(nombre):
            return re.sub(r"[^a-zA-Z0-9\-]", "", nombre.replace(" ", "-"))

        ruta_clausula = f"{normalizar_nombre(str(id_clausula))}-{normalizar_nombre(nombre_clausula)}-{normalizar_nombre(contrato)}"

        # Procesar nuevas filas
        for fila in nuevas_filas:
            if "adjunto" in fila and fila["adjunto"]:  # Verificar si hay adjuntos
                try:
                    fecha_entrega = fila.get("fecha_entrega")
                    if not fecha_entrega:
                        raise ValueError("Fecha de entrega no encontrada en la fila.")

                    anio, mes, _ = fecha_entrega.split("-")
                    nombres_adjuntos = []

                    for adjunto in fila["adjunto"].split(", "):  # Adjuntos separados por coma
                        ruta_archivo = f"{ruta_clausula}/{anio}/{mes}/{adjunto}"
                        blob_client = container_client.get_blob_client(ruta_archivo)
                        blob_client.upload_blob(b"Contenido de prueba", overwrite=True)
                        nombres_adjuntos.append(adjunto)

                    fila["adjunto"] = ", ".join(nombres_adjuntos)

                except Exception as e:
                    print(f"Error al procesar adjuntos para la fila: {e}")
                    fila["adjunto"] = None  # Si falla, no bloquear el resto del procesamiento

        # Procesar filas existentes
        for fila in filas_existentes:
            if "adjunto" in fila and fila["adjunto"]:
                try:
                    fecha_entrega = fila.get("fecha_entrega")
                    if not fecha_entrega:
                        raise ValueError("Fecha de entrega no encontrada en la fila.")

                    anio, mes, _ = fecha_entrega.split("-")
                    nombres_adjuntos = []

                    for adjunto in fila["adjunto"].split(", "):  # Adjuntos separados por coma
                        ruta_archivo = f"{ruta_clausula}/{anio}/{mes}/{adjunto}"
                        blob_client = container_client.get_blob_client(ruta_archivo)
                        blob_client.upload_blob(b"Contenido de prueba", overwrite=True)
                        nombres_adjuntos.append(adjunto)

                    fila["adjunto"] = ", ".join(nombres_adjuntos)

                except Exception as e:
                    print(f"Error al procesar adjuntos para la fila existente: {e}")
                    fila["adjunto"] = None  # Si falla, no bloquear el resto del procesamiento

        # Insertar nuevas filas
        if nuevas_filas:
            gestion.insertar_filas_gestion_nuevas(id_clausula, nuevas_filas)

        # Actualizar filas existentes
        if filas_existentes:
            gestion.actualizar_filas_gestion(filas_existentes)

        # Retornar las filas insertadas o actualizadas para mantener sincronización
        filas = gestion.obtener_filas_gestion_por_clausula(id_clausula)
        return JSONResponse(content={"message": "Gestión de filas completada.", "filas": filas})

    except Exception as e:
        print(f"Error al gestionar filas: {e}")
        return JSONResponse(content={"message": f"Error al gestionar filas: {e}"}, status_code=500)

@app.get("/gestion_clausula/{id_clausula}")
def obtener_filas_clausula(id_clausula: int):
    gestion = GestionClausulas()
    try:
        filas = gestion.obtener_filas_gestion_por_clausula(id_clausula)

        # Identificar la fila con la fecha de entrega más reciente
        if filas:
            fila_mas_reciente = max(
                filas, key=lambda x: datetime.strptime(x["fecha_entrega"], "%d/%m/%Y")
            )
            fecha_mas_reciente = fila_mas_reciente["fecha_entrega"]
            estado_mas_reciente = fila_mas_reciente["estado"]
        else:
            fecha_mas_reciente = "Sin Fecha"
            estado_mas_reciente = "Sin Estado"

        return JSONResponse(
            content={
                "filas": filas,
                "fecha_entrega_mas_reciente": fecha_mas_reciente,
                "estado_mas_reciente": estado_mas_reciente,
            }
        )
    finally:
        gestion.close()

@app.get("/clausulas")
def obtener_clausulas_completas():
    gestion = GestionClausulas()
    try:
        clausulas = gestion.obtener_clausulas_con_entrega_estado()
        return JSONResponse(content=clausulas)
    finally:
        gestion.close()

@app.post("/clausulas/adjuntos/{id_clausula}/{anio}/{mes}")
async def cargar_adjuntos(id_clausula: int, anio: str, mes: str, files: List[UploadFile] = File(...), fecha_entrega: str = Form(...)):
    try:
        gestion_clausulas = GestionClausulas()

        # Obtener los datos de la cláusula
        nombre_clausula = gestion_clausulas.obtener_clausula_nombre(id_clausula)
        contrato = gestion_clausulas.obtener_clausula_contrato(id_clausula)

        # Normalizar la ruta dentro del contenedor
        def normalizar_nombre(nombre):
            return re.sub(r"[^a-zA-Z0-9\-]", "", nombre.replace(" ", "-"))

        carpeta_principal = f"{normalizar_nombre(str(id_clausula))}-{normalizar_nombre(nombre_clausula)}-{normalizar_nombre(contrato)}"
        ruta_carpeta = f"{carpeta_principal}/{anio}/{mes}/"

        #print(f"Ruta destino de los adjuntos: {CONTAINER_NAME}/{ruta_carpeta}")

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Subir cada archivo al Blob Storage
        for file in files:
            # Renombrar el archivo agregando la fecha completa
            fecha_entrega_normalizada = fecha_entrega.replace("/", "-")  # Reemplazar "/" por "-" en la fecha
            nombre_archivo = f"{fecha_entrega_normalizada}_{file.filename}"  # Prefijar la fecha al nombre del archivo
            ruta_archivo = f"{ruta_carpeta}{nombre_archivo}" # Mantener la ruta original
            
            # Subir el archivo
            blob_client = container_client.get_blob_client(ruta_archivo)
            blob_client.upload_blob(await file.read(), overwrite=True)

        return JSONResponse(content={"success": True, "message": "Adjuntos cargados correctamente"})
    except Exception as e:
        print(f"Error al cargar adjuntos: {e}")
        return JSONResponse(content={"success": False, "message": str(e)}, status_code=500)

@app.get("/obtener_responsables", response_class=JSONResponse)
async def obtener_responsables(req: Request):
    gestion = GestionClausulas()
    try:
        responsables = gestion.obtener_responsables()
        return {"responsables": responsables}
    finally:
        gestion.close()
    
@app.get("/clausula/{id_clausula}/copia", response_class=JSONResponse) #Obtener Correos configurados para copia
async def obtener_copia(req: Request, id_clausula: int):
    gestion = GestionClausulas()
    try:
        copia = gestion.obtener_copia_correos(id_clausula)
        return copia
    finally:
        gestion.close()

@app.post("/clausula/{id_clausula}/copia", response_class=JSONResponse)
async def actualizar_copia(req: Request, id_clausula: int):
    data = await req.json()
    responsables_copia = data.get("responsables_copia", [])
    if not responsables_copia:
        return {"success": False, "message": "No se enviaron responsables válidos."}, 400
    #print(f"Datos recibidos para actualizar_copia_correos: {responsables_copia}")  # LOG IMPORTANTE
    gestion = GestionClausulas()
    try:
        gestion.actualizar_copia_correos(id_clausula, responsables_copia)
        return {"success": True, "message": "Responsables en copia actualizados correctamente."}
    except ValueError as e:
        print(f"Error al actualizar responsables en copia: {str(e)}")  # LOG DE ERRORES
        return {"success": False, "message": str(e)}, 400
    finally:
        gestion.close()

############################# TAREAS PROGRAMADAS DE ACTUALIZACIÓN #################################
###################################################################################################
# Instancia de la clase para las tareas programadas
tareas_juridico = TareasProgramadasJuridico()

@app.get("/jobs/calcular_fechas", response_class=JSONResponse)
def ejecutar_calculo_fechas():
    """
    Ejecuta manualmente el cálculo y actualización de fechas dinámicas.
    """
    try:
        print("Iniciando job de cálculo de fechas dinámicas.")
        tareas_juridico.calcular_y_actualizar_fechas_dinamicas()
        return {"message": "Cálculo y actualización de fechas dinámicas ejecutado exitosamente."}
    except Exception as e:
        print(f"Error al ejecutar el job: {e}")
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@app.get("/jobs/sincronizar_estados", response_class=JSONResponse)
def ejecutar_sincronizacion_estados():
    """
    Ejecuta manualmente la sincronización de estados dinámicos de las filas.
    """
    try:
        tareas_juridico.sincronizar_estados_filas_gestion()
        return {"message": "Sincronización de estados ejecutada exitosamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

########################## CORREOS AUTOMATICOS JURIDICO ##############################
# NOTIFICACIONES RECORDATORIO POR CORREO Y TAREAS PROGRAMADAS - PANTALLA JURIDICO
gestion = GestionClausulas()
@app.get("/jobs/envio_correos_recordatorio", response_class=JSONResponse)
def envio_correos_recordatorio():
    """
    Prueba manual para enviar correos de recordatorios.
    """
    try:
        tareas_juridico.tarea_diaria_recordatorio()
        return {"message": "Correos de recordatorio enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@app.get("/jobs/envio_correos_incumplimiento", response_class=JSONResponse)
def envio_correos_incumplimiento():
    """
    Prueba manual para enviar correos de incumplimiento.
    """
    try:
        tareas_juridico.tarea_semanal_incumplimientos()
        return {"message": "Correos de incumplimiento enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@app.get("/jobs/envio_correos_incumplimiento_direccion", response_class=JSONResponse)
def envio_correos_incumplimiento_direccion():
    """
    Prueba manual para enviar correos de incumplimiento.
    """
    try:
        tareas_juridico.tarea_semanal_incumplimientos()
        return {"message": "Correos de incumplimiento enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})
    
############################### REPORTE JURÍDICO #################################
# CONSULTA Y DESCARGA A DETALLE LA INFORMACIÓN DE LA GESTIÓN DE CLAUSULAS
# Endpoint para obtener los filtros dinámicos disponibles
def json_serial(obj):
    """Convierte objetos no serializables a JSON."""
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError("Type not serializable")

@app.get("/filtros_reportes")
def filtros_reportes():
    try:
        filtros = gestion.obtener_filtros_disponibles()
        if "error" in filtros:
            return JSONResponse(status_code=500, content=filtros)
        return JSONResponse(content=filtros)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/consulta_reportes")
def consulta_reportes(
    id: str = None, control: str = None, clausula: str = None, etapa: str = None,
    contrato_concesion: str = None, tipo_clausula: str = None, frecuencia: str = None,
    responsable: str = None, fecha_entrega: str = None, plan_accion: str = None,
    estado: str = None, registrado_por: str = None
):
    try:
        filtros = {
            "id": id, "control": control, "clausula": clausula, "etapa": etapa,
            "contrato_concesion": contrato_concesion, "tipo_clausula": tipo_clausula,
            "frecuencia": frecuencia, "responsable": responsable, "fecha_entrega": fecha_entrega,
            "plan_accion": plan_accion, "estado": estado, "registrado_por": registrado_por
        }
        
        # Filtrar solo los valores que no son None ni vacíos
        filtros = {k: v for k, v in filtros.items() if v}

        #print(f"Filtros enviados a la consulta: {filtros}")  # 🔍 Log para depuración

        data = gestion.obtener_reporte_clausulas(**filtros)

        if isinstance(data, dict) and "error" in data:
            print(f"Error en consulta_reportes: {data['error']}")  # 🔍 Log del error
            return JSONResponse(status_code=500, content={"error": data["error"]})

        if not isinstance(data, list):
            print("Error: La respuesta no es una lista.")  # 🔍 Log del error
            return JSONResponse(content=[])

        return JSONResponse(content=data)

    except Exception as e:
        print(f"Error general en consulta_reportes: {str(e)}")  # 🔍 Log del error general
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/descargar_reporte")
def descargar_reporte(formato: str, request: Request):
    try:
        filtros = dict(request.query_params)
        filtros.pop("formato", None)  # Eliminar el formato de los filtros

        file_path, content_type, filename = gestion.exportar_reporte(formato, **filtros)

        if not file_path or not os.path.exists(file_path):
            return JSONResponse(status_code=400, content={"error": "No hay datos para exportar"})

        return FileResponse(path=file_path, filename=filename, media_type=content_type)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
################### PROCESAMIENTO DE TEXTO (NPL) PARA CHATBOT ########################
'''
app.include_router(npl_router , prefix="/npl")

@app.get("/NPL_chatbot", response_class=HTMLResponse, include_in_schema=False)
async def get_NPL_chatbot(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)    
    return templates.TemplateResponse("NPL_chatbot.html", {"request": req, "user_session": user_session})
'''
############################### MODULO DE CHATBOT ##################################
'''
app.include_router(chatbot_router) # Incluir las rutas factorizadas en `route_chatbot.py`

# Ruta para servir el chatbot.html
@app.get("/chatbot", response_class=HTMLResponse, include_in_schema=False)
async def get_chatbot(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)    
    return templates.TemplateResponse("chatbot.html", {"request": req, "user_session": user_session})
'''
############################### MODULO DE CHECKLIST #################################
app.include_router(checklist_router) # Incluir las rutas factorizadas en `route_checklist.py`

@app.get("/checklist", response_class=HTMLResponse)
def checklist(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("checklist.html", {"request": req, "user_session": user_session})

############################### MODULO DE ROLES POWER BI #################################
app.include_router(router_roles_powerbi) # Incluir las rutas factorizadas en `route_roles_powerbi.py`

@app.get("/roles_powerbi", response_class=HTMLResponse)
def roles_powerbi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("roles_powerbi.html", {"request": req, "user_session": user_session})

############################### MODULO DE CENTROS DE OPERACIÓN   #################################
app.include_router(router_cop) # Incluir las rutas factorizadas en `route_cop.py`

@app.get("/centros_operacion", response_class=HTMLResponse)
def cop(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("centro_operacion.html", {"request": req, "user_session": user_session})

############################### MODULO DE BUSES CEXP #################################
app.include_router(router_buses) # Incluir las rutas factorizadas en `route_buses.py`

@app.get("/buses_cexp", response_class=HTMLResponse)
def buses_cexp(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("buses_cexp.html", {"request": req, "user_session": user_session})

############################### MODULO DE RUTAS CEXP #################################
app.include_router(router_rutas) # Incluir las rutas factorizadas en `route_rutas.py`

@app.get("/rutas_cexp", response_class=HTMLResponse)
def rutas_cexp(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("rutas_cexp.html", {"request": req, "user_session": user_session})

############################### MODULO DE SGI   #################################
app.include_router(router_sgi) # Incluir las rutas factorizadas en `route_sgi.py`

@app.get("/sgi", response_class=HTMLResponse)
def sgi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sgi.html", {"request": req, "user_session": user_session})

############################# MODULO DE ESTACIÓN DE SERVICIO + SURTIDOR  ###############################
app.include_router(router_eds) # Incluir las rutas factorizadas en `route_eds.py`

@app.get("/eds_config", response_class=HTMLResponse)
def eds(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("eds_config.html", {"request": req, "user_session": user_session})

############################  MODULO DE REGISTRO COMBUSTIBLE   ###############################
app.include_router(router_eds) # Incluir las rutas factorizadas en `route_eds.py`

@app.get("/eds_registro", response_class=HTMLResponse)
def eds(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("eds_registro.html", {"request": req, "user_session": user_session})

######################  MODULO SNE SERVICIOS NO EJECUTADOS   #########################
app.include_router(router_sne_motivos) # Incluir las rutas factorizadas en `route_sne_motivos.py`
@app.get("/motivos_sne", response_class=HTMLResponse)
def sne_motivos(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_motivos.html", {"request": req, "user_session": user_session})

app.include_router(router_sne) # Incluir las rutas factorizadas en `route_sne.py`
@app.get("/sne_asignacion", response_class=HTMLResponse)
def sne_asignancion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_asignacion.html", {"request": req, "user_session": user_session})

@app.get("/sne_objecion", response_class=HTMLResponse)
def sne_objecion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_objecion.html", {"request": req, "user_session": user_session})
