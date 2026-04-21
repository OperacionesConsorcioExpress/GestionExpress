######################### Importar librerías necesarias #################################
import os, logging
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer
from starlette.middleware.sessions import SessionMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

################## Importar Backend de Herramientas ###########################
from lib.verifcar_clave import check_user

################### Importar Controladores Endpoint ##########################
from controller.route_usuarios import router_usuarios
from controller.route_asigna_ccz import router_asigna_ccz
#from controller.route_chatbot import chatbot_router # Comentar
#from controller.route_NPL_chatbot import npl_router # Comentar
from controller.route_checklist import checklist_router
from controller.route_roles_powerbi import router_roles_powerbi
from controller.route_powerbi import router_powerbi
from controller.route_blobstorage import router_blobstorage
from controller.route_clausulas import router_clausulas
from controller.route_sgi import router_sgi
from controller.route_cop import router_cop
from controller.route_buses import router_buses
from controller.route_rutas import router_rutas
from controller.route_eds import router_eds
from controller.route_eds_kilometros import router_eds_kilometros
from controller.route_sne_motivos import router_sne_motivos
from controller.route_sne_plantillas import router_sne_plantillas
from controller.route_sne_asignacion import router_sne_asignacion
from controller.route_sne_objecion import router_sne_objecion
from controller.route_sne_monitor import router_sne_monitor

##################### Importar Modelos Backend ##########################
from database.database_manager import get_db_connection, get_pool_status, close_pool, reset_circuit_breaker

############################### Carga de Variables de Entorno ###########################
load_dotenv()
logger = logging.getLogger("main")
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

####################### Ciclo de vida de la aplicación ##########################
# Startup y shutdown de la aplicación, pool de conexiones se inicialice al arrancar y se cierre al detener la app.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    print("🟢 [main] Aplicación iniciando...")
    yield
    # ── Shutdown ──
    close_pool()
    print("🔴 [main] Aplicación detenida — pool DB cerrado.")

####################### Iniciar la aplicación FastAPI y configurar middleware ##########################
app = FastAPI(lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY"), max_age=1800) # la sesión expira después de 30 minutos (1800 segundos) de inactividad
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="./view")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Habilitar CORS para permitir solicitudes desde el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todas las solicitudes de origen cruzado
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Respetar X-Forwarded-Proto de Azure App Service (SSL termina en el proxy, no en la app)
# Evita que url_for() y RedirectResponse generen URLs con http:// en producción
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

############################### RUTAS INICIALES DEL SISTEMA ###########################
# ─────────────────────────────────────────────────────────────────────────────
# Health Check — GET /health
# ─────────────────────────────────────────────────────────────────────────────
# Monitorea el estado real de la app y la base de datos Azure App Service 
# Campos clave en la respuesta:
#   circuit_breaker → CLOSED=ok | OPEN=bloqueado | HALF=recuperando

@app.get("/health")
def health_check():
    db_status = get_pool_status()
    return {
        "status": "🟢 Gestión Express activo",
        "database": db_status
    }

# ─────────────────────────────────────────────────────────────────────────────
# Reset Circuit Breaker — POST /admin/reset-circuit-breaker
# ─────────────────────────────────────────────────────────────────────────────
# Resetea el circuit breaker cuando quedó OPEN por fallos de un redeploy pero la DB ya está funcionando (verificar con GET /health primero).

#Flujo recomendado ante circuit_breaker OPEN:
#   1. GET  /health                  → verificar db_ping = "ok"
#   2. POST /reset-circuit-breaker   → resetear estado OPEN → CLOSED
#   3. GET  /health                  → confirmar circuit_breaker = "CLOSED"

@app.get("/reset-circuit-breaker")
def admin_reset_circuit_breaker():
    resultado = reset_circuit_breaker()
    logger.info("🔄 Circuit Breaker reseteado manualmente")
    return resultado

# Función para verificar si el usuario ha iniciado sesión
def get_user_session(req: Request):
    return req.session.get('user')

# Ruta principal
@app.get("/", response_class=HTMLResponse)
def root(req: Request, user_session: dict = Depends(get_user_session)):
    return templates.TemplateResponse("index.html", {"request": req, "user_session": user_session}, title="Centro de Información - Gestión Express")

# Ruta Login
@app.post("/", response_class=HTMLResponse)
def login(req: Request, username: str = Form(...), password_user: str = Form(...)):
    # Verifica las credenciales del usuario
    verify, nombres, apellidos = check_user(username, password_user)
    
    if verify:
        # Obtén la información completa del usuario desde la base de datos
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM usuarios WHERE username = %s", (username,))
                user_data = cur.fetchone()

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

###################### ADMINISTRACIÓN DE USUARIOS ########################
app.include_router(router_usuarios)
@app.get("/inicio", response_class=HTMLResponse)
def registrarse(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)    
    return templates.TemplateResponse("inicio.html", {"request": req, "user_session": user_session})

############################### MODULO DE ROLES POWER BI #################################
app.include_router(router_roles_powerbi)
@app.get("/roles_powerbi", response_class=HTMLResponse)
def roles_powerbi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("roles_powerbi.html", {"request": req, "user_session": user_session})

######################## ASINGNACION DE CONTROLES EN CENTRO DE CONTROL ########################
app.include_router(router_asigna_ccz)
@app.get("/asignacion", response_class=HTMLResponse)
def asignacion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("asignacion.html", {"request": req, "user_session": user_session})

############### SECCIÓN POWER_BI POR MEDIO DE IFRAME Y API PARA OBTENER URLS DE REPORTES SEGÚN ROL ###############
app.include_router(router_powerbi)
@router_powerbi.get("/powerbi", response_class=HTMLResponse)
def get_powerbi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("powerbi.html", {"request": req, "user_session": user_session})

################## TRANSFERENCIA DE DATOS EN BLOB STORAGE ####################
app.include_router(router_blobstorage)
@router_blobstorage.get("/containers", response_class=HTMLResponse)
def get_containers(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("containers.html", {"request": req, "user_session": user_session})

################## SECCIÓN JURIDICO ####################
app.include_router(router_clausulas)
@router_clausulas.get("/juridico", response_class=HTMLResponse)
def control_clausulas(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("juridico.html", {"request": req, "user_session": user_session})

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
app.include_router(checklist_router) 
@app.get("/checklist", response_class=HTMLResponse)
def checklist(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("checklist.html", {"request": req, "user_session": user_session})

############################### MODULO DE CENTROS DE OPERACIÓN   #################################
app.include_router(router_cop) 
@app.get("/centros_operacion", response_class=HTMLResponse)
def cop(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("centro_operacion.html", {"request": req, "user_session": user_session})

############################### MODULO DE BUSES CEXP #################################
app.include_router(router_buses) 
@app.get("/buses_cexp", response_class=HTMLResponse)
def buses_cexp(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("buses_cexp.html", {"request": req, "user_session": user_session})

############################### MODULO DE RUTAS CEXP #################################
app.include_router(router_rutas) 
@app.get("/rutas_cexp", response_class=HTMLResponse)
def rutas_cexp(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("rutas_cexp.html", {"request": req, "user_session": user_session})

############################### MODULO DE SGI   #################################
app.include_router(router_sgi) 
@app.get("/sgi", response_class=HTMLResponse)
def sgi(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sgi.html", {"request": req, "user_session": user_session})

############################# MODULO DE ESTACIÓN DE SERVICIO + SURTIDOR  ###############################
app.include_router(router_eds) 
@app.get("/eds_config", response_class=HTMLResponse)
def eds(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("eds_config.html", {"request": req, "user_session": user_session})

############################  MODULO DE REGISTRO COMBUSTIBLE   ###############################
app.include_router(router_eds) 
@app.get("/eds_registro", response_class=HTMLResponse)
def eds(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("eds_registro.html", {"request": req, "user_session": user_session})

app.include_router(router_eds_kilometros) 
@app.get("/eds_kilometros", response_class=HTMLResponse)
def eds(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("eds_kilometros.html", {"request": req, "user_session": user_session})

######################  MODULO SNE SERVICIOS NO EJECUTADOS   #########################
app.include_router(router_sne_motivos) 
@app.get("/sne_motivos", response_class=HTMLResponse)
def sne_motivos(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_motivos.html", {"request": req, "user_session": user_session})

app.include_router(router_sne_plantillas)
@app.get("/sne_plantillas", response_class=HTMLResponse)
def sne_plantillas(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_plantillas.html", {"request": req, "user_session": user_session})

app.include_router(router_sne_asignacion) 
@app.get("/sne_asignacion", response_class=HTMLResponse)
def sne_asignancion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_asignacion.html", {"request": req, "user_session": user_session})

app.include_router(router_sne_objecion) 
@app.get("/sne_objecion", response_class=HTMLResponse)
def sne_objecion(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_objecion.html", {"request": req, "user_session": user_session})

app.include_router(router_sne_monitor) 
@app.get("/sne_monitor", response_class=HTMLResponse)
def sne_monitor(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("sne_monitor.html", {"request": req, "user_session": user_session})
