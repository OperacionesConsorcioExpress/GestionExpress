import os, shutil, msal, requests
from fastapi import APIRouter, Request, Depends, Query, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from model.gestion_usuarios import HandleDB, CargueLicenciasBI
from model.gestion_roles_powerbi import ModeloRolesPowerBI
from model.gestion_reportbi import ReportBIGestion
from database.database_manager import get_db_connection

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
load_dotenv()
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID     = os.getenv("TENANT_ID")

AUTHORITY_URL = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE         = ["https://analysis.windows.net/powerbi/api/.default"]

router_powerbi = APIRouter(tags=["powerbi"])
templates      = Jinja2Templates(directory="./view")

# Instanciar el manejador de reportes — igual que en main.py
report_handler = ReportBIGestion()

# ─────────────────────────────────────────────
# Helper de sesión — igual que en main.py
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────
# conecar a la clase de usuarios para obtener licencias
# ─────────────────────────────────────────────
db = HandleDB()

# =====================================================================
# Funciones auxiliares de Power BI — igual que en main.py
# =====================================================================
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY_URL, client_credential=CLIENT_SECRET
    )
    token_response = app.acquire_token_for_client(scopes=SCOPE)
    access_token = token_response.get('access_token')
    #print("Token de acceso:", access_token)
    return access_token

def get_available_reports(access_token):
    url = "https://api.powerbi.com/v1.0/myorg/reports"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            reports_data = response.json()
            return reports_data.get('value', [])
        except ValueError:
            print("Error al decodificar la respuesta JSON.")
            print("Contenido de la respuesta:", response.text)
            return []
    else:
        print(f"Error en la API de Power BI: {response.status_code}")
        print("Contenido de la respuesta:", response.text)
        return []

# =====================================================================
# ENDPOINTS
# =====================================================================
@router_powerbi.get("/powerbi", response_class=HTMLResponse)
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

@router_powerbi.get("/api/get_report_url")
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

@router_powerbi.post("/cargar_licencias")
async def cargar_licencias(file: UploadFile = File(...)):
    try:
        # Guardar el archivo temporalmente
        file_path = f"temp_{file.filename}"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Instanciar el cargador de licencias y cargar el archivo
        with get_db_connection() as db_conn:
            cargador = CargueLicenciasBI(db_conn)
            result = cargador.cargar_licencias_excel(file_path)

        # Eliminar el archivo temporal
        os.remove(file_path)

        # Retornar el resultado en formato JSON
        return result
    except Exception as e:
        return {"error": str(e)}