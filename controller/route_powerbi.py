import os, shutil, msal, requests
from fastapi import APIRouter, Request, Depends, Query, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
# Importaciones de modelos y DB
from model.gestion_usuarios import HandleDB, CargueLicenciasBI
from model.gestion_roles_powerbi import ModeloRolesPowerBI
from model.gestion_reportbi import obtener_reportes_por_ids, obtener_url_bi
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

# HandleDB se usa solo para fetch_one de licencias — no guarda conexión persistente
db = HandleDB()

# ─────────────────────────────────────────────
# Helper de sesión
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get("user")

# ─────────────────────────────────────────────
# Funciones auxiliares Power BI (sin DB)
# ─────────────────────────────────────────────
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY_URL, client_credential=CLIENT_SECRET
    )
    return app.acquire_token_for_client(scopes=SCOPE).get("access_token")

def get_available_reports(access_token):
    url     = "https://api.powerbi.com/v1.0/myorg/reports"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp    = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

# =====================================================================
# ENDPOINTS
# =====================================================================
@router_powerbi.get("/powerbi", response_class=HTMLResponse)
def get_powerbi(req: Request, user_session: dict = Depends(get_user_session)):
    """
    Carga la pantalla Power BI con la lista de reportes del usuario.

    DB: 3 queries rápidas → pool libera la conexión antes de renderizar el HTML.
    Después del render: CERO conexiones activas para esta pantalla.
    """
    if not user_session:
        return RedirectResponse(url="/", status_code=302)

    # ── 1) Licencia BI del usuario ──────────────────────────────────
    cedula = user_session.get("username")
    result = db.fetch_one(
        "SELECT licencia_bi, contraseña_licencia FROM licencias_bi WHERE cedula = %s",
        (cedula,)
    )

    # ── 2) Rol Power BI (sesión o BD como fallback) ─────────────────
    id_rol_bi = int(user_session.get("rol_powerbi") or 0)
    if id_rol_bi <= 0:
        row = db.fetch_one(
            "SELECT rol_powerbi FROM usuarios WHERE id = %s",
            (user_session["id"],)
        )
        id_rol_bi = int(row[0]) if (row and row[0]) else 0
        req.session["user"]["rol_powerbi"] = id_rol_bi

    # ── 3) Reportes permitidos para el rol ──────────────────────────
    # Una sola apertura de conexión: obtiene ids del rol + reportes
    ids_permitidos = []
    report_data    = {}

    if id_rol_bi > 0:
        modelo = ModeloRolesPowerBI()
        rol    = modelo.obtener_rol_por_id(id_rol_bi)   # conn abierta y cerrada
        if rol and rol["estado"] == 1:
            ids_permitidos = rol["id_powerbi"]

    if ids_permitidos:
        report_data = obtener_reportes_por_ids(ids_permitidos)  # conn abierta y cerrada

    # A partir de aquí: CERO conexiones DB activas ───────────────────
    default_report_id = ids_permitidos[0] if ids_permitidos else None

    return templates.TemplateResponse("powerbi.html", {
        "request":            req,
        "user_session":       user_session,
        "licencia_bi":        result[0] if result else None,
        "contraseña_licencia": result[1] if result else None,
        "report_data":        report_data,
        "default_report_id":  default_report_id,
        "error_message":      None if result else "No se encontraron licencias para el usuario.",
    })

@router_powerbi.get("/api/get_report_url")
def get_report_url(
    report_id: int = Query(..., title="Report ID"),
    user_session: dict = Depends(get_user_session)
):
    """
    Valida el rol del usuario y devuelve la URL del informe.

    DB: 2 queries (rol + url) → conexión liberada ANTES de retornar el JSON.
    El iframe se carga en el browser sin ninguna conexión DB activa.
    """
    if not user_session:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    id_rol_bi = int(user_session.get("rol_powerbi") or 0)
    if id_rol_bi <= 0:
        return JSONResponse({"error": "Sin rol Power BI asignado"}, status_code=403)

    # ── Validar rol ─────────────────────────────────────────────────
    modelo = ModeloRolesPowerBI()
    rol    = modelo.obtener_rol_por_id(id_rol_bi)  # conn abierta y cerrada

    if not rol or rol["estado"] != 1:
        return JSONResponse({"error": "Rol Power BI inactivo o inexistente"}, status_code=403)

    if report_id not in set(rol["id_powerbi"]):
        return JSONResponse({"error": "No autorizado para este informe"}, status_code=403)

    # ── Obtener URL ──────────────────────────────────────────────────
    report_url = obtener_url_bi(report_id)   # conn abierta y cerrada

    # A partir de aquí: CERO conexiones DB activas ───────────────────
    if report_url:
        return JSONResponse({"url": report_url})

    return JSONResponse({"error": "No existe enlace para este informe"}, status_code=404)

@router_powerbi.post("/cargar_licencias")
async def cargar_licencias(file: UploadFile = File(...)):
    """Carga masiva de licencias BI desde Excel."""
    try:
        file_path = f"temp_{file.filename}"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        with get_db_connection() as conn:
            cargador = CargueLicenciasBI(conn)
            result   = cargador.cargar_licencias_excel(file_path)

        os.remove(file_path)
        return result

    except Exception as e:
        return {"error": str(e)}