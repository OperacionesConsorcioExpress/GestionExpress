from fastapi import APIRouter, Depends, Request, HTTPException, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import List
# Importaciones de modelos y DB
from model.gestion_roles_powerbi import ModeloRolesPowerBI
from model.gestion_reportbi import obtener_opciones_reportes, resolver_nombres_reportes

router_roles_powerbi = APIRouter()
templates = Jinja2Templates(directory="./view")

def obtener_sesion_usuario(req: Request):
    return req.session.get("user")

# ─────────────────────────────────────────────
# Vista principal
# ─────────────────────────────────────────────
@router_roles_powerbi.get("/roles_powerbi", response_class=HTMLResponse)
def vista_roles_powerbi(req: Request, sesion_usuario: dict = Depends(obtener_sesion_usuario)):
    if not sesion_usuario:
        return RedirectResponse(url="/", status_code=302)

    modelo = ModeloRolesPowerBI()

    # Opciones para el <select> — una query, conexión liberada
    opciones_reportes = obtener_opciones_reportes()

    # Roles existentes — una query, conexión liberada
    filas_roles = modelo.obtener_todos_roles()

    roles = []
    for fila in filas_roles:
        lista_ids       = [int(x) for x in fila[2].split(",") if x.strip().isdigit()]
        nombres_resueltos = resolver_nombres_reportes(lista_ids)  # una query, liberada
        roles.append({
            "id":              fila[0],
            "nombre":          fila[1],
            "ids":             lista_ids,
            "ids_csv":         fila[2],
            "estado":          fila[3],
            "nombres_reportes": nombres_resueltos,
        })

    return templates.TemplateResponse("roles_powerbi.html", {
        "request":         req,
        "user_session":    sesion_usuario,
        "opciones_reportes": opciones_reportes,
        "roles":           roles,
        "success_message": req.query_params.get("success"),
        "error_message":   req.query_params.get("error"),
    })

# ─────────────────────────────────────────────
# Crear rol
# ─────────────────────────────────────────────
@router_roles_powerbi.post("/roles_powerbi", response_class=HTMLResponse)
async def crear_rol_powerbi(
    req: Request,
    nombre_rol: str = Form(...),
    id_powerbi: List[str] = Form(...)
):
    if not nombre_rol.strip():
        return RedirectResponse(
            url="/roles_powerbi?error=Debe ingresar un nombre para el rol",
            status_code=303
        )
    if not id_powerbi:
        return RedirectResponse(
            url="/roles_powerbi?error=Debe seleccionar al menos un reporte",
            status_code=303
        )

    lista_ids = [int(v) for v in id_powerbi if isinstance(v, str) and v.strip().isdigit()]
    if not lista_ids:
        return RedirectResponse(
            url="/roles_powerbi?error=Selección inválida de reportes",
            status_code=303
        )

    try:
        ModeloRolesPowerBI().insertar_rol(nombre_rol, lista_ids)
        return RedirectResponse(url="/roles_powerbi?success=Rol creado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=Ocurrió un error: {e}", status_code=303)

# ─────────────────────────────────────────────
# Datos de un rol (para modal editar)
# ─────────────────────────────────────────────
@router_roles_powerbi.get("/roles_powerbi/{id_rol}/datos", response_class=JSONResponse)
async def obtener_datos_rol_powerbi(id_rol: int):
    rol = ModeloRolesPowerBI().obtener_rol_por_id(id_rol)
    if not rol:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    return JSONResponse(content=rol)

# ─────────────────────────────────────────────
# Editar rol
# ─────────────────────────────────────────────
@router_roles_powerbi.post("/roles_powerbi/{id_rol}/editar", response_class=HTMLResponse)
async def editar_rol_powerbi(
    req: Request,
    id_rol: int,
    nombre_rol: str = Form(...),
    id_powerbi: List[str] = Form(...)
):
    if not nombre_rol.strip():
        return RedirectResponse(
            url="/roles_powerbi?error=Debe ingresar un nombre para el rol",
            status_code=303
        )
    if not id_powerbi:
        return RedirectResponse(
            url="/roles_powerbi?error=Debe seleccionar al menos un reporte",
            status_code=303
        )

    lista_ids = [int(v) for v in id_powerbi if isinstance(v, str) and v.strip().isdigit()]
    if not lista_ids:
        return RedirectResponse(
            url="/roles_powerbi?error=Selección inválida de reportes",
            status_code=303
        )

    try:
        ModeloRolesPowerBI().actualizar_rol(id_rol, nombre_rol, lista_ids)
        return RedirectResponse(url="/roles_powerbi?success=Rol actualizado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=Ocurrió un error: {e}", status_code=303)

# ─────────────────────────────────────────────
# Inactivar / Activar rol
# ─────────────────────────────────────────────
@router_roles_powerbi.post("/roles_powerbi/{id_rol}/inactivar", response_class=HTMLResponse)
async def inactivar_rol_powerbi(id_rol: int):
    try:
        ModeloRolesPowerBI().cambiar_estado(id_rol, 0)
        return RedirectResponse(url="/roles_powerbi?success=Rol inactivado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=No fue posible inactivar: {e}", status_code=303)


@router_roles_powerbi.post("/roles_powerbi/{id_rol}/activar", response_class=HTMLResponse)
async def activar_rol_powerbi(id_rol: int):
    try:
        ModeloRolesPowerBI().cambiar_estado(id_rol, 1)
        return RedirectResponse(url="/roles_powerbi?success=Rol activado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=No fue posible activar: {e}", status_code=303)