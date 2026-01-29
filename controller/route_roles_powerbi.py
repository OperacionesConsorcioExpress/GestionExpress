from fastapi import APIRouter, Depends, Request, HTTPException, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import List
from model.roles_powerbi import ModeloRolesPowerBI

# Crear router
router_roles_powerbi = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Sesión de usuario (local a este router)
def obtener_sesion_usuario(req: Request):
    return req.session.get('user')

@router_roles_powerbi.get("/roles_powerbi", response_class=HTMLResponse)
def vista_roles_powerbi(req: Request, sesion_usuario: dict = Depends(obtener_sesion_usuario)):
    if not sesion_usuario:
        return RedirectResponse(url="/", status_code=302)

    modelo = ModeloRolesPowerBI()

    # Opciones de reportes para el multiselección
    opciones_reportes = modelo.obtener_opciones_reportes()  # [{id, etiqueta}]

    # Roles existentes
    filas_roles = modelo.obtener_todos_roles()  # [(id, nombre, id_powerbi_csv, estado)]
    roles = []
    for fila in filas_roles:
        lista_ids = [int(x) for x in fila[2].split(",") if x.strip().isdigit()]
        nombres_resueltos = modelo.resolver_nombres_reportes(lista_ids)
        roles.append({
            "id": fila[0],
            "nombre": fila[1],
            "ids": lista_ids,
            "ids_csv": fila[2],
            "estado": fila[3],
            "nombres_reportes": nombres_resueltos
        })

    mensaje_exito = req.query_params.get("success")
    mensaje_error = req.query_params.get("error")

    return templates.TemplateResponse(
        "roles_powerbi.html",
        {
            "request": req,
            "user_session": sesion_usuario,
            "opciones_reportes": opciones_reportes,
            "roles": roles,
            "success_message": mensaje_exito,
            "error_message": mensaje_error
        }
    )

@router_roles_powerbi.post("/roles_powerbi", response_class=HTMLResponse)
async def crear_rol_powerbi(
    req: Request,
    nombre_rol: str = Form(...),
    id_powerbi: List[str] = Form(...)
):
    try:
        if not nombre_rol.strip():
            return RedirectResponse(url="/roles_powerbi?error=Debe ingresar un nombre para el rol", status_code=303)
        if not id_powerbi:
            return RedirectResponse(url="/roles_powerbi?error=Debe seleccionar al menos un reporte", status_code=303)

        # Sanitizar a enteros
        lista_ids = []
        for v in id_powerbi:
            if isinstance(v, str) and v.strip().isdigit():
                lista_ids.append(int(v))
        if not lista_ids:
            return RedirectResponse(url="/roles_powerbi?error=Selección inválida de reportes", status_code=303)

        modelo = ModeloRolesPowerBI()
        modelo.insertar_rol(nombre_rol, lista_ids)
        return RedirectResponse(url="/roles_powerbi?success=Rol creado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=Ocurrió un error: {str(e)}", status_code=303)

@router_roles_powerbi.get("/roles_powerbi/{id_rol}/datos", response_class=JSONResponse)
async def obtener_datos_rol_powerbi(id_rol: int):
    modelo = ModeloRolesPowerBI()
    rol = modelo.obtener_rol_por_id(id_rol)
    if not rol:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    return JSONResponse(content=rol)

@router_roles_powerbi.post("/roles_powerbi/{id_rol}/editar", response_class=HTMLResponse)
async def editar_rol_powerbi(
    req: Request,
    id_rol: int,
    nombre_rol: str = Form(...),
    id_powerbi: List[str] = Form(...)
):
    try:
        if not nombre_rol.strip():
            return RedirectResponse(url="/roles_powerbi?error=Debe ingresar un nombre para el rol", status_code=303)
        if not id_powerbi:
            return RedirectResponse(url="/roles_powerbi?error=Debe seleccionar al menos un reporte", status_code=303)

        lista_ids = []
        for v in id_powerbi:
            if isinstance(v, str) and v.strip().isdigit():
                lista_ids.append(int(v))
        if not lista_ids:
            return RedirectResponse(url="/roles_powerbi?error=Selección inválida de reportes", status_code=303)

        modelo = ModeloRolesPowerBI()
        modelo.actualizar_rol(id_rol, nombre_rol, lista_ids)
        return RedirectResponse(url="/roles_powerbi?success=Rol actualizado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=Ocurrió un error: {str(e)}", status_code=303)

@router_roles_powerbi.post("/roles_powerbi/{id_rol}/inactivar", response_class=HTMLResponse)
async def inactivar_rol_powerbi(id_rol: int):
    try:
        modelo = ModeloRolesPowerBI()
        modelo.cambiar_estado(id_rol, 0)
        return RedirectResponse(url="/roles_powerbi?success=Rol inactivado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=No fue posible inactivar: {str(e)}", status_code=303)

@router_roles_powerbi.post("/roles_powerbi/{id_rol}/activar", response_class=HTMLResponse)
async def activar_rol_powerbi(id_rol: int):
    try:
        modelo = ModeloRolesPowerBI()
        modelo.cambiar_estado(id_rol, 1)
        return RedirectResponse(url="/roles_powerbi?success=Rol activado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_powerbi?error=No fue posible activar: {str(e)}", status_code=303)