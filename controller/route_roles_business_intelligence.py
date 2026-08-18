from fastapi import APIRouter, Depends, Request, HTTPException, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import List
from model.gestion_roles_bi import ModeloRolesBi, obtener_opciones_graficas, construir_mapa_nombres

router_roles_bi = APIRouter()
templates = Jinja2Templates(directory="./view")

def obtener_sesion_usuario(req: Request):
    return req.session.get("user")

# ─────────────────────────────────────────────
# Vista principal
# ─────────────────────────────────────────────
@router_roles_bi.get("/roles_bi", response_class=HTMLResponse)
def vista_roles_bi(req: Request, sesion: dict = Depends(obtener_sesion_usuario)):
    if not sesion:
        return RedirectResponse(url="/", status_code=302)

    modelo         = ModeloRolesBi()
    opciones       = obtener_opciones_graficas()
    mapa_nombres   = construir_mapa_nombres()
    filas_roles    = modelo.obtener_todos_roles()

    roles = []
    for fila in filas_roles:
        lista_graficas = [g.strip() for g in fila[2].split(",") if g.strip()]
        roles.append({
            "id":               fila[0],
            "nombre":           fila[1],
            "graficas":         lista_graficas,
            "graficas_csv":     fila[2],
            "estado":           fila[3],
            "nombres_graficas": [mapa_nombres.get(g, g) for g in lista_graficas],
        })

    return templates.TemplateResponse("roles_business_intelligence.html", {
        "request":           req,
        "user_session":      sesion,
        "opciones_graficas": opciones,
        "roles":             roles,
        "success_message":   req.query_params.get("success"),
        "error_message":     req.query_params.get("error"),
    })


# ─────────────────────────────────────────────
# Crear rol
# ─────────────────────────────────────────────
@router_roles_bi.post("/roles_bi", response_class=HTMLResponse)
async def crear_rol_bi(
    req: Request,
    nombre_rol: str       = Form(...),
    graficas:   List[str] = Form(...),
):
    if not nombre_rol.strip():
        return RedirectResponse(
            url="/roles_bi?error=Debe ingresar un nombre para el rol",
            status_code=303,
        )
    if not graficas:
        return RedirectResponse(
            url="/roles_bi?error=Debe seleccionar al menos una visualización",
            status_code=303,
        )
    try:
        ModeloRolesBi().insertar_rol(nombre_rol, graficas)
        return RedirectResponse(url="/roles_bi?success=Rol creado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_bi?error=Ocurrió un error: {e}", status_code=303)


# ─────────────────────────────────────────────
# Datos de un rol (para modal editar)
# ─────────────────────────────────────────────
@router_roles_bi.get("/roles_bi/{id_rol}/datos", response_class=JSONResponse)
async def obtener_datos_rol_bi(id_rol: int):
    rol = ModeloRolesBi().obtener_rol_por_id(id_rol)
    if not rol:
        raise HTTPException(status_code=404, detail="Rol no encontrado")
    return JSONResponse(content=rol)


# ─────────────────────────────────────────────
# Editar rol
# ─────────────────────────────────────────────
@router_roles_bi.post("/roles_bi/{id_rol}/editar", response_class=HTMLResponse)
async def editar_rol_bi(
    req: Request,
    id_rol: int,
    nombre_rol: str       = Form(...),
    graficas:   List[str] = Form(...),
):
    if not nombre_rol.strip():
        return RedirectResponse(
            url="/roles_bi?error=Debe ingresar un nombre para el rol",
            status_code=303,
        )
    if not graficas:
        return RedirectResponse(
            url="/roles_bi?error=Debe seleccionar al menos una visualización",
            status_code=303,
        )
    try:
        ModeloRolesBi().actualizar_rol(id_rol, nombre_rol, graficas)
        return RedirectResponse(url="/roles_bi?success=Rol actualizado correctamente.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_bi?error=Ocurrió un error: {e}", status_code=303)


# ─────────────────────────────────────────────
# Inactivar / Activar rol
# ─────────────────────────────────────────────
@router_roles_bi.post("/roles_bi/{id_rol}/inactivar", response_class=HTMLResponse)
async def inactivar_rol_bi(id_rol: int):
    try:
        ModeloRolesBi().cambiar_estado(id_rol, 0)
        return RedirectResponse(url="/roles_bi?success=Rol inactivado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_bi?error=No fue posible inactivar: {e}", status_code=303)


@router_roles_bi.post("/roles_bi/{id_rol}/activar", response_class=HTMLResponse)
async def activar_rol_bi(id_rol: int):
    try:
        ModeloRolesBi().cambiar_estado(id_rol, 1)
        return RedirectResponse(url="/roles_bi?success=Rol activado.", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/roles_bi?error=No fue posible activar: {e}", status_code=303)
