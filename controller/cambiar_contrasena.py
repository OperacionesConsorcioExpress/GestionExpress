from fastapi import Request
from fastapi.responses import JSONResponse
from werkzeug.security import generate_password_hash
from lib.cambiar_contrasena_db import actualizar_contrasena_db

async def cambiar_contrasena_post(request: Request, user_session: dict):
    if not user_session:
        #print("[Cambiar contraseña] Sesión inválida")
        return JSONResponse(content={"success": False, "message": "Sesión no válida."}, status_code=401)

    data = await request.json()
    nueva_password = data.get("password")

    if not nueva_password or len(nueva_password) < 6:
        #print("[Cambiar contraseña] Contraseña corta o vacía")
        return JSONResponse(content={"success": False, "message": "La contraseña debe tener mínimo 6 caracteres."}, status_code=400)

    # Busca el campo correcto para id en sesión
    user_id = user_session.get("id") or user_session.get("user_id") or user_session.get("usuario_id")
    #print("[Cambiar contraseña] user_id en sesión:", user_id)
    if not user_id:
        return JSONResponse(content={"success": False, "message": "No se pudo identificar el usuario logueado."}, status_code=400)
    
    try:
        user_id = int(user_id)
    except Exception as e:
        #print("[Cambiar contraseña] Error convirtiendo id:", e)
        return JSONResponse(content={"success": False, "message": "El id de usuario es inválido."}, status_code=400)

    # Generar hash seguro
    password_hash = generate_password_hash(nueva_password, method="pbkdf2:sha256", salt_length=30)
    #print("[Cambiar contraseña] hash generado:", password_hash)
    actualizado = actualizar_contrasena_db(user_id, password_hash)

    if actualizado:
        #print("[Cambiar contraseña] Actualización exitosa.")
        return JSONResponse(content={"success": True})
    else:
        #print("[Cambiar contraseña] No se pudo actualizar la contraseña.")
        return JSONResponse(content={"success": False, "message": "No se pudo actualizar la contraseña. Intente de nuevo."}, status_code=500)