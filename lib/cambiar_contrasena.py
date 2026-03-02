from fastapi import Request
from fastapi.responses import JSONResponse
from werkzeug.security import generate_password_hash
import re
from model.gestion_usuarios import HandleDB

# Validador de contraseña segura
def es_contrasena_segura(password: str) -> bool:
    if len(password) < 8:
        return False
    if not re.search(r"[A-Z]", password):  # al menos una mayúscula
        return False
    if not re.search(r"[a-z]", password):  # al menos una minúscula
        return False
    if not re.search(r"[0-9]", password):  # al menos un número
        return False
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>_\-+=/\\[\]]", password):  # al menos un símbolo especial
        return False
    return True

async def cambiar_contrasena_post(request: Request, user_session: dict):
    if not user_session:
        return JSONResponse(content={"success": False, "message": "Sesión no válida."}, status_code=401)

    try:
        data = await request.json()
        nueva_password = data.get("password")

        if not nueva_password:
            return JSONResponse(content={"success": False, "message": "La contraseña no puede estar vacía."}, status_code=400)

        if not es_contrasena_segura(nueva_password):
            return JSONResponse(
                content={
                    "success": False,
                    "message": "La contraseña debe tener mínimo 8 caracteres, incluyendo una mayúscula, una minúscula, un número y un carácter especial. Ejemplo: Clave@123"
                },
                status_code=400
            )

        user_id = user_session.get("id") or user_session.get("user_id") or user_session.get("usuario_id")
        if not user_id:
            return JSONResponse(content={"success": False, "message": "No se pudo identificar el usuario logueado."}, status_code=400)

        user_id = int(user_id)
        password_hash = generate_password_hash(nueva_password, method="pbkdf2:sha256", salt_length=12)
        actualizado = actualizar_contrasena_db(user_id, password_hash)

        if actualizado:
            return JSONResponse(content={"success": True})
        else:
            return JSONResponse(content={"success": False, "message": "No se pudo actualizar la contraseña. Intente de nuevo."}, status_code=500)

    except Exception:
        return JSONResponse(content={"success": False, "message": "Ocurrió un error inesperado."}, status_code=500)
    
def actualizar_contrasena_db(usuario_id, nueva_password_hash):
    try:
        #print("[DB] Intentando actualizar contraseña para id:", usuario_id)
        db = HandleDB()
        query = "UPDATE usuarios SET password_user = %s WHERE id = %s"
        connection = db._get_connection()
        try:
            with connection.cursor() as cur:
                cur.execute(query, (nueva_password_hash, usuario_id))
                connection.commit()
                #print("[DB] Rowcount:", cur.rowcount)
                return cur.rowcount == 1
        except Exception:
            connection.rollback()
            return False
        finally:
            db._return_connection(connection)
    except Exception as e:
        #print(f"[Error] actualizar_contrasena_db: {e}")
        return False