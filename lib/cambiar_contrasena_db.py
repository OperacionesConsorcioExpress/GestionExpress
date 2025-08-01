from model.gestionar_db import HandleDB

def actualizar_contrasena_db(usuario_id, nueva_password_hash):
    try:
        #print("[DB] Intentando actualizar contraseña para id:", usuario_id)
        db = HandleDB()
        query = "UPDATE usuarios SET password_user = %s WHERE id = %s"
        db._cur.execute(query, (nueva_password_hash, usuario_id))
        db._con.commit()
        #print("[DB] Rowcount:", db._cur.rowcount)
        return db._cur.rowcount == 1
    except Exception as e:
        #print(f"[Error] actualizar_contrasena_db: {e}")
        return False