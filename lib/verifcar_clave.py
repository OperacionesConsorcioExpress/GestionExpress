from werkzeug.security import check_password_hash
from database.database_manager import get_db_connection

def check_user(username, passw):
    """
    Verifica credenciales. Robusto ante RealDictRow o tupla estándar.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nombres, apellidos, username, password_user, estado
                FROM usuarios
                WHERE username = %s
            """, (username,))
            row = cur.fetchone()

    if not row:
        return False, None, None

    if isinstance(row, dict):
        estado        = row.get("estado")
        password_hash = row.get("password_user")
        nombres       = row.get("nombres")
        apellidos     = row.get("apellidos")
    else:
        estado        = row[5]
        password_hash = row[4]
        nombres       = row[1]
        apellidos     = row[2]

    if estado in (0, "0", False, None):
        return False, None, None

    if password_hash and check_password_hash(password_hash, passw):
        return True, nombres, apellidos

    return False, None, None