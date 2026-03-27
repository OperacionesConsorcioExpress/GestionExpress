from fastapi import HTTPException
from threading import Lock
from bs4 import BeautifulSoup
import pandas as pd
import pytz
from werkzeug.security import generate_password_hash
from database.database_manager import get_db_connection

# Zona horaria de Colombia (se mantiene para compatibilidad)
colombia_tz = pytz.timezone('America/Bogota')

# =====================================================================
# CLASE: HandleDB
# =====================================================================
class HandleDB:
    """
    Manejador de operaciones sobre usuarios y roles.
    Patrón Singleton mantenido para compatibilidad
    Las conexiones ahora vienen del pool centralizado (database_manager).
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls)
            return cls._instance

    def get_all(self):
        """Obtiene todos los registros de usuarios."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM usuarios")
                return cur.fetchall()

    def get_only(self, username):
        """Obtiene un usuario por username."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM usuarios WHERE username = %s", (username,))
                return cur.fetchone()

    def insert(self, data_user):
        """Inserta un nuevo usuario en la base de datos."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usuarios (nombres, apellidos, username, rol, rol_storage, rol_powerbi, password_user, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data_user["nombres"],
                    data_user["apellidos"],
                    data_user["username"],
                    data_user["rol"],
                    data_user["rol_storage"],
                    data_user.get("rol_powerbi", 0),
                    data_user["password_user"],
                    1  # Siempre se insertará como 'activo' con estado 1
                ))
            conn.commit()

    def insert_role(self, role_data):
        """Inserta un nuevo rol en la tabla 'roles'."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO roles (nombre_rol, pantallas_asignadas)
                    VALUES (%s, %s)
                """, (
                    role_data["nombre_rol"],
                    role_data["pantallas_asignadas"]
                ))
            conn.commit()

    def get_all_roles(self):
        """Obtiene todos los roles del sistema."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_rol, nombre_rol, pantallas_asignadas FROM roles ORDER BY id_rol ASC")
                return cur.fetchall()

    def get_pantallas_from_layout(self, layout_path):
        """Obtiene las pantallas del menú lateral desde el HTML del layout."""
        with open(layout_path, 'r', encoding='utf-8') as f:
            layout_html = f.read()
        soup = BeautifulSoup(layout_html, 'html.parser')
        pantallas = [link.text.strip() for link in soup.select(".sidebar .nav-link")]
        return pantallas

    def get_role_by_id(self, role_id):
        """Obtiene un rol por ID. Retorna None si pantallas_asignadas está vacío."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_rol, nombre_rol, pantallas_asignadas FROM roles WHERE id_rol = %s", (role_id,))
                rol_data = cur.fetchone()
                if rol_data and rol_data[2]:  # Validar que pantallas_asignadas no esté vacío
                    return rol_data
                return None

    def update_role(self, role_id, role_name, permissions):
        """Actualiza nombre y permisos de un rol."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE roles SET nombre_rol = %s, pantallas_asignadas = %s WHERE id_rol = %s
                """, (role_name, permissions, role_id))
            conn.commit()

    def delete_role(self, role_id):
        """Elimina un rol por ID."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM roles WHERE id_rol = %s", (role_id,))
            conn.commit()

    def get_pantallas_by_role(self, role_id):
        """Consulta las pantallas asignadas a un rol. Retorna lista."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pantallas_asignadas FROM roles WHERE id_rol = %s", (role_id,))
                result = cur.fetchone()

        if not result:
            return []
        # Robusto ante RealDictRow o tupla
        pantallas = result["pantallas_asignadas"] if isinstance(result, dict) else result[0]
        if not pantallas:
            return []
        return [p.strip() for p in pantallas.split(",") if p.strip()]

    def get_all_users(self):
        """Obtiene todos los usuarios ordenados por ID."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombres, apellidos, username, rol, estado, rol_storage, rol_powerbi
                    FROM usuarios ORDER BY id ASC
                """)
                return cur.fetchall()

    def get_user_by_id(self, user_id):
        """Obtiene un usuario por ID. Retorna dict o None."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombres, apellidos, username, rol, estado, rol_storage, rol_powerbi
                    FROM usuarios WHERE id = %s
                """, (user_id,))
                usuario = cur.fetchone()
                if usuario:
                    return {
                        "id": usuario[0],
                        "nombres": usuario[1],
                        "apellidos": usuario[2],
                        "username": usuario[3],
                        "rol": usuario[4],
                        "estado": usuario[5],
                        "rol_storage": usuario[6],
                        "rol_powerbi": usuario[7]
                    }
                return None

    def update_user(self, user_id, data):
        """Actualiza datos de un usuario. Actualiza contraseña solo si viene en data."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    UPDATE usuarios SET nombres = %s, apellidos = %s, username = %s,
                    rol = %s, estado = %s, rol_storage = %s, rol_powerbi = %s
                """
                params = [
                    data['nombres'], data['apellidos'], data['username'],
                    data['rol'], data['estado'], data['rol_storage'],
                    int(data.get('rol_powerbi', 0))
                ]

                # Solo agrega la contraseña si está presente en los datos
                if "password_user" in data and data["password_user"]:
                    query += ", password_user = %s"
                    params.append(data["password_user"])

                query += " WHERE id = %s"
                params.append(user_id)

                cur.execute(query, params)
            conn.commit()

    def inactivate_user(self, user_id):
        """Marca un usuario como inactivo (estado = 0)."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE usuarios SET estado = 0 WHERE id = %s", (user_id,))
            conn.commit()

    def fetch_one(self, query, values=None):
        """Ejecuta una query y retorna una sola fila. Usado para licencias Power BI."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)
                return cur.fetchone()

    def fetch_all(self, query, params=None):
        """Ejecuta una query y retorna todas las filas."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

# =====================================================================
# CLASE: CargueLicenciasBI
# =====================================================================
class CargueLicenciasBI:
    """
    Carga licencias Power BI desde un archivo Excel a la base de datos.
    Recibe una conexión externa para mantener la transacción bajo control
    del llamador (route_powerbi.py).
    """
    def __init__(self, db_conn):
        self.conn = db_conn
        self.cursor = self.conn.cursor()

    def cargar_licencias_excel(self, file_path):
        try:
            # Leer el archivo Excel
            df = pd.read_excel(file_path)

            # Validar si las columnas necesarias están presentes
            columnas_requeridas = {'cedula', 'nombre', 'correo_corporativo', 'grupo', 'licencia_bi', 'contraseña_licencia'}
            if not columnas_requeridas.issubset(df.columns):
                raise ValueError(
                    "El archivo Excel debe contener las columnas: "
                    "'cedula', 'nombre', 'correo_corporativo', 'grupo', 'licencia_bi' y 'contraseña_licencia'."
                )

            # Insertar o actualizar los datos en la tabla licencias_bi
            for _, row in df.iterrows():
                self.cursor.execute('''
                    INSERT INTO licencias_bi (cedula, nombre, correo_corporativo, grupo, licencia_bi, contraseña_licencia)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cedula) DO UPDATE
                    SET nombre = EXCLUDED.nombre, correo_corporativo = EXCLUDED.correo_corporativo,
                        grupo = EXCLUDED.grupo, licencia_bi = EXCLUDED.licencia_bi,
                        contraseña_licencia = EXCLUDED.contraseña_licencia
                ''', (
                    row['cedula'], row['nombre'], row['correo_corporativo'], row['grupo'],
                    row['licencia_bi'], row['contraseña_licencia']
                ))

            self.conn.commit()
            return {"message": "Licencias cargadas exitosamente."}

        except Exception as e:
            self.conn.rollback()
            raise HTTPException(status_code=400, detail=f"Error al cargar el archivo Excel: {str(e)}")

# =====================================================================
# CLASE: Cargue_Roles_Blob_Storage
# =====================================================================
class Cargue_Roles_Blob_Storage:
    """
    CRUD de roles de Blob Storage (contenedores Azure asignados por rol).
    Patrón Singleton mantenido para compatibilidad.
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls)
            return cls._instance

    def insert_roles_storage(self, role_data):
        """Inserta un nuevo rol de storage."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO roles_storage (nombre_rol_storage, contenedores_asignados,
                        accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    role_data["nombre_rol_storage"],
                    ','.join(role_data["contenedores_asignados"]),
                    role_data.get("accion_ver", True),
                    role_data.get("accion_editar", False),
                    role_data.get("accion_descargar", True),
                    role_data.get("accion_eliminar", False),
                    role_data.get("accion_cargar", False),
                ))
            conn.commit()

    def get_all_roles_storage(self):
        """Obtiene todos los roles de storage ordenados por ID."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id_rol_storage, nombre_rol_storage, contenedores_asignados,
                           accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage ORDER BY id_rol_storage ASC
                """)
                return cur.fetchall()

    def get_role_storage_by_id(self, role_storage_id):
        """Obtiene un rol de storage por ID. Retorna dict o None."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id_rol_storage, nombre_rol_storage, contenedores_asignados,
                           accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage WHERE id_rol_storage = %s
                """, (role_storage_id,))
                role_data = cur.fetchone()
                if role_data:
                    return {
                        "id_rol_storage": role_data[0],
                        "nombre_rol_storage": role_data[1],
                        "contenedores_asignados": role_data[2].split(','),
                        "accion_ver": bool(role_data[3]),
                        "accion_editar": bool(role_data[4]),
                        "accion_descargar": bool(role_data[5]),
                        "accion_eliminar": bool(role_data[6]),
                        "accion_cargar": bool(role_data[7]),
                    }
                return None

    def update_role_storage(self, role_storage_id, role_name, contenedores_asignados,
                            accion_ver=True, accion_editar=False,
                            accion_descargar=True, accion_eliminar=False, accion_cargar=False):
        """Actualiza nombre, contenedores y acciones de un rol de storage."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE roles_storage
                    SET nombre_rol_storage = %s, contenedores_asignados = %s,
                        accion_ver = %s, accion_editar = %s,
                        accion_descargar = %s, accion_eliminar = %s, accion_cargar = %s
                    WHERE id_rol_storage = %s
                """, (role_name, ','.join(contenedores_asignados),
                      accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar,
                      role_storage_id))
            conn.commit()

    def delete_role_storage(self, role_storage_id):
        """Elimina un rol de storage por ID."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM roles_storage WHERE id_rol_storage = %s", (role_storage_id,))
            conn.commit()

    def get_contenedores_por_rol(self, role_storage_id):
        """Obtiene la lista de contenedores asignados a un rol de storage."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contenedores_asignados FROM roles_storage WHERE id_rol_storage = %s
                """, (role_storage_id,))
                result = cur.fetchone()
                if result and result[0]:
                    return result[0].split(',')
                return []

    def get_acciones_por_rol(self, role_storage_id) -> dict:
        """Retorna dict con booleanos de acciones permitidas para el rol."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage WHERE id_rol_storage = %s
                """, (role_storage_id,))
                result = cur.fetchone()
                if result:
                    return {
                        "ver": bool(result[0]),
                        "editar": bool(result[1]),
                        "descargar": bool(result[2]),
                        "eliminar": bool(result[3]),
                        "cargar": bool(result[4]),
                    }
        # Default seguro: solo lectura si no hay rol configurado
        return {"ver": True, "editar": False, "descargar": False, "eliminar": False, "cargar": False}

# =====================================================================
# CLASE: GestionUsuarios
# =====================================================================
class GestionUsuarios():
    """
    Lógica de negocio para crear usuarios.
    Valida duplicados, asigna ID y encripta contraseña.
    """
    data_user = {}

    def __init__(self, data_user):
        self.db = HandleDB()
        self.data_user = data_user

    def create_user(self):
        """Crea un usuario nuevo si el username no existe."""
        existing_user = self.db.get_only(self.data_user["username"])
        if existing_user:
            return {"success": False, "message": "El usuario ya está creado en la base de datos"}

        self._add_id()
        self._passw_encrypt()
        self.db.insert(self.data_user)
        return {"success": True, "message": "Usuario creado exitosamente"}

    def _add_id(self):
        """Asigna el siguiente ID disponible."""
        user = self.db.get_all()
        if user:
            id_user = int(user[-1][0])
            self.data_user["id"] = str(id_user + 1)
        else:
            self.data_user["id"] = "1"

    def _passw_encrypt(self):
        """Encripta la contraseña con pbkdf2:sha256."""
        self.data_user["password_user"] = generate_password_hash(
            self.data_user["password_user"], "pbkdf2:sha256:30", 30
        )