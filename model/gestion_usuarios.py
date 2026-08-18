from fastapi import HTTPException
from threading import Lock
from datetime import datetime
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
                    INSERT INTO usuarios (nombres, apellidos, username, rol, rol_storage, rol_powerbi, rol_bi, password_user, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data_user["nombres"],
                    data_user["apellidos"],
                    data_user["username"],
                    data_user["rol"],
                    data_user["rol_storage"],
                    data_user.get("rol_powerbi", 0),
                    data_user.get("rol_bi", 0),
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
        """Obtiene todos los usuarios ordenados por ID.
        Orden columnas: [0]=id [1]=nombres [2]=apellidos [3]=username
                        [4]=rol [5]=estado [6]=rol_storage [7]=rol_powerbi [8]=rol_bi
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombres, apellidos, username, rol, estado,
                           rol_storage, rol_powerbi, COALESCE(rol_bi, 0)
                    FROM usuarios ORDER BY id ASC
                """)
                return cur.fetchall()

    def get_user_by_id(self, user_id):
        """Obtiene un usuario por ID. Retorna dict o None."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombres, apellidos, username, rol, estado,
                           rol_storage, rol_powerbi, COALESCE(rol_bi, 0)
                    FROM usuarios WHERE id = %s
                """, (user_id,))
                usuario = cur.fetchone()
                if usuario:
                    return {
                        "id":          usuario[0],
                        "nombres":     usuario[1],
                        "apellidos":   usuario[2],
                        "username":    usuario[3],
                        "rol":         usuario[4],
                        "estado":      usuario[5],
                        "rol_storage": usuario[6],
                        "rol_powerbi": usuario[7],
                        "rol_bi":      usuario[8],
                    }
                return None

    def ensure_transmitool_columns(self):
        """Asegura columnas para credenciales diarias de Transmitool."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE public.usuarios
                    ADD COLUMN IF NOT EXISTS usuario_tm VARCHAR(255)
                """)
                cur.execute("""
                    ALTER TABLE public.usuarios
                    ADD COLUMN IF NOT EXISTS clave_tm TEXT
                """)
                cur.execute("""
                    ALTER TABLE public.usuarios
                    ADD COLUMN IF NOT EXISTS tm_actualizado_en TIMESTAMP
                """)
            conn.commit()

    def get_transmitool_credentials(self, user_id):
        """Retorna credenciales TM del usuario y si debe volver a registrarlas hoy."""
        self.ensure_transmitool_columns()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT usuario_tm, clave_tm, tm_actualizado_en
                    FROM public.usuarios
                    WHERE id = %s
                """, (user_id,))
                row = cur.fetchone()

        if not row:
            raise ValueError("Usuario no encontrado")

        usuario_tm = row[0] or ""
        clave_tm = row[1] or ""
        tm_actualizado_en = row[2]

        ahora_bogota = datetime.now(colombia_tz)
        fecha_hoy = ahora_bogota.date()
        fecha_actualizacion = tm_actualizado_en.date() if tm_actualizado_en else None

        return {
            "usuario_tm": usuario_tm,
            "clave_tm": clave_tm,
            "tm_actualizado_en": tm_actualizado_en.strftime("%Y-%m-%d %H:%M:%S") if tm_actualizado_en else None,
            "requiere_registro_hoy": (
                not usuario_tm
                or not clave_tm
                or fecha_actualizacion != fecha_hoy
            ),
            "fecha_hoy": fecha_hoy.isoformat(),
        }

    def update_transmitool_credentials(self, user_id, usuario_tm, clave_tm):
        """Actualiza credenciales TM del usuario logueado y la marca de tiempo."""
        self.ensure_transmitool_columns()

        usuario_tm = (usuario_tm or "").strip()
        clave_tm = (clave_tm or "").strip()

        if not usuario_tm or not clave_tm:
            raise ValueError("Debes ingresar usuario y contraseña de Transmitool.")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.usuarios
                    SET usuario_tm = %s,
                        clave_tm = %s,
                        tm_actualizado_en = timezone('America/Bogota', now()),
                        estado_tm = CASE WHEN estado_tm = 0 THEN NULL ELSE estado_tm END
                    WHERE id = %s
                """, (usuario_tm, clave_tm, user_id))

                if cur.rowcount == 0:
                    raise ValueError("Usuario no encontrado")

            conn.commit()

        return self.get_transmitool_credentials(user_id)

    def update_user(self, user_id, data):
        """Actualiza datos de un usuario. Actualiza contraseña solo si viene en data."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    UPDATE usuarios SET nombres = %s, apellidos = %s, username = %s,
                    rol = %s, estado = %s, rol_storage = %s, rol_powerbi = %s, rol_bi = %s
                """
                params = [
                    data['nombres'], data['apellidos'], data['username'],
                    data['rol'], data['estado'], data['rol_storage'],
                    int(data.get('rol_powerbi', 0)),
                    int(data.get('rol_bi', 0)),
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

    def activate_user(self, user_id):
        """Marca un usuario como activo (estado = 1)."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE usuarios SET estado = 1 WHERE id = %s", (user_id,))
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
        """
        Inserta un nuevo rol de storage con permisos por contenedor.
        role_data = {
            "nombre_rol_storage": str,
            "permisos_por_contenedor": { "<contenedor>": {"ver":bool,"editar":bool,"descargar":bool,"eliminar":bool,"cargar":bool}, ... }
        }
        """
        permisos = role_data["permisos_por_contenedor"]
        contenedores = list(permisos.keys())
        agregados = self._agregar_permisos(permisos)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO roles_storage (nombre_rol_storage, contenedores_asignados,
                        accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id_rol_storage
                """, (
                    role_data["nombre_rol_storage"],
                    ','.join(contenedores),
                    agregados["ver"], agregados["editar"], agregados["descargar"],
                    agregados["eliminar"], agregados["cargar"],
                ))
                role_storage_id = cur.fetchone()[0]
                self._reemplazar_permisos_contenedor(cur, role_storage_id, permisos)
            conn.commit()

    @staticmethod
    def _agregar_permisos(permisos: dict) -> dict:
        """OR agregado de las acciones a través de todos los contenedores (solo para snapshot/legacy)."""
        acciones = ("ver", "editar", "descargar", "eliminar", "cargar")
        return {a: any(bool(p.get(a)) for p in permisos.values()) for a in acciones}

    @staticmethod
    def _reemplazar_permisos_contenedor(cur, role_storage_id, permisos: dict):
        """Reemplaza por completo las filas de roles_storage_contenedores para un rol."""
        cur.execute("DELETE FROM roles_storage_contenedores WHERE id_rol_storage = %s", (role_storage_id,))
        for contenedor, acciones in permisos.items():
            cur.execute("""
                INSERT INTO roles_storage_contenedores
                    (id_rol_storage, contenedor, accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_rol_storage, contenedor) DO UPDATE SET
                    accion_ver = EXCLUDED.accion_ver, accion_editar = EXCLUDED.accion_editar,
                    accion_descargar = EXCLUDED.accion_descargar, accion_eliminar = EXCLUDED.accion_eliminar,
                    accion_cargar = EXCLUDED.accion_cargar
            """, (
                role_storage_id, contenedor,
                bool(acciones.get("ver")), bool(acciones.get("editar")), bool(acciones.get("descargar")),
                bool(acciones.get("eliminar")), bool(acciones.get("cargar")),
            ))

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

    def get_resumen_acciones_por_rol(self) -> dict:
        """
        Retorna { id_rol_storage: {"total": n, "ver": x, "editar": x, "descargar": x, "eliminar": x, "cargar": x} }
        con el conteo de contenedores que tienen cada acción habilitada, para el listado de roles.
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id_rol_storage, COUNT(*),
                           COUNT(*) FILTER (WHERE accion_ver),
                           COUNT(*) FILTER (WHERE accion_editar),
                           COUNT(*) FILTER (WHERE accion_descargar),
                           COUNT(*) FILTER (WHERE accion_eliminar),
                           COUNT(*) FILTER (WHERE accion_cargar)
                    FROM roles_storage_contenedores
                    GROUP BY id_rol_storage
                """)
                resumen = {}
                for row in cur.fetchall():
                    resumen[row[0]] = {
                        "total": row[1], "ver": row[2], "editar": row[3],
                        "descargar": row[4], "eliminar": row[5], "cargar": row[6],
                    }
                return resumen

    def get_role_storage_by_id(self, role_storage_id):
        """Obtiene un rol de storage por ID, incluida la matriz de permisos por contenedor. Retorna dict o None."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id_rol_storage, nombre_rol_storage
                    FROM roles_storage WHERE id_rol_storage = %s
                """, (role_storage_id,))
                role_data = cur.fetchone()
                if not role_data:
                    return None

                cur.execute("""
                    SELECT contenedor, accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage_contenedores WHERE id_rol_storage = %s
                    ORDER BY contenedor
                """, (role_storage_id,))
                permisos_por_contenedor = {
                    row[0]: {
                        "ver": bool(row[1]), "editar": bool(row[2]), "descargar": bool(row[3]),
                        "eliminar": bool(row[4]), "cargar": bool(row[5]),
                    }
                    for row in cur.fetchall()
                }

                return {
                    "id_rol_storage": role_data[0],
                    "nombre_rol_storage": role_data[1],
                    "contenedores_asignados": list(permisos_por_contenedor.keys()),
                    "permisos_por_contenedor": permisos_por_contenedor,
                }

    def update_role_storage(self, role_storage_id, role_name, permisos_por_contenedor):
        """Actualiza nombre y permisos por contenedor de un rol de storage."""
        contenedores = list(permisos_por_contenedor.keys())
        agregados = self._agregar_permisos(permisos_por_contenedor)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE roles_storage
                    SET nombre_rol_storage = %s, contenedores_asignados = %s,
                        accion_ver = %s, accion_editar = %s,
                        accion_descargar = %s, accion_eliminar = %s, accion_cargar = %s
                    WHERE id_rol_storage = %s
                """, (role_name, ','.join(contenedores),
                      agregados["ver"], agregados["editar"], agregados["descargar"],
                      agregados["eliminar"], agregados["cargar"],
                      role_storage_id))
                self._reemplazar_permisos_contenedor(cur, role_storage_id, permisos_por_contenedor)
            conn.commit()

    def delete_role_storage(self, role_storage_id):
        """Elimina un rol de storage por ID (el CASCADE limpia roles_storage_contenedores)."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM roles_storage WHERE id_rol_storage = %s", (role_storage_id,))
            conn.commit()

    def get_contenedores_por_rol(self, role_storage_id):
        """Obtiene la lista de contenedores asignados a un rol de storage."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT contenedor FROM roles_storage_contenedores
                    WHERE id_rol_storage = %s ORDER BY contenedor
                """, (role_storage_id,))
                return [row[0] for row in cur.fetchall()]

    def get_acciones_por_rol(self, role_storage_id, container_name) -> dict:
        """Retorna dict con booleanos de acciones permitidas para el rol EN un contenedor específico."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage_contenedores
                    WHERE id_rol_storage = %s AND contenedor = %s
                """, (role_storage_id, container_name))
                result = cur.fetchone()
                if result:
                    return {
                        "ver": bool(result[0]),
                        "editar": bool(result[1]),
                        "descargar": bool(result[2]),
                        "eliminar": bool(result[3]),
                        "cargar": bool(result[4]),
                    }
        # El rol no tiene ese contenedor asignado -> sin acceso
        return {"ver": False, "editar": False, "descargar": False, "eliminar": False, "cargar": False}

    def get_acciones_por_rol_todos(self, role_storage_id) -> dict:
        """Retorna { "<contenedor>": {ver, editar, descargar, eliminar, cargar} } para todos los contenedores del rol."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contenedor, accion_ver, accion_editar, accion_descargar, accion_eliminar, accion_cargar
                    FROM roles_storage_contenedores WHERE id_rol_storage = %s
                """, (role_storage_id,))
                return {
                    row[0]: {
                        "ver": bool(row[1]), "editar": bool(row[2]), "descargar": bool(row[3]),
                        "eliminar": bool(row[4]), "cargar": bool(row[5]),
                    }
                    for row in cur.fetchall()
                }

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

# =====================================================================
# CLASE: DesactivarRetirados
# =====================================================================
class DesactivarRetirados:
    """
    Lee un archivo Excel con cédulas de personal desvinculado y pone
    estado = 0 en la tabla public.usuarios para cada cédula encontrada.

    Estructura esperada del Excel:
        - Columna: CEDULA  (numérica o texto, una cédula por fila)
    """

    def procesar(self, file_path: str) -> dict:
        """
        Procesa el archivo y desactiva los usuarios encontrados.

        Retorna un dict con:
            total_cedulas   : cuántas cédulas venían en el archivo
            desactivados    : lista de usernames (cédulas) que se pusieron en estado 0
            no_encontrados  : lista de cédulas del archivo que no existen en usuarios
        """
        # ── Leer Excel ────────────────────────────────────────────────
        df = pd.read_excel(file_path)

        # Buscar la columna de cédulas (acepta "CEDULA", "cedula", "Cedula", etc.)
        col = next(
            (c for c in df.columns if c.strip().lower() == "cedula"),
            None
        )
        if col is None:
            raise ValueError(
                "El archivo Excel debe tener una columna llamada 'CEDULA'. "
                f"Columnas encontradas: {list(df.columns)}"
            )

        # Convertir a entero y eliminar filas vacías / duplicadas
        # username en la tabla usuarios es INTEGER, por eso se castea aquí
        cedulas_raw = (
            df[col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .unique()
            .tolist()
        )

        # Descartar valores que no sean numéricos enteros
        cedulas = []
        for c in cedulas_raw:
            try:
                cedulas.append(int(c))
            except ValueError:
                pass  # ignora filas con texto no numérico

        if not cedulas:
            raise ValueError("El archivo no contiene cédulas numéricas válidas.")

        # ── Consultar cuáles existen en usuarios (activos) ────────────
        # Se usa BIGINT para cubrir cédulas de 10 dígitos que superan
        # el límite de INTEGER (~2.1 mil millones). PostgreSQL puede
        # comparar bigint con integer sin problema.
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT username::text
                    FROM   public.usuarios
                    WHERE  username::bigint = ANY(%s::bigint[])
                      AND  estado = 1
                    """,
                    (cedulas,)
                )
                encontrados = [row[0] for row in cur.fetchall()]

        cedulas_str    = [str(c) for c in cedulas]
        no_encontrados = [c for c in cedulas_str if c not in encontrados]

        # ── Desactivar los que sí están ───────────────────────────────
        if encontrados:
            encontrados_int = [int(c) for c in encontrados]
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.usuarios
                        SET    estado = 0
                        WHERE  username::bigint = ANY(%s::bigint[])
                        """,
                        (encontrados_int,)
                    )
                conn.commit()

        return {
            "total_cedulas":  len(cedulas),
            "desactivados":   encontrados,
            "no_encontrados": no_encontrados,
        }
