import os, re
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "1206-operaciones-centro-de-control-preoperativos"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

# Función para normalizar nombres para rutas en blob
def _normalizar(nombre: str) -> str:
    """
    Normaliza nombres para rutas en blob:
    - quita tildes/caracteres no permitidos
    - reemplaza espacios por guiones
    - mantiene solo [A-Za-z0-9-_]
    """
    s = (nombre or "").strip()
    s = s.replace(" ", "-")
    # quita todo lo que no sea alfanumérico, guión o underscore
    s = re.sub(r"[^A-Za-z0-9\-_]", "", s)
    # evita dobles guiones
    s = re.sub(r"-{2,}", "-", s)
    return s.upper()  # placas/mantenimientos en mayúscula por consistencia

# Función para patrones LIKE en SQL
def _like(val: Optional[str]) -> Optional[str]:
    return f"%{val.strip()}%" if val else None

class GestionPreoperacionalMotos:
    """Gestión de parámetros preoperacionales de motos (mantenimientos y km óptimo)."""

    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                DATABASE_PATH,
                options='-c timezone=America/Bogota'
            )
            self.cursor = self.connection.cursor()

            # Fijar TZ por si el parámetro no se respeta
            with self.connection.cursor() as c:
                c.execute("SET TIME ZONE 'America/Bogota';")
            self.connection.commit()

        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e

        # ---------- Azure Blob ----------
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en .env")

        self._blob_service = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        self._container = self._blob_service.get_container_client(CONTAINER_NAME)

        # Extraer AccountName / AccountKey para SAS
        self._account_name, self._account_key = self._parse_conn_str(
            AZURE_STORAGE_CONNECTION_STRING
        )

    def _cursor_dict(self):
        return self.connection.cursor(cursor_factory=RealDictCursor)

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None):
            self.connection.close()

# -------- CRUD PARAMETRIZACIÓN KM MANTENIMIENTOS ---------
    def _normalizar_mantenimiento(self, texto: str) -> str:
        if texto is None:
            return ""
        return texto.strip().upper()
    
    def listar_parametros(self, incluir_inactivos: bool = True):
        """Lista parámetros registrados. Si incluir_inactivos=False, solo estado=1."""
        query = """
            SELECT id, mantenimiento, km_optimo, estado, creado_en, actualizado_en
            FROM public.checklist_param_motos
        """
        if not incluir_inactivos:
            query += " WHERE estado = 1"
        query += " ORDER BY mantenimiento ASC;"

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def crear_parametro(self, mantenimiento: str, km_optimo: int | None, estado: int = 1):
        """
        Crea o actualiza (upsert) un parámetro por mantenimiento (UNIQUE).
        Si ya existe, actualiza km_optimo y estado.
        """
        if estado not in (0, 1):
            return {"error": "Estado debe ser 0 o 1."}

        mantenimiento_norm = self._normalizar_mantenimiento(mantenimiento)
        if not mantenimiento_norm:
            return {"error": "El nombre de mantenimiento es obligatorio."}

        query = """
            INSERT INTO public.checklist_param_motos (mantenimiento, km_optimo, estado)
            VALUES (%s, %s, %s)
            ON CONFLICT (mantenimiento) 
            DO UPDATE SET 
                km_optimo = EXCLUDED.km_optimo,
                estado = EXCLUDED.estado,
                actualizado_en = NOW()
            RETURNING id, mantenimiento, km_optimo, estado, creado_en, actualizado_en;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query, (mantenimiento_norm, km_optimo, estado))
                fila = cursor.fetchone()
                self.connection.commit()
                return {"message": "Parámetro guardado", "data": fila}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al crear/actualizar parámetro: {str(e)}"}

    def obtener_parametro(self, id_param: int):
        """Obtiene un parámetro por id."""
        query = """
            SELECT id, mantenimiento, km_optimo, estado, creado_en, actualizado_en
            FROM public.checklist_param_motos
            WHERE id = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (id_param,))
            return cursor.fetchone()

    def actualizar_parametro(self, id_param: int, mantenimiento: str, km_optimo: int | None, estado: int):
        """Actualiza mantenimiento, km_optimo y estado por id."""
        if estado not in (0, 1):
            return {"error": "Estado debe ser 0 o 1."}

        mantenimiento_norm = self._normalizar_mantenimiento(mantenimiento)
        if not mantenimiento_norm:
            return {"error": "El nombre de mantenimiento es obligatorio."}

        query = """
            UPDATE public.checklist_param_motos
            SET mantenimiento = %s,
                km_optimo = %s,
                estado = %s,
                actualizado_en = NOW()
            WHERE id = %s
            RETURNING id, mantenimiento, km_optimo, estado, creado_en, actualizado_en;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query, (mantenimiento_norm, km_optimo, estado, id_param))
                fila = cursor.fetchone()
                self.connection.commit()
                if not fila:
                    return {"error": "No existe el parámetro especificado."}
                return {"message": "Parámetro actualizado", "data": fila}
            except psycopg2.Error as e:
                self.connection.rollback()
                # 23505 = unique_violation
                if getattr(e, "pgcode", None) == "23505":
                    return {"error": "Ya existe un mantenimiento con ese nombre."}
                return {"error": f"Error al actualizar parámetro: {str(e)}"}

    def cambiar_estado(self, id_param: int, estado: int):
        """Activa/Inactiva un parámetro por id."""
        if estado not in (0, 1):
            return {"error": "Estado debe ser 0 o 1."}

        query = """
            UPDATE public.checklist_param_motos
            SET estado = %s,
                actualizado_en = NOW()
            WHERE id = %s
            RETURNING id, mantenimiento, km_optimo, estado, creado_en, actualizado_en;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query, (estado, id_param))
                fila = cursor.fetchone()
                self.connection.commit()
                if not fila:
                    return {"error": "No existe el parámetro especificado."}
                return {"message": "Estado actualizado", "data": fila}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al cambiar estado: {str(e)}"}
            
# -------- CRUD REGISTRO PREOPERATIVOS ---------
    def obtener_id_vehiculo_por_placa(self, placa: str) -> Optional[int]:
        q = "SELECT id FROM public.vehiculos WHERE placa = %s;"
        with self.connection.cursor() as c:
            c.execute(q, (placa,))
            row = c.fetchone()
            return row[0] if row else None

    def crear_registro_preop(
        self,
        id_vehiculo: int,
        fecha_mantenimiento: str,  # "YYYY-MM-DD"
        mantenimiento: str,
        observacion: Optional[str],
        usuario_registro: str
    ) -> Dict:
        if not id_vehiculo:
            return {"error": "id_vehiculo es obligatorio."}
        if not fecha_mantenimiento:
            return {"error": "fecha_mantenimiento es obligatoria."}
        if not mantenimiento:
            return {"error": "mantenimiento es obligatorio."}
        if not usuario_registro:
            return {"error": "usuario_registro es obligatorio."}

        # ← sello de tiempo en zona Bogotá
        fecha_guardado_bog = now_bogota()

        q = """
            INSERT INTO public.checklist_preop_registro
                (id_vehiculo, fecha_mantenimiento, mantenimiento, observacion, usuario_registro, fecha_guardado)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id_preop, id_vehiculo, fecha_mantenimiento, mantenimiento, observacion, usuario_registro, fecha_guardado;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as c:
            try:
                c.execute(q, (
                    id_vehiculo,
                    fecha_mantenimiento,
                    mantenimiento,
                    observacion,
                    usuario_registro,
                    fecha_guardado_bog,      # <-- Bogotá-aware
                ))
                row = c.fetchone()
                self.connection.commit()
                return {"message": "Registro preoperacional creado", "data": row}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"error": f"Error al crear registro: {str(e)}"}

    def obtener_resumen_preop(self, id_preop: int) -> Optional[Dict]:
        q = """
            SELECT r.id_preop, r.mantenimiento, r.fecha_mantenimiento, v.placa
            FROM public.checklist_preop_registro r
            JOIN public.vehiculos v ON v.id = r.id_vehiculo
            WHERE r.id_preop = %s;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(q, (id_preop,))
            return c.fetchone()

    def guardar_adjunto_bd(
        self,
        id_preop: int,
        nombre_archivo: str,
        ruta_blob: str,
        url_publica: Optional[str],
        content_type: Optional[str],
        tamano_bytes: Optional[int]
    ) -> Dict:
        # ← sello de tiempo Bogotá para los adjuntos
        subido_en_bog = now_bogota()

        q = """
            INSERT INTO public.checklist_preop_adjunto
                (id_preop, nombre_archivo, ruta_blob, url_publica, content_type, tamano_bytes, subido_en)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id_adjunto;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as c:
            try:
                c.execute(q, (
                    id_preop,
                    nombre_archivo,
                    ruta_blob,
                    url_publica,
                    content_type,
                    tamano_bytes,
                    subido_en_bog,            # <-- Bogotá-aware
                ))
                row = c.fetchone()
                self.connection.commit()
                return {"ok": True, "id_adjunto": row["id_adjunto"]}
            except psycopg2.Error as e:
                self.connection.rollback()
                return {"ok": False, "error": f"Error al guardar metadatos adjunto: {str(e)}"}

    # ---------- CARGA Y CREACIÓN DE ESTRUCTURA EN BLOB AZURE ----------
    def _get_container_client(self):
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en .env")
        bsc = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        return bsc.get_container_client(CONTAINER_NAME)

    def crear_estructura_blob_preop(self, placa: str, mantenimiento: str, fechas: List[str]):
        """
        Simula 'carpetas' creando un marcador vacío en:
        <CONTAINER>/<PLACA>/<MANTENIMIENTO>/<AAAA>/<MM>/estructura.txt
        """
        container = self._get_container_client()

        placa_norm = _normalizar(placa)
        mant_norm  = _normalizar(mantenimiento)

        for f in fechas:
            dt = datetime.strptime(f, "%Y-%m-%d")
            anio, mes = dt.year, f"{dt.month:02d}"

            ruta_carpeta = f"{placa_norm}/{mant_norm}/{anio}/{mes}/"
            blob = container.get_blob_client(f"{ruta_carpeta}estructura.txt")
            # si no existe, lo creamos (Blob Storage no tiene carpetas reales)
            try:
                if not blob.exists():
                    blob.upload_blob(b"", overwrite=True)
            except Exception:
                # Si el contenedor es privado y exists() falla por permisos, igualmente subimos
                blob.upload_blob(b"", overwrite=True)

    def subir_archivo_blob(
        self,
        placa: str,
        mantenimiento: str,
        anio: str,
        mes: str,
        fecha_mantenimiento: str,
        file_stream: bytes,
        filename: str
    ) -> Dict:
        """
        Guarda archivo en:
        <CONTAINER>/<PLACA>/<MANTENIMIENTO>/<AAAA>/<MM>/<FECHA>_<FILENAME>
        """
        container = self._get_container_client()

        placa_norm = _normalizar(placa)
        mant_norm  = _normalizar(mantenimiento)

        # nombre final
        fecha_norm = fecha_mantenimiento.replace("/", "-")
        nombre_final = f"{fecha_norm}_{filename}"

        # ruta final
        ruta = f"{placa_norm}/{mant_norm}/{anio}/{mes}/{nombre_final}"

        blob = container.get_blob_client(ruta)
        blob.upload_blob(file_stream, overwrite=True)

        url_publica = None
        try:
            url_publica = blob.url  # si el contenedor es público
        except Exception:
            pass

        return {"ruta_blob": ruta, "url_publica": url_publica}

#---------- VISUALIZACIÓN DE REGISTROS PREOPERATIVOS Y ADJUNTOS ----------
# ---------- Consolidado para la grilla principal ----------
    def listar_consolidado_vehiculos(
        self,
        placa: Optional[str] = None,
        tipo: Optional[str] = None,
        marca: Optional[str] = None,
        linea: Optional[str] = None,
        modelo: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retorna por vehículo:
        - ultimo_km_odometro (del checklist general más reciente por id_vehiculo)
        - fecha_ultimo_mantenimiento = MAX(fecha_mantenimiento) en checklist_preop_registro
        - tipo_vehiculo: nombre desde public.checklist_tipo_vehiculo
        - filtrado por flota (roles_flota_asis) si se informa user_id.
        - SOLO Motos (id_tipo_vehiculo = 2).
        """
        sql = """
        WITH veh AS (
            SELECT
                v.id AS id_vehiculo,
                v.placa,
                v.id_proceso,                  -- <- para filtrar por flota del usuario
                tv.id_tipo_vehiculo,               -- <- id del tipo
                tv.nombre AS tipo_vehiculo,          -- <- nombre del tipo
                v.marca,
                v.linea,
                v.modelo,
                CASE WHEN v.estado = 1 THEN 'Activo' ELSE 'Inactivo' END AS vinculacion
            FROM public.vehiculos v
            LEFT JOIN public.checklist_tipo_vehiculo tv
                ON tv.id_tipo_vehiculo = v.id_tipo_vehiculo
        ),
        ult_km AS (
            SELECT DISTINCT ON (c.id_vehiculo)
                c.id_vehiculo,
                c.km_odometro,
                c.fecha_hora_registro
            FROM public.checklist_registro c
            WHERE c.km_odometro IS NOT NULL
            ORDER BY c.id_vehiculo, c.fecha_hora_registro DESC
        ),
        ult_mtto AS (
            SELECT id_vehiculo, MAX(fecha_mantenimiento) AS fecha_ultimo_mantenimiento
            FROM public.checklist_preop_registro
            GROUP BY id_vehiculo
        )
        SELECT veh.placa, veh.tipo_vehiculo, veh.marca, veh.linea, veh.modelo, veh.vinculacion,
            uk.km_odometro AS ultimo_km_odometro,
            um.fecha_ultimo_mantenimiento
        FROM veh
        LEFT JOIN ult_km  uk ON uk.id_vehiculo = veh.id_vehiculo
        LEFT JOIN ult_mtto um ON um.id_vehiculo = veh.id_vehiculo
        WHERE veh.id_tipo_vehiculo = 2           -- <- SOLO MOTOS
        AND (%(placa)s  IS NULL OR veh.placa ILIKE %(placa)s)
        AND (%(tipo)s   IS NULL OR veh.tipo_vehiculo ILIKE %(tipo)s)
        AND (%(marca)s  IS NULL OR veh.marca ILIKE %(marca)s)
        AND (%(linea)s  IS NULL OR veh.linea ILIKE %(linea)s)
        AND (%(modelo)s IS NULL OR CAST(veh.modelo AS TEXT) ILIKE %(modelo)s)
        AND (%(user_id)s IS NULL OR EXISTS (
            SELECT 1 FROM public.roles_flota_asis rfa
            WHERE rfa.user_id = %(user_id)s
                AND rfa.id_proceso = veh.id_proceso
        ))
        ORDER BY veh.placa ASC;
        """
        params = {
            "placa": f"%{placa.strip()}%" if placa else None,
            "tipo": f"%{tipo.strip()}%" if tipo else None,
            "marca": f"%{marca.strip()}%" if marca else None,
            "linea": f"%{linea.strip()}%" if linea else None,
            "modelo": f"%{modelo.strip()}%" if modelo else None,
            "user_id": user_id, 
        }

        with self._cursor_dict() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            fecha_mtto = r["fecha_ultimo_mantenimiento"]
            out.append({
                "placa": r["placa"],
                "tipo_vehiculo": r["tipo_vehiculo"] or "",
                "marca": r["marca"],
                "linea": r["linea"],
                "modelo": r["modelo"],
                "vinculacion": r["vinculacion"],  # Activo/Inactivo calculado arriba
                "ultimo_km_odometro": r["ultimo_km_odometro"],
                "fecha_ultimo_mantenimiento": fecha_mtto.isoformat() if fecha_mtto else None,
            })
        return out

    # ---------- Helpers vehículo ----------
    def _obtener_id_vehiculo_por_placa(self, placa: str) -> Optional[int]:
        with self.connection.cursor() as cur:
            cur.execute("SELECT id FROM public.vehiculos WHERE placa = %s", (placa,))
            row = cur.fetchone()
            return int(row[0]) if row else None

    # ---------- Modal: grupos (mantenimientos) ----------
    def listar_grupos_por_vehiculo(self, placa: str) -> List[str]:
        idv = self._obtener_id_vehiculo_por_placa(placa)
        if not idv:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT mantenimiento
                FROM public.checklist_preop_registro
                WHERE id_vehiculo = %s
                ORDER BY mantenimiento ASC
            """, (idv,))
            return [r[0] for r in cur.fetchall()]

    # ---------- Modal: filas por grupo ----------
    def listar_registros_por_grupo(self, placa: str, mantenimiento: str) -> List[Dict[str, Any]]:
        idv = self._obtener_id_vehiculo_por_placa(placa)
        if not idv:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

        with self._cursor_dict() as cur:
            cur.execute("""
                SELECT r.id_preop,
                    r.fecha_mantenimiento,
                    r.observacion,
                    r.usuario_registro,
                    r.fecha_guardado,
                    COALESCE(a.cnt, 0) AS adjuntos_count
                FROM public.checklist_preop_registro r
                LEFT JOIN (
                    SELECT id_preop, COUNT(*) AS cnt
                    FROM public.checklist_preop_adjunto
                    GROUP BY id_preop
                ) a ON a.id_preop = r.id_preop
                WHERE r.id_vehiculo = %s
                AND r.mantenimiento = %s
                ORDER BY r.fecha_mantenimiento DESC, r.id_preop DESC
            """, (idv, mantenimiento))
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            fg = r["fecha_guardado"]
            fg_str = None
            if isinstance(fg, datetime):
                if fg.tzinfo is None:
                    fg = fg.replace(tzinfo=timezone.utc)
                fg = fg.astimezone(TIMEZONE_BOGOTA)
                fg_str = fg.strftime("%Y-%m-%d %H:%M")

            out.append({
                "id_preop": r["id_preop"],
                "fecha_mantenimiento": r["fecha_mantenimiento"].strftime("%Y-%m-%d") if r["fecha_mantenimiento"] else None,
                "observacion": r["observacion"],
                "usuario_registra": r["usuario_registro"],
                "fecha_guardado": fg_str,
                "adjuntos_count": r["adjuntos_count"],
            })
        return out

    def listar_registros_por_grupo_enriquecido(self, placa: str, mantenimiento: str) -> List[Dict[str, Any]]:
        """
        Lee public.vw_preop_historial_motos para placa + mantenimiento.
        Devuelve columnas listas para la grilla del modal (orden requerido).

        Regla de presentación de 'odometro':
        - Primero intenta mostrar el odómetro de corte (día anterior) = odometro_corte.
        - Si no existe (primer registro sin histórico), usa km_corte (fallback calculado en la vista).
        """
        idv = self._obtener_id_vehiculo_por_placa(placa)
        if not idv:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

        mant_norm = (mantenimiento or "").strip().upper()

        sql = """
            SELECT
                id_preop,
                fecha_mantenimiento,
                odometro_corte,              -- puede ser NULL en el primer registro
                km_corte,                    -- COALESCE(odometro_corte, odometro_final_dia)
                diferencia_optimo,
                porcentaje_uso,
                prox_mtto_km,
                observacion,
                usuario_registro        AS usuario_registra,
                fecha_guardado,
                adjuntos_count
            FROM public.vw_preop_historial_motos
            WHERE id_vehiculo = %s
            AND mantenimiento = %s
            ORDER BY fecha_mantenimiento DESC, id_preop DESC;
        """
        with self._cursor_dict() as cur:
            cur.execute(sql, (idv, mant_norm))
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            fg = r.get("fecha_guardado")
            fg_str = None
            if isinstance(fg, datetime):
                if fg.tzinfo is None:
                    fg = fg.replace(tzinfo=timezone.utc)
                fg = fg.astimezone(TIMEZONE_BOGOTA)
                fg_str = fg.strftime("%Y-%m-%d %H:%M")
            else:
                fg_str = fg

            # Fallback: si no hay odómetro de corte, usar km_corte
            odometro_val = r.get("odometro_corte")
            if odometro_val is None:
                odometro_val = r.get("km_corte")

            out.append({
                "id_preop": r["id_preop"],
                "fecha_mantenimiento": r["fecha_mantenimiento"].strftime("%Y-%m-%d") if r["fecha_mantenimiento"] else None,
                "odometro": odometro_val,  # 👈 Ahora nunca se queda vacío si existe lectura del mismo día
                "diferencia_optimo": r["diferencia_optimo"],
                "porcentaje_uso": r["porcentaje_uso"],
                "prox_mtto_km": r["prox_mtto_km"],
                "observacion": r["observacion"],
                "usuario_registra": r["usuario_registra"],
                "fecha_guardado": fg_str,
                "adjuntos_count": r["adjuntos_count"],
            })
        return out

    # ---------- Modal: adjuntos por registro ----------
    def listar_adjuntos_de_registro(self, id_preop: int, minutos_url: int = 15) -> List[Dict[str, Any]]:
        with self._cursor_dict() as cur:
            cur.execute("""
                SELECT id_adjunto, nombre_archivo, ruta_blob, url_publica, content_type, tamano_bytes
                FROM public.checklist_preop_adjunto
                WHERE id_preop = %s
                ORDER BY id_adjunto ASC
            """, (id_preop,))
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            ida = r["id_adjunto"]
            # Preferimos usar SIEMPRE endpoints del backend (proxy seguro)
            preview_url  = f"/preoperacional-motos/adjunto/{ida}/preview"
            download_url = f"/preoperacional-motos/adjunto/{ida}/download"

            out.append({
                "id_adjunto": ida,
                "nombre": r["nombre_archivo"],
                # Para no romper tu JS actual, también exponemos con estas claves:
                "url": preview_url,
                "url_descarga": download_url,
                # Y explícitas:
                "preview_url": preview_url,
                "download_url": download_url,
                "content_type": r["content_type"],
                "tamano_bytes": r["tamano_bytes"],
            })
        return out

    # ---------- Azure helpers ----------
    def _parse_conn_str(self, conn: str) -> Tuple[Optional[str], Optional[str]]:
        parts = {}
        for seg in conn.split(";"):
            if "=" in seg:
                k, v = seg.split("=", 1)
                parts[k.strip()] = v.strip()
        return parts.get("AccountName"), parts.get("AccountKey")

    def _generar_url_sas_lectura(self, ruta_blob: str, minutos: int = 15) -> str:
        # Si no hay llave (contenedor público), intenta URL directa
        if not self._account_name or not self._account_key:
            return f"{self._container.url}/{ruta_blob}"

        sas = generate_blob_sas(
            account_name=self._account_name,
            container_name=CONTAINER_NAME,
            blob_name=ruta_blob,
            account_key=self._account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(minutes=minutos),
        )
        return f"{self._container.url}/{ruta_blob}?{sas}"
    
    # ---------- Obtener metadatos de un adjunto por id ----------
    def obtener_adjunto_por_id(self, id_adjunto: int) -> Optional[Dict[str, Any]]:
        with self._cursor_dict() as cur:
            cur.execute("""
                SELECT id_adjunto, nombre_archivo, ruta_blob, content_type, tamano_bytes
                FROM public.checklist_preop_adjunto
                WHERE id_adjunto = %s
            """, (id_adjunto,))
            row = cur.fetchone()
        return row

    # ---------- Abrir stream del blob (para StreamingResponse) ----------
    def abrir_stream_blob(self, ruta_blob: str):
        blob = self._container.get_blob_client(ruta_blob)
        return blob.download_blob()

    # ---------- Reportes de Mantenimientos Preoperacionales ----------
    def consultar_datos_reporte_preop(
        self,
        fecha_inicio: str,
        fecha_fin: str,
        placa: Optional[str] = None,
        tipo_vehiculo: Optional[str] = None,
        marca: Optional[str] = None,
        usuario: Optional[str] = None,
    ):
        condiciones = ["r.fecha_mantenimiento BETWEEN %s AND %s"]
        params = [fecha_inicio, fecha_fin]

        if placa:
            condiciones.append("v.placa = %s");           params.append(placa)
        if tipo_vehiculo:
            condiciones.append("tv.nombre = %s");         params.append(tipo_vehiculo)
        if marca:
            condiciones.append("v.marca = %s");           params.append(marca)
        if usuario:
            condiciones.append("r.usuario_registro = %s"); params.append(usuario)

        where_clause = " AND ".join(condiciones)

        query = f"""
            SELECT
                r.id_preop,
                r.fecha_mantenimiento::date                               AS fecha_mantenimiento,
                v.placa,
                tv.nombre                                                  AS tipo_vehiculo,
                v.marca, v.linea, v.modelo,
                r.mantenimiento,
                r.observacion,
                r.usuario_registro                                         AS registrado_por,
                r.fecha_guardado,

                -- ❗️Campos “claros” para el usuario:
                vw.odometro_final_dia                                      AS odometro_dia,          -- cómo quedó al final del día
                vw.odometro_corte                                          AS odometro_corte,        -- acumulado del día / corte
                vw.base_periodo                                            AS base_periodo,
                vw.km_utilizados                                           AS km_utilizados,         -- odom_dia - odom_inicial_del_periodo
                vw.diferencia_optimo                                       AS diferencia_optimo,
                vw.porcentaje_uso                                          AS porcentaje_uso,
                vw.prox_mtto_km                                            AS prox_mtto_km
            FROM public.checklist_preop_registro r
            JOIN public.vehiculos v                 ON v.id = r.id_vehiculo
            LEFT JOIN public.checklist_tipo_vehiculo tv ON tv.id_tipo_vehiculo = v.id_tipo_vehiculo
            LEFT JOIN public.vw_preop_historial_motos vw ON vw.id_preop = r.id_preop
            WHERE {where_clause}
            ORDER BY r.fecha_guardado DESC, r.id_preop DESC;
        """

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

        # Formateos seguros (si tu respuesta JSON los necesita en string)
        for r in rows:
            if isinstance(r.get("fecha_guardado"), datetime):
                r["fecha_guardado"] = r["fecha_guardado"].strftime("%Y-%m-%d %H:%M:%S")
        return rows
    