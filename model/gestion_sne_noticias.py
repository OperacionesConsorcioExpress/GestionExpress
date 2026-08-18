import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import get_db_connection

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NOTICIAS = "sne-noticias"

TZ_BOGOTA = ZoneInfo("America/Bogota")
logger = logging.getLogger(__name__)

_CONTENT_TYPES = {
    "pdf":  "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls":  "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc":  "application/msword",
    "png":  "image/png",
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "gif":  "image/gif",
    "webp": "image/webp",
}

_DDL_NOTICIAS = """
CREATE TABLE IF NOT EXISTS sne.noticias (
    id              SERIAL PRIMARY KEY,
    asunto          TEXT     NOT NULL,
    observacion     TEXT     NOT NULL,
    imagen          JSONB    NOT NULL DEFAULT '[]'::jsonb,
    estado          SMALLINT NOT NULL DEFAULT 1,
    usuario_publico TEXT,
    fecha_registro  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'America/Bogota')
);
CREATE INDEX IF NOT EXISTS idx_noticias_estado ON sne.noticias (estado);
CREATE INDEX IF NOT EXISTS idx_noticias_fecha  ON sne.noticias (fecha_registro DESC);
"""


def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)


class GestionSneNoticias:
    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        self._tabla_creada = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    def _crear_tabla(self):
        if self._tabla_creada:
            return
        self.cursor.execute(_DDL_NOTICIAS)
        self.connection.commit()
        self._tabla_creada = True

    def contar_activas(self) -> int:
        self._crear_tabla()
        self.cursor.execute("SELECT COUNT(*) AS total FROM sne.noticias WHERE estado = 1")
        row = self.cursor.fetchone()
        return int(row["total"]) if row else 0

    def listar_noticias(self, estado=None, q=None):
        self._crear_tabla()
        where, params = " WHERE 1=1 ", []
        if estado is not None:
            where += " AND estado = %s "
            params.append(int(estado))
        if q:
            qn = f"%{q.strip().upper()}%"
            where += " AND (UPPER(asunto) LIKE %s OR UPPER(observacion) LIKE %s) "
            params += [qn, qn]
        self.cursor.execute(
            f"""
            SELECT id, asunto, observacion, imagen, estado, usuario_publico,
                   to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
            FROM sne.noticias
            {where}
            ORDER BY fecha_registro DESC, id DESC
            """,
            params,
        )
        return self.cursor.fetchall()

    def crear_noticia(self, asunto, observacion, estado, usuario_publico, adjuntos):
        self._crear_tabla()
        self.cursor.execute(
            """
            INSERT INTO sne.noticias (asunto, observacion, imagen, estado, usuario_publico, fecha_registro)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s)
            RETURNING id, asunto, observacion, imagen, estado, usuario_publico,
                      to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
            """,
            (
                asunto,
                observacion,
                json.dumps(adjuntos or []),
                int(estado),
                usuario_publico,
                ahora_bogota().replace(tzinfo=None),
            ),
        )
        fila = self.cursor.fetchone()
        self.connection.commit()
        return fila

    def actualizar_noticia(self, id, asunto, observacion, estado, adjuntos=None):
        self._crear_tabla()
        if adjuntos is None:
            self.cursor.execute(
                """
                UPDATE sne.noticias SET asunto=%s, observacion=%s, estado=%s
                WHERE id=%s
                RETURNING id, asunto, observacion, imagen, estado, usuario_publico,
                          to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
                """,
                (asunto, observacion, int(estado), int(id)),
            )
        else:
            self.cursor.execute(
                """
                UPDATE sne.noticias SET asunto=%s, observacion=%s, estado=%s, imagen=%s::jsonb
                WHERE id=%s
                RETURNING id, asunto, observacion, imagen, estado, usuario_publico,
                          to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
                """,
                (asunto, observacion, int(estado), json.dumps(adjuntos), int(id)),
            )
        fila = self.cursor.fetchone()
        if not fila:
            raise ValueError("Noticia no encontrada")
        self.connection.commit()
        return fila

    def cambiar_estado_noticia(self, id, estado):
        self._crear_tabla()
        self.cursor.execute(
            "UPDATE sne.noticias SET estado=%s WHERE id=%s RETURNING id, estado",
            (int(estado), int(id)),
        )
        fila = self.cursor.fetchone()
        if not fila:
            raise ValueError("Noticia no encontrada")
        self.connection.commit()
        return fila

    def subir_adjunto_noticia(self, content: bytes, filename: str) -> dict:
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")

        ext = (filename.rsplit(".", 1)[-1].lower() if "." in filename else "")
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")

        ahora = ahora_bogota()
        # Carpetas por año/mes: sne-noticias/2025/06/20250615_143022_archivo.pdf
        carpeta = f"{ahora.strftime('%Y')}/{ahora.strftime('%m')}"
        ts = ahora.strftime("%Y%m%d_%H%M%S")
        blob_path = f"{carpeta}/{ts}_{filename}"

        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_NOTICIAS)
        try:
            container.create_container()
        except ResourceExistsError:
            pass

        blob = container.get_blob_client(blob_path)
        blob.upload_blob(
            content,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )
        return {"nombre": filename, "ruta_blob": blob_path, "tipo": ext}

    def descargar_adjunto_noticia(self, ruta_blob: str):
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_NOTICIAS)
        blob = container.get_blob_client(ruta_blob)
        stream = blob.download_blob()
        contenido = stream.readall()
        nombre_archivo = ruta_blob.split("/")[-1] if "/" in ruta_blob else ruta_blob
        ext = (nombre_archivo.rsplit(".", 1)[-1].lower() if "." in nombre_archivo else "")
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")
        return contenido, content_type, nombre_archivo
