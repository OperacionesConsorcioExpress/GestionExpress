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
CONTAINER_FICHAS_RUTAS = "sne-fichas-rutas"

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

_DDL_FICHA_RUTAS = """
CREATE TABLE IF NOT EXISTS sne.ficha_rutas (
    id              SERIAL PRIMARY KEY,
    id_linea        BIGINT NOT NULL,
    ruta_comercial  TEXT   NOT NULL,
    asunto          TEXT   NOT NULL,
    observacion     TEXT   NOT NULL,
    imagen          JSONB  NOT NULL DEFAULT '[]'::jsonb,
    estado          SMALLINT NOT NULL DEFAULT 1,
    usuario_publico TEXT,
    fecha_registro  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'America/Bogota')
);
CREATE INDEX IF NOT EXISTS idx_ficha_rutas_id_linea ON sne.ficha_rutas (id_linea);
"""


def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)


class GestionSneFichaRutas:
    """
    Modelo de gestión de la "Ficha de Rutas" (bitácora / base de conocimiento por ruta).

    Tablas involucradas:
        sne.ficha_rutas   - historias/publicaciones por ruta (id_linea)
        config.rutas      - id_linea -> ruta_comercial
        config.cop        - id_cop   -> cop / componente / zona
        config.componente - id       -> componente
        config.zona       - id       -> zona
    """

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

    # ── DDL ──────────────────────────────────────────────────────────────────────
    def _crear_tabla(self):
        if self._tabla_creada:
            return
        self.cursor.execute(_DDL_FICHA_RUTAS)
        self.connection.commit()
        self._tabla_creada = True

    # ── Helpers ──────────────────────────────────────────────────────────────────
    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        self.cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return self.cursor.fetchone() is not None

    def _cop_joins_and_exprs(self):
        """Devuelve (joins_sql, comp_expr, zona_expr) según la estructura real de config.cop."""
        has_id_comp = self._col_exists("config", "cop", "id_componente")
        has_id_zona = self._col_exists("config", "cop", "id_zona")
        joins, comp_expr, zona_expr = [], "NULL", "NULL"
        if has_id_comp:
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        if has_id_zona:
            joins.append("LEFT JOIN config.zona z ON z.id = c.id_zona")
            zona_expr = "z.zona"
        return (" ".join(joins), comp_expr, zona_expr)

    def _u(self, s):
        if s is None:
            return None
        return str(s).strip().upper()

    # ── Rutas activas (config.rutas) ────────────────────────────────────────────
    def listar_rutas_activas(self, q=None, componente=None, zona=None, id_cop=None):
        where, params = " WHERE r.estado = 1 ", []

        if q:
            qn = f"%{q.strip().upper()}%"
            where += " AND (UPPER(r.ruta_comercial) LIKE %s OR CAST(r.id_linea AS TEXT) LIKE %s) "
            params += [qn, qn]
        if id_cop:
            where += " AND r.id_cop=%s "
            params.append(int(id_cop))

        joins_cop, comp_expr, zona_expr = self._cop_joins_and_exprs()

        if componente:
            where += f" AND UPPER({comp_expr})=%s "
            params.append(self._u(componente))
        if zona:
            where += f" AND UPPER({zona_expr})=%s "
            params.append(self._u(zona))

        sqlq = f"""
            SELECT
                r.id_linea,
                r.ruta_comercial,
                c.id AS id_cop,
                c.cop,
                {comp_expr} AS componente,
                {zona_expr} AS zona
            FROM config.rutas r
            JOIN config.cop c ON c.id = r.id_cop
            {joins_cop}
            {where}
            ORDER BY r.id_linea ASC, r.ruta_comercial ASC
        """
        self.cursor.execute(sqlq, params)
        return self.cursor.fetchall()

    # ── Fichas / historias ──────────────────────────────────────────────────────
    def listar_fichas(self, id_linea=None, ruta_comercial=None, estado=None, q=None):
        self._crear_tabla()
        where, params = " WHERE 1=1 ", []

        if id_linea is not None:
            where += " AND id_linea = %s "
            params.append(int(id_linea))
        if ruta_comercial:
            where += " AND UPPER(ruta_comercial) = %s "
            params.append(self._u(ruta_comercial))
        if estado is not None:
            where += " AND estado = %s "
            params.append(int(estado))
        if q:
            qn = f"%{q.strip().upper()}%"
            where += " AND (UPPER(asunto) LIKE %s OR UPPER(observacion) LIKE %s) "
            params += [qn, qn]

        sqlq = f"""
            SELECT
                id, id_linea, ruta_comercial, asunto, observacion, imagen, estado,
                usuario_publico,
                to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
            FROM sne.ficha_rutas
            {where}
            ORDER BY fecha_registro DESC, id DESC
        """
        self.cursor.execute(sqlq, params)
        return self.cursor.fetchall()

    def crear_ficha(self, id_linea, ruta_comercial, asunto, observacion, estado, usuario_publico, adjuntos):
        self._crear_tabla()
        self.cursor.execute(
            """
            INSERT INTO sne.ficha_rutas
                (id_linea, ruta_comercial, asunto, observacion, imagen, estado, usuario_publico, fecha_registro)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            RETURNING id, id_linea, ruta_comercial, asunto, observacion, imagen, estado,
                      usuario_publico, to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
            """,
            (
                int(id_linea),
                ruta_comercial,
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

    def actualizar_ficha(self, id, asunto, observacion, estado, adjuntos=None):
        self._crear_tabla()
        if adjuntos is None:
            self.cursor.execute(
                """
                UPDATE sne.ficha_rutas
                SET asunto=%s, observacion=%s, estado=%s
                WHERE id=%s
                RETURNING id, id_linea, ruta_comercial, asunto, observacion, imagen, estado,
                          usuario_publico, to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
                """,
                (asunto, observacion, int(estado), int(id)),
            )
        else:
            self.cursor.execute(
                """
                UPDATE sne.ficha_rutas
                SET asunto=%s, observacion=%s, estado=%s, imagen=%s::jsonb
                WHERE id=%s
                RETURNING id, id_linea, ruta_comercial, asunto, observacion, imagen, estado,
                          usuario_publico, to_char(fecha_registro, 'YYYY-MM-DD HH24:MI') AS fecha_registro
                """,
                (asunto, observacion, int(estado), json.dumps(adjuntos), int(id)),
            )
        fila = self.cursor.fetchone()
        if not fila:
            raise ValueError("Ficha no encontrada")
        self.connection.commit()
        return fila

    def cambiar_estado_ficha(self, id, estado):
        self._crear_tabla()
        self.cursor.execute(
            """
            UPDATE sne.ficha_rutas SET estado=%s WHERE id=%s
            RETURNING id, estado
            """,
            (int(estado), int(id)),
        )
        fila = self.cursor.fetchone()
        if not fila:
            raise ValueError("Ficha no encontrada")
        self.connection.commit()
        return fila

    # ── Blob Storage ─────────────────────────────────────────────────────────────
    def _sanear_carpeta(self, texto: str) -> str:
        import re
        limpio = re.sub(r"[^A-Za-z0-9_-]+", "_", (texto or "").strip())
        return limpio.strip("_") or "SIN_RUTA"

    def subir_adjunto_ficha(self, content: bytes, ruta_comercial: str, filename: str) -> dict:
        """Sube un adjunto al contenedor sne-fichas-rutas dentro de una carpeta por ruta_comercial."""
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")

        ext = (filename.rsplit(".", 1)[-1].lower() if "." in filename else "")
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")

        carpeta = self._sanear_carpeta(ruta_comercial)
        ts = ahora_bogota().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"{ts}_{filename}"
        blob_path = f"{carpeta}/{nombre_archivo}"

        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_FICHAS_RUTAS)
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

    def descargar_adjunto_ficha(self, ruta_blob: str):
        """Retorna (bytes, content_type, nombre_archivo) de un adjunto."""
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_FICHAS_RUTAS)
        blob = container.get_blob_client(ruta_blob)
        stream = blob.download_blob()
        contenido = stream.readall()
        nombre_archivo = ruta_blob.split("/")[-1] if "/" in ruta_blob else ruta_blob
        ext = (nombre_archivo.rsplit(".", 1)[-1].lower() if "." in nombre_archivo else "")
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")
        return contenido, content_type, nombre_archivo
