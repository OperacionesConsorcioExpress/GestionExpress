import os, re, uuid, json, mimetypes
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "eds-adjuntos-gestionexpress"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

class RegistroSNE:
    def __init__(self):
        self.conn = psycopg2.connect(
            DATABASE_PATH,
            cursor_factory=RealDictCursor,
            options='-c timezone=America/Bogota'
        )
        with self.conn.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            self.conn.close()

    # ---------------------------
    # Helpers DB
    # ---------------------------
    def _fetchone(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchone()

    def _fetchall(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
            return c.fetchall()

    def _execute(self, sql: str, params: list = None):
        with self.conn.cursor() as c:
            c.execute(sql, params or [])
