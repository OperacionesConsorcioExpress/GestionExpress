import os
from psycopg2.extras import RealDictCursor
from psycopg2 import extensions as pg_extensions
from datetime import datetime
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import _get_pool as get_db_pool

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "xxxxxxxxxx"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

class RegistroSNE:
    def __init__(self):
        self.conn = get_db_pool().getconn()
        if not self.conn.closed:
            self.conn.rollback()
        self.conn.cursor_factory = RealDictCursor
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
            if not self.conn.closed:
                self.conn.rollback()
                self.conn.cursor_factory = pg_extensions.cursor
                get_db_pool().putconn(self.conn)

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
