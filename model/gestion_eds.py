import os, re
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict
from azure.storage.blob import BlobServiceClient

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

class GestionEDS:
    """Gestión de parámetros preoperacionales de motos (mantenimientos y km óptimo)."""

    def __init__(self):
        try:
            self.connection = psycopg2.connect(DATABASE_PATH,
                options='-c timezone=America/Bogota')
            self.cursor = self.connection.cursor()
            
            # Fallback defensivo (por si el options no se respeta en algún entorno)
            with self.connection.cursor() as c:
                c.execute("SET TIME ZONE 'America/Bogota';")
            self.connection.commit()
            
        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise e

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None):
            self.connection.close()
