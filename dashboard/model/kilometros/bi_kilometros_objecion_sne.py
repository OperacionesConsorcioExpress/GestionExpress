"""
bi_kilometros_objecion_sne.py
─────────────────────────────
Proveedor de datos para el dashboard Objeción SNE (Kilómetros).
Los endpoints de datos son servidos directamente por el router del monitor SNE
(/monitor/api/dashboard/...).  Este módulo queda como punto de extensión
para cuando se requieran consultas directas a DB desde el módulo BI.
"""
import os
from sqlalchemy import create_engine, text


def _engine():
    url = os.environ.get("DATABASE_URL", "")
    return create_engine(url) if url else None


def consultar() -> dict:
    """
    Retorna metadata del dashboard para el endpoint genérico BI.
    Los datos reales de cada gráfica los obtiene el HTML partial
    desde /monitor/api/dashboard/...
    """
    return {
        "ok": True,
        "fuente": "monitor_sne",
        "descripcion": "Dashboard de Objeción SNE — datos vía /monitor/api/dashboard/",
    }
