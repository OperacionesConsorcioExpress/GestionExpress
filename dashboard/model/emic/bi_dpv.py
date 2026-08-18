"""
bi_dpv.py
─────────
Proveedor de datos para el dashboard "DPV" (EMIC).
Lienzo en limpio: sin datos de prueba. Listo para implementar las consultas
reales contra las capas Plata/Oro cuando se definan los requerimientos.
"""
from dashboard.database.database_manager import get_plata_connection, get_oro_connection


def consultar() -> dict:
    """
    Punto de entrada estandar consumido por GET /api/bi/emic/dpv
    (dashboard/router.py). Reemplazar el cuerpo por la consulta real contra
    get_plata_connection() / get_oro_connection() segun corresponda.
    """
    return {
        "ok": True,
        "fuente": "dpv",
        "datos": [],
    }
