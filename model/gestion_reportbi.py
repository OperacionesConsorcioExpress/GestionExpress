from typing import List, Dict, Any, Optional
from psycopg2.extras import RealDictCursor
from database.database_manager import get_db_connection

# ─────────────────────────────────────────────
# Utilidad interna
# ─────────────────────────────────────────────
def _agrupar_por_workspace(filas) -> Dict[str, List[Dict[str, Any]]]:
    """
    Recibe filas con llaves: id, workspacename, itemname.
    Devuelve: { "Workspace A": [{report_id, ItemName}, ...], ... }
    """
    agrupado: Dict[str, List[Dict[str, Any]]] = {}
    for row in filas:
        ws = row["workspacename"] or ""
        if ws not in agrupado:
            agrupado[ws] = []
        agrupado[ws].append({
            "report_id": row["id"],
            "ItemName":  row["itemname"] or ""
        })
    return agrupado

# ─────────────────────────────────────────────
# Funciones públicas
# ─────────────────────────────────────────────
def obtener_reportes_por_ids(ids_reportes: List[int]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna reportes agrupados por workspace para los IDs dados.
    Uso: render inicial de powerbi.html.
    """
    if not ids_reportes:
        return {}

    ids_limpios = [x for x in ids_reportes if isinstance(x, int)]
    if not ids_limpios:
        return {}

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, workspacename, itemname
                FROM public.reportbi
                WHERE id = ANY(%s)
                ORDER BY workspacename, itemname
            """, (ids_limpios,))
            filas = cur.fetchall()

    return _agrupar_por_workspace(filas)

def obtener_url_bi(report_id: int) -> Optional[str]:
    """
    Retorna la weburl del informe o None.
    Uso: endpoint /api/get_report_url (un click = una query = conexión liberada).
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT weburl FROM public.reportbi WHERE id = %s",
                (report_id,)
            )
            row = cur.fetchone()

    if not row:
        return None
    url = row["weburl"]
    return url if url and str(url).strip() not in ("", "NaN") else None

def obtener_opciones_reportes() -> List[Dict[str, Any]]:
    """
    Retorna [{id, etiqueta}] para el <select> de roles_powerbi.html.
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id,
                        COALESCE(workspacename, '') AS workspacename,
                        COALESCE(itemname,      '') AS itemname
                FROM public.reportbi
                ORDER BY workspacename, itemname
            """)
            filas = cur.fetchall()

    return [
        {
            "id":      f["id"],
            "etiqueta": f"{f['workspacename']} - {f['itemname']}".strip(" -")
        }
        for f in filas
    ]

def resolver_nombres_reportes(lista_ids: List[int]) -> List[str]:
    """
    Dado [1, 2, 3] retorna ["Workspace - Item", ...].
    Uso: tabla de roles_powerbi.html para mostrar nombres de reportes.
    """
    if not lista_ids:
        return []

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id,
                        COALESCE(workspacename, '') AS workspacename,
                        COALESCE(itemname,      '') AS itemname
                FROM public.reportbi
                WHERE id = ANY(%s)
                ORDER BY workspacename, itemname
            """, (lista_ids,))
            filas = cur.fetchall()

    return [f"{f['workspacename']} - {f['itemname']}".strip(" -") for f in filas]
