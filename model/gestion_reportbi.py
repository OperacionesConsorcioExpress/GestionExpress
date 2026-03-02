from psycopg2.extras import RealDictCursor
from psycopg2 import extensions as pg_extensions
from typing import List, Dict, Any
from model.database_manager import _get_pool as get_db_pool

class ReportBIGestion:
    def __init__(self):
        self.connection = get_db_pool().getconn()
        if not self.connection.closed:
            self.connection.rollback()

    # ---------- utilitario interno ----------
    @staticmethod
    def _agrupar_por_workspace(filas: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Recibe filas con llaves: id, workspacename, itemname
        Devuelve: { "Workspace A": [ {report_id, ItemName}, ... ], ... }
        """
        agrupado: Dict[str, List[Dict[str, Any]]] = {}
        for row in filas:
            ws = row["workspacename"] or ""
            if ws not in agrupado:
                agrupado[ws] = []
            agrupado[ws].append({
                "report_id": row["id"],
                "ItemName": row["itemname"] or ""
            })
        return agrupado

    # ---------- TODOS los reportes (sin filtro) ----------
    def obtener_reportes(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Consulta la tabla 'reportbi' y devuelve TODO agrupado por workspace.
        """
        query = """
            SELECT id, workspacename, itemname
            FROM reportbi
            ORDER BY workspacename, itemname;
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                filas = cursor.fetchall()
            return self._agrupar_por_workspace(filas)
        except Exception as e:
            print(f"Error al consultar la tabla reportbi: {e}")
            return {}

    # ---------- SOLO reportes por lista de IDs ----------
    def obtener_reportes_por_ids(self, ids_reportes: List[int]) -> Dict[str, List[Dict[str, Any]]]:
        if not ids_reportes:
            return {}
        ids_limpios = []
        for x in ids_reportes:
            if isinstance(x, int):
                ids_limpios.append(x)
            elif isinstance(x, str) and x.strip().isdigit():
                ids_limpios.append(int(x))
        if not ids_limpios:
            return {}
        query = """
            SELECT id, workspacename, itemname
            FROM reportbi
            WHERE id = ANY(%s)
            ORDER BY workspacename, itemname;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (ids_limpios,))
            filas = cursor.fetchall()
        return self._agrupar_por_workspace(filas)

    def obtener_url_bi(self, report_id: int) -> str | None:
        """
        Obtiene la URL del informe según su ID.
        """
        query = "SELECT weburl FROM reportbi WHERE id = %s"
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (report_id,))
                result = cursor.fetchone()
                return (result["weburl"] or None) if result else None
        except Exception as e:
            print(f"Error al obtener la URL del informe: {e}")
            return None

    def close(self):
        if not self.connection.closed:
            self.connection.rollback()
            self.connection.cursor_factory = pg_extensions.cursor
            get_db_pool().putconn(self.connection)
