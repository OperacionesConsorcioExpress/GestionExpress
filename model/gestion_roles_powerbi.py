from typing import List, Dict, Any, Optional, Tuple
from psycopg2.extras import RealDictCursor
from database.database_manager import get_db_connection

class ModeloRolesPowerBI:
    """
    Cada instancia NO guarda estado de conexión.
    Se puede instanciar, usar y descartar sin riesgo de leak.
    """
    # ─────────────────────────────────────────────
    # Reportes (fuente: public.reportbi)
    # ─────────────────────────────────────────────
    def obtener_opciones_reportes(self) -> List[Dict[str, Any]]:
        """Retorna [{id, etiqueta}] para el <select> del formulario."""
        from model.gestion_reportbi import obtener_opciones_reportes
        return obtener_opciones_reportes()

    def resolver_nombres_reportes(self, lista_ids: List[int]) -> List[str]:
        """Dado [1,2,3] retorna ['Workspace - Item', ...]."""
        from model.gestion_reportbi import resolver_nombres_reportes
        return resolver_nombres_reportes(lista_ids)

    def obtener_reportes_por_ids(self, ids: List[int]) -> List[Tuple[int, str, str]]:
        """Retorna [(id, workspacename, itemname)] para los ids dados."""
        if not ids:
            return []
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id,
                            COALESCE(workspacename,'') AS workspacename,
                            COALESCE(itemname,'')      AS itemname
                    FROM public.reportbi
                    WHERE id = ANY(%s)
                    ORDER BY workspacename, itemname
                """, (ids,))
                return cur.fetchall()

    # ─────────────────────────────────────────────
    # Roles (fuente: public.roles_powerbi)
    # ─────────────────────────────────────────────
    def insertar_rol(self, nombre_rol: str, lista_ids: List[int]) -> None:
        ids_csv = ",".join(str(i) for i in lista_ids)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.roles_powerbi (nombre_rol_powerbi, id_powerbi, estado)
                    VALUES (%s, %s, 1)
                """, (nombre_rol.strip(), ids_csv))
            conn.commit()

    def obtener_todos_roles(self) -> List[tuple]:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombre_rol_powerbi, id_powerbi, estado
                    FROM public.roles_powerbi
                    ORDER BY id ASC
                """)
                return cur.fetchall()

    def obtener_rol_por_id(self, id_rol: int) -> Optional[Dict[str, Any]]:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombre_rol_powerbi, id_powerbi, estado
                    FROM public.roles_powerbi
                    WHERE id = %s
                """, (id_rol,))
                f = cur.fetchone()

        if not f:
            return None
        lista_ids = [int(x) for x in f[2].split(",") if x.strip().isdigit()]
        return {
            "id":               f[0],
            "nombre_rol_powerbi": f[1],
            "id_powerbi":       lista_ids,
            "estado":           f[3],
        }

    def actualizar_rol(self, id_rol: int, nombre_rol: str, lista_ids: List[int]) -> None:
        ids_csv = ",".join(str(i) for i in lista_ids)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.roles_powerbi
                    SET nombre_rol_powerbi = %s, id_powerbi = %s
                    WHERE id = %s
                """, (nombre_rol.strip(), ids_csv, id_rol))
            conn.commit()

    def cambiar_estado(self, id_rol: int, estado: int) -> None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.roles_powerbi
                    SET estado = %s
                    WHERE id = %s
                """, (estado, id_rol))
            conn.commit()

    # ─────────────────────────────────────────────
    # Helpers combinados
    # ─────────────────────────────────────────────
    def obtener_ids_por_rol(self, id_rol: int) -> List[int]:
        """Devuelve la lista de IDs de reportes del rol."""
        rol = self.obtener_rol_por_id(id_rol)
        return rol["id_powerbi"] if rol else []

    def obtener_reportes_por_rol(self, id_rol: int) -> List[Tuple[int, str, str]]:
        """Devuelve (id, workspacename, itemname) de los reportes del rol."""
        return self.obtener_reportes_por_ids(self.obtener_ids_por_rol(id_rol))