import os
import psycopg2
from threading import Lock
from typing import List, Dict, Any, Optional, Tuple

RUTA_BD = os.getenv("DATABASE_PATH")

class ModeloRolesPowerBI:
    _instancia = None
    _candado = Lock()

    def __new__(cls):
        with cls._candado:
            if not cls._instancia:
                cls._instancia = super().__new__(cls)
                cls._instancia._con = cls._crear_conexion()
                cls._instancia._cur = cls._instancia._con.cursor()
            return cls._instancia

    @staticmethod
    def _crear_conexion():
        return psycopg2.connect(RUTA_BD)

    def _verificar_conexion(self):
        if not hasattr(self, "_con") or self._con.closed:
            self._con = self._crear_conexion()
            self._cur = self._con.cursor()

    # ---------------------- Reportes (fuente: public.reportbi) ----------------------
    def obtener_opciones_reportes(self) -> List[Dict[str, Any]]:
        """
        Retorna [{id:int, etiqueta:str}] donde etiqueta = "Workspace - Item"
        """
        with self._candado:
            self._verificar_conexion()
            self._cur.execute("""
                SELECT id, COALESCE(workspacename, ''), COALESCE(itemname, '')
                FROM public.reportbi
                ORDER BY workspacename, itemname
            """)
            filas = self._cur.fetchall()
            return [{"id": f[0], "etiqueta": f"{f[1]} - {f[2]}".strip(" -")} for f in filas]

    def resolver_nombres_reportes(self, lista_ids: List[int]) -> List[str]:
        """
        Dado [1,2,3] retorna ["Workspace - Item", ...]
        """
        if not lista_ids:
            return []
        with self._candado:
            self._verificar_conexion()
            tupla_ids = tuple(lista_ids)
            marcadores = ",".join(["%s"] * len(tupla_ids))
            self._cur.execute(f"""
                SELECT id, COALESCE(workspacename,''), COALESCE(itemname,'')
                FROM public.reportbi
                WHERE id IN ({marcadores})
                ORDER BY workspacename, itemname
            """, tupla_ids)
            filas = self._cur.fetchall()
            return [f"{f[1]} - {f[2]}".strip(" -") for f in filas]

    def obtener_reportes_por_ids(self, ids: List[int]) -> List[Tuple[int, str, str]]:
        """
        Retorna lista de tuplas (id, workspacename, itemname) para los ids dados.
        """
        if not ids:
            return []
        with self._candado:
            self._verificar_conexion()
            self._cur.execute("""
                SELECT id, COALESCE(workspacename,''), COALESCE(itemname,'')
                FROM public.reportbi
                WHERE id = ANY(%s)
                ORDER BY workspacename, itemname
            """, (ids,))
            return self._cur.fetchall()

    # ---------------------- Roles PowerBI ----------------------
    def insertar_rol(self, nombre_rol: str, lista_ids: List[int]):
        ids_csv = ",".join(str(i) for i in lista_ids)
        with self._candado:
            self._verificar_conexion()
            try:
                self._cur.execute("""
                    INSERT INTO public.roles_powerbi (nombre_rol_powerbi, id_powerbi, estado)
                    VALUES (%s, %s, 1)
                """, (nombre_rol.strip(), ids_csv))
                self._con.commit()
            except Exception as e:
                self._con.rollback()
                raise e

    def obtener_todos_roles(self) -> List[tuple]:
        with self._candado:
            self._verificar_conexion()
            self._cur.execute("""
                SELECT id, nombre_rol_powerbi, id_powerbi, estado
                FROM public.roles_powerbi
                ORDER BY id ASC
            """)
            return self._cur.fetchall()

    def obtener_rol_por_id(self, id_rol: int) -> Optional[Dict[str, Any]]:
        with self._candado:
            self._verificar_conexion()
            self._cur.execute("""
                SELECT id, nombre_rol_powerbi, id_powerbi, estado
                FROM public.roles_powerbi
                WHERE id = %s
            """, (id_rol,))
            f = self._cur.fetchone()
            if not f:
                return None
            lista_ids = [int(x) for x in f[2].split(",") if x.strip().isdigit()]
            return {
                "id": f[0],
                "nombre_rol_powerbi": f[1],
                "id_powerbi": lista_ids,
                "estado": f[3]
            }

    def actualizar_rol(self, id_rol: int, nombre_rol: str, lista_ids: List[int]):
        ids_csv = ",".join(str(i) for i in lista_ids)
        with self._candado:
            self._verificar_conexion()
            try:
                self._cur.execute("""
                    UPDATE public.roles_powerbi
                    SET nombre_rol_powerbi = %s, id_powerbi = %s
                    WHERE id = %s
                """, (nombre_rol.strip(), ids_csv, id_rol))
                self._con.commit()
            except Exception as e:
                self._con.rollback()
                raise e

    def cambiar_estado(self, id_rol: int, estado: int):
        with self._candado:
            self._verificar_conexion()
            try:
                self._cur.execute("""
                    UPDATE public.roles_powerbi
                    SET estado = %s
                    WHERE id = %s
                """, (estado, id_rol))
                self._con.commit()
            except Exception as e:
                self._con.rollback()
                raise e

    # --------------- Obtener Reportes Power BI por Rol ----------------------
    def obtener_ids_por_rol(self, id_rol: int) -> List[int]:
        """
        Devuelve la lista de IDs de reportes (int) definidos en el rol.
        """
        rol = self.obtener_rol_por_id(id_rol)
        if not rol or not rol["id_powerbi"]:
            return []
        return rol["id_powerbi"]

    def obtener_reportes_por_rol(self, id_rol: int) -> List[Tuple[int, str, str]]:
        """
        Devuelve (id, workspacename, itemname) de los reportes asociados al rol.
        """
        ids = self.obtener_ids_por_rol(id_rol)
        return self.obtener_reportes_por_ids(ids)