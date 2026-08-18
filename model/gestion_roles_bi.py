"""
Modelo de datos: Roles de acceso al módulo Business Intelligence
================================================================
Tabla requerida en PostgreSQL (ejecutar una vez):

    CREATE TABLE IF NOT EXISTS public.roles_bi (
        id             SERIAL       PRIMARY KEY,
        nombre_rol_bi  VARCHAR(255) NOT NULL,
        graficas_csv   TEXT         NOT NULL DEFAULT '',
        estado         SMALLINT     NOT NULL DEFAULT 1
    );

graficas_csv almacena las rutas de visualizaciones separadas por coma,
por ejemplo: 'operaciones/chart_sne_resumen,juridico/chart_clausulas_kpi'
"""

from typing import List, Dict, Any, Optional
from database.database_manager import get_db_connection

# ── NOTA: dashboard.config NO se importa a nivel de módulo para evitar
# el import circular con dashboard/__init__.py → router.py → este archivo.
# Se importa de forma lazy dentro de cada función que lo necesita.


# ─── Opciones agrupadas para el <select> del formulario ──────────────────────
# Retorna grupos con estructura para usar <optgroup> en el HTML.
# Cada área de AREAS_GRAFICAS aparece como grupo independiente,
# incluyendo áreas con rutas compartidas (ej.: Mantenimiento).
# Al agregar área/gráfica en config.py queda disponible aquí automáticamente.

def obtener_opciones_graficas() -> List[Dict[str, Any]]:
    """
    Retorna lista de grupos:
      [
        { "area": "Operaciones", "graficas": [{"id": ruta, "nombre": "..."}, ...] },
        ...
      ]
    Usar con <optgroup label="{{ grupo.area }}"> en el template.
    El import de dashboard.config es lazy para evitar import circular.
    """
    from dashboard.config import RUTAS_GRAFICA, AREAS_GRAFICAS  # lazy

    grupos: List[Dict[str, Any]] = []
    for area, nombres in AREAS_GRAFICAS.items():
        graficas_area = []
        for nombre in nombres:
            ruta = RUTAS_GRAFICA.get(nombre)
            if ruta:
                graficas_area.append({"id": ruta, "nombre": nombre})
        if graficas_area:
            grupos.append({"area": area, "graficas": graficas_area})
    return grupos


def construir_mapa_nombres() -> Dict[str, str]:
    """
    Retorna { ruta: 'Área — Nombre' } para resolver etiquetas en la tabla.
    La primera área que registre una ruta define la etiqueta (evita duplicados).
    """
    mapa: Dict[str, str] = {}
    for grupo in obtener_opciones_graficas():
        for g in grupo["graficas"]:
            if g["id"] not in mapa:
                mapa[g["id"]] = f"{grupo['area']} — {g['nombre']}"
    return mapa


# ─── Clase de acceso a datos ─────────────────────────────────────────────────

class ModeloRolesBi:
    """
    Cada instancia no guarda estado de conexión.
    Se puede instanciar, usar y descartar sin riesgo de leak.
    """

    # ── Creación ─────────────────────────────────────────────────────────────
    def insertar_rol(self, nombre_rol: str, graficas: List[str]) -> None:
        """Inserta un nuevo rol con las visualizaciones indicadas."""
        csv = ",".join(graficas)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.roles_bi (nombre_rol_bi, graficas_csv, estado)
                    VALUES (%s, %s, 1)
                """, (nombre_rol.strip(), csv))
            conn.commit()

    # ── Consulta ─────────────────────────────────────────────────────────────
    def obtener_todos_roles(self) -> List[tuple]:
        """Retorna todas las filas de roles_bi ordenadas por id."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombre_rol_bi, graficas_csv, estado
                    FROM public.roles_bi
                    ORDER BY id ASC
                """)
                return cur.fetchall()

    def obtener_rol_por_id(self, id_rol: int) -> Optional[Dict[str, Any]]:
        """Retorna el rol como dict o None si no existe."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nombre_rol_bi, graficas_csv, estado
                    FROM public.roles_bi
                    WHERE id = %s
                """, (id_rol,))
                fila = cur.fetchone()

        if not fila:
            return None

        graficas = [g.strip() for g in fila[2].split(",") if g.strip()]
        return {
            "id":            fila[0],
            "nombre_rol_bi": fila[1],
            "graficas":      graficas,
            "estado":        fila[3],
        }

    # ── Actualización ─────────────────────────────────────────────────────────
    def actualizar_rol(self, id_rol: int, nombre_rol: str, graficas: List[str]) -> None:
        """Actualiza nombre y visualizaciones de un rol existente."""
        csv = ",".join(graficas)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.roles_bi
                    SET nombre_rol_bi = %s, graficas_csv = %s
                    WHERE id = %s
                """, (nombre_rol.strip(), csv, id_rol))
            conn.commit()

    def cambiar_estado(self, id_rol: int, estado: int) -> None:
        """Activa (1) o inactiva (0) un rol."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.roles_bi
                    SET estado = %s
                    WHERE id = %s
                """, (estado, id_rol))
            conn.commit()
