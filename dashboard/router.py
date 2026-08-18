import inspect
import re
import logging
import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from jinja2.exceptions import TemplateNotFound
from starlette.background import BackgroundTask

# ── Configuración del menú ────────────────────────────────────────────────────
from .config import construir_menu

# ── Modelos de datos (uno por visualización, agrupados por área) ──────────────
from .model.emic import bi_dpv, bi_ico, bi_ics, bi_iri, bi_isv
from .model.gerenciales import bi_gestion_cop, bi_gestion_operacional
from .model.ingresos import bi_benchmarking_sitp, bi_ingresos_cexp
from .model.kilometros import (
    bi_kilometros_comercial,
    bi_kilometros_eliminados_no_ejecutados,
    bi_kilometros_objecion_sne,
)
from .model.operacionales import bi_ejecucion_operacional
from .model.operadores import (
    bi_conductas_operacionales,
    bi_disponibilidad_operadores,
    bi_sigma,
)
from .model.pasajeros import bi_demanda_pasajeros_cexp, bi_validaciones_sitp

# ── Modelo de roles BI (para filtrar menú por usuario) ────────────────────────
from model.gestion_roles_bi import ModeloRolesBi

router_bi = APIRouter()
logger = logging.getLogger(__name__)

# ── Motor de plantillas ───────────────────────────────────────────────────────
# Busca primero en dashboard/templates/ (hub + includes + partials)
# luego en view/ para resolver {% extends "components/layout.html" %}
_entorno = Environment(
    loader=FileSystemLoader(["./dashboard/templates", "./view"])
)
bi_plantillas = Jinja2Templates(env=_entorno)

# ── Seguridad de rutas ────────────────────────────────────────────────────────
_PATRON_RUTA = re.compile(r"^[a-z0-9_]+$")

# ── Menú global (base completa — se filtra por rol en cada request) ───────────
MENU_BI = construir_menu()

# ── Registro de proveedores de datos ─────────────────────────────────────────
# Clave = "{area}/{identificador}"  →  debe coincidir con la URL que llama
# el archivo HTML de cada visualización (sin prefijo "chart_").
PROVEEDORES_DATOS: dict[str, callable] = {
    # EMIC
    "emic/dpv": bi_dpv.consultar,
    "emic/ico": bi_ico.consultar,
    "emic/ics": bi_ics.consultar,
    "emic/iri": bi_iri.consultar,
    "emic/isv": bi_isv.consultar,
    # Gerenciales
    "gerenciales/gestion_cop":         bi_gestion_cop.consultar,
    "gerenciales/gestion_operacional": bi_gestion_operacional.consultar,
    # Ingresos
    "ingresos/benchmarking_sitp": bi_benchmarking_sitp.consultar,
    "ingresos/ingresos_cexp":     bi_ingresos_cexp.consultar,
    # Kilómetros
    "kilometros/kilometros_comercial":                bi_kilometros_comercial.consultar,
    "kilometros/kilometros_eliminados_no_ejecutados": bi_kilometros_eliminados_no_ejecutados.consultar,
    "kilometros/kilometros_objecion_sne":              bi_kilometros_objecion_sne.consultar,
    # Operacionales
    "operacionales/ejecucion_operacional": bi_ejecucion_operacional.consultar,
    # Operadores
    "operadores/conductas_operacionales":       bi_conductas_operacionales.consultar,
    "operadores/disponibilidad_operadores":     bi_disponibilidad_operadores.consultar,
    "operadores/sigma":                         bi_sigma.consultar,
    # Pasajeros
    "pasajeros/demanda_pasajeros_cexp": bi_demanda_pasajeros_cexp.consultar,
    "pasajeros/validaciones_sitp":      bi_validaciones_sitp.consultar,
}

FILTROS_EXPORTACION_KMNR = {
    "fecha_inicio", "fecha_fin", "anio", "mes", "semana_iso", "dia_semana", "trimestre", "semestre",
    "concesion", "componente", "id_linea", "nombre_linea", "franja_hora_real", "planificado", "eliminado",
    "estado_motivo_eliminacion", "descripcion_motivo_eliminacion", "vehiculo_real", "conductor", "grupo", "responsable",
}


def _eliminar_archivo_temporal(ruta: str) -> None:
    try:
        os.remove(ruta)
    except OSError:
        logger.warning("No fue posible eliminar archivo temporal de exportación KMNR")


def _nombre_exportacion_kmnr(parametros: dict[str, str]) -> str:
    inicio, fin = parametros.get("fecha_inicio"), parametros.get("fecha_fin")
    if inicio and fin:
        return f"kilometros_no_realizados_{inicio}_{fin}.xlsx"
    return f"kilometros_no_realizados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

def _obtener_sesion(solicitud: Request):
    return solicitud.session.get("user")


def _obtener_argumentos_proveedor(proveedor: callable, solicitud: Request) -> dict[str, object]:
    """
    La firma del proveedor define los filtros admitidos por cada dashboard.
    El router transmite solo parametros permitidos y deja validacion/conversion
    de tipos al modelo, evitando modificar router.py por cada nuevo filtro.
    """
    firma = inspect.signature(proveedor)
    parametros_admitidos = firma.parameters

    if any(
        parametro.kind == inspect.Parameter.VAR_KEYWORD
        for parametro in parametros_admitidos.values()
    ):
        return dict(solicitud.query_params.items())

    return {
        nombre: valor
        for nombre, valor in solicitud.query_params.items()
        if nombre in parametros_admitidos
    }

# ─── Filtrado de menú según rol del usuario ───────────────────────────────────
def _menu_para_usuario(rol_bi: int) -> tuple[dict, int]:
    """
    Filtra MENU_BI según las visualizaciones asignadas al rol BI del usuario.
    - rol_bi = 0       → sin acceso (menú vacío)
    - rol inactivo     → sin acceso (menú vacío)
    - rol_bi > 0 activo → solo las graficas del rol
    """
    if rol_bi == 0:
        return {}, 0

    rol_data = ModeloRolesBi().obtener_rol_por_id(rol_bi)
    if not rol_data or not rol_data.get("graficas"):
        return {}, 0

    if rol_data.get("estado") != 1:
        return {}, 0

    permitidas = set(rol_data["graficas"])
    menu_filtrado = {}

    for area, config in MENU_BI.items():
        graficas_area = [g for g in config["graficas"] if g["ruta"] in permitidas]
        if graficas_area:
            menu_filtrado[area] = {"icono": config["icono"], "graficas": graficas_area}

    total = sum(len(c["graficas"]) for c in menu_filtrado.values())
    return menu_filtrado, total

def _tiene_acceso(rol_bi: int, ruta_template: str) -> bool:
    """
    Verifica si el rol BI tiene acceso a una ruta de template concreta.
    ruta_template ej.: "kilometros/kilometros_objecion_sne"
    """
    if rol_bi == 0:
        return False
    rol_data = ModeloRolesBi().obtener_rol_por_id(rol_bi)
    if not rol_data:
        return False
    if rol_data.get("estado") != 1:
        return False
    return ruta_template in set(rol_data.get("graficas", []))

# ─── Vista principal del hub ──────────────────────────────────────────────────
@router_bi.get("/business_intelligence", response_class=HTMLResponse)
def vista_hub(solicitud: Request, sesion: dict = Depends(_obtener_sesion)):
    if not sesion:
        return RedirectResponse(url="/", status_code=302)

    rol_bi = sesion.get("rol_bi", 0)
    menu_usuario, total_usuario = _menu_para_usuario(rol_bi)

    return bi_plantillas.TemplateResponse(
        "business_intelligence.html",
        {
            "request":        solicitud,
            "user_session":   sesion,
            "menu_bi":        menu_usuario,
            "total_graficas": total_usuario,
        },
    )

# ─── Vista parcial de cada visualización (carga lazy desde el hub) ────────────
@router_bi.get("/bi/partial/{area}/{grafica}", response_class=HTMLResponse)
def vista_partial(
    area: str,
    grafica: str,
    solicitud: Request,
    sesion: dict = Depends(_obtener_sesion),
):
    if not sesion:
        return RedirectResponse(url="/", status_code=302)
    if not _PATRON_RUTA.match(area) or not _PATRON_RUTA.match(grafica):
        raise HTTPException(status_code=400, detail="Parámetro de ruta inválido")

    ruta_template = f"{area}/{grafica}"
    if not _tiene_acceso(sesion.get("rol_bi", 0), ruta_template):
        raise HTTPException(status_code=403, detail="Sin acceso a esta visualización")

    try:
        return bi_plantillas.TemplateResponse(
            f"charts/{area}/{grafica}.html",
            {"request": solicitud},
        )
    except TemplateNotFound:
        raise HTTPException(status_code=404, detail="Visualización no encontrada")

# ─── Endpoint genérico de datos ───────────────────────────────────────────────
@router_bi.get("/api/bi/kilometros/kilometros_eliminados_no_ejecutados/exportar.xlsx")
def exportar_kilometros_eliminados_no_ejecutados(
    solicitud: Request,
    sesion: dict = Depends(_obtener_sesion),
):
    if not sesion:
        raise HTTPException(status_code=401, detail="No autenticado")

    ruta_template = "kilometros/kilometros_eliminados_no_ejecutados"
    if not _tiene_acceso(sesion.get("rol_bi", 0), ruta_template):
        raise HTTPException(status_code=403, detail="Sin acceso a estos datos")

    parametros = dict(solicitud.query_params.items())
    no_admitidos = sorted(set(parametros) - FILTROS_EXPORTACION_KMNR)
    if no_admitidos:
        raise HTTPException(status_code=400, detail="ParÃ¡metros no admitidos para exportaciÃ³n")

    try:
        ruta_archivo = bi_kilometros_eliminados_no_ejecutados.generar_exportacion_xlsx(**parametros)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        logger.error("Error controlado generando exportación XLSX KMNR")
        raise HTTPException(status_code=500, detail="No fue posible generar la exportación.") from exc
    except Exception as exc:
        logger.exception("Error inesperado generando exportación XLSX KMNR")
        raise HTTPException(status_code=500, detail="No fue posible generar la exportación.") from exc

    return FileResponse(
        ruta_archivo,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=_nombre_exportacion_kmnr(parametros),
        background=BackgroundTask(_eliminar_archivo_temporal, ruta_archivo),
    )


@router_bi.get("/api/bi/{area}/{grafica}")
def api_datos(
    area: str,
    grafica: str,
    solicitud: Request,
    sesion: dict = Depends(_obtener_sesion),
):
    if not sesion:
        raise HTTPException(status_code=401, detail="No autenticado")
    if not _PATRON_RUTA.match(area) or not _PATRON_RUTA.match(grafica):
        raise HTTPException(status_code=400, detail="Parámetro de ruta inválido")

    ruta_template = f"{area}/{grafica}"
    if not _tiene_acceso(sesion.get("rol_bi", 0), ruta_template):
        raise HTTPException(status_code=403, detail="Sin acceso a estos datos")

    proveedor = PROVEEDORES_DATOS.get(f"{area}/{grafica}")
    if not proveedor:
        raise HTTPException(
            status_code=404,
            detail=f"No existe proveedor de datos para '{area}/{grafica}'"
        )
    argumentos = _obtener_argumentos_proveedor(proveedor, solicitud)
    return proveedor(**argumentos)
