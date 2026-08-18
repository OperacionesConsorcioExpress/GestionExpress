"""
dashboard/model  — Modelos de datos del módulo Business Intelligence
════════════════════════════════════════════════════════════════════
Estructura espejo de dashboard/templates/charts/:

    model/emic/           ←→   charts/emic/
    model/gerenciales/     ←→   charts/gerenciales/
    model/ingresos/        ←→   charts/ingresos/
    model/kilometros/      ←→   charts/kilometros/
    model/operacionales/   ←→   charts/operacionales/
    model/operadores/      ←→   charts/operadores/
    model/pasajeros/       ←→   charts/pasajeros/

Cada archivo expone una función consultar() -> dict
"""
from .emic import bi_dpv, bi_ico, bi_ics, bi_iri, bi_isv
from .gerenciales import bi_gestion_cop, bi_gestion_operacional
from .ingresos import bi_benchmarking_sitp, bi_ingresos_cexp
from .kilometros import (
    bi_kilometros_comercial,
    bi_kilometros_eliminados_no_ejecutados,
    bi_kilometros_objecion_sne,
)
from .operacionales import bi_ejecucion_operacional
from .operadores import (
    bi_conductas_operacionales,
    bi_disponibilidad_operadores,
    bi_sigma,
)
from .pasajeros import bi_demanda_pasajeros_cexp, bi_validaciones_sitp

__all__ = [
    "bi_dpv",
    "bi_ico",
    "bi_ics",
    "bi_iri",
    "bi_isv",
    "bi_gestion_cop",
    "bi_gestion_operacional",
    "bi_benchmarking_sitp",
    "bi_ingresos_cexp",
    "bi_kilometros_comercial",
    "bi_kilometros_eliminados_no_ejecutados",
    "bi_kilometros_objecion_sne",
    "bi_ejecucion_operacional",
    "bi_conductas_operacionales",
    "bi_disponibilidad_operadores",
    "bi_sigma",
    "bi_demanda_pasajeros_cexp",
    "bi_validaciones_sitp",
]
