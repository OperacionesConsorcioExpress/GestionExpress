"""
config.py — Configuración centralizada del menú Business Intelligence
═══════════════════════════════════════════════════════════════════════
Para agregar una visualización nueva:
    1. Agregar su ícono en ICONOS_GRAFICA
    2. Agregar su ruta de template en RUTAS_GRAFICA
    3. Incluirla en el área correspondiente dentro de AREAS_GRAFICAS
    4. Crear el archivo HTML en dashboard/templates/charts/{area}/
    5. Crear el modelo de datos en dashboard/model/{area}/bi_{nombre}.py
    6. Registrar el proveedor de datos en PROVEEDORES_DATOS (dashboard/router.py)

Para agregar un área nueva:
    1. Agregar su ícono en ICONOS_AREA
    2. Crear la clave en AREAS_GRAFICAS con la lista de visualizaciones

Los grupos (AREAS_GRAFICAS) y las visualizaciones dentro de cada grupo se
mantienen ordenados alfabéticamente.
"""

# ── Íconos Bootstrap por área de trabajo (orden alfabético) ──────────────────
ICONOS_AREA: dict[str, str] = {
    "EMIC":          "bi-building",
    "Gerenciales":   "bi-graph-up-arrow",
    "Ingresos":      "bi-cash-stack",
    "Kilómetros":    "bi-speedometer2",
    "Operacionales": "bi-display",
    "Operadores":    "bi-person-badge",
    "Pasajeros":     "bi-people-fill",
}

# ── Íconos Bootstrap por nombre de visualización ──────────────────────────────
ICONOS_GRAFICA: dict[str, str] = {
    # EMIC
    "DPV": "bi-journal-check",
    "ICO": "bi-clipboard-data",
    "ICS": "bi-shield-check",
    "IRI": "bi-signpost-2-fill",
    "ISV": "bi-eye-fill",
    # Gerenciales
    "Gestión COP":         "bi-diagram-3",
    "Gestión Operacional": "bi-gear-fill",
    # Ingresos
    "Benchmarking SITP": "bi-bar-chart-steps",
    "Ingresos CEXP":     "bi-cash-coin",
    # Kilómetros
    "Kilómetros Comercial":                  "bi-signpost-split-fill",
    "Kilómetros Eliminados y No Ejecutados": "bi-eraser-fill",
    "Objeción SNE":                          "bi-file-earmark-bar-graph",
    # Operacionales
    "Ejecución Operacional": "bi-play-circle-fill",
    # Operadores
    "Conductas Operacionales":       "bi-exclamation-diamond-fill",
    "Disponibilidad de Operadores": "bi-person-check-fill",
    "SIGMA":                        "bi-diagram-3-fill",
    # Pasajeros
    "Demanda de Pasajeros CEXP": "bi-people-fill",
    "Validaciones SITP":         "bi-ticket-perforated-fill",
}

# ── Ruta de la plantilla HTML por nombre de visualización ─────────────────────
# Formato: "{area}/{identificador}"   (relativo a dashboard/templates/charts/)
RUTAS_GRAFICA: dict[str, str] = {
    # EMIC
    "DPV": "emic/dpv",
    "ICO": "emic/ico",
    "ICS": "emic/ics",
    "IRI": "emic/iri",
    "ISV": "emic/isv",
    # Gerenciales
    "Gestión COP":         "gerenciales/gestion_cop",
    "Gestión Operacional": "gerenciales/gestion_operacional",
    # Ingresos
    "Benchmarking SITP": "ingresos/benchmarking_sitp",
    "Ingresos CEXP":     "ingresos/ingresos_cexp",
    # Kilómetros
    "Kilómetros Comercial":                  "kilometros/kilometros_comercial",
    "Kilómetros Eliminados y No Ejecutados": "kilometros/kilometros_eliminados_no_ejecutados",
    "Objeción SNE":                          "kilometros/kilometros_objecion_sne",
    # Operacionales
    "Ejecución Operacional": "operacionales/ejecucion_operacional",
    # Operadores
    "Conductas Operacionales":       "operadores/conductas_operacionales",
    "Disponibilidad de Operadores": "operadores/disponibilidad_operadores",
    "SIGMA":                        "operadores/sigma",
    # Pasajeros
    "Demanda de Pasajeros CEXP": "pasajeros/demanda_pasajeros_cexp",
    "Validaciones SITP":         "pasajeros/validaciones_sitp",
}

# ── Agrupación de visualizaciones por área (orden del menú) ──────────────────
# Grupos y visualizaciones ordenados alfabéticamente.
AREAS_GRAFICAS: dict[str, list[str]] = {
    "EMIC": [
        "DPV",
        "ICO",
        "ICS",
        "IRI",
        "ISV",
    ],
    "Gerenciales": [
        "Gestión COP",
        "Gestión Operacional",
    ],
    "Ingresos": [
        "Benchmarking SITP",
        "Ingresos CEXP",
    ],
    "Kilómetros": [
        "Kilómetros Comercial",
        "Kilómetros Eliminados y No Ejecutados",
        "Objeción SNE",
    ],
    "Operacionales": [
        "Ejecución Operacional",
    ],
    "Operadores": [
        "Conductas Operacionales",
        "Disponibilidad de Operadores",
        "SIGMA",
    ],
    "Pasajeros": [
        "Demanda de Pasajeros CEXP",
        "Validaciones SITP",
    ],
}


def construir_menu() -> dict:
    """
    Construye el diccionario MENU_BI para el template Jinja2
    a partir de los diccionarios planos de configuración.
    """
    return {
        area: {
            "icono": ICONOS_AREA.get(area, "bi-folder2"),
            "graficas": [
                {
                    "nombre": nombre,
                    "ruta":   RUTAS_GRAFICA[nombre],
                    "icono":  ICONOS_GRAFICA.get(nombre, "bi-bar-chart"),
                }
                for nombre in graficas
                if nombre in RUTAS_GRAFICA
            ],
        }
        for area, graficas in AREAS_GRAFICAS.items()
        if graficas
    }
