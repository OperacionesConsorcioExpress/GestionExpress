"""Backend inicial optimizado para Kilómetros Eliminados y No Ejecutados.

Fuente única de datos: pl_k01_kilometros.vw_km_eliminado.

Contrato del endpoint BI HUB:
- metadata: fechas reales de la vista y periodo consultado.
- filters: filtros aplicados y catálogos disponibles para el periodo activo.
- kpis: ocho KPI globales compatibles con la plantilla reutilizable.
- sections: las 16 tarjetas de las secciones 2 a 5, cada una con su opción ECharts.
- detail: kilómetros ajustados consolidados por día, ruta, origen, motivo,
  responsable y tipo de día, ordenados por impacto (sección 6).

Flujo de filtros:
1. Se consulta primero MAX(fecha) y MAX(fecha_hora_carga).
2. Si no llegan fechas, se usa el mes de la fecha máxima disponible.
3. El helper build_where_clause centraliza WHERE, parámetros y filtros activos.
4. Los filtros globales viajan al backend; los estados locales de gráficas
   permanecen en el navegador y no generan SQL adicional.

Cada sección analítica se resuelve en una sola consulta agrupada sobre el mismo
WHERE del período activo, para no multiplicar los viajes a la base de datos.

Puntos futuros: paginación real del detalle (requiere habilitar el paginador de
la plantilla, hoy estático) y exportación XLSX.
"""

from __future__ import annotations
from collections import OrderedDict
from datetime import date, datetime, timedelta
from decimal import Decimal
import copy
import json
import logging
import threading
import time
from typing import Any
from fastapi import HTTPException
from dashboard.database.database_manager import get_plata_connection
from dashboard.database.refresco_bi import registrar_materializada

logger = logging.getLogger(__name__)

# Capa de consumo del dashboard (DDL en dashboard/database/bi_km_eliminados.sql):
# la materializada bi_km_eliminados (hechos + ruta ya resuelta) unida a
# dim_calendario al vuelo. Sustituye a vw_km_eliminado, cuyo LEFT JOIN LATERAL
# sobre dim_rutas se ejecutaba una vez por fila en cada consulta.
FUENTE = "pl_k01_kilometros.vw_bi_km_eliminados"
MATERIALIZADA = "pl_k01_kilometros.bi_km_eliminados"

# METRICA es el nombre de la columna en la base; ETIQUETA_METRICA es cómo se
# nombra en pantalla. Se separan a propósito: renombrar la columna obligaría a
# tocar la materializada y sus índices, mientras que el rótulo visible puede
# cambiar sin afectar los datos.
METRICA = "km_eliminado_ajustado"
ETIQUETA_METRICA = "Km eliminado no ejecutado"
CONSULTAS_CARGA_INICIAL = 3
CONSULTAS_CARGA_FILTRO = 1
PERIOD_FILTER_FIELDS = {
    "anio",
    "numero_mes",
    "semana_iso",
    "trimestre",
    "semestre",
    "numero_dia_semana",
}

KPI_KEYS = {
    "indicador": "Indicador principal",
    "volumen": "Volumen total",
    "variacion": "Variación",
    "promedio": "Promedio",
    "impacto": "Mayor impacto",
    "segmento": "Segmento principal",
    "causa": "Causa principal",
    "responsable": "Responsable principal",
}

FILTER_SPECS: dict[str, tuple[str, str]] = {
    "anio": ("anio", "int"),
    "numero_mes": ("numero_mes", "int"),
    "semana_iso": ("semana_iso", "int"),
    "trimestre": ("trimestre", "int"),
    "semestre": ("semestre", "int"),
    "numero_dia_semana": ("numero_dia_semana", "int"),
    "cop": ("cop", "text"),
    "concesion": ("concesion", "text"),
    "zona": ("zona", "text"),
    "componente": ("componente", "text"),
    "id_linea": ("id_linea::text", "text"),
    "ruta": ("ruta", "text"),
    # 'servicio' se retiró de la capa de datos: 41.989 valores distintos hacían
    # inutilizable su desplegable y ninguna gráfica ni el detalle lo usaban.
    "sentido": ("sentido", "text"),
    "origen_km_no_realizados": ("origen_km_no_realizados", "text"),
    "estado_motivo_eliminacion": ("estado_motivo_eliminacion", "text"),
    "eliminado": ("eliminado", "text"),
    "descripcion_motivo_eliminacion": ("descripcion_motivo_eliminacion", "text"),
    "responsable": ("responsable", "text"),
    "grupo": ("grupo", "text"),
    "tipo_dia_calendario": ("tipo_dia_calendario", "text"),
    "tipo_dia_operativo": ("tipo_dia_operativo", "text"),
    "es_festivo": ("es_festivo", "bool"),
    "estacionalidad": ("estacionalidad", "text"),
}

OPTION_COLUMNS: dict[str, tuple[str, str, str, str]] = {
    "anio": ("anio", "anio", "int", "anio"),
    "numero_mes": ("numero_mes", "NULLIF(BTRIM(nombre_mes::text), '')", "int", "numero_mes"),
    "semana_iso": ("semana_iso", "'Semana ' || semana_iso::text", "int", "semana_iso"),
    "trimestre": ("trimestre", "'Trimestre ' || trimestre::text", "int", "trimestre"),
    "semestre": ("semestre", "'Semestre ' || semestre::text", "int", "semestre"),
    "numero_dia_semana": ("numero_dia_semana", "NULLIF(BTRIM(nombre_dia_semana::text), '')", "int", "numero_dia_semana"),
    "cop": ("cop", "cop", "text", "cop"),
    "concesion": ("concesion", "concesion", "text", "concesion"),
    "zona": ("zona", "zona", "text", "zona"),
    "componente": ("componente", "componente", "text", "componente"),
    "id_linea": ("id_linea::text", "id_linea::text", "text", "id_linea::text"),
    "ruta": ("ruta", "ruta", "text", "ruta"),
    "sentido": ("sentido", "sentido", "text", "sentido"),
    "origen_km_no_realizados": ("origen_km_no_realizados", "origen_km_no_realizados", "text", "origen_km_no_realizados"),
    "estado_motivo_eliminacion": ("estado_motivo_eliminacion", "estado_motivo_eliminacion", "text", "estado_motivo_eliminacion"),
    "eliminado": ("eliminado", "eliminado", "text", "eliminado"),
    "descripcion_motivo_eliminacion": ("descripcion_motivo_eliminacion", "descripcion_motivo_eliminacion", "text", "descripcion_motivo_eliminacion"),
    "responsable": ("responsable", "responsable", "text", "responsable"),
    "grupo": ("grupo", "grupo", "text", "grupo"),
    "tipo_dia_calendario": ("tipo_dia_calendario", "tipo_dia_calendario", "text", "tipo_dia_calendario"),
    "tipo_dia_operativo": ("tipo_dia_operativo", "tipo_dia_operativo", "text", "tipo_dia_operativo"),
    "es_festivo": ("es_festivo", "CASE WHEN es_festivo THEN 'Sí' ELSE 'No' END", "bool", "es_festivo"),
    "estacionalidad": ("estacionalidad", "estacionalidad", "text", "estacionalidad"),
}

# ── Caché en memoria ──────────────────────────────────────────────────────────
# Metadata y catálogos de filtro son estables entre cargas del ETL: recalcularlos
# en cada sesión era el mayor costo del dashboard. La clave de los catálogos
# incluye el sello de carga (fecha_hora_carga), de modo que la invalidación
# ocurre cuando entran datos nuevos y no cuando vence un temporizador arbitrario;
# el TTL queda solo como red de seguridad.

CACHE_METADATA_TTL = 60      # segundos: la metadata es barata, basta con no repetirla en ráfaga
CACHE_OPCIONES_TTL = 1800    # segundos: respaldo por si el sello de carga no cambiara
CACHE_OPCIONES_MAX = 32      # combinaciones de filtro distintas conservadas

_CACHE_LOCK = threading.RLock()
_CACHE_METADATA: dict[str, Any] = {"valor": None, "vence": 0.0}
_CACHE_OPCIONES: "OrderedDict[tuple, tuple[float, dict]]" = OrderedDict()

def _cache_metadata_leer() -> dict[str, Any] | None:
    with _CACHE_LOCK:
        if _CACHE_METADATA["valor"] is not None and time.monotonic() < _CACHE_METADATA["vence"]:
            return dict(_CACHE_METADATA["valor"])
    return None

def _cache_metadata_guardar(valor: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE_METADATA["valor"] = dict(valor)
        _CACHE_METADATA["vence"] = time.monotonic() + CACHE_METADATA_TTL

def _cache_opciones_leer(clave: tuple) -> dict[str, Any] | None:
    ahora = time.monotonic()
    with _CACHE_LOCK:
        entrada = _CACHE_OPCIONES.get(clave)
        if entrada is None:
            return None
        vence, valor = entrada
        if vence <= ahora:
            _CACHE_OPCIONES.pop(clave, None)
            return None
        _CACHE_OPCIONES.move_to_end(clave)  # LRU: lo recién usado sobrevive
        return copy.deepcopy(valor)

def _cache_opciones_guardar(clave: tuple, valor: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE_OPCIONES[clave] = (time.monotonic() + CACHE_OPCIONES_TTL, copy.deepcopy(valor))
        _CACHE_OPCIONES.move_to_end(clave)
        while len(_CACHE_OPCIONES) > CACHE_OPCIONES_MAX:
            _CACHE_OPCIONES.popitem(last=False)

def limpiar_cache() -> dict[str, Any]:
    """Descarta metadata y catálogos cacheados. Útil tras un REFRESH manual."""
    with _CACHE_LOCK:
        entradas = len(_CACHE_OPCIONES)
        _CACHE_OPCIONES.clear()
        _CACHE_METADATA.update({"valor": None, "vence": 0.0})
    logger.info("KMNR caché limpiado (%s combinaciones de filtro descartadas).", entradas)
    return {"limpiado": True, "combinaciones_descartadas": entradas}

def precalentar_cache() -> None:
    """
    Deja metadata y catálogos listos antes de la primera visita.

    El bloque de catálogos cuesta ~11 s y es irreducible con la semántica actual
    (las listas reflejan el período y los filtros activos). Ejecutarlo al
    arrancar evita que sea un usuario quien lo pague. Pensado para lanzarse en
    un hilo aparte: no debe retrasar el arranque ni tumbarlo si la base falla.
    """
    try:
        inicio = time.perf_counter()
        consultar(include_options=True, modo_solicitud="initial")
        logger.info(
            "KMNR caché precalentado en %.1f s; la primera visita ya no espera.",
            time.perf_counter() - inicio,
        )
    except Exception:
        logger.warning("KMNR no se pudo precalentar el caché; se llenará en la primera visita.", exc_info=True)

def estado_cache() -> dict[str, Any]:
    """Diagnóstico del caché para el endpoint de estado."""
    with _CACHE_LOCK:
        return {
            "metadata_vigente": _CACHE_METADATA["valor"] is not None
                                and time.monotonic() < _CACHE_METADATA["vence"],
            "opciones_en_cache": len(_CACHE_OPCIONES),
            "opciones_max": CACHE_OPCIONES_MAX,
            "ttl_metadata_seg": CACHE_METADATA_TTL,
            "ttl_opciones_seg": CACHE_OPCIONES_TTL,
        }

def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "todos"

def _as_list(value: Any) -> list[str]:
    if _is_empty(value):
        return []
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = str(value).split(",")
    return [str(item).strip() for item in raw_values if not _is_empty(item)]

def _parse_date(value: Any, field: str) -> date | None:
    if _is_empty(value):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field} debe tener formato YYYY-MM-DD.") from exc

def _parse_int(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field} debe ser entero.") from exc

def _parse_bool(value: str, field: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "si", "sí", "s", "t"}:
        return True
    if normalized in {"false", "0", "no", "n", "f"}:
        return False
    raise ValueError(f"{field} debe ser booleano.")

def _normalize_filter_value(field: str, kind: str, raw: Any) -> Any:
    values = _as_list(raw)
    if not values:
        return None
    normalized: list[Any] = []
    for item in values:
        if kind == "int":
            normalized.append(_parse_int(item, field))
        elif kind == "bool":
            normalized.append(_parse_bool(item, field))
        else:
            normalized.append(item)
    return normalized if len(normalized) > 1 else normalized[0]

def _period_from_metadata(max_fecha: date | None, parametros: dict[str, Any]) -> tuple[date | None, date | None]:
    default_end = max_fecha
    default_start = default_end - timedelta(days=30) if default_end else None
    fecha_inicio = _parse_date(parametros.get("fecha_inicio"), "fecha_inicio") or default_start
    fecha_fin = _parse_date(parametros.get("fecha_fin"), "fecha_fin") or default_end
    if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
        raise ValueError("fecha_inicio no puede ser mayor que fecha_fin.")
    return fecha_inicio, fecha_fin

def _previous_period(fecha_inicio: date | None, fecha_fin: date | None) -> tuple[date | None, date | None]:
    if not fecha_inicio or not fecha_fin:
        return None, None
    duration = fecha_fin - fecha_inicio
    previous_end = fecha_inicio - timedelta(days=1)
    previous_start = previous_end - duration
    return previous_start, previous_end

def _normalize_filters(parametros: dict[str, Any], max_fecha: date | None) -> dict[str, Any]:
    fecha_inicio, fecha_fin = _period_from_metadata(max_fecha, parametros)
    filters: dict[str, Any] = {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
    for field, (_column, kind) in FILTER_SPECS.items():
        value = _normalize_filter_value(field, kind, parametros.get(field))
        if value is not None:
            filters[field] = value
    return filters

def _public_filters(filters: dict[str, Any]) -> dict[str, Any]:
    public: dict[str, Any] = {}
    for key, value in filters.items():
        if isinstance(value, date):
            public[key] = value.isoformat()
        else:
            public[key] = value
    return public

def build_where_clause(
    filters: dict[str, Any],
    *,
    fecha_inicio: date | None = None,
    fecha_fin: date | None = None,
    exclude_fields: set[str] | None = None,
) -> tuple[str, list[Any], dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    active: dict[str, Any] = {}

    start = fecha_inicio if fecha_inicio is not None else filters.get("fecha_inicio")
    end = fecha_fin if fecha_fin is not None else filters.get("fecha_fin")
    if start is not None:
        clauses.append("fecha >= %s")
        params.append(start)
        active["fecha_inicio"] = start.isoformat()
    if end is not None:
        clauses.append("fecha <= %s")
        params.append(end)
        active["fecha_fin"] = end.isoformat()

    excluded = exclude_fields or set()
    for field, (column, _kind) in FILTER_SPECS.items():
        if field in excluded:
            continue
        value = filters.get(field)
        if value is None:
            continue
        if isinstance(value, list):
            clauses.append(f"{column} = ANY(%s)")
            params.append(value)
        else:
            clauses.append(f"{column} = %s")
            params.append(value)
        active[field] = value

    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params, active

def _row_to_dict(cursor: Any, row: tuple[Any, ...] | None) -> dict[str, Any]:
    if row is None:
        return {}
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))

def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value

def _run_one(cursor: Any, block: str, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
    start = time.perf_counter()
    cursor.execute(sql, params or [])
    row = _row_to_dict(cursor, cursor.fetchone())
    logger.info("KMNR bloque %s finalizado en %.1f ms", block, (time.perf_counter() - start) * 1000)
    return _json_value(row)

def _option_predicate(expression: str, kind: str) -> str:
    if kind == "text":
        normalized = f"NULLIF(BTRIM({expression}::text), '')"
        return f"{normalized} IS NOT NULL AND UPPER({normalized}) <> 'SIN REGISTRO FMS'"
    return f"{expression} IS NOT NULL"

def _options_select(key: str, value_expression: str, label_expression: str, kind: str, order_expression: str) -> str:
    value_predicate = _option_predicate(value_expression, kind)
    label_predicate = _option_predicate(label_expression, "text") if kind != "bool" else f"{label_expression} IS NOT NULL"
    return (
        f"'{key}', (SELECT COALESCE(jsonb_agg(jsonb_build_object('value', value, 'label', label, 'registros', registros) "
        f"ORDER BY orden NULLS LAST, label), '[]'::jsonb) "
        f"FROM (SELECT {value_expression} AS value, {label_expression} AS label, {order_expression} AS orden, COUNT(*) AS registros "
        f"FROM base WHERE {value_predicate} AND {label_predicate} "
        f"GROUP BY {value_expression}, {label_expression}, {order_expression}) opt)"
    )

def _metadata(cursor: Any) -> dict[str, Any]:
    en_cache = _cache_metadata_leer()
    if en_cache is not None:
        logger.info("KMNR bloque metadata servido desde caché")
        return en_cache

    # Cada MAX va en su propia subconsulta para que el planificador resuelva
    # ambos por Index Only Scan; en una sola pasada recorrería la relación.
    sql = f"""
        SELECT
            (SELECT MAX(fecha)::date FROM {FUENTE}) AS fecha_maxima_datos,
            (SELECT MAX(fecha_hora_carga) FROM {FUENTE}) AS fecha_hora_carga
    """
    resultado = _run_one(cursor, "metadata", sql)
    _cache_metadata_guardar(resultado)
    return resultado

def _filter_options(cursor: Any, where: str, params: list[Any], sello_carga: Any = None) -> dict[str, Any]:
    """
    Catálogos de las listas de filtro. Es el bloque más caro del dashboard, y su
    resultado solo cambia cuando entran datos nuevos: se cachea usando el sello
    de carga como parte de la clave.
    """
    clave = (str(sello_carga), where, repr(params))
    en_cache = _cache_opciones_leer(clave)
    if en_cache is not None:
        logger.info("KMNR bloque filtros servido desde caché")
        return en_cache

    # La jerarquía de organización viaja junto a los catálogos para que el
    # navegador pueda encadenar las listas (componente -> concesión -> zona ->
    # COP) sin volver a consultar en cada selección. Son pocas combinaciones
    # reales, así que sale gratis frente al costo de los catálogos.
    entries = ",\n            ".join(
        _options_select(key, value_expression, label_expression, kind, order_expression)
        for key, (value_expression, label_expression, kind, order_expression) in OPTION_COLUMNS.items()
    )
    sql = f"""
        WITH base AS (
            SELECT
                anio, numero_mes, nombre_mes, semana_iso, trimestre, semestre, numero_dia_semana, nombre_dia_semana,
                cop, concesion, zona, componente, id_linea, ruta, sentido,
                origen_km_no_realizados, estado_motivo_eliminacion, eliminado,
                descripcion_motivo_eliminacion, responsable, grupo,
                tipo_dia_calendario, tipo_dia_operativo, es_festivo, estacionalidad
            FROM {FUENTE}
            {where}
        )
        SELECT jsonb_build_object(
            {entries}
        ) AS options,
        COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'componente', componente, 'concesion', concesion, 'zona', zona, 'cop', cop))
            FROM (
                SELECT DISTINCT
                    NULLIF(BTRIM(componente::text), '') AS componente,
                    NULLIF(BTRIM(concesion::text), '')  AS concesion,
                    NULLIF(BTRIM(zona::text), '')       AS zona,
                    NULLIF(BTRIM(cop::text), '')        AS cop
                FROM base
                WHERE componente IS NOT NULL OR concesion IS NOT NULL
                   OR zona IS NOT NULL OR cop IS NOT NULL
            ) j
        ), '[]'::jsonb) AS jerarquia_organizacion
    """
    fila = _run_one(cursor, "filtros", sql, params)
    result = fila.get("options") or {}
    if isinstance(result, str):
        result = json.loads(result)
    jerarquia = fila.get("jerarquia_organizacion") or []
    if isinstance(jerarquia, str):
        jerarquia = json.loads(jerarquia)
    result = dict(result)
    result["_jerarquia_organizacion"] = jerarquia
    _cache_opciones_guardar(clave, result)
    return result

def _solo_opciones(parametros: dict[str, Any]) -> bool:
    """
    Modo ligero: devuelve únicamente los catálogos de filtro, sin calcular las
    tarjetas ni el detalle.

    Sirve para que las listas del panel reflejen los datos realmente disponibles
    tras aplicar un filtro, sin retrasar la respuesta principal: el navegador
    pide primero los datos (rápido) y luego refresca las listas en segundo plano.
    """
    raw = parametros.get("solo_opciones")
    if raw is None:
        return False
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "t", "yes", "y", "si", "sí"}

def _include_options(parametros: dict[str, Any]) -> bool:
    raw = parametros.get("include_options")
    if raw is None:
        return True
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "t", "yes", "y", "si", "sí"}

def _request_mode(parametros: dict[str, Any], include_options: bool) -> str:
    mode = str(parametros.get("modo_solicitud") or "").strip().lower()
    if mode in {"initial", "filter", "refresh"}:
        return mode
    return "initial" if include_options else "filter"

def _has_request_dates(parametros: dict[str, Any]) -> bool:
    return not _is_empty(parametros.get("fecha_inicio")) and not _is_empty(parametros.get("fecha_fin"))

def _aggregate_data(
    cursor: Any,
    filters: dict[str, Any],
    where_actual: str,
    params_actual: list[Any],
    previous_start: date | None,
    previous_end: date | None,
) -> dict[str, Any]:
    if previous_start and previous_end:
        where_previous, params_previous, _ = build_where_clause(
            filters,
            fecha_inicio=previous_start,
            fecha_fin=previous_end,
            exclude_fields=PERIOD_FILTER_FIELDS,
        )
        previous_cte = f"""
            previous_base AS (
                SELECT fecha::date AS fecha, {METRICA}
                FROM {FUENTE}
                {where_previous}
            ),
        """
        params = [*params_actual, *params_previous]
    else:
        previous_cte = f"previous_base AS (SELECT NULL::date AS fecha, 0::numeric AS {METRICA} WHERE FALSE),"
        params = params_actual

    sql = f"""
        WITH actual_base AS (
            SELECT
                fecha::date AS fecha,
                id_linea::text AS id_linea,
                NULLIF(BTRIM(ruta::text), '') AS ruta,
                NULLIF(BTRIM(origen_km_no_realizados::text), '') AS origen,
                NULLIF(BTRIM(descripcion_motivo_eliminacion::text), '') AS causa,
                NULLIF(BTRIM(responsable::text), '') AS responsable,
                origen_km_no_realizados,
                {METRICA}
            FROM {FUENTE}
            {where_actual}
        ),
        {previous_cte}
        total AS (
            SELECT COALESCE(SUM({METRICA}), 0) AS total_km FROM actual_base
        ),
        resumen AS (
            SELECT
                COALESCE((SELECT SUM({METRICA}) FROM actual_base), 0) AS km_actual,
                (SELECT COUNT(*) FROM actual_base) AS registros_actual,
                (SELECT AVG({METRICA}) FROM actual_base) AS promedio_actual,
                COALESCE((SELECT SUM({METRICA}) FROM previous_base), 0) AS km_anterior
        ),
        actual_diario AS (
            SELECT
                fecha,
                COALESCE(SUM({METRICA}), 0) AS total_km_eliminado_ajustado,
                COUNT(*) AS total_registros,
                COALESCE(SUM({METRICA}) FILTER (WHERE LOWER(origen_km_no_realizados) LIKE '%%fms%%'), 0) AS km_eliminacion_fms,
                COALESCE(SUM({METRICA}) FILTER (WHERE LOWER(origen_km_no_realizados) LIKE '%%no ejecutado%%'), 0) AS km_no_ejecutado
            FROM actual_base
            GROUP BY fecha
        ),
        anterior_diario AS (
            SELECT
                fecha,
                COALESCE(SUM({METRICA}), 0) AS total_km_eliminado_ajustado,
                COUNT(*) AS total_registros
            FROM previous_base
            GROUP BY fecha
        ),
        comparacion_base AS (
            SELECT 'actual' AS periodo, fecha, total_km_eliminado_ajustado, total_registros FROM actual_diario
            UNION ALL
            SELECT 'anterior' AS periodo, fecha, total_km_eliminado_ajustado, total_registros FROM anterior_diario
        ),
        comparacion AS (
            SELECT
                periodo,
                fecha,
                ROW_NUMBER() OVER (PARTITION BY periodo ORDER BY fecha) AS posicion_periodo,
                total_km_eliminado_ajustado,
                total_registros
            FROM comparacion_base
        )
        SELECT
            (
                SELECT jsonb_build_object(
                    'km_actual', km_actual,
                    'registros_actual', registros_actual,
                    'promedio_actual', promedio_actual,
                    'km_anterior', km_anterior
                )
                FROM resumen
            ) AS aggregates,
            jsonb_build_object(
                'mayor_impacto', (
                    SELECT jsonb_build_object(
                        'categoria', origen,
                        'km', SUM({METRICA}),
                        'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                    )
                    FROM actual_base
                    WHERE origen IS NOT NULL AND LOWER(origen) <> 'sin clasificación'
                    GROUP BY origen
                    ORDER BY SUM({METRICA}) DESC, origen ASC
                    LIMIT 1
                ),
                'segmento_principal', (
                    SELECT jsonb_build_object(
                        'id_linea', id_linea,
                        'ruta', ruta,
                        'km', SUM({METRICA}),
                        'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                    )
                    FROM actual_base
                    WHERE id_linea IS NOT NULL OR ruta IS NOT NULL
                    GROUP BY id_linea, ruta
                    ORDER BY SUM({METRICA}) DESC, ruta ASC NULLS LAST, id_linea ASC NULLS LAST
                    LIMIT 1
                ),
                'causa_principal', (
                    SELECT jsonb_build_object(
                        'categoria', causa,
                        'km', SUM({METRICA}),
                        'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                    )
                    FROM actual_base
                    WHERE causa IS NOT NULL
                    GROUP BY causa
                    ORDER BY SUM({METRICA}) DESC, causa ASC
                    LIMIT 1
                ),
                'responsable_principal', (
                    SELECT jsonb_build_object(
                        'categoria', responsable,
                        'km', SUM({METRICA}),
                        'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                    )
                    FROM actual_base
                    WHERE responsable IS NOT NULL
                    GROUP BY responsable
                    ORDER BY SUM({METRICA}) DESC, responsable ASC
                    LIMIT 1
                )
            ) AS leaders,
            COALESCE((SELECT jsonb_agg(to_jsonb(actual_diario) ORDER BY fecha) FROM actual_diario), '[]'::jsonb) AS evolucion_principal,
            COALESCE((SELECT jsonb_agg(to_jsonb(comparacion) ORDER BY posicion_periodo, periodo) FROM comparacion), '[]'::jsonb) AS comparacion_temporal
    """
    row = _run_one(cursor, "agregado_principal", sql, params)
    aggregates = row.get("aggregates") or {}
    previous = aggregates.get("km_anterior") or 0
    current = aggregates.get("km_actual") or 0
    aggregates["variacion_pct"] = None if previous == 0 else ((current - previous) * 100 / previous)
    return {
        "aggregates": aggregates,
        "leaders": row.get("leaders") or {},
        "section_data": {
            "evolucion_principal": row.get("evolucion_principal") or [],
            "comparacion_temporal": row.get("comparacion_temporal") or [],
        },
    }

def _kpi_aggregates(
    cursor: Any,
    filters: dict[str, Any],
    previous_start: date | None,
    previous_end: date | None,
) -> dict[str, Any]:
    where_actual, params_actual, _active = build_where_clause(filters)
    if previous_start and previous_end:
        where_previous, params_previous, _ = build_where_clause(
            filters,
            fecha_inicio=previous_start,
            fecha_fin=previous_end,
            exclude_fields=PERIOD_FILTER_FIELDS,
        )
        previous_sql = f"""
            UNION ALL
            SELECT 'anterior' AS periodo, {METRICA}
            FROM {FUENTE}
            {where_previous}
        """
        params = [*params_actual, *params_previous]
    else:
        previous_sql = ""
        params = params_actual

    sql = f"""
        WITH datos AS (
            SELECT 'actual' AS periodo, {METRICA}
            FROM {FUENTE}
            {where_actual}
            {previous_sql}
        )
        SELECT
            COALESCE(SUM({METRICA}) FILTER (WHERE periodo = 'actual'), 0) AS km_actual,
            COUNT(*) FILTER (WHERE periodo = 'actual') AS registros_actual,
            AVG({METRICA}) FILTER (WHERE periodo = 'actual') AS promedio_actual,
            COALESCE(SUM({METRICA}) FILTER (WHERE periodo = 'anterior'), 0) AS km_anterior
        FROM datos
    """
    row = _run_one(cursor, "kpis", sql, params)
    current = row.get("km_actual") or 0
    previous = row.get("km_anterior") or 0
    row["variacion_pct"] = None if previous == 0 else ((current - previous) * 100 / previous)
    return row

def _kpi_leaders(cursor: Any, where: str, params: list[Any]) -> dict[str, Any]:
    sql = f"""
        WITH base AS (
            SELECT
                id_linea::text AS id_linea,
                NULLIF(BTRIM(ruta::text), '') AS ruta,
                NULLIF(BTRIM(origen_km_no_realizados::text), '') AS origen,
                NULLIF(BTRIM(descripcion_motivo_eliminacion::text), '') AS causa,
                NULLIF(BTRIM(responsable::text), '') AS responsable,
                {METRICA}
            FROM {FUENTE}
            {where}
        ), total AS (
            SELECT COALESCE(SUM({METRICA}), 0) AS total_km FROM base
        )
        SELECT
            (
                SELECT jsonb_build_object(
                    'categoria', origen,
                    'km', SUM({METRICA}),
                    'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                )
                FROM base
                WHERE origen IS NOT NULL AND LOWER(origen) <> 'sin clasificación'
                GROUP BY origen
                ORDER BY SUM({METRICA}) DESC, origen ASC
                LIMIT 1
            ) AS mayor_impacto,
            (
                SELECT jsonb_build_object(
                    'id_linea', id_linea,
                    'ruta', ruta,
                    'km', SUM({METRICA}),
                    'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                )
                FROM base
                WHERE id_linea IS NOT NULL OR ruta IS NOT NULL
                GROUP BY id_linea, ruta
                ORDER BY SUM({METRICA}) DESC, ruta ASC NULLS LAST, id_linea ASC NULLS LAST
                LIMIT 1
            ) AS segmento_principal,
            (
                SELECT jsonb_build_object(
                    'categoria', causa,
                    'km', SUM({METRICA}),
                    'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                )
                FROM base
                WHERE causa IS NOT NULL
                GROUP BY causa
                ORDER BY SUM({METRICA}) DESC, causa ASC
                LIMIT 1
            ) AS causa_principal,
            (
                SELECT jsonb_build_object(
                    'categoria', responsable,
                    'km', SUM({METRICA}),
                    'participacion_pct', ROUND((100.0 * SUM({METRICA}) / NULLIF((SELECT total_km FROM total), 0))::numeric, 2)
                )
                FROM base
                WHERE responsable IS NOT NULL
                GROUP BY responsable
                ORDER BY SUM({METRICA}) DESC, responsable ASC
                LIMIT 1
            ) AS responsable_principal
    """
    return _run_one(cursor, "kpis_lideres", sql, params)

def _section_evolucion(
    cursor: Any,
    filters: dict[str, Any],
    previous_start: date | None,
    previous_end: date | None,
) -> dict[str, list[dict[str, Any]]]:
    where_actual, params_actual, _active = build_where_clause(filters)
    if previous_start and previous_end:
        where_previous, params_previous, _ = build_where_clause(
            filters,
            fecha_inicio=previous_start,
            fecha_fin=previous_end,
            exclude_fields=PERIOD_FILTER_FIELDS,
        )
        previous_cte = f"""
            anterior AS (
                SELECT
                    fecha::date AS fecha,
                    COALESCE(SUM({METRICA}), 0) AS total_km_eliminado_ajustado,
                    COUNT(*) AS total_registros
                FROM {FUENTE}
                {where_previous}
                GROUP BY fecha::date
            ),
        """
        previous_union = "SELECT 'anterior' AS periodo, fecha, total_km_eliminado_ajustado, total_registros FROM anterior"
        params = [*params_actual, *params_previous]
    else:
        previous_cte = "anterior AS (SELECT NULL::date AS fecha, 0::numeric AS total_km_eliminado_ajustado, 0::bigint AS total_registros WHERE FALSE),"
        previous_union = "SELECT 'anterior' AS periodo, fecha, total_km_eliminado_ajustado, total_registros FROM anterior"
        params = params_actual

    sql = f"""
        WITH actual AS (
            SELECT
                fecha::date AS fecha,
                COALESCE(SUM({METRICA}), 0) AS total_km_eliminado_ajustado,
                COUNT(*) AS total_registros,
                COALESCE(SUM({METRICA}) FILTER (WHERE LOWER(origen_km_no_realizados) LIKE '%%fms%%'), 0) AS km_eliminacion_fms,
                COALESCE(SUM({METRICA}) FILTER (WHERE LOWER(origen_km_no_realizados) LIKE '%%no ejecutado%%'), 0) AS km_no_ejecutado
            FROM {FUENTE}
            {where_actual}
            GROUP BY fecha::date
        ),
        {previous_cte}
        comparacion_base AS (
            SELECT 'actual' AS periodo, fecha, total_km_eliminado_ajustado, total_registros FROM actual
            UNION ALL
            {previous_union}
        ),
        comparacion AS (
            SELECT
                periodo,
                fecha,
                ROW_NUMBER() OVER (PARTITION BY periodo ORDER BY fecha) AS posicion_periodo,
                total_km_eliminado_ajustado,
                total_registros
            FROM comparacion_base
        )
        SELECT
            COALESCE((SELECT jsonb_agg(to_jsonb(actual) ORDER BY fecha) FROM actual), '[]'::jsonb) AS evolucion_principal,
            COALESCE((SELECT jsonb_agg(to_jsonb(comparacion) ORDER BY posicion_periodo, periodo) FROM comparacion), '[]'::jsonb) AS comparacion_temporal
    """
    row = _run_one(cursor, "seccion_inicial", sql, params)
    return {
        "evolucion_principal": row.get("evolucion_principal") or [],
        "comparacion_temporal": row.get("comparacion_temporal") or [],
    }

# ── Bloques analíticos por sección ────────────────────────────────────────────
# Cada bloque resuelve las cuatro tarjetas de su sección en una sola consulta,
# reutilizando el mismo WHERE del período activo. Las columnas derivadas de la
# vista (anio, numero_mes, tipo_dia_operativo, origen_km_no_realizados) evitan
# recalcular festivos y clasificaciones en Python.

TOP_CATEGORIAS = 15
TOP_RUTAS_ANALITICA = 20

# Los registros sin dimensión de ruta se excluyen de los análisis por ruta
# (ranking, Pareto y diagnóstico): no representan una ruta y desplazarían a las
# reales del top, además de sesgar los cuartiles de criticidad. En motivos,
# grupos, COP y componente sí se conservan, porque allí "Sin registro" es una
# categoría legítima del negocio.
RUTA_UTIL = "ruta IS NOT NULL AND BTRIM(ruta::text) <> '' AND UPPER(BTRIM(ruta::text)) <> 'SIN REGISTRO FMS'"

# ── Tabla de detalle (sección 6) ──────────────────────────────────────────────
# La tabla consolida los kilómetros ajustados por día, ruta, origen, motivo,
# responsable y tipo de día. Se entrega un conjunto acotado y ordenado por
# impacto porque el paginador de la plantilla está deshabilitado en el HTML
# ("Anterior"/"Siguiente" fijos): recibe y pinta de una vez todas las filas.
LIMITE_DETALLE_DEFAULT = 200
LIMITE_DETALLE_MAXIMO = 1000

# Dimensiones de agrupación: (clave, etiqueta, expresión SQL).
DETALLE_DIMENSIONES: list[tuple[str, str, str]] = [
    ("fecha", "Fecha", "fecha::date"),
    ("ruta", "Ruta", "COALESCE(NULLIF(BTRIM(ruta::text), ''), 'Sin registro')"),
    ("origen_km_no_realizados", "Origen", "COALESCE(NULLIF(BTRIM(origen_km_no_realizados::text), ''), 'Sin clasificar')"),
    ("descripcion_motivo_eliminacion", "Motivo", "COALESCE(NULLIF(BTRIM(descripcion_motivo_eliminacion::text), ''), 'Sin registro')"),
    ("responsable", "Responsable", "COALESCE(NULLIF(BTRIM(responsable::text), ''), 'Sin registro')"),
    ("tipo_dia_operativo", "Tipo de día", "COALESCE(NULLIF(BTRIM(tipo_dia_operativo::text), ''), 'Sin clasificar')"),
]

# Medidas calculadas sobre cada grupo.
DETALLE_MEDIDAS: list[tuple[str, str, str]] = [
    ("total_registros", "Registros", "COUNT(*)"),
    ("km_eliminado_ajustado", ETIQUETA_METRICA, f"ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2)"),
]

DETALLE_COLUMNAS: list[tuple[str, str]] = [
    *[(clave, etiqueta) for clave, etiqueta, _ in DETALLE_DIMENSIONES],
    *[(clave, etiqueta) for clave, etiqueta, _ in DETALLE_MEDIDAS],
]

def _limite_detalle(parametros: dict[str, Any]) -> int:
    bruto = parametros.get("limite_detalle")
    if _is_empty(bruto):
        return LIMITE_DETALLE_DEFAULT
    try:
        valor = int(str(bruto).strip())
    except ValueError as exc:
        raise ValueError("limite_detalle debe ser entero.") from exc
    return max(1, min(valor, LIMITE_DETALLE_MAXIMO))

def _section_detalle(cursor: Any, where: str, params: list[Any], limite: int) -> dict[str, Any]:
    """Kilómetros ajustados consolidados por día, ruta, origen, motivo, responsable y tipo de día."""
    dimensiones = ",\n                   ".join(
        f"{expresion} AS {clave}" for clave, _etiqueta, expresion in DETALLE_DIMENSIONES
    )
    medidas = ",\n                   ".join(
        f"{expresion} AS {clave}" for clave, _etiqueta, expresion in DETALLE_MEDIDAS
    )
    # Se agrupa por la expresión, no por el alias: PostgreSQL no admite alias de
    # la lista SELECT dentro de GROUP BY cuando la expresión no es una columna.
    agrupacion = ", ".join(expresion for _clave, _etiqueta, expresion in DETALLE_DIMENSIONES)

    sql = f"""
        WITH agrupado AS (
            SELECT {dimensiones},
                   {medidas},
                   COALESCE(SUM({METRICA}), 0) AS km_sin_redondear
            FROM {FUENTE}
            {where}
            GROUP BY {agrupacion}
        ),
        acotado AS (
            SELECT *,
                   COUNT(*) OVER () AS total_grupos,
                   SUM(total_registros) OVER () AS registros_consolidados,
                   -- El total se calcula sobre los valores sin redondear: sumar
                   -- los ya redondeados de miles de grupos acumula error y deja
                   -- de cuadrar con el KPI del período.
                   ROUND(SUM(km_sin_redondear) OVER ()::numeric, 2) AS km_totales
            FROM agrupado
            ORDER BY km_sin_redondear DESC NULLS LAST, fecha DESC
            LIMIT %s
        )
        SELECT
            COALESCE(MAX(total_grupos), 0) AS total_grupos,
            COALESCE(MAX(registros_consolidados), 0) AS registros_consolidados,
            COALESCE(MAX(km_totales), 0) AS km_totales,
            COALESCE(
                jsonb_agg(
                    to_jsonb(acotado) - 'total_grupos' - 'registros_consolidados'
                                      - 'km_totales' - 'km_sin_redondear'
                    ORDER BY km_sin_redondear DESC NULLS LAST, fecha DESC
                ),
                '[]'::jsonb
            ) AS filas
        FROM acotado
    """
    row = _run_one(cursor, "detalle", sql, [*params, limite])
    filas = row.get("filas") or []
    return {
        "columns": [{"key": clave, "label": etiqueta} for clave, etiqueta in DETALLE_COLUMNAS],
        "rows": filas,
        "total_filas": row.get("total_grupos") or 0,
        "filas_mostradas": len(filas),
        "registros_consolidados": row.get("registros_consolidados") or 0,
        "km_totales": row.get("km_totales") or 0,
        "limite_aplicado": limite,
        "agrupado_por": [etiqueta for _clave, etiqueta, _expresion in DETALLE_DIMENSIONES],
        "orden": "Mayor impacto en kilómetros no realizados",
    }

def _detalle_vacio() -> dict[str, Any]:
    return {
        "columns": [{"key": clave, "label": etiqueta} for clave, etiqueta in DETALLE_COLUMNAS],
        "rows": [],
        "total_filas": 0,
        "filas_mostradas": 0,
        "registros_consolidados": 0,
        "km_totales": 0,
        "limite_aplicado": LIMITE_DETALLE_DEFAULT,
        "agrupado_por": [etiqueta for _clave, etiqueta, _expresion in DETALLE_DIMENSIONES],
        "orden": "Mayor impacto en kilómetros no realizados",
    }

def _section_evolucion_extra(cursor: Any, where: str, params: list[Any]) -> dict[str, Any]:
    """Calendario diario y tendencia agregada mensual (sección 2)."""
    sql = f"""
        WITH base AS (
            SELECT fecha::date AS fecha, anio, numero_mes, nombre_mes,
                   tipo_dia_operativo, {METRICA}
            FROM {FUENTE}
            {where}
        ),
        calendario AS (
            SELECT fecha,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   COUNT(*) AS total_registros,
                   MAX(tipo_dia_operativo) AS tipo_dia
            FROM base
            GROUP BY fecha
        ),
        mensual AS (
            SELECT anio, numero_mes, MAX(nombre_mes) AS nombre_mes,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   COUNT(*) AS total_registros
            FROM base
            GROUP BY anio, numero_mes
        ),
        mensual_calc AS (
            SELECT *,
                   ROUND(SUM(km_no_realizados) OVER (ORDER BY anio, numero_mes)::numeric, 2) AS acumulado,
                   ROUND((100.0 * km_no_realizados
                          / NULLIF(SUM(km_no_realizados) OVER (), 0))::numeric, 2) AS participacion_pct
            FROM mensual
        )
        SELECT
            COALESCE((SELECT jsonb_agg(to_jsonb(calendario) ORDER BY fecha) FROM calendario), '[]'::jsonb) AS calendario,
            COALESCE((SELECT jsonb_agg(to_jsonb(mensual_calc) ORDER BY anio, numero_mes) FROM mensual_calc), '[]'::jsonb) AS mensual
    """
    row = _run_one(cursor, "seccion_evolucion_extra", sql, params)
    return {
        "calendario_estacionalidad": row.get("calendario") or [],
        "tendencia_agregada": row.get("mensual") or [],
    }

def _categoria_cte(alias: str, expresion: str, limite: int, filtro: str = "") -> str:
    """Agregado por categoría con participación y ranking, para las tarjetas top-N."""
    where = f"WHERE {filtro}" if filtro else ""
    return f"""
        {alias} AS (
            SELECT etiqueta,
                   ROUND(km::numeric, 2) AS km_no_realizados,
                   total_registros,
                   ROUND((100.0 * km / NULLIF(SUM(km) OVER (), 0))::numeric, 2) AS participacion_pct
            FROM (
                SELECT {expresion} AS etiqueta,
                       COALESCE(SUM({METRICA}), 0) AS km,
                       COUNT(*) AS total_registros
                FROM base
                {where}
                GROUP BY {expresion}
            ) agrupado
            ORDER BY km_no_realizados DESC NULLS LAST
            LIMIT {limite}
        )
    """

def _section_composicion(cursor: Any, where: str, params: list[Any]) -> dict[str, Any]:
    """Composición por origen, motivos, Pareto de rutas y distribución por grupo (sección 3)."""
    etiqueta_motivo = "COALESCE(NULLIF(BTRIM(descripcion_motivo_eliminacion::text), ''), 'Sin registro')"
    etiqueta_grupo = "COALESCE(NULLIF(BTRIM(grupo::text), ''), 'Sin registro')"
    etiqueta_ruta = "COALESCE(NULLIF(BTRIM(ruta::text), ''), 'Sin registro')"
    etiqueta_origen = "COALESCE(NULLIF(BTRIM(origen_km_no_realizados::text), ''), 'Sin clasificar')"

    sql = f"""
        WITH base AS (
            SELECT origen_km_no_realizados, descripcion_motivo_eliminacion,
                   grupo, ruta, {METRICA}
            FROM {FUENTE}
            {where}
        ),
        {_categoria_cte("origen", etiqueta_origen, 10)},
        {_categoria_cte("motivos", etiqueta_motivo, TOP_CATEGORIAS)},
        {_categoria_cte("grupos", etiqueta_grupo, TOP_CATEGORIAS)},
        rutas_orden AS (
            SELECT {etiqueta_ruta} AS etiqueta,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   COUNT(*) AS total_registros
            FROM base
            WHERE {RUTA_UTIL}
            GROUP BY {etiqueta_ruta}
        ),
        pareto AS (
            SELECT etiqueta, km_no_realizados, total_registros,
                   ROUND((100.0 * km_no_realizados
                          / NULLIF(SUM(km_no_realizados) OVER (), 0))::numeric, 2) AS participacion_pct,
                   ROUND((100.0 * SUM(km_no_realizados) OVER (ORDER BY km_no_realizados DESC
                                                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                          / NULLIF(SUM(km_no_realizados) OVER (), 0))::numeric, 2) AS acumulado_pct
            FROM rutas_orden
            ORDER BY km_no_realizados DESC NULLS LAST
            LIMIT {TOP_CATEGORIAS}
        )
        SELECT
            COALESCE((SELECT jsonb_agg(to_jsonb(origen) ORDER BY km_no_realizados DESC) FROM origen), '[]'::jsonb) AS origen,
            COALESCE((SELECT jsonb_agg(to_jsonb(motivos) ORDER BY km_no_realizados DESC) FROM motivos), '[]'::jsonb) AS motivos,
            COALESCE((SELECT jsonb_agg(to_jsonb(pareto) ORDER BY km_no_realizados DESC) FROM pareto), '[]'::jsonb) AS pareto,
            COALESCE((SELECT jsonb_agg(to_jsonb(grupos) ORDER BY km_no_realizados DESC) FROM grupos), '[]'::jsonb) AS grupos
    """
    row = _run_one(cursor, "seccion_composicion", sql, params)
    return {
        "composicion_principal": row.get("origen") or [],
        "distribucion_secundaria": row.get("motivos") or [],
        "participacion_acumulada": row.get("pareto") or [],
        "distribucion_complementaria": row.get("grupos") or [],
    }

def _section_segmentacion(cursor: Any, where: str, params: list[Any]) -> dict[str, Any]:
    """Rankings por ruta, COP y componente, más el cruce tipo de día × origen (sección 4)."""
    etiqueta_ruta = "COALESCE(NULLIF(BTRIM(ruta::text), ''), 'Sin registro')"
    etiqueta_cop = "COALESCE(NULLIF(BTRIM(cop::text), ''), 'Sin registro')"
    etiqueta_componente = "COALESCE(NULLIF(BTRIM(componente::text), ''), 'Sin registro')"

    sql = f"""
        WITH base AS (
            SELECT ruta, cop, componente, tipo_dia_operativo,
                   origen_km_no_realizados, {METRICA}
            FROM {FUENTE}
            {where}
        ),
        {_categoria_cte("rutas", etiqueta_ruta, TOP_CATEGORIAS, RUTA_UTIL)},
        {_categoria_cte("cops", etiqueta_cop, TOP_CATEGORIAS)},
        {_categoria_cte("componentes", etiqueta_componente, TOP_CATEGORIAS)},
        cruzada AS (
            SELECT COALESCE(NULLIF(BTRIM(tipo_dia_operativo::text), ''), 'Sin clasificar') AS tipo_dia,
                   COALESCE(NULLIF(BTRIM(origen_km_no_realizados::text), ''), 'Sin clasificar') AS origen,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   COUNT(*) AS total_registros
            FROM base
            GROUP BY tipo_dia, origen
        )
        SELECT
            COALESCE((SELECT jsonb_agg(to_jsonb(rutas) ORDER BY km_no_realizados DESC) FROM rutas), '[]'::jsonb) AS rutas,
            COALESCE((SELECT jsonb_agg(to_jsonb(cops) ORDER BY km_no_realizados DESC) FROM cops), '[]'::jsonb) AS cops,
            COALESCE((SELECT jsonb_agg(to_jsonb(componentes) ORDER BY km_no_realizados DESC) FROM componentes), '[]'::jsonb) AS componentes,
            COALESCE((SELECT jsonb_agg(to_jsonb(cruzada) ORDER BY tipo_dia, origen) FROM cruzada), '[]'::jsonb) AS cruzada
    """
    row = _run_one(cursor, "seccion_segmentacion", sql, params)
    return {
        "dimension_principal": row.get("rutas") or [],
        "dimension_territorial": row.get("cops") or [],
        "dimension_operativa": row.get("componentes") or [],
        "comparacion_cruzada": row.get("cruzada") or [],
    }

def _section_diagnostico(cursor: Any, where: str, params: list[Any]) -> dict[str, Any]:
    """Boxplot, dispersión, criticidad IQR y mapa de calor día × franja (sección 5)."""
    etiqueta_ruta = "COALESCE(NULLIF(BTRIM(ruta::text), ''), 'Sin registro')"
    etiqueta_franja = "COALESCE(NULLIF(BTRIM(franja_hora_real::text), ''), 'Sin registro')"

    sql = f"""
        WITH base AS (
            SELECT {etiqueta_ruta} AS ruta,
                   ({RUTA_UTIL}) AS ruta_util,
                   numero_dia_semana, nombre_dia_semana, franja_hora_real,
                   {METRICA}
            FROM {FUENTE}
            {where}
        ),
        por_ruta AS (
            SELECT ruta,
                   COUNT(*) AS total_registros,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   ROUND(AVG({METRICA})::numeric, 2) AS promedio_por_registro,
                   ROUND(MIN({METRICA})::numeric, 2) AS minimo,
                   ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY {METRICA})::numeric, 2) AS q1,
                   ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY {METRICA})::numeric, 2) AS mediana,
                   ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY {METRICA})::numeric, 2) AS q3,
                   ROUND(MAX({METRICA})::numeric, 2) AS maximo
            FROM base
            WHERE ruta_util
            GROUP BY ruta
        ),
        top_rutas AS (
            SELECT * FROM por_ruta ORDER BY km_no_realizados DESC NULLS LAST LIMIT {TOP_RUTAS_ANALITICA}
        ),
        boxplot AS (
            SELECT ruta, total_registros, km_no_realizados, minimo, q1, mediana, q3, maximo,
                   ROUND((q3 - q1)::numeric, 2) AS iqr,
                   ROUND((q3 + 1.5 * (q3 - q1))::numeric, 2) AS limite_superior
            FROM top_rutas
        ),
        umbrales AS (
            SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY km_no_realizados) AS g_q1,
                   percentile_cont(0.50) WITHIN GROUP (ORDER BY km_no_realizados) AS g_mediana,
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY km_no_realizados) AS g_q3
            FROM por_ruta
        ),
        criticidad AS (
            SELECT t.ruta, t.total_registros, t.km_no_realizados, t.promedio_por_registro,
                   CASE
                       WHEN t.km_no_realizados > u.g_q3 THEN 'Impacto crítico'
                       WHEN t.km_no_realizados > u.g_mediana THEN 'Impacto alto'
                       WHEN t.km_no_realizados > u.g_q1 THEN 'Impacto moderado'
                       ELSE 'Impacto bajo'
                   END AS categoria
            FROM top_rutas t CROSS JOIN umbrales u
        ),
        heatmap AS (
            SELECT numero_dia_semana,
                   COALESCE(NULLIF(BTRIM(nombre_dia_semana::text), ''), 'Sin registro') AS nombre_dia,
                   {etiqueta_franja} AS franja,
                   ROUND(COALESCE(SUM({METRICA}), 0)::numeric, 2) AS km_no_realizados,
                   COUNT(*) AS total_registros
            FROM base
            WHERE numero_dia_semana IS NOT NULL
            GROUP BY numero_dia_semana, nombre_dia, franja
        )
        SELECT
            COALESCE((SELECT jsonb_agg(to_jsonb(boxplot) ORDER BY km_no_realizados DESC) FROM boxplot), '[]'::jsonb) AS boxplot,
            COALESCE((SELECT jsonb_agg(to_jsonb(top_rutas) ORDER BY km_no_realizados DESC) FROM top_rutas), '[]'::jsonb) AS dispersion,
            COALESCE((SELECT jsonb_agg(to_jsonb(criticidad) ORDER BY km_no_realizados DESC) FROM criticidad), '[]'::jsonb) AS criticidad,
            COALESCE((SELECT jsonb_agg(to_jsonb(heatmap) ORDER BY numero_dia_semana, franja) FROM heatmap), '[]'::jsonb) AS heatmap
    """
    row = _run_one(cursor, "seccion_diagnostico", sql, params)
    return {
        "distribucion_estadistica": row.get("boxplot") or [],
        "relacion_variables": row.get("dispersion") or [],
        "clasificacion_criticidad": row.get("criticidad") or [],
        "mapa_calor_matriz": row.get("heatmap") or [],
    }

def _kpis_response(aggregates: dict[str, Any], leaders: dict[str, Any]) -> dict[str, dict[str, Any]]:
    km_actual = aggregates.get("km_actual") or 0
    registros = aggregates.get("registros_actual") or 0
    promedio = aggregates.get("promedio_actual")
    variacion = aggregates.get("variacion_pct")
    mayor = leaders.get("mayor_impacto") or {}
    segmento = leaders.get("segmento_principal") or {}
    causa = leaders.get("causa_principal") or {}
    responsable = leaders.get("responsable_principal") or {}

    leader_format = {"note_format": "decimal", "note_decimals": 2, "note_suffix": " km"}
    return {
        "indicador": {"value": round(float(km_actual), 2), "format": "decimal", "decimals": 2, "suffix": " km", "note": "Suma de kilómetros eliminados y no ejecutados."},
        "volumen": {"value": int(registros), "format": "integer", "decimals": 0, "note": "Cantidad total de registros agregados."},
        "variacion": {"value": None if variacion is None else round(float(variacion), 2), "format": "percent", "decimals": 2, "suffix": " %", "note": "Variación porcentual frente al período anterior equivalente."},
        "promedio": {"value": None if promedio is None else round(float(promedio), 2), "format": "decimal", "decimals": 2, "suffix": " km", "note": "Promedio de km por registro."},
        "impacto": {"value": mayor.get("categoria"), "note": mayor.get("km"), **leader_format},
        "segmento": {"value": segmento.get("ruta") or segmento.get("id_linea"), "note": segmento.get("km"), **leader_format},
        "causa": {"value": causa.get("categoria"), "note": causa.get("km"), **leader_format},
        "responsable": {"value": responsable.get("categoria"), "note": responsable.get("km"), **leader_format},
    }

LINE_COLORS = {
    "km_ajustados": "#25397A",
    "eliminacion_fms": "#16A038",
    "no_ejecutado": "#5076E2",
    "actual": "#25397A",
    "anterior": "#83A4FF",
}

def _professional_line_series(name: str, data: list[Any], color: str) -> dict[str, Any]:
    return {
        "name": name,
        "type": "line",
        "data": data,
        "smooth": 0.25,
        "showSymbol": False,
        "symbol": "circle",
        "symbolSize": 6,
        "connectNulls": False,
        "lineStyle": {"color": color, "width": 2.4},
        "itemStyle": {"color": color},
        "emphasis": {
            "focus": "series",
            "scale": True,
            "lineStyle": {"color": color, "width": 3},
            "itemStyle": {"color": color},
        },
        "endLabel": {"show": False, "color": color},
    }

def _professional_line_base(categories: list[Any]) -> dict[str, Any]:
    return {
        "backgroundColor": "transparent",
        "animation": True,
        "animationDuration": 900,
        "animationDurationUpdate": 500,
        "animationEasing": "cubicOut",
        "animationEasingUpdate": "cubicOut",
        "grid": {"left": 56, "right": 24, "top": 64, "bottom": 48, "containLabel": True},
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "line", "lineStyle": {"color": "#83A4FF", "width": 1}},
            "confine": True,
        },
        "legend": {"show": True, "top": 12, "left": "center", "icon": "roundRect"},
        "xAxis": {
            "type": "category",
            "data": categories,
            "boundaryGap": False,
            "axisPointer": {"show": True, "label": {"show": True}},
            "axisTick": {"show": False},
            "axisLine": {"lineStyle": {"color": "#edf2f8"}},
            "axisLabel": {"hideOverlap": True, "margin": 12},
        },
        "yAxis": {
            "type": "value",
            "name": "km",
            "nameGap": 14,
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "axisLabel": {"formatter": "{value} km", "margin": 12},
            "splitLine": {"lineStyle": {"color": "#edf2f8", "type": "dashed"}},
        },
        "dataZoom": [{"type": "inside", "start": 0, "end": 100, "filterMode": "none"}],
    }

def _line_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    option = _professional_line_base([row["fecha"] for row in rows])
    option["series"] = [
        _professional_line_series(ETIQUETA_METRICA, [row["total_km_eliminado_ajustado"] for row in rows], LINE_COLORS["km_ajustados"]),
        _professional_line_series("Eliminación FMS", [row["km_eliminacion_fms"] for row in rows], LINE_COLORS["eliminacion_fms"]),
        _professional_line_series("No ejecutado", [row["km_no_ejecutado"] for row in rows], LINE_COLORS["no_ejecutado"]),
    ]
    return option

def _comparison_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    current = [row for row in rows if row.get("periodo") == "actual"]
    previous = [row for row in rows if row.get("periodo") == "anterior"]
    positions = sorted({row["posicion_periodo"] for row in rows})
    categories = [f"Día {position}" for position in positions]
    by_current = {row["posicion_periodo"]: row["total_km_eliminado_ajustado"] for row in current}
    by_previous = {row["posicion_periodo"]: row["total_km_eliminado_ajustado"] for row in previous}
    option = _professional_line_base(categories)
    option["series"] = [
        _professional_line_series("Actual", [by_current.get(position, 0) for position in positions], LINE_COLORS["actual"]),
        _professional_line_series("Anterior", [by_previous.get(position, 0) for position in positions], LINE_COLORS["anterior"]),
    ]
    return option

# ── Constructores de opciones ECharts ─────────────────────────────────────────
# Comparten rejilla, tooltip y tipografía con las dos gráficas ya existentes
# para que las tarjetas nuevas no introduzcan un lenguaje visual distinto.

PALETA_CATEGORICA = ["#25397A", "#3F5DB2", "#5076E2", "#83A4FF", "#16A038", "#4FB477", "#8C6BD1", "#D98324", "#C7405E", "#0FA3B1"]
COLOR_EJE = "#edf2f8"
COLOR_CRITICIDAD = {
    "Impacto crítico": "#C7405E",
    "Impacto alto": "#D98324",
    "Impacto moderado": "#3F5DB2",
    "Impacto bajo": "#4FB477",
}

def _base_cartesiano(*, horizontal: bool = False) -> dict[str, Any]:
    eje_valor = {
        "type": "value",
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "axisLabel": {"formatter": "{value} km", "margin": 10},
        "splitLine": {"lineStyle": {"color": COLOR_EJE, "type": "dashed"}},
    }
    eje_categoria = {
        "type": "category",
        "axisTick": {"show": False},
        "axisLine": {"lineStyle": {"color": COLOR_EJE}},
        "axisLabel": {"hideOverlap": True, "margin": 10},
    }
    return {
        "backgroundColor": "transparent",
        "animation": True,
        "animationDuration": 900,
        "animationEasing": "cubicOut",
        "grid": {"left": 56, "right": 24, "top": 48, "bottom": 44, "containLabel": True},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}, "confine": True},
        "xAxis": eje_valor if horizontal else eje_categoria,
        "yAxis": eje_categoria if horizontal else eje_valor,
    }

def _serie_barra(nombre: str, datos: list[Any], color: str, *, apilada: str | None = None) -> dict[str, Any]:
    serie = {
        "name": nombre,
        "type": "bar",
        "data": datos,
        "barMaxWidth": 26,
        "itemStyle": {"color": color, "borderRadius": [3, 3, 0, 0]},
        "emphasis": {"focus": "series", "itemStyle": {"color": color}},
    }
    if apilada:
        serie["stack"] = apilada
        serie["itemStyle"]["borderRadius"] = 0
    return serie

def _barras_horizontales_option(rows: list[dict[str, Any]], *, etiqueta: str = "etiqueta", valor: str = "km_no_realizados") -> dict[str, Any] | None:
    if not rows:
        return None
    # ECharts dibuja el eje de categorías de abajo hacia arriba: se invierte el
    # orden para que el mayor impacto quede arriba.
    ordenadas = list(reversed(rows))
    option = _base_cartesiano(horizontal=True)
    option["yAxis"]["data"] = [row.get(etiqueta) for row in ordenadas]
    option["grid"]["left"] = 140
    option["series"] = [
        {
            "name": ETIQUETA_METRICA,
            "type": "bar",
            "data": [row.get(valor) for row in ordenadas],
            "barMaxWidth": 18,
            "itemStyle": {"color": PALETA_CATEGORICA[0], "borderRadius": [0, 3, 3, 0]},
            "emphasis": {"focus": "series"},
        }
    ]
    return option

def _barras_verticales_option(rows: list[dict[str, Any]], *, etiqueta: str = "etiqueta", valor: str = "km_no_realizados") -> dict[str, Any] | None:
    if not rows:
        return None
    option = _base_cartesiano()
    option["xAxis"]["data"] = [row.get(etiqueta) for row in rows]
    option["xAxis"]["axisLabel"]["rotate"] = 30 if len(rows) > 6 else 0
    option["series"] = [_serie_barra(ETIQUETA_METRICA, [row.get(valor) for row in rows], PALETA_CATEGORICA[1])]
    return option

def _dona_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "item", "confine": True, "formatter": "{b}<br/>{c} km ({d} %)"},
        "legend": {"show": True, "bottom": 4, "left": "center", "icon": "roundRect"},
        "series": [
            {
                "name": "Composición",
                "type": "pie",
                "radius": ["46%", "70%"],
                "center": ["50%", "45%"],
                "avoidLabelOverlap": True,
                "itemStyle": {"borderColor": "#fff", "borderWidth": 2},
                "label": {"show": True, "formatter": "{d} %"},
                "emphasis": {"scale": True, "label": {"show": True, "fontWeight": "bold"}},
                "data": [
                    {
                        "name": row.get("etiqueta"),
                        "value": row.get("km_no_realizados"),
                        "itemStyle": {"color": PALETA_CATEGORICA[indice % len(PALETA_CATEGORICA)]},
                    }
                    for indice, row in enumerate(rows)
                ],
            }
        ],
    }

def _pareto_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    option = _base_cartesiano()
    option["xAxis"]["data"] = [row.get("etiqueta") for row in rows]
    option["xAxis"]["axisLabel"]["rotate"] = 30
    option["legend"] = {"show": True, "top": 8, "left": "center", "icon": "roundRect"}
    option["grid"]["top"] = 56
    option["yAxis"] = [
        option["yAxis"],
        {
            "type": "value",
            "name": "%",
            "max": 100,
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "axisLabel": {"formatter": "{value} %"},
            "splitLine": {"show": False},
        },
    ]
    option["series"] = [
        _serie_barra(ETIQUETA_METRICA, [row.get("km_no_realizados") for row in rows], PALETA_CATEGORICA[0]),
        {
            "name": "Acumulado",
            "type": "line",
            "yAxisIndex": 1,
            "data": [row.get("acumulado_pct") for row in rows],
            "smooth": 0.25,
            "symbol": "circle",
            "symbolSize": 6,
            "lineStyle": {"color": PALETA_CATEGORICA[7], "width": 2.4},
            "itemStyle": {"color": PALETA_CATEGORICA[7]},
        },
    ]
    return option

def _calendario_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    valores = [row.get("km_no_realizados") or 0 for row in rows]
    fechas = [row.get("fecha") for row in rows]
    rango = [min(fechas), max(fechas)] if fechas else None
    return {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "item", "confine": True, "formatter": "{c0} km"},
        "visualMap": {
            "min": 0,
            "max": max(valores) if valores else 0,
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": 4,
            "itemWidth": 12,
            "itemHeight": 90,
            "textStyle": {"fontSize": 10},
            "inRange": {"color": ["#eef4ff", "#83A4FF", "#3F5DB2", "#25397A"]},
        },
        "calendar": {
            "top": 48,
            "left": 36,
            "right": 16,
            "cellSize": ["auto", 16],
            "range": rango,
            "itemStyle": {"color": "#fff", "borderColor": COLOR_EJE, "borderWidth": 1},
            "yearLabel": {"show": False},
            "monthLabel": {"fontSize": 10},
            "dayLabel": {"fontSize": 10, "firstDay": 1},
            "splitLine": {"lineStyle": {"color": "#c9d8f6"}},
        },
        "series": [
            {
                "type": "heatmap",
                "coordinateSystem": "calendar",
                "data": [[row.get("fecha"), row.get("km_no_realizados")] for row in rows],
            }
        ],
    }

def _tendencia_agregada_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    option = _base_cartesiano()
    option["xAxis"]["data"] = [f"{row.get('nombre_mes')} {row.get('anio')}" for row in rows]
    option["legend"] = {"show": True, "top": 8, "left": "center", "icon": "roundRect"}
    option["grid"]["top"] = 56
    option["yAxis"] = [
        option["yAxis"],
        {
            "type": "value",
            "name": "acumulado",
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "axisLabel": {"formatter": "{value} km"},
            "splitLine": {"show": False},
        },
    ]
    option["series"] = [
        _serie_barra(f"{ETIQUETA_METRICA} del mes", [row.get("km_no_realizados") for row in rows], PALETA_CATEGORICA[1]),
        {
            "name": "Acumulado",
            "type": "line",
            "yAxisIndex": 1,
            "data": [row.get("acumulado") for row in rows],
            "smooth": 0.25,
            "symbol": "circle",
            "symbolSize": 6,
            "lineStyle": {"color": PALETA_CATEGORICA[4], "width": 2.4},
            "itemStyle": {"color": PALETA_CATEGORICA[4]},
        },
    ]
    return option

def _cruzada_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    tipos = sorted({row.get("tipo_dia") for row in rows if row.get("tipo_dia")})
    origenes = sorted({row.get("origen") for row in rows if row.get("origen")})
    indice = {(row.get("tipo_dia"), row.get("origen")): row.get("km_no_realizados") for row in rows}
    option = _base_cartesiano()
    option["xAxis"]["data"] = tipos
    option["legend"] = {"show": True, "top": 8, "left": "center", "icon": "roundRect"}
    option["grid"]["top"] = 56
    option["series"] = [
        _serie_barra(
            origen,
            [indice.get((tipo, origen), 0) for tipo in tipos],
            PALETA_CATEGORICA[posicion % len(PALETA_CATEGORICA)],
            apilada="origen",
        )
        for posicion, origen in enumerate(origenes)
    ]
    return option

def _boxplot_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    option = _base_cartesiano()
    option["xAxis"]["data"] = [row.get("ruta") for row in rows]
    option["xAxis"]["axisLabel"]["rotate"] = 30
    option["tooltip"] = {"trigger": "item", "confine": True}
    option["series"] = [
        {
            "name": "Distribución por ruta",
            "type": "boxplot",
            "data": [
                [row.get("minimo"), row.get("q1"), row.get("mediana"), row.get("q3"), row.get("maximo")]
                for row in rows
            ],
            "itemStyle": {"color": "#eef4ff", "borderColor": PALETA_CATEGORICA[0], "borderWidth": 1.4},
            "emphasis": {"itemStyle": {"borderColor": PALETA_CATEGORICA[2], "borderWidth": 2}},
        }
    ]
    return option

def _dispersion_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    option = _base_cartesiano()
    option["xAxis"] = {
        "type": "value",
        "name": "registros",
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "splitLine": {"lineStyle": {"color": COLOR_EJE, "type": "dashed"}},
    }
    option["tooltip"] = {"trigger": "item", "confine": True}
    option["series"] = [
        {
            "name": "Rutas",
            "type": "scatter",
            "symbolSize": 12,
            "data": [
                {
                    "name": row.get("ruta"),
                    "value": [row.get("total_registros"), row.get("km_no_realizados")],
                }
                for row in rows
            ],
            "itemStyle": {"color": PALETA_CATEGORICA[2], "opacity": 0.78},
            "emphasis": {"itemStyle": {"color": PALETA_CATEGORICA[0], "opacity": 1}},
        }
    ]
    return option

def _criticidad_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    ordenadas = list(reversed(rows))
    option = _base_cartesiano(horizontal=True)
    option["yAxis"]["data"] = [row.get("ruta") for row in ordenadas]
    option["grid"]["left"] = 140
    option["tooltip"] = {"trigger": "item", "confine": True}
    option["series"] = [
        {
            "name": "Criticidad",
            "type": "bar",
            "barMaxWidth": 18,
            "data": [
                {
                    "value": row.get("km_no_realizados"),
                    "name": row.get("categoria"),
                    "itemStyle": {
                        "color": COLOR_CRITICIDAD.get(row.get("categoria"), PALETA_CATEGORICA[1]),
                        "borderRadius": [0, 3, 3, 0],
                    },
                }
                for row in ordenadas
            ],
            "emphasis": {"focus": "series"},
        }
    ]
    return option

def _heatmap_option(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    dias = [
        nombre
        for _, nombre in sorted(
            {(row.get("numero_dia_semana"), row.get("nombre_dia")) for row in rows if row.get("numero_dia_semana")}
        )
    ]
    franjas = sorted({row.get("franja") for row in rows if row.get("franja")})
    indice_dia = {nombre: posicion for posicion, nombre in enumerate(dias)}
    indice_franja = {franja: posicion for posicion, franja in enumerate(franjas)}
    datos = [
        [indice_franja[row["franja"]], indice_dia[row["nombre_dia"]], row.get("km_no_realizados")]
        for row in rows
        if row.get("franja") in indice_franja and row.get("nombre_dia") in indice_dia
    ]
    valores = [dato[2] or 0 for dato in datos]
    return {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "item", "confine": True},
        "grid": {"left": 70, "right": 20, "top": 20, "bottom": 74, "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": franjas,
            "splitArea": {"show": True},
            "axisLabel": {"rotate": 60, "fontSize": 9, "hideOverlap": True},
            "axisTick": {"show": False},
        },
        "yAxis": {"type": "category", "data": dias, "splitArea": {"show": True}, "axisTick": {"show": False}},
        "visualMap": {
            "min": 0,
            "max": max(valores) if valores else 0,
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": 4,
            "itemHeight": 90,
            "textStyle": {"fontSize": 10},
            "inRange": {"color": ["#eef4ff", "#83A4FF", "#3F5DB2", "#25397A"]},
        },
        "series": [
            {
                "name": ETIQUETA_METRICA,
                "type": "heatmap",
                "data": datos,
                "label": {"show": False},
                "emphasis": {"itemStyle": {"shadowBlur": 8, "shadowColor": "rgba(15,23,42,.28)"}},
            }
        ],
    }

def _tarjeta(rows: list[dict[str, Any]], option: dict[str, Any] | None, note: str) -> dict[str, Any]:
    """Empaqueta una tarjeta en el contrato que espera renderDashboardCard."""
    return {
        "status": "ready" if rows and option else "empty",
        "rows": rows,
        "option": option,
        "note": note,
    }

def _sections_response(section_data: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    def datos(clave: str) -> list[dict[str, Any]]:
        return section_data.get(clave) or []

    evolucion = datos("evolucion_principal")
    comparacion = datos("comparacion_temporal")
    return {
        # Sección 2 — Evolución y comportamiento
        "evolucion_principal": {
            "status": "ready" if evolucion else "empty",
            "rows": evolucion,
            "option": _line_option(evolucion),
            "note": "Agrupación diaria del período consultado.",
        },
        "comparacion_temporal": {
            "status": "ready" if comparacion else "empty",
            "rows": comparacion,
            "option": _comparison_option(comparacion),
            "note": "Comparación contra el período anterior equivalente.",
        },
        "calendario_estacionalidad": _tarjeta(
            datos("calendario_estacionalidad"),
            _calendario_option(datos("calendario_estacionalidad")),
            "Intensidad diaria de kilómetros no realizados en el período.",
        ),
        "tendencia_agregada": _tarjeta(
            datos("tendencia_agregada"),
            _tendencia_agregada_option(datos("tendencia_agregada")),
            "Agregación mensual con acumulado del período.",
        ),
        # Sección 3 — Composición y distribución
        "composicion_principal": _tarjeta(
            datos("composicion_principal"),
            _dona_option(datos("composicion_principal")),
            "Participación por origen de los kilómetros no realizados.",
        ),
        "distribucion_secundaria": _tarjeta(
            datos("distribucion_secundaria"),
            _barras_horizontales_option(datos("distribucion_secundaria")),
            f"Principales {TOP_CATEGORIAS} motivos de eliminación por kilómetros.",
        ),
        "participacion_acumulada": _tarjeta(
            datos("participacion_acumulada"),
            _pareto_option(datos("participacion_acumulada")),
            "Pareto de rutas: barras por km y línea de participación acumulada. Excluye registros sin ruta identificada.",
        ),
        "distribucion_complementaria": _tarjeta(
            datos("distribucion_complementaria"),
            _barras_verticales_option(datos("distribucion_complementaria")),
            "Distribución por grupo operativo.",
        ),
        # Sección 4 — Segmentación y comparación
        "dimension_principal": _tarjeta(
            datos("dimension_principal"),
            _barras_horizontales_option(datos("dimension_principal")),
            f"Ranking de las {TOP_CATEGORIAS} rutas con mayor impacto. Excluye registros sin ruta identificada.",
        ),
        "dimension_territorial": _tarjeta(
            datos("dimension_territorial"),
            _barras_horizontales_option(datos("dimension_territorial")),
            "Comparación por centro de operación (COP).",
        ),
        "dimension_operativa": _tarjeta(
            datos("dimension_operativa"),
            _barras_verticales_option(datos("dimension_operativa")),
            "Comparación por componente operativo.",
        ),
        "comparacion_cruzada": _tarjeta(
            datos("comparacion_cruzada"),
            _cruzada_option(datos("comparacion_cruzada")),
            "Cruce entre tipo de día operativo y origen del kilómetro no realizado.",
        ),
        # Sección 5 — Diagnóstico y priorización
        "distribucion_estadistica": _tarjeta(
            datos("distribucion_estadistica"),
            _boxplot_option(datos("distribucion_estadistica")),
            f"Dispersión por registro en las {TOP_RUTAS_ANALITICA} rutas de mayor impacto.",
        ),
        "relacion_variables": _tarjeta(
            datos("relacion_variables"),
            _dispersion_option(datos("relacion_variables")),
            "Relación entre cantidad de registros y kilómetros no realizados por ruta.",
        ),
        "clasificacion_criticidad": _tarjeta(
            datos("clasificacion_criticidad"),
            _criticidad_option(datos("clasificacion_criticidad")),
            "Clasificación por cuartiles de impacto (IQR) sobre el total de rutas.",
        ),
        "mapa_calor_matriz": _tarjeta(
            datos("mapa_calor_matriz"),
            _heatmap_option(datos("mapa_calor_matriz")),
            "Concentración por día de la semana y franja horaria real.",
        ),
    }

def _empty_options() -> dict[str, list[Any]]:
    return {key: [] for key in OPTION_COLUMNS}

def _empty_aggregates() -> dict[str, Any]:
    return {
        "km_actual": 0.0,
        "registros_actual": 0,
        "promedio_actual": None,
        "variacion_pct": None,
    }

def _consultar_solo_opciones(parametros: dict[str, Any], total_start: float) -> dict[str, Any]:
    """
    Respuesta ligera con los catálogos de filtro del período y filtros activos.

    Cuesta entre 0,4 s y 6 s según lo restrictivo del filtro —bastante menos que
    la respuesta completa— porque no calcula tarjetas ni detalle.
    """
    with get_plata_connection() as connection:
        with connection.cursor() as cursor:
            metadata = _metadata(cursor)
            max_fecha = metadata.get("fecha_maxima_datos")
            if isinstance(max_fecha, str):
                max_fecha = date.fromisoformat(max_fecha)
            filters = _normalize_filters(parametros, max_fecha)
            where, params, _active = build_where_clause(filters)
            options = _filter_options(cursor, where, params, metadata.get("fecha_hora_carga"))

    logger.info(
        "KMNR catálogos de filtro entregados en %.1f ms",
        (time.perf_counter() - total_start) * 1000,
    )
    return {
        "ok": True,
        "fuente": FUENTE,
        "modo_solicitud": "solo_opciones",
        "filters": {"applied": _public_filters(filters), "options": options},
    }

def consultar(**parametros: Any) -> dict[str, Any]:
    """Punto de entrada del endpoint GET /api/bi/kilometros/kilometros_eliminados_no_ejecutados."""
    total_start = time.perf_counter()
    try:
        if _solo_opciones(parametros):
            return _consultar_solo_opciones(parametros, total_start)
        include_options = _include_options(parametros)
        request_mode = _request_mode(parametros, include_options)
        should_read_metadata = include_options or not _has_request_dates(parametros)
        consultas_sql = 0
        metadata: dict[str, Any] = {}
        max_fecha: date | None = None
        with get_plata_connection() as connection:
            with connection.cursor() as cursor:
                if should_read_metadata:
                    # El contador refleja consultas realmente ejecutadas: lo
                    # servido desde caché no viaja a la base de datos.
                    metadata_cacheada = _cache_metadata_leer() is not None
                    metadata = _metadata(cursor)
                    if not metadata_cacheada:
                        consultas_sql += 1
                    max_fecha = metadata.get("fecha_maxima_datos")
                    if isinstance(max_fecha, str):
                        max_fecha = date.fromisoformat(max_fecha)
                if max_fecha is None:
                    if not should_read_metadata:
                        filters = _normalize_filters(parametros, None)
                    else:
                        filters = {}
                if should_read_metadata and max_fecha is None:
                    logger.info("KMNR carga inicial sin datos finalizada en %.1f ms", (time.perf_counter() - total_start) * 1000)
                    return {
                        "ok": True,
                        "fuente": FUENTE,
                        "metadata": {
                            "fecha_hora_carga": _json_value(metadata.get("fecha_hora_carga")),
                            "fecha_maxima_datos": None,
                            "ultima_carga_datos": _json_value(metadata.get("fecha_hora_carga")),
                            "datos_con_registros_hasta": None,
                            "periodo_consultado": {"fecha_inicio": None, "fecha_fin": None},
                            "consultas_sql": consultas_sql,
                        },
                        "filters": {"applied": {}, "options": _empty_options() if include_options else None},
                        "filtros_aplicados": {},
                        "kpis": _kpis_response(_empty_aggregates(), {}),
                        "sections": _sections_response({"evolucion_principal": [], "comparacion_temporal": []}),
                        "detail": _detalle_vacio(),
                    }

                filters = _normalize_filters(parametros, max_fecha)
                where, params, active = build_where_clause(filters)
                previous_start, previous_end = _previous_period(filters.get("fecha_inicio"), filters.get("fecha_fin"))

                options = None
                if include_options:
                    # El sello de carga entra en la clave del caché: si el ETL
                    # publica datos nuevos, los catálogos se recalculan solos.
                    sello = metadata.get("fecha_hora_carga")
                    opciones_cacheadas = _cache_opciones_leer(
                        (str(sello), where, repr(params))
                    ) is not None
                    options = _filter_options(cursor, where, params, sello)
                    if not opciones_cacheadas:
                        consultas_sql += 1
                aggregate_data = _aggregate_data(cursor, filters, where, params, previous_start, previous_end)
                consultas_sql += 1
                aggregates = aggregate_data["aggregates"]
                leaders = aggregate_data["leaders"]
                section_data = aggregate_data["section_data"]

                # Un bloque por sección: comparten el WHERE del período activo y
                # cada uno resuelve sus cuatro tarjetas en una sola consulta.
                for bloque in (
                    _section_evolucion_extra,
                    _section_composicion,
                    _section_segmentacion,
                    _section_diagnostico,
                ):
                    section_data.update(bloque(cursor, where, params))
                    consultas_sql += 1

                detalle = _section_detalle(cursor, where, params, _limite_detalle(parametros))
                consultas_sql += 1

        fecha_inicio = filters.get("fecha_inicio")
        fecha_fin = filters.get("fecha_fin")
        logger.info(
            "KMNR modo %s finalizado en %.1f ms con %s consultas SQL",
            request_mode,
            (time.perf_counter() - total_start) * 1000,
            consultas_sql,
        )
        metadata_response = {
            "periodo_consultado": {
                "fecha_inicio": _json_value(fecha_inicio),
                "fecha_fin": _json_value(fecha_fin),
            },
            "consultas_sql": consultas_sql,
        }
        if metadata:
            metadata_response.update(
                {
                    "fecha_hora_carga": _json_value(metadata.get("fecha_hora_carga")),
                    "fecha_maxima_datos": _json_value(metadata.get("fecha_maxima_datos")),
                    "ultima_carga_datos": _json_value(metadata.get("fecha_hora_carga")),
                    "datos_con_registros_hasta": _json_value(metadata.get("fecha_maxima_datos")),
                }
            )
        return {
            "ok": True,
            "fuente": FUENTE,
            "modo_solicitud": request_mode,
            "metadata": metadata_response,
            "filters": {"applied": _public_filters(filters), "options": options},
            "filtros_aplicados": active,
            "kpis": _kpis_response(aggregates, leaders),
            "sections": _sections_response(section_data),
            "detail": detalle,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Error consultando Kilómetros Eliminados y No Ejecutados")
        raise HTTPException(status_code=500, detail="Ocurrió un error al consultar la información.") from exc

def generar_exportacion_xlsx(**_parametros: Any) -> str:
    """La exportación masiva queda fuera de esta primera fase de conexión."""
    raise RuntimeError("La exportación XLSX estará disponible cuando el dashboard tenga datos exportables.")

# ══════════════════════════════════════════════════════════════════════════════
# CAPA DE DATOS — definición y mantenimiento
# ══════════════════════════════════════════════════════════════════════════════
# Dos objetos en la base, cada uno con una responsabilidad:
#
#   1. bi_km_eliminados (materializada) — hechos de km_eliminado con la
#      dimensión de ruta ya resuelta. 18 columnas: solo lo que el tablero usa.
#      Es lo único que ocupa disco. Se auditó columna por columna contra el
#      modelo y la plantilla; la única que se descartó fue 'servicio', porque
#      sus 41.989 valores distintos hacían inservible su desplegable y ninguna
#      gráfica ni el detalle la utilizaban.
#
#   2. vw_bi_km_eliminados (vista) — la materializada unida a dim_calendario.
#      No ocupa disco: el calendario vive solo en su dimensional.
#
# Por qué así, con las mediciones que lo respaldan:
#
#   · La dimensión de RUTA necesita un LATERAL con ORDER BY ... LIMIT 1, porque
#     dim_rutas maneja vigencias y algunas se solapan. Resolverlo por fila en
#     cada consulta costaba 8x; materializado se paga una vez por carga.
#
#   · La dimensión de CALENDARIO, en cambio, es un hash join contra 4.025 filas.
#     Medido: unirla al vuelo resultó un 8% MÁS RÁPIDO que llevarla duplicada en
#     el millón de filas, porque deja la materializada más estrecha y habilita
#     Parallel Index Only Scan. Duplicarla no aportaba nada.
#
#   · El grano se mantiene a nivel de REGISTRO. Se evaluó pre-agregar por las 16
#     dimensiones: reducía apenas un 12% (1.061.135 -> 938.327 filas) y alteraba
#     los percentiles del boxplot de 'distribucion_estadistica', que se calculan
#     sobre valores individuales. No compensaba.
#
# Este DDL se ejecuta con crear_capa_datos(). Solo hace falta al desplegar por
# primera vez o si cambia la estructura; el día a día lo cubre el refresco
# automático de dashboard/database/refresco_bi.py.

DDL_CAPA_DATOS = f"""
DROP VIEW IF EXISTS {FUENTE};
DROP MATERIALIZED VIEW IF EXISTS {MATERIALIZADA};

CREATE MATERIALIZED VIEW {MATERIALIZADA} AS
SELECT
    -- Clave e identidad
    ke.id_km_eliminado,
    ke.fecha,
    ke.fecha_hora_carga,

    -- Métrica principal
    ke.km_eliminado_ajustado,

    -- Dimensión de ruta (resuelta desde dim_rutas por línea y vigencia)
    ke.id_linea,
    dr.cop,
    dr.ruta,
    dr.concesion,
    dr.zona,
    dr.componente,
    ke.sentido,

    -- Operación. 'SIN REGISTRO FMS' se normaliza a NULL para que no aparezca
    -- como opción seleccionable en los filtros del tablero.
    CASE
        WHEN upper(btrim(COALESCE(ke.franja_hora_real, ''::character varying)::text)) = 'SIN REGISTRO FMS'
        THEN NULL::character varying
        ELSE ke.franja_hora_real
    END::character varying(20) AS franja_hora_real,
    CASE
        WHEN upper(btrim(COALESCE(ke.eliminado, ''::character varying)::text)) = 'SIN REGISTRO FMS'
        THEN NULL::character varying
        ELSE ke.eliminado
    END::character varying(50) AS eliminado,
    CASE
        WHEN upper(btrim(COALESCE(ke.estado_motivo_eliminacion, ''::character varying)::text)) = 'SIN REGISTRO FMS'
        THEN NULL::character varying
        ELSE ke.estado_motivo_eliminacion
    END::character varying(100) AS estado_motivo_eliminacion,
    ke.descripcion_motivo_eliminacion,

    -- Clasificación funcional del kilómetro no realizado
    CASE
        WHEN lower(btrim(COALESCE(ke.descripcion_motivo_eliminacion, ''::character varying)::text))
             = lower('Cálculo Km NO Ejecutado')
        THEN 'Registro de Km no ejecutado'::text
        WHEN btrim(COALESCE(ke.descripcion_motivo_eliminacion, ''::character varying)::text) <> ''
        THEN 'Registro de eliminación en FMS'::text
        ELSE 'Sin clasificación'::text
    END AS origen_km_no_realizados,

    -- Responsabilidad
    ke.grupo,
    ke.responsable

FROM pl_k01_kilometros.km_eliminado ke
    LEFT JOIN LATERAL (
        SELECT d.cop, d.ruta, d.concesion, d.zona, d.componente
        FROM m01_dimensionales.dim_rutas d
        WHERE d.linea = ke.id_linea::text
        -- Preferencia 1: la vigencia que cubre la fecha del hecho.
        -- Preferencia 2: si ninguna la cubre, la vigencia más reciente conocida
        --   de esa línea. Sin este respaldo, 7.706 hechos quedaban sin ruta:
        --   dim_rutas conserva vigencias caducadas que no se extendieron (la
        --   línea 10024 figura vigente hasta 2026-01-04 con hechos hasta
        --   agosto). La línea existe y su ruta se conoce; lo que falta es el
        --   mantenimiento de la vigencia, no el dato. Verificado: el respaldo
        --   solo añade valores, nunca altera los que ya se resolvían.
        ORDER BY
            (ke.fecha >= d.fecha_inicial AND ke.fecha <= d.fecha_fin) DESC,
            d.fecha_fin DESC,
            d.fecha_inicial DESC
        LIMIT 1
    ) dr ON true
WITH DATA;

-- El índice ÚNICO habilita REFRESH ... CONCURRENTLY, que no bloquea lecturas.
CREATE UNIQUE INDEX bi_km_eliminados_pk
    ON {MATERIALIZADA} (id_km_eliminado);

-- Todo filtro del tablero acota primero por rango de fechas.
CREATE INDEX bi_km_eliminados_fecha
    ON {MATERIALIZADA} (fecha);

-- Combinaciones más frecuentes del panel de filtros.
CREATE INDEX bi_km_eliminados_fecha_concesion ON {MATERIALIZADA} (fecha, concesion);
CREATE INDEX bi_km_eliminados_fecha_cop       ON {MATERIALIZADA} (fecha, cop);
CREATE INDEX bi_km_eliminados_fecha_ruta      ON {MATERIALIZADA} (fecha, ruta);
CREATE INDEX bi_km_eliminados_fecha_origen    ON {MATERIALIZADA} (fecha, origen_km_no_realizados);

-- El tablero abre pidiendo MAX(fecha) y MAX(fecha_hora_carga): con este índice
-- se resuelven por Index Only Scan en microsegundos.
CREATE INDEX bi_km_eliminados_fecha_hora_carga
    ON {MATERIALIZADA} (fecha_hora_carga DESC);

ANALYZE {MATERIALIZADA};

-- Vista de consumo: expone los atributos de calendario que el tablero usa para
-- filtrar, agrupar y etiquetar. LEFT JOIN por prudencia: si algún día faltara
-- una fecha en la dimensional, el hecho no desaparecería del tablero.
CREATE VIEW {FUENTE} AS
SELECT
    k.*,
    dc.anio,
    dc.semestre,
    dc.trimestre,
    dc.numero_mes,
    dc.nombre_mes,
    dc.semana_iso,
    dc.numero_dia_semana,
    dc.nombre_dia_semana,
    dc.es_festivo,
    dc.tipo_dia_calendario,
    dc.tipo_dia_operativo,
    dc.estacionalidad
FROM {MATERIALIZADA} k
    LEFT JOIN m01_dimensionales.dim_calendario dc ON dc.fecha = k.fecha;
"""

def crear_capa_datos() -> dict[str, Any]:
    """
    Crea o recrea la materializada y su vista de consumo.

    Es una operación de despliegue, no de uso diario: reconstruye desde cero
    (unos 30 s sobre un millón de filas) y deja la estructura lista. El día a
    día lo cubre el refresco automático.
    """
    inicio = time.perf_counter()
    with get_plata_connection() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(DDL_CAPA_DATOS)
        conexion.commit()
        with conexion.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {MATERIALIZADA}")
            filas = cursor.fetchone()[0]
    limpiar_cache()
    duracion = round(time.perf_counter() - inicio, 1)
    logger.info("KMNR capa de datos creada en %.1f s (%s filas).", duracion, filas)
    return {"creada": True, "filas": filas, "duracion_seg": duracion}

# El vigilante de refresco descubre esta materializada al importarse el modelo.
registrar_materializada(
    nombre="bi_km_eliminados",
    objeto=MATERIALIZADA,
    origen="pl_k01_kilometros.km_eliminado",
    columna_sello="fecha_hora_carga",
    al_refrescar=limpiar_cache,
)
