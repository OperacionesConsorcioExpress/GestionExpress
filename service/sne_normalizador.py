"""
service/sne_normalizador.py

Capa de normalización centralizada para el motor de evaluación SNE Asignación.

Responsabilidades:
  - Transformar texto a forma canónica (ASCII, minúsculas, sin tildes).
  - Resolver alias conocidos del negocio: "Centro de Control" → "ccz",
    "Alimentación" → "alimentacion", etc.
  - Descomponer campos multi-valor (separados por ' | ') en listas de tokens normalizados.
  - Preparar el objeto de caso normalizado que consume el evaluador.

Toda comparación de texto en el evaluador debe pasar por este módulo.
Esto elimina falsos negativos por tildes, mayúsculas o variantes nominales del negocio.
"""
from __future__ import annotations

import unicodedata
from math import isfinite
from typing import Optional

KM_CAMPOS_EVALUABLES = frozenset({
    "km_revision",
    "km_prog_ad",
    "km_elim_eic",
    "km_ejecutado",
    "offset_inicio",
    "offset_fin",
})


# ── Normalización base ─────────────────────────────────────────────────────────

def norm_base(s: str) -> str:
    """
    Normaliza un texto a su forma canónica base:
    1. Descompone caracteres unicode (NFD)
    2. Elimina marcas de acento (categoría Mn)
    3. Re-encode a ASCII ignorando lo que no tiene equivalente
    4. Lowercase + strip

    Esta función es la única fuente de verdad para normalización básica.
    """
    nfd = unicodedata.normalize("NFD", s)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return sin_tildes.encode("ascii", "ignore").decode("ascii").strip().lower()


# ── Alias maps ─────────────────────────────────────────────────────────────────
# Clave: forma ya normalizada con norm_base() → Valor: forma canónica del negocio.
# Las claves deben estar siempre en minúsculas sin tildes.
# Un mismo valor canónico puede tener múltiples claves (N variantes → 1 forma).

# ---- Responsable / Revisores -------------------------------------------------
ALIAS_RESPONSABLE: dict[str, str] = {
    # Revisión Offline
    "revision offline":                 "revision offline",
    # CCZ / Centro de Control
    "ccz":                              "ccz",
    "centro de control":                "ccz",
    "centro de control zonal":          "ccz",
    "centro control zonal":             "ccz",
    "centro control":                   "ccz",
    # Alimentación
    "alimentacion":                     "alimentacion",
    # Zonal (genérico)
    "zonal":                            "zonal",
    # Operador
    "operador":                         "operador",
}

# ---- Observación / Motivos de eliminación ------------------------------------
#
# Regla de mapeo:
#   Clave: forma ya normalizada con norm_base() → lowercase, sin tildes.
#   Valor: forma canónica usada para la comparación.
#
# Principio de diseño: los motivos reales de sne.motivos_eliminacion y los
# valores en el campo 'observacion' de una regla deben resolver al MISMO valor
# canónico después de norm_base() + lookup en este mapa.
# Si un motivo no tiene alias registrado, se usa directamente su forma normalizada.

ALIAS_OBSERVACION: dict[str, str] = {
    # ── Viaje Tardío ──────────────────────────────────────────────────────────
    "viaje tardio":                             "viaje tardio",
    "viaje tardia":                             "viaje tardio",
    "viaje tardio al despacho":                 "viaje tardio",
    # ── Viaje no ejecutado no eliminado ───────────────────────────────────────
    "viaje no ejecutado no eliminado":          "viaje no ejecutado no eliminado",
    "viaje no ejecutado":                       "viaje no ejecutado no eliminado",
    "viaje no ejecutado - no eliminado":        "viaje no ejecutado no eliminado",
    # ── Novedad general / validada ────────────────────────────────────────────
    "novedad general validada":                 "novedad general validada",
    "novedad validada":                         "novedad general validada",
    "novedad general":                          "novedad general validada",
    "novedad":                                  "novedad general validada",
    # ── Deslocalización con eliminación ───────────────────────────────────────
    # (condición de KM > 5 suele acompañar; son motivos distintos a "Deslocalización")
    "deslocalizacion con eliminacion":          "deslocalizacion con eliminacion",
    "desloc con eliminacion":                   "deslocalizacion con eliminacion",
    "deslocalizacion c/eliminacion":            "deslocalizacion con eliminacion",
    # ── OffSet Deslocalización ────────────────────────────────────────────────
    "offset deslocalizacion":                   "offset deslocalizacion",
    "offset & deslocalizacion":                 "offset deslocalizacion",
    "off set deslocalizacion":                  "offset deslocalizacion",
    # ── Regulación Offline ────────────────────────────────────────────────────
    "regulacion offline":                       "regulacion offline",
    "regulacion off line":                      "regulacion offline",
    # ── Deslocalización (genérica, sin "con eliminación") ─────────────────────
    "deslocalizacion":                          "deslocalizacion",
    # ── Acción de regulación ─────────────────────────────────────────────────
    "accion de regulacion":                     "accion de regulacion",
    "accion regulacion":                        "accion de regulacion",
    "accion de regulacion ccz":                 "accion de regulacion",
    # ── Offset & Limitación ───────────────────────────────────────────────────
    "offset y limitacion":                      "offset y limitacion",
    "offset limitacion":                        "offset y limitacion",
    "offset & limitacion":                      "offset y limitacion",
    "offset limitacion ccz":                    "offset y limitacion",
}

# ---- Componente --------------------------------------------------------------
ALIAS_COMPONENTE: dict[str, str] = {
    "zonal":            "zonal",
    "alimentacion":     "alimentacion",
    "general":          "general",
}


# ── Resolución de alias ────────────────────────────────────────────────────────

def _resolver(valor_norm: str, alias_map: dict[str, str]) -> str:
    """
    Aplica el mapa de alias a un valor ya normalizado con norm_base().
    Si no hay alias registrado, devuelve el valor tal cual (sin perderlo).
    """
    return alias_map.get(valor_norm, valor_norm)


def normalizar_responsable(s: Optional[str]) -> str:
    """
    Normaliza y resuelve alias para un valor de responsable.
    Uso: comparar el campo 'responsable' de una regla contra tokens del caso.
    """
    if not s:
        return ""
    return _resolver(norm_base(s), ALIAS_RESPONSABLE)


def normalizar_componente(s: Optional[str]) -> str:
    """
    Normaliza y resuelve alias para un valor de componente.
    """
    if not s:
        return ""
    return _resolver(norm_base(s), ALIAS_COMPONENTE)


def normalizar_ruta_comercial(s: Optional[str]) -> str:
    """
    Normaliza la ruta comercial para comparaciones exactas canonicas.
    """
    if not s:
        return ""
    return norm_base(str(s))


def normalizar_observacion_token(s: Optional[str]) -> str:
    """
    Normaliza y resuelve alias para un token individual de observación/motivo.
    """
    if not s:
        return ""
    return _resolver(norm_base(s), ALIAS_OBSERVACION)


def tokens_campo_multi(campo_str: Optional[str], alias_map: dict[str, str]) -> list[str]:
    """
    Descompone un campo multi-valor (separado por ' | ') en una lista de tokens
    normalizados y resueltos con el mapa de alias dado.

    Ejemplo:
        tokens_campo_multi("Revisión Offline | CCZ", ALIAS_RESPONSABLE)
        → ["revision offline", "ccz"]

        tokens_campo_multi("Centro de Control Zonal | Alimentación", ALIAS_RESPONSABLE)
        → ["ccz", "alimentacion"]
    """
    if not campo_str:
        return []
    tokens = []
    for parte in campo_str.split("|"):
        parte = parte.strip()
        if parte:
            tokens.append(_resolver(norm_base(parte), alias_map))
    return tokens


def tokens_observacion_regla(observacion_regla: Optional[str]) -> list[str]:
    """
    Descompone la condición de observación de una regla (separada por coma)
    en una lista de tokens normalizados.

    Una regla puede especificar múltiples observaciones alternativas:
        "Viaje Tardío, Deslocalización" → ["viaje tardio", "deslocalizacion"]
    La evaluación es OR: basta que UNO de estos tokens aparezca en los motivos del caso.
    """
    if not observacion_regla:
        return []
    tokens = []
    for parte in observacion_regla.split(","):
        parte = parte.strip()
        if parte:
            tokens.append(normalizar_observacion_token(parte))
    return [t for t in tokens if t]


def _to_float_or_none(value) -> Optional[float]:
    try:
        parsed = float(value) if value is not None else None
        if parsed is not None and not isfinite(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


# ── Normalización del caso completo ───────────────────────────────────────────

def normalizar_caso(row: dict) -> dict:
    """
    Prepara un caso ICS para evaluación añadiendo campos normalizados.

    No muta el dict original; devuelve una copia con campos _norm_* adicionales:
      _norm_componente          str   — componente canónico
      _norm_responsables        list  — lista de responsables canónicos
      _norm_motivos_eliminacion list  — lista de motivos canónicos

    Estos campos son los que debe leer el evaluador. Nunca deben leerse
    directamente los campos de texto originales para comparar condiciones.
    """
    responsables = tokens_campo_multi(row.get("responsables"), ALIAS_RESPONSABLE)
    motivos = tokens_campo_multi(row.get("motivos_eliminacion"), ALIAS_OBSERVACION)
    km_por_campo = {campo: _to_float_or_none(row.get(campo)) for campo in KM_CAMPOS_EVALUABLES}

    return {
        **row,
        "_norm_componente": normalizar_componente(row.get("componente")),
        "_norm_ruta_comercial": normalizar_ruta_comercial(row.get("ruta_comercial")),
        "_norm_responsables": responsables,
        "_norm_responsables_set": frozenset(responsables),
        "_norm_motivos_eliminacion": motivos,
        "_norm_motivos_eliminacion_set": frozenset(motivos),
        "_norm_has_novedad": any("novedad" in motivo for motivo in motivos),
        "_norm_km_float_by_field": km_por_campo,
        "_norm_km_revision_float": km_por_campo.get("km_revision"),
    }
