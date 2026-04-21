"""
service/sne_evaluador.py

Motor de evaluación de reglas SNE Asignación.

Convierte una lista de reglas activas y un caso ICS normalizado en un resultado
de asignación: grupo_asignacion, prioridad_grupo, regla_aplicada, version_modelo.

Responsabilidades:
  - Evaluar la condición de KM (operadores GT, GTE, LT, LTE, EQ, BETWEEN).
  - Evaluar cada condición de texto (componente, responsable, observación, novedad).
  - Implementar la lógica "primera regla que cumple gana" (orden_evaluacion ASC).
  - Devolver fallback PENDIENTE_REGLA cuando ninguna regla aplica.
  - Producir trazas de diagnóstico que expliquen qué condición falló y por qué.

Este módulo NO hace consultas a DB. Recibe datos ya preparados por la capa Model.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional

from service.sne_normalizador import (
    KM_CAMPOS_EVALUABLES,
    normalizar_caso,
    normalizar_componente,
    normalizar_ruta_comercial,
    normalizar_responsable,
    normalizar_observacion_token,
    tokens_observacion_regla,
)

# ── Constantes ─────────────────────────────────────────────────────────────────

FALLBACK_GRUPO      = "PENDIENTE_REGLA"
FALLBACK_PRIORIDAD  = 99
FALLBACK_REGLA_COD  = "SIN_REGLA_VIGENTE"

OPERADORES_VALIDOS = frozenset({"GT", "GTE", "LT", "LTE", "EQ", "BETWEEN"})
KM_CAMPOS_VALIDOS = KM_CAMPOS_EVALUABLES


# ── DTOs de resultado y trazabilidad ──────────────────────────────────────────

@dataclass
class ResultadoEvaluacion:
    grupo_asignacion: str
    prioridad_grupo:  Optional[int]
    regla_aplicada:   Optional[str]
    version_modelo:   Optional[str]

    def as_dict(self) -> dict:
        return asdict(self)


RESULTADO_FALLBACK = ResultadoEvaluacion(
    grupo_asignacion=FALLBACK_GRUPO,
    prioridad_grupo=FALLBACK_PRIORIDAD,
    regla_aplicada=FALLBACK_REGLA_COD,
    version_modelo=None,
)


@dataclass
class CondicionTraza:
    """Resultado de evaluar una condición individual de una regla."""
    campo:         str
    descripcion:   str   # qué buscaba la regla
    valor_caso:    str   # qué tenía el caso (normalizado)
    cumple:        bool

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ReglaTraza:
    """Traza de evaluación de una regla completa sobre un caso."""
    id_regla:    int
    codigo:      str
    nombre:      str
    cumple:      bool
    condiciones: list[CondicionTraza] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "id_regla":    self.id_regla,
            "codigo":      self.codigo,
            "nombre":      self.nombre,
            "cumple":      self.cumple,
            "condiciones": [c.as_dict() for c in self.condiciones],
        }


# ── Evaluación de operador KM ──────────────────────────────────────────────────

def evaluar_operador_km(
    km: float,
    operador: str,
    v1: float,
    v2: Optional[float] = None,
) -> bool:
    """
    Evalúa la condición de km_revision contra el operador y valores de la regla.

    Operadores soportados: GT, GTE, LT, LTE, EQ, BETWEEN.
    Para BETWEEN se requiere v2; si falta, se considera no cumplida.
    """
    op = operador.strip().upper()
    if op == "GT":      return km > v1
    if op == "GTE":     return km >= v1
    if op == "LT":      return km < v1
    if op == "LTE":     return km <= v1
    if op == "EQ":      return km == v1
    if op == "BETWEEN": return v2 is not None and v1 <= km <= v2
    return False


# ── Motor principal ───────────────────────────────────────────────────────────

class EvaluadorReglas:
    """
    Motor sin estado. Aplica reglas sobre casos ya recuperados de DB.

    Uso típico:
        evaluador = EvaluadorReglas()
        reglas    = modelo.reglas_activas()          # list[dict] de DB
        filas_enriquecidas = evaluador.enriquecer_lote(filas, reglas)

    El evaluador recibe dicts planos (filas DB / dicts JSON) tanto para
    el caso como para las reglas. No depende de ORMs ni de modelos DB.
    """

    # ── Evaluación de condiciones individuales ─────────────────────────────────

    def compilar_reglas(self, reglas: list[dict]) -> list[dict]:
        for regla in reglas or []:
            self._ensure_compiled_rule(regla)
        return reglas

    def _ensure_compiled_rule(self, regla: dict) -> dict:
        if regla.get("_compiled_rule"):
            return regla

        operador_km = (regla.get("operador_km") or "").strip().upper()
        km_columna = str(regla.get("km_columna") or "km_revision").strip().lower()
        if km_columna not in KM_CAMPOS_VALIDOS:
            km_columna = "km_revision"
        km_valor_1 = regla.get("km_valor_1")
        km_valor_2 = regla.get("km_valor_2")

        regla["_compiled_rule"] = True
        regla["_compiled_componente"] = normalizar_componente(regla.get("componente") or "")
        regla["_compiled_ruta_comercial"] = normalizar_ruta_comercial(regla.get("ruta_comercial") or "")
        regla["_compiled_responsable"] = normalizar_responsable(regla.get("responsable") or "")
        regla["_compiled_observacion_tokens"] = tuple(
            tokens_observacion_regla((regla.get("observacion") or "").strip())
        )
        regla["_compiled_operador_km"] = operador_km
        regla["_compiled_km_columna"] = km_columna
        regla["_compiled_km_valor_1"] = float(km_valor_1) if km_valor_1 is not None else None
        regla["_compiled_km_valor_2"] = float(km_valor_2) if km_valor_2 is not None else None
        regla["_compiled_requiere_novedad"] = bool(regla.get("requiere_novedad"))
        return regla

    def _cond_componente(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        """None si la condición no está activa en la regla (campo vacío o GENERAL)."""
        regla = self._ensure_compiled_rule(regla)
        rcmp = regla["_compiled_componente"]
        if not rcmp or rcmp == "general":
            return None
        caso_cmp = caso_norm.get("_norm_componente", "")
        # Comparación de igualdad exacta contra el valor canónico normalizado.
        # Ambos lados pasan por norm_base + alias_map, por lo que son comparables.
        cumple = rcmp == caso_cmp
        return CondicionTraza(
            campo="componente",
            descripcion=f"componente == '{rcmp}'",
            valor_caso=caso_cmp,
            cumple=cumple,
        )

    def _cond_responsable(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        regla = self._ensure_compiled_rule(regla)
        rresp = regla["_compiled_responsable"]
        if not rresp:
            return None
        caso_resps: list[str] = caso_norm.get("_norm_responsables", [])
        caso_resps_set = caso_norm.get("_norm_responsables_set", frozenset(caso_resps))
        cumple = rresp in caso_resps_set
        return CondicionTraza(
            campo="responsable",
            descripcion=f"'{rresp}' en lista de responsables",
            valor_caso=repr(caso_resps),
            cumple=cumple,
        )

    def _cond_ruta(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        regla = self._ensure_compiled_rule(regla)
        rruta = regla["_compiled_ruta_comercial"]
        if not rruta:
            return None
        caso_ruta = caso_norm.get("_norm_ruta_comercial", "")
        cumple = rruta == caso_ruta
        return CondicionTraza(
            campo="ruta_comercial",
            descripcion=f"ruta == '{rruta}'",
            valor_caso=caso_ruta,
            cumple=cumple,
        )

    def _cond_observacion(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        caso_mots: list[str] = caso_norm.get("_norm_motivos_eliminacion", [])
        regla = self._ensure_compiled_rule(regla)
        caso_mots_set = caso_norm.get("_norm_motivos_eliminacion_set", frozenset(caso_mots))
        partes_regla = regla["_compiled_observacion_tokens"]
        if not partes_regla:
            return None
        cumple = any(p in caso_mots_set for p in partes_regla)
        return CondicionTraza(
            campo="observacion",
            descripcion=f"alguna de {partes_regla} en motivos (OR)",
            valor_caso=repr(caso_mots),
            cumple=cumple,
        )

    def _cond_km(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        regla = self._ensure_compiled_rule(regla)
        operador = regla["_compiled_operador_km"]
        km_columna = regla["_compiled_km_columna"]
        v1 = regla["_compiled_km_valor_1"]
        v2 = regla["_compiled_km_valor_2"]
        if not operador or v1 is None:
            return None
        km = caso_norm.get("_norm_km_float_by_field", {}).get(km_columna)
        if km is None:
            return CondicionTraza(
                campo=km_columna,
                descripcion=f"{km_columna} {operador} {v1}" + (f" Y {v2}" if v2 is not None else ""),
                valor_caso="null",
                cumple=False,
            )
        cumple = evaluar_operador_km(km, operador, v1, v2)
        label_op = f"{operador} {v1}" + (f" Y {v2}" if v2 is not None else "")
        return CondicionTraza(
            campo=km_columna,
            descripcion=f"{km_columna} {label_op}",
            valor_caso=str(km),
            cumple=cumple,
        )

    def _cond_novedad(self, caso_norm: dict, regla: dict) -> Optional[CondicionTraza]:
        regla = self._ensure_compiled_rule(regla)
        if not regla["_compiled_requiere_novedad"]:
            return None
        caso_mots: list[str] = caso_norm.get("_norm_motivos_eliminacion", [])
        # La forma canónica de novedad es "novedad general validada";
        # buscamos que "novedad" sea substring de algún token para absorber variantes.
        cumple = bool(caso_norm.get("_norm_has_novedad"))
        return CondicionTraza(
            campo="requiere_novedad",
            descripcion="'novedad' presente en motivos",
            valor_caso=repr(caso_mots),
            cumple=cumple,
        )

    # ── Evaluación de una regla completa ──────────────────────────────────────

    def _evaluar_condiciones(self, caso_norm: dict, regla: dict) -> list[CondicionTraza]:
        """
        Construye la lista de condiciones activas de la regla y las evalúa.
        Solo incluye condiciones que la regla tiene configuradas (no nulas).
        """
        evaluadores = [
            self._cond_componente,
            self._cond_ruta,
            self._cond_responsable,
            self._cond_observacion,
            self._cond_km,
            self._cond_novedad,
        ]
        condiciones = []
        for fn in evaluadores:
            cond = fn(caso_norm, regla)
            if cond is not None:
                condiciones.append(cond)

        if not condiciones:
            # Regla sin condiciones restrictivas → siempre aplica (catch-all)
            condiciones.append(CondicionTraza(
                campo="(catch-all)",
                descripcion="regla sin condiciones: aplica a todos los casos",
                valor_caso="",
                cumple=True,
            ))
        return condiciones

    def cumple_regla(self, caso_norm: dict, regla: dict) -> bool:
        """
        Retorna True si el caso normalizado cumple TODAS las condiciones de la regla.
        Short-circuits en la primera condición que falla (eficiencia).
        """
        regla = self._ensure_compiled_rule(regla)

        # Componente
        rcmp = regla["_compiled_componente"]
        if rcmp and rcmp != "general":
            if rcmp != caso_norm.get("_norm_componente", ""):
                return False

        # Ruta comercial
        rruta = regla["_compiled_ruta_comercial"]
        if rruta:
            if rruta != caso_norm.get("_norm_ruta_comercial", ""):
                return False

        # Responsable
        rresp = regla["_compiled_responsable"]
        if rresp:
            if rresp not in caso_norm.get("_norm_responsables_set", frozenset(caso_norm.get("_norm_responsables", []))):
                return False

        # Observación
        partes = regla["_compiled_observacion_tokens"]
        if partes:
            caso_mots = caso_norm.get("_norm_motivos_eliminacion_set", frozenset(caso_norm.get("_norm_motivos_eliminacion", [])))
            if partes and not any(p in caso_mots for p in partes):
                return False

        # KM
        operador = regla["_compiled_operador_km"]
        v1 = regla["_compiled_km_valor_1"]
        if operador and v1 is not None:
            km_columna = regla["_compiled_km_columna"]
            km = caso_norm.get("_norm_km_float_by_field", {}).get(km_columna)
            if km is None:
                return False
            v2 = regla["_compiled_km_valor_2"]
            if not evaluar_operador_km(km, operador, v1, v2):
                return False

        # Novedad
        if regla["_compiled_requiere_novedad"]:
            if not caso_norm.get("_norm_has_novedad"):
                return False

        return True

    # ── Evaluación de un caso contra el conjunto de reglas ────────────────────

    def evaluar_caso(self, caso: dict, reglas: list[dict]) -> ResultadoEvaluacion:
        """
        Aplica todas las reglas ordenadas al caso y retorna el resultado de la
        primera que cumple. Si ninguna coincide, devuelve RESULTADO_FALLBACK.

        :param caso:   Fila ICS en formato dict (sin normalizar; la función lo hace).
        :param reglas: Lista de reglas vigentes ordenadas por orden_evaluacion ASC.
        """
        caso_norm = normalizar_caso(caso)
        reglas = self.compilar_reglas(reglas)
        for regla in reglas:
            if self.cumple_regla(caso_norm, regla):
                return ResultadoEvaluacion(
                    grupo_asignacion=str(regla["grupo_asignacion"]),
                    prioridad_grupo=regla.get("prioridad_grupo"),
                    regla_aplicada=str(regla["codigo"]),
                    version_modelo=regla.get("version_modelo"),
                )
        return RESULTADO_FALLBACK

    def enriquecer_lote(self, rows: list[dict], reglas: list[dict]) -> list[dict]:
        """
        Aplica el evaluador a cada fila del lote.
        Muta cada dict añadiendo los campos de asignación.
        Retorna el mismo lote mutado para facilitar uso en cadena.

        Carga la versión normalizada del caso UNA vez → O(n×r) total.
        """
        reglas = self.compilar_reglas(reglas)
        for row in rows:
            caso_norm = normalizar_caso(row)
            resultado = RESULTADO_FALLBACK
            for regla in reglas:
                if self.cumple_regla(caso_norm, regla):
                    resultado = ResultadoEvaluacion(
                        grupo_asignacion=str(regla["grupo_asignacion"]),
                        prioridad_grupo=regla.get("prioridad_grupo"),
                        regla_aplicada=str(regla["codigo"]),
                        version_modelo=regla.get("version_modelo"),
                    )
                    break
            row["grupo_asignacion"] = resultado.grupo_asignacion
            row["prioridad_grupo"]  = resultado.prioridad_grupo
            row["regla_aplicada"]   = resultado.regla_aplicada
            row["version_modelo"]   = resultado.version_modelo
        return rows

    # ── Trazabilidad ──────────────────────────────────────────────────────────

    def trazar_caso(self, caso: dict, reglas: list[dict]) -> dict:
        """
        Evalúa un caso exponiendo la traza completa de cada regla evaluada.

        Retorna:
          - caso: valores originales + campos normalizados usados en la evaluación
          - resultado: asignación final (igual a evaluar_caso)
          - reglas_evaluadas: lista de ReglaTraza con condición-a-condición
          - regla_ganadora: código de la primera regla que cumplió (o null)
        """
        caso_norm = normalizar_caso(caso)
        reglas = self.compilar_reglas(reglas)
        trazas:    list[ReglaTraza] = []
        ganadora:  Optional[dict]   = None

        for regla in reglas:
            condiciones  = self._evaluar_condiciones(caso_norm, regla)
            cumple_todas = all(c.cumple for c in condiciones)
            trazas.append(ReglaTraza(
                id_regla=regla["id_regla"],
                codigo=regla["codigo"],
                nombre=regla.get("nombre", ""),
                cumple=cumple_todas,
                condiciones=condiciones,
            ))
            if cumple_todas and ganadora is None:
                ganadora = regla

        if ganadora:
            resultado = ResultadoEvaluacion(
                grupo_asignacion=str(ganadora["grupo_asignacion"]),
                prioridad_grupo=ganadora.get("prioridad_grupo"),
                regla_aplicada=str(ganadora["codigo"]),
                version_modelo=ganadora.get("version_modelo"),
            )
        else:
            resultado = RESULTADO_FALLBACK

        return {
            "caso": {
                "id_ics":               caso.get("id_ics"),
                "componente":           caso.get("componente"),
                "ruta_comercial":       caso.get("ruta_comercial"),
                "responsables":         caso.get("responsables"),
                "motivos_eliminacion":  caso.get("motivos_eliminacion"),
                "km_prog_ad":           caso.get("km_prog_ad"),
                "km_elim_eic":          caso.get("km_elim_eic"),
                "km_ejecutado":         caso.get("km_ejecutado"),
                "offset_inicio":        caso.get("offset_inicio"),
                "offset_fin":           caso.get("offset_fin"),
                "km_revision":          caso.get("km_revision"),
                "_normalizacion": {
                    "componente":           caso_norm.get("_norm_componente"),
                    "ruta_comercial":       caso_norm.get("_norm_ruta_comercial"),
                    "responsables":         caso_norm.get("_norm_responsables"),
                    "motivos_eliminacion":  caso_norm.get("_norm_motivos_eliminacion"),
                    "km_por_campo":         caso_norm.get("_norm_km_float_by_field"),
                },
            },
            "resultado":        resultado.as_dict(),
            "regla_ganadora":   ganadora["codigo"] if ganadora else None,
            "reglas_evaluadas": [t.as_dict() for t in trazas],
        }
