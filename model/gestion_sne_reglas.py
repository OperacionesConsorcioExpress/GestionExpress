"""
model/gestion_sne_reglas.py

Modelo para el módulo de Reglas de Asignación SNE.

Provee:
  - CRUD sobre sne.reglas_asignacion con validaciones y registro de historial.
  - Delegación del motor de evaluación al servicio EvaluadorReglas.
  - enriquecer_filas(rows): aplica reglas activas al lote de filas ICS.
  - trazar_fila(id_ics): devuelve diagnóstico completo de evaluación para un caso.
  - historial_regla(id_regla): historial de cambios de una regla.

Patrón de conexión idéntico al resto de modelos del proyecto.
El motor de evaluación y la normalización viven en service/.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from datetime import date, datetime
from typing import Any, Optional

from psycopg2 import extensions as pg_extensions
from psycopg2.extras import RealDictCursor

from database.database_manager import _get_pool as get_db_pool
from service.sne_evaluador import EvaluadorReglas, KM_CAMPOS_VALIDOS

log = logging.getLogger(__name__)

# ── Constantes ─────────────────────────────────────────────────────────────────

GRUPOS_EVALUACION = [
    "PAQUETE_INICIAL_OFFLINE",
    "EXCEPCION_NOVEDAD_GENERAL",
    "PRIORIDAD_2_OFFLINE",
    "RESTO_OFFLINE",
    "AUTO_CCZ",
    "PENDIENTE_REGLA",
]

OPERADORES_KM_VALIDOS = {"GT", "GTE", "LT", "LTE", "EQ", "BETWEEN"}


# ── Serialización ──────────────────────────────────────────────────────────────

def _serial(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _to_dict(row: Any) -> dict:
    return {k: _serial(v) for k, v in dict(row).items()}


# ── Validación de regla ────────────────────────────────────────────────────────

def validar_regla_data(data: dict, *, es_nueva: bool = True) -> list[str]:
    """
    Valida los datos de una regla antes de persistirlos.
    Retorna lista de mensajes de error (vacía si todo es válido).
    """
    errores: list[str] = []

    if es_nueva:
        codigo = (data.get("codigo") or "").strip()
        if not codigo:
            errores.append("El campo 'codigo' es obligatorio.")
        elif len(codigo) > 30:
            errores.append("El campo 'codigo' no puede superar 30 caracteres.")

        nombre = (data.get("nombre") or "").strip()
        if not nombre:
            errores.append("El campo 'nombre' es obligatorio.")

    grupo = (data.get("grupo_asignacion") or "").strip()
    if grupo is not None and grupo != "" and (len(grupo) > 50 or not grupo.replace("_", "").isalnum()):
        errores.append(
            "grupo_asignacion debe ser alfanumérico con guiones bajos y máximo 50 caracteres "
            "(ej: NUEVO_GRUPO)."
        )

    operador = (data.get("operador_km") or "").strip().upper()
    km_columna = str(data.get("km_columna") or "km_revision").strip().lower()
    ruta_comercial = str(data.get("ruta_comercial") or "").strip()
    if km_columna and km_columna not in KM_CAMPOS_VALIDOS:
        errores.append(
            f"km_columna '{km_columna}' no es valida. "
            f"Valores permitidos: {', '.join(sorted(KM_CAMPOS_VALIDOS))}"
        )
    if len(ruta_comercial) > 150:
        errores.append("ruta_comercial no puede superar 150 caracteres.")

    if operador and operador not in OPERADORES_KM_VALIDOS:
        errores.append(
            f"operador_km '{operador}' no es válido. "
            f"Valores permitidos: {', '.join(sorted(OPERADORES_KM_VALIDOS))}"
        )

    if operador == "BETWEEN":
        v1 = data.get("km_valor_1")
        v2 = data.get("km_valor_2")
        if v1 is None or v2 is None:
            errores.append("BETWEEN requiere km_valor_1 y km_valor_2.")
        elif float(v1) >= float(v2):
            errores.append("Para BETWEEN, km_valor_1 debe ser menor que km_valor_2.")

    if operador and operador != "BETWEEN" and data.get("km_valor_1") is None:
        errores.append(f"El operador '{operador}' requiere km_valor_1.")

    vd = data.get("vigencia_desde")
    vh = data.get("vigencia_hasta")
    if vd and vh:
        try:
            if str(vd) > str(vh):
                errores.append("vigencia_desde no puede ser posterior a vigencia_hasta.")
        except Exception:
            pass

    return errores


def _normalizar_payload_regla(data: dict, *, default_km_columna: bool = False) -> dict:
    normalizada = dict(data or {})
    if "operador_km" in normalizada and normalizada.get("operador_km"):
        normalizada["operador_km"] = str(normalizada["operador_km"]).strip().upper()
    if default_km_columna or "km_columna" in normalizada:
        normalizada["km_columna"] = str(normalizada.get("km_columna") or "km_revision").strip().lower()
    if "ruta_comercial" in normalizada and normalizada.get("ruta_comercial"):
        normalizada["ruta_comercial"] = str(normalizada["ruta_comercial"]).strip()
    return normalizada


# ── Clase principal ────────────────────────────────────────────────────────────

class GestionReglasSNE:

    def __init__(self):
        self.conn = get_db_pool().getconn()
        if not self.conn.closed:
            self.conn.rollback()
        self.conn.cursor_factory = RealDictCursor
        self._col_cache: dict[tuple[str, str, str], bool] = {}
        self._evaluador = EvaluadorReglas()

    def __enter__(self) -> "GestionReglasSNE":
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if not self.conn.closed:
                self.conn.rollback()
                self.conn.cursor_factory = pg_extensions.cursor
                get_db_pool().putconn(self.conn)

    # ── Helpers DB ────────────────────────────────────────────────────────────

    def _fetchall(self, sql: str, params=None) -> list[dict]:
        with self.conn.cursor() as c:
            c.execute(sql, params)
            return [_to_dict(r) for r in c.fetchall()]

    def _fetchone(self, sql: str, params=None) -> dict | None:
        with self.conn.cursor() as c:
            c.execute(sql, params)
            row = c.fetchone()
            return _to_dict(row) if row else None

    def _execute(self, sql: str, params=None) -> int:
        with self.conn.cursor() as c:
            c.execute(sql, params)
            return c.rowcount

    def _col_exists(self, schema: str, table: str, column: str) -> bool:
        key = (schema, table, column)
        if key not in self._col_cache:
            with self.conn.cursor() as c:
                c.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND column_name = %s
                    LIMIT 1
                    """,
                    (schema, table, column),
                )
                self._col_cache[key] = c.fetchone() is not None
        return self._col_cache[key]

    def _cop_joins_and_exprs(self) -> tuple[str, str]:
        joins = [
            "LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1",
            "LEFT JOIN config.cop c ON c.id = r.id_cop",
        ]
        comp_expr = "NULL::text"
        if self._col_exists("config", "cop", "id_componente"):
            joins.append("LEFT JOIN config.componente comp ON comp.id = c.id_componente")
            comp_expr = "comp.componente"
        return " ".join(joins), comp_expr

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def listar_reglas(self) -> list[dict]:
        return self._fetchall(
            "SELECT * FROM sne.reglas_asignacion ORDER BY orden_evaluacion ASC, id_regla ASC"
        )

    def obtener_regla(self, id_regla: int) -> dict | None:
        return self._fetchone(
            "SELECT * FROM sne.reglas_asignacion WHERE id_regla = %s",
            (id_regla,),
        )

    _COLS_EDITABLES = [
        "codigo", "nombre", "descripcion", "componente", "activa",
        "version_modelo", "vigencia_desde", "vigencia_hasta",
        "responsable", "observacion", "ruta_comercial", "operador_km", "km_columna",
        "km_valor_1", "km_valor_2", "requiere_novedad",
        "grupo_asignacion", "prioridad_grupo", "orden_evaluacion",
    ]

    def crear_regla(self, data: dict, creado_por: str, ip: Optional[str] = None) -> dict:
        data = _normalizar_payload_regla(data, default_km_columna=True)
        errores = validar_regla_data(data, es_nueva=True)
        if errores:
            raise ValueError("; ".join(errores))

        cols = self._COLS_EDITABLES + ["creado_por", "actualizado_por"]
        vals = [data.get(c) for c in self._COLS_EDITABLES] + [creado_por, creado_por]
        col_str = ", ".join(cols)
        ph_str  = ", ".join(["%s"] * len(cols))
        row = self._fetchone(
            f"INSERT INTO sne.reglas_asignacion ({col_str}) VALUES ({ph_str}) RETURNING *",
            vals,
        )
        if row:
            self._registrar_historial(row["id_regla"], "CREAR", row, creado_por, ip)
        return row  # type: ignore[return-value]

    def actualizar_regla(
        self, id_regla: int, data: dict, actualizado_por: str, ip: Optional[str] = None
    ) -> dict | None:
        data = _normalizar_payload_regla(data)
        presentes = [c for c in self._COLS_EDITABLES if c in data]
        if not presentes:
            return self.obtener_regla(id_regla)

        errores = validar_regla_data(data, es_nueva=False)
        if errores:
            raise ValueError("; ".join(errores))

        sets   = [f"{c} = %s" for c in presentes] + ["actualizado_por = %s", "updated_at = NOW()"]
        params = [data[c] for c in presentes] + [actualizado_por, id_regla]
        row = self._fetchone(
            f"UPDATE sne.reglas_asignacion SET {', '.join(sets)} WHERE id_regla = %s RETURNING *",
            params,
        )
        if row:
            self._registrar_historial(id_regla, "EDITAR", row, actualizado_por, ip)
        return row

    def toggle_activa(
        self, id_regla: int, actualizado_por: str, ip: Optional[str] = None
    ) -> dict | None:
        row = self._fetchone(
            """
            UPDATE sne.reglas_asignacion
               SET activa = NOT activa,
                   actualizado_por = %s,
                   updated_at = NOW()
             WHERE id_regla = %s
            RETURNING *
            """,
            (actualizado_por, id_regla),
        )
        if row:
            accion = "ACTIVAR" if row.get("activa") else "DESACTIVAR"
            self._registrar_historial(id_regla, accion, row, actualizado_por, ip)
        return row

    def duplicar_regla(
        self, id_regla: int, creado_por: str, ip: Optional[str] = None
    ) -> dict:
        orig = self.obtener_regla(id_regla)
        if not orig:
            raise ValueError(f"Regla {id_regla} no encontrada")
        copia = {**orig}
        copia["codigo"] = f"{orig['codigo']}-COPIA"
        copia["nombre"] = f"{orig['nombre']} (copia)"
        copia["activa"] = False
        # crear_regla ya registra historial como "CREAR"
        nueva = self.crear_regla(copia, creado_por, ip)
        # Registro adicional de origen
        self._registrar_historial(
            nueva["id_regla"], "DUPLICAR",
            {**nueva, "_origen_id_regla": id_regla},
            creado_por, ip,
        )
        return nueva

    def eliminar_regla(
        self, id_regla: int, eliminado_por: str = "sistema", ip: Optional[str] = None
    ) -> bool:
        snapshot = self.obtener_regla(id_regla)
        n = self._execute(
            "DELETE FROM sne.reglas_asignacion WHERE id_regla = %s", (id_regla,)
        )
        eliminado = n > 0
        if eliminado and snapshot:
            # Se pasa None como id_regla porque la regla ya no existe en la tabla.
            # La FK historial.id_regla permite NULL (ON DELETE SET NULL).
            # Insertar con el id original causaría FK violation → transacción ABORTED.
            self._registrar_historial(None, "ELIMINAR", snapshot, eliminado_por, ip)
        return eliminado

    # ── Historial ─────────────────────────────────────────────────────────────

    def _registrar_historial(
        self,
        id_regla: int,
        accion: str,
        snapshot: dict,
        usuario: str,
        ip: Optional[str] = None,
    ) -> None:
        """
        Inserta un registro en sne.historial_reglas_asignacion.
        Silencia errores para no bloquear operaciones principales
        si la tabla de historial aún no existe (migración 002 pendiente).
        """
        try:
            self._execute(
                """
                INSERT INTO sne.historial_reglas_asignacion
                    (id_regla, accion, snapshot, usuario, ip_origen)
                VALUES (%s, %s, %s::jsonb, %s, %s)
                """,
                (id_regla, accion, json.dumps(snapshot, default=str), usuario, ip),
            )
        except Exception as exc:
            log.warning("historial_reglas: no se pudo registrar [%s id=%s]: %s", accion, id_regla, exc)

    def historial_regla(self, id_regla: int) -> list[dict]:
        """Retorna el historial de cambios de una regla, más reciente primero."""
        return self._fetchall(
            """
            SELECT id, id_regla, accion, snapshot, usuario, ip_origen, created_at
              FROM sne.historial_reglas_asignacion
             WHERE id_regla = %s
             ORDER BY created_at DESC
            """,
            (id_regla,),
        )

    # ── Reglas activas (con vigencia) ─────────────────────────────────────────

    def reglas_activas(self) -> list[dict]:
        return self._fetchall(
            """
            SELECT *
              FROM sne.reglas_asignacion
             WHERE activa = TRUE
               AND (vigencia_desde IS NULL OR vigencia_desde <= CURRENT_DATE)
               AND (vigencia_hasta IS NULL OR vigencia_hasta >= CURRENT_DATE)
             ORDER BY orden_evaluacion ASC, id_regla ASC
            """
        )

    # ── Motor de evaluación (delegado a service/sne_evaluador.py) ─────────────

    def enriquecer_filas(self, rows: list[dict]) -> list[dict]:
        """
        Aplica las reglas activas al lote de filas ICS.
        Añade a cada fila: grupo_asignacion, prioridad_grupo,
        regla_aplicada, version_modelo.
        """
        reglas = self._evaluador.compilar_reglas(self.reglas_activas())
        if not reglas:
            log.warning("enriquecer_filas: no hay reglas activas vigentes. Todas las filas quedarán como PENDIENTE_REGLA.")
        return self._evaluador.enriquecer_lote(rows, reglas)

    def trazar_fila(self, id_ics: int) -> dict:
        """
        Carga la fila ICS indicada y devuelve la traza completa de evaluación:
        normalización usada, qué reglas se evaluaron y por qué condición falló
        o pasó cada una.

        Útil para diagnosticar casos que quedan en PENDIENTE_REGLA.
        """
        joins_cop, comp_expr = self._cop_joins_and_exprs()
        group_by_component = f", {comp_expr}" if comp_expr != "NULL::text" else ""
        caso = self._fetchone(
            f"""
            SELECT
                i.id_ics,
                {comp_expr} AS componente,
                COALESCE(NULLIF(r.ruta_comercial, ''), i.id_linea::text) AS ruta_comercial,
                i.km_prog_ad,
                i.km_elim_eic,
                i.km_ejecutado,
                i.offset_inicio,
                i.offset_fin,
                i.km_revision,
                STRING_AGG(DISTINCT rs.responsable, ' | ') AS responsables,
                STRING_AGG(DISTINCT me.motivo,       ' | ') AS motivos_eliminacion
              FROM sne.ics i
              {joins_cop}
              LEFT JOIN sne.ics_motivo_resp  imr ON imr.id_ics = i.id_ics
              LEFT JOIN sne.motivos_eliminacion me ON me.id    = imr.motivo
              LEFT JOIN sne.responsable_sne      rs ON rs.id   = imr.responsable
             WHERE i.id_ics = %s
             GROUP BY i.id_ics, i.id_linea, r.ruta_comercial,
                      i.km_prog_ad, i.km_elim_eic, i.km_ejecutado,
                      i.offset_inicio, i.offset_fin, i.km_revision{group_by_component}
            """,
            (id_ics,),
        )
        if not caso:
            raise ValueError(f"ICS {id_ics} no encontrado")

        reglas = self._evaluador.compilar_reglas(self.reglas_activas())
        return self._evaluador.trazar_caso(caso, reglas)
