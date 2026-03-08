import re, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from database.database_manager import get_db_connection

logger    = logging.getLogger("gestion_sne_plantillas")
TZ_BOGOTA = ZoneInfo("America/Bogota")

def _ahora() -> datetime:
    return datetime.now(TZ_BOGOTA)

# Regex para encontrar todos los {tokens} en una plantilla
_RE_TOKEN = re.compile(r"\{(\w+)\}")

class GestionSnePlantillas:
    """
    CRUD de tokens y plantillas + lógica de resolución de tokens.
    Sigue el mismo patrón de pool que el resto del proyecto.
    """

    # ── Inicialización y cierre del pool ─────────────────────────────────────
    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    # ══════════════════════════════════════════════════════════════════════════
    # TOKENS
    # ══════════════════════════════════════════════════════════════════════════

    def listar_tokens(self, solo_activos: bool = False):
        """Lista todos los tokens ordenados por orden ASC."""
        where = " WHERE activo = TRUE " if solo_activos else ""
        self.cursor.execute(
            f"""
            SELECT id, token, descripcion, campo_ics, sql_query,
                    requiere_input, input_label, activo, orden,
                    to_char(creado_en,      'YYYY-MM-DD HH24:MI') AS creado_en,
                    to_char(actualizado_en, 'YYYY-MM-DD HH24:MI') AS actualizado_en
            FROM sne.nota_tokens
            {where}
            ORDER BY orden ASC, id ASC
            """
        )
        return self.cursor.fetchall()

    def obtener_token(self, id_token: int):
        self.cursor.execute(
            """
            SELECT id, token, descripcion, campo_ics, sql_query,
                    requiere_input, input_label, activo, orden
            FROM sne.nota_tokens WHERE id = %s
            """,
            (id_token,),
        )
        row = self.cursor.fetchone()
        if not row:
            raise ValueError(f"Token {id_token} no encontrado")
        return row

    def crear_token(
        self,
        token: str,
        descripcion: str,
        campo_ics: str = None,
        sql_query: str = None,
        requiere_input: bool = False,
        input_label: str = None,
        orden: int = 99,
    ):
        token = token.strip()
        if not token.startswith("{") or not token.endswith("}"):
            raise ValueError("El token debe estar entre llaves: {mi_token}")
        if not campo_ics and not sql_query and not requiere_input:
            raise ValueError("El token debe tener campo_ics, sql_query o requiere_input=True")
        try:
            self.cursor.execute(
                """
                INSERT INTO sne.nota_tokens
                    (token, descripcion, campo_ics, sql_query,
                    requiere_input, input_label, orden)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, token, descripcion
                """,
                (token, descripcion.strip(), campo_ics or None,
                    sql_query or None, requiere_input,
                    input_label or None, orden),
            )
            row = self.cursor.fetchone()
            self.connection.commit()
            return row
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError(f"Ya existe un token con el nombre '{token}'")
        except Exception as e:
            self.connection.rollback()
            raise e

    def actualizar_token(
        self,
        id_token: int,
        descripcion: str,
        campo_ics: str = None,
        sql_query: str = None,
        requiere_input: bool = False,
        input_label: str = None,
        activo: bool = True,
        orden: int = 99,
    ):
        if not campo_ics and not sql_query and not requiere_input:
            raise ValueError("El token debe tener campo_ics, sql_query o requiere_input=True")
        try:
            self.cursor.execute(
                """
                UPDATE sne.nota_tokens
                SET descripcion    = %s,
                    campo_ics      = %s,
                    sql_query      = %s,
                    requiere_input = %s,
                    input_label    = %s,
                    activo         = %s,
                    orden          = %s,
                    actualizado_en = %s
                WHERE id = %s
                RETURNING id, token, descripcion, activo
                """,
                (descripcion.strip(), campo_ics or None, sql_query or None,
                    requiere_input, input_label or None, activo,
                    orden, _ahora(), id_token),
            )
            row = self.cursor.fetchone()
            if not row:
                raise ValueError(f"Token {id_token} no encontrado")
            self.connection.commit()
            return row
        except Exception as e:
            self.connection.rollback()
            raise e

    def probar_sql_token(self, sql_query: str, id_ics: int):
        """
        Ejecuta el sql_query del token con un id_ics de prueba.
        Retorna el valor resultante o un mensaje de error.
        Usado desde la pantalla de gestión para validar queries nuevas.
        """
        try:
            self.cursor.execute(sql_query, {"id_ics": id_ics})
            row = self.cursor.fetchone()
            if not row:
                return {"ok": False, "valor": None, "error": "La query no retornó resultados"}
            # Tomar el primer valor del primer campo
            valor = list(dict(row).values())[0]
            return {"ok": True, "valor": str(valor) if valor is not None else "NULL", "error": None}
        except Exception as e:
            self.connection.rollback()
            return {"ok": False, "valor": None, "error": str(e)}

    # ══════════════════════════════════════════════════════════════════════════
    # PLANTILLAS
    # ══════════════════════════════════════════════════════════════════════════

    def listar_plantillas(self, id_motivo: int = None):
        """
        Lista plantillas con nombre del motivo y creador.
        Si se pasa id_motivo, filtra por ese motivo (todas las versiones).
        """
        where, params = " WHERE 1=1 ", []
        if id_motivo:
            where += " AND np.id_motivo = %s "
            params.append(id_motivo)

        self.cursor.execute(
            f"""
            SELECT
                np.id,
                np.id_motivo,
                me.motivo                                             AS motivo_nombre,
                np.nombre,
                np.plantilla,
                np.activo,
                np.creado_por,
                u.nombres || ' ' || u.apellidos                      AS creador_nombre,
                to_char(np.creado_en,      'YYYY-MM-DD HH24:MI')     AS creado_en,
                to_char(np.actualizado_en, 'YYYY-MM-DD HH24:MI')     AS actualizado_en
            FROM sne.nota_plantillas np
            JOIN sne.motivos_eliminacion me ON me.id = np.id_motivo
            LEFT JOIN public.usuarios u     ON u.id  = np.creado_por
            {where}
            ORDER BY me.motivo ASC, np.activo DESC, np.actualizado_en DESC
            """,
            params,
        )
        return self.cursor.fetchall()

    def listar_motivos_con_estado_plantilla(self):
        """
        Retorna todos los motivos de eliminación con indicador de si
        tienen plantilla activa y cuántas versiones históricas hay.
        Usado en la pantalla de gestión para mostrar el estado completo.
        """
        self.cursor.execute(
            """
            SELECT
                me.id                                               AS id_motivo,
                me.motivo                                           AS motivo_nombre,
                rs.responsable                                      AS responsable_nombre,
                COUNT(np.id)          FILTER (WHERE np.activo)     AS plantillas_activas,
                COUNT(np.id)          FILTER (WHERE NOT np.activo) AS versiones_historicas,
                MAX(np.actualizado_en)                              AS ultima_actualizacion,
                (SELECT np2.id
                    FROM sne.nota_plantillas np2
                    WHERE np2.id_motivo = me.id AND np2.activo = TRUE
                    ORDER BY np2.actualizado_en DESC LIMIT 1)         AS id_plantilla_activa
            FROM sne.motivos_eliminacion me
            LEFT JOIN sne.responsable_sne rs ON rs.id = me.responsable
            LEFT JOIN sne.nota_plantillas  np ON np.id_motivo = me.id
            GROUP BY me.id, me.motivo, rs.responsable
            ORDER BY me.motivo ASC
            """
        )
        return self.cursor.fetchall()

    def obtener_plantilla(self, id_plantilla: int):
        self.cursor.execute(
            """
            SELECT
                np.id, np.id_motivo, me.motivo AS motivo_nombre,
                np.nombre, np.plantilla, np.activo,
                to_char(np.creado_en,      'YYYY-MM-DD HH24:MI') AS creado_en,
                to_char(np.actualizado_en, 'YYYY-MM-DD HH24:MI') AS actualizado_en
            FROM sne.nota_plantillas np
            JOIN sne.motivos_eliminacion me ON me.id = np.id_motivo
            WHERE np.id = %s
            """,
            (id_plantilla,),
        )
        row = self.cursor.fetchone()
        if not row:
            raise ValueError(f"Plantilla {id_plantilla} no encontrada")
        return row

    def guardar_plantilla(
        self,
        id_motivo: int,
        plantilla: str,
        nombre: str = "Plantilla estándar",
        usuario_id: int = None,
    ):
        """
        Crea una nueva versión activa de la plantilla para el motivo dado.
        Desactiva cualquier versión anterior activa del mismo motivo
        (historial sin borrar).
        """
        plantilla = plantilla.strip()
        if not plantilla:
            raise ValueError("La plantilla no puede estar vacía")

        # Verificar que el motivo existe
        self.cursor.execute(
            "SELECT id FROM sne.motivos_eliminacion WHERE id = %s", (id_motivo,)
        )
        if not self.cursor.fetchone():
            raise ValueError(f"Motivo {id_motivo} no encontrado")

        try:
            # Desactivar versiones anteriores
            self.cursor.execute(
                """
                UPDATE sne.nota_plantillas
                SET activo = FALSE, actualizado_en = %s
                WHERE id_motivo = %s AND activo = TRUE
                """,
                (_ahora(), id_motivo),
            )
            # Insertar nueva versión activa
            self.cursor.execute(
                """
                INSERT INTO sne.nota_plantillas
                    (id_motivo, nombre, plantilla, activo, creado_por)
                VALUES (%s, %s, %s, TRUE, %s)
                RETURNING id, id_motivo, nombre, activo
                """,
                (id_motivo, nombre.strip(), plantilla, usuario_id),
            )
            row = self.cursor.fetchone()
            self.connection.commit()
            return row
        except Exception as e:
            self.connection.rollback()
            raise e

    def eliminar_plantilla(self, id_plantilla: int):
        """Elimina físicamente una plantilla (solo permitir sobre inactivas)."""
        try:
            self.cursor.execute(
                """
                DELETE FROM sne.nota_plantillas
                WHERE id = %s AND activo = FALSE
                RETURNING id
                """,
                (id_plantilla,),
            )
            row = self.cursor.fetchone()
            if not row:
                raise ValueError(
                    "Solo se pueden eliminar plantillas inactivas (históricas)"
                )
            self.connection.commit()
            return {"id": row["id"], "eliminado": True}
        except ValueError:
            raise
        except Exception as e:
            self.connection.rollback()
            raise e

    # ══════════════════════════════════════════════════════════════════════════
    # RESOLUCIÓN DE TOKENS — núcleo del módulo
    # ══════════════════════════════════════════════════════════════════════════

    def resolver_plantilla(self, id_motivo: int, id_ics: int):
        """
        Obtiene la plantilla activa del motivo y resuelve todos los tokens
        que tengan fuente automática (campo_ics o sql_query).

        Retorna:
        {
            "plantilla_id":       int | None,
            "texto_base":         str,        ← plantilla original con tokens
            "texto_resuelto":     str,        ← tokens automáticos ya sustituidos
            "tokens_pendientes":  [           ← tokens que el revisor debe llenar
                {"token": "{parada1}", "label": "Parada / punto inicial"},
                ...
            ]
        }
        """
        # 1. Obtener plantilla activa
        self.cursor.execute(
            """
            SELECT np.id, np.plantilla
            FROM sne.nota_plantillas np
            WHERE np.id_motivo = %s AND np.activo = TRUE
            ORDER BY np.actualizado_en DESC
            LIMIT 1
            """,
            (id_motivo,),
        )
        plantilla_row = self.cursor.fetchone()
        if not plantilla_row:
            return {
                "plantilla_id":      None,
                "texto_base":        "",
                "texto_resuelto":    "",
                "tokens_pendientes": [],
                "sin_plantilla":     True,
            }

        texto_base    = plantilla_row["plantilla"]
        plantilla_id  = plantilla_row["id"]

        # 2. Obtener datos del ICS
        ics_data = self._obtener_datos_ics(id_ics)

        # 3. Obtener todos los tokens activos
        self.cursor.execute(
            """
            SELECT token, campo_ics, sql_query, requiere_input, input_label
            FROM sne.nota_tokens
            WHERE activo = TRUE
            ORDER BY orden ASC
            """
        )
        tokens_bd = {r["token"]: r for r in self.cursor.fetchall()}

        # 4. Encontrar qué tokens usa esta plantilla
        tokens_en_plantilla = _RE_TOKEN.findall(texto_base)  # ['ruta','tabla',…]

        texto_resuelto   = texto_base
        tokens_pendientes = []

        # Tokens dinámicos que el frontend reemplaza con valores de pantalla.
        # Se dejan intactos en texto_resuelto — NO se agregan a tokens_pendientes.
        _TOKENS_FRONTEND = {"{km_objetado}", "{km_no_objetado}"}

        for nombre_token in set(tokens_en_plantilla):
            placeholder = "{" + nombre_token + "}"

            # Tokens dinámicos de frontend: dejar intactos
            if placeholder in _TOKENS_FRONTEND:
                continue

            token_info  = tokens_bd.get(placeholder)

            if token_info is None:
                # Token desconocido → reportar como pendiente para input manual
                tokens_pendientes.append({
                    "token": placeholder,
                    "label": nombre_token.replace("_", " ").title(),
                })
                continue

            if token_info["requiere_input"]:
                tokens_pendientes.append({
                    "token": placeholder,
                    "label": token_info["input_label"] or nombre_token,
                })
                continue

            # Resolver automáticamente
            valor = self._resolver_token_auto(token_info, ics_data, id_ics)
            if valor is not None:
                texto_resuelto = texto_resuelto.replace(placeholder, str(valor))

        return {
            "plantilla_id":      plantilla_id,
            "texto_base":        texto_base,
            "texto_resuelto":    texto_resuelto,
            "tokens_pendientes": tokens_pendientes,
            "sin_plantilla":     False,
        }

    def _obtener_datos_ics(self, id_ics: int) -> dict:
        """
        Obtiene todos los campos del ICS + joins para resolver tokens.

        Campos expuestos para los tokens canónicos:
            id_ics, fecha, servicio, tabla, id_viaje, sentido,
            hora_ini_teorica (HH:MM), km_revision, vehiculo_real,
            conductor, ruta_comercial
        """
        self.cursor.execute(
            """
            SELECT
                i.id_ics,
                i.fecha::text                                                AS fecha,
                i.id_linea,
                i.servicio,
                i.tabla,
                i.id_viaje,
                i.sentido,
                i.vehiculo_real,
                -- Hora formateada HH:MM (sin segundos)
                to_char(i.hora_ini_teorica, 'HH24:MI')                      AS hora_ini_teorica,
                i.conductor,
                i.km_revision,
                -- Ruta comercial: JOIN con config.rutas por id_linea
                COALESCE(r.ruta_comercial, i.id_linea::text)                AS ruta_comercial
            FROM sne.ics i
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            WHERE i.id_ics = %s
            LIMIT 1
            """,
            (id_ics,),
        )
        row = self.cursor.fetchone()
        if not row:
            return {}

        data = dict(row)

        # Formato colombiano para km_revision (punto miles, coma decimal)
        if data.get("km_revision") is not None:
            try:
                v = float(data["km_revision"])
                data["km_revision"] = (
                    f"{v:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
                )
            except (TypeError, ValueError):
                pass

        return data

    def _resolver_token_auto(self, token_info: dict, ics_data: dict, id_ics: int):
        """
        Resuelve el valor de un token automático.
        Prioridad: campo_ics directo → sql_query.
        """
        # Opción A: campo directo del ICS
        if token_info["campo_ics"]:
            valor = ics_data.get(token_info["campo_ics"])
            if valor is not None:
                # km_revision ya viene pre-formateado como string desde _obtener_datos_ics
                # Para otros floats que pudieran llegar, aplicar formato colombiano
                if isinstance(valor, float):
                    return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
                return str(valor)
            return None

        # Opción B: sql_query parametrizado
        if token_info["sql_query"]:
            try:
                self.cursor.execute(token_info["sql_query"], {"id_ics": id_ics})
                row = self.cursor.fetchone()
                if row:
                    valor = list(dict(row).values())[0]
                    return str(valor) if valor is not None else None
            except Exception as ex:
                logger.warning(
                    f"Error resolviendo sql_query para token: {ex}"
                )
            return None

        return None

    # ══════════════════════════════════════════════════════════════════════════
    # UTILIDADES PARA LA PANTALLA DE GESTIÓN
    # ══════════════════════════════════════════════════════════════════════════

    def obtener_ics_de_prueba(self):
        """Retorna un id_ics con datos completos para la pestaña de prueba."""
        self.cursor.execute(
            """
            SELECT i.id_ics, i.fecha::text AS fecha,
                    COALESCE(r.ruta_comercial, i.id_linea::text) AS ruta
            FROM sne.ics i
            LEFT JOIN config.rutas r ON r.id_linea = i.id_linea AND r.estado = 1
            WHERE i.km_revision IS NOT NULL
            AND i.km_revision > 0
            ORDER BY i.fecha DESC, i.id_ics DESC
            LIMIT 20
            """
        )
        return self.cursor.fetchall()

    def tokens_en_plantilla(self, texto: str):
        """
        Analiza un texto y retorna la lista de tokens encontrados
        con su metadata. Usado para la vista previa en tiempo real.
        """
        nombres = list(set(_RE_TOKEN.findall(texto)))
        if not nombres:
            return []
        placeholders = ["{" + n + "}" for n in nombres]

        self.cursor.execute(
            """
            SELECT token, descripcion, requiere_input, input_label, activo
            FROM sne.nota_tokens
            WHERE token = ANY(%s)
            ORDER BY orden ASC
            """,
            (placeholders,),
        )
        encontrados = {r["token"]: r for r in self.cursor.fetchall()}

        resultado = []
        for nombre in nombres:
            ph = "{" + nombre + "}"
            if ph in encontrados:
                resultado.append(encontrados[ph])
            else:
                resultado.append({
                    "token":          ph,
                    "descripcion":    f"Token no registrado: {nombre}",
                    "requiere_input": True,
                    "input_label":    nombre,
                    "activo":         False,
                })
        return resultado