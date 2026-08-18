import os, json, logging
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, Tuple
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import get_db_connection

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_BITACORA = "b01-gestion-express"
CARPETA_BITACORA   = "bitacora_mantenimiento"

TZ_BOGOTA = ZoneInfo("America/Bogota")
logger = logging.getLogger(__name__)

_CONTENT_TYPES = {
    "pdf":  "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls":  "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc":  "application/msword",
    "csv":  "text/csv",
    "txt":  "text/plain",
    "png":  "image/png",
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "gif":  "image/gif",
    "webp": "image/webp",
    "mp4":  "video/mp4",
}


def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)


# Mensaje único para la regla de buses no vinculados / inactivos
MENSAJE_BUS_INACTIVO = (
    "No es posible gestionar este bus porque no está vinculado o está inactivo. "
    "Solicite al Proceso de Programación el cambio de estado del bus para poder gestionar."
)


# ═════════════════════════════════════════════════════════════════════════════
#  DDL — SCHEMA "mantenimiento"
# ═════════════════════════════════════════════════════════════════════════════
# Catálogos de listas desplegables + tabla transaccional de la bitácora.
# Todo es idempotente (IF NOT EXISTS / ON CONFLICT DO NOTHING) y se ejecuta de
# forma perezosa en la primera operación del modelo.
_DDL_BITACORA = """
CREATE SCHEMA IF NOT EXISTS mantenimiento;

CREATE TABLE IF NOT EXISTS mantenimiento.sistema_funcional (
    id                SERIAL PRIMARY KEY,
    sistema_funcional TEXT     NOT NULL UNIQUE,
    orden             SMALLINT NOT NULL DEFAULT 0,
    estado            SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS mantenimiento.causa_entrada_inoperativo (
    id                         SERIAL PRIMARY KEY,
    causa_entrada_inoperativo  TEXT     NOT NULL UNIQUE,
    orden                      SMALLINT NOT NULL DEFAULT 0,
    estado                     SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS mantenimiento.estado_pendiente_actual (
    id                      SERIAL PRIMARY KEY,
    estado_pendiente_actual TEXT     NOT NULL UNIQUE,
    orden                   SMALLINT NOT NULL DEFAULT 0,
    estado                  SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS mantenimiento.ubicacion (
    id        SERIAL PRIMARY KEY,
    ubicacion TEXT     NOT NULL UNIQUE,
    orden     SMALLINT NOT NULL DEFAULT 0,
    estado    SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS mantenimiento.estado_disponibilidad (
    id                    SERIAL PRIMARY KEY,
    estado_disponibilidad TEXT     NOT NULL UNIQUE,
    orden                 SMALLINT NOT NULL DEFAULT 0,
    estado                SMALLINT NOT NULL DEFAULT 1
);

-- Tabla transaccional: cada guardado es un registro nuevo (histórico de cambios).
CREATE TABLE IF NOT EXISTS mantenimiento.bitacora_mtto_guardada (
    id                           BIGSERIAL PRIMARY KEY,
    fecha                        DATE      NOT NULL DEFAULT (now() AT TIME ZONE 'America/Bogota')::date,
    hora                         TIME,
    id_bus                       BIGINT    NOT NULL REFERENCES config.buses_cexp (id),
    novedad                      TEXT,
    id_sistema_funcional         INTEGER   REFERENCES mantenimiento.sistema_funcional (id),
    id_causa_entrada_inoperativo INTEGER   REFERENCES mantenimiento.causa_entrada_inoperativo (id),
    id_estado_pendiente_actual   INTEGER   REFERENCES mantenimiento.estado_pendiente_actual (id),
    id_ubicacion                 INTEGER   REFERENCES mantenimiento.ubicacion (id),
    id_estado_disponibilidad     INTEGER   REFERENCES mantenimiento.estado_disponibilidad (id),
    ot_sap_pm                    BIGINT,
    reserva_sap_mm               BIGINT,
    fecha_inoperativo_mtto       DATE,
    costo                        NUMERIC(16,2),
    fecha_cumplible_presentacion DATE,
    fecha_ingreso_cein           DATE,
    dias_inoperativo             INTEGER,
    id_usuario_registra          BIGINT,
    fecha_guardado               TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'America/Bogota'),
    ruta_archivos                JSONB     NOT NULL DEFAULT '[]'::jsonb
);

-- Instalaciones previas a la hora del cambio
ALTER TABLE mantenimiento.bitacora_mtto_guardada ADD COLUMN IF NOT EXISTS hora TIME;

-- Días de inoperatividad calculados al guardar cada gestión
ALTER TABLE mantenimiento.bitacora_mtto_guardada ADD COLUMN IF NOT EXISTS dias_inoperativo INTEGER;

CREATE INDEX IF NOT EXISTS idx_bitacora_mtto_bus    ON mantenimiento.bitacora_mtto_guardada (id_bus);
CREATE INDEX IF NOT EXISTS idx_bitacora_mtto_fecha  ON mantenimiento.bitacora_mtto_guardada (fecha DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_bitacora_mtto_bus_fe ON mantenimiento.bitacora_mtto_guardada (id_bus, fecha DESC, id DESC);
"""

# Parametrización por usuario del módulo de bitácora:
#   · usuario_cop    → centros de operación que puede ver cada usuario
#   · usuario_config → permiso para registrar gestiones con fecha retroactiva
_DDL_USUARIO_COP = """
CREATE SCHEMA IF NOT EXISTS mantenimiento;

CREATE TABLE IF NOT EXISTS mantenimiento.usuario_cop (
    id        BIGSERIAL   PRIMARY KEY,
    user_id   BIGINT      NOT NULL REFERENCES usuarios (id) ON DELETE CASCADE,
    id_cop    BIGINT      NOT NULL REFERENCES config.cop (id),
    creado_en TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, id_cop)
);

CREATE INDEX IF NOT EXISTS idx_usuario_cop_user ON mantenimiento.usuario_cop (user_id);

CREATE TABLE IF NOT EXISTS mantenimiento.usuario_config (
    user_id                   BIGINT      PRIMARY KEY REFERENCES usuarios (id) ON DELETE CASCADE,
    permite_fecha_retroactiva SMALLINT    NOT NULL DEFAULT 0,
    actualizado_en            TIMESTAMPTZ DEFAULT NOW()
);
"""

# Días hacia atrás permitidos sin autorización especial para la Fecha del Cambio
DIAS_RETROACTIVO_PERMITIDOS = 2

MENSAJE_FECHA_RETROACTIVA = (
    "No es posible registrar gestiones con fecha de más de "
    f"{DIAS_RETROACTIVO_PERMITIDOS} días de antigüedad. "
    "Comuníquese con el área de Programación de Mantenimiento."
)

MENSAJE_SIN_COPS = (
    "No tiene centros de operación asignados para visualizar la flota. "
    "Solicite al administrador la parametrización de sus COP en el registro de usuario."
)

# Valores iniciales de los catálogos (el orden define la posición en el desplegable)
_CATALOGOS_SEED = {
    "sistema_funcional": [
        "Admisión y Escape", "Caja de Velocidades", "Carrocería", "Dirección",
        "Ejes y Transmisión", "Eléctrico", "Frenos", "Inyección de Combustible",
        "Llantas", "Motor", "Mtto Preventivo", "Multiplexado", "Neumático",
        "Puertas", "Sirci", "Refrigeración", "Suspensión", "Estructura Chasis",
        "Patios Movilidad - URI", "Req. Administrativo",
    ],
    "causa_entrada_inoperativo": [
        "Revisión Pre_Post", "Colisión", "Daño Operacional", "Falla Técnica",
        "Garantía_Campaña", "Mtto Preventivo", "QA y Confiabilidad",
        "Patios Movilidad - URI", "Chatarrización", "Req. Administrativo",
        "Inmovilización BRT", "Extensión de vida útil",
    ],
    "estado_pendiente_actual": [
        "Herramientas", "Trabajo en Proceso", "Esperando Intervención",
        "Proveedor Externo", "Repuestos", "Req. Administrativo", "Mtto Preventivo",
        "Sirci", "Patios Movilidad - URI", "Proveedor in house",
    ],
    "ubicacion": [
        "20 de Julio - Troncal", "20 de Julio - Dual", "20 de Julio - Alimentación",
        "CEIM - Cruces", "Talleres Externos (Especificar cual)", "Patios Movilidad - URI",
        "Usaquen", "Toberin", "191 Padron", "Suba", "Conejera", "Engativa", "Bosa",
        "San Francisco", "Cruces", "Gaviotas A", "Gaviotas B", "Juan Rey",
        "Taller Externo (Cummins de los Andes)", "Taller Externo (Stewart Stevenson)",
        "Taller Externo (Américas)", "Mectronics",
    ],
    "estado_disponibilidad": [
        "Operativo", "Inoperativo",
    ],
}

# Ajustes sobre catálogos ya cargados en producción. Se ejecutan antes de sembrar
# para que los renombres no choquen con los valores nuevos.
#   Disponible    → Operativo
#   Inoperable    → Inoperativo
#   Mantenimiento → se elimina; si tiene gestiones históricas se desactiva para
#                   no romper la integridad referencial (deja de verse en las listas).
_MIGRACION_ESTADO_DISPONIBILIDAD = """
UPDATE mantenimiento.estado_disponibilidad
SET estado_disponibilidad = 'Operativo'
WHERE estado_disponibilidad = 'Disponible'
  AND NOT EXISTS (
      SELECT 1 FROM mantenimiento.estado_disponibilidad
      WHERE estado_disponibilidad = 'Operativo'
  );

UPDATE mantenimiento.estado_disponibilidad
SET estado_disponibilidad = 'Inoperativo'
WHERE estado_disponibilidad = 'Inoperable'
  AND NOT EXISTS (
      SELECT 1 FROM mantenimiento.estado_disponibilidad
      WHERE estado_disponibilidad = 'Inoperativo'
  );

UPDATE mantenimiento.estado_disponibilidad
SET estado = 0
WHERE estado_disponibilidad = 'Mantenimiento';

DELETE FROM mantenimiento.estado_disponibilidad ed
WHERE ed.estado_disponibilidad = 'Mantenimiento'
  AND NOT EXISTS (
      SELECT 1 FROM mantenimiento.bitacora_mtto_guardada g
      WHERE g.id_estado_disponibilidad = ed.id
  );
"""

# Campo de texto de cada catálogo (coincide con el nombre de la tabla)
_CATALOGOS = list(_CATALOGOS_SEED.keys())


class ConfiguracionUsuarioBitacora:
    """
    Parametrización por usuario del módulo de Bitácora de Mantenimiento.

    Vive aparte de GestionEstadoBitacoraMtto porque la usa también la pantalla de
    registro de usuarios, que no necesita el resto del modelo.

      · COP asignados            → qué flota puede ver y gestionar el usuario
      · permite_fecha_retroactiva → si puede registrar con fecha de días pasados
    """

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        self._creada = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    def _asegurar(self):
        if self._creada:
            return
        self.cursor.execute(_DDL_USUARIO_COP)
        self.connection.commit()
        self._creada = True

    # ── Catálogo en cascada para la pantalla de usuarios ──────────────
    def arbol_cops(self) -> List[Dict[str, Any]]:
        """COPs activos con su componente y zona, para armar los desplegables."""
        sql = """
            SELECT
                c.id            AS id_cop,
                c.cop,
                c.id_componente,
                comp.componente,
                c.id_zona,
                z.zona
            FROM config.cop c
            LEFT JOIN config.componente comp ON comp.id = c.id_componente
            LEFT JOIN config.zona       z    ON z.id    = c.id_zona
            WHERE c.estado = 1
            ORDER BY comp.componente, z.zona, c.cop;
        """
        self.cursor.execute(sql)
        return [dict(r) for r in self.cursor.fetchall()]

    # ── COPs del usuario ──────────────────────────────────────────────
    def cops_usuario(self, user_id: int) -> List[int]:
        self._asegurar()
        self.cursor.execute(
            "SELECT id_cop FROM mantenimiento.usuario_cop WHERE user_id = %s ORDER BY id_cop;",
            [int(user_id)],
        )
        return [r["id_cop"] for r in self.cursor.fetchall()]

    def reemplazar_cops(self, user_id: int, ids_cop: List[int]) -> None:
        """Deja exactamente los COP indicados (set completo)."""
        self._asegurar()
        ids = sorted({int(i) for i in (ids_cop or []) if i})

        self.cursor.execute(
            "DELETE FROM mantenimiento.usuario_cop WHERE user_id = %s;", [int(user_id)]
        )
        for id_cop in ids:
            self.cursor.execute(
                """
                INSERT INTO mantenimiento.usuario_cop (user_id, id_cop)
                VALUES (%s, %s)
                ON CONFLICT (user_id, id_cop) DO NOTHING;
                """,
                [int(user_id), id_cop],
            )
        self.connection.commit()

    # ── Permiso de fecha retroactiva ──────────────────────────────────
    def permite_fecha_retroactiva(self, user_id: int) -> bool:
        self._asegurar()
        self.cursor.execute(
            """
            SELECT permite_fecha_retroactiva
            FROM mantenimiento.usuario_config
            WHERE user_id = %s;
            """,
            [int(user_id)],
        )
        fila = self.cursor.fetchone()
        return bool(fila and int(fila.get("permite_fecha_retroactiva") or 0) == 1)

    def guardar_config(self, user_id: int, permite_fecha_retroactiva: bool) -> None:
        self._asegurar()
        self.cursor.execute(
            """
            INSERT INTO mantenimiento.usuario_config (user_id, permite_fecha_retroactiva, actualizado_en)
            VALUES (%s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET permite_fecha_retroactiva = EXCLUDED.permite_fecha_retroactiva,
                actualizado_en            = NOW();
            """,
            [int(user_id), 1 if permite_fecha_retroactiva else 0],
        )
        self.connection.commit()

    def configuracion_usuario(self, user_id: int) -> Dict[str, Any]:
        """Todo lo parametrizado para un usuario, en una sola llamada."""
        return {
            "cops": self.cops_usuario(user_id),
            "permite_fecha_retroactiva": self.permite_fecha_retroactiva(user_id),
        }


class GestionEstadoBitacoraMtto:
    """
    Bitácora de Mantenimiento de la flota (módulo Gestión de Flota).

    Estructura de datos — schema "mantenimiento":
      - sistema_funcional            → catálogo de sistemas funcionales
      - causa_entrada_inoperativo    → catálogo de causas de entrada a inoperativo
      - estado_pendiente_actual      → catálogo de estados pendientes
      - ubicacion                    → catálogo de ubicaciones / talleres
      - estado_disponibilidad        → catálogo de disponibilidad (Disponible/Inoperable/Mantenimiento)
      - bitacora_mtto_guardada       → transaccional; cada guardado es un registro nuevo,
                                       lo que constituye el histórico de cambios del bus.

    Datos de apoyo (schema "config"):
      - buses_cexp, cop, componente, zona   → flota y ubicación organizacional
      - km_recorrido_bus                    → posicionamiento GPS (movil_bus, fecha, dist_final_km)
      - km_fms_bus                          → km ejecutado FMS Comercial (vehiculo_real, fecha, km_ejecutado)

    Evidencias: Azure Blob Storage, contenedor "b01-gestion-express",
    ruta  bitacora_mantenimiento/<PLACA>/<AAAA>/<MM>/<timestamp>_<archivo>.
    """

    # =========================================================
    # CICLO DE VIDA DE LA CONEXIÓN
    # =========================================================

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        self._estructura_creada = False
        self._expr_hab = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "cursor", None):
            try:
                self.cursor.close()
            except Exception:
                pass
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # =========================================================
    # ESTRUCTURA (schema + tablas + catálogos base)
    # =========================================================

    def _asegurar_estructura(self):
        """Crea schema, tablas y siembra catálogos una sola vez por instancia."""
        if self._estructura_creada:
            return
        self.cursor.execute(_DDL_BITACORA)
        self.cursor.execute(_DDL_USUARIO_COP)
        self.cursor.execute(_MIGRACION_ESTADO_DISPONIBILIDAD)
        for tabla, valores in _CATALOGOS_SEED.items():
            for orden, valor in enumerate(valores, start=1):
                self.cursor.execute(
                    f"""
                    INSERT INTO mantenimiento.{tabla} ({tabla}, orden)
                    VALUES (%s, %s)
                    ON CONFLICT ({tabla}) DO NOTHING
                    """,
                    (valor, orden),
                )
        self.connection.commit()
        self._estructura_creada = True

    # =========================================================
    # UTILIDADES
    # =========================================================

    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _fetchall(self, sql: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        self.cursor.execute(sql, params or [])
        return self.cursor.fetchall()

    def _fetchone(self, sql: str, params: Optional[list] = None) -> Optional[Dict[str, Any]]:
        self.cursor.execute(sql, params or [])
        return self.cursor.fetchone()

    # Campos que solo aplican cuando el bus queda Inoperativo. En estado Operativo
    # se guardan en NULL y todas las lecturas los reportan como "No Aplica"
    # (no pueden llevar ese texto: son enteros, fechas, FK y numérico).
    CAMPOS_SOLO_INOPERATIVO = [
        "id_sistema_funcional", "id_causa_entrada_inoperativo",
        "id_estado_pendiente_actual", "id_ubicacion",
        "ot_sap_pm", "reserva_sap_mm", "fecha_inoperativo_mtto", "costo",
        "fecha_cumplible_presentacion", "fecha_ingreso_cein",
    ]

    def es_estado_operativo(self, id_estado_disponibilidad: Optional[int]) -> bool:
        """True si el id corresponde al estado 'Operativo' del catálogo."""
        if not id_estado_disponibilidad:
            return False
        fila = self._fetchone(
            """
            SELECT estado_disponibilidad
            FROM mantenimiento.estado_disponibilidad
            WHERE id = %s;
            """,
            [id_estado_disponibilidad],
        )
        nombre = (fila or {}).get("estado_disponibilidad") or ""
        return nombre.strip().lower() == "operativo"

    # =========================================================
    # DÍAS INOPERATIVO
    # =========================================================
    # El contador arranca con la Fecha Inoperativo Mtto de la gestión que deja el
    # bus Inoperativo y se detiene en la gestión que lo devuelve a Operativo:
    #   · Inoperativo → fecha de la gestión − fecha_inoperativo_mtto (sigue corriendo
    #     en la grilla contra el día de hoy).
    #   · Paso a Operativo → fecha del cambio − fecha_inoperativo_mtto del ciclo
    #     abierto; ese total queda guardado y el contador se reinicia en cero.
    #   · Operativo sin ciclo abierto → cero.

    def _ciclos_abiertos(self, ids_bus: List[int]) -> Dict[int, Optional[date]]:
        """
        {id_bus: fecha_inoperativo_mtto} de la última gestión, solo para los buses
        que quedaron Inoperativos (ciclo de inoperatividad abierto).
        """
        if not ids_bus:
            return {}

        sql = """
            SELECT DISTINCT ON (g.id_bus)
                g.id_bus,
                g.fecha_inoperativo_mtto,
                (LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'inoperativo') AS abierto
            FROM mantenimiento.bitacora_mtto_guardada g
            LEFT JOIN mantenimiento.estado_disponibilidad ed ON ed.id = g.id_estado_disponibilidad
            WHERE g.id_bus = ANY(%s)
            ORDER BY g.id_bus, g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC;
        """
        filas = self._fetchall(sql, [list(ids_bus)])
        return {
            f["id_bus"]: f["fecha_inoperativo_mtto"]
            for f in filas
            if f.get("abierto") and f.get("fecha_inoperativo_mtto")
        }

    def validar_fecha_retroactiva(self, fecha: Optional[date], permitido: bool = False) -> None:
        """
        La Fecha del Cambio no puede tener más de DIAS_RETROACTIVO_PERMITIDOS días
        de antigüedad, salvo que el usuario tenga habilitado el permiso.
        """
        if permitido or not fecha:
            return
        limite = ahora_bogota().date() - timedelta(days=DIAS_RETROACTIVO_PERMITIDOS)
        if fecha < limite:
            raise ValueError(MENSAJE_FECHA_RETROACTIVA)

    @staticmethod
    def _dias_entre(desde: Optional[date], hasta: Optional[date]) -> int:
        """Días completos entre dos fechas, nunca negativos."""
        if not desde or not hasta:
            return 0
        if isinstance(desde, datetime):
            desde = desde.date()
        if isinstance(hasta, datetime):
            hasta = hasta.date()
        return max(0, (hasta - desde).days)

    def _existe_tabla(self, nombre: str) -> bool:
        """La tabla de inmovilizados la crea el job; puede no existir aún."""
        fila = self._fetchone("SELECT to_regclass(%s) IS NOT NULL AS existe;", [nombre])
        return bool(fila and fila.get("existe"))

    def _existe_columna(self, tabla: str, columna: str) -> bool:
        fila = self._fetchone(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = split_part(%s, '.', 1)
                  AND table_name   = split_part(%s, '.', 2)
                  AND column_name  = %s
            ) AS existe;
            """,
            [tabla, tabla, columna],
        )
        return bool(fila and fila.get("existe"))

    def _fin_inmovilizacion(self) -> str:
        """
        Expresión con la fecha en que terminó la inmovilización.

        El archivo de TMSA nunca entrega la habilitación: el bus habilitado
        simplemente deja de aparecer, y el job cierra el ciclo dejando la fecha
        en `fecha_habilitacion_estimada`. Se toma la real si algún día llega y,
        si no, la estimada. Mientras el job no haya creado la columna, se
        conserva el comportamiento anterior para no romper la pantalla.
        """
        if self._expr_hab is None:
            self._expr_hab = (
                "COALESCE(i.fecha_habilitacion, i.fecha_habilitacion_estimada)"
                if self._existe_columna("mantenimiento.buses_inmovilizados",
                                        "fecha_habilitacion_estimada")
                else "i.fecha_habilitacion"
            )
        return self._expr_hab

    # =========================================================
    # FILTROS DE FLOTA
    # =========================================================

    def filtros_tipologia(self) -> List[Dict]:
        sql = """
            SELECT DISTINCT tipologia
            FROM config.buses_cexp
            WHERE tipologia IS NOT NULL
            ORDER BY tipologia ASC;
        """
        return self._fetchall(sql, [])

    def filtros_linea(self) -> List[Dict]:
        sql = """
            SELECT DISTINCT linea
            FROM config.buses_cexp
            WHERE linea IS NOT NULL AND linea <> ''
            ORDER BY linea ASC;
        """
        return self._fetchall(sql, [])

    def filtros_combustible(self) -> List[Dict]:
        sql = """
            SELECT DISTINCT combustible
            FROM config.buses_cexp
            WHERE combustible IS NOT NULL
            ORDER BY combustible ASC;
        """
        return self._fetchall(sql, [])

    def filtros_componente(self, cops_permitidos: Optional[List[int]] = None) -> List[Dict]:
        """Componentes con buses, limitados a los COP habilitados para el usuario."""
        sql = """
            SELECT DISTINCT comp.id, comp.componente
            FROM config.componente comp
            INNER JOIN config.cop c        ON c.id_componente = comp.id
            INNER JOIN config.buses_cexp b ON b.id_cop        = c.id
            WHERE comp.estado = 1
              AND (%s::bigint[] IS NULL OR c.id = ANY(%s))
            ORDER BY comp.componente ASC;
        """
        cops = list(cops_permitidos) if cops_permitidos else None
        return self._fetchall(sql, [cops, cops])

    def filtros_zona(self, id_componente: Optional[int] = None,
                     cops_permitidos: Optional[List[int]] = None) -> List[Dict]:
        """Zonas activas; si id_componente, sólo las zonas de ese componente."""
        sql = """
            SELECT DISTINCT z.id, z.zona
            FROM config.zona z
            INNER JOIN config.cop c        ON c.id_zona = z.id
            INNER JOIN config.buses_cexp b ON b.id_cop  = c.id
            WHERE z.estado = 1
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint[] IS NULL OR c.id = ANY(%s))
            ORDER BY z.zona ASC;
        """
        cops = list(cops_permitidos) if cops_permitidos else None
        return self._fetchall(sql, [id_componente, id_componente, cops, cops])

    def filtros_cop(
        self,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        cops_permitidos: Optional[List[int]] = None,
    ) -> List[Dict]:
        """COPs activos con buses; filtrables por componente y/o zona."""
        sql = """
            SELECT DISTINCT c.id, c.cop
            FROM config.cop c
            INNER JOIN config.buses_cexp b ON b.id_cop = c.id
            WHERE c.estado = 1
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
              AND (%s::bigint[] IS NULL OR c.id = ANY(%s))
            ORDER BY c.cop ASC;
        """
        cops = list(cops_permitidos) if cops_permitidos else None
        return self._fetchall(sql, [id_componente, id_componente, id_zona, id_zona, cops, cops])

    # =========================================================
    # CATÁLOGOS DEL MÓDULO (listas desplegables)
    # =========================================================

    def catalogo(self, nombre: str) -> List[Dict]:
        """
        Retorna [{id, <nombre>}] de un catálogo del schema mantenimiento.
        `nombre` debe pertenecer a la lista blanca _CATALOGOS.
        """
        if nombre not in _CATALOGOS:
            raise ValueError(f"Catálogo no válido: {nombre}")
        self._asegurar_estructura()
        sql = f"""
            SELECT id, {nombre}
            FROM mantenimiento.{nombre}
            WHERE estado = 1
            ORDER BY orden ASC, {nombre} ASC;
        """
        return self._fetchall(sql, [])

    def catalogos_todos(self) -> Dict[str, List[Dict]]:
        """Todos los catálogos del módulo en una sola llamada."""
        return {nombre: self.catalogo(nombre) for nombre in _CATALOGOS}

    # =========================================================
    # GRILLA PRINCIPAL
    # =========================================================

    def listar_bitacora(
        self,
        pagina: int = 1,
        tamano: int = 5000,
        placa: Optional[str] = None,
        no_interno: Optional[str] = None,
        tipologia: Optional[str] = None,
        linea: Optional[str] = None,
        combustible: Optional[str] = None,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        id_cop: Optional[int] = None,
        estado: Optional[int] = None,
        id_sistema_funcional: Optional[int] = None,
        id_causa_entrada_inoperativo: Optional[int] = None,
        id_estado_pendiente_actual: Optional[int] = None,
        id_ubicacion: Optional[int] = None,
        id_estado_disponibilidad: Optional[int] = None,
        inmovilizado_tmsa: Optional[str] = None,
        fecha_km_inicio: Optional[date] = None,
        fecha_km_fin: Optional[date] = None,
        cops_permitidos: Optional[List[int]] = None,
    ) -> Tuple[List[Dict], int, Dict[str, str]]:
        """
        Flota con la ÚLTIMA gestión registrada en la bitácora (estado de
        disponibilidad vigente) más Posicionamiento y FMS Comercial acumulados.

        La grilla ya no tiene filtros de fecha: el acumulado de kilómetros se
        calcula sobre el mes en curso salvo que se envíen fecha_km_inicio/fin.

        Retorna (data, total, rango_km_aplicado).
        """
        self._asegurar_estructura()

        hoy = ahora_bogota().date()
        fi = fecha_km_inicio or hoy.replace(day=1)
        ff = fecha_km_fin or hoy

        cops = list(cops_permitidos) if cops_permitidos else None

        filter_params = [
            placa, placa,
            no_interno, no_interno,
            tipologia, tipologia,
            linea, linea,
            combustible, combustible,
            id_componente, id_componente,
            id_zona, id_zona,
            id_cop, id_cop,
            estado, estado,
            id_sistema_funcional, id_sistema_funcional,
            id_causa_entrada_inoperativo, id_causa_entrada_inoperativo,
            id_estado_pendiente_actual, id_estado_pendiente_actual,
            id_ubicacion, id_ubicacion,
            id_estado_disponibilidad, id_estado_disponibilidad,
            inmovilizado_tmsa, inmovilizado_tmsa,
            cops, cops,
        ]

        where = """
            WHERE 1=1
              AND (%s::text   IS NULL OR b.placa        ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.no_interno   ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.tipologia     = %s)
              AND (%s::text   IS NULL OR b.linea         = %s)
              AND (%s::text   IS NULL OR b.combustible   = %s)
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
              AND (%s::bigint IS NULL OR b.id_cop        = %s)
              AND (%s::int    IS NULL OR b.estado        = %s)
              AND (%s::int    IS NULL OR u.id_sistema_funcional         = %s)
              AND (%s::int    IS NULL OR u.id_causa_entrada_inoperativo = %s)
              AND (%s::int    IS NULL OR u.id_estado_pendiente_actual   = %s)
              AND (%s::int    IS NULL OR u.id_ubicacion                 = %s)
              AND (%s::int    IS NULL OR u.id_estado_disponibilidad     = %s)
              AND (%s::text   IS NULL OR COALESCE(inm.inmovilizado_tmsa, 'Habilitado') = %s)
              -- Solo la flota de los COP habilitados para el usuario
              AND (%s::bigint[] IS NULL OR b.id_cop = ANY(%s))
        """

        # Última gestión por bus (DISTINCT ON) vigente a la fecha de consulta:
        # se toma el registro más reciente con fecha <= fecha_fin del filtro.
        cte_ultima = """
            ultima AS (
                SELECT DISTINCT ON (g.id_bus)
                    g.id_bus,
                    g.id,
                    g.fecha,
                    g.hora,
                    g.id_sistema_funcional,
                    g.id_causa_entrada_inoperativo,
                    g.id_estado_pendiente_actual,
                    g.id_ubicacion,
                    g.id_estado_disponibilidad,
                    g.fecha_inoperativo_mtto,
                    g.novedad,
                    g.ot_sap_pm,
                    g.reserva_sap_mm,
                    g.costo,
                    g.fecha_cumplible_presentacion
                FROM mantenimiento.bitacora_mtto_guardada g
                WHERE g.fecha <= %s
                ORDER BY g.id_bus, g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC
            )
        """

        # Inmovilización TMSA vigente por bus (la más reciente):
        #   Habilitado   → tiene fecha de habilitación anterior a ahora
        #   Inmovilizado → sin fecha de habilitación o con fecha futura
        # Si el job de inmovilizados nunca ha corrido, la tabla no existe y el CTE
        # queda vacío para que la grilla siga funcionando.
        if self._existe_tabla("mantenimiento.buses_inmovilizados"):
            hab = self._fin_inmovilizacion()
            cte_inmov = f"""
                inmov AS (
                    SELECT DISTINCT ON (i.id_bus)
                        i.id_bus,
                        i.fecha_inmovilizacion,
                        {hab} AS fecha_habilitacion,
                        CASE
                            WHEN {hab} IS NOT NULL
                             AND {hab} < (now() AT TIME ZONE 'America/Bogota')
                            THEN 'Habilitado'
                            ELSE 'Inmovilizado'
                        END AS inmovilizado_tmsa,
                        ROUND(EXTRACT(EPOCH FROM (
                            COALESCE({hab}, (now() AT TIME ZONE 'America/Bogota'))
                            - i.fecha_inmovilizacion
                        )) / 86400.0, 1) AS dias_inmovilizado
                    FROM mantenimiento.buses_inmovilizados i
                    WHERE i.id_bus IS NOT NULL
                    ORDER BY i.id_bus, i.fecha_inmovilizacion DESC, i.id DESC
                )
            """
        else:
            cte_inmov = """
                inmov AS (
                    SELECT NULL::bigint    AS id_bus,
                           NULL::timestamp AS fecha_inmovilizacion,
                           NULL::timestamp AS fecha_habilitacion,
                           NULL::text      AS inmovilizado_tmsa,
                           NULL::numeric   AS dias_inmovilizado
                    WHERE false
                )
            """

        joins = """
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id    = b.id_cop
            LEFT JOIN config.componente comp ON comp.id = c.id_componente
            LEFT JOIN config.zona       z    ON z.id    = c.id_zona
            LEFT JOIN ultima            u    ON u.id_bus = b.id
            LEFT JOIN inmov             inm  ON inm.id_bus = b.id
        """

        # ── COUNT ────────────────────────────────────────────
        sql_count = f"""
            WITH {cte_ultima},
            {cte_inmov}
            SELECT COUNT(*)::int AS total
            {joins}
            {where};
        """
        total = (self._fetchone(sql_count, [ff] + filter_params) or {}).get("total", 0)

        # ── DATA ─────────────────────────────────────────────
        pag_sql, pag_params = self._paginacion(pagina, tamano)

        # Condición reutilizada en el SELECT para marcar los campos que no aplican
        es_op = "LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'operativo'"

        sql = f"""
            WITH pos_rango AS (
                SELECT movil_bus, SUM(dist_final_km) AS km_posicionamiento
                FROM config.km_recorrido_bus
                WHERE fecha BETWEEN %s AND %s
                GROUP BY movil_bus
            ),
            fms_rango AS (
                SELECT vehiculo_real, SUM(km_ejecutado) AS km_fms_comercial
                FROM config.km_fms_bus
                WHERE fecha BETWEEN %s AND %s
                GROUP BY vehiculo_real
            ),
            {cte_ultima},
            {cte_inmov}
            SELECT
                b.id,
                b.placa,
                b.no_interno,
                b.tipologia,
                b.linea,
                b.combustible,
                b.estado,
                c.id     AS id_cop,
                c.cop,
                c.id_componente,
                comp.componente,
                c.id_zona,
                z.zona,
                ed.estado_disponibilidad,
                u.id_estado_disponibilidad,
                -- Días Inoperativo: corre contra hoy mientras el bus siga Inoperativo;
                -- en Operativo el contador queda en cero.
                CASE
                    WHEN LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'inoperativo'
                     AND u.fecha_inoperativo_mtto IS NOT NULL
                    THEN GREATEST(0, ((now() AT TIME ZONE 'America/Bogota')::date - u.fecha_inoperativo_mtto))
                    ELSE 0
                END AS dias_inoperativo,
                to_char(u.fecha_inoperativo_mtto, 'YYYY-MM-DD')        AS fecha_inoperativo_mtto_vigente,
                -- Última gestión del día consultado (todos los campos salen del
                -- mismo registro). En Operativo esos campos no aplican.
                u.novedad,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE sf.sistema_funcional END          AS sistema_funcional,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ce.causa_entrada_inoperativo END  AS causa_entrada_inoperativo,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.ot_sap_pm::text END             AS ot_sap_pm,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.reserva_sap_mm::text END        AS reserva_sap_mm,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.costo::text END                 AS costo,
                CASE WHEN {es_op} THEN 'No Aplica'
                     ELSE to_char(u.fecha_inoperativo_mtto, 'YYYY-MM-DD') END             AS fecha_inoperativo_mtto,
                CASE WHEN {es_op} THEN 'No Aplica'
                     ELSE to_char(u.fecha_cumplible_presentacion, 'YYYY-MM-DD') END       AS fecha_cumplible_presentacion,
                COALESCE(inm.inmovilizado_tmsa, 'Habilitado')          AS inmovilizado_tmsa,
                COALESCE(inm.dias_inmovilizado, 0)                     AS dias_inmovilizado,
                to_char(inm.fecha_inmovilizacion, 'YYYY-MM-DD HH24:MI') AS fecha_inmovilizacion,
                to_char(inm.fecha_habilitacion,   'YYYY-MM-DD HH24:MI') AS fecha_habilitacion,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ub.ubicacion END               AS ubicacion,
                u.id_ubicacion,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ep.estado_pendiente_actual END AS estado_pendiente_actual,
                u.id_estado_pendiente_actual,
                to_char(u.fecha, 'YYYY-MM-DD')     AS fecha_ultima_gestion,
                to_char(u.hora,  'HH24:MI')        AS hora_ultima_gestion,
                COALESCE(p.km_posicionamiento, 0)  AS posicionamiento,
                COALESCE(fk.km_fms_comercial, 0)   AS fms_comercial,
                -- Vacío FMS: km de posicionamiento que no quedaron registrados en FMS Comercial
                COALESCE(p.km_posicionamiento, 0) - COALESCE(fk.km_fms_comercial, 0) AS vacio_fms
            {joins}
            LEFT JOIN mantenimiento.estado_disponibilidad     ed ON ed.id = u.id_estado_disponibilidad
            LEFT JOIN mantenimiento.ubicacion                 ub ON ub.id = u.id_ubicacion
            LEFT JOIN mantenimiento.estado_pendiente_actual   ep ON ep.id = u.id_estado_pendiente_actual
            LEFT JOIN mantenimiento.sistema_funcional         sf ON sf.id = u.id_sistema_funcional
            LEFT JOIN mantenimiento.causa_entrada_inoperativo ce ON ce.id = u.id_causa_entrada_inoperativo
            LEFT JOIN pos_rango p   ON p.movil_bus     = b.no_interno
            LEFT JOIN fms_rango fk  ON fk.vehiculo_real = b.no_interno
            {where}
            ORDER BY
                CASE
                    WHEN COALESCE(b.no_interno, '') ~ '^[0-9]+$' THEN 0
                    ELSE 1
                END,
                LENGTH(COALESCE(b.no_interno, '')),
                COALESCE(b.no_interno, '') ASC
        """

        data = self._fetchall(
            sql + pag_sql,
            [fi, ff, fi, ff, ff] + filter_params + pag_params,
        )

        return data, total, {"fecha_inicio": str(fi), "fecha_fin": str(ff)}

    # =========================================================
    # RESUMEN DE FLOTA (tarjetas de indicadores)
    # =========================================================

    def resumen_flota(
        self,
        fecha_corte: Optional[date] = None,
        hora: Optional[str] = None,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        id_cop: Optional[int] = None,
        estado: Optional[int] = None,
        dias_tendencia: int = 30,
        cops_permitidos: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Fotografía de la flota a un día y hora de corte, con tres lecturas:

          · Uso Operación   → si el bus puede prestar servicio comercial:
                              Operativo Y habilitado por TMSA.
          · Mantenimiento   → estado de disponibilidad de la bitácora.
          · Inmovilización  → estado TMSA.

        El corte funciona igual que en el reporte: se toma el último registro
        de bitácora anterior o igual a esa fecha y hora, y la inmovilización
        vigente en ese mismo instante.
        """
        self._asegurar_estructura()

        fecha_ref = fecha_corte or ahora_bogota().date()
        hora_ref = (hora or "23:59").strip()[:5]
        momento = f"{fecha_ref} {hora_ref}:59"
        tiene_inmov = self._existe_tabla("mantenimiento.buses_inmovilizados")

        if tiene_inmov:
            cte_inmov = f"""
                inmov AS (
                    SELECT DISTINCT ON (i.id_bus)
                        i.id_bus, i.fecha_inmovilizacion,
                        {self._fin_inmovilizacion()} AS fecha_habilitacion,
                        i.causa_inmovilizacion
                    FROM mantenimiento.buses_inmovilizados i
                    WHERE i.id_bus IS NOT NULL
                      AND i.fecha_inmovilizacion <= %s::timestamp
                    ORDER BY i.id_bus, i.fecha_inmovilizacion DESC, i.id DESC
                )
            """
            params_inmov = [momento]
        else:
            cte_inmov = """
                inmov AS (
                    SELECT NULL::bigint    AS id_bus,
                           NULL::timestamp AS fecha_inmovilizacion,
                           NULL::timestamp AS fecha_habilitacion,
                           NULL::text      AS causa_inmovilizacion
                    WHERE false
                )
            """
            params_inmov = []

        cops_res = list(cops_permitidos) if cops_permitidos else None
        filtros = [id_componente, id_componente, id_zona, id_zona, id_cop, id_cop,
                   estado, estado, cops_res, cops_res]

        sql = f"""
            WITH ultima AS (
                SELECT DISTINCT ON (g.id_bus)
                    g.id_bus, g.id_estado_disponibilidad, g.id_ubicacion,
                    g.id_sistema_funcional, g.id_causa_entrada_inoperativo,
                    g.id_estado_pendiente_actual, g.fecha_inoperativo_mtto
                FROM mantenimiento.bitacora_mtto_guardada g
                WHERE (g.fecha::timestamp + COALESCE(g.hora, '00:00:00'::time)) <= %s::timestamp
                ORDER BY g.id_bus, g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC
            ),
            {cte_inmov}
            SELECT
                b.id,
                b.placa,
                b.no_interno,
                COALESCE(NULLIF(TRIM(b.tipologia), ''), 'Sin tipología') AS tipologia,
                b.estado,
                c.cop,
                z.zona,
                comp.componente,
                COALESCE(ed.estado_disponibilidad, 'Sin gestión') AS estado_disponibilidad,
                ub.ubicacion,
                sf.sistema_funcional,
                ce.causa_entrada_inoperativo,
                ep.estado_pendiente_actual,
                i.causa_inmovilizacion,
                -- Días inoperativo: corre hasta el corte mientras siga Inoperativo
                CASE
                    WHEN LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'inoperativo'
                     AND u.fecha_inoperativo_mtto IS NOT NULL
                    THEN GREATEST(0, (%s::date - u.fecha_inoperativo_mtto))
                    ELSE NULL
                END AS dias_inoperativo,
                CASE
                    WHEN i.id_bus IS NULL THEN 'Habilitado'
                    WHEN i.fecha_habilitacion IS NOT NULL
                     AND i.fecha_habilitacion < %s::timestamp THEN 'Habilitado'
                    ELSE 'Inmovilizado'
                END AS inmovilizado_tmsa,
                CASE
                    WHEN i.id_bus IS NULL THEN NULL
                    WHEN i.fecha_habilitacion IS NOT NULL
                     AND i.fecha_habilitacion < %s::timestamp THEN NULL
                    ELSE ROUND(EXTRACT(EPOCH FROM (
                            %s::timestamp - i.fecha_inmovilizacion
                         )) / 86400.0, 1)
                END AS dias_inmovilizado
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id    = b.id_cop
            LEFT JOIN config.componente comp ON comp.id = c.id_componente
            LEFT JOIN config.zona       z    ON z.id    = c.id_zona
            LEFT JOIN ultima            u    ON u.id_bus = b.id
            LEFT JOIN inmov             i    ON i.id_bus = b.id
            LEFT JOIN mantenimiento.estado_disponibilidad ed ON ed.id = u.id_estado_disponibilidad
            LEFT JOIN mantenimiento.ubicacion             ub ON ub.id = u.id_ubicacion
            LEFT JOIN mantenimiento.sistema_funcional     sf ON sf.id = u.id_sistema_funcional
            LEFT JOIN mantenimiento.causa_entrada_inoperativo ce ON ce.id = u.id_causa_entrada_inoperativo
            LEFT JOIN mantenimiento.estado_pendiente_actual    ep ON ep.id = u.id_estado_pendiente_actual
            WHERE 1=1
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
              AND (%s::bigint IS NULL OR b.id_cop        = %s)
              AND (%s::int    IS NULL OR b.estado        = %s)
              AND (%s::bigint[] IS NULL OR b.id_cop      = ANY(%s));
        """

        # ultima(momento) + inmov(momento) + dias_inoperativo(fecha) +
        # 3 usos del momento en las columnas TMSA + filtros
        params = ([momento] + params_inmov + [fecha_ref, momento, momento, momento] + filtros)
        buses = self._fetchall(sql, params)

        resumen = self._construir_resumen(buses, fecha_ref, hora_ref)
        resumen["tendencia"] = self.tendencia_flota(
            fecha_corte=fecha_ref, hora=hora_ref, dias=dias_tendencia,
            id_componente=id_componente, id_zona=id_zona, id_cop=id_cop,
            estado=estado, cops_permitidos=cops_permitidos,
        )
        return resumen

    # Uso Operación: el bus solo puede prestar servicio comercial si está
    # Operativo en mantenimiento Y habilitado por TMSA. Basta con que falle uno.
    MOTIVO_DISPONIBLE   = "Disponible"
    MOTIVO_MANTENIMIENTO = "Solo mantenimiento"
    MOTIVO_TMSA          = "Solo TMSA"
    MOTIVO_AMBOS         = "Mantenimiento y TMSA"

    @staticmethod
    def _clasificar_uso(bus: Dict) -> str:
        falla_mtto = (bus.get("estado_disponibilidad") or "") != "Operativo"
        falla_tmsa = bus.get("inmovilizado_tmsa") == "Inmovilizado"
        if falla_mtto and falla_tmsa:
            return GestionEstadoBitacoraMtto.MOTIVO_AMBOS
        if falla_mtto:
            return GestionEstadoBitacoraMtto.MOTIVO_MANTENIMIENTO
        if falla_tmsa:
            return GestionEstadoBitacoraMtto.MOTIVO_TMSA
        return GestionEstadoBitacoraMtto.MOTIVO_DISPONIBLE

    @staticmethod
    def _metricas_dias(valores: List[float]) -> Dict[str, Any]:
        """Promedio, mayor tiempo y acumulados por antigüedad de un grupo de días."""
        return {
            "promedio":    round(sum(valores) / len(valores), 1) if valores else 0.0,
            "maximo":      round(max(valores), 1) if valores else 0.0,
            "mas_30_dias": sum(1 for d in valores if d > 30),
            "mas_60_dias": sum(1 for d in valores if d > 60),
        }

    @staticmethod
    def _rangos_dias(valores: List[float]) -> List[Dict[str, Any]]:
        """Distribución por antigüedad, para el gráfico de barras."""
        cortes = [(0, 7, "0 a 7"), (7, 15, "8 a 15"), (15, 30, "16 a 30"),
                  (30, 60, "31 a 60"), (60, None, "Más de 60")]
        salida = []
        for desde, hasta, etiqueta in cortes:
            n = sum(1 for d in valores
                    if d > desde - 0.0001 and (hasta is None or d <= hasta))
            salida.append({"rango": etiqueta, "conteo": n})
        return salida

    def _construir_resumen(self, buses: List[Dict], corte: date, hora: str) -> Dict[str, Any]:
        """Agrega los buses en las tres secciones del modal de resumen."""
        total = len(buses)

        def pct(n: int) -> float:
            return round(n * 100.0 / total, 1) if total else 0.0

        def bloque(n: int) -> Dict[str, Any]:
            return {"conteo": n, "porcentaje": pct(n)}

        def conteo_por(campo: str, base: Optional[List[Dict]] = None) -> List[Dict]:
            universo = buses if base is None else base
            acumulado: Dict[str, int] = {}
            for b in universo:
                clave = b.get(campo) or "Sin dato"
                acumulado[clave] = acumulado.get(clave, 0) + 1
            filas = [{"nombre": k, "conteo": v,
                      "porcentaje": round(v * 100.0 / len(universo), 1) if universo else 0.0}
                     for k, v in acumulado.items()]
            return sorted(filas, key=lambda x: x["conteo"], reverse=True)

        # ── Uso Operación (disponibilidad completa) ──────────
        for b in buses:
            b["uso_operacion"] = ("Disponible" if self._clasificar_uso(b) == self.MOTIVO_DISPONIBLE
                                  else "No Disponible")
            b["motivo_no_disponible"] = self._clasificar_uso(b)

        disponibles    = [b for b in buses if b["uso_operacion"] == "Disponible"]
        no_disponibles = [b for b in buses if b["uso_operacion"] != "Disponible"]
        solo_mtto  = [b for b in no_disponibles if b["motivo_no_disponible"] == self.MOTIVO_MANTENIMIENTO]
        solo_tmsa  = [b for b in no_disponibles if b["motivo_no_disponible"] == self.MOTIVO_TMSA]
        ambos      = [b for b in no_disponibles if b["motivo_no_disponible"] == self.MOTIVO_AMBOS]

        # ── Mantenimiento (estado de disponibilidad) ─────────
        def por_estado_disp(nombre: str) -> List[Dict]:
            return [b for b in buses if b["estado_disponibilidad"] == nombre]

        operativos    = por_estado_disp("Operativo")
        inoperativos  = por_estado_disp("Inoperativo")
        sin_gestion   = por_estado_disp("Sin gestión")
        # Estados retirados del catálogo que siguen en gestiones históricas
        otros_estados = [b for b in buses if b["estado_disponibilidad"]
                         not in ("Operativo", "Inoperativo", "Sin gestión")]

        dias_inop = [float(b["dias_inoperativo"]) for b in inoperativos
                     if b.get("dias_inoperativo") is not None]

        # ── Inmovilización TMSA ──────────────────────────────
        inmovilizados = [b for b in buses if b["inmovilizado_tmsa"] == "Inmovilizado"]
        habilitados   = [b for b in buses if b["inmovilizado_tmsa"] != "Inmovilizado"]
        dias_inmov = [float(b["dias_inmovilizado"]) for b in inmovilizados
                      if b.get("dias_inmovilizado") is not None]

        # ── Desagregado por tipología ────────────────────────
        tipologias: Dict[str, Dict[str, Any]] = {}
        for b in buses:
            t = b["tipologia"]
            fila = tipologias.setdefault(t, {
                "tipologia": t, "total": 0, "disponible": 0, "operativo": 0,
                "inoperativo": 0, "sin_gestion": 0, "otros": 0, "inmovilizado": 0,
                "dias_inmov": [], "dias_inop": [],
            })
            fila["total"] += 1
            if b["uso_operacion"] == "Disponible":
                fila["disponible"] += 1
            clave = {
                "Operativo": "operativo", "Inoperativo": "inoperativo",
                "Sin gestión": "sin_gestion",
            }.get(b["estado_disponibilidad"], "otros")
            fila[clave] += 1
            if b.get("dias_inoperativo") is not None:
                fila["dias_inop"].append(float(b["dias_inoperativo"]))
            if b["inmovilizado_tmsa"] == "Inmovilizado":
                fila["inmovilizado"] += 1
                if b.get("dias_inmovilizado") is not None:
                    fila["dias_inmov"].append(float(b["dias_inmovilizado"]))

        por_tipologia = []
        for fila in tipologias.values():
            t = fila["total"]
            d_inmov = fila.pop("dias_inmov")
            d_inop = fila.pop("dias_inop")
            fila["porcentaje_flota"]        = round(t * 100.0 / total, 1) if total else 0.0
            fila["porcentaje_disponible"]   = round(fila["disponible"] * 100.0 / t, 1) if t else 0.0
            fila["porcentaje_operativo"]    = round(fila["operativo"] * 100.0 / t, 1) if t else 0.0
            fila["porcentaje_inmovilizado"] = round(fila["inmovilizado"] * 100.0 / t, 1) if t else 0.0
            fila["dias_promedio"]           = round(sum(d_inmov) / len(d_inmov), 1) if d_inmov else 0.0
            fila["dias_inop_promedio"]      = round(sum(d_inop) / len(d_inop), 1) if d_inop else 0.0
            por_tipologia.append(fila)
        por_tipologia.sort(key=lambda x: x["total"], reverse=True)

        # Detalle por bus para los tooltips de las tarjetas. Va en formato
        # compacto — columnas + filas, con los textos repetidos convertidos en
        # catálogos — porque son miles de buses y el modal lo pide entero.
        catalogos: Dict[str, List[str]] = {"tipologia": [], "cop": [],
                                           "estado": [], "motivo": []}

        def idx(catalogo: str, valor: Optional[str]) -> int:
            lista = catalogos[catalogo]
            texto = valor or "Sin dato"
            if texto not in lista:
                lista.append(texto)
            return lista.index(texto)

        filas_detalle = [[
            b.get("placa") or "--",
            b.get("no_interno") or "--",
            idx("tipologia", b.get("tipologia")),
            idx("cop", b.get("cop")),
            1 if b["uso_operacion"] == "Disponible" else 0,
            idx("motivo", b["motivo_no_disponible"]),
            idx("estado", b["estado_disponibilidad"]),
            1 if b["inmovilizado_tmsa"] == "Inmovilizado" else 0,
            b.get("dias_inoperativo"),
            float(b["dias_inmovilizado"]) if b.get("dias_inmovilizado") is not None else None,
        ] for b in buses]

        return {
            "fecha_corte": str(corte),
            "hora_corte":  hora,
            "corte":       f"{corte} {hora}",
            "total_flota": total,

            # 1) Disponibilidad completa
            "uso_operacion": {
                "disponibles":    bloque(len(disponibles)),
                "no_disponibles": bloque(len(no_disponibles)),
                "solo_mantenimiento": bloque(len(solo_mtto)),
                "solo_tmsa":          bloque(len(solo_tmsa)),
                "ambos":              bloque(len(ambos)),
                "motivos": [
                    {"nombre": self.MOTIVO_MANTENIMIENTO, "conteo": len(solo_mtto)},
                    {"nombre": self.MOTIVO_TMSA,          "conteo": len(solo_tmsa)},
                    {"nombre": self.MOTIVO_AMBOS,         "conteo": len(ambos)},
                ],
                "por_tipologia": conteo_por("tipologia", no_disponibles)[:8],
                "por_cop":       conteo_por("cop", no_disponibles)[:8],
                # Dentro de la no disponibilidad por mantenimiento pesa mucho el
                # bus sin gestión registrada, que no es lo mismo que inoperativo
                "por_estado_mantenimiento": conteo_por("estado_disponibilidad", no_disponibles),
            },

            # 2) Disponibilidad mantenimiento
            "mantenimiento": {
                "operativos":    bloque(len(operativos)),
                "inoperativos":  bloque(len(inoperativos)),
                "sin_gestion":   bloque(len(sin_gestion)),
                "otros_estados": bloque(len(otros_estados)),
                "dias":          self._metricas_dias(dias_inop),
                "rangos_dias":   self._rangos_dias(dias_inop),
            },

            # 3) Inmovilización TMSA
            "tmsa": {
                "inmovilizados": bloque(len(inmovilizados)),
                "habilitados":   bloque(len(habilitados)),
                "dias":          self._metricas_dias(dias_inmov),
                "rangos_dias":   self._rangos_dias(dias_inmov),
            },

            "por_estado_disponibilidad": conteo_por("estado_disponibilidad"),
            "por_tipologia":             por_tipologia,
            "top_ubicaciones":           conteo_por("ubicacion", inoperativos)[:6],
            "top_sistemas_funcionales":  conteo_por("sistema_funcional", inoperativos)[:6],
            "top_causas_inoperativo":    conteo_por("causa_entrada_inoperativo", inoperativos)[:6],
            "top_estados_pendientes":    conteo_por("estado_pendiente_actual", inoperativos)[:6],
            "top_causas_inmovilizacion": conteo_por("causa_inmovilizacion", inmovilizados)[:6],
            "detalle": {
                "columnas": ["placa", "no_interno", "tipologia", "cop", "disponible",
                             "motivo", "estado", "inmovilizado",
                             "dias_inoperativo", "dias_inmovilizado"],
                "catalogos": catalogos,
                "filas": filas_detalle,
            },
        }

    # =========================================================
    # TENDENCIA DIARIA (gráfico de evolución)
    # =========================================================

    def tendencia_flota(
        self,
        fecha_corte: Optional[date] = None,
        hora: Optional[str] = None,
        dias: int = 30,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        id_cop: Optional[int] = None,
        estado: Optional[int] = None,
        cops_permitidos: Optional[List[int]] = None,
    ) -> List[Dict]:
        """
        Serie diaria de la flota: disponibles para operar, inoperativos e
        inmovilizados, reconstruidos al cierre de cada día.

        Cada día se evalúa contra su último instante, salvo el día del corte,
        que usa la hora indicada para coincidir con la foto que se muestra.
        Las tablas involucradas son pequeñas y el rango está acotado, así que
        se resuelve en una sola consulta agrupada.
        """
        self._asegurar_estructura()

        fecha_ref = fecha_corte or ahora_bogota().date()
        hora_ref = (hora or "23:59").strip()[:5]
        momento = f"{fecha_ref} {hora_ref}:59"
        dias = max(1, min(int(dias or 30), 180))
        desde = fecha_ref - timedelta(days=dias - 1)

        tiene_inmov = self._existe_tabla("mantenimiento.buses_inmovilizados")
        if tiene_inmov:
            cte_inmov = f"""
                inmov_dia AS (
                    SELECT m.d, i.id_bus
                    FROM momentos m
                    JOIN mantenimiento.buses_inmovilizados i
                      ON i.id_bus IS NOT NULL
                     AND i.fecha_inmovilizacion <= m.momento
                     AND ({self._fin_inmovilizacion()} IS NULL
                          OR {self._fin_inmovilizacion()} >= m.momento)
                    GROUP BY m.d, i.id_bus
                )
            """
        else:
            cte_inmov = """
                inmov_dia AS (
                    SELECT NULL::date AS d, NULL::bigint AS id_bus WHERE false
                )
            """

        cops_res = list(cops_permitidos) if cops_permitidos else None

        sql = f"""
            WITH momentos AS (
                SELECT g.d::date AS d,
                       LEAST(g.d + INTERVAL '1 day' - INTERVAL '1 second',
                             %s::timestamp) AS momento
                FROM generate_series(%s::date, %s::date, '1 day') AS g(d)
            ),
            flota AS (
                SELECT b.id
                FROM config.buses_cexp b
                LEFT JOIN config.cop c ON c.id = b.id_cop
                WHERE (%s::bigint IS NULL OR c.id_componente = %s)
                  AND (%s::bigint IS NULL OR c.id_zona       = %s)
                  AND (%s::bigint IS NULL OR b.id_cop        = %s)
                  AND (%s::int    IS NULL OR b.estado        = %s)
                  AND (%s::bigint[] IS NULL OR b.id_cop      = ANY(%s))
            ),
            estado_dia AS (
                SELECT m.d, g.id_bus,
                       (array_agg(ed.estado_disponibilidad
                                  ORDER BY g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC))[1] AS estado
                FROM momentos m
                JOIN mantenimiento.bitacora_mtto_guardada g
                  ON (g.fecha::timestamp + COALESCE(g.hora, '00:00:00'::time)) <= m.momento
                LEFT JOIN mantenimiento.estado_disponibilidad ed
                  ON ed.id = g.id_estado_disponibilidad
                GROUP BY m.d, g.id_bus
            ),
            {cte_inmov}
            SELECT
                to_char(m.d, 'YYYY-MM-DD') AS fecha,
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE COALESCE(e.estado, 'Sin gestión') = 'Operativo'
                      AND im.id_bus IS NULL
                ) AS disponibles,
                COUNT(*) FILTER (
                    WHERE COALESCE(e.estado, 'Sin gestión') <> 'Operativo'
                       OR im.id_bus IS NOT NULL
                ) AS no_disponibles,
                COUNT(*) FILTER (WHERE COALESCE(e.estado, 'Sin gestión') = 'Operativo')   AS operativos,
                COUNT(*) FILTER (WHERE COALESCE(e.estado, 'Sin gestión') = 'Inoperativo') AS inoperativos,
                COUNT(*) FILTER (WHERE im.id_bus IS NOT NULL)                             AS inmovilizados
            FROM momentos m
            CROSS JOIN flota f
            LEFT JOIN estado_dia e  ON e.d  = m.d AND e.id_bus  = f.id
            LEFT JOIN inmov_dia  im ON im.d = m.d AND im.id_bus = f.id
            GROUP BY m.d
            ORDER BY m.d;
        """

        params = [momento, desde, fecha_ref,
                  id_componente, id_componente, id_zona, id_zona,
                  id_cop, id_cop, estado, estado, cops_res, cops_res]

        filas = self._fetchall(sql, params)
        for f in filas:
            total = f["total"] or 0
            f["porcentaje_disponible"] = round((f["disponibles"] or 0) * 100.0 / total, 1) if total else 0.0
        return filas

    # =========================================================
    # ENCABEZADO DEL BUS (solo consulta)
    # =========================================================

    def obtener_bus(self, id_bus: int) -> Optional[Dict]:
        """Ficha completa de config.buses_cexp para el encabezado del modal."""
        sql = """
            SELECT
                b.id, b.placa, b.no_interno, b.tipologia, b.modelo, b.marca,
                b.linea, b.carroceria, b.combustible, b.tecnologia, b.estado,
                b.id_cop, c.cop,
                c.id_componente, comp.componente,
                c.id_zona, z.zona,
                to_char(b.created_at, 'YYYY-MM-DD HH24:MI') AS created_at,
                to_char(b.updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id    = b.id_cop
            LEFT JOIN config.componente comp ON comp.id = c.id_componente
            LEFT JOIN config.zona       z    ON z.id    = c.id_zona
            WHERE b.id = %s;
        """
        return self._fetchone(sql, [id_bus])

    # =========================================================
    # GESTIÓN: ÚLTIMA + HISTÓRICO
    # =========================================================

    _SELECT_GESTION = """
        SELECT
            g.id,
            g.id_bus,
            to_char(g.fecha, 'YYYY-MM-DD')                        AS fecha,
            to_char(g.hora,  'HH24:MI')                           AS hora,
            g.novedad,
            g.id_sistema_funcional,          sf.sistema_funcional,
            g.id_causa_entrada_inoperativo,  ce.causa_entrada_inoperativo,
            g.id_estado_pendiente_actual,    ep.estado_pendiente_actual,
            g.id_ubicacion,                  ub.ubicacion,
            g.id_estado_disponibilidad,      ed.estado_disponibilidad,
            g.ot_sap_pm,
            g.reserva_sap_mm,
            to_char(g.fecha_inoperativo_mtto, 'YYYY-MM-DD')       AS fecha_inoperativo_mtto,
            g.costo,
            to_char(g.fecha_cumplible_presentacion, 'YYYY-MM-DD') AS fecha_cumplible_presentacion,
            to_char(g.fecha_ingreso_cein, 'YYYY-MM-DD')           AS fecha_ingreso_cein,
            COALESCE(g.dias_inoperativo, 0)                       AS dias_inoperativo,
            g.id_usuario_registra,
            TRIM(COALESCE(us.nombres, '') || ' ' || COALESCE(us.apellidos, '')) AS usuario_registra,
            to_char(g.fecha_guardado, 'YYYY-MM-DD HH24:MI')       AS fecha_guardado,
            g.ruta_archivos,
            -- En Operativo los campos de inoperatividad no aplican
            (LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'operativo') AS es_operativo
        FROM mantenimiento.bitacora_mtto_guardada g
        LEFT JOIN mantenimiento.sistema_funcional         sf ON sf.id = g.id_sistema_funcional
        LEFT JOIN mantenimiento.causa_entrada_inoperativo ce ON ce.id = g.id_causa_entrada_inoperativo
        LEFT JOIN mantenimiento.estado_pendiente_actual   ep ON ep.id = g.id_estado_pendiente_actual
        LEFT JOIN mantenimiento.ubicacion                 ub ON ub.id = g.id_ubicacion
        LEFT JOIN mantenimiento.estado_disponibilidad     ed ON ed.id = g.id_estado_disponibilidad
        LEFT JOIN usuarios                                us ON us.id = g.id_usuario_registra
    """

    def ultima_gestion(self, id_bus: int) -> Optional[Dict]:
        """Última gestión registrada del bus (la que precarga el formulario)."""
        self._asegurar_estructura()
        sql = f"""
            {self._SELECT_GESTION}
            WHERE g.id_bus = %s
            ORDER BY g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC
            LIMIT 1;
        """
        return self._fetchone(sql, [id_bus])

    def historial_gestiones(
        self,
        id_bus: int,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
    ) -> Tuple[List[Dict], Dict[str, str]]:
        """
        Histórico de cambios del bus. Por defecto los últimos 2 meses;
        el rango es ajustable desde el panel de histórico del modal.
        """
        self._asegurar_estructura()

        hoy = ahora_bogota().date()
        ff = fecha_fin or hoy
        fi = fecha_inicio or (ff - timedelta(days=60))

        sql = f"""
            {self._SELECT_GESTION}
            WHERE g.id_bus = %s
              AND g.fecha BETWEEN %s AND %s
            ORDER BY g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC;
        """
        data = self._fetchall(sql, [id_bus, fi, ff])
        return data, {"fecha_inicio": str(fi), "fecha_fin": str(ff)}

    def obtener_registro(self, id_registro: int) -> Optional[Dict]:
        """Detalle de un registro puntual del histórico."""
        self._asegurar_estructura()
        sql = f"""
            {self._SELECT_GESTION}
            WHERE g.id = %s;
        """
        return self._fetchone(sql, [id_registro])

    # =========================================================
    # INMOVILIZADOS TMSA POR BUS
    # =========================================================
    # Estado:
    #   Habilitado        → fecha_habilitacion existe y es anterior a ahora
    #   Inmovilizado TMSA → sin fecha_habilitacion, o con fecha futura
    # Días:
    #   dias_transcurridos → duración del evento: COALESCE(habilitación, ahora) − inmovilización
    #   dias_en_curso      → el contador que sigue corriendo; 0 cuando ya está habilitado

    _SELECT_INMOVILIZACION = """
        SELECT
            i.id,
            i.id_bus,
            i.placa,
            {extra_cierre}
            i.concesionario_operacion,
            i.origen,
            i.tipo_servicio,
            to_char(i.fecha_inmovilizacion, 'YYYY-MM-DD HH24:MI') AS fecha_inmovilizacion,
            i.causa_inmovilizacion,
            i.descripcion_novedad,
            i.creador,
            to_char(i.fecha_revision, 'YYYY-MM-DD HH24:MI')       AS fecha_revision,
            i.revisor,
            i.descripcion_revision,
            to_char({hab}, 'YYYY-MM-DD HH24:MI')                  AS fecha_habilitacion,
            i.habilitador,
            i.archivo,
            to_char(i.fecha_archivo, 'YYYY-MM-DD')                AS fecha_archivo,
            to_char(i.actualizado_en AT TIME ZONE 'America/Bogota',
                    'YYYY-MM-DD HH24:MI')                         AS actualizado_en,
            CASE
                WHEN {hab} IS NOT NULL
                 AND {hab} < (now() AT TIME ZONE 'America/Bogota')
                THEN 'Habilitado'
                ELSE 'Inmovilizado TMSA'
            END AS estado_tmsa,
            ROUND(EXTRACT(EPOCH FROM (
                COALESCE({hab}, (now() AT TIME ZONE 'America/Bogota'))
                - i.fecha_inmovilizacion
            )) / 86400.0, 1) AS dias_transcurridos,
            CASE
                WHEN {hab} IS NOT NULL
                 AND {hab} < (now() AT TIME ZONE 'America/Bogota')
                THEN 0
                ELSE ROUND(EXTRACT(EPOCH FROM (
                        (now() AT TIME ZONE 'America/Bogota') - i.fecha_inmovilizacion
                     )) / 86400.0, 1)
            END AS dias_en_curso
        FROM mantenimiento.buses_inmovilizados i
    """

    def _select_inmovilizacion(self) -> str:
        """
        `_SELECT_INMOVILIZACION` con la fecha de fin y la marca de cómo se supo:
        del archivo (habilitación entregada) o por ausencia (deducida).
        """
        hab = self._fin_inmovilizacion()
        if self._existe_columna("mantenimiento.buses_inmovilizados", "cerrado_por"):
            extra = """
            i.cerrado_por,
            COALESCE(i.cerrado_por = 'ausencia', FALSE)            AS cierre_estimado,
            to_char(i.visto_en_archivo, 'YYYY-MM-DD')              AS visto_en_archivo,
            """
        else:
            extra = """
            NULL::text    AS cerrado_por,
            FALSE         AS cierre_estimado,
            NULL::text    AS visto_en_archivo,
            """
        return self._SELECT_INMOVILIZACION.format(hab=hab, extra_cierre=extra)

    def inmovilizacion_vigente(self, id_bus: int) -> Optional[Dict]:
        """Última inmovilización TMSA del bus (la que muestra el formulario)."""
        if not self._existe_tabla("mantenimiento.buses_inmovilizados"):
            return None
        sql = f"""
            {self._select_inmovilizacion()}
            WHERE i.id_bus = %s
            ORDER BY i.fecha_inmovilizacion DESC, i.id DESC
            LIMIT 1;
        """
        return self._fetchone(sql, [id_bus])

    def historial_inmovilizaciones(
        self,
        id_bus: int,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
    ) -> List[Dict]:
        """
        Inmovilizaciones del bus dentro del rango, según la fecha de inmovilización.
        Alimenta el panel de histórico del modal, con el mismo rango de las gestiones.
        """
        if not self._existe_tabla("mantenimiento.buses_inmovilizados"):
            return []

        hoy = ahora_bogota().date()
        ff = fecha_fin or hoy
        fi = fecha_inicio or (ff - timedelta(days=60))

        sql = f"""
            {self._select_inmovilizacion()}
            WHERE i.id_bus = %s
              AND i.fecha_inmovilizacion::date BETWEEN %s AND %s
            ORDER BY i.fecha_inmovilizacion DESC, i.id DESC;
        """
        return self._fetchall(sql, [id_bus, fi, ff])

    # =========================================================
    # CARGUE DE BUSES INMOVILIZADOS (log del job)
    # =========================================================

    def estado_cargue_inmovilizados(self) -> Dict[str, Any]:
        """
        Estado del último cargue de los archivos de buses inmovilizados.

        Lee log.procesa_buses_inmovilizados (la crea el job jobs/buses_inmovilizados.py).
        Si el job nunca ha corrido, la tabla no existe todavía y se retorna vacío.
        """
        existe = self._fetchone(
            "SELECT to_regclass('log.procesa_buses_inmovilizados') IS NOT NULL AS existe;"
        )
        if not existe or not existe.get("existe"):
            return {"ultima_carga": None, "fecha_archivo": None, "detalle": []}

        sql = """
            SELECT
                t.origen,
                t.archivo,
                to_char(t.fecha_archivo, 'YYYY-MM-DD') AS fecha_archivo,
                t.estado,
                to_char(t.ultima_ejecucion AT TIME ZONE 'America/Bogota',
                        'YYYY-MM-DD HH24:MI')          AS ultima_ejecucion,
                t.filas_archivo,
                t.filas_nuevas,
                t.filas_actualizadas,
                t.filas_omitidas,
                t.filas_sin_bus,
                t.ejecutado_por,
                LEFT(COALESCE(t.mensaje, ''), 300)     AS mensaje
            FROM (
                SELECT DISTINCT ON (origen) *
                FROM log.procesa_buses_inmovilizados
                ORDER BY origen, fecha_archivo DESC, id DESC
            ) t
            ORDER BY t.origen;
        """
        detalle = self._fetchall(sql)

        resumen = self._fetchone("""
            SELECT
                to_char(MAX(ultima_ejecucion) AT TIME ZONE 'America/Bogota',
                        'YYYY-MM-DD HH24:MI')   AS ultima_carga,
                to_char(MAX(fecha_archivo), 'YYYY-MM-DD') AS fecha_archivo
            FROM log.procesa_buses_inmovilizados
            WHERE estado = 'ok';
        """) or {}

        return {
            "ultima_carga":  resumen.get("ultima_carga"),
            "fecha_archivo": resumen.get("fecha_archivo"),
            "detalle":       [dict(r) for r in detalle],
        }

    # =========================================================
    # REPORTE POR DÍA Y HORA DE CORTE
    # =========================================================

    def reporte_estado(
        self,
        fecha: Optional[date] = None,
        hora: Optional[str] = None,
        placa: Optional[str] = None,
        no_interno: Optional[str] = None,
        tipologia: Optional[str] = None,
        linea: Optional[str] = None,
        id_componente: Optional[int] = None,
        id_zona: Optional[int] = None,
        id_cop: Optional[int] = None,
        estado: Optional[int] = None,
        id_estado_disponibilidad: Optional[int] = None,
        inmovilizado_tmsa: Optional[str] = None,
        cops_permitidos: Optional[List[int]] = None,
    ) -> Tuple[List[Dict], str]:
        """
        Estado de cada bus tal como estaba en un instante puntual (día + hora).

        No trabaja por rangos: toma el último registro de bitácora cuya fecha y
        hora sean anteriores o iguales al corte, aunque el cambio sea de días
        atrás, y lo cruza con la inmovilización TMSA vigente a ese mismo momento.
        """
        self._asegurar_estructura()

        fecha_corte = fecha or ahora_bogota().date()
        hora_corte = (hora or "23:59").strip()[:5]
        momento = f"{fecha_corte} {hora_corte}:59"

        es_op = "LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'operativo'"

        if self._existe_tabla("mantenimiento.buses_inmovilizados"):
            cte_inmov = f"""
                inmov AS (
                    SELECT DISTINCT ON (i.id_bus)
                        i.id_bus, i.fecha_inmovilizacion, i.causa_inmovilizacion,
                        i.descripcion_novedad, i.creador, i.fecha_revision, i.revisor,
                        i.descripcion_revision, i.habilitador,
                        {self._fin_inmovilizacion()} AS fecha_habilitacion
                    FROM mantenimiento.buses_inmovilizados i
                    WHERE i.id_bus IS NOT NULL
                      AND i.fecha_inmovilizacion <= %s::timestamp
                    ORDER BY i.id_bus, i.fecha_inmovilizacion DESC, i.id DESC
                )
            """
            params_inmov = [momento]
        else:
            cte_inmov = """
                inmov AS (
                    SELECT NULL::bigint AS id_bus, NULL::timestamp AS fecha_inmovilizacion,
                           NULL::text AS causa_inmovilizacion, NULL::text AS descripcion_novedad,
                           NULL::text AS creador, NULL::timestamp AS fecha_revision,
                           NULL::text AS revisor, NULL::text AS descripcion_revision,
                           NULL::timestamp AS fecha_habilitacion, NULL::text AS habilitador
                    WHERE false
                )
            """
            params_inmov = []

        filtros = [
            placa, placa,
            no_interno, no_interno,
            tipologia, tipologia,
            linea, linea,
            id_componente, id_componente,
            id_zona, id_zona,
            id_cop, id_cop,
            estado, estado,
            id_estado_disponibilidad, id_estado_disponibilidad,
            inmovilizado_tmsa, inmovilizado_tmsa,
        ]
        cops_rep = list(cops_permitidos) if cops_permitidos else None

        sql = f"""
            WITH ultima AS (
                SELECT DISTINCT ON (g.id_bus)
                    g.id_bus, g.fecha, g.hora, g.novedad,
                    g.id_sistema_funcional, g.id_causa_entrada_inoperativo,
                    g.id_estado_pendiente_actual, g.id_ubicacion, g.id_estado_disponibilidad,
                    g.ot_sap_pm, g.reserva_sap_mm, g.fecha_inoperativo_mtto, g.costo,
                    g.fecha_cumplible_presentacion, g.fecha_ingreso_cein, g.dias_inoperativo
                FROM mantenimiento.bitacora_mtto_guardada g
                WHERE (g.fecha::timestamp + COALESCE(g.hora, '00:00:00'::time)) <= %s::timestamp
                ORDER BY g.id_bus, g.fecha DESC, g.hora DESC NULLS LAST, g.id DESC
            ),
            {cte_inmov}
            SELECT
                %s::date                                              AS fecha_disponibilidad,
                %s::text                                              AS hora_disponibilidad,
                to_char(u.fecha, 'YYYY-MM-DD')                        AS fecha_cambio,
                to_char(u.hora, 'HH24:MI')                            AS hora_cambio,
                comp.componente,
                z.zona,
                c.cop,
                b.tipologia,
                b.placa,
                b.no_interno,
                COALESCE(ed.estado_disponibilidad, 'Sin gestión')     AS estado_disponibilidad,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ub.ubicacion END              AS ubicacion,
                u.novedad,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE sf.sistema_funcional END      AS sistema_funcional,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ce.causa_entrada_inoperativo END AS causa_inoperativo,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE ep.estado_pendiente_actual END   AS estado_pendiente_actual,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.ot_sap_pm::text END         AS ot_sap_pm,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.reserva_sap_mm::text END    AS reserva_sap_mm,
                CASE WHEN {es_op} THEN 'No Aplica'
                     ELSE to_char(u.fecha_inoperativo_mtto, 'YYYY-MM-DD') END         AS fecha_inoperativo_mtto,
                -- Corre hasta el corte mientras el bus siga Inoperativo
                CASE
                    WHEN LOWER(TRIM(COALESCE(ed.estado_disponibilidad, ''))) = 'inoperativo'
                     AND u.fecha_inoperativo_mtto IS NOT NULL
                    THEN GREATEST(0, (%s::date - u.fecha_inoperativo_mtto))
                    ELSE 0
                END                                                   AS dias_inoperativo,
                CASE
                    WHEN i.id_bus IS NULL THEN 'Habilitado'
                    WHEN i.fecha_habilitacion IS NOT NULL
                     AND i.fecha_habilitacion < %s::timestamp THEN 'Habilitado'
                    ELSE 'Inmovilizado'
                END                                                   AS inmovilizado_tmsa,
                CASE WHEN {es_op} THEN 'No Aplica' ELSE u.costo::text END             AS costo,
                CASE WHEN {es_op} THEN 'No Aplica'
                     ELSE to_char(u.fecha_cumplible_presentacion, 'YYYY-MM-DD') END   AS fecha_cumplible_presentacion,
                CASE WHEN {es_op} THEN 'No Aplica'
                     ELSE to_char(u.fecha_ingreso_cein, 'YYYY-MM-DD') END             AS fecha_ingreso_cein,
                to_char(i.fecha_inmovilizacion, 'YYYY-MM-DD HH24:MI')  AS fecha_inmovilizacion,
                i.causa_inmovilizacion,
                i.creador,
                to_char(i.fecha_revision, 'YYYY-MM-DD HH24:MI')        AS fecha_revision,
                i.revisor,
                to_char(i.fecha_habilitacion, 'YYYY-MM-DD HH24:MI')    AS fecha_habilitacion,
                i.habilitador,
                CASE
                    WHEN i.id_bus IS NULL THEN 0
                    ELSE ROUND(EXTRACT(EPOCH FROM (
                            COALESCE(i.fecha_habilitacion, %s::timestamp) - i.fecha_inmovilizacion
                         )) / 86400.0, 1)
                END                                                   AS dias_inmovilizacion,
                i.descripcion_novedad,
                i.descripcion_revision
            FROM config.buses_cexp b
            LEFT JOIN config.cop        c    ON c.id    = b.id_cop
            LEFT JOIN config.componente comp ON comp.id = c.id_componente
            LEFT JOIN config.zona       z    ON z.id    = c.id_zona
            LEFT JOIN ultima            u    ON u.id_bus = b.id
            LEFT JOIN inmov             i    ON i.id_bus = b.id
            LEFT JOIN mantenimiento.estado_disponibilidad     ed ON ed.id = u.id_estado_disponibilidad
            LEFT JOIN mantenimiento.ubicacion                 ub ON ub.id = u.id_ubicacion
            LEFT JOIN mantenimiento.estado_pendiente_actual   ep ON ep.id = u.id_estado_pendiente_actual
            LEFT JOIN mantenimiento.sistema_funcional         sf ON sf.id = u.id_sistema_funcional
            LEFT JOIN mantenimiento.causa_entrada_inoperativo ce ON ce.id = u.id_causa_entrada_inoperativo
            WHERE 1=1
              AND (%s::text   IS NULL OR b.placa        ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.no_interno   ILIKE '%%' || %s || '%%')
              AND (%s::text   IS NULL OR b.tipologia     = %s)
              AND (%s::text   IS NULL OR b.linea         = %s)
              AND (%s::bigint IS NULL OR c.id_componente = %s)
              AND (%s::bigint IS NULL OR c.id_zona       = %s)
              AND (%s::bigint IS NULL OR b.id_cop        = %s)
              AND (%s::int    IS NULL OR b.estado        = %s)
              AND (%s::int    IS NULL OR u.id_estado_disponibilidad = %s)
              AND (%s::text   IS NULL OR (CASE
                    WHEN i.id_bus IS NULL THEN 'Habilitado'
                    WHEN i.fecha_habilitacion IS NOT NULL
                     AND i.fecha_habilitacion < %s::timestamp THEN 'Habilitado'
                    ELSE 'Inmovilizado' END) = %s)
              AND (%s::bigint[] IS NULL OR b.id_cop = ANY(%s))
            ORDER BY
                CASE
                    WHEN COALESCE(b.no_interno, '') ~ '^[0-9]+$' THEN 0
                    ELSE 1
                END,
                LENGTH(COALESCE(b.no_interno, '')),
                COALESCE(b.no_interno, '') ASC;
        """

        # Orden: ultima(momento) + inmov(momento) + fecha/hora disponibilidad
        # + días inoperativo + inmovilizado + días inmovilización + filtros
        # (el filtro de inmovilizado repite el momento antes de su valor)
        params = (
            [momento] + params_inmov
            + [fecha_corte, hora_corte, fecha_corte, momento, momento]
            + filtros[:-2] + [inmovilizado_tmsa, momento, inmovilizado_tmsa, cops_rep, cops_rep]
        )

        return self._fetchall(sql, params), f"{fecha_corte} {hora_corte}"

    # =========================================================
    # GUARDAR GESTIÓN (siempre inserta → conserva el histórico)
    # =========================================================

    def guardar_gestion(
        self,
        id_bus: int,
        id_usuario_registra: Optional[int],
        fecha: Optional[date] = None,
        hora: Optional[str] = None,
        novedad: Optional[str] = None,
        id_sistema_funcional: Optional[int] = None,
        id_causa_entrada_inoperativo: Optional[int] = None,
        id_estado_pendiente_actual: Optional[int] = None,
        id_ubicacion: Optional[int] = None,
        id_estado_disponibilidad: Optional[int] = None,
        ot_sap_pm: Optional[int] = None,
        reserva_sap_mm: Optional[int] = None,
        fecha_inoperativo_mtto: Optional[date] = None,
        costo: Optional[float] = None,
        fecha_cumplible_presentacion: Optional[date] = None,
        fecha_ingreso_cein: Optional[date] = None,
        ruta_archivos: Optional[List[Dict]] = None,
        permite_fecha_retroactiva: bool = False,
    ) -> Dict:
        """
        Inserta un registro nuevo de bitácora. Nunca actualiza registros previos:
        el histórico de cambios se construye con cada guardado.

        Reglas de negocio:
          · La novedad es obligatoria en cualquier estado de disponibilidad.
          · En estado Operativo los campos de inoperatividad no aplican y se
            guardan en NULL, sin importar lo que llegue del formulario.
        """
        self._asegurar_estructura()

        if not (novedad or "").strip():
            raise ValueError("La novedad / observación es obligatoria")

        self.validar_fecha_retroactiva(fecha, permite_fecha_retroactiva)

        # Un bus desvinculado o inactivo no se puede gestionar
        bus = self._fetchone(
            "SELECT estado FROM config.buses_cexp WHERE id = %s;", [int(id_bus)]
        )
        if not bus:
            raise ValueError("El bus no existe en la flota")
        if int(bus.get("estado") or 0) != 1:
            raise ValueError(MENSAJE_BUS_INACTIVO)

        fecha_gestion = fecha or ahora_bogota().date()
        operativo = self.es_estado_operativo(id_estado_disponibilidad)

        if operativo:
            # Cierra el ciclo abierto: guarda los días acumulados y limpia el contador
            inicio = self._ciclos_abiertos([id_bus]).get(int(id_bus))
            dias_inoperativo = self._dias_entre(inicio, fecha_gestion)

            id_sistema_funcional = None
            id_causa_entrada_inoperativo = None
            id_estado_pendiente_actual = None
            id_ubicacion = None
            ot_sap_pm = None
            reserva_sap_mm = None
            fecha_inoperativo_mtto = None
            costo = None
            fecha_cumplible_presentacion = None
            fecha_ingreso_cein = None
        else:
            dias_inoperativo = self._dias_entre(fecha_inoperativo_mtto, fecha_gestion)

        sql = """
            INSERT INTO mantenimiento.bitacora_mtto_guardada (
                fecha, hora, id_bus, novedad,
                id_sistema_funcional, id_causa_entrada_inoperativo,
                id_estado_pendiente_actual, id_ubicacion, id_estado_disponibilidad,
                ot_sap_pm, reserva_sap_mm, fecha_inoperativo_mtto, costo,
                fecha_cumplible_presentacion, fecha_ingreso_cein, dias_inoperativo,
                id_usuario_registra, fecha_guardado, ruta_archivos
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s::jsonb
            )
            RETURNING id;
        """
        self.cursor.execute(sql, (
            fecha_gestion,
            hora or ahora_bogota().strftime("%H:%M"),
            int(id_bus),
            (novedad or "").strip() or None,
            id_sistema_funcional,
            id_causa_entrada_inoperativo,
            id_estado_pendiente_actual,
            id_ubicacion,
            id_estado_disponibilidad,
            ot_sap_pm,
            reserva_sap_mm,
            fecha_inoperativo_mtto,
            costo,
            fecha_cumplible_presentacion,
            fecha_ingreso_cein,
            dias_inoperativo,
            id_usuario_registra,
            ahora_bogota().replace(tzinfo=None),
            json.dumps(ruta_archivos or []),
        ))
        fila = self.cursor.fetchone()
        self.connection.commit()
        return self.obtener_registro(fila["id"])

    # =========================================================
    # GESTIÓN MASIVA (una misma gestión para varios buses)
    # =========================================================

    def guardar_gestion_masiva(
        self,
        ids_bus: List[int],
        id_usuario_registra: Optional[int],
        fecha: Optional[date] = None,
        hora: Optional[str] = None,
        novedad: Optional[str] = None,
        id_sistema_funcional: Optional[int] = None,
        id_causa_entrada_inoperativo: Optional[int] = None,
        id_estado_pendiente_actual: Optional[int] = None,
        id_ubicacion: Optional[int] = None,
        id_estado_disponibilidad: Optional[int] = None,
        ot_sap_pm: Optional[int] = None,
        reserva_sap_mm: Optional[int] = None,
        fecha_inoperativo_mtto: Optional[date] = None,
        costo: Optional[float] = None,
        fecha_cumplible_presentacion: Optional[date] = None,
        fecha_ingreso_cein: Optional[date] = None,
        permite_fecha_retroactiva: bool = False,
    ) -> Dict[str, Any]:
        """
        Inserta la MISMA gestión para varios buses en una sola transacción.

        Igual que el guardado individual, cada bus recibe un registro nuevo, así
        que el histórico de cada uno queda completo. No sube evidencias: los
        archivos se cargan por bus desde su propio modal.
        """
        self._asegurar_estructura()

        ids = [int(i) for i in ids_bus if i]
        if not ids:
            raise ValueError("No se recibieron buses para la gestión masiva")

        if not (novedad or "").strip():
            raise ValueError("La novedad / observación es obligatoria")

        self.validar_fecha_retroactiva(fecha, permite_fecha_retroactiva)

        # Misma regla que en el guardado individual: Operativo → campos en NULL
        operativo = self.es_estado_operativo(id_estado_disponibilidad)
        if operativo:
            id_sistema_funcional = None
            id_causa_entrada_inoperativo = None
            id_estado_pendiente_actual = None
            id_ubicacion = None
            ot_sap_pm = None
            reserva_sap_mm = None
            fecha_inoperativo_mtto = None
            costo = None
            fecha_cumplible_presentacion = None
            fecha_ingreso_cein = None

        # Solo buses vinculados y activos: los inactivos no se pueden gestionar
        existentes = self._fetchall(
            "SELECT id FROM config.buses_cexp WHERE id = ANY(%s) AND estado = 1 ORDER BY id;",
            [ids],
        )
        ids_validos = [r["id"] for r in existentes]
        if not ids_validos:
            raise ValueError(MENSAJE_BUS_INACTIVO)

        fecha_gestion = fecha or ahora_bogota().date()

        # Cada bus puede venir de un ciclo de inoperatividad distinto
        ciclos = self._ciclos_abiertos(ids_validos) if operativo else {}

        valores = [(
            fecha_gestion,
            hora or ahora_bogota().strftime("%H:%M"),
            id_bus,
            (novedad or "").strip() or None,
            id_sistema_funcional,
            id_causa_entrada_inoperativo,
            id_estado_pendiente_actual,
            id_ubicacion,
            id_estado_disponibilidad,
            ot_sap_pm,
            reserva_sap_mm,
            fecha_inoperativo_mtto,
            costo,
            fecha_cumplible_presentacion,
            fecha_ingreso_cein,
            (self._dias_entre(ciclos.get(id_bus), fecha_gestion) if operativo
             else self._dias_entre(fecha_inoperativo_mtto, fecha_gestion)),
            id_usuario_registra,
            ahora_bogota().replace(tzinfo=None),
            "[]",
        ) for id_bus in ids_validos]

        sql = """
            INSERT INTO mantenimiento.bitacora_mtto_guardada (
                fecha, hora, id_bus, novedad,
                id_sistema_funcional, id_causa_entrada_inoperativo,
                id_estado_pendiente_actual, id_ubicacion, id_estado_disponibilidad,
                ot_sap_pm, reserva_sap_mm, fecha_inoperativo_mtto, costo,
                fecha_cumplible_presentacion, fecha_ingreso_cein, dias_inoperativo,
                id_usuario_registra, fecha_guardado, ruta_archivos
            ) VALUES %s
            RETURNING id;
        """
        from psycopg2.extras import execute_values
        execute_values(
            self.cursor, sql, valores,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
            page_size=500,
        )
        self.connection.commit()

        return {
            "guardados":   len(ids_validos),
            "solicitados": len(ids),
            "omitidos":    len(ids) - len(ids_validos),
        }

    # =========================================================
    # GESTIÓN MASIVA DETALLADA (una gestión distinta por bus)
    # =========================================================

    # Tope por lote: evita bloquear la base con cargas desmedidas
    MAX_GESTIONES_LOTE = 2000

    def guardar_gestiones_detalle(
        self,
        gestiones: List[Dict[str, Any]],
        id_usuario_registra: Optional[int],
        id_estado_disponibilidad: Optional[int] = None,
        permite_fecha_retroactiva: bool = False,
        cops_permitidos: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Guarda una gestión propia para cada bus (caso Inoperativo, donde cada
        vehículo tiene su particularidad).

        Se valida fila por fila y se inserta todo en UNA sola sentencia por lote
        dentro de una única transacción: no se abre una conexión por bus ni se
        ejecutan miles de INSERT sueltos.
        """
        self._asegurar_estructura()

        if not gestiones:
            raise ValueError("No se recibieron gestiones para guardar")
        if len(gestiones) > self.MAX_GESTIONES_LOTE:
            raise ValueError(
                f"El lote supera el máximo permitido de {self.MAX_GESTIONES_LOTE} registros"
            )

        operativo = self.es_estado_operativo(id_estado_disponibilidad)

        # Buses válidos: existentes, activos y dentro de los COP del usuario
        ids = sorted({int(g.get("id_bus")) for g in gestiones if g.get("id_bus")})
        if not ids:
            raise ValueError("Ninguna fila tiene un bus asociado")

        cops = list(cops_permitidos) if cops_permitidos else None
        habilitados = {
            r["id"]: r["placa"]
            for r in self._fetchall(
                """
                SELECT id, placa
                FROM config.buses_cexp
                WHERE id = ANY(%s)
                  AND estado = 1
                  AND (%s::bigint[] IS NULL OR id_cop = ANY(%s));
                """,
                [ids, cops, cops],
            )
        }

        # Campos obligatorios cuando el bus queda Inoperativo
        obligatorios = [
            ("id_sistema_funcional", "Sistema Funcional"),
            ("id_causa_entrada_inoperativo", "Causa Inoperativo"),
            ("id_estado_pendiente_actual", "Estado Pendiente Actual"),
            ("id_ubicacion", "Ubicación"),
            ("ot_sap_pm", "N° OT SAP - PM"),
            ("reserva_sap_mm", "N° Reserva SAP - MM"),
            ("costo", "Costo"),
            ("fecha_inoperativo_mtto", "Fecha Inoperativo Mtto"),
            ("fecha_cumplible_presentacion", "Fecha Cumplible Presentación"),
            ("fecha_ingreso_cein", "Fecha Ingreso a CEIN"),
        ]

        ahora = ahora_bogota()
        valores, errores = [], []

        for fila, g in enumerate(gestiones, start=1):
            id_bus = int(g.get("id_bus") or 0)
            etiqueta = g.get("placa") or habilitados.get(id_bus) or f"fila {fila}"

            if id_bus not in habilitados:
                errores.append({"fila": fila, "bus": etiqueta,
                                "motivo": MENSAJE_BUS_INACTIVO})
                continue

            if not (g.get("novedad") or "").strip():
                errores.append({"fila": fila, "bus": etiqueta,
                                "motivo": "La novedad / observación es obligatoria"})
                continue

            fecha_gestion = g.get("fecha") or ahora.date()
            try:
                self.validar_fecha_retroactiva(fecha_gestion, permite_fecha_retroactiva)
            except ValueError as exc:
                errores.append({"fila": fila, "bus": etiqueta, "motivo": str(exc)})
                continue

            if not operativo:
                faltantes = [nombre for campo, nombre in obligatorios
                             if g.get(campo) in (None, "", [])]
                if faltantes:
                    errores.append({
                        "fila": fila, "bus": etiqueta,
                        "motivo": "Faltan campos obligatorios: " + ", ".join(faltantes),
                    })
                    continue

            valores.append((
                fecha_gestion,
                (g.get("hora") or ahora.strftime("%H:%M")),
                id_bus,
                (g.get("novedad") or "").strip() or None,
                g.get("id_sistema_funcional") if not operativo else None,
                g.get("id_causa_entrada_inoperativo") if not operativo else None,
                g.get("id_estado_pendiente_actual") if not operativo else None,
                g.get("id_ubicacion") if not operativo else None,
                id_estado_disponibilidad,
                g.get("ot_sap_pm") if not operativo else None,
                g.get("reserva_sap_mm") if not operativo else None,
                g.get("fecha_inoperativo_mtto") if not operativo else None,
                g.get("costo") if not operativo else None,
                g.get("fecha_cumplible_presentacion") if not operativo else None,
                g.get("fecha_ingreso_cein") if not operativo else None,
                (0 if operativo
                 else self._dias_entre(g.get("fecha_inoperativo_mtto"), fecha_gestion)),
                id_usuario_registra,
                ahora.replace(tzinfo=None),
                "[]",
            ))

        if not valores:
            raise ValueError(
                "Ninguna fila quedó lista para guardar. " +
                (errores[0]["motivo"] if errores else "Revise los datos ingresados.")
            )

        sql = """
            INSERT INTO mantenimiento.bitacora_mtto_guardada (
                fecha, hora, id_bus, novedad,
                id_sistema_funcional, id_causa_entrada_inoperativo,
                id_estado_pendiente_actual, id_ubicacion, id_estado_disponibilidad,
                ot_sap_pm, reserva_sap_mm, fecha_inoperativo_mtto, costo,
                fecha_cumplible_presentacion, fecha_ingreso_cein, dias_inoperativo,
                id_usuario_registra, fecha_guardado, ruta_archivos
            ) VALUES %s;
        """
        from psycopg2.extras import execute_values
        execute_values(
            self.cursor, sql, valores,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
            page_size=250,   # lotes moderados: no satura la conexión
        )
        self.connection.commit()

        return {
            "guardados":   len(valores),
            "solicitados": len(gestiones),
            "omitidos":    len(errores),
            "errores":     errores[:50],
        }

    # =========================================================
    # EVIDENCIAS EN BLOB STORAGE
    # =========================================================
    # Ruta:  bitacora_mantenimiento/<PLACA>/<AAAA>/<MM>/<timestamp>_<archivo>
    # El año y el mes se toman de la fecha de la gestión.

    def _container_client(self):
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada en .env")
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(CONTAINER_BITACORA)
        try:
            container.create_container()
        except ResourceExistsError:
            pass
        return container

    def subir_evidencia(
        self,
        content: bytes,
        filename: str,
        placa: str,
        fecha_ref: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Sube un archivo de evidencia y retorna su metadata para ruta_archivos."""
        ext = (filename.rsplit(".", 1)[-1].lower() if "." in filename else "")
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")

        ref = fecha_ref or ahora_bogota().date()
        placa_folder = (placa or "SIN_PLACA").strip().upper().replace("/", "-").replace(" ", "_")
        ts = ahora_bogota().strftime("%Y%m%d_%H%M%S")
        seguro = filename.replace("/", "-").replace("\\", "-").strip()

        blob_path = (
            f"{CARPETA_BITACORA}/{placa_folder}/"
            f"{ref.strftime('%Y')}/{ref.strftime('%m')}/{ts}_{seguro}"
        )

        blob = self._container_client().get_blob_client(blob_path)
        blob.upload_blob(
            content,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

        return {
            "nombre":    filename,
            "ruta_blob": blob_path,
            "tipo":      ext,
            "tamano":    len(content),
            "fecha":     ahora_bogota().strftime("%Y-%m-%d %H:%M"),
        }

    def descargar_evidencia(self, ruta_blob: str) -> Tuple[bytes, str, str]:
        """Descarga una evidencia; retorna (contenido, content_type, nombre)."""
        if not (ruta_blob or "").startswith(f"{CARPETA_BITACORA}/"):
            raise ValueError("Ruta de archivo no permitida")

        blob = self._container_client().get_blob_client(ruta_blob)
        contenido = blob.download_blob().readall()

        nombre = ruta_blob.split("/")[-1]
        ext = (nombre.rsplit(".", 1)[-1].lower() if "." in nombre else "")
        return contenido, _CONTENT_TYPES.get(ext, "application/octet-stream"), nombre

    def listar_evidencias_bus(self, placa: str) -> List[Dict[str, Any]]:
        """Todas las evidencias cargadas para una placa (navegación por blob)."""
        placa_folder = (placa or "SIN_PLACA").strip().upper().replace("/", "-").replace(" ", "_")
        prefijo = f"{CARPETA_BITACORA}/{placa_folder}/"

        archivos = []
        for blob in self._container_client().list_blobs(name_starts_with=prefijo):
            nombre = blob.name.split("/")[-1]
            ext = (nombre.rsplit(".", 1)[-1].lower() if "." in nombre else "")
            archivos.append({
                "nombre":    nombre,
                "ruta_blob": blob.name,
                "tipo":      ext,
                "tamano":    blob.size or 0,
                "fecha":     blob.last_modified.isoformat() if blob.last_modified else None,
            })
        return sorted(archivos, key=lambda a: a["ruta_blob"], reverse=True)
