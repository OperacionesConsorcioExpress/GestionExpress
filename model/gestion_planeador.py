import os
import base64
import calendar
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from database.database_manager import get_db_connection

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

TZ_BOGOTA = ZoneInfo("America/Bogota")

_SCHEMA_INICIALIZADO = False
logger = logging.getLogger(__name__)

def ahora_bogota() -> datetime:
    return datetime.now(TZ_BOGOTA)

COLUMNAS_DEFECTO = ["Backlog", "Por hacer", "En curso", "Revisión", "Hecho"]


# ── Festivos Colombia ──────────────────────────────────────────────────────
def _festivos_colombia(anio: int) -> set:
    """Retorna el conjunto de fechas festivas en Colombia para el año dado."""
    def sig_lunes(d: date) -> date:
        dow = d.isoweekday()
        return d if dow == 1 else d + timedelta(days=8 - dow)

    festivos = set()
    for m, d in [(1,1),(5,1),(7,20),(8,7),(12,8),(12,25)]:
        festivos.add(date(anio, m, d))
    for m, d in [(1,6),(3,19),(6,29),(8,15),(10,12),(11,1),(11,11)]:
        festivos.add(sig_lunes(date(anio, m, d)))

    # Semana Santa (fórmula de Meeus/Jones/Butcher)
    a = anio % 19
    b, c = divmod(anio, 100)
    dd, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19*a + b - dd - g + 15) % 30
    i2, k = divmod(c, 4)
    l = (32 + 2*e + 2*i2 - h - k) % 7
    m_val = (a + 11*h + 22*l) // 451
    mes = (h + l - 7*m_val + 114) // 31
    dia = (h + l - 7*m_val + 114) % 31 + 1
    pascua = date(anio, mes, dia)
    for offset in [-3, -2, 39, 60, 68]:
        festivos.add(pascua + timedelta(days=offset))

    return festivos


def _siguiente_dia_habil(fecha: date) -> date:
    """Avanza la fecha al próximo día hábil si cae en fin de semana o festivo."""
    anio = fecha.year
    festivos = _festivos_colombia(anio)
    while fecha.isoweekday() >= 6 or fecha in festivos:
        fecha = fecha + timedelta(days=1)
        if fecha.year != anio:
            anio = fecha.year
            festivos = _festivos_colombia(anio)
    return fecha


class Gestionplaneador:
    """
    Tablas involucradas (schema planer):
        planer.equipos                  - equipos de trabajo por área
        planer.miembros_equipo          - usuarios asignados a un equipo + rol_equipo
        planer.proyectos                - proyectos de un equipo
        planer.tableros                 - tablero scrumban (1 por equipo)
        planer.columnas                 - columnas del tablero
        planer.tarjetas                 - tarjetas del tablero
        planer.checklist_items          - checklist por tarjeta
        planer.comentarios              - comentarios por tarjeta
        planer.actividades_recurrentes  - definición de recurrencias
        planer.recurrentes_ejecuciones  - ocurrencias por fecha de una recurrencia
        planer.notas                    - notas de un equipo
        planer.archivos                 - metadata de archivos subidos a Blob Storage
        public.usuarios                 - usuarios del sistema
    """

    def __enter__(self):
        self._ctx = get_db_connection()
        self.connection = self._ctx.__enter__()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        self._col_cache: dict = {}
        global _SCHEMA_INICIALIZADO
        if not _SCHEMA_INICIALIZADO:
            self._ensure_schema()
            _SCHEMA_INICIALIZADO = True
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

    # ─────────────────────────────────────────────
    # Schema / tablas
    # ─────────────────────────────────────────────
    def _ensure_schema(self):
        cur = self.cursor
        cur.execute("CREATE SCHEMA IF NOT EXISTS planer;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.equipos (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                area TEXT,
                descripcion TEXT,
                color TEXT DEFAULT '#3B82F6',
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.miembros_equipo (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
                rol_equipo TEXT NOT NULL CHECK (rol_equipo IN ('admin','editor','lector')),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE(equipo_id, usuario_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.proyectos (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                estado TEXT NOT NULL DEFAULT 'activo' CHECK (estado IN ('activo','pausado','cerrado')),
                fecha_inicio DATE,
                fecha_fin DATE,
                color TEXT DEFAULT '#3B82F6',
                responsable_id INTEGER REFERENCES usuarios(id),
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("ALTER TABLE planer.proyectos ADD COLUMN IF NOT EXISTS responsable_id INTEGER REFERENCES usuarios(id);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.tableros (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL UNIQUE REFERENCES planer.equipos(id) ON DELETE CASCADE,
                nombre TEXT NOT NULL DEFAULT 'Tablero Scrumban'
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.columnas (
                id SERIAL PRIMARY KEY,
                tablero_id INTEGER NOT NULL REFERENCES planer.tableros(id) ON DELETE CASCADE,
                nombre TEXT NOT NULL,
                orden INTEGER NOT NULL DEFAULT 0,
                wip_limit INTEGER,
                color TEXT DEFAULT '#6B7280'
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.tarjetas (
                id SERIAL PRIMARY KEY,
                columna_id INTEGER NOT NULL REFERENCES planer.columnas(id) ON DELETE CASCADE,
                proyecto_id INTEGER REFERENCES planer.proyectos(id) ON DELETE SET NULL,
                titulo TEXT NOT NULL,
                descripcion TEXT,
                asignado_a INTEGER REFERENCES usuarios(id),
                prioridad TEXT NOT NULL DEFAULT 'media' CHECK (prioridad IN ('baja','media','alta','urgente')),
                etiquetas TEXT[],
                fecha_limite DATE,
                orden INTEGER NOT NULL DEFAULT 0,
                archivado BOOLEAN NOT NULL DEFAULT FALSE,
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
                actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.checklist_items (
                id SERIAL PRIMARY KEY,
                tarjeta_id INTEGER NOT NULL REFERENCES planer.tarjetas(id) ON DELETE CASCADE,
                texto TEXT NOT NULL,
                completado BOOLEAN NOT NULL DEFAULT FALSE,
                orden INTEGER NOT NULL DEFAULT 0
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.comentarios (
                id SERIAL PRIMARY KEY,
                tarjeta_id INTEGER NOT NULL REFERENCES planer.tarjetas(id) ON DELETE CASCADE,
                usuario_id INTEGER REFERENCES usuarios(id),
                texto TEXT NOT NULL,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.actividades_recurrentes (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                titulo TEXT NOT NULL,
                descripcion TEXT,
                frecuencia TEXT NOT NULL CHECK (frecuencia IN ('diaria','semanal','mensual')),
                dias_semana INTEGER[],
                dia_mes INTEGER,
                hora TIME,
                responsable_id INTEGER REFERENCES usuarios(id),
                proyecto_id INTEGER REFERENCES planer.proyectos(id) ON DELETE SET NULL,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                duracion_minutos INTEGER NOT NULL DEFAULT 30,
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("ALTER TABLE planer.actividades_recurrentes ADD COLUMN IF NOT EXISTS duracion_minutos INTEGER NOT NULL DEFAULT 30;")
        cur.execute("ALTER TABLE planer.tarjetas ADD COLUMN IF NOT EXISTS fecha_inicio DATE;")
        cur.execute("ALTER TABLE planer.tarjetas ADD COLUMN IF NOT EXISTS predecesora_id INTEGER REFERENCES planer.tarjetas(id) ON DELETE SET NULL;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.recurrentes_ejecuciones (
                id SERIAL PRIMARY KEY,
                actividad_id INTEGER NOT NULL REFERENCES planer.actividades_recurrentes(id) ON DELETE CASCADE,
                fecha DATE NOT NULL,
                estado TEXT NOT NULL DEFAULT 'pendiente' CHECK (estado IN ('pendiente','completada','omitida','no_realizada')),
                completado_por INTEGER REFERENCES usuarios(id),
                completado_en TIMESTAMPTZ,
                observaciones TEXT,
                UNIQUE(actividad_id, fecha)
            );
        """)
        # Ampliar constraint si la tabla ya existía con el CHECK antiguo (sin 'no_realizada')
        cur.execute("""
            DO $$
            DECLARE con_name text;
            BEGIN
                SELECT conname INTO con_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname='planer' AND t.relname='recurrentes_ejecuciones'
                  AND c.contype='c' AND c.conname LIKE '%estado%';
                IF con_name IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE planer.recurrentes_ejecuciones DROP CONSTRAINT ' || quote_ident(con_name);
                    EXECUTE 'ALTER TABLE planer.recurrentes_ejecuciones ADD CONSTRAINT recurrentes_ejecuciones_estado_check CHECK (estado IN (''pendiente'',''completada'',''omitida'',''no_realizada''))';
                END IF;
            END $$;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.notas (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                titulo TEXT NOT NULL,
                contenido TEXT,
                fijado BOOLEAN NOT NULL DEFAULT FALSE,
                responsable_id INTEGER REFERENCES usuarios(id),
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
                actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("ALTER TABLE planer.notas ADD COLUMN IF NOT EXISTS responsable_id INTEGER REFERENCES usuarios(id);")
        cur.execute("ALTER TABLE planer.notas ADD COLUMN IF NOT EXISTS color TEXT;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.archivos (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                nota_id INTEGER REFERENCES planer.notas(id) ON DELETE CASCADE,
                nombre_archivo TEXT NOT NULL,
                blob_path TEXT NOT NULL,
                tamano_bytes BIGINT,
                content_type TEXT,
                subido_por INTEGER REFERENCES usuarios(id),
                subido_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("ALTER TABLE planer.archivos ADD COLUMN IF NOT EXISTS tarjeta_id INTEGER REFERENCES planer.tarjetas(id) ON DELETE CASCADE;")
        cur.execute("ALTER TABLE planer.actividades_recurrentes ADD COLUMN IF NOT EXISTS mes_inicio INTEGER;")
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE planer.actividades_recurrentes
                    DROP CONSTRAINT IF EXISTS actividades_recurrentes_frecuencia_check;
                EXECUTE 'ALTER TABLE planer.actividades_recurrentes ADD CONSTRAINT actividades_recurrentes_frecuencia_check
                    CHECK (frecuencia IN (''diaria'',''semanal'',''quincenal'',''mensual'',''trimestral'',''semestral'',''anual''))';
            END $$;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.notas_historial (
                id SERIAL PRIMARY KEY,
                nota_id INTEGER NOT NULL REFERENCES planer.notas(id) ON DELETE CASCADE,
                usuario_id INTEGER REFERENCES usuarios(id),
                accion TEXT NOT NULL,
                detalle TEXT,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS planer.eventos_calendario (
                id SERIAL PRIMARY KEY,
                equipo_id INTEGER NOT NULL REFERENCES planer.equipos(id) ON DELETE CASCADE,
                titulo TEXT NOT NULL,
                observacion TEXT,
                fecha DATE NOT NULL,
                creado_por INTEGER REFERENCES usuarios(id),
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
                actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        self.connection.commit()

    # ─────────────────────────────────────────────
    # Equipos / miembros / permisos
    # ─────────────────────────────────────────────
    def crear_equipo(self, nombre, area, descripcion, color, usuario_id):
        self.cursor.execute(
            """INSERT INTO planer.equipos (nombre, area, descripcion, color, creado_por)
               VALUES (%s,%s,%s,%s,%s) RETURNING *""",
            (nombre, area, descripcion, color or '#3B82F6', usuario_id),
        )
        equipo = self.cursor.fetchone()
        self.cursor.execute(
            """INSERT INTO planer.miembros_equipo (equipo_id, usuario_id, rol_equipo)
               VALUES (%s,%s,'admin')""",
            (equipo["id"], usuario_id),
        )
        self.cursor.execute(
            """INSERT INTO planer.tableros (equipo_id, nombre) VALUES (%s,'Tablero Scrumban') RETURNING id""",
            (equipo["id"],),
        )
        tablero = self.cursor.fetchone()
        for i, nombre_col in enumerate(COLUMNAS_DEFECTO):
            self.cursor.execute(
                """INSERT INTO planer.columnas (tablero_id, nombre, orden) VALUES (%s,%s,%s)""",
                (tablero["id"], nombre_col, i),
            )
        self.connection.commit()
        return equipo

    def listar_equipos(self, usuario_id):
        self.cursor.execute(
            """SELECT e.*, m.rol_equipo
               FROM planer.equipos e
               JOIN planer.miembros_equipo m ON m.equipo_id = e.id
               WHERE m.usuario_id = %s AND e.activo = TRUE
               ORDER BY e.nombre""",
            (usuario_id,),
        )
        return self.cursor.fetchall()

    def obtener_nombre_equipo(self, equipo_id):
        self.cursor.execute("SELECT nombre FROM planer.equipos WHERE id=%s", (equipo_id,))
        row = self.cursor.fetchone()
        return row["nombre"] if row else f"equipo_{equipo_id}"

    def obtener_titulo_nota(self, nota_id):
        self.cursor.execute("SELECT titulo FROM planer.notas WHERE id=%s", (nota_id,))
        row = self.cursor.fetchone()
        return row["titulo"] if row else f"nota_{nota_id}"

    def actualizar_equipo(self, equipo_id, nombre, area, descripcion, color):
        self.cursor.execute(
            """UPDATE planer.equipos SET nombre=%s, area=%s, descripcion=%s, color=%s
               WHERE id=%s RETURNING *""",
            (nombre, area, descripcion, color, equipo_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_equipo(self, equipo_id):
        self.cursor.execute("UPDATE planer.equipos SET activo = FALSE WHERE id=%s", (equipo_id,))
        self.connection.commit()

    def obtener_rol_usuario(self, equipo_id, usuario_id):
        self.cursor.execute(
            """SELECT rol_equipo FROM planer.miembros_equipo WHERE equipo_id=%s AND usuario_id=%s""",
            (equipo_id, usuario_id),
        )
        row = self.cursor.fetchone()
        return row["rol_equipo"] if row else None

    def listar_miembros(self, equipo_id):
        self.cursor.execute(
            """SELECT m.id, m.usuario_id, m.rol_equipo, u.nombres, u.apellidos, u.username
               FROM planer.miembros_equipo m
               JOIN usuarios u ON u.id = m.usuario_id
               WHERE m.equipo_id=%s
               ORDER BY u.nombres""",
            (equipo_id,),
        )
        return self.cursor.fetchall()

    def agregar_miembro(self, equipo_id, usuario_id, rol_equipo):
        self.cursor.execute(
            """INSERT INTO planer.miembros_equipo (equipo_id, usuario_id, rol_equipo)
               VALUES (%s,%s,%s)
               ON CONFLICT (equipo_id, usuario_id) DO UPDATE SET rol_equipo=EXCLUDED.rol_equipo
               RETURNING *""",
            (equipo_id, usuario_id, rol_equipo),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def actualizar_rol_miembro(self, equipo_id, usuario_id, rol_equipo):
        self.cursor.execute(
            """UPDATE planer.miembros_equipo SET rol_equipo=%s WHERE equipo_id=%s AND usuario_id=%s RETURNING *""",
            (rol_equipo, equipo_id, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_miembro(self, equipo_id, usuario_id):
        self.cursor.execute(
            "DELETE FROM planer.miembros_equipo WHERE equipo_id=%s AND usuario_id=%s",
            (equipo_id, usuario_id),
        )
        self.connection.commit()

    # ─────────────────────────────────────────────
    # Proyectos
    # ─────────────────────────────────────────────
    def crear_proyecto(self, equipo_id, nombre, descripcion, estado, fecha_inicio, fecha_fin, color, responsable_id, usuario_id):
        self.cursor.execute(
            """INSERT INTO planer.proyectos
               (equipo_id, nombre, descripcion, estado, fecha_inicio, fecha_fin, color, responsable_id, creado_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (equipo_id, nombre, descripcion, estado or 'activo', fecha_inicio, fecha_fin, color or '#3B82F6', responsable_id, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def listar_proyectos(self, equipo_id):
        self.cursor.execute(
            """SELECT p.*, u.nombres AS responsable_nombre, u.apellidos AS responsable_apellido
               FROM planer.proyectos p
               LEFT JOIN usuarios u ON u.id = p.responsable_id
               WHERE p.equipo_id=%s ORDER BY p.creado_en DESC""",
            (equipo_id,),
        )
        return self.cursor.fetchall()

    def actualizar_proyecto(self, proyecto_id, nombre, descripcion, estado, fecha_inicio, fecha_fin, color, responsable_id=None):
        self.cursor.execute(
            """UPDATE planer.proyectos SET nombre=%s, descripcion=%s, estado=%s,
               fecha_inicio=%s, fecha_fin=%s, color=%s, responsable_id=COALESCE(%s, responsable_id)
               WHERE id=%s RETURNING *""",
            (nombre, descripcion, estado, fecha_inicio, fecha_fin, color, responsable_id, proyecto_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_proyecto(self, proyecto_id):
        self.cursor.execute("DELETE FROM planer.proyectos WHERE id=%s", (proyecto_id,))
        self.connection.commit()

    def obtener_equipo_de_proyecto(self, proyecto_id):
        self.cursor.execute("SELECT equipo_id FROM planer.proyectos WHERE id=%s", (proyecto_id,))
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    # ─────────────────────────────────────────────
    # Tablero / columnas / tarjetas
    # ─────────────────────────────────────────────
    def obtener_tablero(self, equipo_id):
        self.cursor.execute("SELECT * FROM planer.tableros WHERE equipo_id=%s", (equipo_id,))
        tablero = self.cursor.fetchone()
        if not tablero:
            return None
        self.cursor.execute(
            "SELECT * FROM planer.columnas WHERE tablero_id=%s ORDER BY orden", (tablero["id"],)
        )
        columnas = self.cursor.fetchall()
        for col in columnas:
            self.cursor.execute(
                """SELECT t.*, u.nombres AS asignado_nombre, u.apellidos AS asignado_apellido,
                          p.nombre AS proyecto_nombre, p.color AS proyecto_color,
                          tp.titulo AS predecesora_titulo
                   FROM planer.tarjetas t
                   LEFT JOIN usuarios u ON u.id = t.asignado_a
                   LEFT JOIN planer.proyectos p ON p.id = t.proyecto_id
                   LEFT JOIN planer.tarjetas tp ON tp.id = t.predecesora_id
                   WHERE t.columna_id=%s AND t.archivado = FALSE
                   ORDER BY t.orden""",
                (col["id"],),
            )
            tarjetas = self.cursor.fetchall()
            for t in tarjetas:
                self.cursor.execute(
                    "SELECT * FROM planer.checklist_items WHERE tarjeta_id=%s ORDER BY orden",
                    (t["id"],),
                )
                t["checklist"] = self.cursor.fetchall()
            col["tarjetas"] = tarjetas
        tablero["columnas"] = columnas
        return tablero

    def obtener_equipo_de_columna(self, columna_id):
        self.cursor.execute(
            """SELECT tb.equipo_id FROM planer.columnas c
               JOIN planer.tableros tb ON tb.id = c.tablero_id
               WHERE c.id=%s""",
            (columna_id,),
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    def obtener_equipo_de_tarjeta(self, tarjeta_id):
        self.cursor.execute(
            """SELECT tb.equipo_id FROM planer.tarjetas t
               JOIN planer.columnas c ON c.id = t.columna_id
               JOIN planer.tableros tb ON tb.id = c.tablero_id
               WHERE t.id=%s""",
            (tarjeta_id,),
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    def crear_columna(self, equipo_id, nombre, wip_limit, color):
        self.cursor.execute("SELECT id FROM planer.tableros WHERE equipo_id=%s", (equipo_id,))
        tablero = self.cursor.fetchone()
        self.cursor.execute(
            "SELECT COALESCE(MAX(orden),-1)+1 AS siguiente FROM planer.columnas WHERE tablero_id=%s",
            (tablero["id"],),
        )
        siguiente = self.cursor.fetchone()["siguiente"]
        self.cursor.execute(
            """INSERT INTO planer.columnas (tablero_id, nombre, orden, wip_limit, color)
               VALUES (%s,%s,%s,%s,%s) RETURNING *""",
            (tablero["id"], nombre, siguiente, wip_limit, color or '#6B7280'),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def actualizar_columna(self, columna_id, nombre, orden, wip_limit, color):
        self.cursor.execute(
            """UPDATE planer.columnas SET nombre=%s, orden=%s, wip_limit=%s, color=%s
               WHERE id=%s RETURNING *""",
            (nombre, orden, wip_limit, color, columna_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_columna(self, columna_id):
        self.cursor.execute("DELETE FROM planer.columnas WHERE id=%s", (columna_id,))
        self.connection.commit()

    def crear_tarjeta(self, columna_id, proyecto_id, titulo, descripcion, asignado_a, prioridad, etiquetas, fecha_limite, usuario_id, fecha_inicio=None, predecesora_id=None):
        self.cursor.execute(
            "SELECT COALESCE(MAX(orden),-1)+1 AS siguiente FROM planer.tarjetas WHERE columna_id=%s",
            (columna_id,),
        )
        siguiente = self.cursor.fetchone()["siguiente"]
        self.cursor.execute(
            """INSERT INTO planer.tarjetas
               (columna_id, proyecto_id, titulo, descripcion, asignado_a, prioridad, etiquetas, fecha_limite, fecha_inicio, predecesora_id, orden, creado_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (columna_id, proyecto_id, titulo, descripcion, asignado_a, prioridad or 'media',
             etiquetas, fecha_limite, fecha_inicio, predecesora_id, siguiente, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def actualizar_tarjeta(self, tarjeta_id, **campos):
        permitidos = {"columna_id", "proyecto_id", "titulo", "descripcion", "asignado_a",
                      "prioridad", "etiquetas", "fecha_limite", "fecha_inicio", "predecesora_id", "orden", "archivado"}
        sets, valores = [], []
        for k, v in campos.items():
            if k in permitidos and v is not None:
                sets.append(f"{k}=%s")
                valores.append(v)
        if not sets:
            self.cursor.execute("SELECT * FROM planer.tarjetas WHERE id=%s", (tarjeta_id,))
            return self.cursor.fetchone()
        sets.append("actualizado_en=now()")
        valores.append(tarjeta_id)
        self.cursor.execute(
            f"UPDATE planer.tarjetas SET {', '.join(sets)} WHERE id=%s RETURNING *", tuple(valores)
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_tarjeta(self, tarjeta_id):
        self.cursor.execute("DELETE FROM planer.tarjetas WHERE id=%s", (tarjeta_id,))
        self.connection.commit()

    def fechas_inicio_disponibles(self, equipo_id):
        """Devuelve los años y meses distintos con fecha_inicio registrada en tarjetas del equipo."""
        self.cursor.execute("SELECT id FROM planer.tableros WHERE equipo_id=%s", (equipo_id,))
        tablero = self.cursor.fetchone()
        if not tablero:
            return {"anios": [], "meses": []}
        self.cursor.execute(
            """SELECT DISTINCT
                   EXTRACT(YEAR FROM t.fecha_inicio)::int  AS anio,
                   EXTRACT(MONTH FROM t.fecha_inicio)::int AS mes
               FROM planer.tarjetas t
               JOIN planer.columnas c ON c.id = t.columna_id
               WHERE c.tablero_id=%s AND t.archivado=FALSE AND t.fecha_inicio IS NOT NULL
               ORDER BY anio DESC, mes""",
            (tablero["id"],),
        )
        filas = self.cursor.fetchall()
        anios = sorted({f["anio"] for f in filas}, reverse=True)
        meses = sorted({f["mes"] for f in filas})
        return {"anios": anios, "meses": meses}

    def resumen_tablero_por_persona(self, equipo_id, anio=None, mes=None):
        self.cursor.execute("SELECT id FROM planer.tableros WHERE equipo_id=%s", (equipo_id,))
        tablero = self.cursor.fetchone()
        if not tablero:
            return []
        self.cursor.execute(
            "SELECT id, nombre, orden, color FROM planer.columnas WHERE tablero_id=%s ORDER BY orden",
            (tablero["id"],),
        )
        columnas = self.cursor.fetchall()
        if not columnas:
            return []
        ultima_columna_id = columnas[-1]["id"]

        condiciones = ["c.tablero_id=%s", "t.archivado=FALSE", "t.asignado_a IS NOT NULL"]
        params = [tablero["id"]]
        if anio:
            condiciones.append("EXTRACT(YEAR FROM t.fecha_inicio)=%s")
            params.append(anio)
        if mes:
            condiciones.append("EXTRACT(MONTH FROM t.fecha_inicio)=%s")
            params.append(mes)
        where = " AND ".join(condiciones)

        self.cursor.execute(
            f"""SELECT t.asignado_a, t.columna_id, u.nombres, u.apellidos, COUNT(*) AS total
               FROM planer.tarjetas t
               JOIN planer.columnas c ON c.id = t.columna_id
               LEFT JOIN usuarios u ON u.id = t.asignado_a
               WHERE {where}
               GROUP BY t.asignado_a, t.columna_id, u.nombres, u.apellidos""",
            params,
        )
        filas = self.cursor.fetchall()

        # Conteo de tarjetas vencidas por persona (no completadas, fecha_limite pasada)
        self.cursor.execute(
            f"""SELECT t.asignado_a, COUNT(*) AS vencidas
               FROM planer.tarjetas t
               JOIN planer.columnas c ON c.id = t.columna_id
               WHERE {where}
                 AND t.columna_id != %s
                 AND t.fecha_limite IS NOT NULL
                 AND t.fecha_limite < CURRENT_DATE
               GROUP BY t.asignado_a""",
            params + [ultima_columna_id],
        )
        vencidas_map = {r["asignado_a"]: r["vencidas"] for r in self.cursor.fetchall()}

        personas = {}
        for f in filas:
            uid = f["asignado_a"]
            if uid not in personas:
                personas[uid] = {
                    "usuario_id": uid, "nombres": f["nombres"], "apellidos": f["apellidos"],
                    "columnas": {c["id"]: 0 for c in columnas}, "total": 0,
                }
            personas[uid]["columnas"][f["columna_id"]] = f["total"]
            personas[uid]["total"] += f["total"]

        resultado = []
        for p in personas.values():
            completadas = p["columnas"].get(ultima_columna_id, 0)
            vencidas = vencidas_map.get(p["usuario_id"], 0)
            resultado.append({
                "usuario_id": p["usuario_id"],
                "nombres": p["nombres"],
                "apellidos": p["apellidos"],
                "total": p["total"],
                "completadas": completadas,
                "vencidas": vencidas,
                "pct_completado": round((completadas / p["total"]) * 100) if p["total"] else 0,
                "columnas": [
                    {"columna_id": c["id"], "nombre": c["nombre"], "color": c["color"], "total": p["columnas"][c["id"]]}
                    for c in columnas
                ],
            })
        resultado.sort(key=lambda x: ((x["nombres"] or "").upper(), (x["apellidos"] or "").upper()))
        return resultado

    def crear_checklist_item(self, tarjeta_id, texto):
        self.cursor.execute(
            "SELECT COALESCE(MAX(orden),-1)+1 AS siguiente FROM planer.checklist_items WHERE tarjeta_id=%s",
            (tarjeta_id,),
        )
        siguiente = self.cursor.fetchone()["siguiente"]
        self.cursor.execute(
            """INSERT INTO planer.checklist_items (tarjeta_id, texto, orden) VALUES (%s,%s,%s) RETURNING *""",
            (tarjeta_id, texto, siguiente),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def actualizar_checklist_item(self, item_id, texto=None, completado=None):
        sets, valores = [], []
        if texto is not None:
            sets.append("texto=%s"); valores.append(texto)
        if completado is not None:
            sets.append("completado=%s"); valores.append(completado)
        if not sets:
            return None
        valores.append(item_id)
        self.cursor.execute(
            f"UPDATE planer.checklist_items SET {', '.join(sets)} WHERE id=%s RETURNING *", tuple(valores)
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_checklist_item(self, item_id):
        self.cursor.execute("DELETE FROM planer.checklist_items WHERE id=%s", (item_id,))
        self.connection.commit()

    def obtener_equipo_de_checklist_item(self, item_id):
        self.cursor.execute(
            """SELECT tb.equipo_id FROM planer.checklist_items ci
               JOIN planer.tarjetas t ON t.id = ci.tarjeta_id
               JOIN planer.columnas c ON c.id = t.columna_id
               JOIN planer.tableros tb ON tb.id = c.tablero_id
               WHERE ci.id=%s""",
            (item_id,),
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    def crear_comentario(self, tarjeta_id, usuario_id, texto):
        self.cursor.execute(
            """INSERT INTO planer.comentarios (tarjeta_id, usuario_id, texto) VALUES (%s,%s,%s) RETURNING *""",
            (tarjeta_id, usuario_id, texto),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def listar_comentarios(self, tarjeta_id):
        self.cursor.execute(
            """SELECT c.*, u.nombres, u.apellidos FROM planer.comentarios c
               LEFT JOIN usuarios u ON u.id = c.usuario_id
               WHERE c.tarjeta_id=%s ORDER BY c.creado_en""",
            (tarjeta_id,),
        )
        return self.cursor.fetchall()

    # ─────────────────────────────────────────────
    # Recurrentes
    # ─────────────────────────────────────────────
    def crear_recurrente(self, equipo_id, titulo, descripcion, frecuencia, dias_semana, dia_mes, mes_inicio, hora, responsable_id, proyecto_id, duracion_minutos, usuario_id):
        if not responsable_id:
            raise ValueError("El responsable es obligatorio")
        self.cursor.execute(
            """INSERT INTO planer.actividades_recurrentes
               (equipo_id, titulo, descripcion, frecuencia, dias_semana, dia_mes, mes_inicio, hora, responsable_id, proyecto_id, duracion_minutos, creado_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (equipo_id, titulo, descripcion, frecuencia, dias_semana, dia_mes, mes_inicio, hora, responsable_id, proyecto_id, duracion_minutos or 30, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def listar_recurrentes(self, equipo_id):
        self.cursor.execute(
            """SELECT ar.*, u.nombres AS responsable_nombre, u.apellidos AS responsable_apellido
               FROM planer.actividades_recurrentes ar
               LEFT JOIN usuarios u ON u.id = ar.responsable_id
               WHERE ar.equipo_id=%s ORDER BY ar.titulo""",
            (equipo_id,),
        )
        return self.cursor.fetchall()

    def listar_recurrentes_resumen(self, equipo_id):
        hoy = date.today()
        horizonte = hoy + timedelta(days=366)
        self._generar_ejecuciones_faltantes(equipo_id, horizonte)
        self.cursor.execute(
            """SELECT ar.*, u.nombres AS responsable_nombre, u.apellidos AS responsable_apellido
               FROM planer.actividades_recurrentes ar
               LEFT JOIN usuarios u ON u.id = ar.responsable_id
               WHERE ar.equipo_id=%s
               ORDER BY ar.activo DESC, ar.titulo""",
            (equipo_id,),
        )
        actividades = self.cursor.fetchall()
        for actividad in actividades:
            if not actividad["activo"]:
                actividad["proxima_ejecucion"] = None
                continue
            self.cursor.execute(
                """SELECT * FROM planer.recurrentes_ejecuciones
                   WHERE actividad_id=%s AND estado='pendiente'
                   ORDER BY fecha ASC LIMIT 1""",
                (actividad["id"],),
            )
            actividad["proxima_ejecucion"] = self.cursor.fetchone()
        return actividades

    def historial_ejecuciones(self, actividad_id):
        # Próximo evento pendiente (futuro o hoy pendiente)
        self.cursor.execute(
            """SELECT re.*, u.nombres AS completado_por_nombre, u.apellidos AS completado_por_apellido
               FROM planer.recurrentes_ejecuciones re
               LEFT JOIN usuarios u ON u.id = re.completado_por
               WHERE re.actividad_id=%s AND re.estado='pendiente'
               ORDER BY re.fecha ASC
               LIMIT 1""",
            (actividad_id,),
        )
        proxima = self.cursor.fetchone()
        # Últimas 10 ejecuciones ya gestionadas (completada / no_realizada / omitida), más reciente primero
        self.cursor.execute(
            """SELECT re.*, u.nombres AS completado_por_nombre, u.apellidos AS completado_por_apellido
               FROM planer.recurrentes_ejecuciones re
               LEFT JOIN usuarios u ON u.id = re.completado_por
               WHERE re.actividad_id=%s AND re.estado <> 'pendiente'
               ORDER BY re.fecha DESC
               LIMIT 10""",
            (actividad_id,),
        )
        gestionadas = list(self.cursor.fetchall())
        return ([proxima] if proxima else []) + gestionadas

    def resumen_carga_equipo(self, equipo_id):
        self.cursor.execute(
            """SELECT ar.responsable_id, u.nombres, u.apellidos, ar.frecuencia, ar.dias_semana, ar.duracion_minutos
               FROM planer.actividades_recurrentes ar
               LEFT JOIN usuarios u ON u.id = ar.responsable_id
               WHERE ar.equipo_id=%s AND ar.activo=TRUE AND ar.responsable_id IS NOT NULL""",
            (equipo_id,),
        )
        filas = self.cursor.fetchall()
        acumulado = {}
        for f in filas:
            duracion = f["duracion_minutos"] or 0
            if f["frecuencia"] == "diaria":
                minutos_semana = duracion * 5
            elif f["frecuencia"] == "quincenal":
                minutos_semana = duracion * 24 / 52
            elif f["frecuencia"] == "semanal":
                minutos_semana = duracion * len(f["dias_semana"] or [])
            elif f["frecuencia"] == "mensual":
                minutos_semana = duracion * 12 / 52
            elif f["frecuencia"] == "trimestral":
                minutos_semana = duracion * 4 / 52
            elif f["frecuencia"] == "semestral":
                minutos_semana = duracion * 2 / 52
            elif f["frecuencia"] == "anual":
                minutos_semana = duracion * 1 / 52
            else:
                minutos_semana = 0
            key = f["responsable_id"]
            if key not in acumulado:
                acumulado[key] = {
                    "usuario_id": key, "nombres": f["nombres"], "apellidos": f["apellidos"], "minutos_semana": 0,
                }
            acumulado[key]["minutos_semana"] += minutos_semana
        resultado = sorted(acumulado.values(), key=lambda x: -x["minutos_semana"])
        for r in resultado:
            r["minutos_semana"] = round(r["minutos_semana"])
        return resultado

    def actualizar_recurrente(self, actividad_id, **campos):
        permitidos = {"titulo", "descripcion", "frecuencia", "dias_semana", "dia_mes", "mes_inicio",
                      "hora", "responsable_id", "proyecto_id", "activo", "duracion_minutos"}
        sets, valores = [], []
        for k, v in campos.items():
            if k in permitidos and v is not None:
                sets.append(f"{k}=%s")
                valores.append(v)
        if not sets:
            self.cursor.execute("SELECT * FROM planer.actividades_recurrentes WHERE id=%s", (actividad_id,))
            return self.cursor.fetchone()
        valores.append(actividad_id)
        self.cursor.execute(
            f"UPDATE planer.actividades_recurrentes SET {', '.join(sets)} WHERE id=%s RETURNING *", tuple(valores)
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_recurrente(self, actividad_id):
        self.cursor.execute("DELETE FROM planer.actividades_recurrentes WHERE id=%s", (actividad_id,))
        self.connection.commit()

    def obtener_equipo_de_recurrente(self, actividad_id):
        self.cursor.execute(
            "SELECT equipo_id FROM planer.actividades_recurrentes WHERE id=%s", (actividad_id,)
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    def _aplica_en_fecha(self, actividad, fecha: date) -> bool:
        frecuencia = actividad["frecuencia"]
        if frecuencia == "diaria":
            return True
        if frecuencia == "semanal":
            dias = actividad.get("dias_semana") or []
            return (fecha.isoweekday() % 7) in dias or fecha.isoweekday() in dias
        if frecuencia == "mensual":
            return actividad.get("dia_mes") == fecha.day
        if frecuencia == "quincenal":
            dia = actividad.get("dia_mes") or 1
            dia2 = min(dia + 15, calendar.monthrange(fecha.year, fecha.month)[1])
            return fecha.day == dia or fecha.day == dia2
        mes_ini = actividad.get("mes_inicio") or 1
        dia = actividad.get("dia_mes") or 1
        if frecuencia == "trimestral":
            meses = {((mes_ini - 1 + i * 3) % 12) + 1 for i in range(4)}
            return fecha.month in meses and fecha.day == dia
        if frecuencia == "semestral":
            meses = {mes_ini, ((mes_ini - 1 + 6) % 12) + 1}
            return fecha.month in meses and fecha.day == dia
        if frecuencia == "anual":
            return fecha.month == mes_ini and fecha.day == dia
        return False

    def _generar_para_actividad(self, actividad, desde: date, hasta: date):
        """
        Genera ejecuciones respetando días hábiles:
        - diaria: solo días hábiles (lun-vie sin festivos); los festivos se omiten.
        - todas las demás: si la fecha calculada cae en fin de semana o festivo,
          se mueve al siguiente día hábil.
        """
        cursor_fecha = desde
        frecuencia = actividad["frecuencia"]
        festivos_cache: dict = {}

        def festivos(anio):
            if anio not in festivos_cache:
                festivos_cache[anio] = _festivos_colombia(anio)
            return festivos_cache[anio]

        while cursor_fecha <= hasta:
            if self._aplica_en_fecha(actividad, cursor_fecha):
                if frecuencia == "diaria":
                    # Para diaria solo generamos en días hábiles; festivos y fines de
                    # semana se omiten (no se desplazan para evitar duplicados).
                    if cursor_fecha.isoweekday() >= 6 or cursor_fecha in festivos(cursor_fecha.year):
                        cursor_fecha += timedelta(days=1)
                        continue
                    fecha_insertar = cursor_fecha
                else:
                    # Para otras frecuencias la ocurrencia se desplaza al siguiente
                    # día hábil si cae en fin de semana o festivo.
                    fecha_insertar = _siguiente_dia_habil(cursor_fecha)
                self.cursor.execute(
                    """INSERT INTO planer.recurrentes_ejecuciones (actividad_id, fecha)
                       VALUES (%s,%s) ON CONFLICT (actividad_id, fecha) DO NOTHING""",
                    (actividad["id"], fecha_insertar),
                )
            cursor_fecha += timedelta(days=1)

    def _generar_ejecuciones_faltantes(self, equipo_id, hasta: date):
        self.cursor.execute(
            "SELECT * FROM planer.actividades_recurrentes WHERE equipo_id=%s AND activo=TRUE",
            (equipo_id,),
        )
        actividades = self.cursor.fetchall()
        for actividad in actividades:
            self.cursor.execute(
                "SELECT MAX(fecha) AS ultima FROM planer.recurrentes_ejecuciones WHERE actividad_id=%s",
                (actividad["id"],),
            )
            ultima = self.cursor.fetchone()["ultima"]
            desde = (ultima + timedelta(days=1)) if ultima else actividad["creado_en"].date()
            if desde > hasta:
                continue
            self._generar_para_actividad(actividad, desde, hasta)
        self.connection.commit()

    def migrar_ejecuciones_a_dias_habiles(self, equipo_id):
        """
        Mueve (o elimina) todas las ejecuciones pendientes que caigan en fin de
        semana o festivo Colombia:
        - Actividades 'diaria': los registros en días no hábiles se ELIMINAN porque
          la política diaria no genera ejecuciones esos días.
        - Resto de frecuencias: se mueven al siguiente día hábil. Si ya existe una
          ejecución en la fecha destino, se elimina la original (evita duplicados).
        El cálculo de conflictos se hace en Python antes de tocar la BD, sin usar
        rollbacks parciales dentro del bucle.
        Retorna la cantidad de filas procesadas.
        """
        hoy = date.today()
        self.cursor.execute(
            """SELECT re.id, re.fecha, re.actividad_id, ar.frecuencia
               FROM planer.recurrentes_ejecuciones re
               JOIN planer.actividades_recurrentes ar ON ar.id = re.actividad_id
               WHERE ar.equipo_id = %s AND re.fecha >= %s AND re.estado = 'pendiente'
               ORDER BY re.actividad_id, re.fecha""",
            (equipo_id, hoy),
        )
        filas = list(self.cursor.fetchall())
        if not filas:
            return 0

        act_ids = list({f["actividad_id"] for f in filas})
        self.cursor.execute(
            """SELECT actividad_id, fecha FROM planer.recurrentes_ejecuciones
               WHERE actividad_id = ANY(%s)""",
            (act_ids,),
        )
        existentes: set = {(r["actividad_id"], r["fecha"]) for r in self.cursor.fetchall()}

        ids_eliminar: list = []
        updates: list = []  # (id, nueva_fecha)

        for fila in filas:
            es_no_habil = (
                fila["fecha"].isoweekday() >= 6
                or fila["fecha"] in _festivos_colombia(fila["fecha"].year)
            )
            if not es_no_habil:
                continue

            if fila["frecuencia"] == "diaria":
                # Días no hábiles en actividades diarias se eliminan, no se desplazan
                ids_eliminar.append(fila["id"])
                existentes.discard((fila["actividad_id"], fila["fecha"]))
            else:
                nueva = _siguiente_dia_habil(fila["fecha"])
                clave_destino = (fila["actividad_id"], nueva)
                if clave_destino in existentes:
                    # Destino ocupado → eliminar el original (es redundante)
                    ids_eliminar.append(fila["id"])
                else:
                    updates.append((fila["id"], nueva))
                    existentes.add(clave_destino)
                existentes.discard((fila["actividad_id"], fila["fecha"]))

        actualizadas = 0
        if ids_eliminar:
            self.cursor.execute(
                "DELETE FROM planer.recurrentes_ejecuciones WHERE id = ANY(%s)",
                (ids_eliminar,),
            )
            actualizadas += len(ids_eliminar)

        for row_id, nueva_fecha in updates:
            self.cursor.execute(
                "UPDATE planer.recurrentes_ejecuciones SET fecha=%s WHERE id=%s",
                (nueva_fecha, row_id),
            )
            actualizadas += 1

        self.connection.commit()
        return actualizadas

    def recalcular_ejecuciones_actividad(self, actividad_id):
        """Elimina ejecuciones futuras sin gestión y regenera según la frecuencia actual."""
        hoy = date.today()
        self.cursor.execute(
            """DELETE FROM planer.recurrentes_ejecuciones
               WHERE actividad_id=%s AND fecha >= %s AND estado='pendiente'""",
            (actividad_id, hoy),
        )
        self.cursor.execute(
            "SELECT * FROM planer.actividades_recurrentes WHERE id=%s", (actividad_id,)
        )
        actividad = self.cursor.fetchone()
        if actividad and actividad["activo"]:
            horizonte = hoy + timedelta(days=366)
            self._generar_para_actividad(actividad, hoy, horizonte)
        self.connection.commit()

    def obtener_ejecuciones(self, equipo_id, desde: date, hasta: date):
        self._generar_ejecuciones_faltantes(equipo_id, hasta)
        self.cursor.execute(
            """SELECT re.*, ar.titulo, ar.descripcion, ar.responsable_id, ar.proyecto_id,
                      u.nombres AS responsable_nombre, u.apellidos AS responsable_apellido
               FROM planer.recurrentes_ejecuciones re
               JOIN planer.actividades_recurrentes ar ON ar.id = re.actividad_id
               LEFT JOIN usuarios u ON u.id = ar.responsable_id
               WHERE ar.equipo_id=%s AND re.fecha BETWEEN %s AND %s
               ORDER BY re.fecha, ar.titulo""",
            (equipo_id, desde, hasta),
        )
        return self.cursor.fetchall()

    def marcar_ejecucion(self, ejecucion_id, estado, usuario_id, observaciones=None):
        self.cursor.execute(
            """UPDATE planer.recurrentes_ejecuciones
               SET estado=%s, completado_por=%s, completado_en=now(), observaciones=COALESCE(%s, observaciones)
               WHERE id=%s RETURNING *""",
            (estado, usuario_id, observaciones, ejecucion_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def obtener_equipo_de_ejecucion(self, ejecucion_id):
        self.cursor.execute(
            """SELECT ar.equipo_id FROM planer.recurrentes_ejecuciones re
               JOIN planer.actividades_recurrentes ar ON ar.id = re.actividad_id
               WHERE re.id=%s""",
            (ejecucion_id,),
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    # ─────────────────────────────────────────────
    # Notas / archivos
    # ─────────────────────────────────────────────
    def crear_nota(self, equipo_id, titulo, contenido, usuario_id, color=None):
        self.cursor.execute(
            """INSERT INTO planer.notas (equipo_id, titulo, contenido, color, creado_por)
               VALUES (%s,%s,%s,%s,%s) RETURNING *""",
            (equipo_id, titulo, contenido, color, usuario_id),
        )
        row = self.cursor.fetchone()
        self.cursor.execute(
            "INSERT INTO planer.notas_historial (nota_id, usuario_id, accion, detalle) VALUES (%s,%s,%s,%s)",
            (row["id"], usuario_id, "Creación", f"Nota «{titulo}» creada"),
        )
        self.connection.commit()
        return row

    def listar_notas(self, equipo_id):
        self.cursor.execute(
            """SELECT n.*,
                      uc.nombres AS creado_por_nombre, uc.apellidos AS creado_por_apellido,
                      uh.nombres AS modificado_por_nombre, uh.apellidos AS modificado_por_apellido,
                      h.usuario_id AS modificado_por_id, h.creado_en AS ultima_modificacion
               FROM planer.notas n
               LEFT JOIN usuarios uc ON uc.id = n.creado_por
               LEFT JOIN LATERAL (
                   SELECT usuario_id, creado_en FROM planer.notas_historial
                   WHERE nota_id = n.id ORDER BY creado_en DESC LIMIT 1
               ) h ON TRUE
               LEFT JOIN usuarios uh ON uh.id = h.usuario_id
               WHERE n.equipo_id=%s ORDER BY n.fijado DESC, n.actualizado_en DESC""",
            (equipo_id,),
        )
        notas = self.cursor.fetchall()
        for nota in notas:
            self.cursor.execute(
                "SELECT * FROM planer.archivos WHERE nota_id=%s ORDER BY subido_en", (nota["id"],)
            )
            nota["archivos"] = self.cursor.fetchall()
        return notas

    def actualizar_nota(self, nota_id, usuario_id, titulo=None, contenido=None, fijado=None, color=None):
        sets, valores = [], []
        cambios = []
        if titulo is not None:
            sets.append("titulo=%s"); valores.append(titulo); cambios.append("título")
        if contenido is not None:
            sets.append("contenido=%s"); valores.append(contenido); cambios.append("contenido")
        if fijado is not None:
            sets.append("fijado=%s"); valores.append(fijado); cambios.append("fijado" if fijado else "desfijado")
        if color is not None:
            sets.append("color=%s"); valores.append(color); cambios.append("color")
        if not sets:
            self.cursor.execute("SELECT * FROM planer.notas WHERE id=%s", (nota_id,))
            return self.cursor.fetchone()
        sets.append("actualizado_en=now()")
        valores.append(nota_id)
        self.cursor.execute(
            f"UPDATE planer.notas SET {', '.join(sets)} WHERE id=%s RETURNING *", tuple(valores)
        )
        row = self.cursor.fetchone()
        detalle = "Modificó: " + ", ".join(cambios) if cambios else "Actualización"
        self.cursor.execute(
            "INSERT INTO planer.notas_historial (nota_id, usuario_id, accion, detalle) VALUES (%s,%s,%s,%s)",
            (nota_id, usuario_id, "Edición", detalle),
        )
        self.connection.commit()
        return row

    def obtener_historial_nota(self, nota_id):
        self.cursor.execute(
            """SELECT h.*, u.nombres, u.apellidos
               FROM planer.notas_historial h
               LEFT JOIN usuarios u ON u.id = h.usuario_id
               WHERE h.nota_id=%s ORDER BY h.creado_en DESC
               LIMIT 20""",
            (nota_id,),
        )
        return self.cursor.fetchall()

    def eliminar_nota(self, nota_id):
        self.cursor.execute("DELETE FROM planer.notas WHERE id=%s", (nota_id,))
        self.connection.commit()

    # ─────────────────────────────────────────────
    # Calendario — eventos manuales + datos agregados
    # ─────────────────────────────────────────────
    def crear_evento_calendario(self, equipo_id, titulo, observacion, fecha, usuario_id):
        self.cursor.execute(
            "INSERT INTO planer.eventos_calendario (equipo_id, titulo, observacion, fecha, creado_por) VALUES (%s,%s,%s,%s,%s) RETURNING *",
            (equipo_id, titulo, observacion, fecha, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def actualizar_evento_calendario(self, evento_id, titulo=None, observacion=None, fecha=None):
        sets, valores = [], []
        if titulo is not None: sets.append("titulo=%s"); valores.append(titulo)
        if observacion is not None: sets.append("observacion=%s"); valores.append(observacion)
        if fecha is not None: sets.append("fecha=%s"); valores.append(fecha)
        if not sets:
            self.cursor.execute("SELECT * FROM planer.eventos_calendario WHERE id=%s", (evento_id,))
            return self.cursor.fetchone()
        sets.append("actualizado_en=now()")
        valores.append(evento_id)
        self.cursor.execute(
            f"UPDATE planer.eventos_calendario SET {', '.join(sets)} WHERE id=%s RETURNING *", tuple(valores)
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def eliminar_evento_calendario(self, evento_id):
        self.cursor.execute("DELETE FROM planer.eventos_calendario WHERE id=%s", (evento_id,))
        self.connection.commit()

    def obtener_datos_calendario(self, equipo_id, desde, hasta):
        self.cursor.execute(
            """SELECT re.id, re.fecha::text AS fecha, re.estado, re.actividad_id,
                      ar.titulo, ar.responsable_id,
                      u.nombres AS responsable_nombre, u.apellidos AS responsable_apellido
               FROM planer.recurrentes_ejecuciones re
               JOIN planer.actividades_recurrentes ar ON ar.id = re.actividad_id
               LEFT JOIN usuarios u ON u.id = ar.responsable_id
               WHERE ar.equipo_id = %s AND re.fecha BETWEEN %s AND %s
               ORDER BY re.fecha""",
            (equipo_id, desde, hasta),
        )
        recurrentes = self.cursor.fetchall()

        self.cursor.execute(
            """SELECT t.id, t.titulo, t.prioridad, t.fecha_limite::text AS fecha,
                      c.nombre AS columna_nombre,
                      u.nombres AS asignado_nombre, u.apellidos AS asignado_apellido
               FROM planer.tarjetas t
               JOIN planer.columnas c ON c.id = t.columna_id
               JOIN planer.tableros tb ON tb.id = c.tablero_id
               LEFT JOIN usuarios u ON u.id = t.asignado_a
               WHERE tb.equipo_id = %s AND t.archivado = FALSE AND t.fecha_limite BETWEEN %s AND %s
               ORDER BY t.fecha_limite""",
            (equipo_id, desde, hasta),
        )
        tarjetas = self.cursor.fetchall()

        self.cursor.execute(
            """SELECT e.*, e.fecha::text AS fecha_str, u.nombres, u.apellidos
               FROM planer.eventos_calendario e
               LEFT JOIN usuarios u ON u.id = e.creado_por
               WHERE e.equipo_id = %s AND e.fecha BETWEEN %s AND %s
               ORDER BY e.fecha""",
            (equipo_id, desde, hasta),
        )
        eventos = self.cursor.fetchall()
        return {"recurrentes": recurrentes, "tarjetas": tarjetas, "eventos": eventos}

    def obtener_equipo_de_nota(self, nota_id):
        self.cursor.execute("SELECT equipo_id FROM planer.notas WHERE id=%s", (nota_id,))
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None

    def registrar_archivo(self, equipo_id, nota_id, nombre_archivo, blob_path, tamano_bytes, content_type, usuario_id):
        self.cursor.execute(
            """INSERT INTO planer.archivos
               (equipo_id, nota_id, nombre_archivo, blob_path, tamano_bytes, content_type, subido_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (equipo_id, nota_id, nombre_archivo, blob_path, tamano_bytes, content_type, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def listar_archivos(self, equipo_id):
        self.cursor.execute(
            "SELECT * FROM planer.archivos WHERE equipo_id=%s ORDER BY subido_en DESC", (equipo_id,)
        )
        return self.cursor.fetchall()

    def obtener_archivo(self, archivo_id):
        self.cursor.execute("SELECT * FROM planer.archivos WHERE id=%s", (archivo_id,))
        return self.cursor.fetchone()

    def eliminar_archivo(self, archivo_id):
        self.cursor.execute("DELETE FROM planer.archivos WHERE id=%s", (archivo_id,))
        self.connection.commit()

    def registrar_archivo_tarjeta(self, equipo_id, tarjeta_id, nombre_archivo, blob_path, tamano_bytes, content_type, usuario_id):
        self.cursor.execute(
            """INSERT INTO planer.archivos
               (equipo_id, tarjeta_id, nombre_archivo, blob_path, tamano_bytes, content_type, subido_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (equipo_id, tarjeta_id, nombre_archivo, blob_path, tamano_bytes, content_type, usuario_id),
        )
        row = self.cursor.fetchone()
        self.connection.commit()
        return row

    def listar_archivos_tarjeta(self, tarjeta_id):
        self.cursor.execute(
            "SELECT * FROM planer.archivos WHERE tarjeta_id=%s ORDER BY subido_en DESC", (tarjeta_id,)
        )
        return self.cursor.fetchall()

    def obtener_titulo_tarjeta(self, tarjeta_id):
        self.cursor.execute("SELECT titulo FROM planer.tarjetas WHERE id=%s", (tarjeta_id,))
        row = self.cursor.fetchone()
        return row["titulo"] if row else f"tarjeta_{tarjeta_id}"

    def obtener_equipo_de_tarjeta(self, tarjeta_id):
        self.cursor.execute(
            """SELECT c.tablero_id, t.equipo_id FROM planer.tarjetas tr
               JOIN planer.columnas c ON c.id = tr.columna_id
               JOIN planer.tableros t ON t.id = c.tablero_id
               WHERE tr.id=%s""", (tarjeta_id,)
        )
        row = self.cursor.fetchone()
        return row["equipo_id"] if row else None
