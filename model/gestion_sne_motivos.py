from psycopg2.extras import RealDictCursor
from psycopg2 import errors, extensions as pg_extensions
from zoneinfo import ZoneInfo
from database.database_manager import _get_pool as get_db_pool

TZ_BOGOTA = ZoneInfo("America/Bogota")

class GestionSneMotivos:
    """
    CRUD sobre sne.motivos_eliminacion.
    Relaciona con sne.responsable_sne para obtener el nombre del responsable.
    """

    def __init__(self):
        self.connection = get_db_pool().getconn()
        if not self.connection.closed:
            self.connection.rollback()
        self.connection.cursor_factory = RealDictCursor
        self.cursor = self.connection.cursor()
        with self.connection.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.connection.commit()

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None) and not self.connection.closed:
            self.connection.rollback()
            self.connection.cursor_factory = pg_extensions.cursor
            get_db_pool().putconn(self.connection)

    # ── Helpers ──────────────────────────────────────────────────────────────
    def _fila_o_error(self, cur):
        fila = cur.fetchone()
        if not fila:
            raise ValueError("Registro no encontrado")
        return fila

    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _u(self, s):
        if s is None:
            return None
        return str(s).strip().upper()

    def _titulo(self, s):
        """Capitaliza correctamente (Title Case) preservando espacios."""
        if s is None:
            return None
        return str(s).strip().title()

    def _filtro_busqueda(self, q: str):
        if not q:
            return "", []
        qn = f"%{q.strip().upper()}%"
        return (
            " AND (UPPER(m.motivo) LIKE %s OR UPPER(r.responsable) LIKE %s) ",
            [qn, qn],
        )

    # ── Listar motivos ────────────────────────────────────────────────────────
    def listar_motivos(
        self,
        q: str = None,
        id_responsable: int = None,
        pagina: int = 1,
        tamano: int = 10,
    ):
        where, params = " WHERE 1=1 ", []

        s, sp = self._filtro_busqueda(q)
        where += s
        params += sp

        if id_responsable:
            where += " AND m.responsable = %s "
            params.append(int(id_responsable))

        sql_count = f"""
            SELECT COUNT(1)
            FROM sne.motivos_eliminacion m
            LEFT JOIN sne.responsable_sne r ON r.id = m.responsable
            {where}
        """
        self.cursor.execute(sql_count, params)
        total = self.cursor.fetchone()["count"]

        order = " ORDER BY m.id ASC "
        pag, pp = self._paginacion(pagina, tamano)
        sqlq = f"""
            SELECT
                m.id,
                m.motivo,
                m.responsable        AS id_responsable,
                r.responsable        AS nombre_responsable
            FROM sne.motivos_eliminacion m
            LEFT JOIN sne.responsable_sne r ON r.id = m.responsable
            {where} {order} {pag}
        """
        self.cursor.execute(sqlq, params + pp)
        return self.cursor.fetchall(), total

    # ── Crear ─────────────────────────────────────────────────────────────────
    def crear_motivo(self, motivo: str, id_responsable: int):
        obs = self._titulo(motivo)
        if not obs:
            raise ValueError("El motivo es obligatoria")
        if not id_responsable:
            raise ValueError("El motivo es obligatorio")

        # Verificar que el responsable existe
        self.cursor.execute(
            "SELECT id FROM sne.responsable_sne WHERE id = %s", (int(id_responsable),)
        )
        if not self.cursor.fetchone():
            raise ValueError("El responsable seleccionado no existe")

        try:
            self.cursor.execute(
                """
                INSERT INTO sne.motivos_eliminacion (motivo, responsable)
                VALUES (%s, %s)
                RETURNING id, motivo, responsable AS id_responsable
                """,
                (obs, int(id_responsable)),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            # Enriquecer con nombre del responsable
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo con esa motivo")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Actualizar ────────────────────────────────────────────────────────────
    def actualizar_motivo(self, id: int, motivo: str, id_responsable: int):
        obs = self._titulo(motivo)
        if not obs:
            raise ValueError("El motivo es obligatoria")
        if not id_responsable:
            raise ValueError("El motivo es obligatorio")

        self.cursor.execute(
            "SELECT id FROM sne.responsable_sne WHERE id = %s", (int(id_responsable),)
        )
        if not self.cursor.fetchone():
            raise ValueError("El responsable seleccionado no existe")

        try:
            self.cursor.execute(
                """
                UPDATE sne.motivos_eliminacion
                SET motivo = %s, responsable = %s
                WHERE id = %s
                RETURNING id, motivo, responsable AS id_responsable
                """,
                (obs, int(id_responsable), int(id)),
            )
            fila = self._fila_o_error(self.cursor)
            self.connection.commit()
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo con ese motivo")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Eliminar ──────────────────────────────────────────────────────────────
    def eliminar_motivo(self, id: int):
        """
        Elimina físicamente el motivo. Si está referenciado en otras tablas
        PostgreSQL lanzará ForeignKeyViolation y se captura limpiamente.
        """
        try:
            self.cursor.execute(
                "DELETE FROM sne.motivos_eliminacion WHERE id = %s RETURNING id",
                (int(id),),
            )
            fila = self._fila_o_error(self.cursor)
            self.connection.commit()
            return {"id": fila["id"], "eliminado": True}
        except errors.ForeignKeyViolation:
            self.connection.rollback()
            raise ValueError(
                "No se puede eliminar: el motivo está en uso en otros registros"
            )
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Sincronización desde sne.ics ─────────────────────────────────────────
    def sincronizar_desde_ics(self):
        """
        Compara los valores únicos y no nulos de sne.ics.motivo
        contra los que ya existen en sne.motivos_eliminacion.
        Los que no existan se insertan con responsable = NULL.

        Retorna:
            {
                "nuevos":    int,   ← cuántos se insertaron
                "existentes": int,  ← cuántos ya estaban
                "vacios":    int,   ← cuántos venían NULL/vacíos en ics (ignorados)
                "detalle":   list   ← los textos recién insertados
            }
        """
        # 1. Leer motivos únicos de sne.ics (no nulos, no vacíos)
        self.cursor.execute("""
            SELECT DISTINCT TRIM(motivo) AS obs
            FROM sne.ics
            WHERE motivo IS NOT NULL
                AND TRIM(motivo) <> ''
            ORDER BY obs
        """)
        desde_ics = {r["obs"] for r in self.cursor.fetchall()}

        # 2. Leer motivos que ya existen en motivos_eliminacion
        self.cursor.execute("""
            SELECT UPPER(TRIM(motivo)) AS obs
            FROM sne.motivos_eliminacion
            WHERE motivo IS NOT NULL
        """)
        ya_existen_upper = {r["obs"] for r in self.cursor.fetchall()}

        # 3. Filtrar los que realmente son nuevos (comparación case-insensitive)
        nuevos_textos = [
            obs for obs in desde_ics
            if obs.upper() not in ya_existen_upper
        ]

        # 4. Insertar los nuevos con responsable = NULL
        insertados = []
        for obs in nuevos_textos:
            try:
                self.cursor.execute(
                    """
                    INSERT INTO sne.motivos_eliminacion (motivo, responsable)
                    VALUES (%s, NULL)
                    ON CONFLICT DO NOTHING
                    RETURNING id, motivo
                    """,
                    (obs,),
                )
                fila = self.cursor.fetchone()
                if fila:
                    insertados.append(fila["motivo"])
            except Exception:
                # Si hay unique constraint viola, simplemente ignorar
                self.connection.rollback()
                continue

        self.connection.commit()

        return {
            "nuevos":     len(insertados),
            "existentes": len(desde_ics) - len(nuevos_textos),
            "detalle":    insertados,
        }

    # ── Responsables (datos de apoyo) ─────────────────────────────────────────
    def listar_responsables(self):
        self.cursor.execute(
            "SELECT id, responsable FROM sne.responsable_sne ORDER BY responsable ASC"
        )
        return self.cursor.fetchall()

    # ── Helper interno ────────────────────────────────────────────────────────
    def _enriquecer(self, fila: dict) -> dict:
        """Agrega nombre_responsable a una fila recién insertada/actualizada."""
        if not fila:
            return fila
        self.cursor.execute(
            "SELECT responsable FROM sne.responsable_sne WHERE id = %s",
            (fila["id_responsable"],),
        )
        resp = self.cursor.fetchone()
        return {
            **dict(fila),
            "nombre_responsable": resp["responsable"] if resp else None,
        }