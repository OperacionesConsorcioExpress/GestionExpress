from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from zoneinfo import ZoneInfo
from database.database_manager import get_db_connection

TZ_BOGOTA = ZoneInfo("America/Bogota")


# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionSneMotivos
# CRUD sobre sne.motivos_eliminacion
# ═══════════════════════════════════════════════════════════════════════════════
class GestionSneMotivos:
    """
    CRUD sobre sne.motivos_eliminacion.
    Relaciona con sne.responsable_sne para obtener el nombre del responsable.
    La columna `estado` (boolean) permite activar/inactivar sin perder historial.
    """

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

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # ── Helpers ──────────────────────────────────────────────────────────────
    def _fila_o_error(self, cur):
        fila = cur.fetchone()
        if not fila:
            raise ValueError("Registro no encontrado")
        return fila

    def _paginacion(self, pagina: int, tamano: int):
        off = (pagina - 1) * tamano
        return " LIMIT %s OFFSET %s ", [tamano, off]

    def _titulo(self, s):
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
        estado: bool = None,
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

        if estado is not None:
            where += " AND m.estado = %s "
            params.append(bool(estado))

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
                r.responsable        AS nombre_responsable,
                m.estado
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
            raise ValueError("El motivo es obligatorio")
        if not id_responsable:
            raise ValueError("El responsable es obligatorio")

        self.cursor.execute(
            "SELECT id FROM sne.responsable_sne WHERE id = %s AND estado = TRUE",
            (int(id_responsable),)
        )
        if not self.cursor.fetchone():
            raise ValueError("El responsable seleccionado no existe o está inactivo")

        try:
            self.cursor.execute(
                """
                INSERT INTO sne.motivos_eliminacion (motivo, responsable, estado)
                VALUES (%s, %s, TRUE)
                RETURNING id, motivo, responsable AS id_responsable, estado
                """,
                (obs, int(id_responsable)),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo con ese texto")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Actualizar ────────────────────────────────────────────────────────────
    def actualizar_motivo(self, id: int, motivo: str, id_responsable: int):
        obs = self._titulo(motivo)
        if not obs:
            raise ValueError("El motivo es obligatorio")
        if not id_responsable:
            raise ValueError("El responsable es obligatorio")

        self.cursor.execute(
            "SELECT id FROM sne.responsable_sne WHERE id = %s AND estado = TRUE",
            (int(id_responsable),)
        )
        if not self.cursor.fetchone():
            raise ValueError("El responsable seleccionado no existe o está inactivo")

        try:
            self.cursor.execute(
                """
                UPDATE sne.motivos_eliminacion
                SET motivo = %s, responsable = %s
                WHERE id = %s
                RETURNING id, motivo, responsable AS id_responsable, estado
                """,
                (obs, int(id_responsable), int(id)),
            )
            fila = self._fila_o_error(self.cursor)

            self.cursor.execute(
                """
                UPDATE sne.ics_motivo_resp
                SET responsable = %s
                WHERE motivo = %s
                  AND responsable = 0
                """,
                (int(id_responsable), int(id)),
            )

            self.connection.commit()
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo con ese texto")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Cambiar estado (activo / inactivo) ────────────────────────────────────
    def cambiar_estado_motivo(self, id: int, estado: bool):
        """
        Activa o inactiva un motivo sin eliminar el registro.
        Los historiales que lo referencian quedan intactos.
        """
        try:
            self.cursor.execute(
                """
                UPDATE sne.motivos_eliminacion
                SET estado = %s
                WHERE id = %s
                RETURNING id, estado
                """,
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Sincronización desde sne.ics ─────────────────────────────────────────
    def sincronizar_desde_ics(self):
        self.cursor.execute("""
            SELECT DISTINCT TRIM(motivo) AS obs
            FROM sne.ics
            WHERE motivo IS NOT NULL
                AND TRIM(motivo) <> ''
            ORDER BY obs
        """)
        desde_ics = {r["obs"] for r in self.cursor.fetchall()}

        self.cursor.execute("""
            SELECT UPPER(TRIM(motivo)) AS obs
            FROM sne.motivos_eliminacion
            WHERE motivo IS NOT NULL
        """)
        ya_existen_upper = {r["obs"] for r in self.cursor.fetchall()}

        nuevos_textos = [
            obs for obs in desde_ics
            if obs.upper() not in ya_existen_upper
        ]

        insertados = []
        for obs in nuevos_textos:
            try:
                self.cursor.execute(
                    """
                    INSERT INTO sne.motivos_eliminacion (motivo, responsable, estado)
                    VALUES (%s, NULL, TRUE)
                    ON CONFLICT DO NOTHING
                    RETURNING id, motivo
                    """,
                    (obs,),
                )
                fila = self.cursor.fetchone()
                if fila:
                    insertados.append(fila["motivo"])
            except Exception:
                self.connection.rollback()
                continue

        self.connection.commit()
        return {
            "nuevos":     len(insertados),
            "existentes": len(desde_ics) - len(nuevos_textos),
            "detalle":    insertados,
        }

    # ── Responsables activos (datos de apoyo para dropdowns) ─────────────────
    def listar_responsables(self):
        self.cursor.execute(
            "SELECT id, responsable FROM sne.responsable_sne WHERE estado = TRUE ORDER BY responsable ASC"
        )
        return self.cursor.fetchall()

    # ── Helper interno ────────────────────────────────────────────────────────
    def _enriquecer(self, fila: dict) -> dict:
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

# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionMotivosNotas
# CRUD sobre sne.motivos_notas (id, motivo_nota, estado)
# ═══════════════════════════════════════════════════════════════════════════════
class GestionMotivosNotas:
    """CRUD sobre sne.motivos_notas. La columna `estado` permite inactivar sin borrar."""

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

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # ── Listar ────────────────────────────────────────────────────────────────
    def listar(self, q: str = None, estado: bool = None, pagina: int = 1, tamano: int = 10):
        where, params = " WHERE 1=1 ", []
        if q:
            where += " AND UPPER(motivo_nota) LIKE %s "
            params.append(f"%{q.strip().upper()}%")
        if estado is not None:
            where += " AND estado = %s "
            params.append(bool(estado))

        self.cursor.execute(
            f"SELECT COUNT(1) FROM sne.motivos_notas {where}", params
        )
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"""
            SELECT id, motivo_nota, estado
            FROM sne.motivos_notas
            {where}
            ORDER BY id ASC
            LIMIT %s OFFSET %s
            """,
            params + [tamano, off],
        )
        return self.cursor.fetchall(), total

    # ── Crear ─────────────────────────────────────────────────────────────────
    def crear(self, motivo_nota: str):
        texto = str(motivo_nota).strip().title()
        if not texto:
            raise ValueError("El motivo nota es obligatorio")
        try:
            self.cursor.execute(
                """
                INSERT INTO sne.motivos_notas (motivo_nota, estado)
                VALUES (%s, TRUE)
                RETURNING id, motivo_nota, estado
                """,
                (texto,),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return dict(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo nota con ese texto")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Actualizar ────────────────────────────────────────────────────────────
    def actualizar(self, id: int, motivo_nota: str):
        texto = str(motivo_nota).strip().title()
        if not texto:
            raise ValueError("El motivo nota es obligatorio")
        try:
            self.cursor.execute(
                """
                UPDATE sne.motivos_notas
                SET motivo_nota = %s
                WHERE id = %s
                RETURNING id, motivo_nota, estado
                """,
                (texto, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return dict(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un motivo nota con ese texto")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Cambiar estado ────────────────────────────────────────────────────────
    def cambiar_estado(self, id: int, estado: bool):
        try:
            self.cursor.execute(
                "UPDATE sne.motivos_notas SET estado = %s WHERE id = %s RETURNING id, estado",
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e

# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionResponsableSne
# CRUD sobre sne.responsable_sne (id, responsable, estado)
# ═══════════════════════════════════════════════════════════════════════════════
class GestionResponsableSne:
    """CRUD sobre sne.responsable_sne. La columna `estado` permite inactivar sin borrar."""

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

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # ── Listar ────────────────────────────────────────────────────────────────
    def listar(self, q: str = None, estado: bool = None, pagina: int = 1, tamano: int = 10):
        where, params = " WHERE 1=1 ", []
        if q:
            where += " AND UPPER(responsable) LIKE %s "
            params.append(f"%{q.strip().upper()}%")
        if estado is not None:
            where += " AND estado = %s "
            params.append(bool(estado))

        self.cursor.execute(
            f"SELECT COUNT(1) FROM sne.responsable_sne {where}", params
        )
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"""
            SELECT id, responsable, estado
            FROM sne.responsable_sne
            {where}
            ORDER BY responsable ASC
            LIMIT %s OFFSET %s
            """,
            params + [tamano, off],
        )
        return self.cursor.fetchall(), total

    # ── Crear ─────────────────────────────────────────────────────────────────
    def crear(self, responsable: str):
        texto = str(responsable).strip().title()
        if not texto:
            raise ValueError("El nombre del responsable es obligatorio")
        try:
            self.cursor.execute(
                """
                INSERT INTO sne.responsable_sne (responsable, estado)
                VALUES (%s, TRUE)
                RETURNING id, responsable, estado
                """,
                (texto,),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return dict(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un responsable con ese nombre")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Actualizar ────────────────────────────────────────────────────────────
    def actualizar(self, id: int, responsable: str):
        texto = str(responsable).strip().title()
        if not texto:
            raise ValueError("El nombre del responsable es obligatorio")
        try:
            self.cursor.execute(
                """
                UPDATE sne.responsable_sne
                SET responsable = %s
                WHERE id = %s
                RETURNING id, responsable, estado
                """,
                (texto, int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return dict(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un responsable con ese nombre")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Cambiar estado ────────────────────────────────────────────────────────
    def cambiar_estado(self, id: int, estado: bool):
        """
        Inactiva o activa el responsable.
        Al inactivar, los motivos que lo referencian conservan su historial intacto.
        """
        try:
            self.cursor.execute(
                "UPDATE sne.responsable_sne SET estado = %s WHERE id = %s RETURNING id, estado",
                (bool(estado), int(id)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e

# ═══════════════════════════════════════════════════════════════════════════════
# CLASE: GestionJustificacion
# CRUD sobre sne.justificacion, con referencia a sne.acciones
# ═══════════════════════════════════════════════════════════════════════════════
class GestionJustificacion:
    """
    CRUD sobre sne.justificacion (id_justificacion, id_acc, justificacion, estado).
    Relaciona con sne.acciones (id_acc, accion).
    """

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

    def __del__(self):
        try:
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.__exit__(None, None, None)
                self._ctx = None
        except Exception:
            pass

    # ── Listar ────────────────────────────────────────────────────────────────
    def listar(
        self,
        q: str = None,
        id_acc: int = None,
        estado: bool = None,
        pagina: int = 1,
        tamano: int = 10,
    ):
        where, params = " WHERE 1=1 ", []
        if q:
            qn = f"%{q.strip().upper()}%"
            where += " AND (UPPER(j.justificacion) LIKE %s OR UPPER(a.accion) LIKE %s) "
            params += [qn, qn]
        if id_acc:
            where += " AND j.id_acc = %s "
            params.append(int(id_acc))
        if estado is not None:
            where += " AND j.estado = %s "
            params.append(bool(estado))

        sql_count = f"""
            SELECT COUNT(1)
            FROM sne.justificacion j
            LEFT JOIN sne.acciones a ON a.id_acc = j.id_acc
            {where}
        """
        self.cursor.execute(sql_count, params)
        total = self.cursor.fetchone()["count"]

        off = (pagina - 1) * tamano
        self.cursor.execute(
            f"""
            SELECT
                j.id_justificacion,
                j.id_acc,
                j.justificacion,
                j.estado,
                a.accion AS nombre_accion
            FROM sne.justificacion j
            LEFT JOIN sne.acciones a ON a.id_acc = j.id_acc
            {where}
            ORDER BY j.id_justificacion ASC
            LIMIT %s OFFSET %s
            """,
            params + [tamano, off],
        )
        return self.cursor.fetchall(), total

    # ── Crear ─────────────────────────────────────────────────────────────────
    def crear(self, justificacion: str, id_acc: int):
        texto = str(justificacion).strip().title()
        if not texto:
            raise ValueError("La justificación es obligatoria")
        if not id_acc:
            raise ValueError("La acción es obligatoria")

        self.cursor.execute(
            "SELECT id_acc FROM sne.acciones WHERE id_acc = %s", (int(id_acc),)
        )
        if not self.cursor.fetchone():
            raise ValueError("La acción seleccionada no existe")

        try:
            self.cursor.execute(
                """
                INSERT INTO sne.justificacion (justificacion, id_acc, estado)
                VALUES (%s, %s, TRUE)
                RETURNING id_justificacion, id_acc, justificacion, estado
                """,
                (texto, int(id_acc)),
            )
            fila = self.cursor.fetchone()
            self.connection.commit()
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe esa justificación")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Actualizar ────────────────────────────────────────────────────────────
    def actualizar(self, id_justificacion: int, justificacion: str, id_acc: int):
        texto = str(justificacion).strip().title()
        if not texto:
            raise ValueError("La justificación es obligatoria")
        if not id_acc:
            raise ValueError("La acción es obligatoria")

        self.cursor.execute(
            "SELECT id_acc FROM sne.acciones WHERE id_acc = %s", (int(id_acc),)
        )
        if not self.cursor.fetchone():
            raise ValueError("La acción seleccionada no existe")

        try:
            self.cursor.execute(
                """
                UPDATE sne.justificacion
                SET justificacion = %s, id_acc = %s
                WHERE id_justificacion = %s
                RETURNING id_justificacion, id_acc, justificacion, estado
                """,
                (texto, int(id_acc), int(id_justificacion)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return self._enriquecer(fila)
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe esa justificación")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Cambiar estado ────────────────────────────────────────────────────────
    def cambiar_estado(self, id_justificacion: int, estado: bool):
        """
        Activa o inactiva la justificación preservando los registros históricos.
        """
        try:
            self.cursor.execute(
                """
                UPDATE sne.justificacion
                SET estado = %s
                WHERE id_justificacion = %s
                RETURNING id_justificacion, estado
                """,
                (bool(estado), int(id_justificacion)),
            )
            fila = self.cursor.fetchone()
            if not fila:
                raise ValueError("Registro no encontrado")
            self.connection.commit()
            return {"id": fila["id_justificacion"], "estado": fila["estado"]}
        except Exception as e:
            self.connection.rollback()
            raise e

    # ── Acciones (datos de apoyo) ─────────────────────────────────────────────
    def listar_acciones(self):
        self.cursor.execute(
            "SELECT id_acc, accion FROM sne.acciones ORDER BY accion ASC"
        )
        return self.cursor.fetchall()

    # ── Helper interno ────────────────────────────────────────────────────────
    def _enriquecer(self, fila: dict) -> dict:
        if not fila:
            return fila
        self.cursor.execute(
            "SELECT accion FROM sne.acciones WHERE id_acc = %s",
            (fila["id_acc"],),
        )
        acc = self.cursor.fetchone()
        return {
            **dict(fila),
            "nombre_accion": acc["accion"] if acc else None,
        }
