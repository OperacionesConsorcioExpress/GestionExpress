import os, re
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql, errors
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Cargar variables de entorno
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
TIMEZONE_BOGOTA = ZoneInfo("America/Bogota")

# Función para obtener la fecha y hora actual en Bogotá
def now_bogota() -> datetime:
    # Datetime consciente de zona horaria Bogotá
    return datetime.now(TIMEZONE_BOGOTA)

class GestionCOP:
    def __init__(self):
        self.connection = psycopg2.connect(DATABASE_PATH, options='-c timezone=America/Bogota')
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        with self.connection.cursor() as c:
            c.execute("SET TIME ZONE 'America/Bogota';")
        self.connection.commit()

    def cerrar_conexion(self):
        if getattr(self, "cursor", None):
            self.cursor.close()
        if getattr(self, "connection", None):
            self.connection.close()

    # ---------- utilidades ----------
    def _build_search_filter(self, field_name, q):
        if not q: 
            return "", []
        return f"AND UPPER({field_name}) LIKE %s", [f"%{q.upper()}%"]

    def _paginate(self, page:int, size:int):
        off = (page-1)*size
        return " LIMIT %s OFFSET %s ", [size, off]

    def _row_or_error(self, cur):
        r = cur.fetchone()
        if not r: 
            raise ValueError("Registro no encontrado")
        return r

    # ---------- COMPONENTE ----------
    def list_componentes(self, q=None, estado=None, page=1, size=10):
        where, params = "WHERE 1=1 ", []
        s, sp = self._build_search_filter("componente", q)
        where += s; params += sp
        if estado is not None:
            where += " AND estado = %s "; params.append(int(estado))
        count_sql = f"SELECT COUNT(1) FROM config.componente {where}"
        self.cursor.execute(count_sql, params)
        total = self.cursor.fetchone()["count"]

        order = " ORDER BY id DESC "
        pag, pp = self._paginate(page, size)
        sqlq = f"""SELECT id, componente, estado,
                        to_char(created_at, 'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at, 'YYYY-MM-DD HH24:MI') as updated_at
                FROM config.componente {where} {order} {pag}"""
        self.cursor.execute(sqlq, params + pp)
        return self.cursor.fetchall(), total

    def create_componente(self, componente:str, estado:int=1):
        componente_u = (componente or "").upper().strip()
        if not componente_u:
            raise ValueError("El componente es obligatorio")
        try:
            sqlq = """INSERT INTO config.componente (componente, estado)
                    VALUES (%s, %s)
                    RETURNING id, componente, estado,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (componente_u, int(estado)))
            row = self.cursor.fetchone()
            self.connection.commit()
            return row
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("El componente ya existe")
        except Exception as e:
            self.connection.rollback()
            raise e

    def update_componente(self, id:int, componente:str, estado:int=1):
        componente_u = (componente or "").upper().strip()
        if not componente_u:
            raise ValueError("El componente es obligatorio")
        try:
            sqlq = """UPDATE config.componente
                        SET componente=%s, estado=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id, componente, estado,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (componente_u, int(estado), id))
            row = self._row_or_error(self.cursor)
            self.connection.commit()
            return row
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("El componente ya existe")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ---------- ZONA ----------
    def list_zonas(self, q=None, estado=None, page=1, size=10):
        where, params = "WHERE 1=1 ", []
        s, sp = self._build_search_filter("zona", q)
        where += s; params += sp
        if estado is not None:
            where += " AND estado = %s "; params.append(int(estado))
        self.cursor.execute(f"SELECT COUNT(1) FROM config.zona {where}", params)
        total = self.cursor.fetchone()["count"]

        order = " ORDER BY id DESC "
        pag, pp = self._paginate(page, size)
        sqlq = f"""SELECT id, zona, estado,
                        to_char(created_at, 'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at, 'YYYY-MM-DD HH24:MI') as updated_at
                FROM config.zona {where} {order} {pag}"""
        self.cursor.execute(sqlq, params + pp)
        return self.cursor.fetchall(), total

    def create_zona(self, zona:str, estado:int=1):
        zona_u = (zona or "").upper().strip()
        if not zona_u:
            raise ValueError("La zona es obligatoria")
        try:
            sqlq = """INSERT INTO config.zona (zona, estado)
                    VALUES (%s, %s)
                    RETURNING id, zona, estado,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (zona_u, int(estado)))
            row = self.cursor.fetchone()
            self.connection.commit()
            return row
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("La zona ya existe")
        except Exception as e:
            self.connection.rollback()
            raise e

    def update_zona(self, id:int, zona:str, estado:int=1):
        zona_u = (zona or "").upper().strip()
        if not zona_u:
            raise ValueError("La zona es obligatoria")
        try:
            sqlq = """UPDATE config.zona
                    SET zona=%s, estado=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id, zona, estado,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (zona_u, int(estado), id))
            row = self._row_or_error(self.cursor)
            self.connection.commit()
            return row
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("La zona ya existe")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ---------- COP ----------
    def list_cop(self, q=None, estado=None, id_componente=None, id_zona=None, page=1, size=10):
        where, params = "WHERE 1=1 ", []
        s, sp = self._build_search_filter("c.cop", q)
        where += s; params += sp
        if estado is not None:
            where += " AND c.estado = %s "; params.append(int(estado))
        if id_componente:
            where += " AND c.id_componente = %s "; params.append(int(id_componente))
        if id_zona:
            where += " AND c.id_zona = %s "; params.append(int(id_zona))

        count_sql = f"""SELECT COUNT(1)
                        FROM config.cop c
                        JOIN config.componente co ON co.id=c.id_componente
                        JOIN config.zona z ON z.id=c.id_zona
                        {where}"""
        self.cursor.execute(count_sql, params)
        total = self.cursor.fetchone()["count"]

        order = " ORDER BY c.id DESC "
        pag, pp = self._paginate(page, size)
        sqlq = f"""
            SELECT c.id, c.cop, c.estado, c.id_componente, c.id_zona,
                co.componente, z.zona,
                to_char(c.created_at,'YYYY-MM-DD HH24:MI') as created_at,
                to_char(c.updated_at,'YYYY-MM-DD HH24:MI') as updated_at
            FROM config.cop c
            JOIN config.componente co ON co.id=c.id_componente
            JOIN config.zona z ON z.id=c.id_zona
            {where} {order} {pag}
        """
        self.cursor.execute(sqlq, params + pp)
        return self.cursor.fetchall(), total

    def create_cop(self, cop:str, id_componente:int, id_zona:int, estado:int=1):
        cop_u = (cop or "").upper().strip()
        if not cop_u or not id_componente or not id_zona:
            raise ValueError("COP, Componente y Zona son obligatorios")
        try:
            sqlq = """INSERT INTO config.cop (cop, id_componente, id_zona, estado)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, cop, estado, id_componente, id_zona,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (cop_u, int(id_componente), int(id_zona), int(estado)))
            row = self.cursor.fetchone()
            self.connection.commit()
            return row
        except errors.ForeignKeyViolation:
            self.connection.rollback()
            raise ValueError("Componente o Zona no existe")
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un COP con el mismo nombre, componente y zona")
        except Exception as e:
            self.connection.rollback()
            raise e

    def update_cop(self, id:int, cop:str, id_componente:int, id_zona:int, estado:int=1):
        cop_u = (cop or "").upper().strip()
        if not cop_u or not id_componente or not id_zona:
            raise ValueError("COP, Componente y Zona son obligatorios")
        try:
            sqlq = """UPDATE config.cop
                    SET cop=%s, id_componente=%s, id_zona=%s, estado=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id, cop, estado, id_componente, id_zona,
                        to_char(created_at,'YYYY-MM-DD HH24:MI') as created_at,
                        to_char(updated_at,'YYYY-MM-DD HH24:MI') as updated_at"""
            self.cursor.execute(sqlq, (cop_u, int(id_componente), int(id_zona), int(estado), id))
            row = self._row_or_error(self.cursor)
            self.connection.commit()
            return row
        except errors.ForeignKeyViolation:
            self.connection.rollback()
            raise ValueError("Componente o Zona no existe")
        except errors.UniqueViolation:
            self.connection.rollback()
            raise ValueError("Ya existe un COP con el mismo nombre, componente y zona")
        except Exception as e:
            self.connection.rollback()
            raise e

    # ---------- Cambiar estado genérico ----------
    def toggle_estado(self, table:str, id:int, estado:int):
        if table not in ("componente","zona","cop"):
            raise ValueError("Tabla no permitida")
        q = sql.SQL("UPDATE config.{tbl} SET estado=%s, updated_at=now() WHERE id=%s RETURNING *").format(
            tbl=sql.Identifier(table)
        )
        self.cursor.execute(q, (int(estado), id))
        row = self._row_or_error(self.cursor)
        self.connection.commit()
        # normalizar respuesta mínima
        row["created_at"] = row.get("created_at").astimezone(TIMEZONE_BOGOTA).strftime("%Y-%m-%d %H:%M") if row.get("created_at") else None
        row["updated_at"] = row.get("updated_at").astimezone(TIMEZONE_BOGOTA).strftime("%Y-%m-%d %H:%M") if row.get("updated_at") else None
        return row
