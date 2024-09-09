import sqlite3
from sqlite3 import OperationalError
from threading import Lock
from datetime import datetime, time
import pytz
from fastapi import HTTPException

DATABASE_PATH = "./centro_control.db"
# Establecer la zona horaria de Colombia
colombia_tz = pytz.timezone('America/Bogota')

class HandleDB:
    _instance = None
    _lock = Lock()

    def __new__(cls): # Implementación del patrón Singleton para asegurar una única instancia de la clase HandleDB
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls)
                cls._instance._con = sqlite3.connect("./centro_control.db", check_same_thread=False)  # Habilita multi-hilo
                cls._instance._cur = cls._instance._con.cursor()
            return cls._instance

    def get_all(self): # Método para obtener todos los registros de usuarios
        with self._lock:
            self._cur.execute("SELECT * FROM usuarios")
            return self._cur.fetchall()

    def get_only(self, username): # Método para obtener un único registro de usuario filtrado por nombre de usuario
        with self._lock:
            self._cur.execute("SELECT * FROM usuarios WHERE username = ?", (username,))
            return self._cur.fetchone()

    def insert(self, data_user): # Método para insertar un nuevo usuario en la base de datos
        with self._lock:
            self._cur.execute("INSERT INTO usuarios VALUES (?, ?, ?, ?, ?, ?)", (
                data_user["id"],
                data_user["nombres"],
                data_user["apellidos"],
                data_user["username"],
                data_user["cargo"],
                data_user["password_user"]
            ))
            self._con.commit()

    def __del__(self): # Método para cerrar la conexión con la base de datos al finalizar el uso
        with self._lock:
            self._con.close()

class Cargue_Controles:
    def __init__(self):
        self.database_path = "./centro_control.db"
        self.conn = sqlite3.connect(self.database_path, timeout=10)
        self.cursor = self.conn.cursor()

    def borrar_tablas(self):
        self.cursor.execute('DELETE FROM planta')
        self.cursor.execute('DELETE FROM controles')
        self.cursor.execute('DELETE FROM supervisores')
        self.cursor.execute('DELETE FROM turnos')
        self.conn.commit()

    def cargar_datos(self, data):
        self.borrar_tablas()  # Borrar los datos existentes antes de cargar los nuevos
        print("Iniciando la carga de datos...")
        self._cargar_planta(data['planta'])
        self._cargar_supervisores(data['supervisores'])
        self._cargar_turnos(data['turnos'])
        self._cargar_controles(data['controles'])
        print("Datos cargados exitosamente.")
        self.conn.close()

    def _cargar_planta(self, planta_data):
        try:
            for row in planta_data:
                print(f"Insertando o actualizando en planta: {row}")
                self.cursor.execute('''
                    INSERT INTO planta (cedula, nombre) VALUES (?, ?)
                    ON CONFLICT(cedula) DO UPDATE SET nombre=excluded.nombre
                ''', (row['cedula'], row['nombre']))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error al cargar los datos de planta: {str(e)}")
            raise
        
    def _cargar_supervisores(self, supervisores_data):
        try:
            for row in supervisores_data:
                print(f"Insertando o actualizando en supervisores: {row}")
                self.cursor.execute('''
                    INSERT INTO supervisores (cedula, nombre) VALUES (?, ?)
                    ON CONFLICT(cedula) DO UPDATE SET nombre=excluded.nombre
                ''', (row['cedula'], row['nombre']))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error al cargar los datos de supervisores: {str(e)}")
            raise
        
    def _cargar_turnos(self, turnos_data):
        try:
            for row in turnos_data:
                print(f"Insertando o actualizando en turnos: {row}")
                self.cursor.execute('''
                    INSERT OR REPLACE INTO turnos (turno, hora_inicio, hora_fin, detalles)
                    VALUES (?, ?, ?, ?)
                    ''', (str(row['turno']), str(row['hora_inicio']), str(row['hora_fin']), str(row['detalles']))
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error al cargar los datos de turnos: {str(e)}")
            raise

    def _cargar_controles(self, controles_data):
        batch_size = 10  # Tamaño del lote
        retries = 10  # Número de reintentos
        delay = 1  # Retraso entre reintentos en segundos

        for i in range(0, len(controles_data), batch_size):
            batch = controles_data[i:i + batch_size]
            for _ in range(retries):
                try:
                    conn = sqlite3.connect(self.database_path, timeout=10)
                    cursor = conn.cursor()
                    cursor.execute('PRAGMA busy_timeout = 30000')  # Establecer tiempo de espera de 30 segundos

                    for row in batch:
                        print(f"Insertando o actualizando en controles: {row}")
                        cursor.execute('''
                            INSERT OR REPLACE INTO controles (concesion, puestos, control, ruta, linea, admin, cop, tablas) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (row['concesion'], row['puestos'], row['control'], row['ruta'], row['linea'], row['admin'], row['cop'], row['tablas']))
                    conn.commit()
                    conn.close()
                    break
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        print("La base de datos está bloqueada, reintentando...")
                        time.sleep(delay)  # Esperar antes de reintentar
                    else:
                        raise
                finally:
                    if conn:
                        conn.close()
            else:
                raise sqlite3.OperationalError("No se desbloquear la base de datos después de varios intentos")
            
class Cargue_Asignaciones:
    def __init__(self):
        self.database_path = DATABASE_PATH

    def procesar_asignaciones(self, assignments, user_session):
        processed_data = []
        try:
            conn = sqlite3.connect(self.database_path, timeout=10)
            cursor = conn.cursor()
            for asignacion in assignments:
                rutas = asignacion['rutas_asociadas'].split(',')
                for ruta in rutas:
                    if asignacion['turno'] in ["AUSENCIA", "DESCANSO", "VACACIONES", "OTRAS TAREAS"]:
                        concesion = control = linea = cop = "NO APLICA"
                        ruta = "NO APLICA"  # No se usa directamente en el dicccionario
                    else:
                        cursor.execute("SELECT linea, cop FROM controles WHERE ruta = ?", (ruta.strip(),))
                        control_data = cursor.fetchone()
                        linea, cop = control_data if control_data else ("", "")
                        concesion = asignacion['concesion']
                        control = asignacion['control']
                        
                    processed_data.append({
                        'fecha': asignacion['fecha'],
                        'cedula': asignacion['cedula'],
                        'nombre': asignacion['nombre'],
                        'turno': asignacion['turno'],
                        'h_inicio': asignacion['hora_inicio'],
                        'h_fin': asignacion['hora_fin'],
                        'concesion': concesion,
                        'control': control,
                        'ruta': ruta if asignacion['turno'] in ["AUSENCIA", "DESCANSO", "VACACIONES", "OTRAS TAREAS"] else ruta.strip(),
                        'linea': linea,
                        'cop': cop,
                        'observaciones': asignacion['observaciones'],
                        'usuario_registra': user_session['username'],  # Agregar username
                        'registrado_por': f"{user_session['nombres']} {user_session['apellidos']}",   # Agregar nombre y apellido
                        'fecha_hora_registro': datetime.now(colombia_tz).strftime("%d-%m-%Y %H:%M:%S"),
                        'puestosSC': int(asignacion.get('puestosSC', 0)),  # Configuración campo puestosSC
                        'puestosUQ': int(asignacion.get('puestosUQ', 0)),   # Configuración campo puestosUQ
                        'cedula_enlace': asignacion['cedula_enlace'],  # Añadir cedula_enlace
                        'nombre_supervisor_enlace': asignacion['nombre_supervisor_enlace']  # Añadir nombre_supervisor_enlace                        
                    })
            conn.close()
        except sqlite3.Error as e:
            print(f"Error al procesar asignaciones: {e}")
        return processed_data

    def cargar_asignaciones(self, processed_data):
        try:
            conn = sqlite3.connect(self.database_path, timeout=10)
            cursor = conn.cursor()

            # Fase de eliminación
            for data in processed_data:
                # Asegurarse de que todos los campos obligatorios tienen un valor
                if not all([data['fecha'], data['cedula'], data['nombre'], data['turno'], data['h_inicio'], data['h_fin']]):
                    print(f"Datos incompletos para la cédula: {data['cedula']}, data: {data}")
                    continue

                # Eliminar todos los registros existentes con la misma combinación de (fecha, cédula, turno, h_inicio, h_fin, control)
                cursor.execute('''
                    DELETE FROM asignaciones
                    WHERE fecha = ? AND cedula = ? AND turno = ? AND h_inicio = ? AND h_fin = ? AND control = ?
                ''', (data['fecha'], data['cedula'], data['turno'], data['h_inicio'], data['h_fin'], data['control']))
                print(f"Registros eliminados para cédula: {data['cedula']}, fecha: {data['fecha']}, turno: {data['turno']}, h_inicio: {data['h_inicio']}, h_fin: {data['h_fin']}, control: {data['control']}")

            # Fase de inserción
            for data in processed_data:
                # Desglosar las rutas en filas separadas e insertar los nuevos registros
                rutas = data['ruta'].split(',')
                for ruta in rutas:
                    ruta = ruta.strip()  # Asegurarse de que no hay espacios innecesarios

                    cursor.execute('''
                        INSERT INTO asignaciones (fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, ruta, linea, cop, observaciones, usuario_registra, registrado_por, fecha_hora_registro, puestosSC, puestosUQ, cedula_enlace, nombre_supervisor_enlace)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        data['fecha'], data['cedula'], data['nombre'], data['turno'], data['h_inicio'], data['h_fin'],
                        data['concesion'], data['control'], ruta, data['linea'], data['cop'], data['observaciones'],
                        data['usuario_registra'], data['registrado_por'], data['fecha_hora_registro'], data['puestosSC'], data['puestosUQ'],
                        data['cedula_enlace'], data['nombre_supervisor_enlace']
                    ))
                    print(f"Nuevo registro insertado para cédula: {data['cedula']}, fecha: {data['fecha']}, ruta: {ruta}")

            # Confirmar la transacción
            conn.commit()
            return {"status": "success", "message": "Asignaciones guardadas exitosamente."}

        except sqlite3.Error as e:
            # Revertir la transacción en caso de error
            conn.rollback()
            print(f"Error al guardar asignaciones: {e}")
            raise HTTPException(status_code=500, detail=f"Error al guardar asignaciones: {e}")

        finally:
            conn.close()  # Cerrar la conexión
