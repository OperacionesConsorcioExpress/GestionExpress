"""
Modelo de Notificaciones para el Sistema SGI
Gestiona la creación y consulta de notificaciones de aprobaciones, rechazos y cierres
"""

from typing import List, Dict, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno
_BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=_BASE_DIR / ".env")
DATABASE_PATH = os.getenv("DATABASE_PATH")


class GestionNotificaciones:
    """Gestiona las notificaciones del sistema SGI"""

    def __init__(self):
        """Inicializar conexión a la base de datos"""
        try:
            self.connection = psycopg2.connect(
                dsn=DATABASE_PATH,
                options='-c timezone=America/Bogota'
            )
            self.connection.autocommit = False
        except psycopg2.OperationalError as e:
            print(f"Error al conectar a la base de datos en GestionNotificaciones: {e}")
            raise e

    def cerrar_conexion(self):
        """Cerrar la conexión a la base de datos"""
        if self.connection:
            self.connection.close()

    def crear_notificacion(self,
                          usuario: str,
                          tipo: str,
                          titulo: str,
                          mensaje: str,
                          codigo_referencia: str = None,
                          tipo_entidad: str = None,
                          actividad_id: int = None,
                          metadata: dict = None) -> Dict:
        """
        Crear una nueva notificación

        Args:
            usuario: Usuario destinatario
            tipo: Tipo de notificación (APROBACION, RECHAZO, CIERRE)
            titulo: Título de la notificación
            mensaje: Mensaje descriptivo
            codigo_referencia: Código del PA/GC/RVD relacionado
            tipo_entidad: Tipo de entidad (PA, GC, RVD)
            actividad_id: ID de la actividad relacionada
            metadata: Información adicional en formato dict

        Returns:
            Dict con success y mensaje
        """
        try:
            query = """
                INSERT INTO sgi.sgi_notificaciones (
                    usuario, tipo, titulo, mensaje, codigo_referencia,
                    tipo_entidad, actividad_id, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    usuario,
                    tipo.upper(),
                    titulo,
                    mensaje,
                    codigo_referencia,
                    tipo_entidad,
                    actividad_id,
                    psycopg2.extras.Json(metadata) if metadata else None
                ))

                notif_id = cursor.fetchone()[0]
                self.connection.commit()

                return {
                    'success': True,
                    'mensaje': 'Notificación creada exitosamente',
                    'notificacion_id': notif_id
                }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al crear notificación: {e}")
            return {
                'success': False,
                'mensaje': f'Error al crear notificación: {str(e)}'
            }

    def obtener_notificaciones_usuario(self,
                                       usuario: str,
                                       solo_no_leidas: bool = False,
                                       limite: int = 50) -> List[Dict]:
        """
        Obtener notificaciones de un usuario

        Args:
            usuario: Usuario para consultar notificaciones
            solo_no_leidas: Si True, solo devuelve notificaciones no leídas
            limite: Número máximo de notificaciones a devolver

        Returns:
            Lista de notificaciones
        """
        try:
            query = """
                SELECT
                    id,
                    usuario,
                    tipo,
                    titulo,
                    mensaje,
                    codigo_referencia,
                    tipo_entidad,
                    actividad_id,
                    estado,
                    fecha_creacion,
                    fecha_lectura,
                    metadata
                FROM sgi.sgi_notificaciones
                WHERE usuario = %s
            """

            params = [usuario]

            if solo_no_leidas:
                query += " AND estado = 'NO_LEIDA'"

            query += " ORDER BY fecha_creacion DESC LIMIT %s"
            params.append(limite)

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                notificaciones = [dict(row) for row in cursor.fetchall()]

                # Serializar fechas y metadata
                for notif in notificaciones:
                    if notif.get('fecha_creacion'):
                        notif['fecha_creacion'] = notif['fecha_creacion'].isoformat()
                    if notif.get('fecha_lectura'):
                        notif['fecha_lectura'] = notif['fecha_lectura'].isoformat()

                return notificaciones

        except Exception as e:
            print(f"Error al obtener notificaciones: {e}")
            return []

    def marcar_como_leida(self, notificacion_id: int) -> Dict:
        """
        Marcar una notificación como leída

        Args:
            notificacion_id: ID de la notificación

        Returns:
            Dict con success y mensaje
        """
        try:
            query = """
                UPDATE sgi.sgi_notificaciones
                SET estado = 'LEIDA', fecha_lectura = CURRENT_TIMESTAMP
                WHERE id = %s AND estado = 'NO_LEIDA'
            """

            with self.connection.cursor() as cursor:
                cursor.execute(query, (notificacion_id,))
                self.connection.commit()

                return {
                    'success': True,
                    'mensaje': 'Notificación marcada como leída'
                }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al marcar notificación como leída: {e}")
            return {
                'success': False,
                'mensaje': f'Error: {str(e)}'
            }

    def marcar_todas_como_leidas(self, usuario: str) -> Dict:
        """
        Marcar todas las notificaciones de un usuario como leídas

        Args:
            usuario: Usuario

        Returns:
            Dict con success y mensaje
        """
        try:
            query = """
                UPDATE sgi.sgi_notificaciones
                SET estado = 'LEIDA', fecha_lectura = CURRENT_TIMESTAMP
                WHERE usuario = %s AND estado = 'NO_LEIDA'
            """

            with self.connection.cursor() as cursor:
                cursor.execute(query, (usuario,))
                filas_actualizadas = cursor.rowcount
                self.connection.commit()

                return {
                    'success': True,
                    'mensaje': f'{filas_actualizadas} notificaciones marcadas como leídas'
                }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al marcar notificaciones como leídas: {e}")
            return {
                'success': False,
                'mensaje': f'Error: {str(e)}'
            }

    def contar_no_leidas(self, usuario: str) -> int:
        """
        Contar notificaciones no leídas de un usuario

        Args:
            usuario: Usuario

        Returns:
            Número de notificaciones no leídas
        """
        try:
            query = """
                SELECT COUNT(*) as total
                FROM sgi.sgi_notificaciones
                WHERE usuario = %s AND estado = 'NO_LEIDA'
            """

            with self.connection.cursor() as cursor:
                cursor.execute(query, (usuario,))
                result = cursor.fetchone()
                return result[0] if result else 0

        except Exception as e:
            print(f"Error al contar notificaciones no leídas: {e}")
            return 0

    def eliminar_notificacion(self, notificacion_id: int, usuario: str) -> Dict:
        """
        Eliminar una notificación (solo si pertenece al usuario)

        Args:
            notificacion_id: ID de la notificación
            usuario: Usuario propietario

        Returns:
            Dict con success y mensaje
        """
        try:
            query = """
                DELETE FROM sgi.sgi_notificaciones
                WHERE id = %s AND usuario = %s
            """

            with self.connection.cursor() as cursor:
                cursor.execute(query, (notificacion_id, usuario))
                filas_eliminadas = cursor.rowcount
                self.connection.commit()

                if filas_eliminadas > 0:
                    return {
                        'success': True,
                        'mensaje': 'Notificación eliminada'
                    }
                else:
                    return {
                        'success': False,
                        'mensaje': 'Notificación no encontrada o no autorizada'
                    }

        except Exception as e:
            self.connection.rollback()
            print(f"Error al eliminar notificación: {e}")
            return {
                'success': False,
                'mensaje': f'Error: {str(e)}'
            }
