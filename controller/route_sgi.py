from fastapi import APIRouter, Request, HTTPException, UploadFile, Depends, Body, Query, File, Form, BackgroundTasks
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates
from model.gestion_checklist import GestionChecklist
from pydantic import BaseModel, validator
from datetime import datetime, time
from typing import List, Optional, Dict, Any
import io, json, os, re, tempfile, zipfile
import pandas as pd
from model.gestion_sgi import GestionSGI
from model.gestionar_db import HandleDB

# Importar sistema de auditoría
try:
    from model.auditoria_sgi import AuditoriaSGI
    AUDITORIA_AVAILABLE = True
    # Print ya mostrado en gestion_sgi.py al cargar el módulo
except ImportError as e:
    print(f"[WARNING] Sistema de Auditoria no disponible: {e}")
    AUDITORIA_AVAILABLE = False

# Importar sistema de notificaciones
try:
    from model.gestion_notificaciones import GestionNotificaciones
    NOTIFICACIONES_AVAILABLE = True
    print("[INFO] Sistema de Notificaciones disponible ✅")
except ImportError as e:
    print(f"[WARNING] Sistema de Notificaciones no disponible: {e}")
    NOTIFICACIONES_AVAILABLE = False

# Crear router
router_sgi = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Función localmente para validar usuario
def get_user_session(req: Request):
    return req.session.get('user')

def _roles_cierre_sgi_configurados() -> set:
    raw = os.getenv("SGI_CIERRE_ROLES", "")
    roles = set()
    for item in raw.split(","):
        item = item.strip()
        if item.isdigit():
            roles.add(int(item))
    return roles

def usuario_puede_cerrar_sgi(user_session: dict) -> bool:
    if not user_session:
        return False
    role_id = user_session.get("rol")
    if not role_id:
        return False

    roles_config = _roles_cierre_sgi_configurados()
    if roles_config:
        return role_id in roles_config

    try:
        pantallas = HandleDB().get_pantallas_by_role(role_id) or []
        return any("sgi" in (pantalla or "").lower() for pantalla in pantallas)
    except Exception:
        return False

# --- RUTA PRINCIPAL SISTEMA INTEGRAL DE CALIDAD ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi", response_class=HTMLResponse)
def sgi_main(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "sgi.html",
        {
            "request": req,
            "user_session": user_session,
            "componentes": {}
        }
    )

# --- ENDPOINT PARA REFRESH VISTA MATERIALIZADA ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/refresh-cache")
def refresh_cache_sgi(user_session: dict = Depends(get_user_session)):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        resultado = gestion_sgi.refresh_vista_materializada_sgi()
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error actualizando cache: {str(e)}"}
        )

# --- ENDPOINT PARA BÚSQUEDA INTELIGENTE ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/busqueda-inteligente")
def busqueda_inteligente_sgi(
    q: str = Query(..., description="Texto a buscar"),
    limite: int = Query(100, description="Límite de resultados"),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        resultados = gestion_sgi.busqueda_sgi_inteligente(q, limite)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "resultados": resultados,
            "total": len(resultados),
            "consulta": q
        })

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error en búsqueda: {str(e)}"}
        )

# --- ENDPOINTS ENTERPRISE AVANZADOS ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/enterprise-refresh")
def enterprise_refresh_cache(user_session: dict = Depends(get_user_session)):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        from lib.sgi_enterprise_integration import SGIDataFacade
        facade = SGIDataFacade()
        result = facade.refresh_cache()

        return JSONResponse(content={
            "success": True,
            "resultado": result,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error en refresh enterprise: {str(e)}"}
        )

@router_sgi.get("/sgi/procesos-enterprise")
def obtener_procesos_enterprise(
    req: Request,
    contexto: str = Query('dashboard', description="Contexto: critical, dashboard, report"),
    usuario: Optional[str] = Query(None),
    tipo_gestion: Optional[str] = Query(None),
    proceso: Optional[str] = Query(None),
    subproceso: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    """🏢 ENDPOINT ENTERPRISE con contexto inteligente"""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        from lib.sgi_enterprise_integration import SGIControllerEnterprise

        # Preparar filtros
        filtros = {}
        if usuario:
            filtros['usuario'] = usuario
        if tipo_gestion:
            filtros['tipo_gestion'] = tipo_gestion
        if proceso:
            filtros['proceso'] = proceso
        if subproceso:
            filtros['subproceso'] = subproceso
        if estado:
            filtros['estado'] = estado
        if fecha_desde:
            filtros['fecha_desde'] = fecha_desde
        if fecha_hasta:
            filtros['fecha_hasta'] = fecha_hasta

        # Usar controlador enterprise
        enterprise_controller = SGIControllerEnterprise()
        procesos = enterprise_controller.obtener_procesos_sgi_enterprise(
            filtros, contexto
        )

        # Obtener métricas para respuesta
        facade = enterprise_controller.data_facade
        metrics = facade.get_performance_metrics()

        return JSONResponse(content={
            "success": True,
            "procesos": procesos,
            "contexto_usado": contexto,
            "total_registros": len(procesos),
            "metrics_summary": metrics.get('summary', {}),
            "timestamp": datetime.now().isoformat()
        })

    except ImportError:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema Enterprise no disponible"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error en consulta Enterprise: {str(e)}"}
        )

# --- OBTENER PROCESOS SGI ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/procesos")
def obtener_procesos_sgi(
    req: Request,
    usuario: Optional[str] = Query(None),
    tipo_gestion: Optional[str] = Query(None),
    proceso: Optional[str] = Query(None),
    subproceso: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Preparar filtros
        filtros = {}
        if usuario:
            filtros['usuario'] = usuario
        if tipo_gestion:
            filtros['tipo_gestion'] = tipo_gestion
        if proceso:
            filtros['proceso'] = proceso
        if subproceso:
            filtros['subproceso'] = subproceso
        if estado:
            filtros['estado'] = estado
        if fecha_desde:
            filtros['fecha_desde'] = fecha_desde
        if fecha_hasta:
            filtros['fecha_hasta'] = fecha_hasta

        # 🚀 USAR VERSIÓN ENTERPRISE ULTRA-OPTIMIZADA (<50ms)
        try:
            from lib.sgi_enterprise_integration import SGIControllerEnterprise
            enterprise_controller = SGIControllerEnterprise()
            procesos = enterprise_controller.obtener_procesos_sgi_enterprise(
                filtros, contexto='dashboard'
            )
            print("🏢 Usando consulta SGI Enterprise")
        except ImportError:
            # Fallback a versión ultra si Enterprise no disponible
            try:
                procesos = gestion_sgi.obtener_procesos_sgi_ultra(filtros)
                print("⚡ Usando consulta SGI ultra-optimizada")
            except Exception as e:
                print(f"🔄 Fallback a consulta estándar: {e}")
                procesos = gestion_sgi.obtener_procesos_sgi(filtros)
        except Exception as e:
            print(f"⚠️ Enterprise fallback: {e}")
            try:
                procesos = gestion_sgi.obtener_procesos_sgi_ultra(filtros)
                print("⚡ Usando consulta SGI ultra-optimizada")
            except Exception as e2:
                print(f"🔄 Fallback final a consulta estándar: {e2}")
                procesos = gestion_sgi.obtener_procesos_sgi(filtros)

        estadisticas = gestion_sgi.obtener_estadisticas_sgi()

        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "procesos": procesos,
            "estadisticas": estadisticas
        })

    except Exception as e:
        print(f"Error al obtener procesos SGI: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER DATOS POR TIPO DE GESTIÓN ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/filtrar_por_tipo")
async def filtrar_por_tipo_gestion(req: Request, user_session: dict = Depends(get_user_session)):
    """Endpoint para filtrar datos según el tipo de gestión seleccionado."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        # Obtener los datos del filtro desde el request
        filtros = await req.json()
        tipo_gestion = filtros.get('tipo_gestion', '').strip()

        gestion_sgi = GestionSGI()

        # Obtener datos según el tipo de gestión (incluyendo el caso de todos los datos)
        datos = gestion_sgi.obtener_datos_por_tipo_gestion(tipo_gestion, filtros)

        gestion_sgi.cerrar_conexion()

        # Determinar el mensaje y tipo para la respuesta
        if not tipo_gestion:
            mensaje_tipo = "todos los tipos"
            tipo_respuesta = "TODOS"
        else:
            mensaje_tipo = tipo_gestion
            tipo_respuesta = tipo_gestion

        # Registrar en auditoría SOLO si hay filtros aplicados
        filtros_aplicados = []

        if tipo_gestion:
            filtros_aplicados.append(f"Tipo: {tipo_gestion}")

        if filtros.get('codigo'):
            filtros_aplicados.append(f"Código: {filtros['codigo']}")

        if filtros.get('proceso'):
            filtros_aplicados.append(f"Proceso: {filtros['proceso']}")

        if filtros.get('subproceso'):
            filtros_aplicados.append(f"Subproceso: {filtros['subproceso']}")

        if filtros.get('estado'):
            filtros_aplicados.append(f"Estado: {filtros['estado']}")

        if filtros.get('fecha_desde'):
            filtros_aplicados.append(f"Fecha desde: {filtros['fecha_desde']}")

        if filtros.get('fecha_hasta'):
            filtros_aplicados.append(f"Fecha hasta: {filtros['fecha_hasta']}")

        if filtros.get('filtro_vencimiento'):
            vencimiento_nombres = {
                'vencido': 'Vencidos',
                'proximo': 'Próximos a vencer',
                'ok': 'Al día'
            }
            vencimiento_texto = vencimiento_nombres.get(filtros['filtro_vencimiento'], filtros['filtro_vencimiento'])
            filtros_aplicados.append(f"Vencimiento: {vencimiento_texto}")

        # Solo registrar si hay filtros aplicados
        if filtros_aplicados and AUDITORIA_AVAILABLE:
            try:
                auditoria = AuditoriaSGI()
                detalle_filtros = " | ".join(filtros_aplicados)
                cantidad_registros = len(datos)
                auditoria.log_accion_rapida(
                    usuario=user_session.get('username', 'unknown'),
                    accion='CONSULTAR',
                    modulo='SGI',
                    detalle=f"Filtros en Pendientes: {detalle_filtros} ({cantidad_registros} registros encontrados)",
                    resultado='exito'
                )
                auditoria.cerrar_conexion()
            except Exception as log_error:
                print(f"⚠️ Error al registrar filtros de pendientes: {log_error}")

        return JSONResponse(content={
            "success": True,
            "datos": datos,
            "tipo_gestion": tipo_respuesta,
            "mensaje_tipo": mensaje_tipo,
            "total": len(datos)
        })

    except Exception as e:
        print(f"Error al filtrar por tipo de gestión: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- CREAR PROCESO SGI ---
# ----------------------------------------------------------------------
class ActividadPHVA(BaseModel):
    nombre_actividad: str
    phva: str  # Acepta: P, H, V, A
    descripcion: str
    fecha_ejecucion: str
    responsable: str
    tipo: Optional[str] = None  # 'phva' o 'correctiva' (campo adicional opcional)

    @validator('phva')
    def validar_phva(cls, v):
        valor = (v or '').strip().upper()
        if valor not in {'P', 'H', 'V', 'A'}:
            raise ValueError('PHVA invalido (use P, H, V o A)')
        return valor

class ProcesoSGICreate(BaseModel):
    tipo: str
    proceso: str
    subproceso: str
    cop: Optional[str] = None
    tipo_accion: Optional[str] = None
    estado: Optional[str] = None
    fecha_apertura: Optional[str] = None
    fecha_cierre: str
    fuente: Optional[str] = None
    hallazgo: Optional[str] = None
    causa_raiz: Optional[str] = None
    actividades_phva: List[ActividadPHVA] = []

class PlanAccionCreate(BaseModel):
    proceso: str
    subproceso: str
    cop: str
    tipo_accion: str
    estado: str = 'Pendiente'
    fecha_apertura: Optional[str] = None
    fecha_cierre: str
    fuente: Optional[str] = None
    hallazgo: str
    causa_raiz: str
    responsable: str
    observaciones: Optional[str] = None
    actividades_phva: Optional[List[ActividadPHVA]] = []

    @validator('responsable')
    def validar_responsable(cls, v):
        if not v or len(v.strip()) < 1:
            raise ValueError('El responsable es obligatorio')
        if len(v.strip()) > 120:
            raise ValueError('El responsable no puede exceder 120 caracteres')
        return v.strip()

@router_sgi.post("/sgi/procesos")
def crear_proceso_sgi(
    req: Request,
    datos: ProcesoSGICreate,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Agregar información del usuario
        datos_proceso = datos.dict()
        datos_proceso['usuario_creacion'] = user_session.get('usuario', 'unknown')

        resultado = gestion_sgi.crear_proceso_sgi(datos_proceso)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al crear proceso SGI: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- CREAR PLAN DE ACCIÓN ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/planes-accion")
def crear_plan_accion(
    req: Request,
    datos: PlanAccionCreate,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Agregar información del usuario
        datos_plan = datos.dict()
        datos_plan['usuario_creacion'] = user_session.get('usuario', 'system')

        # Validar campos requeridos
        if not datos_plan.get('proceso') or not datos_plan.get('subproceso'):
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Proceso y subproceso son requeridos"}
            )

        if not datos_plan.get('responsable'):
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "El responsable es requerido"}
            )

        if not datos_plan.get('hallazgo') or not datos_plan.get('causa_raiz'):
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Hallazgo y causa raíz son requeridos"}
            )

        resultado = gestion_sgi.crear_plan_accion(datos_plan)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al crear plan de acción: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER CÓDIGOS DE PLANES DE ACCIÓN ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/planes-accion/codigos")
def obtener_codigos_planes_accion(
    req: Request,
    tipo_gestion: Optional[str] = Query(None),
    proceso: Optional[str] = Query(None),
    subproceso: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        filtros = {}
        if tipo_gestion:
            filtros['tipo_gestion'] = tipo_gestion
        if proceso:
            filtros['proceso'] = proceso
        if subproceso:
            filtros['subproceso'] = subproceso

        codigos = gestion_sgi.obtener_codigos_planes_accion(filtros)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={"success": True, "datos": codigos})

    except Exception as e:
        print(f"Error al obtener códigos de planes de acción: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER CÓDIGOS DE PLANES DE ACCIÓN ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/planes-accion")
def obtener_planes_accion(
    req: Request,
    codigo: Optional[str] = Query(None),
    proceso: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        filtros = {}
        if codigo:
            filtros['codigo'] = codigo
        if proceso:
            filtros['proceso'] = proceso
        if estado:
            filtros['estado'] = estado

        planes = gestion_sgi.obtener_planes_accion(filtros)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={"success": True, "datos": planes})

    except Exception as e:
        print(f"Error al obtener planes de acción: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- GENERAR CÓDIGO AUTOMÁTICO PA ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/planes-accion/generar-codigo")
def generar_codigo_pa(
    proceso: str = Query(...),
    subproceso: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        codigo = gestion_sgi.generar_codigo_pa(proceso, subproceso)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={"success": True, "codigo": codigo})

    except Exception as e:
        print(f"Error al generar código PA: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# ==================== GESTIÓN DEL CAMBIO (GC) ====================

# --- CREAR GESTIÓN DEL CAMBIO ---
# ----------------------------------------------------------------------
class ActividadGC(BaseModel):
    """Modelo para actividades de Gestión del Cambio"""
    etapa: str
    nombre_actividad: str
    descripcion: str
    fecha_ejecucion: str
    responsable: str

    @validator('etapa')
    def validar_etapa(cls, v):
        etapas_validas = ['Antes', 'Durante', 'Después']
        if not v or v.strip() not in etapas_validas:
            raise ValueError(f'La etapa debe ser una de: {", ".join(etapas_validas)}')
        return v.strip()

    @validator('nombre_actividad')
    def validar_nombre(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError('El nombre de la actividad debe tener al menos 2 caracteres')
        if len(v.strip()) > 150:
            raise ValueError('El nombre de la actividad no puede exceder 150 caracteres')
        return v.strip()

    @validator('responsable')
    def validar_responsable(cls, v):
        if not v or len(v.strip()) < 1:
            raise ValueError('El responsable es obligatorio')
        if len(v.strip()) > 120:
            raise ValueError('El responsable no puede exceder 120 caracteres')
        return v.strip()

    @validator('descripcion')
    def validar_descripcion(cls, v):
        if not v or len(v.strip()) < 1:
            raise ValueError('La descripción es obligatoria')
        return v.strip()


class GestionCambioCreate(BaseModel):
    proceso: str
    subproceso: str
    cop: str
    estado: str
    fecha_apertura: str
    fecha_cierre: str
    responsable: str
    nombre_cambio: str
    descripcion_cambio: str
    actividades: List[ActividadGC]  # Array de actividades (cada una con su etapa)

    @validator('actividades')
    def validar_actividades(cls, v):
        if not v or len(v) == 0:
            raise ValueError('Debe agregar al menos una actividad')
        return v

    @validator('nombre_cambio')
    def validar_nombre_cambio(cls, v):
        if not v or len(v.strip()) < 3:
            raise ValueError('El nombre del cambio debe tener al menos 3 caracteres')
        if len(v.strip()) > 150:
            raise ValueError('El nombre del cambio no puede exceder 150 caracteres')
        return v.strip()

    @validator('responsable')
    def validar_responsable_gc(cls, v):
        if not v or len(v.strip()) < 1:
            raise ValueError('El responsable es obligatorio')
        if len(v.strip()) > 120:
            raise ValueError('El responsable no puede exceder 120 caracteres')
        return v.strip()

@router_sgi.post("/sgi/gestion-cambio")
def crear_gestion_cambio(
    req: Request,
    datos: GestionCambioCreate,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Preparar datos del GC principal
        datos_gc = {
            "proceso": datos.proceso,
            "subproceso": datos.subproceso,
            "cop": datos.cop,
            "estado": datos.estado,
            "fecha_apertura": datos.fecha_apertura,
            "fecha_cierre": datos.fecha_cierre,
            "nombre_cambio": datos.nombre_cambio,
            "descripcion_cambio": datos.descripcion_cambio,
            "responsable": datos.responsable,
            "actividades": [actividad.dict() for actividad in datos.actividades],
            "usuario_creacion": user_session.get('username', 'unknown')
        }

        resultado = gestion_sgi.crear_gestion_cambio(datos_gc)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al crear gestión del cambio: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- GENERAR CÓDIGO AUTOMÁTICO GC ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/gestion-cambio/generar-codigo")
def generar_codigo_gc(
    proceso: str = Query(...),
    subproceso: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        codigo = gestion_sgi.generar_codigo_gc(proceso, subproceso)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={"success": True, "codigo": codigo})

    except Exception as e:
        print(f"Error al generar código GC: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER GESTIONES DEL CAMBIO ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/gestion-cambio")
def obtener_gestiones_cambio(
    req: Request,
    proceso: Optional[str] = Query(None),
    subproceso: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    etapa: Optional[str] = Query(None),
    responsable: Optional[str] = Query(None),
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Preparar filtros
        filtros = {}
        if proceso:
            filtros['proceso'] = proceso
        if subproceso:
            filtros['subproceso'] = subproceso
        if estado:
            filtros['estado'] = estado
        if etapa:
            filtros['etapa'] = etapa
        if responsable:
            filtros['responsable'] = responsable
        if fecha_desde:
            filtros['fecha_desde'] = fecha_desde
        if fecha_hasta:
            filtros['fecha_hasta'] = fecha_hasta

        gestiones = gestion_sgi.obtener_gestiones_cambio(filtros)
        estadisticas = gestion_sgi.obtener_estadisticas_gc()

        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "gestiones": gestiones,
            "estadisticas": estadisticas
        })

    except Exception as e:
        print(f"Error al obtener gestiones del cambio: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER GESTIÓN DEL CAMBIO POR CÓDIGO ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/gestion-cambio/{codigo}")
def obtener_gestion_cambio_por_codigo(
    codigo: str,
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        gestion = gestion_sgi.obtener_gestion_cambio_por_codigo(codigo)
        gestion_sgi.cerrar_conexion()

        if gestion:
            return JSONResponse(content={
                "success": True,
                "gestion": gestion
            })
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Gestión del cambio no encontrada"}
            )

    except Exception as e:
        print(f"Error al obtener gestión del cambio: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- ACTUALIZAR ESTADO PROCESO SGI ---
# ----------------------------------------------------------------------
class EstadoProcesoUpdate(BaseModel):
    codigo: str
    nuevo_estado: str
    observacion: Optional[str] = None

@router_sgi.put("/sgi/procesos/estado")
def actualizar_estado_proceso(
    req: Request,
    datos: EstadoProcesoUpdate,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        resultado = gestion_sgi.actualizar_estado_proceso(
            datos.codigo,
            datos.nuevo_estado,
            datos.observacion
        )

        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al actualizar estado del proceso: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER PROCESO ESPECÍFICO ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/procesos/{codigo}")
def obtener_proceso_por_codigo(
    codigo: str,
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        proceso = gestion_sgi.obtener_proceso_por_codigo(codigo)

        gestion_sgi.cerrar_conexion()

        if proceso:
            return JSONResponse(content={
                "success": True,
                "proceso": proceso
            })
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Proceso no encontrado"}
            )

    except Exception as e:
        print(f"Error al obtener proceso: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER ACTIVIDADES PHVA DE UN PROCESO ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/procesos/{codigo}/actividades")
def obtener_actividades_phva(
    codigo: str,
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        actividades = gestion_sgi.obtener_actividades_phva(codigo)

        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "actividades": actividades
        })

    except Exception as e:
        print(f"Error al obtener actividades PHVA: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- OBTENER ACTIVIDADES SGI POR CODIGO Y TIPO ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/actividades")
def obtener_actividades_sgi(
    codigo: str = Query(...),
    tipo: str = Query(...),
    debug: bool = Query(False),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    tipo_origen = (tipo or "").strip().upper()
    if tipo_origen not in ("PA", "GC", "RVD"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": "Tipo de gestion invalido"}
        )

    try:
        gestion_sgi = GestionSGI()
        actividades = gestion_sgi.obtener_actividades_por_referencia(codigo, tipo_origen)
        debug_info = None
        if debug:
            debug_info = gestion_sgi.diagnostico_actividades(codigo, tipo_origen)
        gestion_sgi.cerrar_conexion()

        payload = {
            "success": True,
            "actividades": actividades
        }
        if debug_info:
            payload["debug"] = debug_info

        return JSONResponse(content=payload)

    except Exception as e:
        print(f"Error al obtener actividades SGI: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- LISTAR EVIDENCIAS DE UNA ACTIVIDAD ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/actividades/{actividad_id}/evidencias")
def listar_evidencias_actividad(
    actividad_id: int,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        evidencias = gestion_sgi.obtener_evidencias_actividad(actividad_id)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "evidencias": evidencias
        })
    except Exception as e:
        print(f"Error al listar evidencias: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

def _sanitizar_nombre_zip(nombre: str, evidencia_id: int) -> str:
    base = os.path.basename(nombre or f"evidencia_{evidencia_id}")
    base = base.replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
    if not base:
        base = f"evidencia_{evidencia_id}"
    return base

def _asegurar_nombre_unico(nombre: str, evidencia_id: int, usados: set) -> str:
    if nombre not in usados:
        usados.add(nombre)
        return nombre

    if "." in nombre:
        base, ext = nombre.rsplit(".", 1)
        candidato = f"{base}_id{evidencia_id}.{ext}"
    else:
        candidato = f"{nombre}_id{evidencia_id}"

    if candidato not in usados:
        usados.add(candidato)
        return candidato

    contador = 2
    while True:
        if "." in nombre:
            base, ext = nombre.rsplit(".", 1)
            candidato = f"{base}_{contador}.{ext}"
        else:
            candidato = f"{nombre}_{contador}"
        if candidato not in usados:
            usados.add(candidato)
            return candidato
        contador += 1

def _generar_zip_evidencias(
    evidencias: List[Dict],
    actividad_id: int,
    gestion_sgi: GestionSGI
) -> Optional[str]:
    if not evidencias:
        return None

    fd, temp_path = tempfile.mkstemp(prefix=f"evidencias_act_{actividad_id}_", suffix=".zip")
    os.close(fd)

    container_clients: Dict[str, Any] = {}
    try:
        usados = set()
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            for evidencia in evidencias:
                container = evidencia.get("container") or gestion_sgi.obtener_container_evidencias(evidencia.get("tipo_origen"))
                blob_path = evidencia.get("blob_path")
                if not container or not blob_path:
                    continue

                if container not in container_clients:
                    container_clients[container] = gestion_sgi._get_container_client(container)

                blob_client = container_clients[container].get_blob_client(blob_path)
                contenido = blob_client.download_blob().readall()

                nombre_archivo = _sanitizar_nombre_zip(
                    evidencia.get("nombre_archivo") or blob_path,
                    evidencia.get("id", 0)
                )
                nombre_archivo = _asegurar_nombre_unico(nombre_archivo, evidencia.get("id", 0), usados)
                zip_path = nombre_archivo
                zipf.writestr(zip_path, contenido)

        return temp_path
    except Exception as e:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        print(f"Error al generar zip de evidencias: {e}")
        return None

# --- DESCARGAR TODAS LAS EVIDENCIAS DE UNA ACTIVIDAD ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/actividades/{actividad_id}/evidencias/descargar")
def descargar_todas_evidencias(
    actividad_id: int,
    background_tasks: BackgroundTasks,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    try:
        evidencias = gestion_sgi.obtener_evidencias_actividad(actividad_id)
        if not evidencias:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "No hay evidencias para descargar"}
            )

        zip_path = _generar_zip_evidencias(evidencias, actividad_id, gestion_sgi)
        gestion_sgi.cerrar_conexion()

        if not zip_path:
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No se pudo generar el ZIP"}
            )

        codigo = evidencias[0].get("codigo_referencia", "actividad")
        nombre_zip = f"evidencias_{codigo}_act-{actividad_id}.zip"
        background_tasks.add_task(os.remove, zip_path)

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename=nombre_zip,
            background=background_tasks
        )

    except Exception as e:
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )


class EvidenciasDownloadRequest(BaseModel):
    evidencias: List[int]

class RegistroAdminRequest(BaseModel):
    tipo: str
    codigo: str
    datos: Dict[str, Any] = {}


class CierreRegistroRequest(BaseModel):
    codigo: str
    tipo: str
    responsable_cierre: str
    fecha_cierre: str
    tipo_cierre: str
    descripcion_cierre: str

class ActividadAdminRequest(BaseModel):
    codigo_referencia: str
    tipo_origen: str
    tipo_actividad: Optional[str] = None
    etapa_actividad: Optional[str] = None
    nombre_actividad: Optional[str] = None
    descripcion: Optional[str] = None
    fecha_cierre: Optional[str] = None
    responsable: Optional[str] = None
    estado: Optional[str] = None
    categoria: Optional[str] = None

class EvidenciaEstadoRequest(BaseModel):
    estado: str
    motivo_rechazo: Optional[str] = None

# --- DESCARGAR EVIDENCIAS SELECCIONADAS ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/actividades/{actividad_id}/evidencias/descargar")
def descargar_evidencias_seleccionadas(
    actividad_id: int,
    datos: EvidenciasDownloadRequest,
    background_tasks: BackgroundTasks,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not datos.evidencias:
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": "No se enviaron evidencias"}
        )

    gestion_sgi = GestionSGI()
    try:
        evidencias = gestion_sgi.obtener_evidencias_por_ids(actividad_id, datos.evidencias)
        if not evidencias:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "No se encontraron evidencias"}
            )

        zip_path = _generar_zip_evidencias(evidencias, actividad_id, gestion_sgi)
        gestion_sgi.cerrar_conexion()

        if not zip_path:
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No se pudo generar el ZIP"}
            )

        codigo = evidencias[0].get("codigo_referencia", "actividad")
        nombre_zip = f"evidencias_{codigo}_act-{actividad_id}_seleccionadas.zip"
        background_tasks.add_task(os.remove, zip_path)

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename=nombre_zip,
            background=background_tasks
        )

    except Exception as e:
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- SUBIR EVIDENCIAS MULTIPLES PARA UNA ACTIVIDAD ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/actividades/{actividad_id}/evidencias")
async def subir_evidencias_actividad(
    actividad_id: int,
    files: List[UploadFile] = File(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not files:
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": "No se enviaron archivos"}
        )

    gestion_sgi = GestionSGI()
    try:
        actividad = gestion_sgi.obtener_actividad_por_id(actividad_id)
        if not actividad:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Actividad no encontrada"}
            )

        container = gestion_sgi.obtener_container_evidencias(actividad.get("tipo_origen"))
        if not container:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No hay contenedor de evidencias configurado"}
            )
        container_client = gestion_sgi._get_container_client(container)

        resultados = []
        for file in files:
            contenido = await file.read()
            if not contenido:
                resultados.append({
                    "archivo": file.filename,
                    "ok": False,
                    "mensaje": "Archivo vacio"
                })
                continue

            blob_path = gestion_sgi.construir_ruta_blob_evidencia(
                actividad.get("tipo_origen", ""),
                actividad.get("codigo_referencia", ""),
                actividad_id,
                file.filename or "archivo"
            )

            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(contenido, overwrite=True)

            url_publica = None
            try:
                url_publica = blob_client.url
            except Exception:
                url_publica = None

            resultado_bd = gestion_sgi.registrar_evidencia_actividad(
                actividad_id=actividad_id,
                codigo_referencia=actividad.get("codigo_referencia", ""),
                tipo_origen=actividad.get("tipo_origen", ""),
                container=container or "",
                blob_path=blob_path,
                url_publica=url_publica,
                nombre_archivo=file.filename or "archivo",
                content_type=file.content_type,
                tamano_bytes=len(contenido),
                usuario=user_session.get("username", "unknown")
            )

            resultados.append({
                "archivo": file.filename,
                "ok": resultado_bd.get("ok", False),
                "id_evidencia": resultado_bd.get("id"),
                "blob_path": blob_path,
                "url_publica": url_publica,
                "mensaje": resultado_bd.get("error")
            })

        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "container": container,
            "resultados": resultados
        })

    except Exception as e:
        print(f"Error al subir evidencias: {e}")
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- REEMPLAZAR ARCHIVO DE EVIDENCIA ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/evidencias/{evidencia_id}/reemplazar")
async def reemplazar_evidencia_archivo(
    evidencia_id: int,
    file: UploadFile = File(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    try:
        evidencia = gestion_sgi.obtener_evidencia_por_id(evidencia_id)
        if not evidencia:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Evidencia no encontrada"}
            )

        container = evidencia.get("container") or gestion_sgi.obtener_container_evidencias(evidencia.get("tipo_origen"))
        if not container:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No hay contenedor configurado"}
            )

        contenido = await file.read()
        if not contenido:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Archivo vacio"}
            )

        container_client = gestion_sgi._get_container_client(container)
        blob_path = evidencia.get("blob_path")
        if not blob_path:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "Ruta de blob no encontrada"}
            )

        blob_client = container_client.get_blob_client(blob_path)
        blob_client.upload_blob(contenido, overwrite=True)

        resultado = gestion_sgi.reemplazar_archivo_evidencia(
            evidencia_id=evidencia_id,
            nombre_archivo=file.filename or evidencia.get("nombre_archivo") or "archivo",
            content_type=file.content_type,
            tamano_bytes=len(contenido),
            usuario_subida=user_session.get("username", "unknown")
        )

        gestion_sgi.cerrar_conexion()

        if not resultado.get("ok"):
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo actualizar evidencia")}
            )

        return JSONResponse(content={"success": True, "mensaje": "Evidencia reemplazada"})

    except Exception as e:
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- ACTUALIZAR ESTADO DE EVIDENCIA (ADMIN) ---
# ----------------------------------------------------------------------
@router_sgi.put("/sgi/evidencias/{evidencia_id}/estado")
def actualizar_estado_evidencia(
    evidencia_id: int,
    datos: EvidenciaEstadoRequest,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()

    # Obtener información de la evidencia ANTES de actualizar
    info_evidencia = None
    try:
        info_evidencia = gestion_sgi._obtener_info_evidencia_para_notificacion(evidencia_id)
        print(f"📋 Info evidencia obtenida: {info_evidencia}")
    except Exception as e:
        print(f"⚠️ Error al obtener info evidencia: {e}")

    # Actualizar el estado
    resultado = gestion_sgi.actualizar_estado_evidencia(
        evidencia_id=evidencia_id,
        estado=datos.estado,
        usuario_validador=user_session.get("username", "unknown"),
        motivo_rechazo=datos.motivo_rechazo
    )
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo actualizar")}
        )

    # Crear notificación si se aprobó o rechazó
    if info_evidencia and datos.estado.lower() in ['aprobada', 'rechazada']:
        try:
            from model.gestion_notificaciones import GestionNotificaciones
            notif = GestionNotificaciones()

            tipo_notif = "APROBACION" if datos.estado.lower() == "aprobada" else "RECHAZO"
            titulo = f"Evidencia {'aprobada' if tipo_notif == 'APROBACION' else 'rechazada'}"
            mensaje = f"La evidencia '{info_evidencia.get('nombre_archivo', 'archivo')}' de la actividad '{info_evidencia.get('nombre_actividad', '')}' ha sido {'aprobada' if tipo_notif == 'APROBACION' else 'rechazada'}"

            if tipo_notif == "RECHAZO" and datos.motivo_rechazo:
                mensaje += f". Motivo: {datos.motivo_rechazo}"

            # TEMPORAL: Crear notificación para el usuario actual (quien aprobó/rechazó)
            # En producción debería ser para el responsable del PA
            usuario_notificacion = user_session.get('nombre', user_session.get('username', 'unknown'))

            print(f"🔔 Creando notificación de {tipo_notif} para usuario actual: '{usuario_notificacion}'")

            notif.crear_notificacion(
                usuario=usuario_notificacion,  # Cambiado temporalmente
                tipo=tipo_notif,
                titulo=titulo,
                mensaje=mensaje,
                codigo_referencia=info_evidencia.get('codigo_referencia', ''),
                tipo_entidad=info_evidencia.get('tipo_origen', 'PA'),
                actividad_id=info_evidencia.get('actividad_id')
            )
            notif.cerrar_conexion()
            print(f"✅ Notificación de {tipo_notif} creada para {usuario_notificacion}")
        except Exception as e:
            print(f"⚠️ No se pudo crear notificación: {e}")
            import traceback
            traceback.print_exc()
            # No fallar la operación si falla la notificación

    return JSONResponse(content={"success": True})

# --- ELIMINAR EVIDENCIA (ADMIN) ---
# ----------------------------------------------------------------------
@router_sgi.delete("/sgi/evidencias/{evidencia_id}")
def eliminar_evidencia_admin(
    evidencia_id: int,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    usuario = user_session.get("username", "unknown")
    resultado = gestion_sgi.eliminar_evidencia(evidencia_id, usuario)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar")}
        )

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })

# --- LISTAR EVIDENCIAS POR CODIGO (ADMIN) ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/evidencias")
def listar_evidencias_por_codigo(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    evidencias = gestion_sgi.obtener_evidencias_por_codigo(codigo, tipo)
    gestion_sgi.cerrar_conexion()
    return JSONResponse(content={"success": True, "evidencias": evidencias})

@router_sgi.delete("/sgi/evidencias")
def eliminar_evidencias_por_codigo(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    usuario = user_session.get("username", "unknown")
    resultado = gestion_sgi.eliminar_evidencias_por_codigo(codigo, tipo, usuario)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar")}
        )

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })

ANALISIS_CAUSAS_EXTS = {"pdf", "xlsx", "xls", "docx", "doc", "jpg", "jpeg", "png"}

def _extension_analisis_causas_valida(nombre: str) -> bool:
    ext = os.path.splitext(nombre or "")[1].lower().lstrip(".")
    return ext in ANALISIS_CAUSAS_EXTS

# --- ADJUNTOS ANALISIS DE CAUSAS ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/analisis-causas")
def listar_adjuntos_analisis_causas(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    adjuntos = gestion_sgi.obtener_adjuntos_analisis_causas(codigo, tipo)
    gestion_sgi.cerrar_conexion()
    return JSONResponse(content={"success": True, "adjuntos": adjuntos})

@router_sgi.post("/sgi/analisis-causas")
async def subir_adjuntos_analisis_causas(
    codigo: str = Query(...),
    tipo: str = Query(...),
    files: List[UploadFile] = File(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not files:
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": "No se enviaron archivos"}
        )

    gestion_sgi = GestionSGI()
    try:
        tipo_norm = (tipo or "").strip().upper()
        if tipo_norm not in ("PA", "GC", "RVD"):
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Tipo invalido"}
            )

        bloqueo = gestion_sgi._validar_registro_modificable(codigo, tipo_norm)
        if bloqueo:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": bloqueo}
            )

        container = gestion_sgi.obtener_container_analisis_causas()
        if not container:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No hay contenedor configurado"}
            )

        container_client = gestion_sgi._get_container_client(container)
        resultados = []

        for file in files:
            nombre_archivo = file.filename or "archivo"
            if not _extension_analisis_causas_valida(nombre_archivo):
                resultados.append({
                    "archivo": nombre_archivo,
                    "ok": False,
                    "mensaje": "Extension no permitida"
                })
                continue

            contenido = await file.read()
            if not contenido:
                resultados.append({
                    "archivo": nombre_archivo,
                    "ok": False,
                    "mensaje": "Archivo vacio"
                })
                continue

            blob_path = gestion_sgi.construir_ruta_blob_analisis_causas(
                tipo_norm,
                codigo,
                nombre_archivo
            )

            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(contenido, overwrite=True)

            url_publica = None
            try:
                url_publica = blob_client.url
            except Exception:
                url_publica = None

            resultado_bd = gestion_sgi.registrar_adjunto_analisis_causas(
                codigo_referencia=codigo,
                tipo_origen=tipo_norm,
                container=container,
                blob_path=blob_path,
                url_publica=url_publica,
                nombre_archivo=nombre_archivo,
                content_type=file.content_type,
                tamano_bytes=len(contenido),
                usuario=user_session.get("username", "unknown")
            )

            resultados.append({
                "archivo": nombre_archivo,
                "ok": resultado_bd.get("ok", False),
                "id_adjunto": resultado_bd.get("id"),
                "blob_path": blob_path,
                "url_publica": url_publica,
                "mensaje": resultado_bd.get("error")
            })

        gestion_sgi.cerrar_conexion()
        return JSONResponse(content={
            "success": True,
            "container": container,
            "resultados": resultados
        })

    except Exception as e:
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.post("/sgi/analisis-causas/{adjunto_id}/reemplazar")
async def reemplazar_adjunto_analisis_causas(
    adjunto_id: int,
    file: UploadFile = File(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    try:
        adjunto = gestion_sgi.obtener_adjunto_analisis_causas_por_id(adjunto_id)
        if not adjunto:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Adjunto no encontrado"}
            )

        if int(adjunto.get("reemplazos") or 0) >= 1:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "El adjunto ya fue reemplazado"}
            )

        nombre_archivo = file.filename or adjunto.get("nombre_archivo") or "archivo"
        if not _extension_analisis_causas_valida(nombre_archivo):
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Extension no permitida"}
            )

        contenido = await file.read()
        if not contenido:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Archivo vacio"}
            )

        container = adjunto.get("container") or gestion_sgi.obtener_container_analisis_causas()
        if not container:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "No hay contenedor configurado"}
            )

        blob_path = adjunto.get("blob_path")
        if not blob_path:
            gestion_sgi.cerrar_conexion()
            return JSONResponse(
                status_code=500,
                content={"success": False, "mensaje": "Ruta de blob no encontrada"}
            )

        container_client = gestion_sgi._get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        blob_client.upload_blob(contenido, overwrite=True)

        resultado = gestion_sgi.reemplazar_adjunto_analisis_causas(
            adjunto_id=adjunto_id,
            nombre_archivo=nombre_archivo,
            content_type=file.content_type,
            tamano_bytes=len(contenido),
            usuario_subida=user_session.get("username", "unknown")
        )

        gestion_sgi.cerrar_conexion()

        if not resultado.get("ok"):
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo reemplazar el adjunto")}
            )

        return JSONResponse(content={"success": True, "mensaje": "Adjunto reemplazado"})

    except Exception as e:
        gestion_sgi.cerrar_conexion()
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.delete("/sgi/analisis-causas/{adjunto_id}")
def eliminar_adjunto_analisis_causas(
    adjunto_id: int,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.eliminar_adjunto_analisis_causas(adjunto_id)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar")}
        )

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })
# --- CRUD REGISTRO PRINCIPAL (ADMIN) ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/registro")
def obtener_registro_admin(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    registro = gestion_sgi.obtener_registro_por_codigo(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not registro:
        return JSONResponse(
            status_code=404,
            content={"success": False, "mensaje": "Registro no encontrado"}
        )

    return JSONResponse(content={"success": True, "registro": registro})

@router_sgi.get("/sgi/registro/historial")
def obtener_historial_registro_admin(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.obtener_historial_registro_admin(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo cargar el historial")}
        )

    return JSONResponse(content={
        "success": True,
        "historial": resultado.get("historial", [])
    })

@router_sgi.put("/sgi/registro")
def actualizar_registro_admin(
    datos: RegistroAdminRequest,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    # Obtener usuario de la sesión
    usuario = user_session.get("username", "unknown")

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.actualizar_registro_por_codigo(datos.codigo, datos.tipo, datos.datos, usuario)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo actualizar")}
        )
    return JSONResponse(content={"success": True})
    return JSONResponse(content={"success": True})

@router_sgi.get("/sgi/cierre/validar")
def validar_cierre_registro(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not usuario_puede_cerrar_sgi(user_session):
        return JSONResponse(
            status_code=403,
            content={"success": False, "mensaje": "No autorizado para validar cierres SGI"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.validar_cierre_registro(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo validar el cierre")}
        )

    payload = dict(resultado)
    payload.pop("ok", None)
    payload["success"] = True
    return JSONResponse(content=payload)

@router_sgi.get("/sgi/cierre/detalle")
def obtener_detalle_cierre(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.obtener_detalle_cierre_registro(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=404,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se encontro cierre")}
        )

    return JSONResponse(content={
        "success": True,
        "detalle": resultado.get("detalle", {})
    })

@router_sgi.get("/sgi/cierre/historial")
def obtener_historial_cierre(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.obtener_historial_cierre_registro(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo cargar el historial")}
        )

    return JSONResponse(content={
        "success": True,
        "historial": resultado.get("historial", [])
    })

@router_sgi.post("/sgi/cierre")
def cerrar_registro(
    datos: CierreRegistroRequest,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not usuario_puede_cerrar_sgi(user_session):
        return JSONResponse(
            status_code=403,
            content={"success": False, "mensaje": "No autorizado para cerrar registros SGI"}
        )

    usuario = user_session.get("username", "unknown")

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.registrar_cierre_registro(
        datos.codigo,
        datos.tipo,
        datos.responsable_cierre,
        datos.fecha_cierre,
        datos.tipo_cierre,
        datos.descripcion_cierre,
        usuario
    )
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo cerrar el registro")}
        )

    if NOTIFICACIONES_AVAILABLE:
        try:
            usuario_notificacion = user_session.get('nombre', user_session.get('username', 'unknown'))
            tipo_registro = (datos.tipo or '').strip().upper()
            codigo_registro = (datos.codigo or '').strip()
            titulo = f"Cierre registrado ({tipo_registro})"
            mensaje = (
                f"El registro {codigo_registro} fue cerrado como '{datos.tipo_cierre}'. "
                f"Responsable: {datos.responsable_cierre}."
            )

            notif = GestionNotificaciones()
            notif.crear_notificacion(
                usuario=usuario_notificacion,
                tipo="CIERRE",
                titulo=titulo,
                mensaje=mensaje,
                codigo_referencia=codigo_registro,
                tipo_entidad=tipo_registro,
                metadata={
                    "tipo_cierre": datos.tipo_cierre,
                    "fecha_cierre": datos.fecha_cierre,
                    "responsable_cierre": datos.responsable_cierre
                }
            )
            notif.cerrar_conexion()
            print(f"Notificacion de CIERRE creada para {usuario_notificacion}")
        except Exception as e:
            print(f"No se pudo crear notificacion de cierre: {e}")

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje", "Registro cerrado")
    })

@router_sgi.put("/sgi/cierre")
def actualizar_cierre_registro(
    datos: CierreRegistroRequest,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not usuario_puede_cerrar_sgi(user_session):
        return JSONResponse(
            status_code=403,
            content={"success": False, "mensaje": "No autorizado para editar cierres SGI"}
        )

    usuario = user_session.get("username", "unknown")

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.actualizar_cierre_registro(
        datos.codigo,
        datos.tipo,
        datos.responsable_cierre,
        datos.fecha_cierre,
        datos.tipo_cierre,
        datos.descripcion_cierre,
        usuario
    )
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        status = 404 if resultado.get("no_encontrado") else 400
        return JSONResponse(
            status_code=status,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo actualizar el cierre")}
        )

    if NOTIFICACIONES_AVAILABLE:
        try:
            usuario_notificacion = user_session.get('nombre', user_session.get('username', 'unknown'))
            tipo_registro = (datos.tipo or '').strip().upper()
            codigo_registro = (datos.codigo or '').strip()
            titulo = f"Cierre actualizado ({tipo_registro})"
            mensaje = (
                f"El cierre del registro {codigo_registro} fue actualizado a '{datos.tipo_cierre}'. "
                f"Responsable: {datos.responsable_cierre}."
            )

            notif = GestionNotificaciones()
            notif.crear_notificacion(
                usuario=usuario_notificacion,
                tipo="CIERRE",
                titulo=titulo,
                mensaje=mensaje,
                codigo_referencia=codigo_registro,
                tipo_entidad=tipo_registro,
                metadata={
                    "tipo_cierre": datos.tipo_cierre,
                    "fecha_cierre": datos.fecha_cierre,
                    "responsable_cierre": datos.responsable_cierre,
                    "accion": "actualizacion"
                }
            )
            notif.cerrar_conexion()
            print(f"Notificacion de CIERRE (actualizacion) creada para {usuario_notificacion}")
        except Exception as e:
            print(f"No se pudo crear notificacion de cierre (actualizacion): {e}")

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje", "Cierre actualizado"),
        "detalle": resultado.get("detalle", {})
    })

@router_sgi.delete("/sgi/registro")
def eliminar_registro_admin(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.eliminar_registro_por_codigo(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar")}
        )
    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })

# --- CRUD ACTIVIDADES (ADMIN) ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/actividades")
def crear_actividad_admin(
    datos: ActividadAdminRequest,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.crear_actividad(datos.dict())
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo crear actividad")}
        )
    return JSONResponse(content={"success": True, "id": resultado.get("id")})

@router_sgi.put("/sgi/actividades/{actividad_id}")
def actualizar_actividad_admin(
    actividad_id: int,
    datos: Dict[str, Any] = Body(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.actualizar_actividad(actividad_id, datos)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo actualizar actividad")}
        )
    return JSONResponse(content={"success": True})

@router_sgi.delete("/sgi/actividades/{actividad_id}")
def eliminar_actividad_admin(
    actividad_id: int,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.eliminar_actividad(actividad_id)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar actividad")}
        )
    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })

@router_sgi.delete("/sgi/actividades")
def eliminar_actividades_por_codigo(
    codigo: str = Query(...),
    tipo: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    gestion_sgi = GestionSGI()
    resultado = gestion_sgi.eliminar_actividades_por_codigo(codigo, tipo)
    gestion_sgi.cerrar_conexion()

    if not resultado.get("ok"):
        return JSONResponse(
            status_code=400,
            content={"success": False, "mensaje": resultado.get("mensaje", "No se pudo eliminar")}
        )

    return JSONResponse(content={
        "success": True,
        "mensaje": resultado.get("mensaje"),
        "advertencias": resultado.get("advertencias", [])
    })

# ============================================================================
# ENDPOINTS PARA GESTIÓN DE PROCESOS Y SUBPROCESOS
# ============================================================================

@router_sgi.get("/sgi/categorias-procesos")
def obtener_categorias_procesos(user_session: dict = Depends(get_user_session)):
    """Obtener todas las categorías de procesos."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        categorias = gestion_sgi.obtener_categorias_procesos()
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "categorias": categorias
        })

    except Exception as e:
        print(f"Error al obtener categorías de procesos: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.get("/sgi/procesos-lista")
def obtener_procesos_lista(
    categoria_id: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    """Obtener lista de procesos, opcionalmente filtrados por categoría."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        procesos = gestion_sgi.obtener_procesos(categoria_id)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "procesos": procesos
        })

    except Exception as e:
        print(f"Error al obtener procesos: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.get("/sgi/subprocesos-lista")
def obtener_subprocesos_lista(
    proceso_id: Optional[int] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    """Obtener lista de subprocesos, opcionalmente filtrados por proceso."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        subprocesos = gestion_sgi.obtener_subprocesos(proceso_id)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "subprocesos": subprocesos
        })

    except Exception as e:
        print(f"Error al obtener subprocesos: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.get("/sgi/estructura-procesos")
def obtener_estructura_procesos(user_session: dict = Depends(get_user_session)):
    """Obtener la estructura completa de categorías, procesos y subprocesos."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        estructura = gestion_sgi.obtener_estructura_completa_procesos()
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "estructura": estructura
        })

    except Exception as e:
        print(f"Error al obtener estructura de procesos: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.get("/sgi/proceso-por-codigo/{codigo}")
def obtener_proceso_por_codigo(
    codigo: str,
    user_session: dict = Depends(get_user_session)
):
    """Obtener un proceso específico por su código."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        proceso = gestion_sgi.obtener_proceso_por_codigo(codigo)
        gestion_sgi.cerrar_conexion()

        if proceso:
            return JSONResponse(content={
                "success": True,
                "proceso": proceso
            })
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "mensaje": "Proceso no encontrado"}
            )

    except Exception as e:
        print(f"Error al obtener proceso por código: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

@router_sgi.get("/sgi/cops-lista")
def obtener_cops_lista(user_session: dict = Depends(get_user_session)):
    """Obtener lista de COPs únicos."""
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        gestion_sgi = GestionSGI()
        cops = gestion_sgi.obtener_cops_unicos()
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "cops": cops
        })

    except Exception as e:
        print(f"Error al obtener COPs: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# ==================== REVISIÓN POR LA DIRECCIÓN (RVD) ====================

# --- GENERAR CÓDIGO AUTOMÁTICO RVD ---
# ----------------------------------------------------------------------
@router_sgi.get("/sgi/revision-direccion/generar-codigo")
def generar_codigo_rvd(
    proceso: str = Query(...),
    subproceso: str = Query(...),
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()
        codigo = gestion_sgi.generar_codigo_rvd(proceso, subproceso)
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content={"success": True, "codigo": codigo})

    except Exception as e:
        print(f"Error al generar código RVD: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- CREAR REVISIÓN POR LA DIRECCIÓN ---
# ----------------------------------------------------------------------
class RevisionDireccionCreate(BaseModel):
    proceso: str
    subproceso: str
    cop: str
    ano_rvd: int
    accion_compromiso: str
    responsable: str
    estado: str
    fecha_apertura: str
    fecha_cierre: Optional[str] = None

@router_sgi.post("/sgi/revision-direccion")
def crear_revision_direccion(
    req: Request,
    datos: RevisionDireccionCreate,
    user_session: dict = Depends(get_user_session)
):
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        gestion_sgi = GestionSGI()

        # Preparar datos para la base de datos
        datos_rvd = {
            "proceso": datos.proceso,
            "subproceso": datos.subproceso,
            "cop": datos.cop,
            "ano_rvd": datos.ano_rvd,
            "actividad": datos.accion_compromiso,
            "responsable": datos.responsable,
            "estado": datos.estado,
            "fecha_apertura": datos.fecha_apertura,
            "fecha_cierre": datos.fecha_cierre,
        }

        resultado = gestion_sgi.crear_revision_direccion(
            datos_rvd,
            user_session.get('username', 'unknown')
        )
        gestion_sgi.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al crear revisión por la dirección: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error del servidor: {str(e)}"}
        )

# --- FUNCIÓN AUXILIAR PARA FILTRO DE VENCIMIENTO ---
# ----------------------------------------------------------------------
def aplicar_filtro_vencimiento_backend(datos, filtro_vencimiento):
    """Aplica filtro de vencimiento en el backend basado en fechas de cierre."""
    from datetime import date

    today = date.today()
    datos_filtrados = []

    for registro in datos:
        fecha_cierre_str = registro.get('fecha_cierre_formato') or registro.get('fecha_cierre')

        if not fecha_cierre_str:
            # Registros sin fecha se consideran "en plazo" para filtro 'ok'
            if filtro_vencimiento == 'ok':
                datos_filtrados.append(registro)
            continue

        try:
            # Parsear fecha - puede venir en formato DD/MM/YYYY o YYYY-MM-DD
            if '/' in fecha_cierre_str:
                # Formato DD/MM/YYYY
                day, month, year = fecha_cierre_str.split('/')
                fecha_cierre = date(int(year), int(month), int(day))
            elif '-' in fecha_cierre_str:
                # Formato YYYY-MM-DD
                fecha_cierre = date.fromisoformat(fecha_cierre_str)
            else:
                # Formato no reconocido, saltar
                continue

            # Calcular diferencia en días
            diff = (fecha_cierre - today).days

            # Aplicar filtro según el tipo
            if filtro_vencimiento == 'vencido' and diff < 0:
                datos_filtrados.append(registro)
            elif filtro_vencimiento == 'proximo' and 0 <= diff <= 10:
                datos_filtrados.append(registro)
            elif filtro_vencimiento == 'ok' and diff > 10:
                datos_filtrados.append(registro)

        except (ValueError, TypeError) as e:
            # Error al parsear fecha, saltar registro
            print(f"Error parseando fecha {fecha_cierre_str}: {e}")
            continue

    return datos_filtrados

# --- ENDPOINT PARA DESCARGAR DATOS FILTRADOS ---
# ----------------------------------------------------------------------
@router_sgi.post("/sgi/exportar_pendientes")
async def exportar_pendientes(
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    """Endpoint para exportar datos filtrados de pendientes en formato XLSX o CSV."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    try:
        # Obtener datos del request
        body = await req.json()
        formato = body.get('formato', 'xlsx').lower()
        filtros = body.get('filtros', {})

        if formato not in ['xlsx', 'csv']:
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "Formato no soportado. Use 'xlsx' o 'csv'"}
            )

        gestion_sgi = GestionSGI()

        # Obtener datos filtrados (usando la misma lógica que el endpoint filtrar_por_tipo)
        tipo_gestion = filtros.get('tipo_gestion', '').strip()
        datos = gestion_sgi.obtener_datos_por_tipo_gestion(tipo_gestion, filtros)

        gestion_sgi.cerrar_conexion()

        if not datos:
            return JSONResponse(
                status_code=400,
                content={"success": False, "mensaje": "No hay datos para exportar"}
            )

        # Aplicar filtro de vencimiento si está presente (filtro del lado cliente)
        filtro_vencimiento = filtros.get('filtro_vencimiento')
        if filtro_vencimiento:
            datos = aplicar_filtro_vencimiento_backend(datos, filtro_vencimiento)

        # Construir detalle de filtros aplicados para auditoría
        filtros_aplicados = []

        if tipo_gestion:
            filtros_aplicados.append(f"Tipo: {tipo_gestion}")

        if filtros.get('codigo'):
            filtros_aplicados.append(f"Código: {filtros['codigo']}")

        if filtros.get('proceso'):
            filtros_aplicados.append(f"Proceso: {filtros['proceso']}")

        if filtros.get('subproceso'):
            filtros_aplicados.append(f"Subproceso: {filtros['subproceso']}")

        if filtros.get('estado'):
            filtros_aplicados.append(f"Estado: {filtros['estado']}")

        if filtros.get('fecha_desde'):
            filtros_aplicados.append(f"Fecha desde: {filtros['fecha_desde']}")

        if filtros.get('fecha_hasta'):
            filtros_aplicados.append(f"Fecha hasta: {filtros['fecha_hasta']}")

        if filtro_vencimiento:
            vencimiento_nombres = {
                'vencido': 'Vencidos',
                'proximo': 'Próximos a vencer',
                'ok': 'Al día'
            }
            vencimiento_texto = vencimiento_nombres.get(filtro_vencimiento, filtro_vencimiento)
            filtros_aplicados.append(f"Vencimiento: {vencimiento_texto}")

        detalle_filtros = " | ".join(filtros_aplicados) if filtros_aplicados else "Sin filtros"

        # Crear DataFrame con los datos
        df = pd.DataFrame(datos)

        # Mapeo de columnas para mostrar nombres más amigables
        column_mapping = {
            'codigo': 'Codigo',
            'tipo': 'Tipo',
            'proceso': 'Proceso',
            'subproceso': 'Subproceso',
            'actividad': 'Actividad',
            'fecha_apertura_formato': 'Fecha Apertura',
            'fecha_cierre_formato': 'Fecha Cierre',
            'estado': 'Estado',
            'responsable': 'Responsable',
            'fecha_creacion_formato': 'Fecha Creación',
            'usuario_creacion': 'Usuario Creación',
            'cop': 'COP',
            'tipo_accion': 'Tipo Acción',
            'fuente': 'Fuente',
            'hallazgo': 'Hallazgo',
            'causa_raiz': 'Causa Raíz',
            'observaciones': 'Observaciones'
        }

        # Renombrar columnas que existan en el DataFrame
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)

        # Seleccionar columnas importantes en orden específico
        columns_order = [
            'Codigo', 'Tipo', 'Proceso', 'Subproceso', 'Actividad',
            'Fecha Apertura', 'Fecha Cierre', 'Estado', 'Responsable'
        ]

        # Filtrar solo las columnas que existen
        available_columns = [col for col in columns_order if col in df.columns]
        if available_columns:
            df = df[available_columns]

        # Crear archivo en memoria
        output = io.BytesIO()

        if formato == 'csv':
            # Usar comas nativas como separador para mejor compatibilidad con Excel
            df.to_csv(output, index=False, sep=",", encoding="utf-8-sig")
            output.seek(0)

            # Determinar nombre del archivo
            tipo_nombre = tipo_gestion if tipo_gestion else "Todos"
            filename = f"SGI_Pendientes_{tipo_nombre}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            # Registrar exportación en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    auditoria = AuditoriaSGI()
                    auditoria.log_accion_rapida(
                        usuario=user_session.get('username', 'unknown'),
                        accion='EXPORTAR',
                        modulo='SGI',
                        detalle=f"Exportación CSV de {len(datos)} registros - {detalle_filtros}",
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                except Exception as log_error:
                    print(f"⚠️ Error al registrar exportación: {log_error}")

            return StreamingResponse(
                output,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        else:  # XLSX por defecto
            try:
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Pendientes SGI")
            except ImportError:
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name="Pendientes SGI")

            output.seek(0)

            # Determinar nombre del archivo
            tipo_nombre = tipo_gestion if tipo_gestion else "Todos"
            filename = f"SGI_Pendientes_{tipo_nombre}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            # Registrar exportación en auditoría
            if AUDITORIA_AVAILABLE:
                try:
                    auditoria = AuditoriaSGI()
                    auditoria.log_accion_rapida(
                        usuario=user_session.get('username', 'unknown'),
                        accion='EXPORTAR',
                        modulo='SGI',
                        detalle=f"Exportación Excel de {len(datos)} registros - {detalle_filtros}",
                        resultado='exito'
                    )
                    auditoria.cerrar_conexion()
                except Exception as log_error:
                    print(f"⚠️ Error al registrar exportación: {log_error}")

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

    except Exception as e:
        print(f"Error al exportar pendientes: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "mensaje": f"Error al exportar: {str(e)}"
            }
        )


# =====================================================
# ENDPOINTS DE AUDITORÍA SGI
# =====================================================

@router_sgi.get("/sgi/auditoria/logs")
def obtener_logs_auditoria(
    req: Request,
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    usuario: Optional[str] = Query(None),
    accion: Optional[str] = Query(None),
    modulo: Optional[str] = Query(None),
    resultado: Optional[str] = Query(None),
    codigo: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    user_session: dict = Depends(get_user_session)
):
    """Obtiene logs de auditoría con filtros y paginación."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not AUDITORIA_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de auditoría no disponible"}
        )

    auditoria = None
    try:
        auditoria = AuditoriaSGI()

        # Obtener logs con filtros
        resultado_query = auditoria.obtener_logs(
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            usuario=usuario,
            accion=accion,
            modulo=modulo,
            resultado=resultado,
            codigo_registro=codigo,
            page=page,
            per_page=per_page
        )

        # Registrar consulta de auditoría SOLO si hay filtros aplicados
        filtros_aplicados = []
        if fecha_desde:
            filtros_aplicados.append(f"Fecha desde: {fecha_desde}")
        if fecha_hasta:
            filtros_aplicados.append(f"Fecha hasta: {fecha_hasta}")
        if usuario:
            filtros_aplicados.append(f"Usuario: {usuario}")
        if accion:
            filtros_aplicados.append(f"Acción: {accion}")
        if modulo:
            filtros_aplicados.append(f"Módulo: {modulo}")
        if resultado:
            filtros_aplicados.append(f"Resultado: {resultado}")
        if codigo:
            filtros_aplicados.append(f"Código: {codigo}")

        # Solo registrar si hay filtros aplicados
        if filtros_aplicados:
            try:
                detalle_filtros = " | ".join(filtros_aplicados)
                total_encontrados = resultado_query.get('total', 0)
                auditoria.log_accion_rapida(
                    usuario=user_session.get('username', 'unknown'),
                    accion='CONSULTAR',
                    modulo='AUDITORIA',
                    detalle=f"Filtros aplicados: {detalle_filtros} ({total_encontrados} registros encontrados)",
                    resultado='exito'
                )
            except Exception as log_error:
                print(f"⚠️ Error al registrar filtros de auditoría: {log_error}")

        return JSONResponse(content=resultado_query)

    except Exception as e:
        print(f"❌ Error al obtener logs: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "mensaje": f"Error al obtener logs: {str(e)}"
            }
        )
    finally:
        # CRÍTICO: Siempre devolver conexión al pool
        if auditoria is not None:
            try:
                auditoria.cerrar_conexion()
            except Exception as e:
                print(f"⚠️ Error al cerrar conexión de auditoría: {e}")


@router_sgi.get("/sgi/auditoria/estadisticas")
def obtener_estadisticas_auditoria(
    req: Request,
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    """Obtiene estadísticas de auditoría."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not AUDITORIA_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de auditoría no disponible"}
        )

    auditoria = None
    try:
        auditoria = AuditoriaSGI()

        # Obtener estadísticas
        stats = auditoria.obtener_estadisticas(
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta
        )

        return JSONResponse(content=stats)

    except Exception as e:
        print(f"❌ Error al obtener estadísticas: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "mensaje": f"Error al obtener estadísticas: {str(e)}"
            }
        )
    finally:
        # CRÍTICO: Siempre devolver conexión al pool
        if auditoria is not None:
            try:
                auditoria.cerrar_conexion()
            except Exception as e:
                print(f"⚠️ Error al cerrar conexión de auditoría: {e}")


@router_sgi.get("/sgi/auditoria/actividad-reciente")
def obtener_actividad_reciente_auditoria(
    req: Request,
    limite: int = Query(50, ge=1, le=200),
    user_session: dict = Depends(get_user_session)
):
    """Obtiene las últimas acciones registradas."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not AUDITORIA_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de auditoría no disponible"}
        )

    try:
        auditoria = AuditoriaSGI()

        # Obtener actividad reciente
        logs = auditoria.obtener_actividad_reciente(limite=limite)

        auditoria.cerrar_conexion()

        return JSONResponse(content={
            "success": True,
            "logs": logs,
            "total": len(logs)
        })

    except Exception as e:
        print(f"❌ Error al obtener actividad reciente: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "mensaje": f"Error al obtener actividad reciente: {str(e)}"
            }
        )


@router_sgi.get("/sgi/auditoria/exportar")
def exportar_auditoria(
    req: Request,
    formato: str = Query("excel", regex="^(excel|csv)$"),
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    usuario: Optional[str] = Query(None),
    accion: Optional[str] = Query(None),
    modulo: Optional[str] = Query(None),
    resultado: Optional[str] = Query(None),
    codigo: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session)
):
    """Exporta logs de auditoría a Excel o CSV."""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not AUDITORIA_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de auditoría no disponible"}
        )

    auditoria = None
    try:
        auditoria = AuditoriaSGI()

        # Obtener todos los logs sin paginación
        resultado_query = auditoria.obtener_logs(
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            usuario=usuario,
            accion=accion,
            modulo=modulo,
            resultado=resultado,
            codigo_registro=codigo,
            page=1,
            per_page=10000  # Máximo para exportación
        )

        if not resultado_query.get('success'):
            return JSONResponse(
                status_code=500,
                content=resultado_query
            )

        logs = resultado_query.get('logs', [])

        if not logs:
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "mensaje": "No hay datos para exportar"
                }
            )

        # Construir detalle de filtros aplicados
        filtros_aplicados = []
        if fecha_desde:
            filtros_aplicados.append(f"Fecha desde: {fecha_desde}")
        if fecha_hasta:
            filtros_aplicados.append(f"Fecha hasta: {fecha_hasta}")
        if usuario:
            filtros_aplicados.append(f"Usuario: {usuario}")
        if accion:
            filtros_aplicados.append(f"Acción: {accion}")
        if modulo:
            filtros_aplicados.append(f"Módulo: {modulo}")
        if resultado:
            filtros_aplicados.append(f"Resultado: {resultado}")
        if codigo:
            filtros_aplicados.append(f"Código: {codigo}")

        detalle_filtros = " | ".join(filtros_aplicados) if filtros_aplicados else "Sin filtros"

        # Convertir a DataFrame
        df = pd.DataFrame(logs)

        # Seleccionar y renombrar columnas
        columnas_export = {
            'id': 'ID',
            'fecha_formateada': 'Fecha/Hora',
            'usuario': 'Usuario',
            'accion': 'Acción',
            'modulo': 'Módulo',
            'tipo_entidad': 'Tipo Entidad',
            'codigo_registro': 'Código',
            'detalle_registro': 'Detalle',
            'resultado': 'Resultado',
            'duracion_ms': 'Duración (ms)',
            'error_mensaje': 'Error'
        }

        # Filtrar solo columnas existentes
        existing_cols = {k: v for k, v in columnas_export.items() if k in df.columns}
        df = df[list(existing_cols.keys())].rename(columns=existing_cols)

        # Crear archivo en memoria
        output = io.BytesIO()

        if formato == 'csv':
            df.to_csv(output, index=False, encoding='utf-8-sig')
            output.seek(0)

            filename = f"Auditoria_SGI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            # Registrar exportación
            try:
                auditoria.log_accion_rapida(
                    usuario=user_session.get('username', 'unknown'),
                    accion='EXPORTAR',
                    modulo='AUDITORIA',
                    detalle=f'Exportación CSV de {len(logs)} registros - {detalle_filtros}',
                    resultado='exito'
                )
            except:
                pass

            return StreamingResponse(
                output,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        else:  # Excel
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Auditoría SGI')

                # Formato del Excel
                workbook = writer.book
                worksheet = writer.sheets['Auditoría SGI']

                # Formato de encabezado
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#4472C4',
                    'font_color': 'white',
                    'border': 1
                })

                # Aplicar formato al encabezado
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                # Ajustar anchos de columna
                for i, col in enumerate(df.columns):
                    max_len = max(
                        df[col].astype(str).str.len().max(),
                        len(col)
                    )
                    worksheet.set_column(i, i, min(max_len + 2, 50))

            output.seek(0)

            filename = f"Auditoria_SGI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            # Registrar exportación
            try:
                auditoria.log_accion_rapida(
                    usuario=user_session.get('username', 'unknown'),
                    accion='EXPORTAR',
                    modulo='AUDITORIA',
                    detalle=f'Exportación Excel de {len(logs)} registros - {detalle_filtros}',
                    resultado='exito'
                )
            except:
                pass

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

    except Exception as e:
        print(f"❌ Error al exportar auditoría: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "mensaje": f"Error al exportar: {str(e)}"
            }
        )
    finally:
        # CRÍTICO: Siempre devolver conexión al pool
        if auditoria is not None:
            try:
                auditoria.cerrar_conexion()
            except Exception as e:
                print(f"⚠️ Error al cerrar conexión de auditoría: {e}")


# ========================================================================================
#                        ENDPOINTS DE NOTIFICACIONES
# ========================================================================================

@router_sgi.get("/sgi/notificaciones")
def obtener_notificaciones(
    req: Request,
    solo_no_leidas: bool = Query(False),
    limite: int = Query(50),
    user_session: dict = Depends(get_user_session)
):
    """Obtener notificaciones del usuario actual"""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not NOTIFICACIONES_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de notificaciones no disponible"}
        )

    try:
        usuario = user_session.get('nombre', user_session.get('username', 'unknown'))
        #print(f"🔍 Usuario consultando notificaciones: '{usuario}' (de sesión: {user_session})")

        gestion_notif = GestionNotificaciones()
        notificaciones = gestion_notif.obtener_notificaciones_usuario(
            usuario=usuario,
            solo_no_leidas=solo_no_leidas,
            limite=limite
        )
        no_leidas = gestion_notif.contar_no_leidas(usuario)
        gestion_notif.cerrar_conexion()

        #print(f"📊 Notificaciones encontradas: {len(notificaciones)} | No leídas: {no_leidas}")

        return JSONResponse(content={
            "success": True,
            "notificaciones": notificaciones,
            "total_no_leidas": no_leidas
        })

    except Exception as e:
        print(f"Error al obtener notificaciones: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error: {str(e)}"}
        )


@router_sgi.post("/sgi/notificaciones/{notificacion_id}/marcar-leida")
def marcar_notificacion_leida(
    notificacion_id: int,
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    """Marcar una notificación como leída"""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not NOTIFICACIONES_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de notificaciones no disponible"}
        )

    try:
        gestion_notif = GestionNotificaciones()
        resultado = gestion_notif.marcar_como_leida(notificacion_id)
        gestion_notif.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al marcar notificación como leída: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error: {str(e)}"}
        )


@router_sgi.post("/sgi/notificaciones/marcar-todas-leidas")
def marcar_todas_notificaciones_leidas(
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    """Marcar todas las notificaciones del usuario como leídas"""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not NOTIFICACIONES_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de notificaciones no disponible"}
        )

    try:
        usuario = user_session.get('nombre', user_session.get('username', 'unknown'))

        gestion_notif = GestionNotificaciones()
        resultado = gestion_notif.marcar_todas_como_leidas(usuario)
        gestion_notif.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al marcar todas las notificaciones como leídas: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error: {str(e)}"}
        )


@router_sgi.delete("/sgi/notificaciones/{notificacion_id}")
def eliminar_notificacion(
    notificacion_id: int,
    req: Request,
    user_session: dict = Depends(get_user_session)
):
    """Eliminar una notificación"""
    if not user_session:
        return JSONResponse(
            status_code=401,
            content={"success": False, "mensaje": "No autorizado"}
        )

    if not NOTIFICACIONES_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"success": False, "mensaje": "Sistema de notificaciones no disponible"}
        )

    try:
        usuario = user_session.get('nombre', user_session.get('username', 'unknown'))

        gestion_notif = GestionNotificaciones()
        resultado = gestion_notif.eliminar_notificacion(notificacion_id, usuario)
        gestion_notif.cerrar_conexion()

        return JSONResponse(content=resultado)

    except Exception as e:
        print(f"Error al eliminar notificación: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "mensaje": f"Error: {str(e)}"}
        )


# ========================================================================================
#                        EXPORTAR PENDIENTES SGI (TODOS LOS MÓDULOS)
# ========================================================================================


