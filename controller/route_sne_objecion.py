import base64
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from typing import Optional
from pydantic import BaseModel
from model.gestion_sne_objecion import GestionSneObjecion
from model.gestion_usuarios import HandleDB

_TZ_BOGOTA = ZoneInfo("America/Bogota")

def _calc_estado_objecion_desc(estado_objecion, fecha_cierre_dp_str: str) -> str:
    """Calcula el estado descriptivo de la objeción igual que listar_ics_por_usuario."""
    if estado_objecion == 1:
        return "Objetado"
    if estado_objecion == 2:
        return "Vencido"
    if estado_objecion == 0 or estado_objecion is None:
        fecha_cierre = None
        if fecha_cierre_dp_str:
            try:
                fecha_cierre = datetime.strptime(fecha_cierre_dp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_TZ_BOGOTA)
            except Exception:
                pass
        ahora = datetime.now(_TZ_BOGOTA)
        if fecha_cierre and fecha_cierre < ahora:
            return "Vencido"
        return "Abierto"
    return "--"

router_sne_objecion = APIRouter(prefix="/sne", tags=["sne_objecion"])
templates = Jinja2Templates(directory="./view")
usuarios_db = HandleDB()

def get_user_session(req: Request):
    return req.session.get("user")

def require_session(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesion no valida")
    return user

# =====================================================================
# HTML: VISTA PRINCIPAL
# =====================================================================
@router_sne_objecion.get("/objecion", response_class=HTMLResponse)
def sne_objecion(
    req: Request,
    user_session: dict = Depends(get_user_session),
):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "sne_objecion.html",
        {"request": req, "user_session": user_session},
    )

class CredencialesTransmitoolPayload(BaseModel):
    usuario_tm: str
    clave_tm: str

@router_sne_objecion.get("/api/transmitool/credenciales")
def obtener_credenciales_transmitool(
    user_session: dict = Depends(require_session),
):
    usuario_id = user_session.get("id")
    try:
        data = usuarios_db.get_transmitool_credentials(usuario_id)
        return {"ok": True, "data": data}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No fue posible consultar credenciales TM: {str(e)}")

@router_sne_objecion.post("/api/transmitool/credenciales")
def guardar_credenciales_transmitool(
    payload: CredencialesTransmitoolPayload,
    user_session: dict = Depends(require_session),
):
    usuario_id = user_session.get("id")
    try:
        data = usuarios_db.update_transmitool_credentials(
            user_id=usuario_id,
            usuario_tm=payload.usuario_tm,
            clave_tm=payload.clave_tm,
        )
        return {"ok": True, "msg": "Credenciales TM actualizadas correctamente", "data": data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No fue posible guardar credenciales TM: {str(e)}")

# =====================================================================
# API: FILTROS
# =====================================================================
@router_sne_objecion.get("/api/filtros/ultima-fecha")
def filtros_ultima_fecha(user_session: dict = Depends(require_session)):
    """
    Retorna la ultima fecha con registros en sne.ics.

    Response:
    {
        "ok": true,
        "data": "2026-02-10"
    }
    """
    with GestionSneObjecion() as db:
        fecha = db.ultima_fecha_ics()
    return {"ok": True, "data": fecha}

@router_sne_objecion.get("/api/filtros/ics")
def filtros_ics(
    fecha: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna lista de id_ics unicos para la fecha dada,
    filtrados por los que tiene asignados el usuario logueado.

    Response:
    {
        "ok": true,
        "data": [103338422, 103338426, ...]
    }
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        if not fecha:
            fecha = db.ultima_fecha_ics()
        data = db.listar_id_ics_por_fecha(fecha=fecha, usuario_id=usuario_id) if fecha else []
    return {"ok": True, "data": data}

@router_sne_objecion.get("/api/filtros/componentes")
def filtros_componentes(user_session: dict = Depends(require_session)):
    """
    Retorna lista de componentes unicos desde config.cop -> config.componente.

    Response:
    {
        "ok": true,
        "data": ["COMPONENTE A", "COMPONENTE B", ...]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_componentes()
    return {"ok": True, "data": data}

@router_sne_objecion.get("/api/filtros/zonas")
def filtros_zonas(
    componente: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna zonas filtradas por componente.

    Response:
    {
        "ok": true,
        "data": ["ZONA 1", "ZONA 2", ...]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_zonas(componente=componente)
    return {"ok": True, "data": data}

@router_sne_objecion.get("/api/filtros/cop")
def filtros_cop(
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna COPs filtrados por componente y/o zona.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "cop": "COP NORTE", "componente": "...", "zona": "..."},
            ...
        ]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_cop(componente=componente, zona=zona)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/filtros/rutas")
def filtros_rutas(
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna rutas comerciales filtradas por COP.
    Muestra el campo ruta_comercial en la lista desplegable.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "id_linea": 10177, "ruta_comercial": "RUTA 177", "id_cop": 3},
            ...
        ]
    }
    """
    with GestionSneObjecion() as db:
        data = db.listar_rutas_por_cop(id_cop=id_cop, zona=zona)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/filtros/responsables")
def filtros_responsables(
    tab: str = Query("revisar", regex="^(revisar|revisados|validar)$"),
    fecha: Optional[str] = None,
    id_ics: Optional[int] = None,
    id_linea: Optional[int] = None,
    id_concesion: Optional[int] = None,
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Retorna responsables realmente vinculados a los ICS visibles
    por medio de sne.ics_motivo_resp.

    Response:
    {
        "ok": true,
        "data": [
            {"id": 1, "responsable": "JUAN PEREZ"},
            ...
        ]
    }
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        data = db.listar_responsables(
            usuario_id=usuario_id,
            fecha=fecha,
            id_ics=id_ics,
            id_linea=id_linea,
            id_concesion=id_concesion,
            id_cop=id_cop,
            zona=zona,
            componente=componente,
            tab=tab,
        )
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/filtros/todos-responsables")
def filtros_todos_responsables(user_session: dict = Depends(require_session)):
    """
    Retorna TODOS los responsables desde sne.responsable_sne sin filtrar por usuario.
    Se usa en el modal de objeción para mostrar todos los responsables disponibles.
    """
    with GestionSneObjecion() as db:
        data = db.listar_todos_responsables()
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/filtros/acciones")
def filtros_acciones(user_session: dict = Depends(require_session)):
    """Retorna lista de acciones desde sne.acciones."""
    with GestionSneObjecion() as db:
        data = db.listar_acciones()
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/filtros/justificaciones")
def filtros_justificaciones(
    id_acc: Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Retorna justificaciones filtradas por id_acc."""
    with GestionSneObjecion() as db:
        data = db.listar_justificaciones(id_acc=id_acc)
    return {"ok": True, "data": [{"id": r["id_justificacion"], "id_acc": r["id_acc"], "justificacion": r["justificacion"]} for r in data]}

@router_sne_objecion.get("/api/filtros/motivos")
def filtros_motivos(
    id_responsable: Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Retorna motivos activos filtrados por responsable."""
    with GestionSneObjecion() as db:
        data = db.listar_motivos_por_responsable(id_responsable=id_responsable)
    return {"ok": True, "data": [{"id": r["id"], "motivo": r["motivo"], "responsable": r["responsable"]} for r in data]}

@router_sne_objecion.get("/api/filtros/motivos-notas")
def filtros_motivos_notas(user_session: dict = Depends(require_session)):
    """Retorna motivos de notas activos desde sne.motivos_notas."""
    with GestionSneObjecion() as db:
        data = db.listar_motivos_notas_activos()
    return {"ok": True, "data": [{"id": r["id"], "motivo_nota": r["motivo_nota"]} for r in data]}

@router_sne_objecion.get("/api/registros/{id_ics}/motivos-responsables")
def motivos_responsables_ics(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """
    Retorna los motivos y responsables asignados al ICS
    desde sne.ics_motivo_resp. Solo lectura.
    """
    with GestionSneObjecion() as db:
        data = db.listar_motivos_responsables_por_ics(id_ics=id_ics)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_objecion.get("/api/estadisticas")
def estadisticas(
    fecha: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """
    Estadisticas de los ICS asignados al usuario logueado.
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        stats = db.estadisticas_usuario(usuario_id=usuario_id, fecha=fecha)
    return {"ok": True, "data": dict(stats) if stats else {}}

# =====================================================================
# API: MAPA - POSICIONAMIENTOS POR BUS Y FECHA
# =====================================================================
@router_sne_objecion.get("/api/mapa/posicionamientos")
def mapa_posicionamientos(
    bus: str,
    fecha: Optional[str] = None,
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    hora_ini: str = "00:00:00",
    hora_fin: str = "23:59:59",
    user_session: dict = Depends(require_session),
):
    """
    Retorna posicionamientos GPS para un bus en una fecha y rango de horas.
    Se usa en el mapa del modal de gestión.
    """
    # Asegurar segundos en las horas recibidas desde el front (HH:MM -> HH:MM:SS)
    def _pad_hora(h: str, fin: bool = False) -> str:
        parts = h.split(':')
        if len(parts) == 2:
            return h + (':59' if fin else ':00')
        return h
    
    with GestionSneObjecion() as db:
        fecha_base = fecha or db.ultima_fecha_ics()
        fecha_ini = fecha_ini or fecha_base
        fecha_fin = fecha_fin or fecha_ini

        if fecha_ini and fecha_fin and fecha_ini > fecha_fin:
            fecha_ini, fecha_fin = fecha_fin, fecha_ini

        data = db.listar_posicionamientos(
            movil_bus=bus.strip().upper(),
            fecha=fecha_base,
            fecha_ini=fecha_ini,
            fecha_fin=fecha_fin,
            hora_ini=_pad_hora(hora_ini, fin=False),
            hora_fin=_pad_hora(hora_fin, fin=True),
        )
        
    return {
        "ok": True,
        "bus": bus,
        "fecha": fecha_base,
        "fecha_ini": fecha_ini,
        "fecha_fin": fecha_fin,
        "total": len(data),
        "data": [dict(r) for r in data],
    }

# =====================================================================
# API: LISTADO DE REGISTROS ICS POR TAB
# =====================================================================
@router_sne_objecion.get("/api/registros")
def listar_registros(
    tab: str = Query("revisar", regex="^(revisar|revisados|validar)$"),
    fecha: Optional[str] = None,
    id_ics: Optional[int] = None,
    id_linea: Optional[int] = None,
    id_concesion: Optional[int] = None,
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    id_responsable: Optional[int] = None,
    texto_busqueda: Optional[str] = None,
    orden: Optional[str] = None,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(50, ge=1, le=500),
    user_session: dict = Depends(require_session),
):
    """
    Retorna los registros ICS asignados al usuario logueado segun el tab activo.

    Tabs:
        - revisar   -> asignados pendientes de revision (revisor > 0, estado_asignacion=1)
        - revisados -> ya revisados (estado_asignacion=2)
        - validar   -> con objecion pendiente de validar (estado_objecion=1)
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        registros, total = db.listar_ics_por_usuario(
            usuario_id=usuario_id,
            fecha=fecha,
            id_ics=id_ics,
            id_linea=id_linea,
            id_concesion=id_concesion,
            id_cop=id_cop,
            zona=zona,
            componente=componente,
            id_responsable=id_responsable,
            texto_busqueda=texto_busqueda,
            orden=orden,
            tab=tab,
            pagina=pagina,
            tamano=tamano,
        )
    return {
        "ok": True,
        "tab": tab,
        "total": total,
        "pagina": pagina,
        "tamano": tamano,
        "data": [dict(r) for r in registros],
    }

@router_sne_objecion.get("/api/registros/{id_ics}")
def detalle_registro(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """
    Retorna el detalle completo de un ICS para el modal de revision.
    Solo accesible si el ICS esta asignado al usuario logueado.
    """
    usuario_id = user_session.get("id")
    with GestionSneObjecion() as db:
        detalle = db.obtener_detalle_ics(id_ics=id_ics, usuario_id=usuario_id)
    if not detalle:
        raise HTTPException(
            status_code=404,
            detail=f"ICS {id_ics} no encontrado o no asignado a este usuario",
        )
    data = dict(detalle)
    data["estado_objecion_desc"] = _calc_estado_objecion_desc(
        data.get("estado_objecion"), data.get("fecha_cierre_dp")
    )
    return {"ok": True, "data": data}

# =====================================================================
# API: REPORTES DE TABLAS RELACIONADAS A UN ICS
# =====================================================================
@router_sne_objecion.get("/api/registros/{id_ics}/reportes/conteos")
def reportes_conteos(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """Conteos de las 8 tablas de reportes para un ICS."""
    with GestionSneObjecion() as db:
        conteos = db.obtener_conteo_reportes(id_ics=id_ics)
    return {"ok": True, "id_ics": id_ics, "data": conteos}

@router_sne_objecion.get("/api/registros/{id_ics}/reportes/all")
def reportes_all(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """Datos completos de todas las tablas de reporte (para nueva ventana)."""
    with GestionSneObjecion() as db:
        reportes = db.obtener_todos_reportes(id_ics=id_ics)
    return {"ok": True, "id_ics": id_ics, "data": reportes}

@router_sne_objecion.get("/api/registros/{id_ics}/reportes/{tabla}")
def reporte_tabla(
    id_ics: int,
    tabla: str,
    user_session: dict = Depends(require_session),
):
    """Datos de una tabla de reporte específica para un ICS."""
    with GestionSneObjecion() as db:
        if tabla not in db.REPORT_TABLES:
            raise HTTPException(status_code=400, detail=f"Tabla '{tabla}' no es válida")
        reporte = db.obtener_reporte(id_ics=id_ics, tabla_key=tabla)
    if reporte is None:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    return {"ok": True, "id_ics": id_ics, "tabla": tabla, "data": reporte}

# =====================================================================
# API: ACTUALIZAR GESTION DE UN ICS
# PATCH /sne/api/registros/{id_ics}/gestion
# =====================================================================
class GestionPayload(BaseModel):
    estado_asignacion: Optional[int] = None
    estado_objecion: Optional[int] = None
    estado_transmitools: Optional[int] = None
    motivo: Optional[str] = None
    id_responsable: Optional[int] = None
    id_accion: Optional[int] = None
    id_justificacion: Optional[int] = None
    km_objetado: Optional[float] = None

@router_sne_objecion.patch("/api/registros/{id_ics}/gestion")
def actualizar_gestion(
    id_ics: int,
    payload: GestionPayload,
    user_session: dict = Depends(require_session),
):
    """
    Actualiza el estado de gestion de un ICS.
    Solo el revisor asignado puede modificarlo.
    """
    usuario_id = user_session.get("id")
    try:
        with GestionSneObjecion() as db:
            db.actualizar_gestion(
                id_ics=id_ics,
                usuario_id=usuario_id,
                estado_asignacion=payload.estado_asignacion,
                estado_objecion=payload.estado_objecion,
                estado_transmitools=payload.estado_transmitools,
                motivo=payload.motivo,
                id_responsable=payload.id_responsable,
                id_accion=payload.id_accion,
                id_justificacion=payload.id_justificacion,
                km_objetado=payload.km_objetado,
            )
        return {"ok": True, "msg": "Gestion actualizada correctamente"}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))

# =====================================================================
# API: GUARDAR OBJECIÓN COMPLETA (INSERT objecion_guardada + UPDATE gestion_sne)
# POST /sne/api/registros/{id_ics}/guardar-objecion
# =====================================================================
class ObjecionCompletaPayload(BaseModel):
    km_objetado:         Optional[float] = None
    km_no_objetado:      Optional[float] = None
    id_responsable:      Optional[int]   = None
    id_motivo:           Optional[int]   = None
    id_accion:           Optional[int]   = None
    id_justificacion:    Optional[int]   = None
    nota_objecion:       Optional[str]   = None
    tiempo_objecion_seg: Optional[int]   = None
    pdf_base64:          Optional[str]   = None   # PDF generado en frontend, codificado en base64

@router_sne_objecion.post("/api/registros/{id_ics}/guardar-objecion")
def guardar_objecion_completa(
    id_ics: int,
    payload: ObjecionCompletaPayload,
    user_session: dict = Depends(require_session),
):
    """
    Guarda la objeción completa:
    1. Sube el PDF al Azure Blob Storage (sne-informes-objecion).
    2. Inserta en sne.objecion_guardada.
    3. Actualiza sne.gestion_sne (estado_objecion=1, usuario_objeta, fecha_hora_objecion).
    """
    usuario_id = user_session.get("id")
    ruta_reporte = None

    try:
        with GestionSneObjecion() as db:
            # Subir PDF al blob si viene en el payload
            if payload.pdf_base64:
                try:
                    pdf_bytes = base64.b64decode(payload.pdf_base64)
                    # Obtener la fecha del registro ICS (no la fecha de guardado)
                    db.cursor.execute("SELECT fecha FROM sne.ics WHERE id_ics = %s", (id_ics,))
                    row_fecha = db.cursor.fetchone()
                    fecha_ics = row_fecha["fecha"] if row_fecha and row_fecha["fecha"] else None
                    ruta_reporte = db.subir_pdf_objecion(pdf_bytes, id_ics, fecha_ics)
                except Exception as e:
                    # Si falla el blob no bloqueamos el guardado, solo logueamos
                    print(f"[SNE] Error subiendo PDF objecion ICS {id_ics}: {e}")

            db.guardar_objecion_completa(
                id_ics=id_ics,
                usuario_id=usuario_id,
                km_objetado=payload.km_objetado,
                km_no_objetado=payload.km_no_objetado,
                id_responsable=payload.id_responsable,
                id_motivo=payload.id_motivo,
                id_accion=payload.id_accion,
                id_justificacion=payload.id_justificacion,
                nota_objecion=payload.nota_objecion,
                tiempo_objecion_seg=payload.tiempo_objecion_seg,
                ruta_reporte=ruta_reporte,
            )
        return {"ok": True, "msg": "Objeción guardada correctamente", "ruta_reporte": ruta_reporte}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar objeción: {str(e)}")

# =====================================================================
# API: DETALLE DE OBJECIÓN GUARDADA (para modal Revisados modo lectura)
# GET /sne/api/registros/{id_ics}/objecion-guardada
# =====================================================================
@router_sne_objecion.get("/api/registros/{id_ics}/objecion-guardada")
def detalle_objecion_guardada(
    id_ics: int,
    user_session: dict = Depends(require_session),
):
    """
    Retorna el detalle de la objeción guardada para un id_ics.
    Se usa en la pestaña 'Revisados' para mostrar el modal en modo lectura.
    """
    with GestionSneObjecion() as db:
        detalle = db.obtener_detalle_objecion_guardada(id_ics=id_ics)
    return {"ok": True, "data": dict(detalle) if detalle else None}

# =====================================================================
# API: SERVIR PDF INFORME OBJECIÓN DESDE BLOB STORAGE
# GET /sne/api/informe-objecion?ruta=YYYY/MM/DDMMYYYY_IDICS.pdf
# =====================================================================
@router_sne_objecion.get("/api/informe-objecion")
def servir_informe_objecion(
    ruta: str = Query(..., description="Ruta del blob dentro del contenedor sne-informes-objecion"),
    user_session: dict = Depends(require_session),
):
    """
    Descarga el PDF de informe de objeción desde Azure Blob Storage
    y lo retorna como StreamingResponse para visualización en el navegador.
    """
    try:
        from azure.storage.blob import BlobServiceClient
        from dotenv import load_dotenv
        load_dotenv()
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not conn_str:
            raise HTTPException(status_code=500, detail="AZURE_STORAGE_CONNECTION_STRING no configurada")

        service   = BlobServiceClient.from_connection_string(conn_str)
        container = service.get_container_client("sne-informes-objecion")
        blob      = container.get_blob_client(ruta)
        stream    = blob.download_blob()
        pdf_bytes = stream.readall()

        nombre_archivo = ruta.split("/")[-1] if "/" in ruta else ruta
        return StreamingResponse(
            iter([pdf_bytes]),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{nombre_archivo}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Informe no encontrado: {str(e)}")

# =====================================================================
# API: DASHBOARD DE RESULTADOS
# =====================================================================
@router_sne_objecion.get("/api/dashboard/registros")
def dashboard_registros(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    id_linea:  Optional[int] = None,
    id_cop:    Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Conteo diario de IDs Asignados vs Revisados para gráfica de líneas."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        rows = db.dashboard_comportamiento_registros(uid, fecha_ini, fecha_fin, id_linea, id_cop)
    result = []
    for r in rows:
        a = int(r["ids_asignados"] or 0)
        v = int(r["ids_revisados"] or 0)
        result.append({"fecha": r["fecha"], "ids_asignados": a, "ids_revisados": v,
                        "pct_cump": round(v / a * 100, 1) if a else 0.0})
    return {"ok": True, "data": result}

@router_sne_objecion.get("/api/dashboard/kilometros")
def dashboard_kilometros(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    id_linea:  Optional[int] = None,
    id_cop:    Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Suma diaria de km_revision vs km_objetado para gráfica de líneas."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        rows = db.dashboard_comportamiento_km(uid, fecha_ini, fecha_fin, id_linea, id_cop)
    result = []
    for r in rows:
        kr = float(r["km_revision"] or 0)
        ko = float(r["km_objetado"] or 0)
        result.append({"fecha": r["fecha"], "km_revision": round(kr, 3), "km_objetado": round(ko, 3),
                        "pct_cump": round(ko / kr * 100, 1) if kr else 0.0})
    return {"ok": True, "data": result}

@router_sne_objecion.get("/api/dashboard/motivos")
def dashboard_motivos(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    id_linea:  Optional[int] = None,
    id_cop:    Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Distribución de km_revision por motivo y por responsable para polar area."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        data = db.dashboard_distribucion_motivos(uid, fecha_ini, fecha_fin, id_linea, id_cop)
    return {"ok": True, "data": data}

@router_sne_objecion.get("/api/dashboard/rutas")
def dashboard_rutas(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    id_linea:  Optional[int] = None,
    id_cop:    Optional[int] = None,
    user_session: dict = Depends(require_session),
):
    """Datos por año/mes/ruta para scatter chart y tabla detalle."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        rows = db.dashboard_por_ruta(uid, fecha_ini, fecha_fin, id_linea, id_cop)
    result = []
    for r in rows:
        a  = int(r["ids_asignados"] or 0)
        v  = int(r["ids_revisados"] or 0)
        kr = float(r["km_revision"] or 0)
        ko = float(r["km_objetado"] or 0)
        result.append({
            "anio": r["anio"], "mes": r["mes"], "ruta_comercial": r["ruta_comercial"],
            "ids_asignados": a, "ids_revisados": v,
            "pct_cump_ids": round(v / a * 100, 1) if a else 0.0,
            "km_revision": round(kr, 3), "km_objetado": round(ko, 3),
            "pct_cump_km": round(ko / kr * 100, 1) if kr else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_objecion.get("/api/dashboard/meta")
def dashboard_meta(
    user_session: dict = Depends(require_session),
):
    """Última fecha con asignaciones del usuario y primer día de ese mes (defaults dashboard)."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        data = db.dashboard_meta_fechas(uid)
    return {"ok": True, "data": data}

@router_sne_objecion.get("/api/dashboard/rutas-filtro")
def dashboard_rutas_filtro(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    user_session: dict = Depends(require_session),
):
    """Rutas comerciales disponibles para el usuario en el rango dado (select del dashboard)."""
    uid = user_session.get("id")
    with GestionSneObjecion() as db:
        rows = db.dashboard_rutas_disponibles(uid, fecha_ini, fecha_fin)
    return {"ok": True, "data": rows}
