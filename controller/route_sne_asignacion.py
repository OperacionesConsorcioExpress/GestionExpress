import logging
from datetime import date
from fastapi import APIRouter, Request, Depends, Query, HTTPException, Body
from starlette.responses import JSONResponse
from typing import Optional, Dict, Any
from model.gestion_sne_asignacion import RegistroSNE
from model.gestion_sne_reglas import GestionReglasSNE

log = logging.getLogger(__name__)

router_sne_asignacion = APIRouter(
    prefix="/sne-asignacion",
    tags=["sne_asignacion"],
)


def require_session(req: Request) -> dict:
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesion no valida")
    return user


def _ip(req: Request) -> Optional[str]:
    """Extrae la IP real del cliente, respetando proxies."""
    forwarded = req.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.client.host if req.client else None


def _normalizar_texto(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def _estado_ui_fila(row: Dict[str, Any]) -> str:
    if str((row or {}).get("estado_dp") or "").strip().upper() == "VENCIDO":
        return "Vencido"
    if not str((row or {}).get("revisor_asignado") or "").strip():
        return "Sin revisor"
    return "Asignado"


def _prioridad_grupo_fila(row: Dict[str, Any]) -> Optional[int]:
    raw = (row or {}).get("prioridad_grupo")
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


# =====================================================================
# API: FECHAS DISPONIBLES EN ICS
# =====================================================================

@router_sne_asignacion.get("/api/fechas-disponibles")
def fechas_disponibles(
    _user: dict = Depends(require_session),
) -> JSONResponse:
    with RegistroSNE() as db:
        fechas = db.fechas_disponibles()
    return JSONResponse(content={"ok": True, "fechas": fechas})


@router_sne_asignacion.get("/api/tipo-dia")
def tipo_dia_operativo(
    fecha: str = Query(..., description="Fecha operativa en formato YYYY-MM-DD"),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    with RegistroSNE() as db:
        data = db.clasificar_fecha_operativa(fecha)
    return JSONResponse(content={"ok": True, "data": data})


# =====================================================================
# API: CONFIGURACIÓN DE REVISORES
# =====================================================================

@router_sne_asignacion.get("/api/revisores")
def listar_revisores(
    _user: dict = Depends(require_session),
) -> JSONResponse:
    with RegistroSNE() as db:
        data = db.listar_config_revisores()
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.put("/api/revisores")
def guardar_revisores(
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    revisores = body.get("revisores", [])
    try:
        with RegistroSNE() as db:
            data = db.guardar_config_revisores(
                revisores=revisores,
                usuario_actualiza=_user.get("username", "sistema"),
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.put("/api/asignaciones/revisor")
def asignar_revisor_manual(
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    ids_ics = body.get("ids_ics", [])
    revisor_user_id = body.get("revisor_user_id")
    origen = body.get("origen", "tabla_menu")
    try:
        with RegistroSNE() as db:
            data = db.asignar_revisor_manual(
                ids_ics=ids_ics,
                revisor_user_id=revisor_user_id,
                usuario_actualiza=_user.get("username", "sistema"),
                origen=str(origen or "tabla_menu"),
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.delete("/api/asignaciones/revisor")
def desasignar_revisor_manual(
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    ids_ics = body.get("ids_ics", [])
    origen = body.get("origen", "tabla_menu_clear")
    try:
        with RegistroSNE() as db:
            data = db.desasignar_revisor_manual(
                ids_ics=ids_ics,
                usuario_actualiza=_user.get("username", "sistema"),
                origen=str(origen or "tabla_menu_clear"),
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.post("/api/asignaciones/ejecutar")
def ejecutar_asignacion(
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    ids_ics = body.get("ids_ics", [])
    try:
        with RegistroSNE() as db:
            data = db.ejecutar_asignacion(
                ids_ics=ids_ics,
                usuario_asigna_id=_user.get("id"),
                usuario_actualiza=_user.get("username", "sistema"),
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        log.exception("Error ejecutando asignacion SNE")
        raise HTTPException(status_code=500, detail="No fue posible ejecutar la asignacion")
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.post("/api/asignaciones/revertir")
def revertir_asignacion(
    req: Request,
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    ids_ics = body.get("ids_ics", [])
    motivo = body.get("motivo", "")
    try:
        with RegistroSNE() as db:
            data = db.revertir_asignacion(
                ids_ics=ids_ics,
                usuario_revierte_id=_user.get("id"),
                usuario_revierte=_user.get("username", "sistema"),
                motivo=str(motivo or ""),
                ip_origen=_ip(req),
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        log.exception("Error revirtiendo asignacion SNE")
        raise HTTPException(status_code=500, detail="No fue posible revertir la asignacion")
    return JSONResponse(content={"ok": True, "data": data})


# =====================================================================
# API: TABLA ICS COMPLETA — con enriquecimiento de reglas activas
# =====================================================================

@router_sne_asignacion.get("/api/tabla-ics")
def tabla_ics(
    req: Request,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(50, ge=1, le=200),
    vista: str = Query("pendientes"),
    id_ejecucion: Optional[str] = Query(None),
    # Filtros texto
    id_ics: Optional[str] = Query(None),
    fecha: Optional[str] = Query(None),
    id_linea: Optional[str] = Query(None),
    ruta_comercial: Optional[str] = Query(None),
    vehiculo_real: Optional[str] = Query(None),
    conductor: Optional[str] = Query(None),
    motivos: Optional[str] = Query(None),
    responsables: Optional[str] = Query(None),
    servicio: Optional[str] = Query(None),
    tabla: Optional[str] = Query(None),
    viaje_linea: Optional[str] = Query(None),
    id_viaje: Optional[str] = Query(None),
    sentido: Optional[str] = Query(None),
    hora_ini_teorica: Optional[str] = Query(None),
    hora_ini_teorica_min: Optional[str] = Query(None),
    hora_ini_teorica_max: Optional[str] = Query(None),
    motivo_original: Optional[str] = Query(None),
    id_concesion: Optional[str] = Query(None),
    fecha_carga: Optional[str] = Query(None),
    fecha_ejecucion: Optional[str] = Query(None),
    componente: Optional[str] = Query(None),
    revisor_asignado: Optional[str] = Query(None),
    revisor_asignado_tabla: Optional[str] = Query(None),
    usuario_asigna_nombre: Optional[str] = Query(None),
    regla_aplicada: Optional[str] = Query(None),
    grupo_asignacion: Optional[str] = Query(None),
    prioridad_grupo: Optional[str] = Query(None),
    estado_ui: Optional[str] = Query(None),
    # Filtros rango KM
    km_prog_ad_min: Optional[float] = Query(None),
    km_prog_ad_max: Optional[float] = Query(None),
    km_elim_eic_min: Optional[float] = Query(None),
    km_elim_eic_max: Optional[float] = Query(None),
    km_ejecutado_min: Optional[float] = Query(None),
    km_ejecutado_max: Optional[float] = Query(None),
    offset_inicio_min: Optional[float] = Query(None),
    offset_inicio_max: Optional[float] = Query(None),
    offset_fin_min: Optional[float] = Query(None),
    offset_fin_max: Optional[float] = Query(None),
    km_revision_min: Optional[float] = Query(None),
    km_revision_max: Optional[float] = Query(None),
    incluir_resumen: bool = Query(True),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    km_ranges = {
        "km_prog_ad":    (km_prog_ad_min,    km_prog_ad_max),
        "km_elim_eic":   (km_elim_eic_min,   km_elim_eic_max),
        "km_ejecutado":  (km_ejecutado_min,   km_ejecutado_max),
        "offset_inicio": (offset_inicio_min,  offset_inicio_max),
        "offset_fin":    (offset_fin_min,     offset_fin_max),
        "km_revision":   (km_revision_min,    km_revision_max),
    }
    filtros_enriquecidos_activos = any([
        str(revisor_asignado_tabla or "").strip(),
        str(regla_aplicada or "").strip(),
        str(grupo_asignacion or "").strip(),
        str(prioridad_grupo or "").strip(),
        str(estado_ui or "").strip(),
    ])
    include_summary = bool(incluir_resumen or filtros_enriquecidos_activos)
    try:
        with RegistroSNE() as db:
            resultado = db.listar_ics_tabla(
                pagina=pagina,
                tamano=tamano,
                include_summary=include_summary,
                vista=vista or "pendientes",
                id_ejecucion=id_ejecucion or None,
                usuario_actual_id=_user.get("id"),
                id_ics=id_ics or None,
                fecha=fecha or None,
                id_linea=id_linea or None,
                ruta_comercial=ruta_comercial or None,
                vehiculo_real=vehiculo_real or None,
                conductor=conductor or None,
                motivos_filter=motivos or None,
                responsables_filter=responsables or None,
                servicio=servicio or None,
                tabla=tabla or None,
                viaje_linea=viaje_linea or None,
                id_viaje=id_viaje or None,
                sentido=sentido or None,
                hora_ini_teorica=hora_ini_teorica or None,
                hora_ini_teorica_min=hora_ini_teorica_min or None,
                hora_ini_teorica_max=hora_ini_teorica_max or None,
                motivo_original=motivo_original or None,
                id_concesion=id_concesion or None,
                fecha_carga=fecha_carga or None,
                fecha_ejecucion=fecha_ejecucion or None,
                componente=componente or None,
                revisor_asignado=revisor_asignado or None,
                usuario_asigna_nombre=usuario_asigna_nombre or None,
                km_ranges=km_ranges,
            )
    except Exception as exc:
        log.exception("tabla-ics: error consultando datos: %s", exc)
        raise HTTPException(status_code=500, detail="Error interno consultando tabla ICS")

    if resultado.get("resumen_rows"):
        try:
            with GestionReglasSNE() as reglas_db:
                resultado["resumen_rows"] = reglas_db.enriquecer_filas(resultado["resumen_rows"])
        except Exception as exc:
            # El enriquecimiento falla (ej: migración pendiente). Se loguea
            # y se devuelve la tabla sin campos de asignación antes que 500.
            log.exception("tabla-ics: fallo al enriquecer filas con reglas: %s", exc)
            resultado["advertencia"] = "Enriquecimiento de reglas no disponible temporalmente."
    elif resultado.get("data"):
        try:
            with GestionReglasSNE() as reglas_db:
                resultado["data"] = reglas_db.enriquecer_filas(resultado["data"])
        except Exception as exc:
            log.exception("tabla-ics: fallo al enriquecer pagina con reglas: %s", exc)
            resultado["advertencia"] = "Enriquecimiento de reglas no disponible temporalmente."

    resumen_rows = resultado.get("resumen_rows") or []
    if include_summary:
        inicio = max(0, (pagina - 1) * tamano)
        fin = inicio + tamano if tamano > 0 else None
        resultado["data"] = resumen_rows[inicio:fin]

    revisor_tabla_norm = _normalizar_texto(revisor_asignado_tabla)
    regla_aplicada_norm = _normalizar_texto(regla_aplicada)
    grupo_asignacion_norm = _normalizar_texto(grupo_asignacion)
    estado_ui_norm = _normalizar_texto(estado_ui)
    prioridad_grupo_texto = str(prioridad_grupo or "").strip()

    if filtros_enriquecidos_activos:
        filtradas = list(resumen_rows)

        if revisor_tabla_norm:
            filtradas = [
                row for row in filtradas
                if revisor_tabla_norm in str((row or {}).get("revisor_asignado") or "").strip().lower()
            ]

        if regla_aplicada_norm:
            filtradas = [
                row for row in filtradas
                if regla_aplicada_norm in str((row or {}).get("regla_aplicada") or "").strip().lower()
            ]

        if grupo_asignacion_norm:
            filtradas = [
                row for row in filtradas
                if grupo_asignacion_norm in str((row or {}).get("grupo_asignacion") or "").strip().lower()
            ]

        if prioridad_grupo_texto:
            try:
                prioridad_grupo_num = int(float(prioridad_grupo_texto))
            except (TypeError, ValueError):
                filtradas = []
            else:
                filtradas = [
                    row for row in filtradas
                    if _prioridad_grupo_fila(row) == prioridad_grupo_num
                ]

        if estado_ui_norm:
            filtradas = [
                row for row in filtradas
                if estado_ui_norm in _estado_ui_fila(row).lower()
            ]

        total_filtradas = len(filtradas)
        paginas_filtradas = ((total_filtradas + tamano - 1) // tamano) if tamano > 0 else 0
        inicio = max(0, (pagina - 1) * tamano)
        fin = inicio + tamano if tamano > 0 else None
        resultado["resumen_rows"] = filtradas
        resultado["data"] = filtradas[inicio:fin]
        resultado["total"] = total_filtradas
        resultado["pagina"] = pagina
        resultado["tamano"] = tamano
        resultado["paginas"] = paginas_filtradas

    return JSONResponse(content={"ok": True, **resultado})


# =====================================================================
# API: TRAZABILIDAD — diagnóstico de evaluación para un caso
# =====================================================================

@router_sne_asignacion.get("/api/trazabilidad/{id_ics}")
def trazabilidad_caso(
    id_ics: int,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    """
    Devuelve la traza completa de evaluación de reglas para un caso ICS.

    Útil para diagnosticar por qué un caso quedó en PENDIENTE_REGLA:
    muestra la normalización aplicada, qué reglas se evaluaron y cuál
    condición falló en cada una.
    """
    try:
        with GestionReglasSNE() as db:
            traza = db.trazar_fila(id_ics)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": traza})


# =====================================================================
# API REGLAS SNE — CRUD
# =====================================================================

@router_sne_asignacion.get("/api/reglas")
def listar_reglas(
    _user: dict = Depends(require_session),
) -> JSONResponse:
    with GestionReglasSNE() as db:
        data = db.listar_reglas()
    return JSONResponse(content={"ok": True, "data": data})


@router_sne_asignacion.get("/api/reglas/{id_regla}")
def obtener_regla(
    id_regla: int,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    with GestionReglasSNE() as db:
        regla = db.obtener_regla(id_regla)
    if not regla:
        raise HTTPException(status_code=404, detail="Regla no encontrada")
    return JSONResponse(content={"ok": True, "data": regla})


@router_sne_asignacion.post("/api/reglas")
def crear_regla(
    req: Request,
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    creado_por: str = _user.get("username", "sistema")
    try:
        with GestionReglasSNE() as db:
            regla = db.crear_regla(body, creado_por, ip=_ip(req))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": regla}, status_code=201)


@router_sne_asignacion.put("/api/reglas/{id_regla}")
def actualizar_regla(
    id_regla: int,
    req: Request,
    body: Dict[str, Any] = Body(...),
    _user: dict = Depends(require_session),
) -> JSONResponse:
    actualizado_por: str = _user.get("username", "sistema")
    try:
        with GestionReglasSNE() as db:
            regla = db.actualizar_regla(id_regla, body, actualizado_por, ip=_ip(req))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if not regla:
        raise HTTPException(status_code=404, detail="Regla no encontrada")
    return JSONResponse(content={"ok": True, "data": regla})


@router_sne_asignacion.patch("/api/reglas/{id_regla}/toggle")
def toggle_activa(
    id_regla: int,
    req: Request,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    actualizado_por: str = _user.get("username", "sistema")
    with GestionReglasSNE() as db:
        regla = db.toggle_activa(id_regla, actualizado_por, ip=_ip(req))
    if not regla:
        raise HTTPException(status_code=404, detail="Regla no encontrada")
    return JSONResponse(content={"ok": True, "data": regla})


@router_sne_asignacion.post("/api/reglas/{id_regla}/duplicar")
def duplicar_regla(
    id_regla: int,
    req: Request,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    creado_por: str = _user.get("username", "sistema")
    with GestionReglasSNE() as db:
        try:
            regla = db.duplicar_regla(id_regla, creado_por, ip=_ip(req))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content={"ok": True, "data": regla}, status_code=201)


@router_sne_asignacion.delete("/api/reglas/{id_regla}")
def eliminar_regla(
    id_regla: int,
    req: Request,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    eliminado_por: str = _user.get("username", "sistema")
    with GestionReglasSNE() as db:
        eliminado = db.eliminar_regla(id_regla, eliminado_por, ip=_ip(req))
    if not eliminado:
        raise HTTPException(status_code=404, detail="Regla no encontrada")
    return JSONResponse(content={"ok": True})


# =====================================================================
# API REGLAS SNE — HISTORIAL
# =====================================================================

@router_sne_asignacion.get("/api/reglas/{id_regla}/historial")
def historial_regla(
    id_regla: int,
    _user: dict = Depends(require_session),
) -> JSONResponse:
    """
    Devuelve el historial de cambios de una regla (CREAR, EDITAR,
    ACTIVAR, DESACTIVAR, DUPLICAR, ELIMINAR), más reciente primero.
    """
    with GestionReglasSNE() as db:
        data = db.historial_regla(id_regla)
    return JSONResponse(content={"ok": True, "data": data})

# =====================================================================
# API DIAGNÓSTICO — reglas activas vigentes
# =====================================================================

@router_sne_asignacion.get("/api/debug/reglas-activas")
def debug_reglas_activas(
    _user: dict = Depends(require_session),
) -> JSONResponse:
    """
    Diagnóstico: devuelve la lista de reglas que el motor de evaluación
    usará en la próxima llamada a /api/tabla-ics.

    Útil para verificar:
      - Si hay reglas activas vigentes (campo total > 0)
      - Si alguna regla tiene vigencia_hasta en el pasado (expirada)
      - Los valores exactos de componente / responsable / observacion guardados

    Si total = 0 → todas las filas quedarán en PENDIENTE_REGLA.
    """
    with GestionReglasSNE() as db:
        activas = db.reglas_activas()
        todas   = db.listar_reglas()

    expiradas = [
        r for r in todas
        if r.get("vigencia_hasta") and r["vigencia_hasta"] < date.today().isoformat()
    ]

    return JSONResponse(content={
        "ok":            True,
        "fecha_hoy":     date.today().isoformat(),
        "total_activas": len(activas),
        "total_reglas":  len(todas),
        "expiradas":     len(expiradas),
        "advertencia":   (
            "No hay reglas activas y vigentes. Todos los casos quedarán en PENDIENTE_REGLA. "
            "Ejecute la migración 003 o revise que las reglas estén activas y sin vigencia expirada."
        ) if not activas else None,
        "reglas_activas": activas,
        "reglas_expiradas": expiradas,
    })
