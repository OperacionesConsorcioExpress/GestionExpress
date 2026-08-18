from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import date
from psycopg2.errors import QueryCanceled
from model.gestion_sne_monitor import GestionSneMonitor

router_sne_monitor = APIRouter(prefix="/monitor", tags=["sne_monitor"])
templates = Jinja2Templates(directory="./view")

# Tope del rango de reportes, alineado con el límite de 6 meses que aplica
# _syncDateRange() en view/sne_monitor.html (con holgura para meses de 31 días).
_MAX_RANGO_REPORTE_DIAS = 186

def _sesion(req: Request):
    return req.session.get("user")

def _require_sesion(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesión no válida")
    return user

def _validar_rango_reporte(fecha_ini: Optional[str], fecha_fin: Optional[str]) -> None:
    """
    Los reportes recorren sne.ics sin paginación. Sin rango de fechas la consulta
    degenera en un full scan con ORDER BY que bloquea la base de datos y agota la
    memoria del App Service, así que el rango es obligatorio y acotado.
    """
    if not fecha_ini or not fecha_fin:
        raise HTTPException(
            status_code=400,
            detail="Debe indicar fecha inicio y fecha fin para consultar el reporte.",
        )
    try:
        ini = date.fromisoformat(fecha_ini)
        fin = date.fromisoformat(fecha_fin)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Formato de fecha inválido. Use AAAA-MM-DD.",
        )
    if fin < ini:
        raise HTTPException(
            status_code=400,
            detail="La fecha final no puede ser inferior a la fecha inicial.",
        )
    if (fin - ini).days > _MAX_RANGO_REPORTE_DIAS:
        raise HTTPException(
            status_code=400,
            detail=f"El rango del reporte no puede superar {_MAX_RANGO_REPORTE_DIAS} días (~6 meses).",
        )

def _error_timeout_reporte() -> HTTPException:
    """
    La consulta superó el statement_timeout y PostgreSQL la canceló (liberando sus
    locks). Se traduce a un mensaje accionable en vez de un 500 opaco.
    """
    return HTTPException(
        status_code=504,
        detail="El reporte tardó demasiado y fue cancelado. Reduzca el rango de fechas "
               "o aplique filtros de ruta, COP o zona.",
    )

# ══════════════════════════════════════════════════════════════════════════════
# META FECHAS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/meta")
def monitor_meta(user_session: dict = Depends(_require_sesion)):
    """Última fecha con asignaciones y primer día del mes (inicializar filtros)."""
    with GestionSneMonitor() as db:
        data = db.monitor_meta_fechas()
    return {"ok": True, "data": data}

# ══════════════════════════════════════════════════════════════════════════════
# USUARIOS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/usuarios")
def monitor_usuarios(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Usuarios (revisores) con asignaciones en el rango → select de Resultados."""
    with GestionSneMonitor() as db:
        data = db.monitor_usuarios_disponibles(fecha_ini, fecha_fin)
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/usuarios-revisores")
def monitor_usuarios_revisores(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Revisores disponibles para filtro de Asignaciones."""
    with GestionSneMonitor() as db:
        data = db.monitor_usuarios_revisores(fecha_ini, fecha_fin)
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/usuarios-asignadores")
def monitor_usuarios_asignadores(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Usuarios asignadores (usuario_asigna) disponibles para filtro de Asignaciones."""
    with GestionSneMonitor() as db:
        data = db.monitor_usuarios_asignadores(fecha_ini, fecha_fin)
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/usuarios-objetores")
def monitor_usuarios_objetores(
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Usuarios objetores (usuario_objeta) disponibles para filtro de Asignaciones."""
    with GestionSneMonitor() as db:
        data = db.monitor_usuarios_objetores(fecha_ini, fecha_fin)
    return {"ok": True, "data": data}

# ══════════════════════════════════════════════════════════════════════════════
# FILTROS CATÁLOGO
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/filtros/componentes")
def mon_componentes(user_session: dict = Depends(_require_sesion)):
    with GestionSneMonitor() as db:
        data = db.listar_componentes()
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/filtros/zonas")
def mon_zonas(
    componente: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneMonitor() as db:
        data = db.listar_zonas(componente=componente)
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/filtros/cop")
def mon_cop(
    componente: Optional[str] = None,
    zona: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneMonitor() as db:
        data = db.listar_cop(componente=componente, zona=zona)
    return {"ok": True, "data": [dict(r) for r in data]}

@router_sne_monitor.get("/api/filtros/rutas")
def mon_rutas(
    id_cop: Optional[int] = None,
    zona: Optional[str] = None,
    fecha_ini: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    usuario_id: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    with GestionSneMonitor() as db:
        data = db.listar_rutas_disponibles(
            id_cop=id_cop, zona=zona,
            fecha_ini=fecha_ini, fecha_fin=fecha_fin,
            usuario_id=usuario_id,
        )
    return {"ok": True, "data": [dict(r) for r in data]}

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD RESULTADOS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/dashboard/registros")
def mon_dash_registros(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Conteo diario de IDs Asignados vs Objetados para gráfica de líneas."""
    with GestionSneMonitor() as db:
        rows = db.monitor_comportamiento_registros(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        a = int(r["ids_asignados"] or 0)
        v = int(r["ids_revisados"] or 0)
        result.append({
            "fecha": r["fecha"],
            "ids_asignados": a,
            "ids_revisados": v,
            "pct_cump": round(v / a * 100, 1) if a else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/kilometros")
def mon_dash_km(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Suma diaria de km_revision vs km_objetado para gráfica de líneas."""
    with GestionSneMonitor() as db:
        rows = db.monitor_comportamiento_km(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        kr = float(r["km_revision"] or 0)
        ko = float(r["km_objetado"] or 0)
        result.append({
            "fecha": r["fecha"],
            "km_revision": round(kr, 3),
            "km_objetado": round(ko, 3),
            "pct_cump": round(ko / kr * 100, 1) if kr else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/objetores")
def mon_dash_objetores(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Km objetado vs km asignado agrupado por objetor."""
    with GestionSneMonitor() as db:
        data = db.monitor_por_objetor(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/dashboard/motivos")
def mon_dash_motivos(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Distribución de km_revision por motivo y por responsable."""
    with GestionSneMonitor() as db:
        data = db.monitor_distribucion_motivos(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    return {"ok": True, "data": data}

@router_sne_monitor.get("/api/dashboard/rutas")
def mon_dash_rutas(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Datos por año/mes/ruta para scatter chart y tabla detalle."""
    with GestionSneMonitor() as db:
        rows = db.monitor_por_ruta(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        a  = int(r["ids_asignados"] or 0)
        v  = int(r["ids_revisados"] or 0)
        kr = float(r["km_revision"] or 0)
        ko = float(r["km_objetado"] or 0)
        ka = float(r["km_aceptado"] or 0)
        result.append({
            "anio": r["anio"], "mes": r["mes"], "ruta_comercial": r["ruta_comercial"],
            "ids_asignados": a, "ids_revisados": v,
            "pct_cump_ids": round(v / a * 100, 1) if a else 0.0,
            "km_revision": round(kr, 3), "km_objetado": round(ko, 3),
            "pct_cump_km": round(ko / kr * 100, 1) if kr else 0.0,
            "km_aceptado": round(ka, 3),
            "pct_exito": round(ka / ko * 100, 1) if ko else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/revisores")
def mon_dash_revisores(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Datos por año/mes/revisor para tabla Comportamiento Revisor y gráfico de tendencia."""
    with GestionSneMonitor() as db:
        rows = db.monitor_por_revisor(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    rows_list = list(rows)

    # Primera pasada: sumatoria de ids_obj_mi (Objetados + Manejo Interno) por período
    # para normalizar el factor de volumen de cada revisor
    period_sum_obj_mi: dict = {}
    for r in rows_list:
        k = (r["anio"], r["mes"])
        obj_mi = int(r["ids_obj_mi"] or 0)
        period_sum_obj_mi[k] = period_sum_obj_mi.get(k, 0) + obj_mi

    result = []
    for r in rows_list:
        a        = int(r["ids_asignados"] or 0)
        v        = int(r["ids_revisados"] or 0)
        obj_mi   = int(r["ids_obj_mi"] or 0)
        kr       = float(r["km_revision"] or 0)
        ko       = float(r["km_objetado"] or 0)
        ka       = float(r["km_aceptado"] or 0)
        pct_ids  = round(v / a * 100, 1) if a else 0.0
        pct_km   = round(ko / kr * 100, 1) if kr else 0.0
        pct_ex   = round(ka / ko * 100, 1) if ko else 0.0
        # Factor de volumen: (IDs Obj+MI del revisor / IDs Obj+MI totales del período) × 100
        sum_obj_mi = period_sum_obj_mi.get((r["anio"], r["mes"]), 1) or 1
        vol_norm   = obj_mi / sum_obj_mi
        resultado  = round(
            pct_ids  * 0.10 +
            pct_km   * 0.10 +
            pct_ex   * 0.60 +
            (vol_norm * 100) * 0.20,
        1)
        result.append({
            "anio": r["anio"], "mes": r["mes"],
            "revisor_id": r["revisor_id"], "revisor_nombre": r["revisor_nombre"],
            "ids_asignados": a, "ids_revisados": v,
            "ids_obj_mi": obj_mi,
            "pct_ids": pct_ids,
            "km_revision": round(kr, 3), "km_objetado": round(ko, 3),
            "pct_km": pct_km,
            "km_aceptado": round(ka, 3),
            "pct_exito": pct_ex,
            "resultado": resultado,
            "vol_norm": round(vol_norm * 100, 1),
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/kilometros-aceptados")
def mon_dash_km_aceptados(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Suma diaria de km_objetado vs km_aceptado."""
    with GestionSneMonitor() as db:
        rows = db.monitor_comportamiento_km_aceptado(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        ko = float(r["km_objetado"] or 0)
        ka = float(r["km_aceptado"] or 0)
        result.append({
            "fecha": r["fecha"],
            "km_objetado": round(ko, 3),
            "km_aceptado": round(ka, 3),
            "pct_cump": round(ka / ko * 100, 1) if ko else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/exito-objecion")
def mon_dash_exito_objecion(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Suma diaria de km_revision vs km_aceptado (Éxito Objeción Contundente)."""
    with GestionSneMonitor() as db:
        rows = db.monitor_exito_objecion(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        kr = float(r["km_revision"] or 0)
        ka = float(r["km_aceptado"] or 0)
        result.append({
            "fecha": r["fecha"],
            "km_revision": round(kr, 3),
            "km_aceptado": round(ka, 3),
            "pct_cump": round(ka / kr * 100, 1) if kr else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/estimacion-ingresos")
def mon_dash_estimacion_ingresos(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Estimación diaria de ingresos: $$ km_objetado y $$ km_aceptado × tarifa."""
    with GestionSneMonitor() as db:
        rows = db.monitor_estimacion_ingresos(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion
        )
    result = []
    for r in rows:
        io = float(r["ingresos_objetado"] or 0)
        ia = float(r["ingresos_aceptado"] or 0)
        result.append({
            "fecha": r["fecha"],
            "ingresos_objetado": round(io, 0),
            "ingresos_aceptado": round(ia, 0),
            "pct_cump": round(ia / io * 100, 1) if io else 0.0,
        })
    return {"ok": True, "data": result}

@router_sne_monitor.get("/api/dashboard/boxplot-rutas")
def mon_dash_boxplot_rutas(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    usuario_revisor: Optional[int] = None,
    zona: Optional[str] = None,
    componente: Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_asigna: Optional[int] = None,
    estado_objecion: Optional[int] = None,
    modo: Optional[str] = "sne",
    user_session: dict = Depends(_require_sesion),
):
    """Registros crudos de km_revision por ruta para boxplot."""
    with GestionSneMonitor() as db:
        rows = db.monitor_boxplot_rutas(
            fecha_ini, fecha_fin, id_linea, id_cop, usuario_revisor,
            zona, componente, estado_asignacion, usuario_asigna, estado_objecion, modo
        )
    result = [{
        "ruta_comercial": r["ruta_comercial"],
        "km_valor": round(float(r["km_valor"] or 0), 3),
    } for r in rows]
    return {"ok": True, "data": result}

# ══════════════════════════════════════════════════════════════════════════════
# TABLA REPORTES
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/reportes/sne-procesado")
def mon_reporte_sne_procesado(
    fecha_ini:  Optional[str] = None,
    fecha_fin:  Optional[str] = None,
    id_linea:   Optional[int] = None,
    id_cop:     Optional[int] = None,
    zona:       Optional[str] = None,
    componente: Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Reporte detallado tabla ICS con cruces a config (ruta, componente, zona, cop)."""
    _validar_rango_reporte(fecha_ini, fecha_fin)
    try:
        with GestionSneMonitor() as db:
            rows = db.reporte_sne_procesado(
                fecha_ini=fecha_ini, fecha_fin=fecha_fin,
                id_linea=id_linea, id_cop=id_cop,
                zona=zona, componente=componente,
            )
    except QueryCanceled:
        raise _error_timeout_reporte()
    return {"ok": True, "total": len(rows), "data": rows}

@router_sne_monitor.get("/api/reportes/sne-gestionado")
def mon_reporte_sne_gestionado(
    fecha_ini:         Optional[str] = None,
    fecha_fin:         Optional[str] = None,
    id_linea:          Optional[int] = None,
    id_cop:            Optional[int] = None,
    zona:              Optional[str] = None,
    componente:        Optional[str] = None,
    id_revisor:        Optional[int] = None,
    id_asignador:      Optional[int] = None,
    estado_asignacion: Optional[int] = None,
    estado_objecion:   Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """Reporte ICS enriquecido con gestión SNE: asignación, objeción, Transmitools y DP."""
    _validar_rango_reporte(fecha_ini, fecha_fin)
    try:
        with GestionSneMonitor() as db:
            rows = db.reporte_sne_gestionado(
                fecha_ini=fecha_ini, fecha_fin=fecha_fin,
                id_linea=id_linea, id_cop=id_cop,
                zona=zona, componente=componente,
                id_revisor=id_revisor,
                id_asignador=id_asignador,
                estado_asignacion=estado_asignacion,
                estado_objecion=estado_objecion,
            )
    except QueryCanceled:
        raise _error_timeout_reporte()
    return {"ok": True, "total": len(rows), "data": rows}

@router_sne_monitor.get("/api/asignaciones")
def mon_asignaciones(
    fecha_ini:         Optional[str] = None,
    fecha_fin:         Optional[str] = None,
    id_linea:          Optional[int] = None,
    id_cop:            Optional[int] = None,
    zona:              Optional[str] = None,
    componente:        Optional[str] = None,
    estado_asignacion: Optional[int] = None,
    usuario_revisor:   Optional[int] = None,
    usuario_asigna:    Optional[int] = None,
    estado_objecion:   Optional[int] = None,
    usuario_objeta:    Optional[int] = None,
    fecha_inicio_dp:   Optional[str] = None,
    fecha_fin_dp:      Optional[str] = None,
    user_session: dict = Depends(_require_sesion),
):
    """
    Tabla agregada por Año/Mes/Ruta con métricas de estado de asignación/objeción
    y tiempos promedio de ciclo.
    """
    with GestionSneMonitor() as db:
        rows = db.monitor_tabla_asignaciones(
            fecha_ini=fecha_ini, fecha_fin=fecha_fin,
            id_linea=id_linea, id_cop=id_cop,
            zona=zona, componente=componente,
            estado_asignacion=estado_asignacion,
            usuario_revisor=usuario_revisor,
            usuario_asigna=usuario_asigna,
            estado_objecion=estado_objecion,
            usuario_objeta=usuario_objeta,
            fecha_inicio_dp=fecha_inicio_dp,
            fecha_fin_dp=fecha_fin_dp,
        )
    return {"ok": True, "total": len(rows), "data": rows}

# ══════════════════════════════════════════════════════════════════════════════
# MONITOR TRANSMITOOLS (global, sin filtro de usuario)
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/transmitools")
def mon_transmitools(
    fecha_ini:       Optional[str] = None,
    fecha_fin:       Optional[str] = None,
    id_linea:        Optional[int] = None,
    id_cop:          Optional[int] = None,
    zona:            Optional[str] = None,
    componente:      Optional[str] = None,
    usuario_revisor: Optional[int] = None,
    user_session: dict = Depends(_require_sesion),
):
    """ICS con estado_asignacion=1 y estado_objecion=1 para monitor global Transmitools."""
    with GestionSneMonitor() as db:
        data = db.monitor_transmitools(
            fecha_ini=fecha_ini, fecha_fin=fecha_fin,
            id_linea=id_linea, id_cop=id_cop,
            zona=zona, componente=componente,
            usuario_revisor=usuario_revisor,
        )
    return {"ok": True, "data": [dict(r) for r in data]}

# ══════════════════════════════════════════════════════════════════════════════
# TABLA PROCESAMIENTOS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/procesamientos")
def mon_procesamientos(
    fecha_ini: Optional[str] = Query(None),
    fecha_fin: Optional[str] = Query(None),
    user_session: dict = Depends(_require_sesion),
):
    """Logs de procesamientos de datos ejecutados por los workflows."""
    with GestionSneMonitor() as db:
        data = db.monitor_procesamientos(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
    return {"ok": True, "data": data}