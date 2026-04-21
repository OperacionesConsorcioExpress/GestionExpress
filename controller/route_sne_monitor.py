from fastapi import APIRouter, Request, HTTPException, Depends, Query
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from model.gestion_sne_monitor import GestionSneMonitor

router_sne_monitor = APIRouter(prefix="/monitor", tags=["sne_monitor"])
templates = Jinja2Templates(directory="./view")

def _sesion(req: Request):
    return req.session.get("user")

def _require_sesion(req: Request):
    user = req.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sesión no válida")
    return user

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
        result.append({
            "anio": r["anio"], "mes": r["mes"], "ruta_comercial": r["ruta_comercial"],
            "ids_asignados": a, "ids_revisados": v,
            "pct_cump_ids": round(v / a * 100, 1) if a else 0.0,
            "km_revision": round(kr, 3), "km_objetado": round(ko, 3),
            "pct_cump_km": round(ko / kr * 100, 1) if kr else 0.0,
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
    with GestionSneMonitor() as db:
        rows = db.reporte_sne_procesado(
            fecha_ini=fecha_ini, fecha_fin=fecha_fin,
            id_linea=id_linea, id_cop=id_cop,
            zona=zona, componente=componente,
        )
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
# TABLA PROCESAMIENTOS
# ══════════════════════════════════════════════════════════════════════════════
@router_sne_monitor.get("/api/procesamientos")
def mon_procesamientos(user_session: dict = Depends(_require_sesion)):
    """Logs de procesamientos de datos ejecutados por los workflows."""
    with GestionSneMonitor() as db:
        data = db.monitor_procesamientos()
    return {"ok": True, "data": data}