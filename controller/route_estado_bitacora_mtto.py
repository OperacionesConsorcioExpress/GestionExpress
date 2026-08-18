from fastapi import APIRouter, Request, Depends, Query, Form, File, UploadFile
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from datetime import date, datetime
from typing import Optional, List
from io import BytesIO
import json

from model.gestion_estado_bitacora_mtto import (
    GestionEstadoBitacoraMtto,
    ConfiguracionUsuarioBitacora,
    MENSAJE_SIN_COPS,
)

# Router del módulo
router_estado_bitacora_mtto = APIRouter()

# Plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Sesión de usuario (validación local)
def obtener_sesion_usuario(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────────────────────────────────────
#  PARAMETRIZACIÓN POR USUARIO
# ─────────────────────────────────────────────────────────────────────────────
def obtener_config_usuario(user_session: dict) -> dict:
    """COP habilitados y permiso de fecha retroactiva del usuario en sesión."""
    with ConfiguracionUsuarioBitacora() as cfg:
        return cfg.configuracion_usuario(user_session.get("id"))

def respuesta_sin_cops() -> JSONResponse:
    """El usuario entra a la pantalla pero no tiene COP parametrizados."""
    return JSONResponse(
        {"detail": MENSAJE_SIN_COPS, "sin_cops": True},
        status_code=403,
    )

# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS DE CONVERSIÓN (los formularios multipart llegan como texto)
# ─────────────────────────────────────────────────────────────────────────────
def _a_int(valor) -> Optional[int]:
    if valor is None:
        return None
    txt = str(valor).strip()
    if txt == "":
        return None
    try:
        return int(float(txt))
    except (TypeError, ValueError):
        return None

def _a_decimal(valor) -> Optional[float]:
    if valor is None:
        return None
    txt = str(valor).strip().replace("$", "").replace(" ", "")
    if txt == "":
        return None
    # Formato colombiano: 1.234.567,89 → 1234567.89
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        txt = txt.replace(".", "") if txt.count(".") > 1 else txt
    try:
        return float(txt)
    except (TypeError, ValueError):
        return None

def _a_fecha(valor) -> Optional[date]:
    if valor is None:
        return None
    txt = str(valor).strip()
    if txt == "":
        return None
    try:
        return datetime.strptime(txt[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────────────────────────────────────────
#  PÁGINA PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/estado_bitacora_mtto", response_class=HTMLResponse)
def estado_bitacora_mtto(req: Request, user_session: dict = Depends(obtener_sesion_usuario)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "estado_bitacora_mtto.html",
        {"request": req, "user_session": user_session},
    )

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS ESTÁTICOS + CATÁLOGOS DEL MÓDULO
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/filtros")
def api_filtros(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        config = obtener_config_usuario(user_session)
        cops = config["cops"]
        if not cops:
            return respuesta_sin_cops()

        with GestionEstadoBitacoraMtto() as g:
            catalogos = g.catalogos_todos()
            return {
                "tipologia":   [r["tipologia"]   for r in g.filtros_tipologia()],
                "linea":       [r["linea"]       for r in g.filtros_linea()],
                "combustible": [r["combustible"] for r in g.filtros_combustible()],
                "componente":  g.filtros_componente(cops_permitidos=cops),
                "permite_fecha_retroactiva": config["permite_fecha_retroactiva"],
                **catalogos,
            }
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS DEPENDIENTES: ZONAS
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/filtros/zonas")
def api_filtros_zonas(
    req: Request,
    id_componente: Optional[int] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        cops = obtener_config_usuario(user_session)["cops"]
        if not cops:
            return respuesta_sin_cops()
        with GestionEstadoBitacoraMtto() as g:
            return g.filtros_zona(id_componente, cops_permitidos=cops)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  FILTROS DEPENDIENTES: COPs
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/filtros/cops")
def api_filtros_cops(
    req: Request,
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        cops = obtener_config_usuario(user_session)["cops"]
        if not cops:
            return respuesta_sin_cops()
        with GestionEstadoBitacoraMtto() as g:
            return g.filtros_cop(id_componente, id_zona, cops_permitidos=cops)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  GRILLA PRINCIPAL  (flota + última gestión + km del mes en curso)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/bitacora")
def api_bitacora(
    req: Request,
    pagina: int = Query(1, ge=1),
    tamano: int = Query(5000, ge=1, le=5000),
    placa: Optional[str] = Query(None),
    no_interno: Optional[str] = Query(None),
    tipologia: Optional[str] = Query(None),
    linea: Optional[str] = Query(None),
    combustible: Optional[str] = Query(None),
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    id_cop: Optional[int] = Query(None),
    estado: Optional[int] = Query(None),
    id_sistema_funcional: Optional[int] = Query(None),
    id_causa_entrada_inoperativo: Optional[int] = Query(None),
    id_estado_pendiente_actual: Optional[int] = Query(None),
    id_ubicacion: Optional[int] = Query(None),
    id_estado_disponibilidad: Optional[int] = Query(None),
    inmovilizado_tmsa: Optional[str] = Query(None, description="Habilitado | Inmovilizado"),
    fecha_km_inicio: Optional[date] = Query(None),
    fecha_km_fin: Optional[date] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    if fecha_km_inicio and fecha_km_fin and fecha_km_fin < fecha_km_inicio:
        return JSONResponse({"detail": "fecha_km_fin no puede ser anterior a fecha_km_inicio"}, status_code=400)

    # Limpiar cadenas vacías → None para que el SQL las trate como sin filtro
    placa       = placa.strip()       or None if placa       else None
    no_interno  = no_interno.strip()  or None if no_interno  else None
    tipologia   = tipologia.strip()   or None if tipologia   else None
    linea       = linea.strip()       or None if linea       else None
    combustible = combustible.strip() or None if combustible else None

    inmovilizado_tmsa = (inmovilizado_tmsa or "").strip() or None
    if inmovilizado_tmsa and inmovilizado_tmsa not in ("Habilitado", "Inmovilizado"):
        return JSONResponse(
            {"detail": "inmovilizado_tmsa debe ser 'Habilitado' o 'Inmovilizado'"},
            status_code=400,
        )

    try:
        cops = obtener_config_usuario(user_session)["cops"]
        if not cops:
            return respuesta_sin_cops()

        with GestionEstadoBitacoraMtto() as g:
            data, total, rango_km = g.listar_bitacora(
                pagina=pagina,
                tamano=tamano,
                placa=placa,
                no_interno=no_interno,
                tipologia=tipologia,
                linea=linea,
                combustible=combustible,
                id_componente=id_componente,
                id_zona=id_zona,
                id_cop=id_cop,
                estado=estado,
                id_sistema_funcional=id_sistema_funcional,
                id_causa_entrada_inoperativo=id_causa_entrada_inoperativo,
                id_estado_pendiente_actual=id_estado_pendiente_actual,
                id_ubicacion=id_ubicacion,
                id_estado_disponibilidad=id_estado_disponibilidad,
                inmovilizado_tmsa=inmovilizado_tmsa,
                fecha_km_inicio=fecha_km_inicio,
                fecha_km_fin=fecha_km_fin,
                cops_permitidos=cops,
            )
        return {"data": [dict(r) for r in data], "total": total, "rango_km": rango_km}
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  BUSES INMOVILIZADOS — estado del cargue automático
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/inmovilizados/estado")
def api_inmovilizados_estado(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        from jobs.buses_inmovilizados import FUENTES

        with GestionEstadoBitacoraMtto() as g:
            estado = g.estado_cargue_inmovilizados()

        estado["fuentes"] = [
            {"origen": f["origen"], "etiqueta": f["etiqueta"]} for f in FUENTES
        ]
        return estado
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  BUSES INMOVILIZADOS — cargue manual (1 o las 4 fuentes)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.post("/api/estado_bitacora_mtto/inmovilizados/cargar")
def api_inmovilizados_cargar(
    req: Request,
    origenes: Optional[str] = Query(None, description="Orígenes separados por coma; vacío = las 4"),
    anio: Optional[int] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    """
    Ejecuta el mismo job del workflow horario dentro del servidor.
    Sirve tanto para el botón de actualización como para la carga manual
    de una o varias fuentes desde el modal.
    """
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    try:
        from jobs.buses_inmovilizados import ejecutar_job_buses_inmovilizados, ORIGENES_VALIDOS

        seleccion = [o.strip() for o in (origenes or "").split(",") if o.strip()] or None
        if seleccion:
            invalidos = [o for o in seleccion if o not in ORIGENES_VALIDOS]
            if invalidos:
                return JSONResponse(
                    {"detail": f"Orígenes no válidos: {', '.join(invalidos)}"},
                    status_code=400,
                )

        usuario = (user_session.get("username") or "app").strip()
        resumen = ejecutar_job_buses_inmovilizados(
            origenes=seleccion,
            anio=anio,
            ejecutado_por=usuario,
        )
        return resumen
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  BUSES INMOVILIZADOS — cargue de archivos subidos por el usuario
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.post("/api/estado_bitacora_mtto/inmovilizados/cargar-archivos")
def api_inmovilizados_cargar_archivos(
    req: Request,
    files: List[UploadFile] = File(default=[]),
    origenes: Optional[str] = Form(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    """
    Procesa uno o varios .xlsx cargados desde la pantalla (arrastrar y soltar).

    `origenes` es una lista separada por coma alineada por posición con `files`;
    un valor vacío deja que la fuente se deduzca del nombre del archivo.
    """
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    if not files:
        return JSONResponse({"detail": "No se recibieron archivos"}, status_code=400)

    try:
        from jobs.buses_inmovilizados import procesar_archivos_subidos, ORIGENES_VALIDOS

        lista_origenes = [o.strip() for o in (origenes or "").split(",")] if origenes else []

        archivos = []
        for i, archivo in enumerate(files):
            if not archivo or not archivo.filename:
                continue

            nombre = archivo.filename
            if not nombre.lower().endswith((".xlsx", ".xlsm")):
                return JSONResponse(
                    {"detail": f"'{nombre}' no es un archivo Excel (.xlsx)"},
                    status_code=400,
                )

            contenido = archivo.file.read()
            if not contenido:
                return JSONResponse({"detail": f"'{nombre}' está vacío"}, status_code=400)

            origen = lista_origenes[i] if i < len(lista_origenes) else ""
            if origen and origen not in ORIGENES_VALIDOS:
                return JSONResponse({"detail": f"Origen no válido: {origen}"}, status_code=400)

            archivos.append({"nombre": nombre, "contenido": contenido, "origen": origen or None})

        if not archivos:
            return JSONResponse({"detail": "No se recibieron archivos válidos"}, status_code=400)

        usuario = (user_session.get("username") or "app").strip()
        return procesar_archivos_subidos(archivos, ejecutado_por=usuario)

    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  REPORTE — estado de la flota a un día y hora de corte
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/reporte")
def api_reporte(
    req: Request,
    fecha: Optional[date] = Query(None),
    hora: Optional[str] = Query(None, description="Hora de corte HH:MM"),
    placa: Optional[str] = Query(None),
    no_interno: Optional[str] = Query(None),
    tipologia: Optional[str] = Query(None),
    linea: Optional[str] = Query(None),
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    id_cop: Optional[int] = Query(None),
    estado: Optional[int] = Query(None),
    id_estado_disponibilidad: Optional[int] = Query(None),
    inmovilizado_tmsa: Optional[str] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    inmovilizado_tmsa = (inmovilizado_tmsa or "").strip() or None
    if inmovilizado_tmsa and inmovilizado_tmsa not in ("Habilitado", "Inmovilizado"):
        return JSONResponse(
            {"detail": "inmovilizado_tmsa debe ser 'Habilitado' o 'Inmovilizado'"},
            status_code=400,
        )

    placa       = placa.strip()       or None if placa       else None
    no_interno  = no_interno.strip()  or None if no_interno  else None
    tipologia   = tipologia.strip()   or None if tipologia   else None
    linea       = linea.strip()       or None if linea       else None

    try:
        cops = obtener_config_usuario(user_session)["cops"]
        if not cops:
            return respuesta_sin_cops()

        with GestionEstadoBitacoraMtto() as g:
            data, corte = g.reporte_estado(
                fecha=fecha,
                hora=hora,
                placa=placa,
                no_interno=no_interno,
                tipologia=tipologia,
                linea=linea,
                id_componente=id_componente,
                id_zona=id_zona,
                id_cop=id_cop,
                estado=estado,
                id_estado_disponibilidad=id_estado_disponibilidad,
                inmovilizado_tmsa=inmovilizado_tmsa,
                cops_permitidos=cops,
            )
        return {"data": [dict(r) for r in data], "total": len(data), "corte": corte}
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

#  RESUMEN DE FLOTA — tarjetas de indicadores
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/resumen")
def api_resumen(
    req: Request,
    fecha_corte: Optional[date] = Query(None),
    hora: Optional[str] = Query(None, description="Hora de corte HH:MM"),
    id_componente: Optional[int] = Query(None),
    id_zona: Optional[int] = Query(None),
    id_cop: Optional[int] = Query(None),
    estado: Optional[int] = Query(None),
    dias_tendencia: int = Query(30, ge=1, le=180),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        cops = obtener_config_usuario(user_session)["cops"]
        if not cops:
            return respuesta_sin_cops()

        with GestionEstadoBitacoraMtto() as g:
            return g.resumen_flota(
                fecha_corte=fecha_corte,
                hora=hora,
                id_componente=id_componente,
                id_zona=id_zona,
                id_cop=id_cop,
                estado=estado,
                dias_tendencia=dias_tendencia,
                cops_permitidos=cops,
            )
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  MODAL DE GESTIÓN — ficha del bus + última gestión (precarga del formulario)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/bus/{id_bus}")
def api_bus(
    req: Request,
    id_bus: int,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBitacoraMtto() as g:
            bus = g.obtener_bus(id_bus)
            if not bus:
                return JSONResponse({"detail": "Bus no encontrado"}, status_code=404)
            ultima = g.ultima_gestion(id_bus)
            inmovilizacion = g.inmovilizacion_vigente(id_bus)
        return {
            "bus":            dict(bus),
            "ultima":         dict(ultima) if ultima else None,
            "inmovilizacion": dict(inmovilizacion) if inmovilizacion else None,
        }
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  HISTÓRICO DE CAMBIOS DEL BUS  (por defecto últimos 2 meses)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/historial/{id_bus}")
def api_historial(
    req: Request,
    id_bus: int,
    fecha_inicio: Optional[date] = Query(None),
    fecha_fin: Optional[date] = Query(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    if fecha_inicio and fecha_fin and fecha_fin < fecha_inicio:
        return JSONResponse({"detail": "fecha_fin no puede ser anterior a fecha_inicio"}, status_code=400)

    try:
        with GestionEstadoBitacoraMtto() as g:
            data, rango = g.historial_gestiones(id_bus, fecha_inicio, fecha_fin)
            inmovilizaciones = g.historial_inmovilizaciones(id_bus, fecha_inicio, fecha_fin)
        return {
            "data":             [dict(r) for r in data],
            "total":            len(data),
            "inmovilizaciones": [dict(r) for r in inmovilizaciones],
            "total_inmovilizaciones": len(inmovilizaciones),
            "rango":            rango,
        }
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  GUARDAR GESTIÓN  (inserta un registro nuevo + sube evidencias)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.post("/api/estado_bitacora_mtto/gestion", status_code=201)
async def api_guardar_gestion(
    req: Request,
    id_bus: int = Form(...),
    fecha: Optional[str] = Form(None),
    hora: Optional[str] = Form(None),
    novedad: Optional[str] = Form(None),
    id_sistema_funcional: Optional[str] = Form(None),
    id_causa_entrada_inoperativo: Optional[str] = Form(None),
    id_estado_pendiente_actual: Optional[str] = Form(None),
    id_ubicacion: Optional[str] = Form(None),
    id_estado_disponibilidad: Optional[str] = Form(None),
    ot_sap_pm: Optional[str] = Form(None),
    reserva_sap_mm: Optional[str] = Form(None),
    fecha_inoperativo_mtto: Optional[str] = Form(None),
    costo: Optional[str] = Form(None),
    fecha_cumplible_presentacion: Optional[str] = Form(None),
    fecha_ingreso_cein: Optional[str] = Form(None),
    archivos_previos: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    try:
        permite_retro = obtener_config_usuario(user_session)["permite_fecha_retroactiva"]
        fecha_gestion = _a_fecha(fecha)

        # Evidencias que ya venían de la gestión anterior y se conservan
        try:
            adjuntos = json.loads(archivos_previos) if archivos_previos else []
            if not isinstance(adjuntos, list):
                adjuntos = []
        except json.JSONDecodeError:
            adjuntos = []

        with GestionEstadoBitacoraMtto() as g:
            bus = g.obtener_bus(id_bus)
            if not bus:
                return JSONResponse({"detail": "Bus no encontrado"}, status_code=404)

            # Subida de nuevas evidencias → carpeta por placa / año / mes
            for archivo in files or []:
                if not archivo or not archivo.filename:
                    continue
                contenido = await archivo.read()
                if not contenido:
                    continue
                adjuntos.append(
                    g.subir_evidencia(
                        content=contenido,
                        filename=archivo.filename,
                        placa=bus.get("placa"),
                        fecha_ref=fecha_gestion,
                    )
                )

            registro = g.guardar_gestion(
                id_bus=id_bus,
                id_usuario_registra=user_session.get("id"),
                fecha=fecha_gestion,
                hora=(hora or "").strip() or None,
                novedad=novedad,
                id_sistema_funcional=_a_int(id_sistema_funcional),
                id_causa_entrada_inoperativo=_a_int(id_causa_entrada_inoperativo),
                id_estado_pendiente_actual=_a_int(id_estado_pendiente_actual),
                id_ubicacion=_a_int(id_ubicacion),
                id_estado_disponibilidad=_a_int(id_estado_disponibilidad),
                ot_sap_pm=_a_int(ot_sap_pm),
                reserva_sap_mm=_a_int(reserva_sap_mm),
                fecha_inoperativo_mtto=_a_fecha(fecha_inoperativo_mtto),
                costo=_a_decimal(costo),
                fecha_cumplible_presentacion=_a_fecha(fecha_cumplible_presentacion),
                fecha_ingreso_cein=_a_fecha(fecha_ingreso_cein),
                ruta_archivos=adjuntos,
                permite_fecha_retroactiva=permite_retro,
            )

        return {"detail": "Gestión guardada correctamente", "registro": dict(registro) if registro else None}
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  GESTIÓN MASIVA — misma gestión para varios buses
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.post("/api/estado_bitacora_mtto/gestion-masiva", status_code=201)
def api_guardar_gestion_masiva(
    req: Request,
    ids_bus: str = Form(..., description="IDs de bus separados por coma"),
    fecha: Optional[str] = Form(None),
    hora: Optional[str] = Form(None),
    novedad: Optional[str] = Form(None),
    id_sistema_funcional: Optional[str] = Form(None),
    id_causa_entrada_inoperativo: Optional[str] = Form(None),
    id_estado_pendiente_actual: Optional[str] = Form(None),
    id_ubicacion: Optional[str] = Form(None),
    id_estado_disponibilidad: Optional[str] = Form(None),
    ot_sap_pm: Optional[str] = Form(None),
    reserva_sap_mm: Optional[str] = Form(None),
    fecha_inoperativo_mtto: Optional[str] = Form(None),
    costo: Optional[str] = Form(None),
    fecha_cumplible_presentacion: Optional[str] = Form(None),
    fecha_ingreso_cein: Optional[str] = Form(None),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    ids = [_a_int(x) for x in (ids_bus or "").split(",")]
    ids = [i for i in ids if i]
    if not ids:
        return JSONResponse({"detail": "Seleccione al menos un bus"}, status_code=400)

    try:
        permite_retro = obtener_config_usuario(user_session)["permite_fecha_retroactiva"]

        with GestionEstadoBitacoraMtto() as g:
            resultado = g.guardar_gestion_masiva(
                ids_bus=ids,
                id_usuario_registra=user_session.get("id"),
                fecha=_a_fecha(fecha),
                hora=(hora or "").strip() or None,
                novedad=novedad,
                id_sistema_funcional=_a_int(id_sistema_funcional),
                id_causa_entrada_inoperativo=_a_int(id_causa_entrada_inoperativo),
                id_estado_pendiente_actual=_a_int(id_estado_pendiente_actual),
                id_ubicacion=_a_int(id_ubicacion),
                id_estado_disponibilidad=_a_int(id_estado_disponibilidad),
                ot_sap_pm=_a_int(ot_sap_pm),
                reserva_sap_mm=_a_int(reserva_sap_mm),
                fecha_inoperativo_mtto=_a_fecha(fecha_inoperativo_mtto),
                costo=_a_decimal(costo),
                fecha_cumplible_presentacion=_a_fecha(fecha_cumplible_presentacion),
                fecha_ingreso_cein=_a_fecha(fecha_ingreso_cein),
                permite_fecha_retroactiva=permite_retro,
            )
        return {
            "detail": f"Gestión aplicada a {resultado['guardados']} buses",
            **resultado,
        }
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  GESTIÓN MASIVA DETALLADA — una gestión distinta por bus (caso Inoperativo)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.post("/api/estado_bitacora_mtto/gestion-masiva-detalle", status_code=201)
async def api_guardar_gestiones_detalle(
    req: Request,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    """
    Recibe {"id_estado_disponibilidad": n, "gestiones": [ {...}, ... ]} en JSON.
    Cada elemento lleva los valores propios del bus tal como quedaron en la
    hoja de cálculo del modal.
    """
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)

    try:
        cuerpo = await req.json()
    except Exception:
        return JSONResponse({"detail": "Cuerpo JSON no válido"}, status_code=400)

    gestiones = cuerpo.get("gestiones") or []
    if not isinstance(gestiones, list) or not gestiones:
        return JSONResponse({"detail": "No se recibieron gestiones"}, status_code=400)

    # Normalización de tipos: la hoja de cálculo envía todo como texto
    filas = []
    for g in gestiones:
        filas.append({
            "id_bus": _a_int(g.get("id_bus")),
            "placa":  (g.get("placa") or "").strip() or None,
            "fecha":  _a_fecha(g.get("fecha")),
            "hora":   (g.get("hora") or "").strip()[:5] or None,
            "novedad": g.get("novedad"),
            "id_sistema_funcional":         _a_int(g.get("id_sistema_funcional")),
            "id_causa_entrada_inoperativo": _a_int(g.get("id_causa_entrada_inoperativo")),
            "id_estado_pendiente_actual":   _a_int(g.get("id_estado_pendiente_actual")),
            "id_ubicacion":                 _a_int(g.get("id_ubicacion")),
            "ot_sap_pm":                    _a_int(g.get("ot_sap_pm")),
            "reserva_sap_mm":               _a_int(g.get("reserva_sap_mm")),
            "costo":                        _a_decimal(g.get("costo")),
            "fecha_inoperativo_mtto":       _a_fecha(g.get("fecha_inoperativo_mtto")),
            "fecha_cumplible_presentacion": _a_fecha(g.get("fecha_cumplible_presentacion")),
            "fecha_ingreso_cein":           _a_fecha(g.get("fecha_ingreso_cein")),
        })

    try:
        config = obtener_config_usuario(user_session)
        if not config["cops"]:
            return respuesta_sin_cops()

        with GestionEstadoBitacoraMtto() as g:
            resultado = g.guardar_gestiones_detalle(
                gestiones=filas,
                id_usuario_registra=user_session.get("id"),
                id_estado_disponibilidad=_a_int(cuerpo.get("id_estado_disponibilidad")),
                permite_fecha_retroactiva=config["permite_fecha_retroactiva"],
                cops_permitidos=config["cops"],
            )
        return {
            "detail": f"Gestión guardada para {resultado['guardados']} buses",
            **resultado,
        }
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  EVIDENCIAS: PREVISUALIZAR / DESCARGAR
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/archivo")
def api_archivo(
    req: Request,
    ruta: str = Query(...),
    descargar: int = Query(0),
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBitacoraMtto() as g:
            contenido, content_type, nombre = g.descargar_evidencia(ruta)

        disposicion = "attachment" if descargar else "inline"
        return StreamingResponse(
            BytesIO(contenido),
            media_type=content_type,
            headers={"Content-Disposition": f'{disposicion}; filename="{nombre}"'},
        )
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
#  EVIDENCIAS: LISTADO COMPLETO POR BUS (todas las cargadas para la placa)
# ─────────────────────────────────────────────────────────────────────────────
@router_estado_bitacora_mtto.get("/api/estado_bitacora_mtto/evidencias/{id_bus}")
def api_evidencias(
    req: Request,
    id_bus: int,
    user_session: dict = Depends(obtener_sesion_usuario),
):
    if not user_session:
        return JSONResponse({"detail": "No autorizado"}, status_code=401)
    try:
        with GestionEstadoBitacoraMtto() as g:
            bus = g.obtener_bus(id_bus)
            if not bus:
                return JSONResponse({"detail": "Bus no encontrado"}, status_code=404)
            archivos = g.listar_evidencias_bus(bus.get("placa"))
        return {"placa": bus.get("placa"), "data": archivos, "total": len(archivos)}
    except Exception as exc:
        return JSONResponse({"detail": str(exc)}, status_code=500)
