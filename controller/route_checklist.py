from fastapi import APIRouter, Request, HTTPException, UploadFile, Depends, Body, Query,  File, Form
from starlette.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from datetime import datetime, time, date
from typing import List, Optional, Dict, Any
from decimal import Decimal
import io, json
import pandas as pd
from model.gestion_checklist import GestionChecklist
from model.gestion_preoperacional_motos import GestionPreoperacionalMotos
from model.gestion_checklist import Proceso_flota_asistencia

# Crear router
checklist_router = APIRouter()

# Configurar plantillas Jinja2
templates = Jinja2Templates(directory="./view")

# Función localmente para validar usuario
def get_user_session(req: Request):
    return req.session.get('user')

# --- RUTA PRINCIPAL CHECKLIST ---
# ----------------------------------------------------------------------
@checklist_router.get("/checklist", response_class=HTMLResponse)
def checklist(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(
        "checklist.html",
        {
            "request": req,
            "user_session": user_session,
            "componentes": {}
        }
    )

# --- REGISTRO Y PARAMETRIZACIÓN DE VEHÍCULOS ---
@checklist_router.get("/procesos")
def obtener_procesos_para_select():
    """
    Devuelve filas: [{id_proceso, proceso, subproceso}] ordenadas.
    El frontend arma el cascader (Proceso -> Subproceso) y usa id_proceso del subproceso.
    """
    gestion = GestionChecklist()
    try:
        filas = gestion.obtener_procesos_para_select()
        return JSONResponse(content=filas)
    finally:
        gestion.cerrar_conexion()

@checklist_router.get("/tipos_vehiculos")
def obtener_tipos_vehiculos():
    gestion = GestionChecklist()
    tipos = gestion.obtener_tipos_vehiculos()
    return JSONResponse(content=tipos)

@checklist_router.post("/vehiculos/crear")
def crear_vehiculo(request: Request, data: dict):
    gestion = GestionChecklist()
    resultado = gestion.crear_vehiculo(data)

    # si vienen km_bases, guardarlos por placa
    try:
        km_bases = data.get("km_bases")
        if resultado.get("placa") and isinstance(km_bases, list) and len(km_bases) > 0:
            gestion.upsert_km_base_batch_por_placa(resultado["placa"], km_bases)
            # agregamos info auxiliar
            resultado["km_bases_saved"] = len(km_bases)
    except Exception as e:
        # no rompemos el alta del vehículo si esto falla
        resultado["km_bases_error"] = str(e)

    return JSONResponse(content=resultado)

@checklist_router.get("/vehiculos")
def obtener_vehiculos():
    gestion = GestionChecklist()
    vehiculos = gestion.obtener_vehiculos()
    return JSONResponse(content=vehiculos)

@checklist_router.get("/vehiculos/{placa}")
def obtener_vehiculo(placa: str):
    gestion = GestionChecklist()
    vehiculo = gestion.obtener_vehiculo_por_placa(placa)
    
    if vehiculo:
        return JSONResponse(content=vehiculo)
    else:
        return JSONResponse(content={"error": "Vehículo no encontrado"}, status_code=404)

@checklist_router.put("/vehiculos/actualizar/{placa}")
def actualizar_vehiculo(placa: str, data: dict):
    gestion = GestionChecklist()
    resultado = gestion.actualizar_vehiculo(placa, data)
    return JSONResponse(content=resultado)

@checklist_router.put("/vehiculos/inactivar/{placa}/{nuevo_estado}")
def inactivar_vehiculo(placa: str, nuevo_estado: int):
    gestion = GestionChecklist()
    resultado = gestion.inactivar_vehiculo(placa, nuevo_estado)
    return JSONResponse(content=resultado)

# --- CONSULTA Y REGISTRO CHECKLIST ESTADO DEL VEHICULO ---
@checklist_router.get("/vehiculos/buscar/{query}")
def buscar_vehiculos(query: str, solo_moto: bool = Query(False)):
    gestion = GestionChecklist()
    vehiculos = gestion.buscar_vehiculos(query, solo_moto=solo_moto)
    return JSONResponse(content=vehiculos)

@checklist_router.get("/documentos")
def obtener_tipos_documentos():
    """Obtiene la lista de tipos de documentos desde la base de datos"""
    gestion = GestionChecklist()
    try:
        documentos = gestion.obtener_tipos_documentos()
        return JSONResponse(content=documentos)
    finally:
        gestion.cerrar_conexion()

@checklist_router.get("/componentes/{placa}")
def obtener_componentes_por_placa(placa: str):
    """Obtiene los componentes para un vehículo según su placa"""
    gestion = GestionChecklist()
    
    # Obtener el vehículo por la placa
    vehiculo = gestion.obtener_vehiculo_por_placa(placa)
    
    if not vehiculo:
        print(f"🚨 ERROR: Vehículo con placa {placa} no encontrado en la BD.")
        return JSONResponse(content={"error": "Vehículo no encontrado"}, status_code=404)

    id_tipo_vehiculo = vehiculo.get("id_tipo_vehiculo")
    #print(f" ID Tipo de Vehículo encontrado: {id_tipo_vehiculo}")

    if not id_tipo_vehiculo:
        print(f"🚨 ERROR: Tipo de vehículo no encontrado para {placa}")
        return JSONResponse(content={"error": "Tipo de vehículo no encontrado"}, status_code=400)

    # Obtener los componentes por tipo de vehículo
    componentes = gestion.obtener_componentes_por_tipo(id_tipo_vehiculo)
    # print(f"🔍 Componentes obtenidos para tipo {id_tipo_vehiculo}: {componentes}")

    if not isinstance(componentes, list) or len(componentes) == 0:
        print(f"🚨 ERROR: No hay componentes para el tipo de vehículo {id_tipo_vehiculo}")
        return JSONResponse(content={"error": "No hay componentes para este tipo de vehículo"}, status_code=404)

    # Agrupar los componentes por grupo
    componentes_agrupados = {}
    for componente in componentes:
        if not isinstance(componente, dict):
            print(f"🚨 ERROR: Componente en formato incorrecto: {componente}")
            return JSONResponse(content={"error": "Formato de componente incorrecto"}, status_code=500)

        grupo = componente["grupo"]
        if grupo not in componentes_agrupados:
            componentes_agrupados[grupo] = []

        componentes_agrupados[grupo].append({
            "id_componente": componente["id_componente"],
            "posicion": componente["posicion"],
            "componente": componente["componente"]
        })

    # print(f" Componentes agrupados listos para envío: {componentes_agrupados}")
    return JSONResponse(content=componentes_agrupados)

@checklist_router.get("/vehiculos/checklist/{placa}")
def obtener_checklist_vehiculo(placa: str):
    """Obtiene la información del vehículo y su checklist más reciente"""
    gestion = GestionChecklist()

    # 1. Buscar el vehículo
    vehiculo = gestion.obtener_vehiculo_por_placa(placa)
    if not vehiculo:
        return JSONResponse(content={"error": "Vehículo no encontrado"}, status_code=404)

    #print(f" Vehículo encontrado: {vehiculo}")

    # 2. Obtener el último checklist registrado
    checklist = gestion.obtener_ultimo_checklist(vehiculo["id_vehiculo"])
    
    # 3. Obtener documentos y detalles del checklist
    documentos = gestion.obtener_documentos_checklist(checklist["id_checklist"]) if checklist else []
    detalles = gestion.obtener_detalles_checklist(checklist["id_checklist"]) if checklist else []

    # Validar que documentos y detalles no sean None
    documentos = documentos if documentos is not None else []
    detalles = detalles if detalles is not None else []

    #print(f" Documentos encontrados: {documentos}")
    #print(f" Detalles encontrados: {detalles}")

    # 4. Obtener los componentes del vehículo
    componentes = gestion.obtener_componentes_por_tipo(vehiculo["id_tipo_vehiculo"])

    # print(f"Componentes agrupados listos para envío: {componentes}")

    # 5. Convertir valores `datetime` y `time` a `str`
    def convertir_a_str(obj):
        """Convierte datetime y time a string"""
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, time):
            return obj.strftime("%H:%M:%S")
        return obj

    if checklist:
        checklist = {key: convertir_a_str(value) for key, value in checklist.items()}

    return JSONResponse(content={
        "vehiculo": vehiculo,
        "checklist": checklist or {},
        "documentos": documentos,
        "detalles": detalles,
        "componentes": componentes
    })

# --- GUARDAR CHECKLIST DEL VEHICULO ---
class RegistroChecklist(BaseModel):
    id_vehiculo: int
    fecha_hora_registro: str
    inicio_turno: str
    fin_turno: str
    km_odometro: int
    observaciones_generales: Optional[str] = ""
    usuario_registro: str

class DocumentoChecklist(BaseModel):
    id_tipo_documento: int
    estado: bool
    observaciones: Optional[str] = ""

class DetalleChecklist(BaseModel):
    id_componente: int
    estado: bool
    observaciones: Optional[str] = ""

class ChecklistRequest(BaseModel):
    registro: RegistroChecklist
    documentos: List[DocumentoChecklist]
    detalles: List[DetalleChecklist]

@checklist_router.post("/guardar_checklist")
async def guardar_checklist(data: ChecklistRequest):
    #print("Datos recibidos:", data.dict())

    if not data.documentos or not data.detalles:
        raise HTTPException(status_code=400, detail="Debe completar documentos y estado del vehículo.")

    # Validación para evitar valores nulos en documentos y detalles
    if any(doc.id_tipo_documento is None for doc in data.documentos):
        raise HTTPException(status_code=400, detail="Error: Hay documentos sin un ID válido.")

    if any(det.id_componente is None for det in data.detalles):
        raise HTTPException(status_code=400, detail="Error: Hay componentes sin un ID válido.")

    gestion = GestionChecklist()
    try:
        # Aquí se valida el odómetro (si es <= al anterior, se lanza ValueError)
        id_checklist = gestion.guardar_checklist_registro(data.registro)
        if not id_checklist:
            raise HTTPException(status_code=500, detail="Error al guardar checklist_registro")

        # Guardar documentos
        for doc in data.documentos:
            gestion.guardar_checklist_documento(id_checklist, doc)

        # Guardar detalles
        for detalle in data.detalles:
            gestion.guardar_checklist_detalle(id_checklist, detalle)

        return {"message": "Checklist guardado correctamente", "id_checklist": id_checklist}

    except ValueError as e:
        # Mensaje claro hacia el frontend (se mostrará en data.detail)
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        # Cierra la conexión siempre
        gestion.cerrar_conexion()

# --- GESTIÓN FALLAS CHECKLIST DEL VEHICULO ---
@checklist_router.get("/fallas_vehiculos")
def obtener_vehiculos_con_fallas(user_session: dict = Depends(get_user_session)):
    """Retorna vehículos con al menos una falla registrada en su checklist, filtrados por flota del usuario (si aplica)."""
    gestion = GestionChecklist()
    uid = user_session.get("id") if user_session else None
    vehiculos = gestion.obtener_vehiculos_con_fallas(user_id=uid)
    return JSONResponse(content=vehiculos)

@checklist_router.get("/fallas_vehiculo/{placa}")
def obtener_detalles_falla_por_placa(placa: str, user_session: dict = Depends(get_user_session)):
    """Retorna todos los detalles de fallas por vehículo, restringido a la flota del usuario (si aplica)."""
    gestion = GestionChecklist()
    uid = user_session.get("id") if user_session else None
    data = gestion.obtener_detalle_falla_vehiculo(placa, user_id=uid)
    return JSONResponse(content=data)

# --- GENERACIÓN REPORTES ---
@checklist_router.get("/reportes/filtros")
def obtener_filtros_para_reportes():
    gestion = GestionChecklist()
    try:
        filtros = gestion.obtener_filtros_reportes()
        return JSONResponse(content=filtros)
    finally:
        gestion.cerrar_conexion()

# Reporte de Checklist        
@checklist_router.get("/reportes/fallas")
def consultar_reporte_fallas(
    # Soporta ambos: fecha única (compat) o rango:
    fecha: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    placa: Optional[str] = None,
    tipo_vehiculo: Optional[str] = None,
    marca: Optional[str] = None,
    estado: Optional[str] = None,
    usuario: Optional[str] = None
):
    # Normaliza: si solo mandan `fecha`, úsala como inicio/fin
    if not (fecha or (fecha_inicio and fecha_fin)):
        raise HTTPException(status_code=400, detail="Debes enviar fecha o fecha_inicio y fecha_fin.")

    if fecha and not (fecha_inicio and fecha_fin):
        fecha_inicio = fecha_fin = fecha

    # Valida rango máx. 5 días (inclusivo)
    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")

    if ff < fi:
        raise HTTPException(status_code=400, detail="La fecha final no puede ser menor que la inicial.")
    dias = (ff - fi).days + 1
    if dias > 15:
        raise HTTPException(status_code=400, detail="El rango no puede ser superior a 15 días.")

    gestion = GestionChecklist()
    try:
        resultados = gestion.consultar_datos_reporte(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            placa=placa,
            tipo_vehiculo=tipo_vehiculo,
            marca=marca,
            estado=estado,
            usuario=usuario
        )
        return JSONResponse(content=resultados)
    finally:
        gestion.cerrar_conexion()

@checklist_router.get("/reportes/fallas/exportar")
def exportar_reporte_fallas(
    # Soporta ambos: fecha única o rango
    fecha: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None,
    placa: str = "",
    tipo_vehiculo: str = "",
    marca: str = "",
    estado: str = "",
    usuario: str = "",
    formato: str = "xlsx"
):
    # Normaliza fechas
    if not (fecha or (fecha_inicio and fecha_fin)):
        raise HTTPException(status_code=400, detail="Debes enviar fecha o fecha_inicio y fecha_fin.")
    if fecha and not (fecha_inicio and fecha_fin):
        fecha_inicio = fecha_fin = fecha

    # Valida rango máx. 5 días (inclusivo)
    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")
    if ff < fi:
        raise HTTPException(status_code=400, detail="La fecha final no puede ser menor que la inicial.")
    dias = (ff - fi).days + 1
    if dias > 15:
        raise HTTPException(status_code=400, detail="El rango no puede ser superior a 15 días.")

    gestion = GestionChecklist()
    resultados = gestion.consultar_datos_reporte(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        placa=placa or None,
        tipo_vehiculo=tipo_vehiculo or None,
        marca=marca or None,
        estado=estado or None,
        usuario=usuario or None
    )

    if not resultados:
        return JSONResponse(content={"error": "No hay datos para exportar"}, status_code=404)

    # ---------- Estado como "OK"/"Falla" en export ----------
    def map_estado(row):
        v = row.get("estado_item")
        if v is True:  return "OK"
        if v is False: return "Falla"
        return ""

    # JSON
    if formato == "json":
        export_rows = []
        for r in resultados:
            rr = dict(r)
            rr["Estado"] = map_estado(rr)
            rr.pop("estado_item", None)
            export_rows.append(rr)
        json_bytes = json.dumps(export_rows, indent=2, ensure_ascii=False).encode("utf-8")
        output = io.BytesIO(json_bytes)
        return StreamingResponse(output, media_type="application/json", headers={
            "Content-Disposition": f"attachment; filename=Reporte_Fallas_{fecha_inicio}_a_{fecha_fin}.json"
        })

    # DataFrame (CSV/XLSX)
    df = pd.DataFrame(resultados)
    if "estado_item" in df.columns:
        df["Estado"] = df["estado_item"].map({True: "OK", False: "Falla"}).fillna("")
        df.drop(columns=["estado_item"], inplace=True)

    output = io.BytesIO()
    if formato == "csv":
        df.to_csv(output, index=False, sep=";", encoding="utf-8-sig")
        output.seek(0)
        return StreamingResponse(output, media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename=Reporte_Fallas_{fecha_inicio}_a_{fecha_fin}.csv"
        })

    # XLSX
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Fallas')
    except ImportError:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Fallas')

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=Reporte_Fallas_{fecha_inicio}_a_{fecha_fin}.xlsx"}
    )

# Reporte Preoperacionales
# --- Preoperacional (listar) ---
@checklist_router.get("/reportes/preop")
def consultar_reporte_preop(
    fecha_inicio: str,
    fecha_fin: str,
    placa: Optional[str] = None,
    tipo_vehiculo: Optional[str] = None,
    marca: Optional[str] = None,
    usuario: Optional[str] = None,
):
    # Validaciones de fechas (máximo 5 días, inclusivo)
    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")
    if ff < fi:
        raise HTTPException(status_code=400, detail="La fecha final no puede ser menor que la inicial.")
    if (ff - fi).days + 1 > 15:
        raise HTTPException(status_code=400, detail="El rango no puede ser superior a 15 días.")

    gestion = GestionPreoperacionalMotos()
    try:
        resultados = gestion.consultar_datos_reporte_preop(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            placa=placa,
            tipo_vehiculo=tipo_vehiculo,
            marca=marca,
            usuario=usuario,
        )
        # convierte Decimal/fecha a JSON-safe
        return JSONResponse(content=jsonable_encoder(resultados))
    finally:
        gestion.cerrar_conexion()

def _json_serialize(o):
    if isinstance(o, datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(o, date):
        return o.strftime("%Y-%m-%d")
    if isinstance(o, Decimal):
        return float(o)
    return str(o)

# --- Preoperacional (exportar) ---
@checklist_router.get("/reportes/preop/exportar")
def exportar_reporte_preop(
    fecha_inicio: str,
    fecha_fin: str,
    placa: str = "",
    tipo_vehiculo: str = "",
    marca: str = "",
    usuario: str = "",
    formato: str = "xlsx",
):
    # Validación de rango (máx. 5 días)
    try:
        fi = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        ff = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")
    if ff < fi:
        raise HTTPException(status_code=400, detail="La fecha final no puede ser menor que la inicial.")
    if (ff - fi).days + 1 > 15:
        raise HTTPException(status_code=400, detail="El rango no puede ser superior a 15 días.")

    gestion = GestionPreoperacionalMotos()
    try:
        resultados = gestion.consultar_datos_reporte_preop(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            placa=placa or None,
            tipo_vehiculo=tipo_vehiculo or None,
            marca=marca or None,
            usuario=usuario or None,
        )
    finally:
        try:
            gestion.cerrar_conexion()
        except Exception:
            pass

    if not resultados:
        return JSONResponse(content={"error": "No hay datos para exportar"}, status_code=404)

    # === Renombrado a los encabezados que ves en el frontend ===
    colmap = {
        "fecha_mantenimiento": "Fecha Mtto",
        "placa": "Placa",
        "tipo_vehiculo": "Tipo Vehículo",
        "marca": "Marca",
        "linea": "Línea",
        "modelo": "Modelo",
        "mantenimiento": "Mantenimiento",

        "odometro_dia":   "Odómetro del día",
        "odometro_corte": "Odómetro (acum.)",
        "base_periodo":   "Base período",
        "km_utilizados":  "Km Usados",
        "diferencia_optimo": "Diferencia",
        "porcentaje_uso":    "% Uso",
        "prox_mtto_km":      "Próximo Mtto",

        "observacion":    "Observación",
        "registrado_por": "Registrado por",
        "fecha_guardado": "Guardado",
    }

    # === Orden idéntico al frontend ===
    orden = [
        "Fecha Mtto", "Placa", "Tipo Vehículo", "Marca", "Línea", "Modelo", "Mantenimiento",
        "Odómetro del día", "Odómetro (acum.)", "Base período", "Km Usados",
        "Diferencia", "% Uso", "Próximo Mtto",
        "Observación", "Registrado por", "Guardado",
    ]

    # Construye DataFrame, renombra y reordena
    df = pd.DataFrame(resultados).rename(columns=colmap)
    cols_presentes = [c for c in orden if c in df.columns]
    if cols_presentes:
        df = df.reindex(columns=cols_presentes)

    # === Salidas ===
    output = io.BytesIO()

    if formato == "json":
        # Exporta con los mismos encabezados y en el mismo orden
        registros = df.to_dict(orient="records")
        json_bytes = json.dumps(
            registros,
            indent=2,
            ensure_ascii=False,
            default=_json_serialize   # convierte date/datetime/Decimal
        ).encode("utf-8")

        output.write(json_bytes)
        output.seek(0)
        return StreamingResponse(
            output,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=Reporte_Preop_{fecha_inicio}_a_{fecha_fin}.json"},
        )

    if formato == "csv":
        df.to_csv(output, index=False, sep=";", encoding="utf-8-sig")
        output.seek(0)
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=Reporte_Preop_{fecha_inicio}_a_{fecha_fin}.csv"},
        )

    # XLSX por defecto
    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Preoperacionales")
    except ImportError:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Preoperacionales")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=Reporte_Preop_{fecha_inicio}_a_{fecha_fin}.xlsx"},
    )

# --- RUTA PRINCIPAL PREOPERACIONALES MOTOS ---
# ----------------------------------------------------------------------
# Crear gestor y asegurar cierre de conexión
def get_gestor_preop_motos():
    gestor = GestionPreoperacionalMotos()
    try:
        yield gestor
    finally:
        gestor.cerrar_conexion()

def _estado_texto(e: int) -> str:
    return "Activo" if e == 1 else "Inactivo"

@checklist_router.get("/preoperacional-motos", response_class=JSONResponse)
def listar_parametros_preoperacional(
    incluir_inactivos: bool = True,
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    try:
        filas = gestor_preop_motos.listar_parametros(incluir_inactivos=incluir_inactivos)
        for f in filas:
            f["estado_texto"] = _estado_texto(f["estado"])
        return {"ok": True, "data": filas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@checklist_router.get("/preoperacional-motos/mantenimientos", response_class=JSONResponse)
def listar_mantenimientos_preoperacional(
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    try:
        mant = gestor_preop_motos.listar_mantenimientos_unicos()
        return {"ok": True, "data": mant}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@checklist_router.post("/preoperacional-motos", response_class=JSONResponse)
def crear_parametro_preoperacional(
    mantenimiento: str = Body(..., embed=True),
    km_optimo: int | None = Body(None, embed=True),
    estado: int = Body(1, embed=True),
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    res = gestor_preop_motos.crear_parametro(mantenimiento, km_optimo, estado)
    if "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
    data = res.get("data", {})
    if data:
        data["estado_texto"] = _estado_texto(data["estado"])
    return {"ok": True, **res}

@checklist_router.get("/preoperacional-motos/{id_param}", response_class=JSONResponse)
def obtener_parametro_preoperacional(
    id_param: int,
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    fila = gestor_preop_motos.obtener_parametro(id_param)
    if not fila:
        raise HTTPException(status_code=404, detail="No encontrado")
    fila["estado_texto"] = _estado_texto(fila["estado"])
    return {"ok": True, "data": fila}

@checklist_router.put("/preoperacional-motos/{id_param}", response_class=JSONResponse)
def actualizar_parametro_preoperacional(
    id_param: int,
    mantenimiento: str = Body(..., embed=True),
    km_optimo: int | None = Body(None, embed=True),
    estado: int = Body(1, embed=True),
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    res = gestor_preop_motos.actualizar_parametro(id_param, mantenimiento, km_optimo, estado)
    if "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
    data = res.get("data", {})
    if data:
        data["estado_texto"] = _estado_texto(data["estado"])
    return {"ok": True, **res}

@checklist_router.patch("/preoperacional-motos/{id_param}/estado", response_class=JSONResponse)
def cambiar_estado_parametro_preoperacional(
    id_param: int,
    estado: int = Body(..., embed=True),
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    res = gestor_preop_motos.cambiar_estado(id_param, estado)
    if "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
    data = res.get("data", {})
    if data:
        data["estado_texto"] = _estado_texto(data["estado"])
    return {"ok": True, **res}

# ----------------------------------------------------------------------
# -----Rutas REGISTRO Base de Kilometros por Vehiculo y manenimientos-----
class KmBaseItem(BaseModel):
    id_mtto: int
    km_base: Optional[int] = None  # permitir null/vacío
    fecha_mtto: Optional[date] = None

@checklist_router.get("/vehiculos/{placa}/km-base", response_class=JSONResponse)
def obtener_km_base_por_placa(placa: str):
    gestion = GestionChecklist()
    try:
        items = gestion.obtener_km_base_por_placa(placa)
        # items: [{ id_mtto, mantenimiento, km_optimo, km_base, fecha_mtto }]
        return JSONResponse(content={"ok": True, "placa": placa, "items": jsonable_encoder(items)})
    except Exception as e:
        return JSONResponse(content={"ok": False, "error": str(e)}, status_code=500)
    finally:
        gestion.cerrar_conexion()

@checklist_router.put("/vehiculos/{placa}/km-base", response_class=JSONResponse)
def upsert_km_base(placa: str, items: List[KmBaseItem]):
    gestion = GestionChecklist()
    try:
        # convierte a lista de dict simples
        payload = [{"id_mtto": it.id_mtto, "km_base": it.km_base, "fecha_mtto": it.fecha_mtto} for it in items]
        res = gestion.upsert_km_base_batch_por_placa(placa, payload)
        return JSONResponse(content={"ok": True, "updated": res.get("updated", 0)})
    except Exception as e:
        return JSONResponse(content={"ok": False, "error": str(e)}, status_code=500)
    finally:
        gestion.cerrar_conexion()

# ----------------------------------------------------------------------
# -----Rutas REGISTRO Preoperacional Motos-----

# Registro preoperacional (crear)
@checklist_router.post("/preoperacional-motos/registro", response_class=JSONResponse)
def crear_registro_preoperacional(
    placa: Optional[str] = Body(None, embed=True),
    id_vehiculo: Optional[int] = Body(None, embed=True),
    fecha_mantenimiento: str = Body(..., embed=True),  # "YYYY-MM-DD"
    mantenimiento: str = Body(..., embed=True),
    observacion: Optional[str] = Body(None, embed=True),
    usuario_registro: str = Body(..., embed=True),
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    """
    Crea un registro preoperacional para un vehículo (según id o placa).
    Devuelve { ok, message, data:{ id_preop, ... } }
    """
    try:
        if not id_vehiculo and not placa:
            raise HTTPException(status_code=400, detail="Debe enviar id_vehiculo o placa.")

        # Resolver id por placa si hace falta
        if not id_vehiculo and placa:
            vid = gestor_preop_motos.obtener_id_vehiculo_por_placa(placa)
            if not vid:
                raise HTTPException(status_code=404, detail="Vehículo no encontrado por placa.")
            id_vehiculo = vid

        res = gestor_preop_motos.crear_registro_preop(
            id_vehiculo=id_vehiculo,
            fecha_mantenimiento=fecha_mantenimiento,
            mantenimiento=mantenimiento,
            observacion=observacion,
            usuario_registro=usuario_registro
        )
        if "error" in res:
            raise HTTPException(status_code=400, detail=res["error"])

        # Preparar “carpetas” (blob virtual) YYYY/MM
        # --- al crear el registro ---
        gestor_preop_motos.crear_estructura_blob_preop(
            placa=placa or "",
            mantenimiento=mantenimiento,
            fechas=[fecha_mantenimiento]
        )

        return {"ok": True, **res}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Cargar adjuntos para registro preoperacional
@checklist_router.post("/preoperacional-motos/registro/{id_preop}/adjuntos/{anio}/{mes}", response_class=JSONResponse)
async def cargar_adjuntos_preop(
    id_preop: int,
    anio: str,
    mes: str,
    files: List[UploadFile] = File(...),
    fecha_mantenimiento: str = Form(...),
    placa: Optional[str] = Form(None),
    mantenimiento: Optional[str] = Form(None),
    gestor_preop_motos: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos)
):
    """
    Sube adjuntos al blob para un registro preoperacional.
    Espera:
      - files[] (UploadFile)
      - fecha_mantenimiento (YYYY-MM-DD)
      - placa y mantenimiento (opcionales – si no llegan se leen de BD)
    """
    try:
        if not files:
            raise HTTPException(status_code=400, detail="No se enviaron archivos.")

        # Si faltan placa/mantenimiento, consultarlos
        if not placa or not mantenimiento:
            row = gestor_preop_motos.obtener_resumen_preop(id_preop)
            if not row:
                raise HTTPException(status_code=404, detail="Registro preop no encontrado.")
            placa = placa or row["placa"]
            mantenimiento = mantenimiento or row["mantenimiento"]

        resultados = []
        for file in files:
            contenido = await file.read()
            meta_blob = gestor_preop_motos.subir_archivo_blob(
                placa=placa or "",
                mantenimiento=mantenimiento or "",
                anio=anio,
                mes=mes,
                fecha_mantenimiento=fecha_mantenimiento,
                file_stream=contenido,
                filename=file.filename
            )
            # Guardar metadatos en BD
            meta_bd = gestor_preop_motos.guardar_adjunto_bd(
                id_preop=id_preop,
                nombre_archivo=file.filename,
                ruta_blob=meta_blob["ruta_blob"],
                url_publica=meta_blob.get("url_publica"),
                content_type=file.content_type,
                tamano_bytes=len(contenido)
            )
            resultados.append({
                "archivo": file.filename,
                "ruta_blob": meta_blob["ruta_blob"],
                "url_publica": meta_blob.get("url_publica"),
                "ok": meta_bd.get("ok", False),
                "id_adjunto": meta_bd.get("id_adjunto")
            })

        return {"ok": True, "message": "Adjuntos cargados", "resultados": resultados}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"ok": False, "message": str(e)}, status_code=500)

# ----------------------------------------------------------------------    
# CONSULTA Y VISUALIZACIÓN de Preoperacional Motos-----

@checklist_router.get("/preop/vehiculos", response_class=JSONResponse)
async def listar_preop_vehiculos(
    placa: Optional[str] = Query(None),
    tipo: Optional[str] = Query(None),
    marca: Optional[str] = Query(None),
    linea: Optional[str] = Query(None),
    modelo: Optional[str] = Query(None),
    user_session: dict = Depends(get_user_session),
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    """
    Devuelve Motos filtradas por flota del usuario (si aplica)
    """
    try:
        uid = user_session.get("id") if user_session else None
        filas = gestor.listar_consolidado_vehiculos(
            placa=placa, tipo=tipo, marca=marca, linea=linea, modelo=modelo, user_id=uid
        )
        # convierte date/datetime/Decimal a tipos JSON‐safe
        return JSONResponse(content=jsonable_encoder(filas))
    except Exception as e:
        # Log opcional para depurar
        print("Error /preop/vehiculos:", e)
        return JSONResponse(content={"ok": False, "message": str(e)}, status_code=500)

# ---------------- Modal: grupos por placa ----------------
@checklist_router.get("/preop/historial/{placa}/grupos", response_class=JSONResponse)
async def grupos_por_placa(
    placa: str,
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    try:
        grupos = gestor.listar_grupos_por_vehiculo(placa)
        return JSONResponse(content=jsonable_encoder({"placa": placa, "grupos": grupos}))
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"ok": False, "message": str(e)}, status_code=500)

# ---------------- Modal: filas por grupo ----------------
@checklist_router.get(
    "/preop/historial/{placa}/grupo/{mantenimiento}", response_class=JSONResponse
)
async def registros_por_grupo(
    placa: str,
    mantenimiento: str,
    enriquecido: bool = Query(False),  # 👈 NUEVO parámetro opcional
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    """
    Registros del grupo 'mantenimiento' para la placa.
    - Modo clásico (enriquecido=false): columnas básicas (compatibilidad).
    - Modo enriquecido (enriquecido=true): agrega Odómetro/Diferencia/%Uso/Próximo Mtto.
    """
    try:
        if enriquecido:
            items = gestor.listar_registros_por_grupo_enriquecido(placa, mantenimiento)
        else:
            items = gestor.listar_registros_por_grupo(placa, mantenimiento)

        return JSONResponse(
            content=jsonable_encoder({"placa": placa, "mantenimiento": mantenimiento, "items": items})
        )
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"ok": False, "message": str(e)}, status_code=500)

# ---------------- Modal: adjuntos de un registro ----------------
@checklist_router.get(
    "/preoperacional-motos/registro/{id_preop}/adjuntos", response_class=JSONResponse
)
async def listar_adjuntos_preop(
    id_preop: int,
    minutos_url: int = Query(default=15, ge=1, le=120),
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    """
    Lista adjuntos del registro. Si el contenedor es privado, genera URL SAS temporal de lectura.
    """
    try:
        adj = gestor.listar_adjuntos_de_registro(id_preop, minutos_url=minutos_url)
        return JSONResponse(content=jsonable_encoder({"ok": True, "id_preop": id_preop, "adjuntos": adj}))
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"ok": False, "message": str(e)}, status_code=500)

@checklist_router.get("/preoperacional-motos/adjunto/{id_adjunto}/preview")
def preview_adjunto(
    id_adjunto: int,
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    meta = gestor.obtener_adjunto_por_id(id_adjunto)
    if not meta:
        raise HTTPException(status_code=404, detail="Adjunto no encontrado")

    downloader = gestor.abrir_stream_blob(meta["ruta_blob"])
    media = meta["content_type"] or "application/octet-stream"
    filename = meta["nombre_archivo"] or f"adjunto-{id_adjunto}"

    # Stream en chunks; Content-Disposition inline para previsualizar
    return StreamingResponse(
        downloader.chunks(),
        media_type=media,
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            # Opcional: cache corto en browser
            "Cache-Control": "private, max-age=60"
        },
    )

# ---------------- Descarga (attachment) de un adjunto ----------------
@checklist_router.get("/preoperacional-motos/adjunto/{id_adjunto}/download")
def download_adjunto(
    id_adjunto: int,
    gestor: GestionPreoperacionalMotos = Depends(get_gestor_preop_motos),
):
    meta = gestor.obtener_adjunto_por_id(id_adjunto)
    if not meta:
        raise HTTPException(status_code=404, detail="Adjunto no encontrado")

    downloader = gestor.abrir_stream_blob(meta["ruta_blob"])
    media = meta["content_type"] or "application/octet-stream"
    filename = meta["nombre_archivo"] or f"adjunto-{id_adjunto}"

    return StreamingResponse(
        downloader.chunks(),
        media_type=media,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store"
        },
    )
    