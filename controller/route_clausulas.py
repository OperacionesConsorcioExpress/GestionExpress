import os, re, json
from datetime import datetime, date
from typing import List
from fastapi import APIRouter, Request, Depends, HTTPException, File, UploadFile, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from model.gestion_clausulas import GestionClausulas
from jobs.job_juridico import TareasProgramadasJuridico

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "5000-juridica-y-riesgos-juridica-clausulas"

router_clausulas = APIRouter(tags=["juridico"])
templates        = Jinja2Templates(directory="./view")

# Tareas programadas (scheduler, no mantiene conexión DB)
tareas_juridico = TareasProgramadasJuridico()

# ─────────────────────────────────────────────
# Helper de sesión — igual que en main.py
# ─────────────────────────────────────────────
def get_user_session(req: Request):
    return req.session.get('user')

# ─────────────────────────────────────────────
# Helper serialización JSON — igual que en main.py
# ─────────────────────────────────────────────
def json_serial(obj):
    """Convierte objetos no serializables a JSON."""
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError("Type not serializable")

# =====================================================================
# PLANTILLA DE CARGUE
# =====================================================================
@router_clausulas.get("/plantilla_cargue_juridico")
async def descargar_plantilla_juridico():
    file_path = "./cargues/parametros_clausulas.xlsx"
    return FileResponse(path=file_path, filename="parametros_clausulas.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# =====================================================================
# PANTALLA PRINCIPAL JURÍDICO
# =====================================================================
@router_clausulas.get("/juridico", response_class=HTMLResponse)
def control_clausulas(req: Request, user_session: dict = Depends(get_user_session)):
    if not user_session:
        return RedirectResponse(url="/", status_code=302)
    
    # Obteniendo los datos desde la base de datos
    with GestionClausulas() as gestion:
        clausulas = gestion.obtener_clausulas()
        clausulas_con_estado = gestion.obtener_clausulas_con_entrega_estado()
        # Solo ajustamos las columnas "Entrega" y "Estado"
        clausulas_dict = {clausula["id"]: clausula for clausula in clausulas_con_estado}
        for clausula in clausulas:
            clausula["fecha_entrega_mas_reciente"] = clausulas_dict.get(clausula["id"], {}).get("fecha_entrega_mas_reciente", "Sin Fecha")
            clausula["estado_mas_reciente"] = clausulas_dict.get(clausula["id"], {}).get("estado_mas_reciente", "Sin Estado")
        etapas = gestion.obtener_opciones_etapas()
        clausulas_lista = gestion.obtener_opciones_clausulas()
        concesiones = gestion.obtener_opciones_concesion()
        contratos = gestion.obtener_opciones_contrato()
        tipos_clausula = gestion.obtener_opciones_tipo_clausula()
        procesos = gestion.obtener_opciones_procesos()
        frecuencias = gestion.obtener_opciones_frecuencias()
        responsables = gestion.obtener_opciones_responsables()
        responsable_entrega = gestion.obtener_opciones_responsables_clausulas()
        estados = gestion.obtener_opciones_estado()
    
    # Permisos de edición de la parametrización Juridica solo para el Rol " 1- Administrador y 3- Jurídico"
    es_editable = user_session.get("rol") in [1, 3]

    return templates.TemplateResponse("juridico.html", {
        "request": req, 
        "user_session": user_session,
        "clausulas": clausulas,
        "etapas": etapas,
        "clausulas_lista": clausulas_lista,
        "concesiones": concesiones,
        "contratos": contratos,
        "tipos_clausula": tipos_clausula,
        "procesos": procesos,
        "frecuencias": frecuencias,
        "responsables": responsables,
        "responsable_entrega": responsable_entrega,
        "estados": estados,
        "es_editable": es_editable
    })

# =====================================================================
# FILTROS Y BÚSQUEDAS
# =====================================================================
@router_clausulas.get("/obtener_subprocesos/{proceso}", response_class=JSONResponse)
def obtener_subprocesos(proceso: str):
    with GestionClausulas() as gestion:
        return gestion.obtener_opciones_subprocesos(proceso)

@router_clausulas.get("/filtrar_clausulas", response_class=HTMLResponse)
def filtrar_clausulas(req: Request, control: str = None, etapa: str = None, clausula: str = None, 
                        concesion: str = None, estado: str = None, responsable: str = None, proceso: str = None, subproceso: str = None,
                        user_session: dict = Depends(get_user_session)):

    with GestionClausulas() as gestion:
        # Mapear concesión a contrato antes de aplicar el filtro
        contrato = None
        if concesion and concesion != "Seleccionar...":
            contrato = gestion.obtener_contrato_por_concesion(concesion)

        clausulas_filtradas = gestion.obtener_clausulas_filtradas(
            control if control != "Seleccionar..." else None,
            etapa if etapa != "Seleccionar..." else None,
            clausula if clausula != "Seleccionar..." else None,
            contrato,
            estado if estado != "Seleccionar..." else None,
            responsable if responsable != "Seleccionar..." else None,
            proceso if proceso != "Seleccionar..." else None,
            subproceso if subproceso != "Seleccionar..." else None
        )

    return templates.TemplateResponse("juridico.html", {
        "request": req, 
        "clausulas": clausulas_filtradas,
        "user_session": user_session
    })

@router_clausulas.get("/obtener_id_proceso")
async def obtener_id_proceso(proceso: str, subproceso: str):
    try:
        with GestionClausulas() as gc:
            id_proceso = gc.obtener_id_proceso(proceso, subproceso)
        return JSONResponse(content={"success": True, "id_proceso": id_proceso})
    except Exception as e:
        return JSONResponse(content={"success": False, "message": str(e)}, status_code=400)

# =====================================================================
# CRUD CLÁUSULAS
# =====================================================================
@router_clausulas.post("/clausulas/nueva")
async def crear_clausula(req: Request, control: str = Form(...), etapa: str = Form(...), 
                        clausula: str = Form(...), modificaciones: str = Form(None), 
                        contrato: str = Form(...), tema: str = Form(...), subtema: str = Form(...), 
                        descripcion: str = Form(...), tipo: str = Form(...), norma: str = Form(None), 
                        consecuencia: str = Form(None), frecuencia: str = Form(...), 
                        periodo_control: str = Form(...), inicio_cumplimiento: str = Form(...), 
                        fin_cumplimiento: str = Form(...), observacion: str = Form(None), 
                        procesos_subprocesos: str = Form(...), 
                        responsable_entrega: str = Form(...), ruta_soporte: str = Form(None)):
    try:       
        # Validar campos obligatorios
        #print(f"Datos del formulario recibidos: {locals()}")
        campos_faltantes = [campo for campo, valor in locals().items() if valor == ""]
        if campos_faltantes:
            raise ValueError(f"Faltan los siguientes campos: {', '.join(campos_faltantes)}")

        # Crear la cláusula en la base de datos
        nueva_clausula = {
            "control": control,
            "etapa": etapa,
            "clausula": clausula,
            "modificacion": modificaciones,
            "contrato": contrato,
            "tema": tema,
            "subtema": subtema,
            "descripcion": descripcion,
            "tipo": tipo,
            "norma": norma,
            "consecuencia": consecuencia,
            "frecuencia": frecuencia,
            "periodo_control": periodo_control,
            "inicio_cumplimiento": inicio_cumplimiento,
            "fin_cumplimiento": fin_cumplimiento,
            "observacion": observacion,
            "responsable_entrega": responsable_entrega,
            "ruta_soporte": None  
        }
        
        with GestionClausulas() as gestion_clausulas:
            clausula_id = gestion_clausulas.crear_clausula(nueva_clausula)

            # Generar y registrar la ruta soporte
            ruta_soporte = f"5000-juridica-y-riesgos-juridica-clausulas/{clausula_id}-{clausula.replace(' ', '-')}-{contrato.replace(' ', '-')}"
            gestion_clausulas.registrar_ruta_soporte(clausula_id, ruta_soporte)

            # Procesar procesos_subprocesos como lista JSON
            procesos_subprocesos = json.loads(procesos_subprocesos)

            # Registrar cada id_proceso en la tabla auxiliar
            gestion_clausulas.registrar_clausula_proceso_subproceso(clausula_id, procesos_subprocesos)

            # Calcular fechas dinámicas y crear la estructura en Blob Storage
            fechas_entrega = gestion_clausulas.calcular_fechas_dinamicas(
                inicio_cumplimiento, fin_cumplimiento, frecuencia, periodo_control
            )
            gestion_clausulas.crear_estructura_blob_storage(
                clausula_id, clausula, contrato, [f["entrega"] for f in fechas_entrega]
            )

        return JSONResponse(content={"success": True, "message": "Cláusula creada exitosamente", "id_clausula": clausula_id})
    
    except Exception as e:
        print(f"Error al crear la cláusula: {e}")
        return JSONResponse(content={"success": False, "message": f"Error al crear la cláusula: {str(e)}"}, status_code=400)

@router_clausulas.get("/clausula/{id}", response_class=JSONResponse)
async def obtener_clausula(id: int):
    with GestionClausulas() as gc:
        clausula = gc.obtener_clausula_por_id(id)
        if not clausula:
            raise HTTPException(status_code=404, detail="Cláusula no encontrada")
        clausula["procesos_subprocesos"] = gc.obtener_procesos_subprocesos_por_clausula(id)
        return clausula

@router_clausulas.post("/clausula/{id}/actualizar", response_class=JSONResponse)
async def actualizar_clausula(id: int, request: Request):
    form_data = await request.form()
    clausula_data = {key: form_data.get(key) for key in form_data.keys()}

    # Extraer procesos y subprocesos del formulario
    procesos_subprocesos = clausula_data.pop("procesos_subprocesos", None)
    if procesos_subprocesos:
        procesos_subprocesos = json.loads(procesos_subprocesos)

    try:
        with GestionClausulas() as gc:
            gc.actualizar_clausula(id, clausula_data)
            if procesos_subprocesos:
                gc.actualizar_procesos_subprocesos(id, procesos_subprocesos)
        return {"message": "Cláusula actualizada correctamente"}
    except Exception as e:
        return {"error": str(e)}, 500

@router_clausulas.get("/api/fechas_dinamicas/{clausula_id}")
def obtener_fechas_dinamicas(clausula_id: int):
    with GestionClausulas() as gc:
        clausula = gc.obtener_clausula_por_id(clausula_id)
        if not clausula:
            raise HTTPException(status_code=404, detail="Cláusula no encontrada")
        fechas = gc.calcular_fechas_dinamicas(
            clausula["inicio_cumplimiento"],
            clausula["fin_cumplimiento"],
            clausula["frecuencia"],
            clausula["periodo_control"],
        )
    return JSONResponse(content=fechas)

# =====================================================================
# GESTIÓN DE FILAS Y ADJUNTOS
# =====================================================================
@router_clausulas.post("/gestion_clausula/{id_clausula}")
async def gestionar_filas_clausula(id_clausula: int, request: Request):
    data = await request.json()
    nuevas_filas = [fila for fila in data.get("filas", []) if not fila.get("id_gestion")]
    filas_existentes = [fila for fila in data.get("filas", []) if fila.get("id_gestion")]

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    try:
        # 1. Leer metadatos de DB (conexión se libera al salir del with)
        with GestionClausulas() as gestion:
            nombre_clausula = gestion.obtener_clausula_nombre(id_clausula)
            contrato = gestion.obtener_clausula_contrato(id_clausula)

        def normalizar_nombre(nombre):
            return re.sub(r"[^a-zA-Z0-9\-]", "", nombre.replace(" ", "-"))

        ruta_clausula = f"{normalizar_nombre(str(id_clausula))}-{normalizar_nombre(nombre_clausula)}-{normalizar_nombre(contrato)}"

        # 2. Procesar adjuntos en Blob Storage (fuera de la conexión DB)
        for fila in nuevas_filas:
            if "adjunto" in fila and fila["adjunto"]:
                try:
                    fecha_entrega = fila.get("fecha_entrega")
                    if not fecha_entrega:
                        raise ValueError("Fecha de entrega no encontrada en la fila.")
                    anio, mes, _ = fecha_entrega.split("-")
                    nombres_adjuntos = []
                    for adjunto in fila["adjunto"].split(", "):
                        ruta_archivo = f"{ruta_clausula}/{anio}/{mes}/{adjunto}"
                        blob_client = container_client.get_blob_client(ruta_archivo)
                        blob_client.upload_blob(b"Contenido de prueba", overwrite=True)
                        nombres_adjuntos.append(adjunto)
                    fila["adjunto"] = ", ".join(nombres_adjuntos)
                except Exception as e:
                    print(f"Error al procesar adjuntos para la fila: {e}")
                    fila["adjunto"] = None

        for fila in filas_existentes:
            if "adjunto" in fila and fila["adjunto"]:
                try:
                    fecha_entrega = fila.get("fecha_entrega")
                    if not fecha_entrega:
                        raise ValueError("Fecha de entrega no encontrada en la fila.")
                    anio, mes, _ = fecha_entrega.split("-")
                    nombres_adjuntos = []
                    for adjunto in fila["adjunto"].split(", "):
                        ruta_archivo = f"{ruta_clausula}/{anio}/{mes}/{adjunto}"
                        blob_client = container_client.get_blob_client(ruta_archivo)
                        blob_client.upload_blob(b"Contenido de prueba", overwrite=True)
                        nombres_adjuntos.append(adjunto)
                    fila["adjunto"] = ", ".join(nombres_adjuntos)
                except Exception as e:
                    print(f"Error al procesar adjuntos para la fila existente: {e}")
                    fila["adjunto"] = None

        # 3. Escribir en DB (nueva conexión, después del I/O lento)
        with GestionClausulas() as gestion:
            if nuevas_filas:
                gestion.insertar_filas_gestion_nuevas(id_clausula, nuevas_filas)
            if filas_existentes:
                gestion.actualizar_filas_gestion(filas_existentes)
            filas = gestion.obtener_filas_gestion_por_clausula(id_clausula)

        return JSONResponse(content={"message": "Gestión de filas completada.", "filas": filas})

    except Exception as e:
        print(f"Error al gestionar filas: {e}")
        return JSONResponse(content={"message": f"Error al gestionar filas: {e}"}, status_code=500)

@router_clausulas.get("/gestion_clausula/{id_clausula}")
def obtener_filas_clausula(id_clausula: int):
    with GestionClausulas() as gestion:
        filas = gestion.obtener_filas_gestion_por_clausula(id_clausula)

    if filas:
        fila_mas_reciente = max(filas, key=lambda x: datetime.strptime(x["fecha_entrega"], "%d/%m/%Y"))
        fecha_mas_reciente = fila_mas_reciente["fecha_entrega"]
        estado_mas_reciente = fila_mas_reciente["estado"]
    else:
        fecha_mas_reciente = "Sin Fecha"
        estado_mas_reciente = "Sin Estado"

    return JSONResponse(content={
        "filas": filas,
        "fecha_entrega_mas_reciente": fecha_mas_reciente,
        "estado_mas_reciente": estado_mas_reciente,
    })

@router_clausulas.get("/clausulas")
def obtener_clausulas_completas():
    with GestionClausulas() as gestion:
        clausulas = gestion.obtener_clausulas_con_entrega_estado()
    return JSONResponse(content=clausulas)

@router_clausulas.post("/clausulas/adjuntos/{id_clausula}/{anio}/{mes}")
async def cargar_adjuntos(id_clausula: int, anio: str, mes: str, files: List[UploadFile] = File(...), fecha_entrega: str = Form(...)):
    try:
        # Leer metadatos de DB antes de hacer I/O con Blob Storage
        with GestionClausulas() as gestion_clausulas:
            nombre_clausula = gestion_clausulas.obtener_clausula_nombre(id_clausula)
            contrato = gestion_clausulas.obtener_clausula_contrato(id_clausula)

        # Normalizar la ruta dentro del contenedor
        def normalizar_nombre(nombre):
            return re.sub(r"[^a-zA-Z0-9\-]", "", nombre.replace(" ", "-"))

        carpeta_principal = f"{normalizar_nombre(str(id_clausula))}-{normalizar_nombre(nombre_clausula)}-{normalizar_nombre(contrato)}"
        ruta_carpeta = f"{carpeta_principal}/{anio}/{mes}/"

        #print(f"Ruta destino de los adjuntos: {CONTAINER_NAME}/{ruta_carpeta}")

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Subir cada archivo al Blob Storage
        for file in files:
            # Renombrar el archivo agregando la fecha completa
            fecha_entrega_normalizada = fecha_entrega.replace("/", "-")  # Reemplazar "/" por "-" en la fecha
            nombre_archivo = f"{fecha_entrega_normalizada}_{file.filename}"  # Prefijar la fecha al nombre del archivo
            ruta_archivo = f"{ruta_carpeta}{nombre_archivo}" # Mantener la ruta original
            
            # Subir el archivo
            blob_client = container_client.get_blob_client(ruta_archivo)
            blob_client.upload_blob(await file.read(), overwrite=True)

        return JSONResponse(content={"success": True, "message": "Adjuntos cargados correctamente"})
    except Exception as e:
        print(f"Error al cargar adjuntos: {e}")
        return JSONResponse(content={"success": False, "message": str(e)}, status_code=500)

# =====================================================================
# RESPONSABLES Y COPIA DE CORREOS
# =====================================================================
@router_clausulas.get("/obtener_responsables", response_class=JSONResponse)
async def obtener_responsables(req: Request):
    with GestionClausulas() as gestion:
        return {"responsables": gestion.obtener_responsables()}

@router_clausulas.get("/clausula/{id_clausula}/copia", response_class=JSONResponse)
async def obtener_copia(req: Request, id_clausula: int):
    with GestionClausulas() as gestion:
        return gestion.obtener_copia_correos(id_clausula)

@router_clausulas.post("/clausula/{id_clausula}/copia", response_class=JSONResponse)
async def actualizar_copia(req: Request, id_clausula: int):
    data = await req.json()
    responsables_copia = data.get("responsables_copia", [])
    if not responsables_copia:
        return {"success": False, "message": "No se enviaron responsables válidos."}, 400
    try:
        with GestionClausulas() as gestion:
            gestion.actualizar_copia_correos(id_clausula, responsables_copia)
        return {"success": True, "message": "Responsables en copia actualizados correctamente."}
    except ValueError as e:
        print(f"Error al actualizar responsables en copia: {str(e)}")
        return {"success": False, "message": str(e)}, 400

# =====================================================================
# TAREAS PROGRAMADAS
# =====================================================================
@router_clausulas.get("/jobs/calcular_fechas", response_class=JSONResponse)
def ejecutar_calculo_fechas():
    """Ejecuta manualmente el cálculo y actualización de fechas dinámicas."""
    try:
        print("Iniciando job de cálculo de fechas dinámicas.")
        tareas_juridico.calcular_y_actualizar_fechas_dinamicas()
        return {"message": "Cálculo y actualización de fechas dinámicas ejecutado exitosamente."}
    except Exception as e:
        print(f"Error al ejecutar el job: {e}")
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@router_clausulas.get("/jobs/sincronizar_estados", response_class=JSONResponse)
def ejecutar_sincronizacion_estados():
    """Ejecuta manualmente la sincronización de estados dinámicos de las filas."""
    try:
        tareas_juridico.sincronizar_estados_filas_gestion()
        return {"message": "Sincronización de estados ejecutada exitosamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@router_clausulas.get("/jobs/envio_correos_recordatorio", response_class=JSONResponse)
def envio_correos_recordatorio():
    """Prueba manual para enviar correos de recordatorios."""
    try:
        tareas_juridico.tarea_diaria_recordatorio()
        return {"message": "Correos de recordatorio enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@router_clausulas.get("/jobs/envio_correos_incumplimiento", response_class=JSONResponse)
def envio_correos_incumplimiento():
    """Prueba manual para enviar correos de incumplimiento."""
    try:
        tareas_juridico.tarea_semanal_incumplimientos()
        return {"message": "Correos de incumplimiento enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

@router_clausulas.get("/jobs/envio_correos_incumplimiento_direccion", response_class=JSONResponse)
def envio_correos_incumplimiento_direccion():
    """Prueba manual para enviar correos de incumplimiento a dirección."""
    try:
        tareas_juridico.tarea_semanal_incumplimientos()
        return {"message": "Correos de incumplimiento enviados correctamente."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error: {str(e)}"})

# =====================================================================
# REPORTES JURÍDICO
# =====================================================================
@router_clausulas.get("/filtros_reportes")
def filtros_reportes():
    try:
        with GestionClausulas() as gestion:
            filtros = gestion.obtener_filtros_disponibles()
        if "error" in filtros:
            return JSONResponse(status_code=500, content=filtros)
        return JSONResponse(content=filtros)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router_clausulas.get("/consulta_reportes")
def consulta_reportes(
    id: str = None, control: str = None, clausula: str = None, etapa: str = None,
    contrato_concesion: str = None, tipo_clausula: str = None, frecuencia: str = None,
    responsable: str = None, fecha_entrega: str = None, plan_accion: str = None,
    estado: str = None, registrado_por: str = None
):
    try:
        filtros = {
            "id": id, "control": control, "clausula": clausula, "etapa": etapa,
            "contrato_concesion": contrato_concesion, "tipo_clausula": tipo_clausula,
            "frecuencia": frecuencia, "responsable": responsable, "fecha_entrega": fecha_entrega,
            "plan_accion": plan_accion, "estado": estado, "registrado_por": registrado_por
        }
        
        # Filtrar solo los valores que no son None ni vacíos
        filtros = {k: v for k, v in filtros.items() if v}

        #print(f"Filtros enviados a la consulta: {filtros}")  # 🔍 Log para depuración

        with GestionClausulas() as gestion:
            data = gestion.obtener_reporte_clausulas(**filtros)

        if isinstance(data, dict) and "error" in data:
            print(f"Error en consulta_reportes: {data['error']}")
            return JSONResponse(status_code=500, content={"error": data["error"]})

        if not isinstance(data, list):
            print("Error: La respuesta no es una lista.")
            return JSONResponse(content=[])

        return JSONResponse(content=data)

    except Exception as e:
        print(f"Error general en consulta_reportes: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router_clausulas.get("/descargar_reporte")
def descargar_reporte(formato: str, request: Request):
    try:
        filtros = dict(request.query_params)
        filtros.pop("formato", None)  # Eliminar el formato de los filtros

        with GestionClausulas() as gestion:
            file_path, content_type, filename = gestion.exportar_reporte(formato, **filtros)

        if not file_path or not os.path.exists(file_path):
            return JSONResponse(status_code=400, content={"error": "No hay datos para exportar"})

        return FileResponse(path=file_path, filename=filename, media_type=content_type)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})