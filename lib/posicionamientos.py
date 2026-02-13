# ============================================================
#  JOB POSICIONAMIENTOS - PROCESO DIARIO CON LOG EN POSTGRESQL
# ============================================================
import os, io, re, math, time, traceback
from datetime import date, datetime, timedelta, timezone
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values
from azure.storage.blob import BlobServiceClient
# ============================================================
# 0) CONFIG AZURE BLOB
# ============================================================
CONTENEDOR = "transmitools"
PREFIJO_BASE = "posicionamientos/"

def obtener_cliente_contenedor():
    cadena_conexion = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not cadena_conexion:
        raise ValueError("No existe la variable AZURE_STORAGE_CONNECTION_STRING")

    servicio_blob = BlobServiceClient.from_connection_string(cadena_conexion)
    return servicio_blob.get_container_client(CONTENEDOR)

# ============================================================
# 1) ESTRUCTURAS DE APOYO
# ============================================================
CATALOGO_ESTADOS = pl.DataFrame({
    "estado_localizacion": [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14],
    "nombre_estado": [
        "No asignado", "Apagado", "Incorporacion", "Retorno", "Localizado en linea",
        "Fuera de linea", "Viaje en vacio", "Desvio", "Averia", "Ubicado en patio",
        "Ubicado en la estacion/parada", "Fuera de ruta (por posicion)", "Ubicado en poligono de cabecera",
    ]
})

MAPEO_COLUMNAS = {
    "eventdatetime": "fecha_evento_utc",
    "driverregistrnum": "operador",
    "lineid": "id_linea",
    "routeid": "id_sublinea",
    "lineservid": "id_viaje",
    "loctypecd": "estado_localizacion",
    "driverservid": "servcond",
    "vehservid": "servbus",
    "vehregistrnum": "movil_bus",
    "longitud": "longitud",
    "latitud": "latitud",
    "routeoffsetvalue": "offset",
    "nextnodeid": "id_sig_nodo",
    "nodeid": "id_nodo",
    "servtripseq": "secuencia_viaje",
    "sectionid": "id_seccion",
    "sectionoffsetvalue": "posicion",
}

R_TIERRA_M = 6371000.0 # Radio promedio de la Tierra en metros
FACTOR_AJUSTE = 0.02 # Ajuste imprecisiones GPS (reduce distancia calculada en un 2%)
VEL_MAX_KMH = 120.0 # Velocidad máxima para un bus en km/h; si se supera = "VEL_IRREAL"
DIST_MAX_M = 5000.0 # Distancia máxima entre puntos consecutivos para un bus; si se supera ="SALTO_GPS"

COLUMNAS_SALIDA = [
        "fecha_evento",
        "hora",
        "estado_localizacion",
        "nombre_estado",
        "servbus",
        "movil_bus",
        "longitud",
        "latitud",
        "id_linea",
        "id_sublinea",
        "id_viaje",
        "posicion",
        "DIST (m)",
        "TIEMPO (s)",
        "VEL (m/s)",
        "DIST FINAL (m)",
]

# ============================================================
# 2) AZURE: LISTAR/LEER PARQUET
# ============================================================
def carpeta_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def listar_parquets_por_carpeta(cliente_contenedor, carpeta_utc: str) -> pl.DataFrame:
    prefijo = f"{PREFIJO_BASE}{carpeta_utc}/"
    filas = []
    for blob in cliente_contenedor.list_blobs(name_starts_with=prefijo):
        if blob.name.lower().endswith(".parquet"):
            archivo = blob.name.split("/")[-1]
            filas.append({"carpeta": carpeta_utc, "ruta_blob": blob.name, "archivo": archivo})
    return pl.DataFrame(filas) if filas else pl.DataFrame({"carpeta": [], "ruta_blob": [], "archivo": []})

def convertir_extension_arrow(tabla_arrow: pa.Table) -> pa.Table:
    columnas_nuevas = []
    nombres = tabla_arrow.schema.names
    for nombre in nombres:
        columna = tabla_arrow[nombre]
        tipo = columna.type
        if isinstance(tipo, pa.ExtensionType):
            tipo_base = tipo.storage_type
            try:
                col_conv = columna.cast(tipo_base)
            except Exception:
                try:
                    col_conv = columna.cast(pa.timestamp("us"))
                except Exception:
                    col_conv = columna.cast(pa.string())
            columnas_nuevas.append(col_conv)
        else:
            columnas_nuevas.append(columna)
    return pa.table(columnas_nuevas, names=nombres)

def leer_parquet_seguro(cliente_contenedor, ruta_blob: str) -> pl.DataFrame:
    datos = cliente_contenedor.get_blob_client(ruta_blob).download_blob().readall()
    tabla_arrow = pq.read_table(io.BytesIO(datos))
    tabla_limpia = convertir_extension_arrow(tabla_arrow)
    return pl.from_arrow(tabla_limpia)

# ============================================================
# 3) PIPELINE TRANSFORMACIÓN
# ============================================================
def limpiar_encabezados(df: pl.DataFrame) -> pl.DataFrame:
    nuevas = []
    for c in df.columns:
        c2 = re.sub(r"[\x00-\x1F\x7F]", "", c)
        c2 = re.sub(r"^Schema", "", c2)
        c2 = c2.strip().lower().replace(" ", "_")
        nuevas.append(c2)
    return df.rename(dict(zip(df.columns, nuevas)))

def aplicar_mapeo(df: pl.DataFrame) -> pl.DataFrame:
    mapping = {k: v for k, v in MAPEO_COLUMNAS.items() if k in df.columns}
    return df.rename(mapping)

def normalizar_fecha_evento(df: pl.DataFrame) -> pl.DataFrame:
    if "fecha_evento_utc" not in df.columns:
        return df
    return (
        df
        .with_columns(
            pl.col("fecha_evento_utc").cast(pl.Utf8).str.strptime(pl.Datetime, strict=False).alias("fecha_evento")
        )
        .with_columns(pl.col("fecha_evento").dt.strftime("%H:%M:%S").alias("hora"))
        .drop("fecha_evento_utc")
    )

def ajustar_movil_bus(df: pl.DataFrame) -> pl.DataFrame:
    if "movil_bus" not in df.columns:
        return df
    s = pl.col("movil_bus").cast(pl.Utf8).str.replace_all(r"\D", "")
    return (
        df
        .with_columns(pl.col("movil_bus").cast(pl.Utf8).alias("movil_bus_original"))
        .with_columns(
            pl.when(s.str.len_chars() == 6)
            .then(pl.lit("Z") + s.str.slice(0, 2) + pl.lit("-") + s.str.slice(2, 4))
            .when((s.str.len_chars() == 5) & (s.str.slice(0, 2).is_in(["10", "11"])))
            .then(pl.lit("D") + s.str.slice(2, 3))
            .when((s.str.len_chars() == 5) & (s.str.slice(0, 2) == "14"))
            .then(pl.lit("N") + s.str.slice(2, 3).str.zfill(4))
            .otherwise(s)
            .alias("movil_bus")
        )
    )

def agregar_nombre_estado(df: pl.DataFrame) -> pl.DataFrame:
    if "estado_localizacion" not in df.columns:
        return df
    df2 = df.with_columns(pl.col("estado_localizacion").cast(pl.Int64, strict=False))
    return df2.join(CATALOGO_ESTADOS, on="estado_localizacion", how="left")

def pipeline_transformacion(df_raw: pl.DataFrame) -> pl.DataFrame:
    df = limpiar_encabezados(df_raw)
    df = aplicar_mapeo(df)
    df = normalizar_fecha_evento(df)
    df = ajustar_movil_bus(df)
    df = agregar_nombre_estado(df)
    return df

# ============================================================
# 4) HAVERSINE + MÉTRICAS
# ============================================================
def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    try:
        lat1 = float(lat1); lon1 = float(lon1); lat2 = float(lat2); lon2 = float(lon2)
    except Exception:
        return 0.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * (math.sin(dlmb / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R_TIERRA_M * c

def calcular_metricas_geodesicas(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("latitud").cast(pl.Float64, strict=False),
        pl.col("longitud").cast(pl.Float64, strict=False),
    ])

    df = df.sort(["movil_bus", "fecha_evento"])

    df = df.with_columns([
        pl.col("latitud").shift(1).over("movil_bus").alias("lat_prev"),
        pl.col("longitud").shift(1).over("movil_bus").alias("lon_prev"),
        pl.col("fecha_evento").shift(1).over("movil_bus").alias("t_prev"),
    ])

    df = df.with_columns(
        (pl.col("fecha_evento") - pl.col("t_prev")).dt.total_seconds().alias("TIEMPO (s)")
    ).with_columns(
        pl.when(pl.col("t_prev").is_null()).then(pl.lit(0.0)).otherwise(pl.col("TIEMPO (s)")).alias("TIEMPO (s)")
    )

    df = df.with_columns(
        pl.struct(["lat_prev", "lon_prev", "latitud", "longitud"]).map_elements(
            lambda r: _haversine_m(r["lat_prev"], r["lon_prev"], r["latitud"], r["longitud"]),
            return_dtype=pl.Float64
        ).alias("DIST (m)")
    )

    df = df.with_columns(
        pl.when(pl.col("TIEMPO (s)") <= 0).then(pl.lit(0.0)).otherwise(pl.col("DIST (m)") / pl.col("TIEMPO (s)"))
        .alias("VEL (m/s)")
    )

    df = df.with_columns((pl.col("DIST (m)") * (1.0 - FACTOR_AJUSTE)).alias("DIST FINAL (m)"))

    # (validaciones calculadas pero NO exportadas)
    lat = pl.col("latitud")
    lon = pl.col("longitud")
    lat_ok = lat.is_between(-90.0, 90.0)
    lon_ok = lon.is_between(-180.0, 180.0)

    df = df.with_columns(
        pl.when(lat.is_null() | lon.is_null()).then(pl.lit("ERROR_NULL_COORD"))
        .when(~lat_ok | ~lon_ok).then(pl.lit("ERROR_RANGO_COORD"))
        .when(pl.col("TIEMPO (s)") < 0).then(pl.lit("ERROR_TIEMPO_NEG"))
        .otherwise(pl.lit("OK"))
        .alias("VALIDACION")
    )

    vel_kmh = pl.col("VEL (m/s)") * 3.6
    df = df.with_columns(
        pl.when(vel_kmh > VEL_MAX_KMH).then(pl.lit("VEL_IRREAL"))
        .when(pl.col("DIST (m)") > DIST_MAX_M).then(pl.lit("SALTO_GPS"))
        .otherwise(pl.lit("OK"))
        .alias("VALIDACION2")
    )

    return df.drop(["lat_prev", "lon_prev", "t_prev"])

def seleccionar_columnas_salida(df: pl.DataFrame) -> pl.DataFrame:
    cols_presentes = [c for c in COLUMNAS_SALIDA if c in df.columns]
    return df.select(cols_presentes)

def calcular_km_recorrido_bus(posicionamientos: pl.DataFrame) -> pl.DataFrame:
    if posicionamientos.is_empty():
        return pl.DataFrame({"fecha": [], "movil_bus": [], "DIST FINAL (km)": []})

    return (
        posicionamientos
        .with_columns(pl.col("fecha_evento").dt.date().cast(pl.Date).alias("fecha"))
        .group_by(["fecha", "movil_bus"])
        .agg((pl.col("DIST FINAL (m)").sum() / 1000.0).alias("DIST FINAL (km)"))
        .sort(["fecha", "movil_bus"])
    )

# ============================================================
# 5) PROCESO DE UN DÍA (retorna dfs + métricas)
# ============================================================
def procesar_dia_completo(dia: date, verbose: bool = False):
    cliente_contenedor = obtener_cliente_contenedor()
    carpeta = carpeta_yyyymmdd(dia)

    df_archivos = listar_parquets_por_carpeta(cliente_contenedor, carpeta).sort("archivo")
    if df_archivos.is_empty():
        return None, None, {
            "estado": "sin_archivos",
            "archivos_total": 0,
            "archivos_ok": 0,
            "archivos_error": 0,
            "errores": 0,
        }

    total = df_archivos.height
    dfs = []
    errores = 0

    for i in range(total):
        ruta_blob = df_archivos[i, "ruta_blob"]
        archivo = df_archivos[i, "archivo"]
        try:
            df_raw = leer_parquet_seguro(cliente_contenedor, ruta_blob)
            df_proc = pipeline_transformacion(df_raw)
            dfs.append(df_proc)

            if verbose and (i % 50 == 0):
                print(f"    {i+1}/{total} OK | {archivo} | filas: {df_proc.height:,}")

        except Exception:
            errores += 1
            if verbose:
                print(f"Error archivo {archivo}:\n{traceback.format_exc()}")
            continue

    if not dfs:
        return None, None, {
            "estado": "error",
            "archivos_total": total,
            "archivos_ok": 0,
            "archivos_error": total,
            "errores": errores,
        }

    df_final = pl.concat(dfs, how="vertical_relaxed")
    df_final = calcular_metricas_geodesicas(df_final)
    df_final = df_final.sort(["fecha_evento", "movil_bus"])

    posicionamientos = seleccionar_columnas_salida(df_final)
    km_recorrido_bus = calcular_km_recorrido_bus(posicionamientos)

    meta = {
        "estado": "ok",
        "archivos_total": total,
        "archivos_ok": len(dfs),
        "archivos_error": errores,
        "errores": errores,
        "registros_pos": posicionamientos.height,
        "filas_km": km_recorrido_bus.height,
    }
    return posicionamientos, km_recorrido_bus, meta

# ============================================================
# 6) POSTGRES: CONEXIÓN + LOG + CARGA POR LOTES
# ============================================================
PG_SCHEMA = "config"
PG_TABLE_POS = "posicionamientos"
PG_TABLE_KM = "km_recorrido_bus"

def _get_pg_conn():
    dsn = os.getenv("DATABASE_PATH")
    if not dsn:
        raise ValueError("No existe la variable de entorno DATABASE_PATH")
    if "sslmode=" not in dsn:
        dsn = dsn + ("&" if "?" in dsn else "?") + "sslmode=require"
    return psycopg2.connect(dsn)

def obtener_ultima_fecha_procesada(cur) -> date | None:
    """
    Obtiene la última fecha procesada exitosamente del log.
    Retorna None si no hay ninguna procesada.
    """
    sql = """
    SELECT fecha
    FROM log.procesa_posicionamientos
    WHERE estado = 'ok'
    ORDER BY fecha DESC
    LIMIT 1;
    """
    cur.execute(sql)
    result = cur.fetchone()
    return result[0] if result else None

def obtener_carpetas_disponibles_en_blob(cliente_contenedor) -> list[date]:
    """
    Lista todas las carpetas con formato YYYYMMDD en Azure Blob Storage.
    Retorna lista de dates ordenadas cronológicamente.
    """
    carpetas = set()
    
    # Listar todos los blobs en el contenedor con el prefijo base
    for blob in cliente_contenedor.list_blobs(name_starts_with=PREFIJO_BASE):
        # Extraer la carpeta (formato: posicionamientos/YYYYMMDD/archivo.parquet)
        partes = blob.name.split("/")
        if len(partes) >= 2:
            carpeta_str = partes[1]
            # Validar que sea un formato YYYYMMDD válido (8 dígitos)
            if carpeta_str.isdigit() and len(carpeta_str) == 8:
                try:
                    d = datetime.strptime(carpeta_str, "%Y%m%d").date()
                    carpetas.add(d)
                except ValueError:
                    continue
    
    # Retornar ordenadas cronológicamente
    return sorted(list(carpetas))

def obtener_fechas_a_procesar(cur, cliente_contenedor, limite_dias: int) -> list[date]:
    """
    Obtiene las fechas a procesar basándose en:
    1. Última fecha procesada del log
    2. Carpetas disponibles en Azure Blob Storage
    
    Retorna las siguientes N fechas (donde N = limite_dias) que tengan datos en Blob.
    """
    # 1. Obtener última fecha procesada
    ultima_procesada = obtener_ultima_fecha_procesada(cur)
    
    # 2. Obtener todas las carpetas disponibles en Blob
    carpetas_blob = obtener_carpetas_disponibles_en_blob(cliente_contenedor)
    
    if not carpetas_blob:
        print("⚠️  No hay carpetas disponibles en Azure Blob Storage")
        return []
    
    # 3. Filtrar carpetas que no han sido procesadas
    if ultima_procesada:
        # Si hay última procesada, tomar carpetas posteriores a esa fecha
        fechas_pendientes = [f for f in carpetas_blob if f > ultima_procesada]
        print(f"ℹ️  Última fecha procesada: {ultima_procesada}")
    else:
        # Si no hay ninguna procesada, tomar todas desde la más antigua
        fechas_pendientes = carpetas_blob
        print(f"ℹ️  Primera ejecución - procesando desde: {carpetas_blob[0] if carpetas_blob else 'N/A'}")
    
    # 4. Tomar solo las primeras N fechas
    fechas_a_procesar = fechas_pendientes[:limite_dias]
    
    if fechas_a_procesar:
        print(f"✅ Fechas encontradas a procesar: {[f.strftime('%Y-%m-%d') for f in fechas_a_procesar]}")
    else:
        print("ℹ️  No hay fechas nuevas a procesar")
    
    return fechas_a_procesar

def asegurar_fecha_en_log(cur, fecha: date):
    """
    Asegura que la fecha existe en el log.procesa_posicionamientos.
    Si no existe, la inserta con estado 'pendiente'.
    """
    sql = """
    INSERT INTO log.procesa_posicionamientos (fecha)
    VALUES (%s)
    ON CONFLICT (fecha) DO NOTHING;
    """
    cur.execute(sql, (fecha,))

def marcar_inicio(cur, dia: date):
    sql = """
    UPDATE log.procesa_posicionamientos
    SET estado='pendiente',
        intentos = intentos + 1,
        ultima_ejecucion = now(),
        mensaje = NULL,
        duracion_seg = NULL,
        archivos_total = NULL,
        archivos_ok = NULL,
        archivos_error = NULL,
        registros_pos = NULL,
        filas_km = NULL,
        actualizado_en = now()
    WHERE fecha = %s;
    """
    cur.execute(sql, (dia,))

def marcar_resultado(cur, dia: date, estado: str, duracion_seg: int, meta: dict, mensaje: str | None):
    sql = """
    UPDATE log.procesa_posicionamientos
    SET estado=%s,
        duracion_seg=%s,
        archivos_total=%s,
        archivos_ok=%s,
        archivos_error=%s,
        registros_pos=%s,
        filas_km=%s,
        mensaje=%s,
        actualizado_en=now()
    WHERE fecha = %s;
    """
    cur.execute(sql, (
        estado,
        duracion_seg,
        meta.get("archivos_total"),
        meta.get("archivos_ok"),
        meta.get("archivos_error"),
        meta.get("registros_pos"),
        meta.get("filas_km"),
        mensaje,
        dia
    ))

def _iter_rows_for_execute_values(df: pl.DataFrame, cols: list[str]):
    pdf = df.select(cols).to_pandas()
    return (tuple(x) for x in pdf.itertuples(index=False, name=None))

def cargar_a_postgresql(posicionamientos: pl.DataFrame, km_recorrido_bus: pl.DataFrame, dia: date,
                    page_size_pos: int = 20000, page_size_km: int = 5000):
    """
    - config.posicionamientos: DELETE del día + INSERT batch (con columnas ajustadas)
    - config.km_recorrido_bus: UPSERT batch
    """
    df_pos = (
        posicionamientos
        .rename({
            "DIST (m)": "dist_m",
            "TIEMPO (s)": "tiempo_s",
            "VEL (m/s)": "vel_m_s",
            "DIST FINAL (m)": "dist_final_m",
        })
        .with_columns([
            pl.col("fecha_evento").cast(pl.Datetime, strict=False),
            pl.col("hora").cast(pl.Utf8),
            pl.col("estado_localizacion").cast(pl.Int64, strict=False),
            pl.col("nombre_estado").cast(pl.Utf8),
            pl.col("servbus").cast(pl.Utf8),
            pl.col("movil_bus").cast(pl.Utf8),
            pl.col("longitud").cast(pl.Float64, strict=False),
            pl.col("latitud").cast(pl.Float64, strict=False),
            pl.col("id_linea").cast(pl.Utf8),
            pl.col("id_sublinea").cast(pl.Utf8),
            pl.col("id_viaje").cast(pl.Utf8),
            pl.col("posicion").cast(pl.Utf8),
            pl.col("dist_m").cast(pl.Float64, strict=False),
            pl.col("tiempo_s").cast(pl.Float64, strict=False),
            pl.col("vel_m_s").cast(pl.Float64, strict=False),
            pl.col("dist_final_m").cast(pl.Float64, strict=False),
        ])
    )
    cols_pos = [
        "fecha_evento", "hora", "estado_localizacion", "nombre_estado",
        "servbus", "movil_bus", "longitud", "latitud",
        "id_linea", "id_sublinea", "id_viaje", "posicion",
        "dist_m", "tiempo_s", "vel_m_s", "dist_final_m"
    ]

    df_km = (
        km_recorrido_bus
        .rename({"DIST FINAL (km)": "dist_final_km"})
        .with_columns([
            pl.col("fecha").cast(pl.Date, strict=False),
            pl.col("movil_bus").cast(pl.Utf8),
            pl.col("dist_final_km").cast(pl.Float64, strict=False),
        ])
        .select(["fecha","movil_bus","dist_final_km"])
    )

    sql_delete_pos = f"DELETE FROM {PG_SCHEMA}.{PG_TABLE_POS} WHERE fecha_evento::date = %s;"
    sql_insert_pos = f"""
        INSERT INTO {PG_SCHEMA}.{PG_TABLE_POS} (
            fecha_evento, hora, estado_localizacion, nombre_estado,
            servbus, movil_bus, longitud, latitud,
            id_linea, id_sublinea, id_viaje, posicion,
            dist_m, tiempo_s, vel_m_s, dist_final_m
        ) VALUES %s
    """
    sql_upsert_km = f"""
        INSERT INTO {PG_SCHEMA}.{PG_TABLE_KM} (fecha, movil_bus, dist_final_km)
        VALUES %s
        ON CONFLICT (fecha, movil_bus)
        DO UPDATE SET dist_final_km = EXCLUDED.dist_final_km
    """

    # Una sola transacción por día
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_delete_pos, (dia,))
            if df_pos.height > 0:
                execute_values(cur, sql_insert_pos, _iter_rows_for_execute_values(df_pos, cols_pos),
                            page_size=page_size_pos)
            if df_km.height > 0:
                execute_values(cur, sql_upsert_km, _iter_rows_for_execute_values(df_km, ["fecha","movil_bus","dist_final_km"]),
                            page_size=page_size_km)
        conn.commit()

# ============================================================
# 7) ORQUESTADOR: BARRIDO DE BLOB (procesa días disponibles)
# ============================================================
def ejecutar_job_posicionamientos(limite_dias_por_ejecucion: int = 1, verbose: bool = False):
    print(f"📅 Días a procesar: {limite_dias_por_ejecucion}")
    print()
    
    # Obtener cliente de contenedor
    cliente_contenedor = obtener_cliente_contenedor()
    
    # Obtener fechas a procesar desde Blob Storage
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            dias = obtener_fechas_a_procesar(cur, cliente_contenedor, limite_dias_por_ejecucion)
            conn.commit()
    
    if not dias:
        print("✅ No hay fechas nuevas a procesar")
        return

    # Procesar cada día
    for i, dia in enumerate(dias, 1):
        print(f"\n[{i}/{len(dias)}] Procesando día: {dia} ...")
        t0 = time.time()

        # Asegurar que la fecha existe en el log
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                asegurar_fecha_en_log(cur, dia)
                marcar_inicio(cur, dia)
            conn.commit()

        try:
            posicionamientos, km_recorrido_bus, meta = procesar_dia_completo(dia, verbose=verbose)

            if meta["estado"] == "sin_archivos":
                dur = int(time.time() - t0)
                with _get_pg_conn() as conn:
                    with conn.cursor() as cur:
                        marcar_resultado(cur, dia, "sin_archivos", dur, meta, "No hay parquets para ese día")
                    conn.commit()
                print(f"  ⚠️  Día {dia}: sin archivos. Marcado en log.")
                continue

            if meta["estado"] != "ok":
                raise RuntimeError("No se logró procesar ningún archivo del día.")

            # cargar a PG
            cargar_a_postgresql(posicionamientos, km_recorrido_bus, dia)

            dur = int(time.time() - t0)
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    marcar_resultado(cur, dia, "ok", dur, meta, None)
                conn.commit()

            print(f"  ✅ Día {dia} OK | registros={meta.get('registros_pos'):,} | dur={dur}s")

        except Exception:
            dur = int(time.time() - t0)
            err = traceback.format_exc()
            meta = meta if "meta" in locals() and isinstance(meta, dict) else {}
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    marcar_resultado(cur, dia, "error", dur, meta, err[:4000])
                conn.commit()
            print(f"  ❌ Día {dia} ERROR")
            print(f"     {err[:200]}...")

# ============================================================
# 8) CLI
# ============================================================
if __name__ == "__main__":
    limite = int(os.getenv("POS_LIMITE_DIAS", "1"))
    verbose = os.getenv("POS_VERBOSE", "0") == "1"

    ejecutar_job_posicionamientos(limite_dias_por_ejecucion=limite, verbose=verbose)