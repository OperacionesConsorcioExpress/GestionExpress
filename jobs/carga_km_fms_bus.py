# ============================================================
#  JOB CARGA KM FMS BUS - PROCESO DIARIO CON LOG EN POSTGRESQL
#  Fuente: Azure Blob Storage → pl-k01-kilometros
#  Destino: config.km_fms_bus | Log: log.procesa_km_fms_bus
# ============================================================
import os, io, time, traceback
from datetime import date, datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from database.database_manager import get_db_connection as _get_pg_conn
from azure.storage.blob import BlobServiceClient

# ============================================================
# 0) CONFIG AZURE BLOB
# ============================================================
CONTENEDOR    = "pl-k01-kilometros"
PREFIJO_BASE  = "01_Fact_km_ejecutado_vehiculo_fms"
FECHA_SEMILLA = date(2026, 5, 18)  # No procesar fechas anteriores a este día

HOJA_EXCEL    = "Fact_km_ejecutado_vehiculo_fms"
SUFIJO_ARCHIVO = "_Fact_km_ejecutado_vehiculo_fms.xlsx"

# Columnas del Excel que se cargan a PostgreSQL
COL_FECHA     = "Fecha"
COL_VEHICULO  = "Vehículo Real"
COL_KM        = "Km Ejecutado"

PG_SCHEMA = "config"
PG_TABLE  = "km_fms_bus"


def obtener_cliente_contenedor():
    cadena = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not cadena:
        raise ValueError("No existe la variable AZURE_STORAGE_CONNECTION_STRING")
    servicio = BlobServiceClient.from_connection_string(cadena)
    return servicio.get_container_client(CONTENEDOR)


# ============================================================
# 1) AZURE: LISTAR ARCHIVOS DISPONIBLES
# ============================================================
def listar_fechas_disponibles(cliente_contenedor) -> list[date]:
    """
    Recorre el contenedor y extrae las fechas de los archivos disponibles.
    Estructura esperada:
      01_Fact_km_ejecutado_vehiculo_fms/YYYY/MM/YYYYMMDD_Fact_km_ejecutado_vehiculo_fms.xlsx
    """
    fechas = set()
    prefijo = PREFIJO_BASE + "/"

    for blob in cliente_contenedor.list_blobs(name_starts_with=prefijo):
        nombre_archivo = blob.name.split("/")[-1]
        if not nombre_archivo.lower().endswith(".xlsx"):
            continue
        # El nombre inicia con la fecha: YYYYMMDD_...
        fecha_str = nombre_archivo.split("_")[0]
        if len(fecha_str) == 8 and fecha_str.isdigit():
            try:
                fechas.add(datetime.strptime(fecha_str, "%Y%m%d").date())
            except ValueError:
                continue

    return sorted(list(fechas))


def construir_ruta_blob(d: date) -> str:
    """Ruta completa en el contenedor para una fecha dada."""
    return f"{PREFIJO_BASE}/{d.year}/{d.month:02d}/{d.strftime('%Y%m%d')}{SUFIJO_ARCHIVO}"


# ============================================================
# 2) LEER Y TRANSFORMAR EXCEL
# ============================================================
def leer_excel_blob(cliente_contenedor, ruta_blob: str) -> pd.DataFrame:
    datos = cliente_contenedor.get_blob_client(ruta_blob).download_blob().readall()
    df = pd.read_excel(io.BytesIO(datos), sheet_name=HOJA_EXCEL, engine="openpyxl")
    return df


def transformar_datos(df_raw: pd.DataFrame, dia: date, verbose: bool = False) -> pd.DataFrame:
    """
    Selecciona las columnas requeridas, normaliza tipos y filtra por fecha objetivo.
    Solo se persisten: Fecha, Vehículo Real, Km Ejecutado.
    """
    if verbose:
        print(f"    Columnas en el Excel: {list(df_raw.columns)}")
        print(f"    Total filas brutas: {df_raw.shape[0]:,}")

    # Buscar columnas de forma flexible (ignorando mayúsculas/espacios extra)
    col_map = {c.strip(): c for c in df_raw.columns}
    requeridas_raw = {}
    for buscado, alias in [(COL_FECHA, "fecha"), (COL_VEHICULO, "vehiculo_real"), (COL_KM, "km_ejecutado")]:
        encontrada = col_map.get(buscado)
        if encontrada is None:
            # búsqueda case-insensitive como fallback
            encontrada = next((c for c in df_raw.columns if c.strip().lower() == buscado.lower()), None)
        if encontrada is None:
            raise ValueError(
                f"Columna '{buscado}' no encontrada en '{HOJA_EXCEL}'. "
                f"Disponibles: {list(df_raw.columns)}"
            )
        requeridas_raw[alias] = encontrada

    df = df_raw[[requeridas_raw["fecha"], requeridas_raw["vehiculo_real"], requeridas_raw["km_ejecutado"]]].copy()
    df.columns = ["fecha", "vehiculo_real", "km_ejecutado"]

    # Convertir tipos → Python nativos para compatibilidad psycopg2
    df["fecha"]         = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df["vehiculo_real"] = df["vehiculo_real"].astype(str).str.strip()
    df["km_ejecutado"]  = pd.to_numeric(df["km_ejecutado"], errors="coerce")

    if verbose:
        print(f"    Filas después de convertir tipos: {df.shape[0]:,}")
        fechas_unicas = df["fecha"].dropna().unique()
        print(f"    Fechas únicas en el archivo: {sorted(str(f) for f in fechas_unicas)}")

    # Eliminar filas con valores nulos en columnas críticas
    antes = df.shape[0]
    df = df.dropna(subset=["fecha", "vehiculo_real", "km_ejecutado"])
    df = df[df["vehiculo_real"].str.len() > 0]
    df = df[~df["vehiculo_real"].str.lower().isin(["nan", "none", ""])]
    if verbose:
        print(f"    Filas después de limpiar nulos: {df.shape[0]:,} (descartadas: {antes - df.shape[0]:,})")

    # Filtrar solo la fecha objetivo (el Excel puede tener datos de todo el mes)
    antes = df.shape[0]
    df = df[df["fecha"] == dia]
    if verbose:
        print(f"    Filas para la fecha {dia}: {df.shape[0]:,} (otras fechas descartadas: {antes - df.shape[0]:,})")

    return df.reset_index(drop=True)


# ============================================================
# 3) POSTGRESQL: TABLAS, LOG Y CARGA
# ============================================================
DDL_LOG = """
    CREATE TABLE IF NOT EXISTS log.procesa_km_fms_bus (
        id               BIGSERIAL    PRIMARY KEY,
        fecha            DATE         NOT NULL,
        estado           TEXT         NOT NULL DEFAULT 'pendiente',
        intentos         INT          NOT NULL DEFAULT 0,
        ultima_ejecucion TIMESTAMPTZ,
        mensaje          TEXT,
        duracion_seg     INT,
        archivos_total   INT,
        archivos_ok      INT,
        archivos_error   INT,
        registros_pos    INT,
        filas_km         INT,
        actualizado_en   TIMESTAMPTZ  DEFAULT NOW(),
        UNIQUE (fecha)
    );
"""

DDL_DATA = """
    CREATE TABLE IF NOT EXISTS config.km_fms_bus (
        id            BIGSERIAL    PRIMARY KEY,
        fecha         DATE         NOT NULL,
        vehiculo_real TEXT         NOT NULL,
        km_ejecutado  NUMERIC(12,2),
        UNIQUE (fecha, vehiculo_real)
    );
"""


def asegurar_tablas(cur):
    cur.execute(DDL_LOG)
    cur.execute(DDL_DATA)


def obtener_ultima_fecha_procesada(cur) -> date | None:
    cur.execute("""
        SELECT fecha
        FROM log.procesa_km_fms_bus
        WHERE estado = 'ok'
        ORDER BY fecha DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    return row[0] if row else None


def obtener_fechas_a_procesar(
    cur, cliente_contenedor, limite_dias: int
) -> list[date]:
    """
    Determina qué fechas procesar comparando el log con los archivos disponibles en Blob.
    """
    ultima = obtener_ultima_fecha_procesada(cur)
    fechas_blob = listar_fechas_disponibles(cliente_contenedor)

    if not fechas_blob:
        print("⚠️  No hay archivos disponibles en Azure Blob Storage")
        return []

    if ultima and ultima >= FECHA_SEMILLA:
        piso = ultima
        print(f"ℹ️  Última fecha procesada: {ultima} (posterior a semilla {FECHA_SEMILLA})")
    else:
        piso = FECHA_SEMILLA - timedelta(days=1)
        if ultima:
            print(
                f"ℹ️  Última procesada ({ultima}) anterior a semilla; "
                f"arrancando desde {FECHA_SEMILLA}"
            )
        else:
            print(f"ℹ️  Sin historial en el log; arrancando desde semilla: {FECHA_SEMILLA}")

    pendientes = [f for f in fechas_blob if f > piso]
    fechas = pendientes[:limite_dias]

    if fechas:
        print(f"✅ Fechas a procesar: {[f.strftime('%Y-%m-%d') for f in fechas]}")
    else:
        print("ℹ️  No hay fechas nuevas a procesar")

    return fechas


def asegurar_fecha_en_log(cur, fecha: date):
    cur.execute("""
        INSERT INTO log.procesa_km_fms_bus (fecha)
        VALUES (%s)
        ON CONFLICT (fecha) DO NOTHING;
    """, (fecha,))


def marcar_inicio(cur, dia: date):
    cur.execute("""
        UPDATE log.procesa_km_fms_bus
        SET estado          = 'pendiente',
            intentos        = intentos + 1,
            ultima_ejecucion = NOW(),
            mensaje         = NULL,
            duracion_seg    = NULL,
            archivos_total  = NULL,
            archivos_ok     = NULL,
            archivos_error  = NULL,
            registros_pos   = NULL,
            filas_km        = NULL,
            actualizado_en  = NOW()
        WHERE fecha = %s;
    """, (dia,))


def marcar_resultado(
    cur, dia: date, estado: str, duracion_seg: int, meta: dict, mensaje: str | None
):
    cur.execute("""
        UPDATE log.procesa_km_fms_bus
        SET estado         = %s,
            duracion_seg   = %s,
            archivos_total = %s,
            archivos_ok    = %s,
            archivos_error = %s,
            registros_pos  = %s,
            filas_km       = %s,
            mensaje        = %s,
            actualizado_en = NOW()
        WHERE fecha = %s;
    """, (
        estado,
        duracion_seg,
        meta.get("archivos_total"),
        meta.get("archivos_ok"),
        meta.get("archivos_error"),
        meta.get("registros_pos"),
        meta.get("filas_km"),
        mensaje,
        dia,
    ))


def cargar_a_postgresql(df: pd.DataFrame, dia: date, page_size: int = 5000):
    """
    DELETE del día + INSERT/UPSERT por lotes.
    Usa tipos Python nativos para garantizar compatibilidad con psycopg2.
    """
    # Convertir explícitamente a tipos Python nativos (evita errores con numpy.float64)
    filas = [
        (
            rec["fecha"],                                                    # datetime.date
            str(rec["vehiculo_real"]).strip(),                               # str
            float(rec["km_ejecutado"]) if pd.notna(rec["km_ejecutado"]) else None,  # float | None
        )
        for rec in df.to_dict("records")
    ]

    sql_delete = f"DELETE FROM {PG_SCHEMA}.{PG_TABLE} WHERE fecha = %s;"
    sql_upsert = f"""
        INSERT INTO {PG_SCHEMA}.{PG_TABLE} (fecha, vehiculo_real, km_ejecutado)
        VALUES %s
        ON CONFLICT (fecha, vehiculo_real)
        DO UPDATE SET km_ejecutado = EXCLUDED.km_ejecutado;
    """

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_delete, (dia,))
            if filas:
                execute_values(cur, sql_upsert, filas, page_size=page_size)
        conn.commit()


# ============================================================
# 4) PROCESAR UN DÍA
# ============================================================
def procesar_dia(
    cliente_contenedor, dia: date, verbose: bool = False
) -> tuple[pd.DataFrame, dict]:
    ruta = construir_ruta_blob(dia)

    if verbose:
        print(f"    Ruta blob: {ruta}")

    df_raw = leer_excel_blob(cliente_contenedor, ruta)
    print(f"    Excel descargado: {df_raw.shape[0]:,} filas brutas, {df_raw.shape[1]} columnas")

    df = transformar_datos(df_raw, dia, verbose=verbose)

    print(f"    Filas válidas para {dia}: {df.shape[0]:,}")

    meta = {
        "archivos_total": 1,
        "archivos_ok":    1,
        "archivos_error": 0,
        "registros_pos":  df.shape[0],
        "filas_km":       df.shape[0],
    }
    return df, meta


# ============================================================
# 5) ORQUESTADOR
# ============================================================
def ejecutar_job_km_fms(limite_dias: int = 1, verbose: bool = False):
    print(f"📅 Días a procesar: {limite_dias}")
    print()

    cliente_contenedor = obtener_cliente_contenedor()

    # Asegurar que existan las tablas
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            asegurar_tablas(cur)
        conn.commit()
    print("✅ Tablas verificadas/creadas")

    # Determinar fechas a procesar
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            dias = obtener_fechas_a_procesar(cur, cliente_contenedor, limite_dias)
        conn.commit()

    if not dias:
        print("✅ No hay fechas nuevas a procesar")
        return

    for i, dia in enumerate(dias, 1):
        print(f"\n[{i}/{len(dias)}] Procesando día: {dia} ...")
        t0 = time.time()
        meta = {}

        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                asegurar_fecha_en_log(cur, dia)
                marcar_inicio(cur, dia)
            conn.commit()

        try:
            df, meta = procesar_dia(cliente_contenedor, dia, verbose)
            cargar_a_postgresql(df, dia)

            dur = int(time.time() - t0)
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    marcar_resultado(cur, dia, "ok", dur, meta, None)
                conn.commit()

            print(f"  ✅ Día {dia} OK | registros={meta.get('registros_pos'):,} | dur={dur}s")

        except Exception:
            dur = int(time.time() - t0)
            err = traceback.format_exc()
            meta_err = {
                "archivos_total": 1,
                "archivos_ok":    0,
                "archivos_error": 1,
                "registros_pos":  0,
                "filas_km":       0,
            }
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    marcar_resultado(cur, dia, "error", dur, meta_err, err[:4000])
                conn.commit()
            print(f"  ❌ Día {dia} ERROR")
            print(f"     {err[:200]}...")


# ============================================================
# 6) CLI
# ============================================================
if __name__ == "__main__":
    limite  = int(os.getenv("FMS_LIMITE_DIAS", "1"))
    verbose = os.getenv("FMS_VERBOSE", "0") == "1"

    ejecutar_job_km_fms(limite_dias=limite, verbose=verbose)
