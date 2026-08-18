# ============================================================
#  JOB BUSES INMOVILIZADOS - PROCESO HORARIO CON LOG EN POSTGRESQL
#  Fuente:  Azure Blob Storage → e01-fms
#           1_sc/<AAAA>/100_vehiculos_inmovilizados_zonal_sc/
#           1_sc/<AAAA>/101_vehiculos_inmovilizados_troncal_sc/
#           2_uq/<AAAA>/100_vehiculos_inmovilizados_zonal_uq/
#           2_uq/<AAAA>/101_vehiculos_inmovilizados_troncal_uq/
#  Destino: mantenimiento.buses_inmovilizados
#  Log:     log.procesa_buses_inmovilizados
#
#  Los archivos se sobrescriben cada hora: el nombre lleva la fecha de
#  actualización (AAAAMMDD_...) y al día siguiente se genera uno nuevo.
#  Por eso el job toma SIEMPRE el archivo más reciente de cada carpeta y la
#  identidad de cada registro la define la Fecha de Inmovilización interna.
#
#  Deduplicación: un registro por inmovilización (placa + fecha de
#  inmovilización). Se calcula un hash de las 11 columnas del archivo:
#    · hash nuevo        → INSERT
#    · hash distinto     → UPDATE (actualizado_en = now)
#    · hash igual        → se omite (no se duplica ni se reescribe)
#
#  CIERRE DEL CICLO POR AUSENCIA
#  El archivo es una foto de los buses inmovilizados en ese momento: las
#  columnas Fecha de Habilitación / Habilitador llegan siempre vacías y la
#  habilitación se manifiesta porque el bus DESAPARECE del archivo siguiente.
#  Por eso, tras cargar un corte, los ciclos abiertos de esa misma fuente que
#  no vinieron en él se cierran con `fecha_habilitacion_estimada` = fecha del
#  archivo donde ya no aparece, y sus días quedan congelados. Si el bus vuelve
#  a aparecer con la misma fecha de inmovilización, el ciclo se reabre.
#  Nada se borra ni se sobrescribe: cada inmovilización conserva su fila y
#  cada transición queda registrada en mantenimiento.buses_inmovilizados_eventos.
# ============================================================
import os, io, re, time, hashlib, unicodedata, traceback
from datetime import date, datetime, timedelta, timezone
import pandas as pd
from psycopg2.extras import execute_values
from database.database_manager import get_db_connection as _get_pg_conn
from azure.storage.blob import BlobServiceClient

# ============================================================
# 0) CONFIG AZURE BLOB
# ============================================================
CONTENEDOR = "e01-fms"

# Cada fuente = una carpeta del contenedor. `origen` es la llave que se usa en
# el log, en la tabla destino y en el modal de carga manual de la aplicación.
FUENTES = [
    {
        "origen":        "zonal_sc",
        "etiqueta":      "Zonal SC",
        "concesion":     "SC",
        "tipo_servicio": "Zonal",
        "carpeta":       "1_sc/{anio}/100_vehiculos_inmovilizados_zonal_sc",
    },
    {
        "origen":        "troncal_sc",
        "etiqueta":      "Troncal SC",
        "concesion":     "SC",
        "tipo_servicio": "Troncal",
        "carpeta":       "1_sc/{anio}/101_vehiculos_inmovilizados_troncal_sc",
    },
    {
        "origen":        "zonal_uq",
        "etiqueta":      "Zonal UQ",
        "concesion":     "UQ",
        "tipo_servicio": "Zonal",
        "carpeta":       "2_uq/{anio}/100_vehiculos_inmovilizados_zonal_uq",
    },
    {
        "origen":        "troncal_uq",
        "etiqueta":      "Troncal UQ",
        "concesion":     "UQ",
        "tipo_servicio": "Troncal",
        "carpeta":       "2_uq/{anio}/101_vehiculos_inmovilizados_troncal_uq",
    },
]

ORIGENES_VALIDOS = [f["origen"] for f in FUENTES]

PG_SCHEMA = "mantenimiento"
PG_TABLE  = "buses_inmovilizados"


_BOGOTA = timezone(timedelta(hours=-5))


def _hoy() -> date:
    """
    Fecha en Bogotá. El runner de GitHub Actions trabaja en UTC y el proceso
    corre hasta las 23:00 locales: sin esto, las corridas de la noche tomarían
    ya el día siguiente (y el 31 de diciembre buscarían la carpeta del año que
    todavía no existe).
    """
    return datetime.now(_BOGOTA).date()


def _log(mensaje: str = ""):
    """
    print seguro: el job también se ejecuta desde la aplicación web y en consolas
    Windows (cp1252) los emojis levantan UnicodeEncodeError, que convertiría un
    cargue exitoso en un error 500. Si la consola no los soporta, se escriben en
    ASCII en lugar de fallar.
    """
    try:
        print(mensaje)
    except UnicodeEncodeError:
        print(mensaje.encode("ascii", "ignore").decode("ascii"))


def obtener_cliente_contenedor():
    cadena = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not cadena:
        raise ValueError("No existe la variable AZURE_STORAGE_CONNECTION_STRING")
    servicio = BlobServiceClient.from_connection_string(cadena)
    return servicio.get_container_client(CONTENEDOR)


# ============================================================
# 1) AZURE: UBICAR EL ARCHIVO MÁS RECIENTE DE CADA CARPETA
# ============================================================
def _fecha_desde_nombre(nombre_archivo: str):
    """El nombre inicia con la fecha de actualización: AAAAMMDD_..."""
    prefijo = nombre_archivo.split("_")[0]
    if len(prefijo) == 8 and prefijo.isdigit():
        try:
            return datetime.strptime(prefijo, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def listar_archivos_fuente(cliente_contenedor, fuente: dict, anio: int) -> list[dict]:
    """Todos los .xlsx de la carpeta de una fuente para un año."""
    prefijo = fuente["carpeta"].format(anio=anio) + "/"
    archivos = []

    for blob in cliente_contenedor.list_blobs(name_starts_with=prefijo):
        nombre = blob.name.split("/")[-1]
        if not nombre.lower().endswith(".xlsx") or nombre.startswith("~$"):
            continue
        archivos.append({
            "ruta_blob":     blob.name,
            "archivo":       nombre,
            "fecha_archivo": _fecha_desde_nombre(nombre),
            "modificado":    blob.last_modified,
        })

    # Más reciente primero: por fecha del nombre y, como desempate, por last_modified
    return sorted(
        archivos,
        key=lambda a: (a["fecha_archivo"] or date.min, a["modificado"] or datetime.min),
        reverse=True,
    )


def obtener_archivo_vigente(cliente_contenedor, fuente: dict, anio: int,
                            fecha_archivo: date | None = None) -> dict | None:
    """
    Archivo a procesar de una fuente.
    - Sin `fecha_archivo`: el más reciente de la carpeta (uso normal, horario).
    - Con `fecha_archivo`: el de esa fecha exacta (reproceso manual de un día).
    """
    archivos = listar_archivos_fuente(cliente_contenedor, fuente, anio)
    if not archivos:
        return None

    if fecha_archivo is None:
        return archivos[0]

    return next((a for a in archivos if a["fecha_archivo"] == fecha_archivo), None)


# ============================================================
# 2) LECTURA TOLERANTE DEL EXCEL
# ============================================================
# Las columnas se buscan normalizando tildes, espacios y mayúsculas, para que el
# archivo pueda variar en detalles de escritura sin romper el proceso.
COLUMNAS_ESPERADAS = {
    "concesionario_operacion": ["concesionario de operacion", "concesionario operacion", "concesionario"],
    "placa":                   ["placa", "placa vehiculo", "placa del vehiculo"],
    "fecha_inmovilizacion":    ["fecha de inmovilizacion", "fecha inmovilizacion"],
    "causa_inmovilizacion":    ["causa de inmovilizacion", "causa inmovilizacion", "causa"],
    "descripcion_novedad":     ["descripcion de la novedad", "descripcion novedad", "novedad"],
    "creador":                 ["creador", "creado por"],
    "fecha_revision":          ["fecha de revision", "fecha revision"],
    "revisor":                 ["revisor", "revisado por"],
    "descripcion_revision":    ["descripcion de revision", "descripcion de la revision", "descripcion revision"],
    "fecha_habilitacion":      ["fecha de habilitacion", "fecha habilitacion"],
    "habilitador":             ["habilitador", "habilitado por"],
}

# Columnas que definen si la fila cambió (todas las del archivo)
COLUMNAS_HASH = list(COLUMNAS_ESPERADAS.keys())


def _normalizar_texto(valor) -> str:
    """minúsculas, sin tildes y con espacios colapsados — para comparar encabezados."""
    if valor is None:
        return ""
    txt = str(valor).strip().lower()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = re.sub(r"[^a-z0-9 ]", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()


def _mapear_columnas(columnas) -> dict:
    """{columna_destino: nombre_real_en_el_excel} para las columnas encontradas."""
    normalizadas = {_normalizar_texto(c): c for c in columnas}
    mapeo = {}

    for destino, alias in COLUMNAS_ESPERADAS.items():
        encontrada = next((normalizadas[a] for a in alias if a in normalizadas), None)
        if encontrada is None:
            # Coincidencia parcial como último recurso (encabezados con texto extra)
            encontrada = next(
                (real for norm, real in normalizadas.items()
                 if any(norm.startswith(a) or a in norm for a in alias)),
                None,
            )
        if encontrada is not None:
            mapeo[destino] = encontrada

    return mapeo


def leer_excel_bytes(datos: bytes, referencia: str = "archivo", verbose: bool = False) -> pd.DataFrame:
    """
    Devuelve la hoja/fila de encabezados que contenga las columnas esperadas.
    Tolera hojas con títulos por encima de la tabla.
    Se usa igual para el archivo del blob y para el que sube el usuario.
    """
    libro = pd.ExcelFile(io.BytesIO(datos), engine="openpyxl")

    mejor_df, mejor_mapeo, mejor_hoja = None, {}, None

    for hoja in libro.sheet_names:
        for fila_encabezado in range(0, 8):  # títulos por encima de la tabla
            try:
                df = libro.parse(hoja, header=fila_encabezado)
            except Exception:
                continue
            if df.empty and fila_encabezado > 0:
                continue

            mapeo = _mapear_columnas(df.columns)
            if len(mapeo) > len(mejor_mapeo):
                mejor_df, mejor_mapeo, mejor_hoja = df, mapeo, (hoja, fila_encabezado)

            # Encabezado completo: no hace falta seguir buscando
            if len(mapeo) == len(COLUMNAS_ESPERADAS):
                break
        if len(mejor_mapeo) == len(COLUMNAS_ESPERADAS):
            break

    if mejor_df is None or "placa" not in mejor_mapeo or "fecha_inmovilizacion" not in mejor_mapeo:
        raise ValueError(
            f"No se encontraron las columnas mínimas (Placa / Fecha de Inmovilización) en {referencia}. "
            f"Hojas revisadas: {libro.sheet_names}"
        )

    if verbose:
        faltantes = [c for c in COLUMNAS_ESPERADAS if c not in mejor_mapeo]
        _log(f"    Hoja/encabezado detectado: {mejor_hoja}")
        if faltantes:
            _log(f"    ⚠️  Columnas no encontradas (quedan en NULL): {faltantes}")

    df = mejor_df.rename(columns={real: destino for destino, real in mejor_mapeo.items()})

    # Garantizar todas las columnas destino, aunque el archivo no las traiga
    for destino in COLUMNAS_ESPERADAS:
        if destino not in df.columns:
            df[destino] = None

    return df[list(COLUMNAS_ESPERADAS.keys())]


def leer_excel_blob(cliente_contenedor, ruta_blob: str, verbose: bool = False) -> pd.DataFrame:
    """Descarga el .xlsx del blob y lo interpreta con `leer_excel_bytes`."""
    datos = cliente_contenedor.get_blob_client(ruta_blob).download_blob().readall()
    return leer_excel_bytes(datos, referencia=ruta_blob, verbose=verbose)


# ============================================================
# 3) TRANSFORMACIÓN
# ============================================================
def _texto(valor) -> str | None:
    """Texto limpio o None. Contempla los vacíos que pandas entrega como NaN/NaT."""
    if valor is None:
        return None
    try:
        if pd.isna(valor):
            return None
    except (TypeError, ValueError):
        pass
    txt = str(valor).replace("\xa0", " ").strip()
    if txt.lower() in ("nan", "nat", "none", ""):
        return None
    return txt


def _limpiar_placa(valor) -> str | None:
    """Placa en mayúsculas, sin espacios ni guiones (para cruzar con la flota)."""
    txt = _texto(valor)
    if txt is None:
        return None
    txt = re.sub(r"[^A-Za-z0-9]", "", txt).upper()
    return txt or None


def _fecha(valor):
    """
    Fecha/hora tolerante.

    El archivo entrega las fechas como TEXTO en formato ISO ('2026-05-06 10:20:34'),
    así que se intenta ISO primero: con el parser flexible y dayfirst=True, una
    fecha como 2026-05-06 se leería como 6 de junio en vez de 6 de mayo.
    El parser flexible queda como respaldo para formatos tipo dd/mm/aaaa.
    """
    # pd.NaT es instancia de datetime: hay que descartarlo antes de nada
    try:
        if valor is None or pd.isna(valor):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(valor, (datetime, pd.Timestamp)):
        return pd.Timestamp(valor).to_pydatetime()

    txt = _texto(valor)
    if txt is None:
        return None

    ts = pd.to_datetime(txt, format="ISO8601", errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(txt, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def _hash_fila(fila: dict) -> str:
    """SHA-256 de las 11 columnas del archivo — define si la fila cambió."""
    partes = []
    for col in COLUMNAS_HASH:
        v = fila.get(col)
        if isinstance(v, datetime):
            partes.append(v.isoformat())
        else:
            partes.append("" if v is None else str(v).strip().upper())
    return hashlib.sha256("|".join(partes).encode("utf-8")).hexdigest()


def cargar_catalogo_placas() -> dict:
    """{placa_normalizada: id_bus} desde config.buses_cexp."""
    sql = """
        SELECT id, UPPER(REGEXP_REPLACE(placa, '[^A-Za-z0-9]', '', 'g')) AS placa_norm
        FROM config.buses_cexp
        WHERE placa IS NOT NULL AND TRIM(placa) <> '';
    """
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            filas = cur.fetchall()
    return {f[1]: f[0] for f in filas if f[1]}


def transformar_datos(df_raw: pd.DataFrame, fuente: dict, archivo: dict,
                      catalogo_placas: dict, verbose: bool = False) -> tuple[list[dict], dict]:
    """Normaliza el archivo, cruza con la flota y calcula el hash de cada fila."""
    filas, sin_bus, descartadas = [], 0, 0

    for registro in df_raw.to_dict("records"):
        placa = _limpiar_placa(registro.get("placa"))
        fecha_inmovilizacion = _fecha(registro.get("fecha_inmovilizacion"))

        # Sin placa o sin fecha de inmovilización no hay identidad del registro
        if not placa or fecha_inmovilizacion is None:
            descartadas += 1
            continue

        fila = {
            "placa":                   placa,
            "fecha_inmovilizacion":    fecha_inmovilizacion,
            "concesionario_operacion": _texto(registro.get("concesionario_operacion")) or fuente["concesion"],
            "causa_inmovilizacion":    _texto(registro.get("causa_inmovilizacion")),
            "descripcion_novedad":     _texto(registro.get("descripcion_novedad")),
            "creador":                 _texto(registro.get("creador")),
            "fecha_revision":          _fecha(registro.get("fecha_revision")),
            "revisor":                 _texto(registro.get("revisor")),
            "descripcion_revision":    _texto(registro.get("descripcion_revision")),
            "fecha_habilitacion":      _fecha(registro.get("fecha_habilitacion")),
            "habilitador":             _texto(registro.get("habilitador")),
        }

        id_bus = catalogo_placas.get(placa)
        if id_bus is None:
            sin_bus += 1

        fila.update({
            "id_bus":           id_bus,
            "origen":           fuente["origen"],
            "tipo_servicio":    fuente["tipo_servicio"],
            "archivo":          archivo["archivo"],
            "fecha_archivo":    archivo["fecha_archivo"],
            # corte en el que consta inmovilizado: base del cierre por ausencia
            "visto_en_archivo": archivo["fecha_archivo"],
            "hash_fila":        _hash_fila(fila),
        })
        filas.append(fila)

    # Si el archivo repite la misma inmovilización, se conserva la última aparición
    unicas = {}
    for f in filas:
        unicas[(f["placa"], f["fecha_inmovilizacion"])] = f
    filas = list(unicas.values())

    if verbose:
        _log(f"    Filas válidas: {len(filas):,} | descartadas: {descartadas:,} | sin bus en flota: {sin_bus:,}")

    return filas, {"filas_archivo": len(filas), "filas_descartadas": descartadas, "filas_sin_bus": sin_bus}


# ============================================================
# 4) POSTGRESQL: TABLAS, LOG Y CARGA
# ============================================================
DDL_LOG = """
    CREATE SCHEMA IF NOT EXISTS log;

    CREATE TABLE IF NOT EXISTS log.procesa_buses_inmovilizados (
        id                 BIGSERIAL   PRIMARY KEY,
        fecha_archivo      DATE        NOT NULL,
        origen             TEXT        NOT NULL,
        archivo            TEXT,
        ruta_blob          TEXT,
        estado             TEXT        NOT NULL DEFAULT 'pendiente',
        intentos           INT         NOT NULL DEFAULT 0,
        ultima_ejecucion   TIMESTAMPTZ,
        mensaje            TEXT,
        duracion_seg       INT,
        filas_archivo      INT,
        filas_nuevas       INT,
        filas_actualizadas INT,
        filas_omitidas     INT,
        filas_sin_bus      INT,
        ejecutado_por      TEXT,
        actualizado_en     TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (fecha_archivo, origen)
    );

    CREATE INDEX IF NOT EXISTS idx_log_inmov_fecha
        ON log.procesa_buses_inmovilizados (fecha_archivo DESC, origen);

    -- Ciclos que este cargue cerró por ausencia del bus en el archivo
    ALTER TABLE log.procesa_buses_inmovilizados
        ADD COLUMN IF NOT EXISTS filas_cerradas INT;
"""

DDL_DATA = """
    CREATE SCHEMA IF NOT EXISTS mantenimiento;

    CREATE TABLE IF NOT EXISTS mantenimiento.buses_inmovilizados (
        id                      BIGSERIAL PRIMARY KEY,
        id_bus                  BIGINT    REFERENCES config.buses_cexp (id),
        placa                   TEXT      NOT NULL,
        concesionario_operacion TEXT,
        origen                  TEXT      NOT NULL,
        tipo_servicio           TEXT,
        fecha_inmovilizacion    TIMESTAMP NOT NULL,
        causa_inmovilizacion    TEXT,
        descripcion_novedad     TEXT,
        creador                 TEXT,
        fecha_revision          TIMESTAMP,
        revisor                 TEXT,
        descripcion_revision    TEXT,
        fecha_habilitacion      TIMESTAMP,
        habilitador             TEXT,
        archivo                 TEXT,
        fecha_archivo           DATE,
        hash_fila               TEXT      NOT NULL,
        creado_en               TIMESTAMPTZ DEFAULT NOW(),
        actualizado_en          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (placa, fecha_inmovilizacion)
    );

    CREATE INDEX IF NOT EXISTS idx_inmov_bus    ON mantenimiento.buses_inmovilizados (id_bus);
    CREATE INDEX IF NOT EXISTS idx_inmov_fecha  ON mantenimiento.buses_inmovilizados (fecha_inmovilizacion DESC);
    CREATE INDEX IF NOT EXISTS idx_inmov_origen ON mantenimiento.buses_inmovilizados (origen);

    -- Cierre del ciclo por ausencia (columnas agregadas sobre tablas ya creadas)
    --   visto_en_archivo            → último corte en el que el bus vino en el archivo
    --   fecha_habilitacion_estimada → cierre deducido; `fecha_habilitacion` se
    --                                 reserva por si el origen algún día la entrega
    --   estado_ciclo                → abierto | cerrado
    --   cerrado_por                 → ausencia | archivo
    ALTER TABLE mantenimiento.buses_inmovilizados
        ADD COLUMN IF NOT EXISTS visto_en_archivo            DATE,
        ADD COLUMN IF NOT EXISTS visto_en_cargue             TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS fecha_habilitacion_estimada TIMESTAMP,
        ADD COLUMN IF NOT EXISTS estado_ciclo                TEXT DEFAULT 'abierto',
        ADD COLUMN IF NOT EXISTS cerrado_por                 TEXT;

    UPDATE mantenimiento.buses_inmovilizados
    SET visto_en_archivo = fecha_archivo
    WHERE visto_en_archivo IS NULL AND fecha_archivo IS NOT NULL;

    UPDATE mantenimiento.buses_inmovilizados
    SET estado_ciclo = CASE WHEN fecha_habilitacion IS NULL THEN 'abierto' ELSE 'cerrado' END
    WHERE estado_ciclo IS NULL;

    CREATE INDEX IF NOT EXISTS idx_inmov_ciclo
        ON mantenimiento.buses_inmovilizados (origen, estado_ciclo);

    -- Bitácora de transiciones: qué cambió, en qué cargue y por qué
    CREATE TABLE IF NOT EXISTS mantenimiento.buses_inmovilizados_eventos (
        id                   BIGSERIAL PRIMARY KEY,
        id_inmovilizacion    BIGINT    REFERENCES mantenimiento.buses_inmovilizados (id) ON DELETE CASCADE,
        id_bus               BIGINT,
        placa                TEXT      NOT NULL,
        fecha_inmovilizacion TIMESTAMP NOT NULL,
        evento               TEXT      NOT NULL,   -- inmovilizado | actualizado | habilitado | reabierto
        origen               TEXT,
        archivo              TEXT,
        fecha_archivo        DATE,
        detalle              TEXT,
        ejecutado_por        TEXT,
        creado_en            TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_inmov_ev_inm ON mantenimiento.buses_inmovilizados_eventos (id_inmovilizacion);
    CREATE INDEX IF NOT EXISTS idx_inmov_ev_bus ON mantenimiento.buses_inmovilizados_eventos (id_bus, creado_en DESC);
"""


def asegurar_tablas(cur):
    cur.execute(DDL_LOG)
    cur.execute(DDL_DATA)


def asegurar_registro_log(cur, fecha_archivo: date, origen: str):
    cur.execute("""
        INSERT INTO log.procesa_buses_inmovilizados (fecha_archivo, origen)
        VALUES (%s, %s)
        ON CONFLICT (fecha_archivo, origen) DO NOTHING;
    """, (fecha_archivo, origen))


def marcar_inicio(cur, fecha_archivo: date, origen: str, archivo: str | None,
                  ruta_blob: str | None, ejecutado_por: str):
    cur.execute("""
        UPDATE log.procesa_buses_inmovilizados
        SET estado             = 'pendiente',
            intentos           = intentos + 1,
            ultima_ejecucion   = NOW(),
            archivo            = %s,
            ruta_blob          = %s,
            ejecutado_por      = %s,
            mensaje            = NULL,
            duracion_seg       = NULL,
            filas_archivo      = NULL,
            filas_nuevas       = NULL,
            filas_actualizadas = NULL,
            filas_omitidas     = NULL,
            filas_cerradas     = NULL,
            filas_sin_bus      = NULL,
            actualizado_en     = NOW()
        WHERE fecha_archivo = %s AND origen = %s;
    """, (archivo, ruta_blob, ejecutado_por, fecha_archivo, origen))


def marcar_resultado(cur, fecha_archivo: date, origen: str, estado: str,
                     duracion_seg: int, meta: dict, mensaje: str | None):
    cur.execute("""
        UPDATE log.procesa_buses_inmovilizados
        SET estado             = %s,
            duracion_seg       = %s,
            filas_archivo      = %s,
            filas_nuevas       = %s,
            filas_actualizadas = %s,
            filas_omitidas     = %s,
            filas_cerradas     = %s,
            filas_sin_bus      = %s,
            mensaje            = %s,
            actualizado_en     = NOW()
        WHERE fecha_archivo = %s AND origen = %s;
    """, (
        estado,
        duracion_seg,
        meta.get("filas_archivo"),
        meta.get("filas_nuevas"),
        meta.get("filas_actualizadas"),
        meta.get("filas_omitidas"),
        meta.get("filas_cerradas"),
        meta.get("filas_sin_bus"),
        mensaje,
        fecha_archivo,
        origen,
    ))


COLUMNAS_INSERT = [
    "id_bus", "placa", "concesionario_operacion", "origen", "tipo_servicio",
    "fecha_inmovilizacion", "causa_inmovilizacion", "descripcion_novedad", "creador",
    "fecha_revision", "revisor", "descripcion_revision", "fecha_habilitacion",
    "habilitador", "archivo", "fecha_archivo", "hash_fila", "visto_en_archivo",
]

COLUMNAS_EVENTO = [
    "id_inmovilizacion", "id_bus", "placa", "fecha_inmovilizacion", "evento",
    "origen", "archivo", "fecha_archivo", "detalle", "ejecutado_por",
]


def _registrar_eventos(cur, eventos: list[dict], page_size: int = 500):
    """Bitácora de transiciones. Se escribe en el mismo lote que la carga."""
    if not eventos:
        return
    execute_values(
        cur,
        f"INSERT INTO {PG_SCHEMA}.buses_inmovilizados_eventos "
        f"({', '.join(COLUMNAS_EVENTO)}) VALUES %s",
        [tuple(e.get(c) for c in COLUMNAS_EVENTO) for e in eventos],
        page_size=page_size,
    )


def _claves_cerradas(cur, filas: list[dict]) -> set:
    """
    Ciclos que estaban cerrados y vuelven a venir en el archivo: son
    reaperturas y se marcan como tales antes de que el UPSERT los reabra.
    """
    cur.execute(
        f"""
        SELECT placa, fecha_inmovilizacion
        FROM {PG_SCHEMA}.{PG_TABLE}
        WHERE estado_ciclo = 'cerrado'
          AND (placa, fecha_inmovilizacion) IN (
              SELECT * FROM unnest(%s::text[], %s::timestamp[])
          );
        """,
        ([f["placa"] for f in filas], [f["fecha_inmovilizacion"] for f in filas]),
    )
    return {(r[0], r[1]) for r in cur.fetchall()}


def cargar_a_postgresql(filas: list[dict], fuente: dict, archivo: dict, inicio_cargue,
                        ejecutado_por: str = "workflow", page_size: int = 1000) -> dict:
    """
    UPSERT por (placa, fecha_inmovilizacion):
      · clave nueva          → INSERT                  (filas_nuevas)
      · hash distinto        → UPDATE                  (filas_actualizadas)
      · hash igual           → solo se marca como visto (filas_omitidas)

    Toda fila presente en el archivo se marca como vista en este cargue
    (`visto_en_cargue`), porque es justamente lo que NO recibe esa marca lo que
    después se cierra por ausencia. El contenido en sí — y `actualizado_en` —
    solo se reescribe cuando el hash cambió, de modo que el refresco horario no
    altera el dato ni el histórico.

    `xmax = 0` distingue el INSERT; `actualizado_en = NOW()` marca el cambio
    real, ya que NOW() es constante dentro de la transacción.
    """
    vacio = {"filas_nuevas": 0, "filas_actualizadas": 0, "filas_omitidas": 0, "filas_reabiertas": 0}
    if not filas:
        return vacio

    valores = [tuple(f.get(c) for c in COLUMNAS_INSERT) + (inicio_cargue,) for f in filas]

    sql = f"""
        INSERT INTO {PG_SCHEMA}.{PG_TABLE} AS t ({", ".join(COLUMNAS_INSERT)}, visto_en_cargue)
        VALUES %s
        ON CONFLICT (placa, fecha_inmovilizacion) DO UPDATE
        SET id_bus                  = EXCLUDED.id_bus,
            concesionario_operacion = EXCLUDED.concesionario_operacion,
            origen                  = EXCLUDED.origen,
            tipo_servicio           = EXCLUDED.tipo_servicio,
            causa_inmovilizacion    = EXCLUDED.causa_inmovilizacion,
            descripcion_novedad     = EXCLUDED.descripcion_novedad,
            creador                 = EXCLUDED.creador,
            fecha_revision          = EXCLUDED.fecha_revision,
            revisor                 = EXCLUDED.revisor,
            descripcion_revision    = EXCLUDED.descripcion_revision,
            fecha_habilitacion      = EXCLUDED.fecha_habilitacion,
            habilitador             = EXCLUDED.habilitador,
            hash_fila               = EXCLUDED.hash_fila,
            archivo                 = CASE WHEN t.hash_fila IS DISTINCT FROM EXCLUDED.hash_fila
                                           THEN EXCLUDED.archivo ELSE t.archivo END,
            fecha_archivo           = CASE WHEN t.hash_fila IS DISTINCT FROM EXCLUDED.hash_fila
                                           THEN EXCLUDED.fecha_archivo ELSE t.fecha_archivo END,
            actualizado_en          = CASE WHEN t.hash_fila IS DISTINCT FROM EXCLUDED.hash_fila
                                           THEN NOW() ELSE t.actualizado_en END,
            visto_en_archivo        = GREATEST(COALESCE(t.visto_en_archivo, t.fecha_archivo),
                                               EXCLUDED.visto_en_archivo),
            -- marca de este cargue: lo que no la reciba es lo que se cerrará
            visto_en_cargue         = NOW(),
            -- el bus volvió a estar en el archivo: el ciclo sigue (o vuelve a estar) abierto
            estado_ciclo                = 'abierto',
            fecha_habilitacion_estimada = NULL,
            cerrado_por                 = NULL
        RETURNING id, id_bus, placa, fecha_inmovilizacion,
                  (xmax = 0) AS insertado, (actualizado_en = NOW()) AS cambiado
    """

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            reabiertas = _claves_cerradas(cur, filas)
            resultado = execute_values(cur, sql, valores, page_size=page_size, fetch=True)

            eventos = []
            for id_inm, id_bus, placa, fecha_inm, insertado, cambiado in resultado:
                if insertado:
                    evento, detalle = "inmovilizado", "Aparece en el archivo por primera vez"
                elif (placa, fecha_inm) in reabiertas:
                    evento, detalle = "reabierto", "Vuelve a aparecer en el archivo tras haberse cerrado"
                elif cambiado:
                    evento, detalle = "actualizado", "Cambió algún dato de la inmovilización"
                else:
                    continue          # solo se refrescó el último corte visto
                eventos.append({
                    "id_inmovilizacion": id_inm, "id_bus": id_bus, "placa": placa,
                    "fecha_inmovilizacion": fecha_inm, "evento": evento,
                    "origen": fuente["origen"], "archivo": archivo["archivo"],
                    "fecha_archivo": archivo["fecha_archivo"], "detalle": detalle,
                    "ejecutado_por": ejecutado_por,
                })
            _registrar_eventos(cur, eventos)
        conn.commit()

    nuevas = sum(1 for r in resultado if r[4])
    cambiadas = sum(1 for r in resultado if not r[4] and r[5])
    reaperturas = sum(1 for r in resultado if not r[4] and (r[2], r[3]) in reabiertas)

    return {
        "filas_nuevas":       nuevas,
        "filas_actualizadas": cambiadas,
        "filas_omitidas":     len(filas) - nuevas - cambiadas,
        "filas_reabiertas":   reaperturas,
    }


def _archivo_sospechoso(cur, fuente: dict, fecha_archivo: date, filas_actuales: int) -> str | None:
    """
    Un archivo recortado (un extracto subido por error, una descarga a medias)
    cerraría de golpe cientos de inmovilizaciones vigentes. Si trae menos de la
    mitad de las filas del último cargue correcto de esa misma fuente, se cargan
    los datos pero no se cierra nada. La comparación es contra el cargue
    anterior, así que una caída real y sostenida deja de bloquear al día
    siguiente.
    """
    cur.execute("""
        SELECT filas_archivo
        FROM log.procesa_buses_inmovilizados
        WHERE origen = %s AND estado = 'ok' AND fecha_archivo < %s
          AND filas_archivo IS NOT NULL AND filas_archivo > 0
        ORDER BY fecha_archivo DESC
        LIMIT 1;
    """, (fuente["origen"], fecha_archivo))
    fila = cur.fetchone()
    if not fila:
        return None

    anterior = fila[0]
    if filas_actuales >= anterior * 0.5:
        return None
    return (f"El archivo trae {filas_actuales} filas frente a {anterior} del cargue anterior: "
            f"no se cierran inmovilizaciones por ausencia para evitar habilitar buses "
            f"con un archivo incompleto.")


def cerrar_ciclos_ausentes(fuente: dict, archivo: dict, filas_actuales: int,
                           inicio_cargue, ejecutado_por: str = "workflow") -> tuple[int, str | None]:
    """
    Cierra los ciclos de esta fuente que no vinieron en el cargue recién hecho.

    El bus habilitado desaparece del archivo, así que la ausencia es la única
    señal de habilitación disponible. Se cierra con el momento del corte en el
    que ya no aparece y los días quedan congelados ahí.

    Se compara contra `visto_en_cargue`, no contra el día: el archivo se
    sobrescribe cada hora, así que un bus habilitado a media mañana debe
    cerrarse en el cargue siguiente y no esperar al día siguiente.

    Protecciones:
      · un archivo vacío o ilegible no cierra nada;
      · uno recortado tampoco (ver `_archivo_sospechoso`);
      · reprocesar un corte antiguo tampoco: no se tocan los ciclos vistos en
        un archivo posterior al que se está cargando.
    """
    corte = archivo["fecha_archivo"]
    if not filas_actuales or corte is None:
        return 0, None

    # El cierre se fecha en el corte; dentro del día vale la hora del cargue.
    # Si el bus ya volvió a inmovilizarse, el ciclo no pudo terminar después de
    # esa nueva entrada: ese inicio acota el cierre y evita ciclos solapados.
    sql = f"""
        UPDATE {PG_SCHEMA}.{PG_TABLE} AS t
        SET estado_ciclo                = 'cerrado',
            cerrado_por                 = 'ausencia',
            fecha_habilitacion_estimada = GREATEST(
                LEAST((now() AT TIME ZONE 'America/Bogota'),
                      %s::date + INTERVAL '1 day' - INTERVAL '1 second',
                      COALESCE((SELECT MIN(s.fecha_inmovilizacion)
                                FROM {PG_SCHEMA}.{PG_TABLE} s
                                WHERE s.placa = t.placa
                                  AND s.fecha_inmovilizacion > t.fecha_inmovilizacion),
                               'infinity'::timestamp)),
                t.fecha_inmovilizacion),
            actualizado_en              = NOW()
        WHERE t.origen       = %s
          AND t.estado_ciclo = 'abierto'
          AND t.fecha_habilitacion IS NULL
          AND COALESCE(t.visto_en_archivo, t.fecha_archivo) <= %s::date
          AND (t.visto_en_cargue IS NULL OR t.visto_en_cargue < %s::timestamptz)
        RETURNING t.id, t.id_bus, t.placa, t.fecha_inmovilizacion,
                  COALESCE(t.visto_en_archivo, t.fecha_archivo)
    """

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            aviso = _archivo_sospechoso(cur, fuente, corte, filas_actuales)
            if aviso:
                _log(f"     ⚠️  {aviso}")
                return 0, aviso

            cur.execute(sql, (corte, fuente["origen"], corte, inicio_cargue))
            cerrados = cur.fetchall()

            _registrar_eventos(cur, [{
                "id_inmovilizacion": c[0], "id_bus": c[1], "placa": c[2],
                "fecha_inmovilizacion": c[3], "evento": "habilitado",
                "origen": fuente["origen"], "archivo": archivo["archivo"],
                "fecha_archivo": corte, "ejecutado_por": ejecutado_por,
                "detalle": f"Ya no aparece en el archivo del {corte}; "
                           f"visto por última vez el {c[4]}",
            } for c in cerrados])
        conn.commit()

    return len(cerrados), None


# ============================================================
# 5) PROCESAR UN ARCHIVO (blob o subido por el usuario)
# ============================================================
def procesar_contenido(datos: bytes, fuente: dict, archivo: dict, catalogo_placas: dict,
                       ejecutado_por: str = "workflow", verbose: bool = False) -> dict:
    """
    Registra en el log, transforma y carga el contenido de un .xlsx.
    Es el núcleo común del proceso automático (blob) y del cargue manual.
    """
    t0 = time.time()
    etiqueta = fuente["etiqueta"]
    fecha_ref = archivo["fecha_archivo"] or _hoy()
    # El corte queda normalizado: es la referencia del avistamiento y del cierre
    archivo = {**archivo, "fecha_archivo": fecha_ref}

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            asegurar_registro_log(cur, fecha_ref, fuente["origen"])
            marcar_inicio(cur, fecha_ref, fuente["origen"], archivo["archivo"],
                          archivo.get("ruta_blob"), ejecutado_por)
            # Reloj de la base: separa lo que este cargue toca de lo que no
            cur.execute("SELECT clock_timestamp();")
            inicio_cargue = cur.fetchone()[0]
        conn.commit()

    try:
        df_raw = leer_excel_bytes(datos, referencia=archivo["archivo"], verbose=verbose)
        filas, meta = transformar_datos(df_raw, fuente, archivo, catalogo_placas, verbose=verbose)
        meta.update(cargar_a_postgresql(filas, fuente, archivo, inicio_cargue, ejecutado_por))
        # Los que no vinieron en este cargue ya no están inmovilizados
        meta["filas_cerradas"], aviso = cerrar_ciclos_ausentes(
            fuente, archivo, len(filas), inicio_cargue, ejecutado_por)

        dur = int(time.time() - t0)
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                marcar_resultado(cur, fecha_ref, fuente["origen"], "ok", dur, meta, aviso)
            conn.commit()

        _log(f"     ✅ {meta['filas_nuevas']} nuevas | {meta['filas_actualizadas']} actualizadas "
              f"| {meta['filas_omitidas']} sin cambios | {meta.get('filas_reabiertas', 0)} reabiertas "
              f"| {meta['filas_cerradas']} habilitadas (ausentes) | {meta['filas_sin_bus']} sin bus")

        return {
            "origen": fuente["origen"], "etiqueta": etiqueta, "estado": "ok",
            "archivo": archivo["archivo"], "fecha_archivo": str(fecha_ref),
            "mensaje": aviso, **meta,
        }

    except Exception:
        dur = int(time.time() - t0)
        err = traceback.format_exc()
        meta_err = {"filas_archivo": 0, "filas_nuevas": 0, "filas_actualizadas": 0,
                    "filas_omitidas": 0, "filas_sin_bus": 0, "filas_cerradas": 0,
                    "filas_reabiertas": 0}
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                marcar_resultado(cur, fecha_ref, fuente["origen"], "error", dur, meta_err, err[:4000])
            conn.commit()

        _log(f"     ❌ {etiqueta} ERROR: {err[:200]}...")
        return {
            "origen": fuente["origen"], "etiqueta": etiqueta, "estado": "error",
            "archivo": archivo["archivo"], "fecha_archivo": str(fecha_ref),
            "mensaje": err[:500], **meta_err,
        }


def procesar_fuente(cliente_contenedor, fuente: dict, catalogo_placas: dict,
                    anio: int, fecha_archivo: date | None = None,
                    ejecutado_por: str = "workflow", verbose: bool = False) -> dict:
    """Ubica el archivo vigente de una carpeta del blob y lo procesa."""
    etiqueta = fuente["etiqueta"]
    archivo = obtener_archivo_vigente(cliente_contenedor, fuente, anio, fecha_archivo)

    if archivo is None:
        _log(f"  ⚠️  {etiqueta}: sin archivos en la carpeta")
        return {
            "origen": fuente["origen"], "etiqueta": etiqueta, "estado": "sin_archivos",
            "archivo": None, "fecha_archivo": None, "filas_archivo": 0,
            "filas_nuevas": 0, "filas_actualizadas": 0, "filas_omitidas": 0,
            "filas_sin_bus": 0, "mensaje": "No hay archivos .xlsx en la carpeta",
        }

    _log(f"  📄 {etiqueta}: {archivo['archivo']}")
    datos = cliente_contenedor.get_blob_client(archivo["ruta_blob"]).download_blob().readall()
    return procesar_contenido(datos, fuente, archivo, catalogo_placas, ejecutado_por, verbose)


# ============================================================
# 5b) CARGUE MANUAL DE ARCHIVOS SUBIDOS DESDE LA APLICACIÓN
# ============================================================
def inferir_origen(nombre_archivo: str) -> str | None:
    """
    Deduce la fuente a partir del nombre del archivo, p. ej.
    '20260706_vehiculos_inmovilizados_zonal_sc.xlsx' → 'zonal_sc'.
    """
    nombre = (nombre_archivo or "").lower()
    for f in FUENTES:
        if nombre.endswith(f"_{f['origen']}.xlsx") or f"_{f['origen']}." in nombre:
            return f["origen"]
    return None


def procesar_archivos_subidos(archivos: list[dict], ejecutado_por: str = "app",
                              verbose: bool = False) -> dict:
    """
    Procesa archivos .xlsx cargados manualmente desde la aplicación.

    `archivos` = [{"nombre": str, "contenido": bytes, "origen": str | None}]
    Si `origen` viene vacío se deduce del nombre; si tampoco se puede deducir,
    ese archivo se reporta con estado 'error' y los demás continúan.
    """
    if not archivos:
        raise ValueError("No se recibieron archivos para cargar")

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            asegurar_tablas(cur)
        conn.commit()

    catalogo_placas = cargar_catalogo_placas()
    _log(f"✅ Catálogo de placas cargado: {len(catalogo_placas):,} buses")

    resultados = []
    for item in archivos:
        nombre = item.get("nombre") or "archivo.xlsx"
        origen = (item.get("origen") or "").strip() or inferir_origen(nombre)
        fuente = next((f for f in FUENTES if f["origen"] == origen), None)

        if fuente is None:
            _log(f"  ⚠️  {nombre}: no se pudo determinar la fuente")
            resultados.append({
                "origen": origen or "desconocido", "etiqueta": nombre, "estado": "error",
                "archivo": nombre, "fecha_archivo": None,
                "mensaje": "No se pudo determinar la fuente del archivo. "
                           "Seleccione manualmente a qué concesión y servicio corresponde.",
                "filas_archivo": 0, "filas_nuevas": 0, "filas_actualizadas": 0,
                "filas_omitidas": 0, "filas_sin_bus": 0,
            })
            continue

        archivo = {
            "archivo":       nombre,
            "ruta_blob":     None,   # cargue manual: no viene del blob
            "fecha_archivo": _fecha_desde_nombre(nombre) or _hoy(),
        }
        _log(f"  📄 {fuente['etiqueta']} (manual): {nombre}")
        resultados.append(
            procesar_contenido(item["contenido"], fuente, archivo, catalogo_placas,
                               ejecutado_por, verbose)
        )

    return _resumir(resultados)


def _resumir(resultados: list[dict]) -> dict:
    resumen = {
        "fuentes":             len(resultados),
        "fuentes_ok":          sum(1 for r in resultados if r["estado"] == "ok"),
        "fuentes_error":       sum(1 for r in resultados if r["estado"] == "error"),
        "fuentes_sin_archivo": sum(1 for r in resultados if r["estado"] == "sin_archivos"),
        "filas_nuevas":        sum(r.get("filas_nuevas", 0) for r in resultados),
        "filas_actualizadas":  sum(r.get("filas_actualizadas", 0) for r in resultados),
        "filas_omitidas":      sum(r.get("filas_omitidas", 0) for r in resultados),
        "filas_cerradas":      sum(r.get("filas_cerradas", 0) for r in resultados),
        "filas_reabiertas":    sum(r.get("filas_reabiertas", 0) for r in resultados),
        "filas_sin_bus":       sum(r.get("filas_sin_bus", 0) for r in resultados),
        "detalle":             resultados,
    }
    _log(f"\n📊 Resumen: {resumen['filas_nuevas']} nuevas | {resumen['filas_actualizadas']} actualizadas "
          f"| {resumen['filas_omitidas']} sin cambios | {resumen['filas_cerradas']} habilitadas "
          f"| {resumen['filas_reabiertas']} reabiertas | {resumen['fuentes_error']} con error")
    return resumen


# ============================================================
# 6) ORQUESTADOR
# ============================================================
def ejecutar_job_buses_inmovilizados(origenes: list[str] | None = None,
                                     anio: int | None = None,
                                     fecha_archivo: date | None = None,
                                     ejecutado_por: str = "workflow",
                                     verbose: bool = False) -> dict:
    """
    Procesa las fuentes indicadas (por defecto las 4).
    Se usa igual desde el workflow horario y desde la carga manual de la app.
    """
    anio = anio or _hoy().year

    seleccionadas = [f for f in FUENTES if not origenes or f["origen"] in origenes]
    if not seleccionadas:
        raise ValueError(f"Orígenes no válidos. Disponibles: {ORIGENES_VALIDOS}")

    _log(f"🚌 Buses inmovilizados | año {anio} | fuentes: {[f['etiqueta'] for f in seleccionadas]}")

    cliente_contenedor = obtener_cliente_contenedor()

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            asegurar_tablas(cur)
        conn.commit()
    _log("✅ Tablas verificadas/creadas")

    catalogo_placas = cargar_catalogo_placas()
    _log(f"✅ Catálogo de placas cargado: {len(catalogo_placas):,} buses")

    resultados = [
        procesar_fuente(cliente_contenedor, fuente, catalogo_placas, anio,
                        fecha_archivo=fecha_archivo, ejecutado_por=ejecutado_por, verbose=verbose)
        for fuente in seleccionadas
    ]

    return _resumir(resultados)


# ============================================================
# 7) CLI
# ============================================================
if __name__ == "__main__":
    origenes_env = os.getenv("INMOV_ORIGENES", "").strip()
    origenes_cli = [o.strip() for o in origenes_env.split(",") if o.strip()] or None
    anio_cli     = int(os.getenv("INMOV_ANIO", "0")) or None
    verbose_cli  = os.getenv("INMOV_VERBOSE", "0") == "1"

    ejecutar_job_buses_inmovilizados(
        origenes=origenes_cli,
        anio=anio_cli,
        ejecutado_por=os.getenv("GITHUB_ACTOR", "workflow"),
        verbose=verbose_cli,
    )
