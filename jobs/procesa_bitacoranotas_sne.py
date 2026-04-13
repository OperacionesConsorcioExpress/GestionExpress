from __future__ import annotations

import os
import re
import csv
import time
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try:
    from database.database_manager import get_db_connection
except Exception:
    from database_manager import get_db_connection


# =============================================================================
# CONFIG
# =============================================================================

FECHA_SEMILLA_STR = "01/04/2026"  # dd/mm/yyyy
PROCESS_DATE_STR = ""

AZURE_CONN_ENV = "AZURE_STORAGE_CONNECTION_STRING"
AZURE_CONTAINER_ICS = "0001-archivos-de-apoyo-descargas-cex-fms"
AZURE_CONTAINER_FMS = "e01-fms"

PG_SCHEMA_NAME = "sne"
PG_TABLE_NAME = "bitacora_notas"
PG_SCHEMA_ICS = "sne"
PG_TABLE_ICS = "ics"
PG_TABLE_ICS_ID_COLUMN = "id_ics"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"
PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"
DEFAULT_ID_REPORTE = 10
NOMBRE_REPORTE_LOG = "Tabla Bitacora_notas"

PG_BATCH_SIZE = 5000
DELETE_EXISTING_FOR_DATE = True

TIPOS_NOTA_VALIDOS = {
    "INTERRUPCION DE SERVICIO ZONAL",
    "NOVEDADES KILOMETRAJE",
    "NOVEDADES SIRCI",
    "NOVEDADES VEHICULOS ZONALES",
    "NOVEDADES VEHICULOS TRONCALES",
}


# =============================================================================
# HELPERS
# =============================================================================

class DataIO:
    @staticmethod
    def limpiar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()

        def _clean_col(c: str) -> str:
            s = str(c)
            s = s.replace("\ufeff", "").replace('"', "").strip()
            s = re.sub(r"^[\uFEFF\u200B\u200C\u200D\u2060]+", "", s).strip()
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df.columns = [_clean_col(c) for c in df.columns]
        return df

    @staticmethod
    def _sniff_sep(sample_text: str, default: str = ",") -> str:
        try:
            dialect = csv.Sniffer().sniff(sample_text, delimiters=[",", ";", "\t", "|"])
            return dialect.delimiter
        except Exception:
            return default

    @classmethod
    def leer_csv_desde_bytes(cls, content: bytes, dtype=str) -> pd.DataFrame:
        sample_bytes = content[: 64 * 1024]

        sample_text = None
        used_encoding = None
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                sample_text = sample_bytes.decode(enc, errors="strict")
                used_encoding = enc
                break
            except Exception:
                continue

        if sample_text is None:
            sample_text = sample_bytes.decode("latin-1", errors="replace")
            used_encoding = "latin-1"

        sep = cls._sniff_sep(sample_text, default=",")
        bio = BytesIO(content)

        for kwargs in (
            dict(sep=sep, encoding=used_encoding, low_memory=False, dtype=dtype),
            dict(sep=sep, encoding="latin-1", low_memory=False, dtype=dtype),
            dict(sep=sep, encoding=used_encoding, engine="python", dtype=dtype),
            dict(sep=sep, encoding="latin-1", engine="python", dtype=dtype),
        ):
            try:
                bio.seek(0)
                df = pd.read_csv(bio, **kwargs)
                return cls.limpiar_columnas(df)
            except Exception:
                continue

        bio.seek(0)
        df = pd.read_csv(bio, sep=sep, encoding="latin-1", engine="python", dtype=dtype)
        return cls.limpiar_columnas(df)


def normalize_name(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("\ufeff", "")
    s = (
        s.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def pick_col(df: pd.DataFrame, aliases: List[str], required: bool = True) -> Optional[str]:
    cols = {str(c).strip(): c for c in df.columns}
    normalized = {normalize_name(k): v for k, v in cols.items()}

    for alias in aliases:
        if alias in cols:
            return cols[alias]

    for alias in aliases:
        na = normalize_name(alias)
        if na in normalized:
            return normalized[na]

    if required:
        raise KeyError(f"No se encontró ninguna de estas columnas: {aliases}")
    return None


def to_int64(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def clean_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


def normalize_text_upper(series: pd.Series) -> pd.Series:
    return clean_text(series).fillna("").str.upper()


def parse_fecha_hora(series: pd.Series) -> pd.Series:
    s = clean_text(series)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    mask_iso = s.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$", na=False)
    if mask_iso.any():
        s_iso = s[mask_iso]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            parsed = pd.to_datetime(s_iso, format=fmt, errors="coerce")
            ok = parsed.notna() & out.loc[s_iso.index].isna()
            if ok.any():
                out.loc[s_iso.index[ok]] = parsed.loc[ok]

    mask_na = out.isna() & s.notna()
    if mask_na.any():
        out.loc[mask_na] = pd.to_datetime(s[mask_na], errors="coerce", dayfirst=True)

    return out


# =============================================================================
# AZURE
# =============================================================================

@dataclass(frozen=True)
class AzureConfig:
    connection_string: str
    container_name: str


class AzureBlobReader:
    def __init__(self, cfg: AzureConfig):
        self.cfg = cfg
        self.service = BlobServiceClient.from_connection_string(cfg.connection_string)
        self.container = self.service.get_container_client(cfg.container_name)

    def exists(self, blob_path: str) -> bool:
        try:
            bc = self.container.get_blob_client(blob_path)
            bc.get_blob_properties()
            return True
        except Exception:
            return False

    def read_bytes(self, blob_path: str, timeout: int = 600) -> bytes:
        bc = self.container.get_blob_client(blob_path)
        stream = bc.download_blob(timeout=timeout, max_concurrency=8, validate_content=False)
        return stream.readall()

    def list_blob_paths(self, prefix: str) -> List[str]:
        out: List[str] = []
        for b in self.container.list_blobs(name_starts_with=prefix):
            if b.name and not b.name.endswith("/"):
                out.append(b.name)
        return out


# =============================================================================
# PROCESO
# =============================================================================

class BitacoraNotasBuilder:
    def __init__(self, az_ics: AzureBlobReader, az_fms: AzureBlobReader):
        self.az_ics = az_ics
        self.az_fms = az_fms
        self.io = DataIO()

    def _blob_path_ics(self, fecha: datetime) -> Tuple[str, str]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")
        nombre = f"ICS_SMART OPERATOR_Etapa1_{fecha_txt}.csv"
        ruta = f"0001-26-fms-ics/{anio}/{mes}/{nombre}"
        return ruta, nombre

    def load_ics(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ICS")
        print("=" * 80)

        ruta, nombre = self._blob_path_ics(fecha)
        if not self.az_ics.exists(ruta):
            raise FileNotFoundError(f"No existe ICS en Azure: {ruta}")

        df = self.io.leer_csv_desde_bytes(self.az_ics.read_bytes(ruta), dtype=str)
        print(f"ICS cargado: {nombre} | filas={len(df)} cols={len(df.columns)}")

        c_linea = pick_col(df, ["Linea SAE", "Linea", "Línea SAE"])
        c_idics = pick_col(df, ["IdICS"])

        out = df.copy()
        out["linea"] = to_int64(out[c_linea])
        out["id_ics"] = to_int64(out[c_idics])
        out = out[["id_ics", "linea"]].dropna(subset=["linea", "id_ics"]).drop_duplicates().copy()

        print(f"ICS listo para cruce | filas={len(out)}")
        return out

    def _prefixes_detalle_notas(self, fecha: datetime) -> List[str]:
        anio = fecha.strftime("%Y")
        return [
            f"2_uq/{anio}/80_detalle_notas_zonal_uq/",
            f"2_uq/{anio}/81_detalle_notas_troncal_uq/",
            f"1_sc/{anio}/80_detalle_notas_zonal_sc/",
            f"1_sc/{anio}/81_detalle_notas_troncal_sc/",
        ]

    def _find_files_by_date(self, prefixes: List[str], fecha: datetime) -> List[str]:
        fecha_yyyymmdd = fecha.strftime("%Y%m%d")
        encontrados: List[str] = []
        for prefix in prefixes:
            blobs = self.az_fms.list_blob_paths(prefix)
            encontrados.extend([b for b in blobs if os.path.basename(b).startswith(fecha_yyyymmdd)])
        return sorted(set(encontrados))

    def load_detalle_notas(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("2) CARGANDO DETALLE DE NOTAS")
        print("=" * 80)

        files = self._find_files_by_date(self._prefixes_detalle_notas(fecha), fecha)
        if not files:
            raise FileNotFoundError(
                f"No se encontraron archivos de detalle de notas para {fecha.strftime('%Y-%m-%d')}"
            )

        frames: List[pd.DataFrame] = []
        for blob_path in files:
            nombre = os.path.basename(blob_path)
            df0 = self.io.leer_csv_desde_bytes(self.az_fms.read_bytes(blob_path), dtype=str)
            df0["__archivo_origen__"] = nombre
            frames.append(df0)
            print(f"Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_tipo_nota = pick_col(df, ["Tipo Nota", "Tipo de Nota"])
        c_id_linea = pick_col(df, ["Id de linea", "Id de línea", "Id linea", "Id línea"])

        out = df.copy()
        out["tipo_nota_norm"] = normalize_text_upper(out[c_tipo_nota])
        out["linea"] = to_int64(out[c_id_linea])

        mapa_tipo = {
            "INTERRUPCION DE SERVICIO ZONAL": "INTERRUPCION DE SERVICIO ZONAL",
            "NOVEDADES KILOMETRAJE": "NOVEDADES KILOMETRAJE",
            "NOVEDADES SIRCI": "NOVEDADES SIRCI",
            "NOVEDADES VEHICULOS ZONALES": "NOVEDADES VEHICULOS ZONALES",
            "NOVEDADES VEHÍCULOS ZONALES": "NOVEDADES VEHICULOS ZONALES",
            "NOVEDADES VEHICULOS TRONCALES": "NOVEDADES VEHICULOS TRONCALES",
            "NOVEDADES VEHÍCULOS TRONCALES": "NOVEDADES VEHICULOS TRONCALES",
        }
        out["tipo_nota_norm2"] = out["tipo_nota_norm"].replace(mapa_tipo)
        out = out[out["tipo_nota_norm2"].isin(TIPOS_NOTA_VALIDOS)].copy()
        out = out.dropna(subset=["linea"]).copy()

        print(f"Detalle notas filtrado por tipo | filas={len(out)}")
        return out

    def build(self, fecha: datetime) -> pd.DataFrame:
        df_ics = self.load_ics(fecha)
        df_notas = self.load_detalle_notas(fecha)

        print("\n" + "=" * 80)
        print("3) CRUZANDO ICS VS DETALLE NOTAS")
        print("=" * 80)

        df_merge = df_notas.merge(df_ics, on="linea", how="inner")

        c_fecha = pick_col(df_merge, ["Fecha"])
        c_concesion = pick_col(df_merge, ["Concesion", "Concesión"])
        c_vehiculo = pick_col(df_merge, ["N Vehiculo", "N° Vehiculo", "N° Vehículo", "No Vehiculo", "Vehiculo"])
        c_id_nota = pick_col(df_merge, ["ID Nota", "Id Nota"])
        c_tipo_nota = pick_col(df_merge, ["Tipo Nota"])
        c_subtipo_nota = pick_col(df_merge, ["Subtipo Nota"], required=False)
        c_observaciones = pick_col(df_merge, ["Observaciones"], required=False)

        out = pd.DataFrame({
            "id_ics": to_int64(df_merge["id_ics"]),
            "fecha_hora": parse_fecha_hora(df_merge[c_fecha]),
            "concesion": clean_text(df_merge[c_concesion]),
            "linea": to_int64(df_merge["linea"]),
            "vehiculo": to_int64(df_merge[c_vehiculo]),
            "id_nota": to_int64(df_merge[c_id_nota]),
            "tipo_nota": clean_text(df_merge[c_tipo_nota]),
            "subtipo_nota": clean_text(df_merge[c_subtipo_nota]) if c_subtipo_nota else pd.NA,
            "observaciones": clean_text(df_merge[c_observaciones]) if c_observaciones else pd.NA,
        })

        out = out.dropna(subset=["id_ics", "fecha_hora", "linea", "id_nota"]).copy()
        out = out.drop_duplicates(
            subset=[
                "id_ics",
                "fecha_hora",
                "concesion",
                "linea",
                "vehiculo",
                "id_nota",
                "tipo_nota",
                "subtipo_nota",
                "observaciones",
            ],
            keep="first",
        ).reset_index(drop=True)

        print(f"Cruce final listo | filas={len(out)}")
        return out


# =============================================================================
# POSTGRES
# =============================================================================

class PostgresBitacoraNotasLoader:
    DB_COLUMNS = [
        "id_ics",
        "fecha_hora",
        "concesion",
        "linea",
        "vehiculo",
        "id_nota",
        "tipo_nota",
        "subtipo_nota",
        "observaciones",
    ]

    def __init__(
        self,
        schema: str,
        table: str,
        batch_size: int = 5000,
        schema_ics: str = PG_SCHEMA_ICS,
        table_ics: str = PG_TABLE_ICS,
        ics_id_column: str = PG_TABLE_ICS_ID_COLUMN,
    ):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.schema_ics = schema_ics
        self.table_ics = table_ics
        self.ics_id_column = ics_id_column

    @staticmethod
    def _py(v):
        if v is None:
            return None
        try:
            if v is pd.NA or pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, np.generic):
            return v.item()
        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            return v.to_pydatetime()
        if isinstance(v, str):
            s = v.strip()
            return None if s == "" or s.lower() == "nat" else s
        return v

    def _prepare_df_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.DB_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(f"Faltan columnas en DF para insertar a Postgres: {missing}")

        d = df[self.DB_COLUMNS].copy()
        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce").astype("Int64")
        d["fecha_hora"] = pd.to_datetime(d["fecha_hora"], errors="coerce")
        d["linea"] = pd.to_numeric(d["linea"], errors="coerce").astype("Int64")
        d["vehiculo"] = pd.to_numeric(d["vehiculo"], errors="coerce").astype("Int64")
        d["id_nota"] = pd.to_numeric(d["id_nota"], errors="coerce").astype("Int64")
        for c in ["concesion", "tipo_nota", "subtipo_nota", "observaciones"]:
            d[c] = clean_text(d[c])
        d = d.dropna(subset=["id_ics", "fecha_hora", "linea", "id_nota"]).drop_duplicates().copy()
        return d

    def filter_existing_ics_ids(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        d = df.copy()
        ids = pd.to_numeric(d["id_ics"], errors="coerce").dropna().astype("int64").unique().tolist()
        if not ids:
            return d.iloc[0:0].copy()

        ids_existentes = set()
        full_ics_table = f'"{self.schema_ics}"."{self.table_ics}"'

        with conn.cursor() as cur:
            for start in range(0, len(ids), self.batch_size):
                chunk_ids = ids[start:start + self.batch_size]
                cur.execute(
                    f"""
                    SELECT {self.ics_id_column}
                    FROM {full_ics_table}
                    WHERE {self.ics_id_column} = ANY(%s)
                    """,
                    (chunk_ids,),
                )
                rows = cur.fetchall()
                ids_existentes.update(int(r[0]) for r in rows if r and r[0] is not None)

        before = len(d)
        d = d[pd.to_numeric(d["id_ics"], errors="coerce").isin(ids_existentes)].copy()
        print(f"Validación FK contra {self.schema_ics}.{self.table_ics}: {before} -> {len(d)} filas")
        return d

    def delete_existing_for_date(self, conn, fecha_date) -> int:
        fecha_ini = datetime.combine(fecha_date, datetime.min.time())
        fecha_fin = fecha_ini + timedelta(days=1)
        with conn.cursor() as cur:
            cur.execute(
                f'''DELETE FROM "{self.schema}"."{self.table}" WHERE "fecha_hora" >= %s AND "fecha_hora" < %s''',
                (fecha_ini, fecha_fin),
            )
            return cur.rowcount

    def insert_df(self, df: pd.DataFrame, conn) -> int:
        df_db = self._prepare_df_for_db(df)
        df_db = self.filter_existing_ics_ids(df_db, conn)
        if df_db.empty:
            print("No hay filas para insertar en Postgres después de validar id_ics contra sne.ics.")
            return 0

        full_table = f'"{self.schema}"."{self.table}"'
        cols = list(df_db.columns)
        col_sql = ",".join([f'"{c}"' for c in cols])
        sql = f"INSERT INTO {full_table} ({col_sql}) VALUES %s"

        total = 0
        with conn.cursor() as cur:
            if DELETE_EXISTING_FOR_DATE:
                fecha0 = df_db.iloc[0]["fecha_hora"].date()
                deleted = self.delete_existing_for_date(conn, fecha0)
                print(f"DELETE previo (fecha={fecha0}): {deleted} filas")

            for start in range(0, len(df_db), self.batch_size):
                chunk = df_db.iloc[start:start + self.batch_size].copy()
                records = [tuple(self._py(x) for x in row) for row in chunk.itertuples(index=False, name=None)]
                if not records:
                    continue
                execute_values(cur, sql, records, page_size=len(records))
                total += len(records)

        return total


# =============================================================================
# LOG
# =============================================================================

class ReportRunLogger:
    def __init__(
        self,
        schema_reportes: str = PG_SCHEMA_REPORTES,
        table_reportes: str = PG_TABLE_REPORTES,
        schema_log: str = PG_SCHEMA_LOG,
        table_log: str = PG_TABLE_LOG,
    ):
        self.schema_reportes = schema_reportes
        self.table_reportes = table_reportes
        self.schema_log = schema_log
        self.table_log = table_log

    def get_id_reporte(self, conn, nombre_reporte: str, default_id: int = DEFAULT_ID_REPORTE) -> int:
        sql = f"""
            SELECT id_reporte
            FROM "{self.schema_reportes}"."{self.table_reportes}"
            WHERE UPPER(TRIM(nombre_reporte)) = UPPER(TRIM(%s))
            LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(sql, (nombre_reporte,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(row[0])
        return int(default_id)

    def get_next_fecha_to_process(self, conn, id_reporte: int, fecha_semilla: date) -> date:
        full_table = f'"{self.schema_log}"."{self.table_log}"'
        sql = f"""
            SELECT MAX("fecha") AS max_fecha
            FROM {full_table}
            WHERE "id_reporte" = %s
              AND LOWER(TRIM(COALESCE("estado", ''))) = 'ok'
        """
        with conn.cursor() as cur:
            cur.execute(sql, (id_reporte,))
            row = cur.fetchone()

        max_fecha = row[0] if row else None
        if max_fecha is None:
            return fecha_semilla
        if isinstance(max_fecha, datetime):
            max_fecha = max_fecha.date()
        return max_fecha + timedelta(days=1)

    def write_log(
        self,
        conn,
        id_reporte: int,
        fecha_reporte_date: date,
        estado: str,
        ultima_ejecucion_ts: datetime,
        duracion_seg: int,
        archivos_total: int,
        archivos_ok: int,
        archivos_error: int,
        registros_proce: int,
        fecha_actualizacion_ts: Optional[datetime] = None,
    ) -> None:
        if fecha_actualizacion_ts is None:
            fecha_actualizacion_ts = ultima_ejecucion_ts

        full_table = f'"{self.schema_log}"."{self.table_log}"'
        sql_upsert = f"""
            INSERT INTO {full_table}
            (
                "id_reporte","fecha","estado","ultima_ejecucion","duracion_seg",
                "archivos_total","archivos_ok","archivos_error","registros_proce","fecha_actualizacion"
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ("id_reporte", "fecha")
            DO UPDATE SET
                "estado" = EXCLUDED."estado",
                "ultima_ejecucion" = EXCLUDED."ultima_ejecucion",
                "duracion_seg" = EXCLUDED."duracion_seg",
                "archivos_total" = EXCLUDED."archivos_total",
                "archivos_ok" = EXCLUDED."archivos_ok",
                "archivos_error" = EXCLUDED."archivos_error",
                "registros_proce" = EXCLUDED."registros_proce",
                "fecha_actualizacion" = EXCLUDED."fecha_actualizacion"
        """
        values = (
            int(id_reporte),
            fecha_reporte_date,
            str(estado).strip().lower(),
            ultima_ejecucion_ts,
            int(duracion_seg),
            int(archivos_total),
            int(archivos_ok),
            int(archivos_error),
            int(registros_proce),
            fecha_actualizacion_ts,
        )
        with conn.cursor() as cur:
            cur.execute(sql_upsert, values)


# =============================================================================
# MAIN
# =============================================================================

def _get_connection_string() -> str:
    env_conn = os.environ.get(AZURE_CONN_ENV, "").strip()
    if env_conn:
        return env_conn
    raise RuntimeError(f"Falta connection string. Usa env {AZURE_CONN_ENV}.")


def _parse_semilla() -> date:
    try:
        return datetime.strptime(FECHA_SEMILLA_STR, "%d/%m/%Y").date()
    except ValueError:
        raise SystemExit("FECHA_SEMILLA_STR debe estar en formato dd/mm/yyyy.")


def _resolve_fecha_to_process(fecha_semilla: date) -> date:
    if str(PROCESS_DATE_STR).strip():
        try:
            return datetime.strptime(str(PROCESS_DATE_STR).strip(), "%d/%m/%Y").date()
        except ValueError:
            raise SystemExit("PROCESS_DATE_STR debe estar en formato dd/mm/yyyy.")

    logger = ReportRunLogger()
    with get_db_connection() as conn:
        id_reporte = logger.get_id_reporte(conn, NOMBRE_REPORTE_LOG, default_id=DEFAULT_ID_REPORTE)
        return logger.get_next_fecha_to_process(conn, id_reporte, fecha_semilla)


def main() -> None:
    load_dotenv()
    start_perf = time.perf_counter()

    fecha_semilla = _parse_semilla()
    fecha_proc = _resolve_fecha_to_process(fecha_semilla)
    fecha_dt = datetime.combine(fecha_proc, datetime.min.time())
    conn_azure = _get_connection_string()

    estado = "ok"
    archivos_total = 1
    archivos_ok = 1
    archivos_error = 0
    registros_proce = 0

    try:
        az_ics = AzureBlobReader(AzureConfig(connection_string=conn_azure, container_name=AZURE_CONTAINER_ICS))
        az_fms = AzureBlobReader(AzureConfig(connection_string=conn_azure, container_name=AZURE_CONTAINER_FMS))

        builder = BitacoraNotasBuilder(az_ics=az_ics, az_fms=az_fms)
        df_final = builder.build(fecha_dt)
        registros_proce = int(len(df_final))
        if registros_proce == 0:
            raise RuntimeError("No se generaron registros para cargar. Revisar cruce con sne.ics o disponibilidad de datos.")

        print("\n" + "=" * 80)
        print(f"4) CARGANDO A POSTGRES {PG_SCHEMA_NAME}.{PG_TABLE_NAME}")
        print("=" * 80)

        with get_db_connection() as conn:
            loader = PostgresBitacoraNotasLoader(
                schema=PG_SCHEMA_NAME,
                table=PG_TABLE_NAME,
                batch_size=PG_BATCH_SIZE,
            )
            total = loader.insert_df(df_final, conn)
            conn.commit()

        print(f"{PG_SCHEMA_NAME}.{PG_TABLE_NAME}: {total} filas insertadas")
    except Exception as e:
        estado = "error"
        archivos_ok = 0
        archivos_error = 1
        print("ERROR en el proceso:", repr(e))
        raise
    finally:
        end_ts = datetime.now()
        duracion_seg = int(round(time.perf_counter() - start_perf))

        print("\n" + "=" * 80)
        print("5) GUARDANDO LOG EN log.procesa_report_sne")
        print("=" * 80)

        try:
            logger = ReportRunLogger()
            with get_db_connection() as conn_log:
                id_reporte = logger.get_id_reporte(conn_log, NOMBRE_REPORTE_LOG, default_id=DEFAULT_ID_REPORTE)
                logger.write_log(
                    conn_log,
                    id_reporte=id_reporte,
                    fecha_reporte_date=fecha_dt.date(),
                    estado=estado,
                    ultima_ejecucion_ts=end_ts,
                    duracion_seg=duracion_seg,
                    archivos_total=archivos_total,
                    archivos_ok=archivos_ok,
                    archivos_error=archivos_error,
                    registros_proce=registros_proce,
                    fecha_actualizacion_ts=end_ts,
                )
                conn_log.commit()
        except Exception as log_error:
            print(f"No se pudo guardar el log: {repr(log_error)}")


if __name__ == "__main__":
    main()
