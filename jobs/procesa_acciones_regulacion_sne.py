from __future__ import annotations

import os
import re
import csv
import time
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient

import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv
import sys
from pathlib import Path

load_dotenv()

CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database.database_manager import get_db_connection

# =============================================================================
# CONFIG
# =============================================================================

FECHA_SEMILLA_STR = "01/03/2026"   # dd/mm/yyyy
FILTRO_ZONA_TIPO = 3            # 1=ZN, 2=TR, 3=Ambas

AZURE_CONN_ENV = "AZURE_STORAGE_CONNECTION_STRING"
AZURE_CONTAINER_ACTIVIDAD = "0001-archivos-de-apoyo-descargas-cex-fms"

CONNECTION_STRING_LOCAL = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=datoscentroinformacion;"
    "AccountKey=zbNe/Asv2IFC0Q0hATwl9uQMTkuCrMYgCoO7hk1ICbC1utlZZ/boOjUBGykhr0LCr5dd9wXsA9jA+ASt8/gtlQ==;"
    "EndpointSuffix=core.windows.net"
)

# -------------------------------
# POSTGRES
# -------------------------------
PG_CONN_ENV = "POSTGRES_CONN_STRING"
POSTGRES_CONN_STRING_LOCAL = ""

PG_SCHEMA_NAME = "sne"
PG_TABLE_NAME = "acciones_regulacion"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

DEFAULT_ID_REPORTE = 4
NOMBRE_REPORTE_LOG = "Tabla Acciones de regulacion"

PG_BATCH_SIZE = 5000
DELETE_EXISTING_FOR_DATE = False

# -------------------------------
# EXPORT CSV LOCAL
# -------------------------------
EXPORT_CSV_ENABLED = False
EXPORT_CSV_DIR = r"C:\Users\analista.centroinf3\OneDrive - Grupo Express\Juan Buitrago\Exportes"
EXPORT_CSV_ENCODING = "utf-8-sig"
EXPORT_CSV_SEP = ";"

# =============================================================================
# HELPERS - IO ROBUSTO EN MEMORIA
# =============================================================================

class DataIO:
    @staticmethod
    def limpiar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()

        def _clean_col(c: str) -> str:
            s = str(c)
            s = s.replace("\ufeff", "").replace("ï»¿", "").replace('"', "").strip()
            s = re.sub(r"^[\uFEFF\u200B\u200C\u200D\u2060]+", "", s).strip()
            s = re.sub(r"^(ï»¿)+", "", s).strip()
            s = re.sub(r"\s+", " ", s).strip()
            return s

        df.columns = [_clean_col(c0) for c0 in df.columns]
        return df

    @classmethod
    def leer_excel_desde_bytes(cls, content: bytes, dtype=str) -> pd.DataFrame:
        bio = BytesIO(content)

        for kwargs in (
            dict(dtype=dtype, engine="openpyxl"),
            dict(dtype=dtype),
        ):
            try:
                bio.seek(0)
                df = pd.read_excel(bio, **kwargs)
                return cls.limpiar_columnas(df)
            except Exception:
                continue

        raise SystemExit("❌ No fue posible leer el archivo Excel de Acciones Regulación.")

# =============================================================================
# TRANSFORM
# =============================================================================

class TransformUtils:
    @staticmethod
    def fecha_key_robusta(serie: pd.Series, prefer_dayfirst: str = "auto") -> pd.Series:
        s = serie.copy()
        s = s.astype(str).str.replace("\ufeff", "", regex=False).str.replace("ï»¿", "", regex=False).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        s_num = pd.to_numeric(s, errors="coerce")
        mask_excel = s_num.notna() & (s_num >= 20000) & (s_num <= 60000)

        out = pd.Series([pd.NA] * len(s), index=s.index, dtype="object")

        if mask_excel.any():
            dt_excel = pd.to_datetime(s_num[mask_excel], unit="D", origin="1899-12-30", errors="coerce")
            out.loc[mask_excel] = dt_excel.dt.strftime("%Y-%m-%d")

        rest = ~mask_excel
        if rest.any():
            sr = s[rest]
            mask_iso = sr.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$", na=False)
            if mask_iso.any():
                dt_iso = pd.to_datetime(sr[mask_iso], errors="coerce", format="%Y-%m-%d")
                na_mask = dt_iso.isna()
                if na_mask.any():
                    dt_iso2 = pd.to_datetime(sr[mask_iso][na_mask], errors="coerce", format="%Y-%m-%d %H:%M:%S")
                    dt_iso.loc[na_mask] = dt_iso2
                out.loc[sr[mask_iso].index] = dt_iso.dt.strftime("%Y-%m-%d")

            idx_non = sr[~mask_iso].index
            if len(idx_non):
                sr2 = sr.loc[idx_non]

                def _parse(dayfirst_flag: bool) -> pd.Series:
                    return pd.to_datetime(sr2, errors="coerce", dayfirst=dayfirst_flag)

                if prefer_dayfirst == "auto":
                    dt1 = _parse(True)
                    dt2 = _parse(False)
                    dt = dt1 if dt1.notna().sum() >= dt2.notna().sum() else dt2
                elif prefer_dayfirst is True:
                    dt = _parse(True)
                else:
                    dt = _parse(False)

                out.loc[idx_non] = dt.dt.strftime("%Y-%m-%d")

        return out

    @staticmethod
    def to_datetime_robusto(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.replace("\ufeff", "", regex=False).str.replace("ï»¿", "", regex=False).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        mask_hora = s.str.match(r"^\d{1,2}:\d{2}(:\d{2})?$", na=False)
        mask_full = ~mask_hora

        if mask_full.any():
            dt1 = pd.to_datetime(s[mask_full], errors="coerce", dayfirst=True)
            dt2 = pd.to_datetime(s[mask_full], errors="coerce", dayfirst=False)
            out.loc[mask_full] = dt1.where(dt1.notna(), dt2)

        if mask_hora.any():
            hh = s[mask_hora].copy()
            hh = hh.where(~hh.str.match(r"^\d{1,2}:\d{2}$", na=False), hh + ":00")
            out.loc[mask_hora] = pd.to_datetime("1900-01-01 " + hh, errors="coerce")

        return out

    @staticmethod
    def fecha_para_nombre_archivo_dd_mm_yyyy(serie_fecha: pd.Series) -> str:
        s = serie_fecha.astype(str).str.replace("\ufeff", "", regex=False).str.replace("ï»¿", "", regex=False).str.strip()
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)

        if dt.notna().any():
            moda = dt.dropna().dt.date.value_counts().idxmax()
            dt_moda = pd.to_datetime(str(moda))
            return dt_moda.strftime("%d_%m_%Y")

        return "SIN_FECHA"

def normalize_name(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("\ufeff", "").replace("ï»¿", "")
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = s.replace("ã¡", "a").replace("ã©", "e").replace("ã­", "i").replace("ã³", "o").replace("ãº", "u")
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

# =============================================================================
# AZURE
# =============================================================================

@dataclass(frozen=True)
class AzureConfig:
    connection_string: str
    container_actividad: str = AZURE_CONTAINER_ACTIVIDAD

class AzureBlobReader:
    def __init__(self, cfg: AzureConfig):
        self.cfg = cfg
        self.service = BlobServiceClient.from_connection_string(cfg.connection_string)
        self.container = self.service.get_container_client(cfg.container_actividad)

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

# =============================================================================
# BUILDER ACCIONES REGULACION
# =============================================================================

class AccionesRegulacionBuilder:
    def __init__(self, azure_reader: AzureBlobReader, filtro_zona_tipo: int):
        self.az = azure_reader
        self.io = DataIO()
        self.tu = TransformUtils()
        self.filtro = filtro_zona_tipo

    def _subpaths(self) -> List[str]:
        if self.filtro == 1:
            return ["ZN"]
        if self.filtro == 2:
            return ["TR"]
        return ["ZN", "TR"]

    def _blob_paths_acciones_reg(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")

        out: List[Tuple[str, str]] = []
        for tipo in self._subpaths():
            for zona in ["US", "SC"]:
                nombre = f"AR_{fecha_txt}_{zona}.xlsx"
                ruta = f"0001-23-fms-acciones-de-regulacion/{anio}/{mes}/{tipo}/{nombre}"
                out.append((ruta, nombre))
        return out

    def load_acciones_reg(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ACCIONES REGULACION (AZURE)")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        for ruta, nombre in self._blob_paths_acciones_reg(fecha):
            if not self.az.exists(ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            raw = self.az.read_bytes(ruta)
            df0 = self.io.leer_excel_desde_bytes(raw, dtype=str)
            df0["__archivo_origen__"] = nombre
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontraron archivos de Acciones Regulación para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        print("📋 Columnas detectadas en Acciones Regulación:")
        print(list(df.columns))

        c_fecha = pick_col(df, ["Fecha"])
        c_instante = pick_col(df, ["Instante"])
        c_id_linea = pick_col(df, ["Id Línea", "Id Linea"], required=False)
        c_linea = pick_col(df, ["Línea", "Linea"], required=False)
        c_tabla = pick_col(df, ["Tabla"])
        c_num_fms = pick_col(df, ["Número FMS Bus", "Numero FMS Bus"])
        c_id_accion = pick_col(df, ["Id Acción", "Id Accion"])
        c_desc_accion = pick_col(df, ["Descripción de Acción", "Descripcion de Accion"])
        c_parametros = pick_col(df, ["Parámetros", "Parametros"], required=False)
        c_motivo = pick_col(df, ["Motivo"], required=False)
        c_reversada_por = pick_col(df, ["Reversada por", "Reversada Por"], required=False)

        out = df.copy()

        fecha_key = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        fecha_dt = pd.to_datetime(fecha_key, errors="coerce")

        instante_dt = self.tu.to_datetime_robusto(out[c_instante])

        mask_hora_sola = instante_dt.notna() & fecha_dt.notna() & (instante_dt.dt.date == date(1900, 1, 1))
        if mask_hora_sola.any():
            instante_dt.loc[mask_hora_sola] = (
                fecha_dt.loc[mask_hora_sola]
                + pd.to_timedelta(instante_dt.loc[mask_hora_sola].dt.hour, unit="h")
                + pd.to_timedelta(instante_dt.loc[mask_hora_sola].dt.minute, unit="m")
                + pd.to_timedelta(instante_dt.loc[mask_hora_sola].dt.second, unit="s")
            )

        out["Id_ICS"] = pd.Series([pd.NA] * len(out), dtype="Int64")
        out["Fecha"] = fecha_dt.dt.date
        out["Instante"] = instante_dt

        serie_linea = out[c_id_linea] if c_id_linea else out[c_linea]
        out["Linea"] = serie_linea.astype("string").str.strip() if serie_linea is not None else pd.NA

        out["Tabla"] = out[c_tabla].astype("string").str.strip()
        out["Numero_FMS_Bus"] = out[c_num_fms].astype("string").str.strip()
        out["Id_Accion"] = out[c_id_accion].astype("string").str.strip()
        out["Descripcion_Accion"] = out[c_desc_accion].astype("string").str.strip()
        out["Parametros"] = out[c_parametros].astype("string").str.strip() if c_parametros else pd.NA
        out["Motivo"] = out[c_motivo].astype("string").str.strip() if c_motivo else pd.NA
        out["Reversada_Por"] = out[c_reversada_por].astype("string").str.strip() if c_reversada_por else pd.NA

        keep = [
            "Id_ICS",
            "Fecha",
            "Instante",
            "Linea",
            "Tabla",
            "Numero_FMS_Bus",
            "Id_Accion",
            "Descripcion_Accion",
            "Parametros",
            "Motivo",
            "Reversada_Por",
        ]
        out = out[keep].copy()

        out = out[out["Fecha"].notna()].copy()

        out = out.drop_duplicates(
            subset=[
                "Fecha", "Instante", "Linea", "Tabla",
                "Numero_FMS_Bus", "Id_Accion", "Descripcion_Accion",
                "Parametros", "Motivo", "Reversada_Por"
            ],
            keep="first"
        ).reset_index(drop=True)

        print(f"✅ Acciones Regulación cargado: filas útiles={len(out)}")
        return out

    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_final = self.load_acciones_reg(fecha)
        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_final["Fecha"].astype(str))

        print("\n✅ Tabla Acciones Regulación construida:")
        print(f"   Filas: {len(df_final)}")
        print(f"📌 Nombre lógico: ACCIONES_REGULACION_{fecha_nombre}")

        return df_final, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.acciones_regulacion
# =============================================================================

class PostgresAccionesRegulacionLoader:
    DF_TO_DB = {
        "Id_ICS": "id_ics",
        "Fecha": "fecha",
        "Instante": "instante",
        "Linea": "linea",
        "Tabla": "tabla",
        "Numero_FMS_Bus": "numero_fms_bus",
        "Id_Accion": "id_accion",
        "Descripcion_Accion": "descripcion_accion",
        "Parametros": "parametros",
        "Motivo": "motivo",
        "Reversada_Por": "reversada_por",
    }

    def __init__(self, schema: str, table: str, batch_size: int = 5000, schema_ics: str = "sne", table_ics: str = "ics", ics_id_column: str = "id_ics"):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.schema_ics = schema_ics
        self.table_ics = table_ics
        self.ics_id_column = ics_id_column

    def _connect(self):
        last_error = None
        for i in range(3):
            try:
                return get_db_connection()
            except psycopg2.OperationalError as e:
                last_error = e
                print(f"⚠️ Intento {i+1}/3 conexión PG falló: {repr(e)}")
                time.sleep(3)
        raise last_error

    @staticmethod
    def _py(v):
        if v is None:
            return None
        try:
            if v is pd.NA:
                return None
        except Exception:
            pass
        if isinstance(v, np.generic):
            return v.item()
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            return v.to_pydatetime()
        if isinstance(v, str):
            s = v.strip()
            return s if s != "" else None
        return v

    @staticmethod
    def _parse_fecha_to_date(fecha_val):
        if isinstance(fecha_val, date) and not isinstance(fecha_val, datetime):
            return fecha_val
        if isinstance(fecha_val, datetime):
            return fecha_val.date()
        if fecha_val is None:
            return None

        s = str(fecha_val).strip()
        if s == "":
            return None

        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()

    @staticmethod
    def _parse_instante_to_datetime(v):
        if v is None:
            return None
        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            return v.to_pydatetime()
        if isinstance(v, datetime):
            return v
        s = str(v).strip()
        if s == "":
            return None
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()

    def _prepare_df_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.DF_TO_DB.keys() if c not in df.columns]
        if missing:
            raise SystemExit(f"❌ Faltan columnas en DF para insertar a Postgres: {missing}")

        d = df[list(self.DF_TO_DB.keys())].copy()
        d = d.rename(columns=self.DF_TO_DB)

        d["fecha"] = d["fecha"].apply(self._parse_fecha_to_date)
        d["instante"] = d["instante"].apply(self._parse_instante_to_datetime)
        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce")

        for c in [
            "linea", "tabla", "numero_fms_bus", "id_accion",
            "descripcion_accion", "parametros", "motivo", "reversada_por"
        ]:
            d[c] = d[c].astype("string")

        d = d.drop_duplicates(
            subset=[
                "fecha", "instante", "linea", "tabla",
                "numero_fms_bus", "id_accion", "descripcion_accion",
                "parametros", "motivo", "reversada_por"
            ],
            keep="first"
        ).copy()

        return d

    def filter_existing_ics_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            print("ℹ️ No hay filas para validar contra sne.ics")
            return df.copy()

        d = df.copy()
        ids = pd.to_numeric(d["id_ics"], errors="coerce").dropna().astype("int64").unique().tolist()
        if not ids:
            print("⚠️ No se encontraron id_ics válidos para validar contra sne.ics")
            return d.iloc[0:0].copy()

        ids_existentes = set()
        full_ics_table = f'"{self.schema_ics}"."{self.table_ics}"'
        ics_id_col = self.ics_id_column

        with self._connect() as conn:
            with conn.cursor() as cur:
                for start in range(0, len(ids), self.batch_size):
                    chunk_ids = ids[start:start + self.batch_size]
                    cur.execute(
                        f"""
                        SELECT {ics_id_col}
                        FROM {full_ics_table}
                        WHERE {ics_id_col} = ANY(%s)
                        """,
                        (chunk_ids,)
                    )
                    rows = cur.fetchall()
                    ids_existentes.update(int(r[0]) for r in rows if r and r[0] is not None)

        before = len(d)
        d = d[pd.to_numeric(d["id_ics"], errors="coerce").isin(ids_existentes)].copy()
        after = len(d)
        descartadas = before - after

        print(f"🔎 Validación FK contra {self.schema_ics}.{self.table_ics}.{ics_id_col}:")
        print(f"   ids_ics en archivo/proceso: {len(ids)}")
        print(f"   ids_ics existentes en tabla ics: {len(ids_existentes)}")
        print(f"   filas antes del filtro FK: {before}")
        print(f"   filas después del filtro FK: {after}")
        print(f"   filas descartadas por id_ics inexistente: {descartadas}")

        return d

    def delete_existing_for_date(self, conn, fecha_date) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{self.table}" WHERE fecha = %s',
                (fecha_date,)
            )
            return cur.rowcount

    def insert_df(self, df: pd.DataFrame) -> int:
        df_db = self._prepare_df_for_db(df)
        df_db = self.filter_existing_ics_ids(df_db)

        if df_db.empty:
            print("⚠️ No hay filas para insertar después de validar id_ics contra sne.ics")
            return 0

        full_table = f'"{self.schema}"."{self.table}"'
        cols = list(df_db.columns)
        col_sql = ",".join([f'"{c}"' for c in cols])

        sql = f"""
            INSERT INTO {full_table} ({col_sql})
            VALUES %s
            ON CONFLICT (
                "fecha",
                "instante",
                "linea",
                "tabla",
                "numero_fms_bus",
                "id_accion",
                "descripcion_accion",
                "parametros",
                "motivo",
                "reversada_por"
            )
            DO UPDATE SET
                "id_ics" = EXCLUDED."id_ics"
        """

        total = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                if DELETE_EXISTING_FOR_DATE and "fecha" in df_db.columns and len(df_db) > 0:
                    fecha0 = df_db.iloc[0]["fecha"]
                    deleted = self.delete_existing_for_date(conn, fecha0)
                    print(f"🧹 DELETE previo (fecha={fecha0}): {deleted} filas")

                for start in range(0, len(df_db), self.batch_size):
                    chunk = df_db.iloc[start:start + self.batch_size]

                    chunk = chunk.drop_duplicates(
                        subset=[
                            "fecha", "instante", "linea", "tabla",
                            "numero_fms_bus", "id_accion", "descripcion_accion",
                            "parametros", "motivo", "reversada_por"
                        ],
                        keep="first"
                    ).copy()

                    records = [tuple(self._py(x) for x in row) for row in chunk.itertuples(index=False, name=None)]

                    if not records:
                        continue

                    execute_values(
                        cur,
                        sql,
                        records,
                        page_size=len(records)
                    )

                    total += len(records)
                    print(f"  ✅ Upsert acciones_regulacion {total}/{len(df_db)}")

            conn.commit()

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

    def _connect(self):
        last_error = None
        for i in range(3):
            try:
                return get_db_connection()
            except psycopg2.OperationalError as e:
                last_error = e
                print(f"⚠️ Intento {i+1}/3 conexión PG falló: {repr(e)}")
                time.sleep(3)
        raise last_error

    def get_id_reporte(self, nombre_reporte: str, default_id: int = DEFAULT_ID_REPORTE) -> int:
        sql = f"""
            SELECT id_reporte
            FROM "{self.schema_reportes}"."{self.table_reportes}"
            WHERE UPPER(TRIM(nombre_reporte)) = UPPER(TRIM(%s))
            LIMIT 1
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (nombre_reporte,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    return int(row[0])
        return int(default_id)

    def write_log(
        self,
        id_reporte: int,
        fecha_reporte_date,
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

        sql_select = f"""
            SELECT "estado"
            FROM {full_table}
            WHERE "id_reporte" = %s AND "fecha" = %s
            LIMIT 1
        """

        sql_insert = f"""
            INSERT INTO {full_table}
            ("id_reporte","fecha","estado","ultima_ejecucion","duracion_seg",
             "archivos_total","archivos_ok","archivos_error","registros_proce","fecha_actualizacion")
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        sql_update = f"""
            UPDATE {full_table}
            SET
                "estado" = %s,
                "ultima_ejecucion" = %s,
                "duracion_seg" = %s,
                "archivos_total" = %s,
                "archivos_ok" = %s,
                "archivos_error" = %s,
                "registros_proce" = %s,
                "fecha_actualizacion" = %s
            WHERE "id_reporte" = %s AND "fecha" = %s
        """

        values_insert = (
            id_reporte,
            fecha_reporte_date,
            estado,
            ultima_ejecucion_ts,
            duracion_seg,
            archivos_total,
            archivos_ok,
            archivos_error,
            registros_proce,
            fecha_actualizacion_ts,
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_select, (id_reporte, fecha_reporte_date))
                row = cur.fetchone()

                if row and str(row[0]).strip().lower() == "ok":
                    conn.commit()
                    return

                if row:
                    cur.execute(
                        sql_update,
                        (
                            estado,
                            ultima_ejecucion_ts,
                            duracion_seg,
                            archivos_total,
                            archivos_ok,
                            archivos_error,
                            registros_proce,
                            fecha_actualizacion_ts,
                            id_reporte,
                            fecha_reporte_date,
                        ),
                    )
                else:
                    cur.execute(sql_insert, values_insert)

            conn.commit()

# =============================================================================
# MAIN
# =============================================================================

def _report_logger_get_next_fecha_to_process(self, id_reporte: int, fecha_semilla: date) -> date:
    full_table = f'"{self.schema_log}"."{self.table_log}"'
    sql = f"""
        SELECT MAX("fecha") AS max_fecha
        FROM {full_table}
        WHERE "id_reporte" = %s
          AND LOWER(TRIM(COALESCE("estado", ''))) = 'ok'
    """
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (id_reporte,))
            row = cur.fetchone()

    max_fecha = row[0] if row else None
    if max_fecha is None:
        return fecha_semilla

    if isinstance(max_fecha, datetime):
        max_fecha = max_fecha.date()

    return max_fecha + timedelta(days=1)

ReportRunLogger.get_next_fecha_to_process = _report_logger_get_next_fecha_to_process

def _get_connection_string() -> str:
    env_conn = os.environ.get(AZURE_CONN_ENV, "").strip()
    if env_conn:
        return env_conn
    if CONNECTION_STRING_LOCAL:
        return CONNECTION_STRING_LOCAL
    raise SystemExit(f"❌ Falta connection string. Usa env {AZURE_CONN_ENV}.")

def _test_pg_connection() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

def _export_df_to_csv(df: pd.DataFrame, fecha_nombre: str) -> Optional[str]:
    if not EXPORT_CSV_ENABLED:
        return None
    if df is None or df.empty:
        print("⚠️ Export CSV: DF vacío, no se genera archivo.")
        return None

    os.makedirs(EXPORT_CSV_DIR, exist_ok=True)
    filename = f"ACCIONES_REGULACION_{fecha_nombre}.csv"
    path = os.path.join(EXPORT_CSV_DIR, filename)

    df.to_csv(
        path,
        index=False,
        sep=EXPORT_CSV_SEP,
        encoding=EXPORT_CSV_ENCODING
    )
    print(f"📤 Export CSV generado: {path}")
    return path

def main() -> None:
    start_perf = time.perf_counter()

    conn_azure = _get_connection_string()

    try:
        fecha_semilla = datetime.strptime(FECHA_SEMILLA_STR, "%d/%m/%Y").date()
    except ValueError:
        raise SystemExit("FECHA_SEMILLA_STR debe estar en formato dd/mm/yyyy, ej: 31/01/2026")

    logger_seed = ReportRunLogger()
    id_reporte_seed = logger_seed.get_id_reporte(NOMBRE_REPORTE_LOG, default_id=DEFAULT_ID_REPORTE)
    fecha_proc = logger_seed.get_next_fecha_to_process(id_reporte_seed, fecha_semilla)
    fecha_dt = datetime.combine(fecha_proc, datetime.min.time())

    estado = "ok"
    archivos_total = 1
    archivos_ok = 1
    archivos_error = 0
    registros_proce = 0

    try:
        print("Conexion a Postgres validada desde database_manager")

        az = AzureBlobReader(AzureConfig(connection_string=conn_azure))
        builder = AccionesRegulacionBuilder(az, filtro_zona_tipo=FILTRO_ZONA_TIPO)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print(f"2) CARGANDO A POSTGRES {PG_SCHEMA_NAME}.{PG_TABLE_NAME}")
        print("=" * 80)

        loader = PostgresAccionesRegulacionLoader(
                        schema=PG_SCHEMA_NAME,
            table=PG_TABLE_NAME,
            batch_size=PG_BATCH_SIZE
        )
        total = loader.insert_df(df_final)
        print(f"✅ {PG_SCHEMA_NAME}.{PG_TABLE_NAME} upsert: {total} filas")

    except Exception as e:
        estado = "error"
        archivos_ok = 0
        archivos_error = 1
        print("❌ ERROR en el proceso:", repr(e))
        raise
    finally:
        end_ts = datetime.now()
        duracion_seg = int(round(time.perf_counter() - start_perf))

        print("\n" + "=" * 80)
        print("3) GUARDANDO LOG EN log.procesa_report_sne")
        print("=" * 80)

        try:
            logger = ReportRunLogger()
            id_reporte = logger.get_id_reporte(NOMBRE_REPORTE_LOG, default_id=DEFAULT_ID_REPORTE)

            logger.write_log(
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

            print(
                f"📌 log: {PG_SCHEMA_LOG}.{PG_TABLE_LOG} | "
                f"id_reporte={id_reporte} | fecha={fecha_dt.date()} | estado={estado}"
            )
            print(f"⏱️ duración_seg={duracion_seg} | registros_proce={registros_proce}")
        except Exception as log_error:
            print(f"⚠️ No se pudo guardar el log: {repr(log_error)}")

    print("\n" + "=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)

if __name__ == "__main__":
    main()
