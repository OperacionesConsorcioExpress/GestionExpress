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
PROCESA_SNE_DIR = ROOT_DIR / "Procesa_sne"
if str(PROCESA_SNE_DIR) not in sys.path:
    sys.path.insert(0, str(PROCESA_SNE_DIR))

try:
    from database.database_manager import get_db_connection
except ImportError:
    from database_manager import get_db_connection

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
PG_TABLE_NAME = "tabla_acciones"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

DEFAULT_ID_REPORTE = 3
NOMBRE_REPORTE_LOG = "Tabla Acciones"

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

        # importante: multiline quoted fields
        for kwargs in (
            dict(sep=sep, encoding=used_encoding, low_memory=False, dtype=dtype, quotechar='"'),
            dict(sep=sep, encoding="latin-1", low_memory=False, dtype=dtype, quotechar='"'),
            dict(sep=sep, encoding=used_encoding, engine="python", dtype=dtype, quotechar='"'),
            dict(sep=sep, encoding="latin-1", engine="python", dtype=dtype, quotechar='"'),
        ):
            try:
                bio.seek(0)
                df = pd.read_csv(bio, **kwargs)
                return cls.limpiar_columnas(df)
            except Exception:
                continue

        bio.seek(0)
        df = pd.read_csv(
            bio,
            sep=sep,
            encoding="latin-1",
            engine="python",
            dtype=dtype,
            quotechar='"'
        )
        return cls.limpiar_columnas(df)

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
    def to_int64(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    @staticmethod
    def normalize_vehicle_key(series: pd.Series) -> pd.Series:
        num = pd.to_numeric(series, errors="coerce").astype("Int64")
        # Para alimentacion el registro puede venir como vehiculo + 10000:
        # 880 -> 10880, 1012 -> 11012. No afecta urbanos de 6 digitos.
        mask_prefix_10 = num.notna() & (num >= 10000) & (num < 20000)
        num = num.where(~mask_prefix_10, num - 10000)
        return num.astype("Int64").astype(str)

    @staticmethod
    def to_datetime_yyyymmdd_hhmmss(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        s14 = s.str.slice(0, 14)
        return pd.to_datetime(s14, format="%Y%m%d%H%M%S", errors="coerce")

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
    s = (
        s.replace("concesiã³n", "concesion")
         .replace("lã­nea", "linea")
         .replace("descripciã³n", "descripcion")
         .replace("vehã­culo", "vehiculo")
         .replace("teã³rica", "teorica")
         .replace("referencia", "referencia")
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
# BUILDER ACCIONES
# =============================================================================

class TablaAccionesBuilder:
    def __init__(self, azure_reader: AzureBlobReader, filtro_zona_tipo: int):
        self.az = azure_reader
        self.filtro = filtro_zona_tipo
        self.io = DataIO()
        self.tu = TransformUtils()

    def _subpaths(self) -> List[str]:
        if self.filtro == 1:
            return ["ZN"]
        if self.filtro == 2:
            return ["TR"]
        return ["ZN", "TR"]

    def _blob_path_ics(self, fecha: datetime) -> Tuple[str, str]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")
        nombre = f"ICS_SMART OPERATOR_Etapa1_{fecha_txt}.csv"
        ruta = f"0001-26-fms-ics/{anio}/{mes}/{nombre}"
        return ruta, nombre

    def _blob_paths_detallado(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")

        out: List[Tuple[str, str]] = []
        for carpeta_tipo in self._subpaths():
            tipo_nombre = "Zonal" if carpeta_tipo == "ZN" else "Troncal"
            for zona in ["US", "SC"]:
                nombre = f"Detallado_{fecha_txt}_{tipo_nombre}_{zona}.csv"
                ruta = f"0001-24-fms-detallado/{anio}/{mes}/{carpeta_tipo}/{nombre}"
                out.append((ruta, nombre))
        return out

    def _blob_paths_acciones(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")

        out: List[Tuple[str, str]] = []
        for zona in ["US", "SC"]:
            nombre = f"Tabla_Acciones_{fecha_txt}_{zona}.csv"
            ruta = f"0001-31-fms-tabla-acciones/{anio}/{mes}/{nombre}"
            out.append((ruta, nombre))
        return out

    def load_ics(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ICS (AZURE)")
        print("=" * 80)

        ruta, nombre = self._blob_path_ics(fecha)
        if not self.az.exists(ruta):
            raise SystemExit(f"❌ No existe ICS en Azure para esa fecha:\n   {ruta}")

        df = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)

        c_idics = pick_col(df, ["IdICS"])
        c_fecha = pick_col(df, ["Fecha Viaje"])
        c_servicio = pick_col(df, ["Servicio"])
        c_coche = pick_col(df, ["Coche"])
        c_viaje_linea = pick_col(df, ["ViajeLinea", "Viaje Línea"])

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst="auto")
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = pd.to_numeric(out[c_coche], errors="coerce").astype("Int64")
        out["Viaje_key"] = pd.to_numeric(out[c_viaje_linea], errors="coerce").astype("Int64")
        out["IdICS"] = self.tu.to_int64(out[c_idics])

        out = out[["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key", "IdICS"]].copy()
        out = out.drop_duplicates(subset=["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"], keep="first")

        print(f"✅ ICS cargado: {nombre} | filas útiles={len(out)}")
        return out

    def load_detallado(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("2) CARGANDO DETALLADO (AZURE)")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        for ruta, nombre in self._blob_paths_detallado(fecha):
            if not self.az.exists(ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)
            df0["__archivo_origen__"] = nombre
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontraron archivos de Detallado para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        print("📋 Columnas detectadas en Detallado:")
        print(list(df.columns))

        c_fecha = pick_col(df, ["Fecha"])
        c_servicio = pick_col(df, ["Servicio"])
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea", "Viaje Línea "])
        c_vehiculo_real = pick_col(df, ["Vehículo Real", "Vehiculo Real", "Vehículo Real "], required=False)

        if c_vehiculo_real is None:
            raise SystemExit("❌ No se encontró 'Vehículo Real' en Detallado, necesario para cruce de Acciones.")

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = pd.to_numeric(out[c_tabla], errors="coerce").astype("Int64")
        out["Viaje_key"] = pd.to_numeric(out[c_viaje_linea], errors="coerce").astype("Int64")

        out["Fecha_exc_key"] = pd.to_datetime(
            out[c_fecha].astype(str).str.replace("\ufeff", "", regex=False).str.strip(),
            errors="coerce",
            dayfirst=True
        ).dt.date

        veh_real_num = pd.to_numeric(
            out[c_vehiculo_real].astype(str).str.replace(r"\D", "", regex=True),
            errors="coerce"
        ).astype("Int64")
        out["Vehiculo_key"] = veh_real_num.astype("Int64").astype(str)

        out["Servicio_exc_key"] = out[c_servicio].astype(str).str.strip().str.upper()

        keep = [
            "Fecha_key", "Servicio_key", "Coche_key", "Viaje_key",
            "Fecha_exc_key", "Vehiculo_key", "Servicio_exc_key"
        ]
        out = out[keep].copy()

        print(f"✅ Detallado cargado: filas útiles={len(out)}")
        return out

    def load_acciones(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("3) CARGANDO TABLA_ACCIONES (AZURE)")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        for ruta, nombre in self._blob_paths_acciones(fecha):
            if not self.az.exists(ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)
            df0["__archivo_origen__"] = nombre
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontraron archivos de Tabla_Acciones para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        print("📋 Columnas detectadas en Tabla_Acciones:")
        print(list(df.columns))

        # columnas destino
        c_apply_date = pick_col(df, ["APPLY_DATE", "Apply Date"])
        c_line_id = pick_col(df, ["LINE_ID", "Line Id"])
        c_veh_registr_num = pick_col(df, ["VEH_REGISTR_NUM", "Veh Registr Num"])
        c_regul_type_id = pick_col(df, ["REGUL_TYPE_ID", "Regul Type Id"])
        c_param_value = pick_col(df, ["PARAM_VALUE", "Param Value"])
        c_creat_datetime = pick_col(df, ["CREAT_DATETIME", "Creat Datetime"])

        # columna necesaria para cruce
        c_veh_serv_id = pick_col(
            df,
            ["VEH_SERV_ID", "Veh Serv Id", "SERV_ID", "SERVICE_ID", "VEH_SERVICE_ID"],
            required=False
        )

        out = df.copy()

        out["Fecha_exc_key"] = pd.to_datetime(
            out[c_apply_date].astype(str).str.strip(),
            format="%Y%m%d",
            errors="coerce"
        ).dt.date

        if c_veh_serv_id:
            out["Servicio_exc_key"] = out[c_veh_serv_id].astype(str).str.strip().str.upper()
        else:
            out["Servicio_exc_key"] = pd.NA

        out["Vehiculo_key"] = self.tu.normalize_vehicle_key(out[c_veh_registr_num])

        out["Fecha"] = pd.to_datetime(
            out[c_apply_date].astype(str).str.strip(),
            format="%Y%m%d",
            errors="coerce"
        ).dt.date

        out["Linea"] = out[c_line_id].astype("string").str.strip()
        out["Vehiculo"] = out[c_veh_registr_num].astype("string").str.strip()
        out["Accion"] = out[c_regul_type_id].astype("string").str.strip()
        out["Parametros"] = out[c_param_value].astype("string").str.strip()
        out["Instante"] = self.tu.to_datetime_yyyymmdd_hhmmss(out[c_creat_datetime])

        keep = [
            "Fecha_exc_key", "Servicio_exc_key", "Vehiculo_key",
            "Fecha", "Linea", "Vehiculo", "Accion", "Parametros", "Instante"
        ]
        out = out[keep].copy()

        print(f"✅ Tabla_Acciones cargado: filas útiles={len(out)}")
        return out

    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_ics = self.load_ics(fecha)
        df_det = self.load_detallado(fecha)
        df_acc = self.load_acciones(fecha)

        print("\n" + "=" * 80)
        print("4) CRUCE DETALLADO ↔ ICS")
        print("=" * 80)

        merge_keys_det = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]
        df_det_con_idics = df_det.merge(
            df_ics[["IdICS"] + merge_keys_det],
            on=merge_keys_det,
            how="left"
        )

        print(f"✅ Detallado+ICS: filas={len(df_det_con_idics)} | con IdICS={df_det_con_idics['IdICS'].notna().sum()}")

        print("\n" + "=" * 80)
        print("5) CRUCE TABLA_ACCIONES ↔ DETALLADO+ICS")
        print("=" * 80)

        df_merge = df_acc.merge(
            df_det_con_idics[["IdICS", "Fecha_exc_key", "Servicio_exc_key", "Vehiculo_key"]],
            on=["Fecha_exc_key", "Servicio_exc_key", "Vehiculo_key"],
            how="left"
        )

        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_acc["Fecha"].astype(str))

        out = pd.DataFrame({
            "Id_ICS": self.tu.to_int64(df_merge["IdICS"]),
            "Fecha": df_merge["Fecha"],
            "Linea": df_merge["Linea"],
            "Vehiculo": df_merge["Vehiculo"],
            "Accion": df_merge["Accion"],
            "Parametros": df_merge["Parametros"],
            "Instante": df_merge["Instante"],
        })

        out = out[out["Id_ICS"].notna()].copy()

        out = out.drop_duplicates(
            subset=[
                "Id_ICS", "Fecha", "Linea", "Vehiculo",
                "Accion", "Parametros", "Instante"
            ],
            keep="first"
        ).reset_index(drop=True)

        print("\n✅ Tabla Acciones construida:")
        print(f"   Filas con Id_ICS: {len(out)}")
        print(f"📌 Nombre lógico: TABLA_ACCIONES_{fecha_nombre}")

        return out, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.tabla_acciones
# =============================================================================

class PostgresTablaAccionesLoader:
    DF_TO_DB = {
        "Id_ICS": "id_ics",
        "Fecha": "fecha",
        "Linea": "linea",
        "Vehiculo": "vehiculo",
        "Accion": "accion",
        "Parametros": "parametros",
        "Instante": "instante",
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

        try:
            if pd.isna(v):
                return None
        except Exception:
            pass

        if isinstance(v, np.generic):
            return v.item()

        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            return v.to_pydatetime()

        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.lower() == "nat":
                return None
            return s

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
    def _parse_datetime(v):
        if v is None:
            return None

        try:
            if pd.isna(v):
                return None
        except Exception:
            pass

        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            return v.to_pydatetime()

        if isinstance(v, datetime):
            return v

        s = str(v).strip()
        if s == "" or s.lower() == "nat":
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
        d["instante"] = d["instante"].apply(self._parse_datetime)
        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce")

        for c in ["linea", "vehiculo", "accion", "parametros"]:
            d[c] = d[c].astype("string")

        d = d[d["id_ics"].notna()].copy()

        d = d.drop_duplicates(
            subset=[
                "id_ics", "fecha", "linea", "vehiculo",
                "accion", "parametros", "instante"
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
                "id_ics",
                "fecha",
                "linea",
                "vehiculo",
                "accion",
                "parametros",
                "instante"
            )
            DO UPDATE SET
                "parametros" = EXCLUDED."parametros",
                "instante" = EXCLUDED."instante"
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
                            "id_ics", "fecha", "linea", "vehiculo",
                            "accion", "parametros", "instante"
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
                    print(f"  ✅ Upsert tabla_acciones {total}/{len(df_db)}")

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
    filename = f"TABLA_ACCIONES_{fecha_nombre}.csv"
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
        builder = TablaAccionesBuilder(az, filtro_zona_tipo=FILTRO_ZONA_TIPO)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print(f"6) CARGANDO A POSTGRES {PG_SCHEMA_NAME}.{PG_TABLE_NAME}")
        print("=" * 80)

        loader = PostgresTablaAccionesLoader(
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
        print("7) GUARDANDO LOG EN log.procesa_report_sne")
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
