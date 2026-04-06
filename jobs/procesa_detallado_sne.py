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
PG_TABLE_NAME = "detallado"

PG_SCHEMA_ICS = "sne"
PG_TABLE_ICS = "ics"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

PG_BATCH_SIZE = 5000
NOMBRE_REPORTE_LOG = "Tabla Detallado"
DEFAULT_ID_REPORTE = 2
DELETE_EXISTING_FOR_DATE = False

# -------------------------------
# EXPORT CSV LOCAL
# -------------------------------
EXPORT_CSV_ENABLED = False
EXPORT_CSV_DIR = r"C:\Users\analista.centroinf3\OneDrive - Grupo Express\Juan Buitrago\Exportes"
EXPORT_CSV_ENCODING = "utf-8-sig"
EXPORT_CSV_SEP = ";"

# Exportar descartados por FK
EXPORT_DESCARTADOS_FK = True

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
    def to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").astype("float64")

    @staticmethod
    def metros_a_km(series: pd.Series) -> pd.Series:
        raw = pd.to_numeric(series, errors="coerce").astype("float64")
        return raw / 1000.0

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

def parse_bool_text(v) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s == "" or s in {"nan", "none"}:
        return None
    if s in {"no", "false", "0"}:
        return False
    return True

def normalize_flag_text(v) -> Optional[str]:
    if v is None:
        return None
    try:
        if v is pd.NA:
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    return s

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

    def list_blob_paths(self, prefix: str) -> List[str]:
        out: List[str] = []
        for b in self.container.list_blobs(name_starts_with=prefix):
            if b.name and not b.name.endswith("/"):
                out.append(b.name)
        return out

# =============================================================================
# BUILDER DETALLADO
# =============================================================================

class DetalladoBuilder:
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
        c_id_viaje = pick_col(df, ["IdViaje", "Id Viaje"])

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst="auto")
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_coche])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])
        out["IdViaje_key"] = self.tu.to_int64(out[c_id_viaje])
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
            raise SystemExit("❌ No se encontró ningún archivo de Detallado para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        print("📋 Columnas detectadas en Detallado:")
        print(list(df.columns))

        c_fecha = pick_col(df, ["Fecha"])
        c_servicio = pick_col(df, ["Servicio"])
        c_id_viaje = pick_col(df, ["Id Viaje", "Id Viaje "])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea", "Viaje Línea "])
        c_id_linea = pick_col(df, ["Id Línea", "Id Linea", "Id Línea "])
        c_sentido = pick_col(df, ["Sentido", "Sentido "], required=False)
        c_tabla = pick_col(df, ["Tabla"])
        c_planificado = pick_col(df, ["Planificado"], required=False)
        c_eliminado = pick_col(df, ["Eliminado"], required=False)
        c_estmotivoelim = pick_col(df, ["EstMotivoElim"], required=False)
        c_desc_motivo = pick_col(df, ["Descripción Motivo Elim", "Descripcion Motivo Elim"], required=False)
        c_vehiculo_real = pick_col(df, ["Vehículo Real", "Vehiculo Real", "Vehículo Real "], required=False)
        c_conductor = pick_col(df, ["Conductor"], required=False)
        c_hora_ini_teorica = pick_col(df, ["Hora Ini Teorica", "Hora Ini Teorica "], required=False)
        c_hora_ini_referencia = pick_col(df, ["Hora Ini Referencia", "Hora Ini Referencia "], required=False)
        c_hora_ini_real_cabecera = pick_col(df, ["Hora Ini Real Cabecera"], required=False)
        c_hora_ini_offset = pick_col(df, ["Hora Ini Offset"], required=False)
        c_offsetinicio = pick_col(df, ["OffsetInicio"], required=False)
        c_hora_ini_est_cabecera = pick_col(df, ["Hora Ini Est Cabecera"], required=False)
        c_hora_fin_real = pick_col(df, ["Hora Fin Real", "Hora Fin Real "], required=False)
        c_offsetfin = pick_col(df, ["OffsetFin"], required=False)
        c_kmfueraruta = pick_col(df, ["KmFueraRuta"], required=False)
        c_servbusref = pick_col(df, ["ServBusRef"], required=False)
        c_id_viaje_ref = pick_col(df, ["Id Viaje Ref"], required=False)
        c_id_linea_referencia = pick_col(df, ["Id línea Referencia", "Id Linea Referencia"], required=False)
        c_lista_acciones = pick_col(df, ["Lista Acciones"], required=False)
        c_kmprogad = pick_col(df, ["KmProgAd"], required=False)
        c_kmejecutado = pick_col(df, ["KmEjecutado"], required=False)

        out = df.copy()

        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_tabla])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])

        out["Fecha"] = pd.to_datetime(out[c_fecha], errors="coerce", dayfirst=True).dt.date
        out["Servicio"] = out[c_servicio].astype("string").str.strip()
        out["Id_Viaje"] = out[c_id_viaje].astype("string").str.strip()
        out["Viaje_Linea"] = out[c_viaje_linea].astype("string").str.strip()
        out["Id_Linea"] = out[c_id_linea].astype("string").str.strip()
        out["Sentido"] = out[c_sentido].astype("string").str.strip() if c_sentido else pd.NA
        out["Tabla"] = out[c_tabla].astype("string").str.strip()

        out["Planificado"] = out[c_planificado].apply(normalize_flag_text) if c_planificado else pd.NA
        out["Eliminado"] = out[c_eliminado].apply(normalize_flag_text) if c_eliminado else pd.NA
        out["EstMotivoElim"] = out[c_estmotivoelim].astype("string").str.strip() if c_estmotivoelim else pd.NA
        out["Descripcion_Motivo_Elim"] = out[c_desc_motivo].astype("string").str.strip() if c_desc_motivo else pd.NA
        out["Vehiculo_Real"] = out[c_vehiculo_real].astype("string").str.strip() if c_vehiculo_real else pd.NA
        out["Conductor"] = out[c_conductor].astype("string").str.strip() if c_conductor else pd.NA
        out["Hora_Ini_Teorica"] = out[c_hora_ini_teorica].astype("string").str.strip() if c_hora_ini_teorica else pd.NA
        out["Hora_Ini_Referencia"] = out[c_hora_ini_referencia].astype("string").str.strip() if c_hora_ini_referencia else pd.NA
        out["Hora_Ini_Real_Cabecera"] = out[c_hora_ini_real_cabecera].astype("string").str.strip() if c_hora_ini_real_cabecera else pd.NA
        out["Hora_Ini_Offset"] = out[c_hora_ini_offset].astype("string").str.strip() if c_hora_ini_offset else pd.NA
        out["OffsetInicio"] = out[c_offsetinicio].astype("string").str.strip() if c_offsetinicio else pd.NA
        out["Hora_Ini_Est_Cabecera"] = out[c_hora_ini_est_cabecera].astype("string").str.strip() if c_hora_ini_est_cabecera else pd.NA
        out["Hora_Fin_Real"] = out[c_hora_fin_real].astype("string").str.strip() if c_hora_fin_real else pd.NA
        out["OffsetFin"] = out[c_offsetfin].astype("string").str.strip() if c_offsetfin else pd.NA
        out["KmFueraRuta"] = self.tu.to_numeric(out[c_kmfueraruta]) if c_kmfueraruta else np.nan
        out["ServBusRef"] = out[c_servbusref].astype("string").str.strip() if c_servbusref else pd.NA
        out["Id_Viaje_Ref"] = out[c_id_viaje_ref].astype("string").str.strip() if c_id_viaje_ref else pd.NA
        out["Id_Linea_Referencia"] = out[c_id_linea_referencia].astype("string").str.strip() if c_id_linea_referencia else pd.NA
        out["Lista_Acciones"] = out[c_lista_acciones].astype("string").str.strip() if c_lista_acciones else pd.NA
        out["KmProgAd"] = self.tu.metros_a_km(out[c_kmprogad]) if c_kmprogad else np.nan
        out["KmEjecutado"] = self.tu.metros_a_km(out[c_kmejecutado]) if c_kmejecutado else np.nan

        keep = [
            "Fecha_key", "Servicio_key", "Coche_key", "Viaje_key",
            "Fecha", "Servicio", "Id_Viaje", "Viaje_Linea", "Id_Linea",
            "Sentido", "Tabla", "Planificado", "Eliminado", "EstMotivoElim",
            "Descripcion_Motivo_Elim", "Vehiculo_Real", "Conductor",
            "Hora_Ini_Teorica", "Hora_Ini_Referencia", "Hora_Ini_Real_Cabecera",
            "Hora_Ini_Offset", "OffsetInicio", "Hora_Ini_Est_Cabecera",
            "Hora_Fin_Real", "OffsetFin", "KmFueraRuta", "ServBusRef",
            "Id_Viaje_Ref", "Id_Linea_Referencia", "Lista_Acciones",
            "KmProgAd", "KmEjecutado"
        ]
        out = out[keep].copy()

        print(f"✅ Detallado cargado: filas útiles={len(out)}")
        return out

    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_ics = self.load_ics(fecha)
        df_det = self.load_detallado(fecha)

        print("\n" + "=" * 80)
        print("3) CRUCE DETALLADO ↔ ICS")
        print("=" * 80)

        merge_keys = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]
        df_merge = df_det.merge(
            df_ics[["IdICS"] + merge_keys],
            on=merge_keys,
            how="left"
        )

        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_det["Fecha"].astype(str))

        out = pd.DataFrame({
            "Id_ICS": self.tu.to_int64(df_merge["IdICS"]),
            "Fecha": df_merge["Fecha"],
            "Servicio": df_merge["Servicio"],
            "Id_Viaje": df_merge["Id_Viaje"],
            "Viaje_Linea": df_merge["Viaje_Linea"],
            "Id_Linea": df_merge["Id_Linea"],
            "Sentido": df_merge["Sentido"],
            "Tabla": df_merge["Tabla"],
            "Planificado": df_merge["Planificado"],
            "Eliminado": df_merge["Eliminado"],
            "EstMotivoElim": df_merge["EstMotivoElim"],
            "Descripcion_Motivo_Elim": df_merge["Descripcion_Motivo_Elim"],
            "Vehiculo_Real": df_merge["Vehiculo_Real"],
            "Conductor": df_merge["Conductor"],
            "Hora_Ini_Teorica": df_merge["Hora_Ini_Teorica"],
            "Hora_Ini_Referencia": df_merge["Hora_Ini_Referencia"],
            "Hora_Ini_Real_Cabecera": df_merge["Hora_Ini_Real_Cabecera"],
            "Hora_Ini_Offset": df_merge["Hora_Ini_Offset"],
            "OffsetInicio": df_merge["OffsetInicio"],
            "Hora_Ini_Est_Cabecera": df_merge["Hora_Ini_Est_Cabecera"],
            "Hora_Fin_Real": df_merge["Hora_Fin_Real"],
            "OffsetFin": df_merge["OffsetFin"],
            "KmFueraRuta": df_merge["KmFueraRuta"],
            "ServBusRef": df_merge["ServBusRef"],
            "Id_Viaje_Ref": df_merge["Id_Viaje_Ref"],
            "Id_Linea_Referencia": df_merge["Id_Linea_Referencia"],
            "Lista_Acciones": df_merge["Lista_Acciones"],
            "KmProgAd": df_merge["KmProgAd"],
            "KmEjecutado": df_merge["KmEjecutado"],
        })

        total_con_match_archivo = int(out["Id_ICS"].notna().sum())
        out = out[out["Id_ICS"].notna()].copy()
        out = out.drop_duplicates(subset=["Id_ICS"], keep="first").reset_index(drop=True)

        print("\n✅ Tabla Detallado construida:")
        print(f"   Filas con Id_ICS desde archivo ICS: {total_con_match_archivo}")
        print(f"   Filas finales únicas por Id_ICS: {len(out)}")
        print(f"📌 Nombre lógico: DETALLADO_{fecha_nombre}")

        return out, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.detallado
# =============================================================================

class PostgresDetalladoLoader:
    DF_TO_DB = {
        "Id_ICS": "id_ics",
        "Fecha": "fecha",
        "Servicio": "servicio",
        "Id_Viaje": "id_viaje",
        "Viaje_Linea": "viaje_linea",
        "Id_Linea": "id_linea",
        "Sentido": "sentido",
        "Tabla": "tabla",
        "Planificado": "planificado",
        "Eliminado": "eliminado",
        "EstMotivoElim": "estmotivoelim",
        "Descripcion_Motivo_Elim": "descripcion_motivo_elim",
        "Vehiculo_Real": "vehiculo_real",
        "Conductor": "conductor",
        "Hora_Ini_Teorica": "hora_ini_teorica",
        "Hora_Ini_Referencia": "hora_ini_referencia",
        "Hora_Ini_Real_Cabecera": "hora_ini_real_cabecera",
        "Hora_Ini_Offset": "hora_ini_offset",
        "OffsetInicio": "offsetinicio",
        "Hora_Ini_Est_Cabecera": "hora_ini_est_cabecera",
        "Hora_Fin_Real": "hora_fin_real",
        "OffsetFin": "offsetfin",
        "KmFueraRuta": "kmfueraruta",
        "ServBusRef": "servbusref",
        "Id_Viaje_Ref": "id_viaje_ref",
        "Id_Linea_Referencia": "id_linea_referencia",
        "Lista_Acciones": "lista_acciones",
        "KmProgAd": "kmprogad",
        "KmEjecutado": "kmejecutado",
    }

    def __init__(
        self,
        schema: str,
        table: str,
        batch_size: int = 5000,
        parent_schema: str = PG_SCHEMA_ICS,
        parent_table: str = PG_TABLE_ICS,
    ):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.parent_schema = parent_schema
        self.parent_table = parent_table

    def _connect(self):
        return get_db_connection()

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
    def _parse_time_to_time(v):
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        if re.match(r"^\d{1,2}:\d{2}$", s):
            s = f"{s}:00"
        if not re.match(r"^\d{1,2}:\d{2}:\d{2}$", s):
            return None
        try:
            return datetime.strptime(s, "%H:%M:%S").time()
        except Exception:
            return None

    def _prepare_df_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.DF_TO_DB.keys() if c not in df.columns]
        if missing:
            raise SystemExit(f"❌ Faltan columnas en DF para insertar a Postgres: {missing}")

        d = df[list(self.DF_TO_DB.keys())].copy()
        d = d.rename(columns=self.DF_TO_DB)

        d["fecha"] = d["fecha"].apply(self._parse_fecha_to_date)

        time_cols = [
            "hora_ini_teorica", "hora_ini_referencia", "hora_ini_real_cabecera",
            "hora_ini_offset", "hora_ini_est_cabecera", "hora_fin_real"
        ]
        for c in time_cols:
            d[c] = d[c].apply(self._parse_time_to_time)

        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce")
        d["kmfueraruta"] = pd.to_numeric(d["kmfueraruta"], errors="coerce")
        d["kmprogad"] = pd.to_numeric(d["kmprogad"], errors="coerce")
        d["kmejecutado"] = pd.to_numeric(d["kmejecutado"], errors="coerce")

        str_cols = [
            "servicio", "id_viaje", "viaje_linea", "id_linea", "sentido", "tabla",
            "planificado", "eliminado",
            "estmotivoelim", "descripcion_motivo_elim", "vehiculo_real", "conductor",
            "offsetinicio", "offsetfin", "servbusref", "id_viaje_ref",
            "id_linea_referencia", "lista_acciones"
        ]
        for c in str_cols:
            d[c] = d[c].astype("string")

        d = d[d["id_ics"].notna()].copy()
        return d

    def _get_existing_ics_ids(self, ids: list[int]) -> set[int]:
        if not ids:
            return set()

        sql = f'''
            SELECT "id_ics"
            FROM "{self.parent_schema}"."{self.parent_table}"
            WHERE "id_ics" = ANY(%s)
        '''

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (ids,))
                rows = cur.fetchall()

        return {int(r[0]) for r in rows if r and r[0] is not None}

    def delete_existing_for_date(self, conn, fecha_date) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{self.table}" WHERE fecha = %s',
                (fecha_date,)
            )
            return cur.rowcount

    def _export_descartados_fk(self, df_desc: pd.DataFrame, fecha_base=None) -> Optional[str]:
        if not EXPORT_DESCARTADOS_FK:
            return None
        if df_desc is None or df_desc.empty:
            return None

        os.makedirs(EXPORT_CSV_DIR, exist_ok=True)

        sufijo_fecha = "SIN_FECHA"
        if fecha_base is not None:
            try:
                sufijo_fecha = pd.to_datetime(fecha_base).strftime("%d_%m_%Y")
            except Exception:
                pass

        path = os.path.join(EXPORT_CSV_DIR, f"DETALLADO_DESCARTADOS_FK_{sufijo_fecha}.csv")
        df_desc.to_csv(path, index=False, sep=EXPORT_CSV_SEP, encoding=EXPORT_CSV_ENCODING)
        print(f"📤 CSV descartados FK: {path}")
        return path

    def insert_df(self, df: pd.DataFrame) -> int:
        df_db = self._prepare_df_for_db(df)

        if df_db.empty:
            print("⚠️ No hay filas para insertar en detallado.")
            return 0

        ids_df = (
            pd.to_numeric(df_db["id_ics"], errors="coerce")
            .dropna()
            .astype("int64")
            .unique()
            .tolist()
        )

        existing_ids = self._get_existing_ics_ids(ids_df)

        df_invalid = df_db[~df_db["id_ics"].isin(existing_ids)].copy()
        before = len(df_db)
        df_db = df_db[df_db["id_ics"].isin(existing_ids)].copy()
        discarded = before - len(df_db)

        print(f"🔎 Filas originales preparadas para DB: {before}")
        print(f"✅ Filas con id_ics existente en {self.parent_schema}.{self.parent_table}: {len(df_db)}")
        print(f"⚠️ Filas descartadas por FK: {discarded}")

        if not df_invalid.empty:
            ejemplos = (
                pd.to_numeric(df_invalid["id_ics"], errors="coerce")
                .dropna()
                .astype("int64")
                .drop_duplicates()
                .head(20)
                .tolist()
            )
            print(f"⚠️ Ejemplos id_ics descartados: {ejemplos}")

            fecha_base = None
            if "fecha" in df_invalid.columns and len(df_invalid) > 0:
                fecha_base = df_invalid.iloc[0]["fecha"]

            self._export_descartados_fk(df_invalid, fecha_base=fecha_base)

        if df_db.empty:
            print("⚠️ No quedaron filas válidas para insertar.")
            return 0

        full_table = f'"{self.schema}"."{self.table}"'
        cols = list(df_db.columns)
        col_sql = ",".join([f'"{c}"' for c in cols])

        sql = f"""
            INSERT INTO {full_table} ({col_sql})
            VALUES %s
            ON CONFLICT ("id_ics")
            DO UPDATE SET
                "fecha" = EXCLUDED."fecha",
                "servicio" = EXCLUDED."servicio",
                "id_viaje" = EXCLUDED."id_viaje",
                "viaje_linea" = EXCLUDED."viaje_linea",
                "id_linea" = EXCLUDED."id_linea",
                "sentido" = EXCLUDED."sentido",
                "tabla" = EXCLUDED."tabla",
                "planificado" = EXCLUDED."planificado",
                "eliminado" = EXCLUDED."eliminado",
                "estmotivoelim" = EXCLUDED."estmotivoelim",
                "descripcion_motivo_elim" = EXCLUDED."descripcion_motivo_elim",
                "vehiculo_real" = EXCLUDED."vehiculo_real",
                "conductor" = EXCLUDED."conductor",
                "hora_ini_teorica" = EXCLUDED."hora_ini_teorica",
                "hora_ini_referencia" = EXCLUDED."hora_ini_referencia",
                "hora_ini_real_cabecera" = EXCLUDED."hora_ini_real_cabecera",
                "hora_ini_offset" = EXCLUDED."hora_ini_offset",
                "offsetinicio" = EXCLUDED."offsetinicio",
                "hora_ini_est_cabecera" = EXCLUDED."hora_ini_est_cabecera",
                "hora_fin_real" = EXCLUDED."hora_fin_real",
                "offsetfin" = EXCLUDED."offsetfin",
                "kmfueraruta" = EXCLUDED."kmfueraruta",
                "servbusref" = EXCLUDED."servbusref",
                "id_viaje_ref" = EXCLUDED."id_viaje_ref",
                "id_linea_referencia" = EXCLUDED."id_linea_referencia",
                "lista_acciones" = EXCLUDED."lista_acciones",
                "kmprogad" = EXCLUDED."kmprogad",
                "kmejecutado" = EXCLUDED."kmejecutado"
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
                    records = [tuple(self._py(x) for x in row) for row in chunk.itertuples(index=False, name=None)]

                    execute_values(
                        cur,
                        sql,
                        records,
                        page_size=len(records)
                    )

                    total += len(records)
                    print(f"  ✅ Upsert detallado {total}/{len(df_db)}")

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
        return get_db_connection()

    def get_id_reporte(self, nombre_reporte: str, default_id: int = 2) -> int:
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
    filename = f"DETALLADO_{fecha_nombre}.csv"
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
        az = AzureBlobReader(AzureConfig(connection_string=conn_azure))
        builder = DetalladoBuilder(az, filtro_zona_tipo=FILTRO_ZONA_TIPO)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print("4) CARGANDO A POSTGRES sne.detallado")
        print("=" * 80)

        loader = PostgresDetalladoLoader(
            schema=PG_SCHEMA_NAME,
            table=PG_TABLE_NAME,
            batch_size=PG_BATCH_SIZE,
            parent_schema=PG_SCHEMA_ICS,
            parent_table=PG_TABLE_ICS,
        )
        total = loader.insert_df(df_final)
        print(f"✅ sne.detallado upsert: {total} filas")

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
        print("5) GUARDANDO LOG EN log.procesa_report_sne")
        print("=" * 80)

        logger = ReportRunLogger()
        id_reporte = logger.get_id_reporte("Tabla Detallado", default_id=2)

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

    print("\n" + "=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)

if __name__ == "__main__":
    main()
