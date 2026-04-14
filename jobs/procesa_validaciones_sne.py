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

CURRENT_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database.database_manager import get_db_connection

# =============================================================================
# CONFIG
# =============================================================================

FECHA_SEMILLA_STR = "01/04/2026"   # dd/mm/yyyy

AZURE_CONN_ENV = "AZURE_STORAGE_CONNECTION_STRING"
AZURE_CONTAINER_ACTIVIDAD = "e02-transmitools"
AZURE_CONTAINER_DETALLADO = "e01-fms"
AZURE_CONTAINER_VALIDACIONES = "e02-transmitools"

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
PG_TABLE_NAME = "validaciones"

PG_SCHEMA_ICS = "sne"
PG_TABLE_ICS = "ics"
PG_TABLE_ICS_ID_COLUMN = "id_ics"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

PG_BATCH_SIZE = 5000
NOMBRE_REPORTE_LOG = "Tabla Validaciones"
DEFAULT_ID_REPORTE = 8
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
    container_detallado: str = AZURE_CONTAINER_DETALLADO
    container_validaciones: str = AZURE_CONTAINER_VALIDACIONES

class AzureBlobReader:
    def __init__(self, cfg: AzureConfig):
        self.cfg = cfg
        self.service = BlobServiceClient.from_connection_string(cfg.connection_string)
        self.container_actividad = self.service.get_container_client(cfg.container_actividad)
        self.container_detallado = self.service.get_container_client(cfg.container_detallado)
        self.container_validaciones = self.service.get_container_client(cfg.container_validaciones)

    def exists(self, container_name: str, blob_path: str) -> bool:
        try:
            if container_name == "actividad":
                container = self.container_actividad
            elif container_name == "detallado":
                container = self.container_detallado
            else:
                container = self.container_validaciones
            bc = container.get_blob_client(blob_path)
            bc.get_blob_properties()
            return True
        except Exception:
            return False

    def read_bytes(self, container_name: str, blob_path: str, timeout: int = 600) -> bytes:
        if container_name == "actividad":
            container = self.container_actividad
        elif container_name == "detallado":
            container = self.container_detallado
        else:
            container = self.container_validaciones
        bc = container.get_blob_client(blob_path)
        stream = bc.download_blob(timeout=timeout, max_concurrency=8, validate_content=False)
        return stream.readall()

# =============================================================================
# BUILDER VALIDACIONES
# =============================================================================

class ValidacionesBuilder:
    def __init__(self, azure_reader: AzureBlobReader):
        self.az = azure_reader
        self.io = DataIO()
        self.tu = TransformUtils()

    def _blob_path_ics_candidates(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        fecha_txt = fecha.strftime("%Y%m%d")
        nombre = f"{fecha_txt}_ics_smartoperator_etapa1.csv"
        return [(f"{anio}/11_ics_offline/10_ics_etapas/10_etapa1/{nombre}", nombre)]

    def _blob_paths_detallado(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        fecha_txt = fecha.strftime("%Y%m%d")

        out: List[Tuple[str, str]] = []
        for carpeta_tipo in ["ZN", "TR"]:
            for zona_blob in ["sc", "uq"]:
                raiz = "1_sc" if zona_blob == "sc" else "2_uq"
                if carpeta_tipo == "ZN":
                    carpeta = f"20_detallado_viaje_zonal_{zona_blob}"
                    nombre = f"{fecha_txt}_detallado_viaje_zonal_{zona_blob}.csv"
                else:
                    carpeta = f"21_detallado_viaje_troncal_{zona_blob}"
                    nombre = f"{fecha_txt}_detallado_viaje_troncal_al_{zona_blob}.csv"
                ruta = f"{raiz}/{anio}/{carpeta}/{nombre}"
                out.append((ruta, nombre))
        return out

    def _blob_paths_validaciones(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        fecha_ymd = fecha.strftime("%Y%m%d")

        out: List[Tuple[str, str]] = []
        for zona in ["sc", "uq"]:
            nombre = f"{fecha_ymd}_pax_{zona}.csv"
            ruta = f"{anio}/60_validaciones/{nombre}"
            out.append((ruta, nombre))
        return out

    def load_ics(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ICS (AZURE)")
        print("=" * 80)

        ruta = None
        nombre = None
        for ruta_cand, nombre_cand in self._blob_path_ics_candidates(fecha):
            if self.az.exists("actividad", ruta_cand):
                ruta = ruta_cand
                nombre = nombre_cand
                break

        if ruta is None or nombre is None:
            rutas = "\n   ".join(r for r, _ in self._blob_path_ics_candidates(fecha))
            raise SystemExit(f"❌ No existe ICS en Azure para esa fecha. Se intentó con:\n   {rutas}")

        df = self.io.leer_csv_desde_bytes(self.az.read_bytes("actividad", ruta), dtype=str)

        c_idics = pick_col(df, ["IdICS"])
        c_fecha = pick_col(df, ["Fecha Viaje"])
        c_servicio = pick_col(df, ["Servicio"])
        c_coche = pick_col(df, ["Coche"])
        c_viaje_linea = pick_col(df, ["ViajeLinea", "Viaje Línea"])

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst="auto")
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_coche])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])
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
            if not self.az.exists("detallado", ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes("detallado", ruta), dtype=str)
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontró ningún Detallado para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["Fecha"])
        c_linea = pick_col(df, ["Id Línea", "Id Linea"])
        c_vehiculo_real = pick_col(df, ["Vehículo Real", "Vehiculo Real"])
        c_hora_ini_real = pick_col(df, ["Hora Ini Real Cabecera"], required=False)
        c_hora_ini_ref = pick_col(df, ["Hora Ini Referencia"], required=False)
        c_hora_fin_real = pick_col(df, ["Hora Fin Real"], required=False)

        c_servicio = pick_col(df, ["Servicio"])
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea"])

        out = df.copy()
        out["Fecha_val_key"] = pd.to_datetime(
            out[c_fecha].astype(str).str.replace("\ufeff", "", regex=False).str.strip(),
            errors="coerce",
            dayfirst=True
        ).dt.date

        out["IdLinea_key"] = out[c_linea].astype(str).str.extract(r"(\d+)", expand=False)
        out["Vehiculo_key"] = out[c_vehiculo_real].astype(str).str.replace(r"\D", "", regex=True)

        out["HoraIniReal_td"] = pd.to_timedelta(out[c_hora_ini_real].astype(str), errors="coerce") if c_hora_ini_real else pd.NaT
        out["HoraFinReal_td"] = pd.to_timedelta(out[c_hora_fin_real].astype(str), errors="coerce") if c_hora_fin_real else pd.NaT
        out["HoraIniRef_td"] = pd.to_timedelta(out[c_hora_ini_ref].astype(str), errors="coerce") if c_hora_ini_ref else pd.NaT

        out["HoraIni_td"] = out["HoraIniReal_td"].fillna(out["HoraIniRef_td"])
        out["HoraFin_td"] = out["HoraFinReal_td"]

        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_tabla])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])

        keep = [
            "Fecha_val_key", "IdLinea_key", "Vehiculo_key",
            "HoraIni_td", "HoraFin_td",
            "Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"
        ]
        out = out[keep].copy()

        print(f"✅ Detallado cargado: filas útiles={len(out)}")
        return out

    def load_validaciones(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("3) CARGANDO VALIDACIONES (AZURE)")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        for ruta, nombre in self._blob_paths_validaciones(fecha):
            if not self.az.exists("validaciones", ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes("validaciones", ruta), dtype=str)
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontraron archivos de Validaciones para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        print("📋 Columnas detectadas en Validaciones:")
        print(list(df.columns))

        c_fecha_clearing = pick_col(df, ["Fecha Clearing"], required=False)
        c_dia_trx = pick_col(df, ["Día Trx", "Dia Trx"], required=False)
        c_linea_sae = pick_col(df, ["Linea SAE", "ID Línea", "ID Linea"])
        c_parada = pick_col(df, ["Parada"])
        c_vehiculo = pick_col(df, ["Vehiculo"])
        c_hora_trx = pick_col(df, ["Hora Trx"])
        c_nombre_linea = pick_col(df, ["Nombre Linea", "Nombre Línea"], required=False)

        out = df.copy()
        c_fecha_base = c_dia_trx or c_fecha_clearing
        fecha_registro_key = self.tu.fecha_key_robusta(
            out[c_fecha_base].astype(str).str.strip(),
            prefer_dayfirst="auto"
        )
        out["Fecha_registro"] = pd.to_datetime(fecha_registro_key, errors="coerce").dt.date

        out["Fecha_val_key"] = out["Fecha_registro"]

        out["IdLinea_key"] = out[c_linea_sae].astype(str).str.extract(r"(\d+)", expand=False)

        out["Vehiculo_key"] = out[c_vehiculo].astype(str).str.replace(r"\D", "", regex=True)
        out["HoraTrx_td"] = pd.to_timedelta(out[c_hora_trx].astype(str), errors="coerce")

        out["Instante"] = pd.to_datetime(
            out[c_hora_trx].astype(str).str.strip(),
            format="%H:%M:%S",
            errors="coerce"
        ).dt.time

        mask_na_instante = out["Instante"].isna()
        if mask_na_instante.any():
            out.loc[mask_na_instante, "Instante"] = pd.to_datetime(
                out.loc[mask_na_instante, c_hora_trx].astype(str).str.strip(),
                format="%H:%M",
                errors="coerce"
            ).dt.time

        if c_nombre_linea:
            out["Linea"] = out[c_nombre_linea].astype("string").str.strip()
        else:
            out["Linea"] = out[c_linea_sae].astype("string").str.strip()
        out["Parada"] = out[c_parada].astype("string").str.strip()
        out["Vehiculo"] = out[c_vehiculo].astype("string").str.strip()

        keep = [
            "Fecha_registro", "Linea", "Parada", "Vehiculo", "Instante",
            "Fecha_val_key", "IdLinea_key", "Vehiculo_key", "HoraTrx_td"
        ]
        out = out[keep].copy()

        print(f"✅ Validaciones cargadas: filas útiles={len(out)}")
        return out

    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_ics = self.load_ics(fecha)
        df_det = self.load_detallado(fecha)
        df_val = self.load_validaciones(fecha)

        print("\n" + "=" * 80)
        print("4) CRUCE DETALLADO ↔ ICS")
        print("=" * 80)

        merge_keys = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]
        df_det_con_idics = df_det.merge(
            df_ics[["IdICS"] + merge_keys],
            on=merge_keys,
            how="left"
        )

        print("\n" + "=" * 80)
        print("5) CRUCE VALIDACIONES ↔ DETALLADO+ICS")
        print("=" * 80)

        tmp_val = df_val.merge(
            df_det_con_idics[["IdICS", "Fecha_val_key", "IdLinea_key", "Vehiculo_key", "HoraIni_td", "HoraFin_td"]],
            on=["Fecha_val_key", "IdLinea_key", "Vehiculo_key"],
            how="left",
        )

        cond_val = (tmp_val["HoraTrx_td"] >= tmp_val["HoraIni_td"]) & (tmp_val["HoraTrx_td"] <= tmp_val["HoraFin_td"])
        df_val_match = tmp_val[cond_val & tmp_val["IdICS"].notna()].copy()

        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_val["Fecha_registro"].astype(str))

        out = pd.DataFrame({
            "Id_ICS": self.tu.to_int64(df_val_match["IdICS"]),
            "Fecha_Registro": df_val_match["Fecha_registro"],
            "Linea": df_val_match["Linea"],
            "Parada": df_val_match["Parada"],
            "Vehiculo": df_val_match["Vehiculo"],
            "Instante": df_val_match["Instante"],
        })

        out = out[out["Id_ICS"].notna()].copy()

        out = out.drop_duplicates(
            subset=["Id_ICS", "Fecha_Registro", "Linea", "Parada", "Vehiculo", "Instante"],
            keep="first"
        ).reset_index(drop=True)

        print("\n✅ Tabla Validaciones construida:")
        print(f"   Filas con Id_ICS: {len(out)}")
        print(f"📌 Nombre lógico: VALIDACIONES_{fecha_nombre}")

        return out, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.validaciones
# =============================================================================

class PostgresValidacionesLoader:
    DF_TO_DB = {
        "Id_ICS": "id_ics",
        "Fecha_Registro": "fecha_registro",
        "Linea": "linea",
        "Parada": "parada",
        "Vehiculo": "vehiculo",
        "Instante": "instante",
    }

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

        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
        else:
            dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
            if pd.isna(dt):
                dt = pd.to_datetime(s, errors="coerce")

        if pd.isna(dt):
            return None
        return dt.date()

    @staticmethod
    def _parse_time_to_time(valor):
        if valor is None:
            return None
        try:
            if valor is pd.NA:
                return None
        except Exception:
            pass

        if pd.isna(valor):
            return None

        if hasattr(valor, "hour") and hasattr(valor, "minute") and hasattr(valor, "second"):
            return valor

        s = str(valor).strip()
        if s == "":
            return None

        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(s, fmt).time()
            except ValueError:
                continue

        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.time()

    def _prepare_df_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.DF_TO_DB.keys() if c not in df.columns]
        if missing:
            raise SystemExit(f"❌ Faltan columnas en DF para insertar a Postgres: {missing}")

        d = df[list(self.DF_TO_DB.keys())].copy()
        d = d.rename(columns=self.DF_TO_DB)

        d["fecha_registro"] = d["fecha_registro"].apply(self._parse_fecha_to_date)
        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce")
        d["instante"] = d["instante"].apply(self._parse_time_to_time)

        for c in ["linea", "parada", "vehiculo"]:
            d[c] = d[c].astype("string")

        d = d[d["id_ics"].notna()].copy()

        d = d.drop_duplicates(
            subset=["id_ics", "fecha_registro", "linea", "parada", "vehiculo", "instante"],
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
                f'DELETE FROM "{self.schema}"."{self.table}" WHERE fecha_registro = %s',
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
                "fecha_registro",
                "linea",
                "parada",
                "vehiculo",
                "instante"
            )
            DO UPDATE SET
                "instante" = EXCLUDED."instante"
        """

        total = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                if DELETE_EXISTING_FOR_DATE and "fecha_registro" in df_db.columns and len(df_db) > 0:
                    fecha0 = df_db.iloc[0]["fecha_registro"]
                    deleted = self.delete_existing_for_date(conn, fecha0)
                    print(f"🧹 DELETE previo (fecha_registro={fecha0}): {deleted} filas")

                for start in range(0, len(df_db), self.batch_size):
                    chunk = df_db.iloc[start:start + self.batch_size]

                    chunk = chunk.drop_duplicates(
                        subset=["id_ics", "fecha_registro", "linea", "parada", "vehiculo", "instante"],
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
                    print(f"  ✅ Upsert validaciones {total}/{len(df_db)}")

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

    def get_id_reporte(self, nombre_reporte: str, default_id: int = 8) -> int:
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
          AND COALESCE("registros_proce", 0) > 0
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
    filename = f"VALIDACIONES_{fecha_nombre}.csv"
    path = os.path.join(EXPORT_CSV_DIR, filename)

    df.to_csv(
        path,
        index=False,
        sep=EXPORT_CSV_SEP,
        encoding=EXPORT_CSV_ENCODING
    )
    print(f"📤 Export CSV generado: {path}")
    return path

def _legacy_main_validaciones() -> None:
    start_perf = time.perf_counter()

    conn_azure = _get_connection_string()
    # Bloque legado: se conserva solo como referencia y no se usa.

    try:
        fecha_dt = datetime.strptime(FECHA_SEMILLA_STR, "%d/%m/%Y")
    except ValueError:
        raise SystemExit("FECHA_SEMILLA_STR debe estar en formato dd/mm/yyyy, ej: 31/01/2026")

    estado = "ok"
    archivos_total = 1
    archivos_ok = 1
    archivos_error = 0
    registros_proce = 0

    try:
        az = AzureBlobReader(AzureConfig(connection_string=conn_azure))
        builder = ValidacionesBuilder(az)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))
        if registros_proce == 0:
            raise RuntimeError("No se generaron registros para cargar. Revisar cruce con sne.ics o disponibilidad de datos.")

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print("6) CARGANDO A POSTGRES sne.validaciones")
        print("=" * 80)

        loader = PostgresValidacionesLoader(
                        schema=PG_SCHEMA_NAME,
            table=PG_TABLE_NAME,
            batch_size=PG_BATCH_SIZE,
            schema_ics=PG_SCHEMA_ICS,
            table_ics=PG_TABLE_ICS,
            ics_id_column=PG_TABLE_ICS_ID_COLUMN,
        )
        total = loader.insert_df(df_final)
        print(f"✅ sne.validaciones upsert: {total} filas")

    except SystemExit as e:
        estado = "error"
        archivos_ok = 0
        archivos_error = 1
        print("❌ ERROR en el proceso:", repr(e))
        raise
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

        logger = ReportRunLogger()
        id_reporte = logger.get_id_reporte("Tabla Validaciones", default_id=8)

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
        builder = ValidacionesBuilder(az)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print("6) CARGANDO A POSTGRES sne.validaciones")
        print("=" * 80)

        loader = PostgresValidacionesLoader(
            schema=PG_SCHEMA_NAME,
            table=PG_TABLE_NAME,
            batch_size=PG_BATCH_SIZE,
            schema_ics=PG_SCHEMA_ICS,
            table_ics=PG_TABLE_ICS,
            ics_id_column=PG_TABLE_ICS_ID_COLUMN,
        )
        total = loader.insert_df(df_final)
        print(f"âœ… sne.validaciones upsert: {total} filas")

    except SystemExit as e:
        estado = "error"
        archivos_ok = 0
        archivos_error = 1
        print("❌ ERROR en el proceso:", repr(e))
        raise
    except Exception as e:
        estado = "error"
        archivos_ok = 0
        archivos_error = 1
        print("âŒ ERROR en el proceso:", repr(e))
        raise
    finally:
        end_ts = datetime.now()
        duracion_seg = int(round(time.perf_counter() - start_perf))

        print("\n" + "=" * 80)
        print("7) GUARDANDO LOG EN log.procesa_report_sne")
        print("=" * 80)

        logger = ReportRunLogger()
        id_reporte = logger.get_id_reporte("Tabla Validaciones", default_id=8)

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
            f"ðŸ“Œ log: {PG_SCHEMA_LOG}.{PG_TABLE_LOG} | "
            f"id_reporte={id_reporte} | fecha={fecha_dt.date()} | estado={estado}"
        )
        print(f"â±ï¸ duraciÃ³n_seg={duracion_seg} | registros_proce={registros_proce}")

    print("\n" + "=" * 80)
    print("âœ… PROCESO COMPLETADO")
    print("=" * 80)


if __name__ == "__main__":
    main()
