from __future__ import annotations

import os
import re
import csv
import time
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict

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

# Si no existe un log OK previo, empezar desde esta fecha
FECHA_SEMILLA_STR = "01/04/2026"   # dd/mm/yyyy

FILTRO_ZONA_TIPO = 3               # 1=ZN, 2=TR, 3=Ambas

AZURE_CONN_ENV = "AZURE_STORAGE_CONNECTION_STRING"
AZURE_CONTAINER_ACTIVIDAD = "e01-fms"
AZURE_CONTAINER_ICS = "e02-transmitools"
AZURE_CONTAINER_DETALLADO = "e01-fms"
AZURE_CONTAINER_FMS = "e01-fms"

# Solo local. En GitHub se espera AZURE_STORAGE_CONNECTION_STRING desde Secrets
CONNECTION_STRING_LOCAL = ""

USE_LOCAL_FILES = False
PROCESS_DATE_STR = ""  # dd/mm/yyyy. Si tiene valor, esta fecha manda.

LOCAL_ICS_FILE = ""
LOCAL_DETALLADO_FILES = [
]
LOCAL_STAT_FILES: List[str] = []
LOCAL_TARDIOS_FILES: List[str] = []

EXPORT_DIR = r"C:\Users\analista.centroinf3\OneDrive - Grupo Express\Juan Buitrago\Desarrollos_Python2\ExportesICS"
ENABLE_POSTGRES_LOAD = True

# -------------------------------
# POSTGRES
# -------------------------------
PG_SCHEMA_NAME = "sne"
PG_TABLE_NAME = "ics"

PG_SCHEMA_GESTION = "sne"
PG_TABLE_GESTION = "gestion_sne"

PG_SCHEMA_MOTIVO_RESP = "sne"
PG_TABLE_MOTIVO_RESP = "ics_motivo_resp"

PG_SCHEMA_MOTIVOS = "sne"
PG_TABLE_MOTIVOS = "motivos_eliminacion"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"
DEFAULT_ID_REPORTE = 1
NOMBRE_REPORTE_LOG = "Tabla ICS"

PG_BATCH_SIZE = 5000

RESPONSABLE_DEFAULT = 0
GESTION_REVISOR_DEFAULT_INT = 0
DELETE_EXISTING_FOR_DATE = False


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
            s = s.replace("\ufeff", "").replace("ï»¿", "").replace('"', "").strip()
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
                na = dt_iso.isna()
                if na.any():
                    dt_iso2 = pd.to_datetime(sr[mask_iso][na], errors="coerce", format="%Y-%m-%d %H:%M:%S")
                    dt_iso.loc[na] = dt_iso2
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
    def to_float(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").astype("float64")

    @staticmethod
    def metros_a_km(series: pd.Series) -> pd.Series:
        raw = pd.to_numeric(series, errors="coerce").astype("float64")
        return raw / 1000.0

    @staticmethod
    def parse_datetime_robusto(series: pd.Series, dayfirst: bool = True) -> pd.Series:
        s = series.astype("string").fillna("").str.replace("\ufeff", "", regex=False).str.replace("ï»¿", "", regex=False).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        # Priorizar formatos ISO para evitar inversiones con dayfirst=True.
        mask_iso = s.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$", na=False)
        if mask_iso.any():
            s_iso = s[mask_iso]
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
                parsed = pd.to_datetime(s_iso, format=fmt, errors="coerce")
                ok = parsed.notna() & out.loc[s_iso.index].isna()
                if ok.any():
                    out.loc[s_iso.index[ok]] = parsed.loc[ok]

        mask_na = out.isna() & s.notna()
        if mask_na.any():
            out.loc[mask_na] = pd.to_datetime(s[mask_na], errors="coerce", dayfirst=dayfirst)

        mask_na = out.isna() & s.notna()
        if mask_na.any():
            s2 = s[mask_na]

            for fmt in (
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y",
                "%Y-%m-%d",
            ):
                parsed = pd.to_datetime(s2, format=fmt, errors="coerce")
                ok = parsed.notna()
                if ok.any():
                    out.loc[s2.index[ok]] = parsed.loc[ok]

                mask_still_na = out.loc[s2.index].isna()
                if not mask_still_na.any():
                    break
                s2 = s2.loc[mask_still_na.index[mask_still_na]]

        return out


def normalize_name(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("\ufeff", "").replace("ï»¿", "")
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = s.replace("ã¡", "a").replace("ã©", "e").replace("ã­", "i").replace("ã³", "o").replace("ãº", "u")
    s = s.replace("lã­nea", "linea").replace("vehã­culo", "vehiculo").replace("descripciã³n", "descripcion")
    s = s.replace("concesiã³n", "concesion")
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


def clean_motivo(v) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\ufeff", "").replace("ï»¿", "")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_motivo(v) -> str:
    return clean_motivo(v).upper()


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
# BUILDER
# =============================================================================

class SNEExportBuilder:
    def __init__(self, azure_reader: Optional[AzureBlobReader], ics_reader: Optional[AzureBlobReader], detallado_reader: Optional[AzureBlobReader], fms_reader: Optional[AzureBlobReader], filtro_zona_tipo: int):
        self.az = azure_reader
        self.az_ics = ics_reader
        self.az_detallado = detallado_reader
        self.az_fms = fms_reader
        self.filtro = filtro_zona_tipo
        self.io = DataIO()
        self.tu = TransformUtils()

    @staticmethod
    def _load_local_csv(path_str: str) -> pd.DataFrame:
        path = Path(path_str)
        if not path.exists():
            raise SystemExit(f"❌ No existe archivo local: {path}")
        return DataIO.leer_csv_desde_bytes(path.read_bytes(), dtype=str)

    @staticmethod
    def _existing_local_files(paths: List[str]) -> List[Path]:
        return [Path(p) for p in paths if p and Path(p).exists()]

    def _subpaths(self) -> List[str]:
        return ["ZN", "TR"]

    def _blob_path_ics(self, fecha: datetime) -> Tuple[str, str]:
        anio = fecha.strftime("%Y")
        fecha_txt = fecha.strftime("%Y%m%d")
        nombre = f"{fecha_txt}_ics_smartoperator_etapa1.csv"
        ruta = f"{anio}/11_ics_offline/10_ics_etapas/10_etapa1/{nombre}"
        return ruta, nombre

    def _blob_paths_detallado(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        fecha_txt = fecha.strftime("%Y%m%d")

        out: List[Tuple[str, str]] = []
        for carpeta_tipo in self._subpaths():
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

    def _blob_paths_stat(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        fecha_txt = fecha.strftime("%Y%m%d")

        out: List[Tuple[str, str]] = []
        for zona_blob in ["sc", "uq"]:
            prefijo = "1_sc" if zona_blob == "sc" else "2_uq"
            raiz = f"{prefijo}/{anio}/100_datos_brutos_{zona_blob}"
            carpeta_db = f"10_DB_RPTDB_zonal_{zona_blob}"
            carpeta = f"20_TBFVH206_viajestat_zonal_{zona_blob}"
            nombre = f"{fecha_txt}_vehicle_location_interval_zonal_{zona_blob}.csv"
            ruta = f"{raiz}/{carpeta_db}/{carpeta}/{nombre}"
            out.append((ruta, nombre))
        return out

    def _prefixes_viajes_tardios(self, fecha: datetime) -> List[str]:
        anio = fecha.strftime("%Y")
        return [
            f"1_sc/{anio}/70_viajes_tardios_zonal_sc/",
            f"2_uq/{anio}/70_viajes_tardios_zonal_uq/",
        ]

    def load_ics(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ICS")
        print("=" * 80)

        local_ics = Path(LOCAL_ICS_FILE) if LOCAL_ICS_FILE else None
        if local_ics and local_ics.exists():
            df = self._load_local_csv(str(local_ics))
            nombre = local_ics.name
            print(f"? ICS local cargado: {nombre} | filas={len(df)} cols={len(df.columns)}")
        else:
            ruta, nombre = self._blob_path_ics(fecha)
            if self.az_ics is None:
                raise SystemExit("? No hay lector Azure disponible y tampoco ICS local configurado.")
            if not self.az_ics.exists(ruta):
                raise SystemExit(f"? No existe ICS en Azure:\n   {ruta}")

            df = self.io.leer_csv_desde_bytes(self.az_ics.read_bytes(ruta), dtype=str)
            print(f"? ICS cargado: {nombre} | filas={len(df)} cols={len(df.columns)}")
        print("DEBUG columnas ICS:", list(df.columns))

        c_fecha = pick_col(df, ["Fecha Viaje"])
        c_serv = pick_col(df, ["Servicio"])
        c_linea = pick_col(df, ["Linea SAE", "Línea SAE"])
        c_coche = pick_col(df, ["Coche"])
        c_viaje_linea = pick_col(df, ["ViajeLinea", "Viaje Línea"])
        c_id_viaje = pick_col(df, ["IdViaje", "Id Viaje"])
        c_idics = pick_col(df, ["IdICS"])
        c_fecha_inicio_dp = pick_col(
            df,
            ["F. Inicio DP", "F Inicio DP", "Fecha Inicio DP", "Fecha_Inicio_DP"],
            required=False,
        )
        c_fecha_cierre_dp = pick_col(
            df,
            ["F. Cierre DP", "F Cierre DP", "Fecha Cierre DP", "Fecha_Cierre_DP"],
            required=False,
        )

        print("DEBUG c_fecha_inicio_dp:", c_fecha_inicio_dp)
        print("DEBUG c_fecha_cierre_dp:", c_fecha_cierre_dp)

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst="auto")
        out["Servicio_key"] = out[c_serv].astype(str).str.strip().str.upper()
        out["Linea_key"] = self.tu.to_int64(out[c_linea])
        out["Tabla_key"] = self.tu.to_int64(out[c_coche])
        out["ViajeLinea_key"] = self.tu.to_int64(out[c_viaje_linea])
        out["IdViaje_key"] = self.tu.to_int64(out[c_id_viaje])
        out["Id_ICS"] = self.tu.to_int64(out[c_idics])

        if c_fecha_inicio_dp:
            out["Fecha_Inicio_DP"] = self.tu.parse_datetime_robusto(out[c_fecha_inicio_dp], dayfirst=True)
        else:
            out["Fecha_Inicio_DP"] = pd.NaT

        if c_fecha_cierre_dp:
            out["Fecha_Cierre_DP"] = self.tu.parse_datetime_robusto(out[c_fecha_cierre_dp], dayfirst=True)
        else:
            out["Fecha_Cierre_DP"] = pd.NaT

        print("DEBUG Fecha_Inicio_DP no nulos:", out["Fecha_Inicio_DP"].notna().sum())
        print("DEBUG Fecha_Cierre_DP no nulos:", out["Fecha_Cierre_DP"].notna().sum())

        out = out[
            [
                "Fecha_key",
                "Servicio_key",
                "Linea_key",
                "Tabla_key",
                "ViajeLinea_key",
                "IdViaje_key",
                "Id_ICS",
                "Fecha_Inicio_DP",
                "Fecha_Cierre_DP",
            ]
        ].drop_duplicates(
            subset=["Fecha_key", "Servicio_key", "Linea_key", "Tabla_key", "ViajeLinea_key", "IdViaje_key"],
            keep="first"
        )

        return out

    def load_detallado(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("2) CARGANDO DETALLADO")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        local_files = self._existing_local_files(LOCAL_DETALLADO_FILES)
        if local_files:
            for path in local_files:
                df0 = self._load_local_csv(str(path))
                df0["__archivo_origen__"] = path.name
                frames.append(df0)
                print(f"  ? Local cargado: {path.name} | filas={len(df0)} cols={len(df0.columns)}")
        else:
            for ruta, nombre in self._blob_paths_detallado(fecha):
                if self.az_detallado is None:
                    raise SystemExit("? No hay lector Azure disponible y tampoco Detallado local configurado.")
                if not self.az_detallado.exists(ruta):
                    print(f"  ?? No existe: {nombre}")
                    continue

                df0 = self.io.leer_csv_desde_bytes(self.az_detallado.read_bytes(ruta), dtype=str)
                df0["__archivo_origen__"] = nombre
                frames.append(df0)
                print(f"  ? Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            raise SystemExit("❌ No se encontró ningún Detallado para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["Fecha"])
        c_concesion = pick_col(df, ["Concesión", "Concesion", "ConcesiÃ³n"])
        c_planificado = pick_col(df, ["Planificado"], required=False)
        c_servicio = pick_col(df, ["Servicio"])
        c_linea = pick_col(df, ["Id Línea", "Id Linea", "Id LÃ­nea"])
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea", "Viaje LÃ­nea"])
        c_id_viaje = pick_col(df, ["Id Viaje", "Id Viaje "])
        c_sentido = pick_col(df, ["Sentido", "Sentido "], required=False)
        c_veh_real = pick_col(df, ["Vehículo Real", "Vehiculo Real", "VehÃ­culo Real"], required=False)
        c_hora_teo = pick_col(df, ["Hora Ini Teorica", "Hora Ini Teorica "], required=False)
        c_hora_ref = pick_col(df, ["Hora Ini Referencia", "Hora Ini Referencia "], required=False)
        c_conductor = pick_col(df, ["Conductor"], required=False)
        c_kmprog = pick_col(df, ["KmProgAd"])
        c_kmelim = pick_col(df, ["KmElimado", "Km Eliminado"])
        c_distsup = pick_col(df, ["DistSupAcc"])
        c_distaut = pick_col(df, ["DistAutorizada"])
        c_distno = pick_col(df, ["DistNoRealizada"])
        c_kmejec = pick_col(df, ["KmEjecutado"])
        c_offset_ini = pick_col(df, ["OffsetInicio"])
        c_offset_fin = pick_col(df, ["OffsetFin"])
        c_eliminado = pick_col(df, ["Eliminado"], required=False)
        c_motivo = pick_col(df, ["Descripción Motivo Elim", "Descripcion Motivo Elim", "DescripciÃ³n Motivo Elim"], required=False)

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Linea_key"] = self.tu.to_int64(out[c_linea])
        out["Tabla_key"] = self.tu.to_int64(out[c_tabla])
        out["ViajeLinea_key"] = self.tu.to_int64(out[c_viaje_linea])
        out["IdViaje_key"] = self.tu.to_int64(out[c_id_viaje])

        out["Vehiculo_key"] = (
            out[c_veh_real].astype("string").fillna("").str.strip().str.upper()
            if c_veh_real else pd.Series([""] * len(out), index=out.index, dtype="string")
        )

        out["Fecha"] = pd.to_datetime(out[c_fecha], errors="coerce", dayfirst=True).dt.date
        out["Concesion_raw"] = out[c_concesion].astype("string").fillna("").str.strip()
        out["Planificado_raw"] = out[c_planificado].astype("string").fillna("").str.strip() if c_planificado else ""
        out["Servicio"] = out[c_servicio].astype("string").fillna("").str.strip()
        out["Linea"] = self.tu.to_int64(out[c_linea])
        out["Tabla"] = self.tu.to_int64(out[c_tabla])
        out["Viaje_Linea"] = self.tu.to_int64(out[c_viaje_linea])
        out["Id_Viaje"] = self.tu.to_int64(out[c_id_viaje])
        out["Sentido"] = out[c_sentido].astype("string").fillna("").str.strip() if c_sentido else pd.NA
        out["Vehiculo_Real"] = out[c_veh_real].astype("string").fillna("").str.strip() if c_veh_real else pd.NA
        out["Hora_Teo_raw"] = out[c_hora_teo].astype("string").fillna("").str.strip() if c_hora_teo else ""
        out["Hora_Ref_raw"] = out[c_hora_ref].astype("string").fillna("").str.strip() if c_hora_ref else ""
        out["Conductor"] = out[c_conductor] if c_conductor else pd.NA

        out["KmProgAd_km"] = self.tu.metros_a_km(out[c_kmprog])
        out["KmElimado_km"] = self.tu.metros_a_km(out[c_kmelim])
        out["DistSupAcc_km"] = self.tu.metros_a_km(out[c_distsup])
        out["DistAutorizada_km"] = self.tu.metros_a_km(out[c_distaut])
        out["DistNoRealizada_km"] = self.tu.metros_a_km(out[c_distno])
        out["KmEjecutado_km"] = self.tu.metros_a_km(out[c_kmejec])
        out["OffsetInicio_km"] = self.tu.metros_a_km(out[c_offset_ini])
        out["OffsetFin_km"] = self.tu.metros_a_km(out[c_offset_fin])

        out["Eliminado_raw"] = out[c_eliminado].astype("string").fillna("").str.strip() if c_eliminado else ""
        out["Motivo_original"] = out[c_motivo].astype("string").fillna("").str.strip() if c_motivo else ""

        out["Categoria_Ejecucion"] = np.where(
            out["KmProgAd_km"].notna() & out["KmEjecutado_km"].notna() &
            np.isclose(out["KmProgAd_km"], out["KmEjecutado_km"], rtol=0, atol=1e-9),
            "Ejecutado",
            "No Ejecutado"
        )

        out = out[out["Categoria_Ejecucion"] == "No Ejecutado"].copy()

        conc_upper = out["Concesion_raw"].astype(str).str.upper().str.strip()
        out = out[~conc_upper.isin(["SAN CRISTOBAL T3", "USAQUEN T3"])].copy()

        print(f"✅ Detallado listo: {len(out)} filas después de filtrar No Ejecutado y quitar T3")
        return out

    def load_stat(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("3) CARGANDO VIAJESTAT")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        local_files = self._existing_local_files(LOCAL_STAT_FILES)
        if local_files:
            for path in local_files:
                df0 = self._load_local_csv(str(path))
                df0["__archivo_origen__"] = path.name
                frames.append(df0)
                print(f"  ? Local cargado: {path.name} | filas={len(df0)} cols={len(df0.columns)}")
        elif self.az_fms is not None:
            for ruta, nombre in self._blob_paths_stat(fecha):
                if not self.az_fms.exists(ruta):
                    print(f"  ?? No existe: {nombre}")
                    continue

                df0 = self.io.leer_csv_desde_bytes(self.az_fms.read_bytes(ruta), dtype=str)
                df0["__archivo_origen__"] = nombre
                frames.append(df0)
                print(f"  ? Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            print("?? No se encontr? ViajeStat. Se continuar? sin Stat.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["APPLY_DATE"])
        c_serv = pick_col(df, ["VEH_SERV_ID"])
        c_idviaje = pick_col(df, ["SERV_TRIP_SEQ"])
        c_loc = pick_col(df, ["LOC_TYPE_CD"])

        out = df.copy()
        out["Fecha_key"] = pd.to_datetime(
            out[c_fecha].astype(str).str.strip(),
            format="%Y%m%d",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        out["Servicio_key"] = out[c_serv].astype(str).str.strip().str.upper()
        out["IdViaje_key"] = self.tu.to_int64(out[c_idviaje])
        out["LOC_TYPE_CD_num"] = self.tu.to_int64(out[c_loc])

        out = out.dropna(subset=["Fecha_key", "Servicio_key", "IdViaje_key", "LOC_TYPE_CD_num"]).copy()
        out["LOC_TYPE_CD_num"] = out["LOC_TYPE_CD_num"].astype(int)

        grp = pd.crosstab(
            index=[
                out["Fecha_key"],
                out["Servicio_key"],
                out["IdViaje_key"]
            ],
            columns=out["LOC_TYPE_CD_num"]
        ).reset_index()

        for estado in [5, 8, 9]:
            if estado not in grp.columns:
                grp[estado] = 0

        grp = grp.rename(columns={
            5: "CNT Estado 5",
            8: "CNT Estado 8",
            9: "CNT Estado 9",
        })

        print(f"✅ ViajeStat listo: {len(grp)} ll
