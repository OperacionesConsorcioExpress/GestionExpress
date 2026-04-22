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
            s = s.replace("\ufeff", "").replace("Ã¯Â»Â¿", "").replace('"', "").strip()
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
        s = s.astype(str).str.replace("\ufeff", "", regex=False).str.replace("Ã¯Â»Â¿", "", regex=False).str.strip()
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
        s = series.astype("string").fillna("").str.replace("\ufeff", "", regex=False).str.replace("Ã¯Â»Â¿", "", regex=False).str.strip()
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
    s = s.replace("\ufeff", "").replace("Ã¯Â»Â¿", "")
    s = s.replace("Ã¡", "a").replace("Ã©", "e").replace("Ã­", "i").replace("Ã³", "o").replace("Ãº", "u").replace("Ã±", "n")
    s = s.replace("Ã£Â¡", "a").replace("Ã£Â©", "e").replace("Ã£Â­", "i").replace("Ã£Â³", "o").replace("Ã£Âº", "u")
    s = s.replace("lÃ£Â­nea", "linea").replace("vehÃ£Â­culo", "vehiculo").replace("descripciÃ£Â³n", "descripcion")
    s = s.replace("concesiÃ£Â³n", "concesion")
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
        raise KeyError(f"No se encontrÃ³ ninguna de estas columnas: {aliases}")
    return None


def clean_motivo(v) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\ufeff", "").replace("Ã¯Â»Â¿", "")
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
        c_serv = pick_col(df, ["Servicio"])
        c_linea = pick_col(df, ["Id Línea", "Id Linea"])
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea"])
        c_id_viaje = pick_col(df, ["Id Viaje", "Id Viaje ", "IdViaje"])
        c_concesion = pick_col(df, ["Concesión", "Concesion"])
        c_planif = pick_col(df, ["Planificado"])
        c_elim = pick_col(df, ["Eliminado"])
        c_mot_elim = pick_col(df, ["Descripción Motivo Elim", "Descripcion Motivo Elim"])
        c_km_prog = pick_col(df, ["KmProgAd"])
        c_km_elim = pick_col(df, ["KmElimado"])
        c_distsup = pick_col(df, ["DistSupAcc"])
        c_distaut = pick_col(df, ["DistAutorizada"])
        c_distnr = pick_col(df, ["DistNoRealizada"])
        c_km_ejec = pick_col(df, ["KmEjecutado"])
        c_off_ini = pick_col(df, ["OffsetInicio"])
        c_off_fin = pick_col(df, ["OffsetFin"])
        c_hora_ini_real = pick_col(df, ["Hora Ini Real Cabecera"], required=False)
        c_hora_ini_ref = pick_col(df, ["Hora Ini Referencia"], required=False)
        c_hora_fin_real = pick_col(df, ["Hora Fin Real", "Hora Fin Real "], required=False)

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_serv].astype(str).str.strip().str.upper()
        out["Linea_key"] = self.tu.to_int64(out[c_linea])
        out["Tabla_key"] = self.tu.to_int64(out[c_tabla])
        out["ViajeLinea_key"] = self.tu.to_int64(out[c_viaje_linea])
        out["IdViaje_key"] = self.tu.to_int64(out[c_id_viaje])

        out["Concesion_txt"] = out[c_concesion].astype(str).str.strip().str.upper()
        out = out[~out["Concesion_txt"].isin(["SAN CRISTOBAL T3", "USAQUEN T3"])].copy()

        km_prog = self.tu.metros_a_km(out[c_km_prog])
        km_ejec = self.tu.metros_a_km(out[c_km_ejec])
        no_ej_mask = km_prog.ne(km_ejec) & km_prog.notna() & km_ejec.notna()
        out = out[no_ej_mask].copy()
        if out.empty:
            raise SystemExit("❌ Detallado sin filas No Ejecutado tras filtrar.")

        out["Planificado_txt"] = out[c_planif].astype(str).str.strip().str.lower()
        out["Eliminado_raw"] = out[c_elim]
        out["Motivo_original"] = out[c_mot_elim].map(clean_motivo)

        out["KmProgAd"] = self.tu.metros_a_km(out[c_km_prog])
        out["KmElimado"] = self.tu.metros_a_km(out[c_km_elim])
        out["DistSupAcc"] = self.tu.metros_a_km(out[c_distsup])
        out["DistAutorizada"] = self.tu.metros_a_km(out[c_distaut])
        out["DistNoRealizada"] = self.tu.metros_a_km(out[c_distnr])
        out["KmEjecutado"] = self.tu.metros_a_km(out[c_km_ejec])
        out["OffsetInicio"] = self.tu.metros_a_km(out[c_off_ini])
        out["OffsetFin"] = self.tu.metros_a_km(out[c_off_fin])

        h_ini_real = pd.to_timedelta(out[c_hora_ini_real].astype(str), errors="coerce") if c_hora_ini_real else pd.NaT
        h_ini_ref = pd.to_timedelta(out[c_hora_ini_ref].astype(str), errors="coerce") if c_hora_ini_ref else pd.NaT
        h_fin_real = pd.to_timedelta(out[c_hora_fin_real].astype(str), errors="coerce") if c_hora_fin_real else pd.NaT

        out["HoraIni_td"] = h_ini_real.fillna(h_ini_ref)
        out["HoraFin_td"] = h_fin_real

        cols_keep = [
            "Fecha_key",
            "Servicio_key",
            "Linea_key",
            "Tabla_key",
            "ViajeLinea_key",
            "IdViaje_key",
            "Concesion_txt",
            "Planificado_txt",
            "Eliminado_raw",
            "Motivo_original",
            "KmProgAd",
            "KmElimado",
            "DistSupAcc",
            "DistAutorizada",
            "DistNoRealizada",
            "KmEjecutado",
            "OffsetInicio",
            "OffsetFin",
            "HoraIni_td",
            "HoraFin_td",
        ]
        out = out[cols_keep].copy()

        print(f"✅ Detallado listo: {len(out)} filas después de filtrar No Ejecutado y quitar T3")
        return out

    def load_viajestat(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("3) CARGANDO VIAJESTAT")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        local_files = self._existing_local_files(LOCAL_STAT_FILES)
        if local_files:
            for path in local_files:
                df0 = self._load_local_csv(str(path))
                frames.append(df0)
                print(f"  ? Local cargado: {path.name} | filas={len(df0)} cols={len(df0.columns)}")
        else:
            for ruta, nombre in self._blob_paths_stat(fecha):
                if self.az_fms is None:
                    raise SystemExit("? No hay lector Azure disponible y tampoco ViajeStat local configurado.")
                if not self.az_fms.exists(ruta):
                    print(f"  ?? No existe: {nombre}")
                    continue
                df0 = self.io.leer_csv_desde_bytes(self.az_fms.read_bytes(ruta), dtype=str)
                frames.append(df0)
                print(f"  ? Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            print("? ViajeStat ausente: se continuará sin este insumo.")
            return pd.DataFrame(columns=["Servicio_key", "IdViaje_key", "KM8", "KM9"])

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_serv = pick_col(df, ["VEH_SERV_ID", "Servicio", "SERVICIO"])
        c_trip = pick_col(df, ["SERV_TRIP_SEQ", "Id Viaje", "IdViaje"])
        c_km8 = pick_col(df, ["KM Estado 8", "KM_ESTADO_8", "KM Estado8"], required=False)
        c_km9 = pick_col(df, ["KM Estado 9", "KM_ESTADO_9", "KM Estado9"], required=False)

        out = df.copy()
        out["Servicio_key"] = out[c_serv].astype(str).str.strip().str.upper()
        out["IdViaje_key"] = self.tu.to_int64(out[c_trip])
        out["KM8"] = self.tu.metros_a_km(out[c_km8]) if c_km8 else 0.0
        out["KM9"] = self.tu.metros_a_km(out[c_km9]) if c_km9 else 0.0

        grp = out.groupby(["Servicio_key", "IdViaje_key"], dropna=False, as_index=False)[["KM8", "KM9"]].sum()
        print(f"ViajeStat listo: {len(grp)} llaves agregadas")
        return grp

    def load_tardios(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("4) CARGANDO VIAJES TARDIOS")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        local_files = self._existing_local_files(LOCAL_TARDIOS_FILES)
        if local_files:
            for path in local_files:
                df0 = self._load_local_csv(str(path))
                df0["__archivo_origen__"] = path.name
                frames.append(df0)
                print(f"  ? Local cargado: {path.name} | filas={len(df0)} cols={len(df0.columns)}")
        else:
            if self.az_fms is None:
                raise SystemExit("? No hay lector Azure disponible y tampoco Tardios local configurado.")

            for prefix in self._prefixes_viajes_tardios(fecha):
                for blob_path in self.az_fms.list_blob_paths(prefix):
                    nombre = Path(blob_path).name
                    if not nombre.lower().endswith(".csv"):
                        continue
                    try:
                        df0 = self.io.leer_csv_desde_bytes(self.az_fms.read_bytes(blob_path), dtype=str)
                        df0["__archivo_origen__"] = nombre
                        frames.append(df0)
                        print(f"  ? Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")
                    except Exception as ex:
                        print(f"  ?? No se pudo leer {nombre}: {repr(ex)}")

        if not frames:
            print("? Tardios ausente: se continuará sin este insumo.")
            return pd.DataFrame(columns=["Fecha_key", "Servicio_key", "ViajeLinea_key", "Tardio_Flag"])

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["Fecha"], required=False)
        c_serv = pick_col(df, ["servicio2", "Servicio", "SERVICIO"], required=False)
        c_viaje_linea = pick_col(df, ["viaje_linea", "ViajeLinea", "Viaje Línea"], required=False)
        c_estado = pick_col(df, ["Estado", "ESTADO", "Estados", "estado_vehiculo"], required=False)

        if c_fecha is None or c_serv is None or c_viaje_linea is None:
            print("? Tardios sin columnas mínimas: se continuará sin este insumo.")
            return pd.DataFrame(columns=["Fecha_key", "Servicio_key", "ViajeLinea_key", "Tardio_Flag"])

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_serv].astype(str).str.strip().str.upper()
        out["ViajeLinea_key"] = self.tu.to_int64(out[c_viaje_linea])

        if c_estado:
            out["Estado_txt"] = out[c_estado].astype(str)
            mask_estado = out["Estado_txt"].str.contains(r"(^|[^0-9])(5|8|9)([^0-9]|$)", regex=True, na=False)
            out = out[mask_estado].copy()
        else:
            out = out.iloc[0:0].copy()

        out["Tardio_Flag"] = 1
        out = out[["Fecha_key", "Servicio_key", "ViajeLinea_key", "Tardio_Flag"]].drop_duplicates()
        print(f"Tardios listo: {len(out)} llaves con match de estado 5/8/9")
        return out

    def build(self, fecha: datetime) -> pd.DataFrame:
        df_det = self.load_detallado(fecha)
        df_ics = self.load_ics(fecha)
        df_stat = self.load_viajestat(fecha)
        df_tard = self.load_tardios(fecha)

        print("\n" + "=" * 80)
        print("5) CRUCE DETALLADO ↔ ICS")
        print("=" * 80)
        keys = ["Fecha_key", "Servicio_key", "Linea_key", "Tabla_key", "ViajeLinea_key", "IdViaje_key"]
        df = df_det.merge(df_ics, on=keys, how="left")

        print("\n" + "=" * 80)
        print("6) CRUCE CON VIAJESTAT")
        print("=" * 80)
        df = df.merge(df_stat, on=["Servicio_key", "IdViaje_key"], how="left")
        df["KM8"] = df["KM8"].fillna(0.0)
        df["KM9"] = df["KM9"].fillna(0.0)

        print("\n" + "=" * 80)
        print("7) CRUCE CON TARDÍOS")
        print("=" * 80)
        df = df.merge(df_tard, on=["Fecha_key", "Servicio_key", "ViajeLinea_key"], how="left")
        df["Tardio_Flag"] = df["Tardio_Flag"].fillna(0).astype(int)

        kmr_raw = np.where(
            df["Planificado_txt"].eq("sustituido"),
            0.0,
            (df["KmProgAd"].fillna(0) + df["DistAutorizada"].fillna(0) - (df["KmElimado"].fillna(0) + df["DistSupAcc"].fillna(0))) - df["KmEjecutado"].fillna(0)
        )
        df["Km_Revision"] = np.round(np.where(kmr_raw < 0, 0.0, kmr_raw), 3)
        df["Km_ElimEIC"] = df["KmElimado"].fillna(0) + df["DistSupAcc"].fillna(0) + df["DistNoRealizada"].fillna(0)

        kma = (df["DistSupAcc"].fillna(0) - df["DistAutorizada"].fillna(0))
        df["Km_ElimAcc"] = np.where(kma < 0, 0.0, kma)

        off_fin = (df["KmProgAd"].fillna(0) - df["OffsetFin"].fillna(0))
        df["Offset_Fin_Calc"] = np.where(off_fin < 0, 0.0, off_fin)

        f2 = df["OffsetInicio"].fillna(0) + df["Offset_Fin_Calc"].fillna(0) - df["KmElimado"].fillna(0)
        df["Km_F2_IniFin"] = np.where(f2 < 0, 0.0, f2)

        motivo = df["Motivo_original"].fillna("").astype(str).str.strip()
        blank = motivo.eq("")
        plan_txt = df["Planificado_txt"].fillna("").astype(str).str.strip().str.lower()
        elim_txt = df["Eliminado_raw"].astype(str).str.strip().str.lower()

        tol_eq = 0.001
        tol_off = 0.06

        kmr0 = df["Km_Revision"].fillna(0).abs() < tol_eq
        kmrne0 = ~kmr0
        eq_prog = (df["Km_Revision"].sub(df["KmProgAd"], fill_value=np.nan).abs() < tol_eq)
        accion_reg = (df["DistAutorizada"].fillna(0).abs() < tol_eq) & ((df["KM8"].fillna(0) + df["KM9"].fillna(0)) > 0)
        is_f2 = df["Km_F2_IniFin"].fillna(0) >= (df["Km_Revision"].fillna(0) - tol_off)
        is_offs = (df["Km_Revision"].fillna(0).abs() > 0) & (df["Km_F2_IniFin"].fillna(0).abs() > 0) & ((df["Km_Revision"].fillna(0) - df["Km_F2_IniFin"].fillna(0)) > tol_off)

        motivo.loc[kmr0 & plan_txt.eq("sustituido") & blank] = "Sustituido"
        motivo.loc[kmr0 & elim_txt.eq("eliminado") & blank] = df.loc[kmr0 & elim_txt.eq("eliminado") & blank, "Motivo_original"]
        motivo.loc[kmr0 & elim_txt.eq("parcial") & blank] = df.loc[kmr0 & elim_txt.eq("parcial") & blank, "Motivo_original"]
        motivo.loc[kmr0 & df["Km_ElimAcc"].fillna(0).abs().ge(tol_eq) & blank] = "Acción de regulación"

        motivo.loc[kmrne0 & accion_reg & blank] = "Regulación Offline"
        mask_desloc_elim = (
            kmrne0
            & elim_txt.eq("parcial")
            & df["Km_ElimEIC"].notna()
            & df["KmElimado"].notna()
            & (df["Km_ElimEIC"] > df["KmElimado"])
        )
        motivo.loc[mask_desloc_elim] = "Deslocalización con eliminación"
        motivo.loc[kmrne0 & eq_prog & df["Tardio_Flag"].eq(1) & blank] = "Viaje Tardío"
        motivo.loc[kmrne0 & eq_prog & df["Tardio_Flag"].ne(1) & blank] = "Viaje no ejecutado no eliminado"
        motivo.loc[kmrne0 & ~eq_prog & is_f2 & blank] = "F2 Inicio-Fin"
        motivo.loc[kmrne0 & ~eq_prog & ~is_f2 & is_offs & blank] = "OffSet Deslocalización"
        motivo.loc[kmrne0 & ~eq_prog & ~is_f2 & ~is_offs & blank] = "Deslocalización"
        motivo.loc[motivo.eq("")] = "Deslocalización"

        concesion_norm = df["Concesion_txt"].fillna("")
        concesion = np.where(
            concesion_norm.str.endswith(" ZN") | concesion_norm.str.endswith("ZN"),
            "Urbano",
            np.where(
                concesion_norm.str.endswith(" AL") | concesion_norm.str.endswith("AL"),
                "Alimentación",
                df["Concesion_txt"],
            ),
        )
        df["Concesión"] = concesion
        df["Motivo"] = motivo.map(clean_motivo)

        mask_al = df["Concesión"].astype(str).str.upper().eq("ALIMENTACIÓN") & df["Motivo"].eq("Acción de regulación")
        if mask_al.any():
            df.loc[mask_al, "Motivo"] = "Revision Alimentacion Acciones de Regulacion"
            df.loc[mask_al, "Km_Revision"] = (df.loc[mask_al, "DistSupAcc"].fillna(0) - df.loc[mask_al, "DistAutorizada"].fillna(0)).clip(lower=0)
        print(f"DEBUG ajuste AL aplicado en {int(mask_al.sum())} filas")

        df_final = pd.DataFrame(
            {
                "Id_ICS": df["Id_ICS"],
                "Fecha": df["Fecha_key"],
                "Ruta": pd.NA,
                "Tabla": df["Tabla_key"],
                "Servicio": df["Servicio_key"],
                "Coche": df["Tabla_key"],
                "Hora_Ini": pd.NA,
                "Hora_Fin": pd.NA,
                "Motivo": df["Motivo"],
                "Km_Revision": df["Km_Revision"].round(3),
                "Concesión": df["Concesión"],
                "Fecha_Inicio_DP": df["Fecha_Inicio_DP"],
                "Fecha_Cierre_DP": df["Fecha_Cierre_DP"],
            }
        )
        df_final = df_final[df_final["Id_ICS"].notna()].copy()
        df_final = df_final[df_final["Km_Revision"].fillna(0) > 0].copy()
        df_final["Id_ICS"] = pd.to_numeric(df_final["Id_ICS"], errors="coerce").astype("Int64")

        print("✅ Tabla final construida")
        print(f"   Filas: {len(df_final)}")
        print("📌 Top Motivos:")
        print(df_final["Motivo"].value_counts().head(10))
        print("DEBUG df_final Fecha_Inicio_DP no nulos:", df_final["Fecha_Inicio_DP"].notna().sum())
        print("DEBUG df_final Fecha_Cierre_DP no nulos:", df_final["Fecha_Cierre_DP"].notna().sum())
        return df_final


# =============================================================================
# CARGUE sne.ics
# =============================================================================

class PostgresLoader:
    def __init__(self, schema: str, table: str, batch_size: int = 5000):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size

    @staticmethod
    def _py(v):
        try:
            if v is pd.NA:
                return None
        except Exception:
            pass

        if pd.isna(v):
            return None

        if isinstance(v, np.generic):
            return v.item()

        if isinstance(v, (pd.Timestamp, datetime)):
            return v.to_pydatetime() if hasattr(v, "to_pydatetime") else v

        return v

    def delete_existing_for_date(self, conn, fecha_key: str) -> int:
        full_table = f'"{self.schema}"."{self.table}"'
        sql = f'DELETE FROM {full_table} WHERE fecha = %s'
        with conn.cursor() as cur:
            cur.execute(sql, (fecha_key,))
            return cur.rowcount

    def insert_df(self, df: pd.DataFrame, conn) -> int:
        if df is None or df.empty:
            print("? sne.ics: DF vacío, no se inserta nada.")
            return 0

        d = df.copy()

        d["Id_ICS"] = pd.to_numeric(d["Id_ICS"], errors="coerce")
        d = d.dropna(subset=["Id_ICS"]).copy()
        d["Id_ICS"] = d["Id_ICS"].astype(int)

        d["Fecha"] = d["Fecha"].astype(str)
        d["Ruta"] = d["Ruta"].astype("string")
        d["Servicio"] = d["Servicio"].astype("string")
        d["Motivo"] = d["Motivo"].astype("string")
        d["Concesión"] = d["Concesión"].astype("string")
        d["Tabla"] = pd.to_numeric(d["Tabla"], errors="coerce").astype("Int64")
        d["Coche"] = pd.to_numeric(d["Coche"], errors="coerce").astype("Int64")
        d["Km_Revision"] = pd.to_numeric(d["Km_Revision"], errors="coerce")

        cols = [
            "Id_ICS",
            "Fecha",
            "Ruta",
            "Tabla",
            "Servicio",
            "Coche",
            "Hora_Ini",
            "Hora_Fin",
            "Motivo",
            "Km_Revision",
            "Concesión",
            "Fecha_Inicio_DP",
            "Fecha_Cierre_DP",
        ]
        missing = [c for c in cols if c not in d.columns]
        if missing:
            raise SystemExit(f"? Faltan columnas obligatorias para sne.ics: {missing}")

        d = d[cols].copy()

        d = d.drop_duplicates(subset=["Id_ICS"], keep="last").sort_values(by=["Fecha", "Id_ICS"]).reset_index(drop=True)

        if DELETE_EXISTING_FOR_DATE and len(d):
            deleted = self.delete_existing_for_date(conn, str(d.iloc[0]["Fecha"]))
            print(f"? DELETE previo sne.ics (fecha={d.iloc[0]['Fecha']}): {deleted} filas")

        full_table = f'"{self.schema}"."{self.table}"'
        sql = f"""
            INSERT INTO {full_table}
            (
                "id_ics", "fecha", "ruta", "tabla", "servicio", "coche",
                "hora_ini", "hora_fin", "motivo", "km_revision", "concesion",
                "fecha_inicio_dp", "fecha_cierre_dp"
            )
            VALUES %s
            ON CONFLICT ("id_ics")
            DO UPDATE SET
                "fecha" = EXCLUDED."fecha",
                "ruta" = EXCLUDED."ruta",
                "tabla" = EXCLUDED."tabla",
                "servicio" = EXCLUDED."servicio",
                "coche" = EXCLUDED."coche",
                "hora_ini" = EXCLUDED."hora_ini",
                "hora_fin" = EXCLUDED."hora_fin",
                "motivo" = EXCLUDED."motivo",
                "km_revision" = EXCLUDED."km_revision",
                "concesion" = EXCLUDED."concesion",
                "fecha_inicio_dp" = EXCLUDED."fecha_inicio_dp",
                "fecha_cierre_dp" = EXCLUDED."fecha_cierre_dp"
        """

        total = 0
        for start in range(0, len(d), self.batch_size):
            chunk = d.iloc[start:start + self.batch_size].copy()
            with conn.cursor() as cur:
                records = [
                    (
                        int(r.Id_ICS),
                        str(r.Fecha),
                        self._py(r.Ruta),
                        self._py(r.Tabla),
                        self._py(r.Servicio),
                        self._py(r.Coche),
                        self._py(r.Hora_Ini),
                        self._py(r.Hora_Fin),
                        self._py(r.Motivo),
                        float(r.Km_Revision) if pd.notna(r.Km_Revision) else None,
                        self._py(r.Concesión),
                        self._py(r.Fecha_Inicio_DP),
                        self._py(r.Fecha_Cierre_DP),
                    )
                    for r in chunk.itertuples(index=False)
                ]
                execute_values(cur, sql, records, page_size=len(records))
                total += len(records)

        return total


# =============================================================================
# CARGUE sne.gestion_sne
# =============================================================================

class GestionSNELoader:
    def __init__(self, schema: str, table: str, batch_size: int = 5000, revisor_default_int: int = 0):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.revisor_default_int = int(revisor_default_int)

    @staticmethod
    def _py(v):
        try:
            if v is pd.NA:
                return None
        except Exception:
            pass

        if pd.isna(v):
            return None

        if isinstance(v, np.generic):
            return v.item()

        if isinstance(v, (pd.Timestamp, datetime)):
            return v.to_pydatetime() if hasattr(v, "to_pydatetime") else v

        return v

    def upsert_from_ics_ids(self, df_sne: pd.DataFrame, conn) -> int:
        if df_sne is None or df_sne.empty or "Id_ICS" not in df_sne.columns:
            print("? gestion_sne: DF vacío o sin Id_ICS.")
            return 0

        d = df_sne[["Id_ICS", "Fecha_Inicio_DP", "Fecha_Cierre_DP"]].copy()
        d["Id_ICS"] = pd.to_numeric(d["Id_ICS"], errors="coerce")
        d = d.dropna(subset=["Id_ICS"]).copy()
        d["Id_ICS"] = d["Id_ICS"].astype(int)
        d = d.drop_duplicates(subset=["Id_ICS"], keep="last").sort_values(by=["Id_ICS"]).reset_index(drop=True)

        print("DEBUG gestion_sne fechas no nulas inicio:", d["Fecha_Inicio_DP"].notna().sum())
        print("DEBUG gestion_sne fechas no nulas cierre:", d["Fecha_Cierre_DP"].notna().sum())

        full_table = f'"{self.schema}"."{self.table}"'
        sql = f"""
            INSERT INTO {full_table}
            (
                "id_ics", "responsable_revision", "fecha_inicio_dp", "fecha_cierre_dp"
            )
            VALUES %s
            ON CONFLICT ("id_ics")
            DO UPDATE SET
                "fecha_inicio_dp" = EXCLUDED."fecha_inicio_dp",
                "fecha_cierre_dp" = EXCLUDED."fecha_cierre_dp"
        """

        total = 0
        for start in range(0, len(d), self.batch_size):
            chunk = d.iloc[start:start + self.batch_size].copy()
            with conn.cursor() as cur:
                records = [
                    (
                        int(r.Id_ICS),
                        self.revisor_default_int,
                        self._py(r.Fecha_Inicio_DP),
                        self._py(r.Fecha_Cierre_DP),
                    )
                    for r in chunk.itertuples(index=False)
                ]
                execute_values(cur, sql, records, page_size=len(records))
                total += len(records)

        return total


# =============================================================================
# CARGUE sne.ics_motivo_resp + AUTO-CREAR motivos
# =============================================================================

class IcsMotivoRespLoader:
    def __init__(
        self,
        schema_motivo_resp: str,
        table_motivo_resp: str,
        schema_motivos: str,
        table_motivos: str,
        batch_size: int = 5000,
        responsable_default: int = 0,
    ):
        self.schema_motivo_resp = schema_motivo_resp
        self.table_motivo_resp = table_motivo_resp
        self.schema_motivos = schema_motivos
        self.table_motivos = table_motivos
        self.batch_size = batch_size
        self.responsable_default = int(responsable_default)

    def _get_catalog(self, conn) -> pd.DataFrame:
        sql = f'SELECT id, motivo, responsable FROM "{self.schema_motivos}"."{self.table_motivos}" ORDER BY id'
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["id", "motivo", "responsable"]) if rows else pd.DataFrame(columns=["id", "motivo", "responsable"])
        if "motivo" not in df.columns:
            df["motivo"] = ""
        df["motivo_clean"] = df["motivo"].map(clean_motivo)
        df["motivo_norm"] = df["motivo_clean"].map(norm_motivo)
        return df

    def _get_next_id(self, conn) -> int:
        sql = f'SELECT COALESCE(MAX(id), 0) + 1 FROM "{self.schema_motivos}"."{self.table_motivos}"'
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 1

    def _fix_sequence(self, conn) -> None:
        full_table = f'"{self.schema_motivos}"."{self.table_motivos}"'
        sql = f"""
            SELECT setval(
                pg_get_serial_sequence('{full_table}', 'id'),
                COALESCE((SELECT MAX(id) FROM {full_table}), 0),
                true
            )
        """
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
            except Exception:
                pass

    def _insert_missing_catalog(self, conn, missing_norm_to_raw: Dict[str, str]) -> int:
        if not missing_norm_to_raw:
            return 0

        next_id = self._get_next_id(conn)
        full_table = f'"{self.schema_motivos}"."{self.table_motivos}"'
        sql_ins = f'INSERT INTO {full_table} ("id","motivo","responsable") VALUES %s'

        items = list(missing_norm_to_raw.items())
        records = []
        current_id = next_id

        for _norm, raw in items:
            records.append((current_id, clean_motivo(raw), int(self.responsable_default)))
            current_id += 1

        total = 0
        with conn.cursor() as cur:
            for start in range(0, len(records), self.batch_size):
                chunk = records[start:start + self.batch_size]
                execute_values(cur, sql_ins, chunk, page_size=len(chunk))
                total += len(chunk)

        self._fix_sequence(conn)
        return total

    def upsert_from_df(self, df_sne: pd.DataFrame, conn) -> int:
        if df_sne is None or df_sne.empty:
            print("⚠️ ics_motivo_resp: DF vacío.")
            return 0

        if "Id_ICS" not in df_sne.columns or "Motivo" not in df_sne.columns:
            print("⚠️ ics_motivo_resp: faltan columnas Id_ICS o Motivo.")
            return 0

        df = df_sne[["Id_ICS", "Motivo"]].copy()
        df["Id_ICS"] = pd.to_numeric(df["Id_ICS"], errors="coerce")
        df = df.dropna(subset=["Id_ICS"]).copy()

        df["Motivo"] = df["Motivo"].map(clean_motivo)
        df["motivo_norm"] = df["Motivo"].map(norm_motivo)
        df = df[df["motivo_norm"].astype(str).str.strip() != ""].copy()

        if df.empty:
            print("⚠️ ics_motivo_resp: todos los motivos están vacíos.")
            return 0

        cat = self._get_catalog(conn)
        cat_norm_set = set(cat["motivo_norm"].dropna().astype(str).tolist())

        motivos_df = df[["motivo_norm", "Motivo"]].drop_duplicates().copy()
        faltantes = motivos_df[~motivos_df["motivo_norm"].isin(cat_norm_set)].copy()

        missing_norm_to_raw: Dict[str, str] = {}
        for r in faltantes.itertuples(index=False):
            norm = str(r.motivo_norm)
            raw = clean_motivo(r.Motivo)
            if norm and raw:
                missing_norm_to_raw[norm] = raw

        if missing_norm_to_raw:
            print(f"⚠️ motivos_eliminacion: se crearán {len(missing_norm_to_raw)} motivos nuevos consecutivos.")
            created = self._insert_missing_catalog(conn, missing_norm_to_raw)
            print(f"✅ motivos_eliminacion: insertados {created} motivos nuevos.")

        cat2 = self._get_catalog(conn)

        df_merge = df.merge(
            cat2[["id", "responsable", "motivo_norm"]],
            on="motivo_norm",
            how="left"
        )

        df_ok = df_merge[df_merge["id"].notna()].copy()
        if df_ok.empty:
            print("⚠️ ics_motivo_resp: no hay filas válidas para insertar.")
            return 0

        df_ok["motivo_id"] = df_ok["id"].astype(int)
        df_ok["responsable_id"] = df_ok["responsable"].astype(int)
        df_ok = df_ok.sort_values(by=["Id_ICS"]).drop_duplicates(subset=["Id_ICS"], keep="last")

        full_table = f'"{self.schema_motivo_resp}"."{self.table_motivo_resp}"'
        sql_upsert = f"""
            INSERT INTO {full_table} ("id_ics","motivo","responsable")
            VALUES %s
            ON CONFLICT ("id_ics")
            DO UPDATE SET
                "motivo" = EXCLUDED."motivo",
                "responsable" = EXCLUDED."responsable"
        """

        total = 0
        with conn.cursor() as cur:
            for start in range(0, len(df_ok), self.batch_size):
                chunk = df_ok.iloc[start:start + self.batch_size]
                records = [(int(r.Id_ICS), int(r.motivo_id), int(r.responsable_id)) for r in chunk.itertuples(index=False)]
                execute_values(cur, sql_upsert, records, page_size=len(records))
                total += len(records)

        return total


# =============================================================================
# LOG + FECHA AUTOMATICA
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

    def get_id_reporte(self, conn, nombre_reporte: str, default_id: int = 1) -> int:
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
              AND COALESCE("registros_proce", 0) > 0
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
                "id_reporte",
                "fecha",
                "estado",
                "ultima_ejecucion",
                "duracion_seg",
                "archivos_total",
                "archivos_ok",
                "archivos_error",
                "registros_proce",
                "fecha_actualizacion"
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
    if CONNECTION_STRING_LOCAL:
        return CONNECTION_STRING_LOCAL
    raise SystemExit(f"❌ Falta connection string. Usa env {AZURE_CONN_ENV}.")


def _parse_semilla() -> date:
    try:
        return datetime.strptime(FECHA_SEMILLA_STR, "%d/%m/%Y").date()
    except ValueError:
        raise SystemExit("❌ FECHA_SEMILLA_STR debe estar en formato dd/mm/yyyy.")


def _is_local_mode() -> bool:
    if not USE_LOCAL_FILES:
        return False
    if LOCAL_ICS_FILE and Path(LOCAL_ICS_FILE).exists():
        return True
    return any(Path(p).exists() for p in LOCAL_DETALLADO_FILES)


def _resolve_fecha_to_process(fecha_semilla: date) -> date:
    if str(PROCESS_DATE_STR).strip():
        try:
            return datetime.strptime(str(PROCESS_DATE_STR).strip(), "%d/%m/%Y").date()
        except ValueError:
            raise SystemExit("❌ PROCESS_DATE_STR debe estar en formato dd/mm/yyyy.")
    local_ics = Path(LOCAL_ICS_FILE) if (USE_LOCAL_FILES and LOCAL_ICS_FILE) else None
    if local_ics and local_ics.exists():
        m = re.search(r"(\d{2})_(\d{2})_(\d{4})", local_ics.name)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    try:
        logger = ReportRunLogger()
        with get_db_connection() as conn:
            id_reporte = logger.get_id_reporte(
                conn,
                NOMBRE_REPORTE_LOG,
                default_id=DEFAULT_ID_REPORTE,
            )
            return logger.get_next_fecha_to_process(conn, id_reporte, fecha_semilla)
    except Exception as e:
        print(f"⚠️ No se pudo resolver la fecha desde log; se usa fecha semilla. Detalle: {e!r}")
        return fecha_semilla


def _fechas_a_procesar(fecha_semilla: date) -> List[date]:
    fecha_inicio = _resolve_fecha_to_process(fecha_semilla)
    if str(PROCESS_DATE_STR).strip() or _is_local_mode():
        return [fecha_inicio]

    fecha_limite = datetime.now().date() - timedelta(days=1)
    if fecha_inicio > fecha_limite:
        return []

    fechas: List[date] = []
    cur = fecha_inicio
    while cur <= fecha_limite:
        fechas.append(cur)
        cur += timedelta(days=1)
    return fechas


def _es_error_por_insumos_faltantes(exc: Exception) -> bool:
    txt = str(exc).lower()
    patrones = (
        "no existe ics en azure",
        "no se encontró ningún detallado",
        "no se encontro ningun detallado",
        "no se encontraron archivos de validaciones",
        "no se encontraron archivos de tardíos",
        "no se encontraron archivos de tardios",
        "no se encontró ningún archivo",
        "no se encontro ningun archivo",
    )
    return any(p in txt for p in patrones)


def main() -> None:
    load_dotenv()
    fecha_semilla = _parse_semilla()
    fechas_a_procesar = _fechas_a_procesar(fecha_semilla)

    if not fechas_a_procesar:
        print("No hay fechas pendientes por procesar.")
        return

    meses_es = {
        1: "enero",
        2: "febrero",
        3: "marzo",
        4: "abril",
        5: "mayo",
        6: "junio",
        7: "julio",
        8: "agosto",
        9: "septiembre",
        10: "octubre",
        11: "noviembre",
        12: "diciembre",
    }

    for fecha_to_process in fechas_a_procesar:
        start_perf = time.perf_counter()
        fecha_dt = datetime.combine(fecha_to_process, datetime.min.time())
        fecha_limite = datetime.now().date() - timedelta(days=1)
        estado = "ok"
        archivos_total = 1
        archivos_ok = 1
        archivos_error = 0
        registros_proce = 0
        soft_stop = False

        fecha_texto = f"{fecha_to_process.day} de {meses_es[fecha_to_process.month]} de {fecha_to_process.year}"

        print("\n" + "=" * 80)
        print(f"FECHA A PROCESAR: {fecha_to_process}")
        print(f"FECHA EN TEXTO: {fecha_texto}")
        print("=" * 80)

        try:
            az = None
            az_ics = None
            az_detallado = None
            az_fms = None
            if not _is_local_mode():
                conn_azure = _get_connection_string()
                az = AzureBlobReader(AzureConfig(connection_string=conn_azure))
                az_ics = AzureBlobReader(AzureConfig(connection_string=conn_azure, container_actividad=AZURE_CONTAINER_ICS))
                az_detallado = AzureBlobReader(AzureConfig(connection_string=conn_azure, container_actividad=AZURE_CONTAINER_DETALLADO))
                az_fms = AzureBlobReader(AzureConfig(connection_string=conn_azure, container_actividad=AZURE_CONTAINER_FMS))

            builder = SNEExportBuilder(az, az_ics, az_detallado, az_fms, filtro_zona_tipo=FILTRO_ZONA_TIPO)
            df_final = builder.build(fecha_dt)

            registros_proce = int(len(df_final))
            duracion_seg = int(round(time.perf_counter() - start_perf))

            print("\n" + "=" * 80)
            print("8) RESULTADO LISTO PARA CARGUE")
            print("=" * 80)
            print(f"Filas exportadas: {len(df_final)}")
            print(f"duracion_seg={duracion_seg}")

            if not ENABLE_POSTGRES_LOAD:
                print("Cargue a Postgres desactivado temporalmente.")
                return

            with get_db_connection() as conn:
                print("\n" + "=" * 80)
                print("9) CARGANDO sne.ics")
                print("=" * 80)

                loader = PostgresLoader(
                    schema=PG_SCHEMA_NAME,
                    table=PG_TABLE_NAME,
                    batch_size=PG_BATCH_SIZE
                )
                total_ics = loader.insert_df(df_final, conn)
                print(f"? sne.ics upsert: {total_ics} filas")

                print("\n" + "=" * 80)
                print("10) CARGANDO sne.gestion_sne")
                print("=" * 80)

                gestion_loader = GestionSNELoader(
                    schema=PG_SCHEMA_GESTION,
                    table=PG_TABLE_GESTION,
                    batch_size=PG_BATCH_SIZE,
                    revisor_default_int=GESTION_REVISOR_DEFAULT_INT,
                )
                total_gestion = gestion_loader.upsert_from_ics_ids(df_final, conn)
                print(f"? sne.gestion_sne actualizado para {total_gestion} ids")

                print("\n" + "=" * 80)
                print("11) CARGANDO sne.ics_motivo_resp y cat?logo de motivos")
                print("=" * 80)

                motivo_resp_loader = IcsMotivoRespLoader(
                    schema_motivo_resp=PG_SCHEMA_MOTIVO_RESP,
                    table_motivo_resp=PG_TABLE_MOTIVO_RESP,
                    schema_motivos=PG_SCHEMA_MOTIVOS,
                    table_motivos=PG_TABLE_MOTIVOS,
                    batch_size=PG_BATCH_SIZE,
                    responsable_default=RESPONSABLE_DEFAULT,
                )
                total_motivo_resp = motivo_resp_loader.upsert_from_df(df_final, conn)
                print(f"? sne.ics_motivo_resp upsert: {total_motivo_resp} filas")
                conn.commit()

        except SystemExit as e:
            estado = "error"
            archivos_ok = 0
            archivos_error = 1
            print("? ERROR en el proceso:", repr(e))
            if fecha_dt.date() == fecha_limite and _es_error_por_insumos_faltantes(e):
                print(f"Se detiene sin fallo duro: aún no hay insumos para {fecha_dt.date()}.")
                soft_stop = True
                return
            raise
        except Exception as e:
            estado = "error"
            archivos_ok = 0
            archivos_error = 1
            print("? ERROR en el proceso:", repr(e))
            if fecha_dt.date() == fecha_limite and _es_error_por_insumos_faltantes(e):
                print(f"Se detiene sin fallo duro: aún no hay insumos para {fecha_dt.date()}.")
                soft_stop = True
                return
            raise
        finally:
            end_ts = datetime.now()
            duracion_seg = int(round(time.perf_counter() - start_perf))

            print("\n" + "=" * 80)
            print("12) GUARDANDO LOG EN log.procesa_report_sne")
            print("=" * 80)

            try:
                logger = ReportRunLogger()
                with get_db_connection() as conn_log:
                    id_reporte = logger.get_id_reporte(
                        conn_log,
                        NOMBRE_REPORTE_LOG,
                        default_id=DEFAULT_ID_REPORTE,
                    )
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

                print(
                    f"log: {PG_SCHEMA_LOG}.{PG_TABLE_LOG} | "
                    f"id_reporte={id_reporte} | fecha={fecha_dt.date()} | estado={estado}"
                )
                print(f"duracion_seg={duracion_seg} | registros_proce={registros_proce}")
            except Exception as log_error:
                print(f"No se pudo guardar el log: {repr(log_error)}")

        if soft_stop:
            return

    print("\n" + "=" * 80)
    print("? PROCESO COMPLETADO")
    print("=" * 80)


if __name__ == "__main__":
    main()
