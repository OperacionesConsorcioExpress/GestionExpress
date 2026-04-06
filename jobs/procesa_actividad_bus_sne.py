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

# ✅ RUTA CORRECTA de matriz según tu script de descargas
AZURE_PREFIX_MATRIZ_BASE = "0001-28-fms-matriz-de-distancia"

# Recomendado: usar variables de entorno
CONNECTION_STRING_LOCAL = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=datoscentroinformacion;"
    "AccountKey=zbNe/Asv2IFC0Q0hATwl9uQMTkuCrMYgCoO7hk1ICbC1utlZZ/boOjUBGykhr0LCr5dd9wXsA9jA+ASt8/gtlQ==;"
    "EndpointSuffix=core.windows.net"
)

PG_CONN_ENV = "POSTGRES_CONN_STRING"
POSTGRES_CONN_STRING_LOCAL = ""

PG_SCHEMA_NAME = "sne"
PG_TABLE_NAME = "actividad_bus"

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

PG_BATCH_SIZE = 5000
NOMBRE_REPORTE_LOG = "Tabla Actividad bus"
DEFAULT_ID_REPORTE = 5
DELETE_EXISTING_FOR_DATE = False

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

    @staticmethod
    def extraer_prefijo_nodo(serie: pd.Series) -> pd.Series:
        return (
            serie.astype(str)
            .str.split("_", n=1)
            .str[0]
            .str.strip()
        )

def normalize_name(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("\ufeff", "").replace("ï»¿", "")
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = s.replace("ã¡", "a").replace("ã©", "e").replace("ã­", "i").replace("ã³", "o").replace("ãº", "u")
    s = s.replace("lã­nea", "linea").replace("teã³rica", "teorica").replace("nãºmero", "numero")
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

    def list_blob_paths(self, prefix: str) -> List[str]:
        out: List[str] = []
        for b in self.container.list_blobs(name_starts_with=prefix):
            if b.name and not b.name.endswith("/"):
                out.append(b.name)
        return out

# =============================================================================
# BUILDER ACTIVIDAD BUS
# =============================================================================

class ActividadBusBuilder:
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

    def _blob_paths_actividad_bus(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")

        encontrados: List[Tuple[str, str]] = []
        for subpath in self._subpaths():
            prefijo = f"0001-22-fms-actividad-vehiculo/{anio}/{mes}/{subpath}/"
            blob_paths = self.az.list_blob_paths(prefijo)

            for ruta in blob_paths:
                nombre = os.path.basename(ruta)
                nombre_upper = nombre.upper()

                if not nombre.lower().endswith(".csv"):
                    continue
                if fecha_txt not in nombre:
                    continue
                if "ACTIVIDADBUS" in nombre_upper or "ACTIVIDAD" in nombre_upper:
                    encontrados.append((ruta, nombre))

        return encontrados

    def _blob_paths_matriz(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")

        encontrados: List[Tuple[str, str]] = []
        for subpath in self._subpaths():
            prefijo = f"{AZURE_PREFIX_MATRIZ_BASE}/{anio}/{mes}/{subpath}/"
            blob_paths = self.az.list_blob_paths(prefijo)

            for ruta in blob_paths:
                nombre = os.path.basename(ruta)
                if not nombre.lower().endswith(".csv"):
                    continue
                if fecha_txt not in nombre:
                    continue
                if nombre.upper().startswith("MD_"):
                    encontrados.append((ruta, nombre))

        return encontrados

    def _etiqueta_zona_desde_ruta(self, ruta: str) -> str:
        ruta_up = ruta.replace("\\", "/").upper()
        if "/ZN/" in ruta_up:
            return "ZN"
        if "/TR/" in ruta_up:
            return "TR"
        return "NA"

    def _nombre_visible_actividad_bus(self, ruta: str, nombre_original: str) -> str:
        zona = self._etiqueta_zona_desde_ruta(ruta)
        return f"ACTIVIDADBUS_{zona}_{nombre_original}"

    def _nombre_visible_matriz(self, ruta: str, nombre_original: str) -> str:
        zona = self._etiqueta_zona_desde_ruta(ruta)
        return f"MD_{zona}_{nombre_original}"

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
            raise SystemExit("❌ No se encontró ningún Detallado para esa fecha.")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["Fecha"])
        c_servicio = pick_col(df, ["Servicio"])
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Línea", "Viaje Linea", "Viaje LÃ­nea"])

        out = df.copy()
        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst=True)
        out["Servicio_key"] = out[c_servicio].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_tabla])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])

        out = out[["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]].copy()
        out = out.drop_duplicates(subset=["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"], keep="first")

        print(f"✅ Detallado cargado: filas útiles={len(out)}")
        return out

    def load_actividad_bus(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("3) CARGANDO ACTIVIDAD BUS (AZURE)")
        print("=" * 80)

        rutas = self._blob_paths_actividad_bus(fecha)
        if not rutas:
            raise SystemExit("❌ No se encontraron archivos de Actividad Bus para esa fecha.")

        frames: List[pd.DataFrame] = []
        resumen_zona = {"ZN": {"archivos": 0, "filas": 0}, "TR": {"archivos": 0, "filas": 0}, "NA": {"archivos": 0, "filas": 0}}

        for ruta, nombre in rutas:
            nombre_visible = self._nombre_visible_actividad_bus(ruta, nombre)
            zona = self._etiqueta_zona_desde_ruta(ruta)

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)
            df0["__archivo_origen__"] = nombre_visible
            frames.append(df0)

            resumen_zona[zona]["archivos"] += 1
            resumen_zona[zona]["filas"] += len(df0)

            print(f"  ✅ Cargado: {nombre_visible} | ruta={ruta} | filas={len(df0)} cols={len(df0.columns)}")

        print("\n📊 Resumen Actividad Bus por zona:")
        for zona in ["ZN", "TR", "NA"]:
            if resumen_zona[zona]["archivos"] > 0:
                print(f"   {zona}: {resumen_zona[zona]['archivos']} archivos | {resumen_zona[zona]['filas']} filas")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_fecha = pick_col(df, ["Fecha"])
        c_id_linea = pick_col(df, ["Id Línea", "Id Linea", "Id LÃ­nea"])
        c_id_ruta = pick_col(df, ["Id Ruta"], required=False)
        c_tabla = pick_col(df, ["Tabla"])
        c_viaje_linea = pick_col(df, ["Viaje Linea", "Viaje Línea", "Viaje LÃ­nea"])
        c_id_viaje = pick_col(df, ["Id Viaje"])
        c_nombre_nodo = pick_col(df, ["Nombre Nodo"])
        c_servicio_bus = pick_col(df, ["Servicio Bus"])
        c_numero_fms = pick_col(df, ["Número FMS Bus", "Numero FMS Bus", "NÃºmero FMS Bus"])
        c_conductor = pick_col(df, ["Conductor"])
        c_nombre_conductor = pick_col(df, ["Nombre de Conductor"])
        c_evento = pick_col(df, ["Evento"], required=False)
        c_hora_teorica = pick_col(df, ["Hora Teórica", "Hora Teorica", "Hora TeÃ³rica"])
        c_hora_referencia = pick_col(df, ["Hora Referencia"])
        c_hora_llegada = pick_col(df, ["Hora Llegada"])
        c_hora_salida = pick_col(df, ["Hora Salida"])
        c_lista_acc_reg = pick_col(df, ["Lista de acciones regulatorias"], required=False)

        out = df.copy()

        out["Fecha_key"] = self.tu.fecha_key_robusta(out[c_fecha], prefer_dayfirst="auto")
        out["Servicio_key"] = out[c_servicio_bus].astype(str).str.strip().str.upper()
        out["Coche_key"] = self.tu.to_int64(out[c_tabla])
        out["Viaje_key"] = self.tu.to_int64(out[c_viaje_linea])

        out["Fecha"] = pd.to_datetime(out[c_fecha], errors="coerce", dayfirst=True).dt.date
        out["Id_Linea"] = out[c_id_linea].astype("string").str.strip()
        out["Id_Ruta"] = out[c_id_ruta].astype("string").str.strip() if c_id_ruta else pd.NA
        out["Tabla"] = out[c_tabla].astype("string").str.strip()
        out["Viaje_Linea"] = out[c_viaje_linea].astype("string").str.strip()
        out["Id_Viaje"] = out[c_id_viaje].astype("string").str.strip()
        out["Nombre_Nodo"] = out[c_nombre_nodo].astype("string").str.strip()
        out["Servicio_Bus"] = out[c_servicio_bus].astype("string").str.strip()
        out["Numero_FMS_Bus"] = out[c_numero_fms].astype("string").str.strip()
        out["Conductor"] = out[c_conductor].astype("string").str.strip()
        out["Nombre_Conductor"] = out[c_nombre_conductor].astype("string").str.strip()
        out["Evento"] = out[c_evento].astype("string").str.strip() if c_evento else pd.NA
        out["Hora_Teorica"] = out[c_hora_teorica].astype("string").str.strip()
        out["Hora_Referencia"] = out[c_hora_referencia].astype("string").str.strip()
        out["Hora_Llegada"] = out[c_hora_llegada].astype("string").str.strip()
        out["Hora_Salida"] = out[c_hora_salida].astype("string").str.strip()
        out["Lista_Acciones_Regulatorias"] = out[c_lista_acc_reg].astype("string").str.strip() if c_lista_acc_reg else pd.NA

        keep = [
            "Fecha_key", "Servicio_key", "Coche_key", "Viaje_key",
            "Fecha", "Id_Linea", "Id_Ruta", "Tabla", "Viaje_Linea", "Id_Viaje",
            "Nombre_Nodo", "Servicio_Bus", "Numero_FMS_Bus", "Conductor",
            "Nombre_Conductor", "Evento", "Hora_Teorica", "Hora_Referencia",
            "Hora_Llegada", "Hora_Salida", "Lista_Acciones_Regulatorias"
        ]
        out = out[keep].copy()

        print(f"✅ Actividad Bus cargado: filas útiles={len(out)}")
        return out

    def load_matriz(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("4) CARGANDO MATRIZ DE DISTANCIA (AZURE)")
        print("=" * 80)

        rutas = self._blob_paths_matriz(fecha)
        if not rutas:
            print(f"⚠️ No se encontraron archivos de matriz para {fecha.strftime('%d/%m/%Y')}")
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        for ruta, nombre in rutas:
            try:
                nombre_visible = self._nombre_visible_matriz(ruta, nombre)
                df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)
                df0["__archivo_origen__"] = nombre_visible
                frames.append(df0)
                print(f"  ✅ Cargado: {nombre_visible} | ruta={ruta} | filas={len(df0)} cols={len(df0.columns)}")
            except Exception as e:
                print(f"  ❌ Error cargando matriz {nombre}: {e}")

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        c_id_linea = pick_col(df, ["Id Línea", "Id Linea", "Id LÃ­nea"])
        c_id_ruta = pick_col(df, ["Id Ruta"])
        c_nombre_nodo = pick_col(df, ["Nombre Nodo"])
        c_posicion = pick_col(df, ["Posición", "Posicion"])

        out = df.copy()
        out["IdLinea_key_m"] = pd.to_numeric(out[c_id_linea], errors="coerce").astype("Int64")
        out["IdRuta_key_m"] = pd.to_numeric(out[c_id_ruta], errors="coerce").astype("Int64")
        out["NodoPrefix_key"] = self.tu.extraer_prefijo_nodo(out[c_nombre_nodo])
        out["Distancia"] = pd.to_numeric(out[c_posicion], errors="coerce").astype("Int64")

        out = out[["IdLinea_key_m", "IdRuta_key_m", "NodoPrefix_key", "Distancia"]].copy()
        out = out.drop_duplicates(subset=["IdLinea_key_m", "IdRuta_key_m", "NodoPrefix_key"], keep="first")

        print(f"✅ Matriz cargada: filas útiles={len(out)}")
        return out

    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_ics = self.load_ics(fecha)
        df_det = self.load_detallado(fecha)
        df_act = self.load_actividad_bus(fecha)
        df_matriz = self.load_matriz(fecha)

        print("\n" + "=" * 80)
        print("5) CRUCE DETALLADO ↔ ICS")
        print("=" * 80)

        merge_keys = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]
        df_det_con_idics = df_det.merge(df_ics, on=merge_keys, how="left")
        df_det_con_idics = df_det_con_idics.drop_duplicates(subset=merge_keys, keep="first")

        print("\n" + "=" * 80)
        print("6) CRUCE ACTIVIDAD BUS ↔ DETALLADO+ICS")
        print("=" * 80)

        df_merge = df_act.merge(
            df_det_con_idics[["IdICS"] + merge_keys],
            on=merge_keys,
            how="left"
        )

        if df_matriz is not None and not df_matriz.empty:
            print("\n" + "=" * 80)
            print("7) CRUCE ACTIVIDAD BUS ↔ MATRIZ (DISTANCIA)")
            print("=" * 80)

            df_merge["IdLinea_key"] = pd.to_numeric(df_merge["Id_Linea"], errors="coerce").astype("Int64")
            df_merge["IdRuta_key"] = pd.to_numeric(df_merge["Id_Ruta"], errors="coerce").astype("Int64")
            df_merge["NodoPrefix_key"] = self.tu.extraer_prefijo_nodo(df_merge["Nombre_Nodo"])

            df_merge = df_merge.merge(
                df_matriz,
                left_on=["IdLinea_key", "IdRuta_key", "NodoPrefix_key"],
                right_on=["IdLinea_key_m", "IdRuta_key_m", "NodoPrefix_key"],
                how="left"
            )

            print(f"✅ Distancia asignada a {df_merge['Distancia'].notna().sum()} filas")
        else:
            df_merge["Distancia"] = pd.NA
            print("⚠️ No se pudo asignar distancia porque la matriz está vacía")

        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_act["Fecha"].astype(str))

        out = pd.DataFrame({
            "Id_ICS": self.tu.to_int64(df_merge["IdICS"]),
            "Fecha": df_merge["Fecha"],
            "Id_Linea": df_merge["Id_Linea"],
            "Tabla": df_merge["Tabla"],
            "Viaje_Linea": df_merge["Viaje_Linea"],
            "Id_Viaje": df_merge["Id_Viaje"],
            "Nombre_Nodo": df_merge["Nombre_Nodo"],
            "Distancia": pd.to_numeric(df_merge["Distancia"], errors="coerce").astype("Int64"),
            "Servicio_Bus": df_merge["Servicio_Bus"],
            "Numero_FMS_Bus": df_merge["Numero_FMS_Bus"],
            "Conductor": df_merge["Conductor"],
            "Nombre_Conductor": df_merge["Nombre_Conductor"],
            "Evento": df_merge["Evento"],
            "Hora_Teorica": df_merge["Hora_Teorica"],
            "Hora_Referencia": df_merge["Hora_Referencia"],
            "Hora_Llegada": df_merge["Hora_Llegada"],
            "Hora_Salida": df_merge["Hora_Salida"],
            "Lista_Acciones_Regulatorias": df_merge["Lista_Acciones_Regulatorias"],
        })

        out = out[out["Id_ICS"].notna()].copy()
        out = out.reset_index(drop=True)

        print("\n✅ Tabla Actividad Bus construida:")
        print(f"   Filas con Id_ICS: {len(out)}")
        print(f"   Filas con Distancia: {out['Distancia'].notna().sum()}")
        print(f"📌 Nombre lógico: ACTIVIDAD_BUS_{fecha_nombre}")

        return out, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.actividad_bus
# =============================================================================

class PostgresActividadBusLoader:
    DF_TO_DB = {
        "Id_ICS": "id_ics",
        "Fecha": "fecha",
        "Id_Linea": "id_linea",
        "Tabla": "tabla",
        "Viaje_Linea": "viaje_linea",
        "Id_Viaje": "id_viaje",
        "Nombre_Nodo": "nombre_nodo",
        "Distancia": "distancia",
        "Servicio_Bus": "servicio_bus",
        "Numero_FMS_Bus": "numero_fms_bus",
        "Conductor": "conductor",
        "Nombre_Conductor": "nombre_conductor",
        "Evento": "evento",
        "Hora_Teorica": "hora_teorica",
        "Hora_Referencia": "hora_referencia",
        "Hora_Llegada": "hora_llegada",
        "Hora_Salida": "hora_salida",
        "Lista_Acciones_Regulatorias": "lista_acciones_regulatorias",
    }

    def __init__(self, schema: str, table: str, batch_size: int = 5000, schema_ics: str = "sne", table_ics: str = "ics", ics_id_column: str = "id_ics"):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.schema_ics = schema_ics
        self.table_ics = table_ics
        self.ics_id_column = ics_id_column

    def _connect(self):
        return get_db_connection()

    def test_connection(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

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

        for c in ["hora_teorica", "hora_referencia", "hora_llegada", "hora_salida"]:
            d[c] = d[c].apply(self._parse_time_to_time)

        d["id_ics"] = pd.to_numeric(d["id_ics"], errors="coerce")
        d["distancia"] = pd.to_numeric(d["distancia"], errors="coerce")

        for c in ["id_linea", "tabla", "viaje_linea", "id_viaje", "numero_fms_bus", "conductor"]:
            d[c] = d[c].astype("string")

        for c in ["nombre_nodo", "servicio_bus", "nombre_conductor", "evento", "lista_acciones_regulatorias"]:
            d[c] = d[c].astype("string")

        d = d[d["id_ics"].notna()].copy()
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
                "id_linea",
                "tabla",
                "viaje_linea",
                "id_viaje",
                "nombre_nodo",
                "servicio_bus",
                "numero_fms_bus",
                "conductor",
                "evento",
                "hora_teorica",
                "hora_referencia",
                "hora_llegada",
                "hora_salida"
            )
            DO UPDATE SET
                "distancia" = EXCLUDED."distancia",
                "nombre_conductor" = EXCLUDED."nombre_conductor",
                "lista_acciones_regulatorias" = EXCLUDED."lista_acciones_regulatorias"
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
                    print(f"  ✅ Upsert actividad_bus {total}/{len(df_db)}")

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

    def get_id_reporte(self, nombre_reporte: str, default_id: int = 5) -> int:
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
    filename = f"ACTIVIDAD_BUS_{fecha_nombre}.csv"
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
        builder = ActividadBusBuilder(az, filtro_zona_tipo=FILTRO_ZONA_TIPO)

        df_final, fecha_nombre = builder.build(fecha_dt)
        registros_proce = int(len(df_final))

        _export_df_to_csv(df_final, fecha_nombre)

        print("\n" + "=" * 80)
        print("8) VALIDANDO CONEXIÓN A POSTGRES")
        print("=" * 80)

        loader = PostgresActividadBusLoader(
                        schema=PG_SCHEMA_NAME,
            table=PG_TABLE_NAME,
            batch_size=PG_BATCH_SIZE
        )

        loader.test_connection()
        print("✅ Conexión a Postgres OK")

        print("\n" + "=" * 80)
        print("9) CARGANDO A POSTGRES sne.actividad_bus")
        print("=" * 80)

        total = loader.insert_df(df_final)
        print(f"✅ sne.actividad_bus upsert: {total} filas")

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
        print("10) GUARDANDO LOG EN log.procesa_report_sne")
        print("=" * 80)

        try:
            logger = ReportRunLogger()
            id_reporte = logger.get_id_reporte("Tabla Actividad bus", default_id=5)

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
        except Exception as e_log:
            print(f"⚠️ No se pudo guardar log: {e_log}")

    print("\n" + "=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)

if __name__ == "__main__":
    main()
