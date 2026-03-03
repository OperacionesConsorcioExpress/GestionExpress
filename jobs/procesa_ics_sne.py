# Pipeline de procesamiento para ICS SNE
from __future__ import annotations
# Importa librerias 
import os, re, csv, time
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict
# realizamos importaciones pesadas solo en funciones específicas para optimizar memoria y velocidad de arranque
import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from psycopg2.extras import execute_values
from dotenv import load_dotenv
# Importamos nuestro manager de base de datos con pool centralizado
from database.database_manager import get_db_connection

# =============================================================================
# CONFIG
# =============================================================================
# Semilla: si NO existe log, procesar este día (dd/mm/yyyy)
FECHA_SEMILLA_STR = "01/02/2026"
FILTRO_ZONA_TIPO = 3          # 1=ZN, 2=TR, 3=Ambas
AZURE_CONN_ENV = "AZURE_STORAGE_CONNECTION_STRING"
AZURE_CONTAINER_ACTIVIDAD = "0001-archivos-de-apoyo-descargas-cex-fms"

# Destinos
PG_SCHEMA_NAME = "sne"
PG_TABLE_NAME = "ics"

PG_SCHEMA_GESTION = "sne"
PG_TABLE_GESTION = "gestion_sne"

PG_SCHEMA_MOTIVO_RESP = "sne"
PG_TABLE_MOTIVO_RESP = "ics_motivo_resp"

PG_SCHEMA_MOTIVOS = "sne"
PG_TABLE_MOTIVOS = "motivos_eliminacion"

RESPONSABLE_DEFAULT = 0

PG_SCHEMA_REPORTES = "sne"
PG_TABLE_REPORTES = "reportes_sne"

PG_SCHEMA_LOG = "log"
PG_TABLE_LOG = "procesa_report_sne"

PG_BATCH_SIZE = 5000

DELETE_EXISTING_FOR_DATE = False
GESTION_REVISOR_DEFAULT_INT = 0

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
    DIVISOR = 10000.0

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
    def _parse_num_mixto(s: pd.Series) -> pd.Series:
        x = s.astype(str).str.strip()
        x = x.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        x = x.str.replace(r"[^\d\.,\-\+]", "", regex=True)

        mask_coma = x.str.contains(",", na=False)
        if mask_coma.any():
            xc = x[mask_coma].str.replace(".", "", regex=False)
            xc = xc.str.replace(",", ".", regex=False)
            x.loc[mask_coma] = xc

        mask_no_coma = ~mask_coma
        if mask_no_coma.any():
            xn = x[mask_no_coma]

            mask_multi_dot = xn.str.count(r"\.") >= 2
            if mask_multi_dot.any():
                xn.loc[mask_multi_dot] = xn.loc[mask_multi_dot].str.replace(".", "", regex=False)

            mask_one_dot = xn.str.count(r"\.") == 1
            if mask_one_dot.any():
                part = xn.loc[mask_one_dot]
                suf = part.str.split(".", n=1).str[1].fillna("")
                mask_miles = suf.str.len() == 3
                miles_idx = part.index[mask_miles]
                if len(miles_idx):
                    xn.loc[miles_idx] = xn.loc[miles_idx].str.replace(".", "", regex=False)

            x.loc[mask_no_coma] = xn

        return pd.to_numeric(x, errors="coerce").astype("float64")

    @classmethod
    def to_km_smart(cls, series: pd.Series) -> pd.Series:
        raw = cls._parse_num_mixto(series)
        out = raw.copy()
        mask = raw.notna() & (raw > 1000)
        out.loc[mask] = raw.loc[mask] / cls.DIVISOR
        return out

    @staticmethod
    def to_offset_detallado_km(series: pd.Series) -> pd.Series:
        raw = pd.to_numeric(series, errors="coerce").astype("float64")
        out = raw / 1000.0
        out.loc[raw == 0] = 0.0
        return out

    @staticmethod
    def to_km_metros(series: pd.Series) -> pd.Series:
        raw = pd.to_numeric(series, errors="coerce").astype("float64")
        return raw / 1000.0

    @staticmethod
    def fecha_para_nombre_archivo_dd_mm_yyyy(serie_fecha_viaje: pd.Series) -> str:
        s = serie_fecha_viaje.astype(str).str.replace("\ufeff", "", regex=False).str.replace("ï»¿", "", regex=False).str.strip()
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)

        if dt.notna().any():
            moda = dt.dropna().dt.date.value_counts().idxmax()
            dt_moda = pd.to_datetime(str(moda))
            return dt_moda.strftime("%d_%m_%Y")

        return "SIN_FECHA"

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
# BUILD SNE (REGLAS COMPLETAS)
# =============================================================================
class SNEFromAzureBuilder:
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

    def _blob_paths_stat(self, fecha: datetime) -> List[Tuple[str, str]]:
        anio = fecha.strftime("%Y")
        mes = fecha.strftime("%m")
        fecha_txt = fecha.strftime("%d_%m_%Y")
        out: List[Tuple[str, str]] = []
        for zona in ["US", "SC"]:
            nombre = f"Tabla_IntervalosViajeStat_{fecha_txt}_{zona}.csv"
            ruta = f"0001-36-fms-tabla-viajestat/{anio}/{mes}/{nombre}"
            out.append((ruta, nombre))
        return out
    
    def _prefix_viajes_tardios(self) -> str:
        return "0001-40-fms-tabla-viajestardios/"
    
    # ---------------------------
    # Loaders
    # ---------------------------
    def load_ics(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("1) CARGANDO ICS (AZURE)")
        print("=" * 80)

        ruta, _ = self._blob_path_ics(fecha)
        if not self.az.exists(ruta):
            raise SystemExit(f"❌ No existe ICS en Azure para esa fecha:\n   {ruta}")

        df = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)

        requeridas = [
            "IdICS", "Fecha Viaje", "Linea SAE", "Servicio", "Coche", "ViajeLinea",
            "Km Programado", "Km Efectivamente Ejecutado",
            "Km Eliminado", "DistAutorizada", "DistSupAcc",
            "IdViaje", "Operador",
        ]
        faltan = [c for c in requeridas if c not in df.columns]
        if faltan:
            print("❌ ICS: faltan columnas requeridas:", faltan)
            print("   Columnas disponibles:", list(df.columns))
            raise SystemExit("❌ Proceso detenido - faltan columnas en ICS")

        df = df.copy()
        df["Fecha_key"] = self.tu.fecha_key_robusta(df["Fecha Viaje"], prefer_dayfirst="auto")
        df["Servicio_key"] = df["Servicio"].astype(str).str.strip().str.upper()
        df["Coche_key"] = self.tu.to_int64(df["Coche"])
        df["Viaje_key"] = self.tu.to_int64(df["ViajeLinea"])
        df["IdICS"] = self.tu.to_int64(df["IdICS"])
        df["IdViaje_key"] = self.tu.to_int64(df["IdViaje"])

        print(f"✅ ICS cargado: {len(df)} filas")
        return df

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
            raise SystemExit("❌ No se encontró ningún Detallado para esa fecha (según ZN/TR y US/SC).")

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        col_viaje_list = [c for c in df.columns if str(c).startswith("Viaje L")]
        if not col_viaje_list:
            print("❌ Detallado: no se encontró columna que empiece por 'Viaje L'")
            print("   Columnas disponibles:", list(df.columns))
            raise SystemExit("❌ Proceso detenido - no hay 'Viaje Línea' en Detallado")
        col_viaje_det = col_viaje_list[0]

        requeridas = ["Fecha", "Servicio", "Tabla", col_viaje_det]
        faltan = [c for c in requeridas if c not in df.columns]
        if faltan:
            print("❌ Detallado: faltan columnas requeridas:", faltan)
            print("   Columnas disponibles:", list(df.columns))
            raise SystemExit("❌ Proceso detenido - faltan columnas en Detallado")

        cols_traer = [
            "Sentido",
            "Vehículo Real",
            "Hora Ini Teorica",
            "Hora Ini Referencia",
            "Conductor",
            "OffsetInicio",
            "OffsetFin",
            "Descripción Motivo Elim",
            "Eliminado",
        ]
        cols_presentes = [c for c in cols_traer if c in df.columns]
        faltan_traer = [c for c in cols_traer if c not in df.columns]
        if faltan_traer:
            print("⚠️ Detallado: estas columnas NO están y quedarán en blanco:", faltan_traer)

        df = df.copy()
        df["Fecha_key"] = self.tu.fecha_key_robusta(df["Fecha"], prefer_dayfirst=True)
        df["Servicio_key"] = df["Servicio"].astype(str).str.strip().str.upper()
        df["Coche_key"] = self.tu.to_int64(df["Tabla"])
        df["Viaje_key"] = self.tu.to_int64(df[col_viaje_det])

        keep = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"] + cols_presentes
        df = df[keep].copy()
        df = df.drop_duplicates(subset=["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"], keep="first")

        print(f"✅ Detallado cargado: {len(df)} filas (post-dedup por llaves)")
        return df

    def load_stat(self, fecha: datetime) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("2B) CARGANDO VIAJESTAT (AZURE)")
        print("=" * 80)

        frames: List[pd.DataFrame] = []
        for ruta, nombre in self._blob_paths_stat(fecha):
            if not self.az.exists(ruta):
                print(f"  ⚠️ No existe: {nombre}")
                continue

            df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(ruta), dtype=str)
            df0["__archivo_origen__"] = nombre
            frames.append(df0)
            print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")

        if not frames:
            print("⚠️ No se encontró Stat. Se omite condición de Regulación Offline.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        req = ["APPLY_DATE", "VEH_SERV_ID", "SERV_TRIP_SEQ", "LOC_TYPE_CD"]
        faltan = [c for c in req if c not in df.columns]
        if faltan:
            print(f"⚠️ Stat: faltan columnas {faltan}. Se omite condición de Regulación Offline.")
            return pd.DataFrame()

        df = df.copy()
        df["Fecha_stat_key"] = pd.to_datetime(
            df["APPLY_DATE"].astype(str).str.strip(),
            format="%Y%m%d",
            errors="coerce"
        ).dt.date
        df["Servicio_stat_key"] = df["VEH_SERV_ID"].astype(str).str.strip().str.upper()
        df["IdViaje_stat_key"] = pd.to_numeric(df["SERV_TRIP_SEQ"], errors="coerce").astype("Int64")
        df["LOC_TYPE_CD_num"] = pd.to_numeric(df["LOC_TYPE_CD"], errors="coerce").astype("Int64")

        dist_col = "ACCUM_TRIP_DIST" if "ACCUM_TRIP_DIST" in df.columns else None
        if dist_col:
            dist_raw = pd.to_numeric(df[dist_col], errors="coerce").astype("float64")
            df["_dist_km"] = np.where(dist_raw > 1000, dist_raw / 1000.0, dist_raw)
        else:
            df["_dist_km"] = np.nan

        before = len(df)
        df = df.dropna(subset=["Fecha_stat_key", "Servicio_stat_key", "IdViaje_stat_key", "LOC_TYPE_CD_num"])
        print(f"✅ Stat listo: {len(df)}/{before} filas con llaves válidas")
        return df

    def load_viajes_tardios(self) -> pd.DataFrame:
        print("\n" + "=" * 80)
        print("2C) CARGANDO VIAJES TARDÍOS (AZURE)")
        print("=" * 80)

        prefix = self._prefix_viajes_tardios()
        blob_paths = self.az.list_blob_paths(prefix)
        blob_paths = [p for p in blob_paths if p.lower().endswith(".csv")]

        if not blob_paths:
            print("⚠️ No se encontraron archivos en Viajes Tardíos. Se omite condición.")
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        for p in blob_paths:
            nombre = os.path.basename(p)
            try:
                df0 = self.io.leer_csv_desde_bytes(self.az.read_bytes(p), dtype=str)
                df0["__archivo_origen__"] = nombre
                frames.append(df0)
                print(f"  ✅ Cargado: {nombre} | filas={len(df0)} cols={len(df0.columns)}")
            except Exception as e:
                print(f"  ⚠️ Error leyendo {nombre}: {e}")

        if not frames:
            print("⚠️ No se pudo leer ningún archivo de Viajes Tardíos. Se omite condición.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = self.io.limpiar_columnas(df)

        req = ["Fecha", "Tabla", "Viaje", "Servicio Viaje", "Estado Localización", "Distancia"]
        faltan = [c for c in req if c not in df.columns]
        if faltan:
            print(f"⚠️ Viajes Tardíos: faltan columnas {faltan}. Se omite condición.")
            return pd.DataFrame()

        sv = df["Servicio Viaje"].astype(str).str.strip()
        servicio = sv.str.split("-", n=2).str[1]
        df = df.copy()
        df["Servicio_extr"] = servicio.astype(str).str.strip().str.upper()

        df["Fecha_key"] = self.tu.fecha_key_robusta(df["Fecha"], prefer_dayfirst="auto")
        df["Coche_key"] = pd.to_numeric(df["Tabla"], errors="coerce").astype("Int64")
        df["Viaje_key"] = pd.to_numeric(df["Viaje"], errors="coerce").astype("Int64")
        df["Servicio_key"] = df["Servicio_extr"]

        df["Estado_loc_num"] = pd.to_numeric(df["Estado Localización"], errors="coerce").astype("Int64")
        df = df[df["Estado_loc_num"].isin([5, 8, 9])].copy()

        df["Dist_km"] = self.tu.to_km_metros(df["Distancia"])

        before = len(df)
        df = df.dropna(subset=["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"])
        print(f"✅ Viajes Tardíos listo: {len(df)}/{before} filas con llaves válidas y estado 5/8/9")
        return df

    # ---------------------------
    # Build
    # ---------------------------
    def build(self, fecha: datetime) -> tuple[pd.DataFrame, str]:
        df_ics = self.load_ics(fecha)
        df_det = self.load_detallado(fecha)
        df_stat = self.load_stat(fecha)
        df_tardios = self.load_viajes_tardios()

        print("\n" + "=" * 80)
        print("3) CRUCE ICS ↔ DETALLADO")
        print("=" * 80)

        merge_keys = ["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"]
        df = df_ics.merge(df_det, on=merge_keys, how="left")

        fecha_nombre = self.tu.fecha_para_nombre_archivo_dd_mm_yyyy(df_ics["Fecha Viaje"])

        km_prog = self.tu.to_km_smart(df["Km Programado"])
        km_ejec = self.tu.to_km_smart(df["Km Efectivamente Ejecutado"])
        km_elim = self.tu.to_km_smart(df["Km Eliminado"])
        dist_aut = self.tu.to_km_smart(df["DistAutorizada"])
        dist_sup = self.tu.to_km_smart(df["DistSupAcc"])

        km_formula = ((km_prog + dist_aut) - (km_elim + dist_sup))
        km_revision = (pd.to_numeric(km_formula, errors="coerce") - pd.to_numeric(km_ejec, errors="coerce"))
        km_revision = km_revision.clip(lower=0)

        offset_ini = self.tu.to_offset_detallado_km(df["OffsetInicio"]) if "OffsetInicio" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
        offset_fin_raw = self.tu.to_offset_detallado_km(df["OffsetFin"]) if "OffsetFin" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)

        km_prog_num = pd.to_numeric(km_prog, errors="coerce")
        offset_fin_num = pd.to_numeric(offset_fin_raw, errors="coerce")
        offset_fin_calc = (km_prog_num - offset_fin_num).clip(lower=0)

        km_elim_num = pd.to_numeric(km_elim, errors="coerce")
        offset_ini_num = pd.to_numeric(offset_ini, errors="coerce")
        km_f2 = (offset_ini_num.fillna(0) + offset_fin_calc.fillna(0) - km_elim_num.fillna(0)).clip(lower=0)

        dist_sup_num = pd.to_numeric(dist_sup, errors="coerce")
        dist_aut_num = pd.to_numeric(dist_aut, errors="coerce")
        km_elim_acc = (dist_sup_num.fillna(0) - dist_aut_num.fillna(0)).clip(lower=0)

        km_elim_eic_aprox = (km_elim_num.fillna(0) + dist_sup_num.fillna(0))

        hora_teo = df["Hora Ini Teorica"] if "Hora Ini Teorica" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
        hora_ref = df["Hora Ini Referencia"] if "Hora Ini Referencia" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
        hora_teo_s = hora_teo.astype("string").fillna("").str.strip()
        hora_ref_s = hora_ref.astype("string").fillna("").str.strip()
        hora_final = hora_teo_s.mask(hora_teo_s.eq(""), hora_ref_s).replace({"": pd.NA})

        fecha_proceso_date = fecha.date()

        out = pd.DataFrame({
            "Fecha": fecha_proceso_date,
            "Id_ICS": self.tu.to_int64(df["IdICS"]),
            "Linea": df["Linea SAE"],
            "Servicio": df["Servicio"],
            "Tabla": df["Coche"],
            "Viaje_Linea": df["ViajeLinea"],
            "Id_Viaje": df["IdViaje"],

            "Sentido": df["Sentido"] if "Sentido" in df.columns else pd.NA,
            "Vehiculo_Real": df["Vehículo Real"] if "Vehículo Real" in df.columns else pd.NA,
            "Hora_Ini_Teorica": hora_final,

            "KmProgAd": km_prog,
            "Conductor": df["Conductor"] if "Conductor" in df.columns else pd.NA,

            "Km_ElimEIC": (km_prog - km_ejec),
            "KmEjecutado": km_ejec,

            "OffsetInicio": offset_ini,
            "OffSet_Fin": offset_fin_raw,
            "Km_Revision": km_revision,
            "Concesion": df["Operador"],

            "Motivo": df["Descripción Motivo Elim"] if "Descripción Motivo Elim" in df.columns else "",
        })

        out["_Eliminado_det"] = df["Eliminado"] if "Eliminado" in df.columns else pd.NA
        out["_DistAut_km"] = dist_aut
        out["_DistSup_km"] = dist_sup
        out["_KmEliminado_ics"] = km_elim

        out["_KmElimAcc"] = km_elim_acc
        out["_KmF2"] = km_f2
        out["_KmElimEIC_aprox"] = km_elim_eic_aprox

        kmprog_out = pd.to_numeric(out["KmProgAd"], errors="coerce")
        kmejec_out = pd.to_numeric(out["KmEjecutado"], errors="coerce")
        mask_equal = kmprog_out.notna() & kmejec_out.notna() & np.isclose(kmprog_out, kmejec_out, rtol=0, atol=1e-9)
        out = out[~mask_equal].copy()

        out["Motivo"] = out["Motivo"].astype("string").fillna("").str.strip()

        tol_eq = 0.001
        tol_off = 0.06

        motivo_vacio = out["Motivo"].eq("")

        kmr = pd.to_numeric(out["Km_Revision"], errors="coerce")
        kmp = pd.to_numeric(out["KmProgAd"], errors="coerce")
        kme_ics = pd.to_numeric(out["_KmEliminado_ics"], errors="coerce")
        km_elimacc_s = pd.to_numeric(out["_KmElimAcc"], errors="coerce")
        km_f2_s = pd.to_numeric(out["_KmF2"], errors="coerce")
        km_elim_eic_calc = pd.to_numeric(out["_KmElimEIC_aprox"], errors="coerce")

        mask_accion = motivo_vacio & km_elimacc_s.notna() & (km_elimacc_s.abs() >= tol_eq)
        out.loc[mask_accion, "Motivo"] = "Acción de regulación"
        motivo_vacio = out["Motivo"].eq("")

        if not df_stat.empty:
            out_keys = pd.DataFrame({
                "Fecha_stat_key": pd.to_datetime(out["Fecha"], errors="coerce").dt.date,
                "Servicio_stat_key": out["Servicio"].astype(str).str.strip().str.upper(),
                "IdViaje_stat_key": pd.to_numeric(out["Id_Viaje"], errors="coerce").astype("Int64"),
            }, index=out.index)

            stat_89 = df_stat[df_stat["LOC_TYPE_CD_num"].isin([8, 9])].copy()

            if stat_89["_dist_km"].notna().any():
                km89_any = (
                    stat_89
                    .groupby(["Fecha_stat_key", "Servicio_stat_key", "IdViaje_stat_key"], as_index=False)["_dist_km"]
                    .sum()
                    .rename(columns={"_dist_km": "km89"})
                )
            else:
                km89_any = (
                    stat_89
                    .groupby(["Fecha_stat_key", "Servicio_stat_key", "IdViaje_stat_key"], as_index=False)
                    .size()
                    .rename(columns={"size": "km89"})
                )

            tmp = out_keys.merge(
                km89_any,
                on=["Fecha_stat_key", "Servicio_stat_key", "IdViaje_stat_key"],
                how="left"
            )

            dist_aut_full = pd.to_numeric(out["_DistAut_km"], errors="coerce").fillna(0)
            mask_distaut_0 = dist_aut_full.abs() < tol_eq

            km89_val = pd.to_numeric(tmp["km89"], errors="coerce").fillna(0)
            mask_km89 = km89_val.abs() >= tol_eq

            mask_reg_off = motivo_vacio & mask_distaut_0.values & mask_km89.values
            out.loc[out.index[mask_reg_off], "Motivo"] = "Regulación Offline"
            motivo_vacio = out["Motivo"].eq("")

        elim_flag = out["_Eliminado_det"].astype("string").fillna("").str.strip().str.upper().eq("PARCIAL")
        mask_desloc_elim = motivo_vacio & elim_flag & kmr.notna() & kme_ics.notna() & (kmr > (kme_ics + tol_eq))
        out.loc[mask_desloc_elim, "Motivo"] = "Deslocalización con eliminación"
        motivo_vacio = out["Motivo"].eq("")

        eq_prog = kmr.notna() & kmp.notna() & ((kmr - kmp).abs() < tol_eq)
        if not df_tardios.empty:
            tard_sum = (
                df_tardios
                .groupby(["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"], as_index=False)["Dist_km"]
                .sum()
                .rename(columns={"Dist_km": "km589"})
            )

            out_k = pd.DataFrame({
                "Fecha_key": self.tu.fecha_key_robusta(out["Fecha"].astype(str), prefer_dayfirst="auto"),
                "Servicio_key": out["Servicio"].astype(str).str.strip().str.upper(),
                "Coche_key": pd.to_numeric(out["Tabla"], errors="coerce").astype("Int64"),
                "Viaje_key": pd.to_numeric(out["Viaje_Linea"], errors="coerce").astype("Int64"),
            }, index=out.index)

            tmp_t = out_k.merge(
                tard_sum,
                on=["Fecha_key", "Servicio_key", "Coche_key", "Viaje_key"],
                how="left"
            )

            has_tard_km = pd.to_numeric(tmp_t["km589"], errors="coerce").fillna(0).abs() >= tol_eq

            mask_viaje_tardio = motivo_vacio & eq_prog & has_tard_km.values
            mask_viaje_no_ej = motivo_vacio & eq_prog & (~has_tard_km.values)

            out.loc[out.index[mask_viaje_tardio], "Motivo"] = "Viaje Tardío"
            out.loc[out.index[mask_viaje_no_ej], "Motivo"] = "Viaje no ejecutado no eliminado"
            motivo_vacio = out["Motivo"].eq("")
        else:
            mask_vne = motivo_vacio & eq_prog
            out.loc[mask_vne, "Motivo"] = "Viaje no ejecutado no eliminado"
            motivo_vacio = out["Motivo"].eq("")

        mask_f2 = motivo_vacio & km_f2_s.notna() & kmr.notna() & (km_f2_s >= (kmr - tol_off))
        out.loc[mask_f2, "Motivo"] = "F2 Inicio-Fin"
        motivo_vacio = out["Motivo"].eq("")

        mask_off_lim = (
            motivo_vacio
            & km_f2_s.notna()
            & km_elimacc_s.notna()
            & km_elim_eic_calc.notna()
            & ((km_f2_s + km_elimacc_s) >= (km_elim_eic_calc - tol_eq))
        )
        out.loc[mask_off_lim, "Motivo"] = "OffSet & Limitación"
        motivo_vacio = out["Motivo"].eq("")

        mask_offs_desloc = (
            motivo_vacio
            & kmr.notna()
            & km_f2_s.notna()
            & (kmr.abs() >= tol_eq)
            & (km_f2_s.abs() >= tol_eq)
            & ((kmr - km_f2_s) > tol_off)
        )
        out.loc[mask_offs_desloc, "Motivo"] = "OffSet Deslocalización"
        motivo_vacio = out["Motivo"].eq("")

        out.loc[motivo_vacio, "Motivo"] = "Deslocalización"

        out["Concesion"] = out["Concesion"].astype(str).str.strip()
        out.loc[out["Concesion"].str.upper() == "CONSORCIO EXPRESS SAN CRISTOBAL", "Concesion"] = "1"
        out.loc[out["Concesion"].str.upper() == "CONSORCIO EXPRESS USAQUEN", "Concesion"] = "2"

        out["Servicio"] = out["Servicio"].astype(str).str.strip()
        out = out[~out["Servicio"].str.upper().str.startswith(("S", "K"), na=False)].copy()

        out["Km_Revision"] = pd.to_numeric(out["Km_Revision"], errors="coerce")
        out = out[(out["Km_Revision"].notna()) & (out["Km_Revision"] != 0)].copy()

        v = out["Vehiculo_Real"].astype("string").fillna("").str.strip()
        out["Vehiculo_Real"] = v.mask(v.eq(""), "Z00-0000")

        out = out.sort_values(by="Km_Revision", ascending=False, na_position="last").reset_index(drop=True)

        for c in ["_Eliminado_det", "_DistAut_km", "_DistSup_km", "_KmEliminado_ics", "_KmElimAcc", "_KmF2", "_KmElimEIC_aprox"]:
            if c in out.columns:
                out = out.drop(columns=[c])

        columnas_finales = [
            "Fecha", "Id_ICS", "Linea", "Servicio", "Tabla", "Viaje_Linea", "Id_Viaje",
            "Sentido", "Vehiculo_Real", "Hora_Ini_Teorica",
            "KmProgAd", "Conductor", "Km_ElimEIC", "KmEjecutado",
            "OffsetInicio", "OffSet_Fin",
            "Km_Revision", "Concesion", "Motivo",
        ]
        out = out[columnas_finales].copy()

        print("\n✅ Tabla SNE construida:")
        print(f"   Filas: {len(out)}")
        print(f"📌 Nombre lógico: SNE_{fecha_nombre}")

        print("\n📌 Top motivos (antes de cargar):")
        print(out["Motivo"].astype(str).str.strip().replace("", "VACIO").value_counts().head(30))

        return out, fecha_nombre

# =============================================================================
# POSTGRES LOAD sne.ics (UPSERT por id_ics) — usando pool
# =============================================================================

class PostgresLoader:
    DF_TO_DB = {
        "Fecha": "fecha",
        "Id_ICS": "id_ics",
        "Linea": "id_linea",
        "Servicio": "servicio",
        "Tabla": "tabla",
        "Viaje_Linea": "viaje_linea",
        "Id_Viaje": "id_viaje",
        "Sentido": "sentido",
        "Vehiculo_Real": "vehiculo_real",
        "Hora_Ini_Teorica": "hora_ini_teorica",
        "KmProgAd": "km_prog_ad",
        "Conductor": "conductor",
        "Km_ElimEIC": "km_elim_eic",
        "KmEjecutado": "km_ejecutado",
        "OffsetInicio": "offset_inicio",
        "OffSet_Fin": "offset_fin",
        "Km_Revision": "km_revision",
        "Concesion": "id_concesion",
        "Motivo": "motivo",
    }

    def __init__(self, schema: str, table: str, batch_size: int = 5000):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size

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
        if isinstance(v, str) and v.strip() == "":
            return None
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
    def _interval_text(v):
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        if re.match(r"^\d{1,2}:\d{2}:\d{2}$", s):
            return s
        return None

    def _prepare_df_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.DF_TO_DB.keys() if c not in df.columns]
        if missing:
            raise SystemExit(f"❌ Faltan columnas en DF para insertar a Postgres: {missing}")

        d = df[list(self.DF_TO_DB.keys())].copy()
        d = d.rename(columns=self.DF_TO_DB)

        d["fecha"] = d["fecha"].apply(self._parse_fecha_to_date)
        d["hora_ini_teorica"] = d["hora_ini_teorica"].apply(self._interval_text)

        for c in ["id_ics", "id_linea", "tabla", "viaje_linea", "id_viaje", "conductor", "id_concesion"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")

        for c in ["km_prog_ad", "km_elim_eic", "km_ejecutado", "offset_inicio", "offset_fin", "km_revision"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")

        for c in ["servicio", "sentido", "vehiculo_real", "motivo"]:
            d[c] = d[c].astype("string")

        return d

    def delete_existing_for_date(self, conn, fecha_date) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{self.table}" WHERE fecha = %s',
                (fecha_date,)
            )
            return cur.rowcount

    def insert_df(self, conn, df: pd.DataFrame) -> int:
        df_db = self._prepare_df_for_db(df)

        full_table = f'"{self.schema}"."{self.table}"'
        cols = list(df_db.columns)
        col_sql = ",".join([f'"{c}"' for c in cols])

        values_template = "(" + ",".join(
            ["%s::interval" if c == "hora_ini_teorica" else "%s" for c in cols]
        ) + ")"

        conflict_col = "id_ics"
        update_cols = [c for c in cols if c != conflict_col]

        set_parts = []
        for c in update_cols:
            if c == "hora_ini_teorica":
                set_parts.append(f'"{c}" = EXCLUDED."{c}"::interval')
            else:
                set_parts.append(f'"{c}" = EXCLUDED."{c}"')
        set_sql = ", ".join(set_parts)

        sql = f"""
            INSERT INTO {full_table} ({col_sql})
            VALUES %s
            ON CONFLICT ("{conflict_col}")
            DO UPDATE SET {set_sql}
        """

        total = 0
        with conn.cursor() as cur:
            if DELETE_EXISTING_FOR_DATE and "fecha" in df_db.columns and len(df_db) > 0:
                fecha0 = df_db.iloc[0]["fecha"]
                deleted = self.delete_existing_for_date(conn, fecha0)
                print(f"🧹 DELETE previo (fecha={fecha0}): {deleted} filas")

            for start in range(0, len(df_db), self.batch_size):
                chunk = df_db.iloc[start:start + self.batch_size]
                records = [tuple(self._py(x) for x in row) for row in chunk.itertuples(index=False, name=None)]
                execute_values(cur, sql, records, template=values_template, page_size=len(records))
                total += len(records)
                print(f"  ✅ Upsert procesado {total}/{len(df_db)}")

        return total

# =============================================================================
# GESTION sne.gestion_sne — usando pool
# =============================================================================
class GestionSNELoader:
    def __init__(
        self,
        schema: str = PG_SCHEMA_GESTION,
        table: str = PG_TABLE_GESTION,
        batch_size: int = 5000,
        revisor_default_int: int = 0,
    ):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.revisor_default_int = int(revisor_default_int)

    @staticmethod
    def _clean_ids(series: pd.Series) -> list[int]:
        s = pd.to_numeric(series, errors="coerce").dropna().astype("int64")
        return sorted(set(int(x) for x in s.tolist()))

    def upsert_from_ics_ids(self, conn, df_sne: pd.DataFrame) -> int:
        if df_sne is None or df_sne.empty or "Id_ICS" not in df_sne.columns:
            print("⚠️ gestion_sne: DF vacío o sin Id_ICS. No se hace nada.")
            return 0

        ids_unique = self._clean_ids(df_sne["Id_ICS"])
        if not ids_unique:
            print("⚠️ gestion_sne: no hay Id_ICS válidos.")
            return 0

        full_table = f'"{self.schema}"."{self.table}"'
        sql = f"""
            INSERT INTO {full_table}
                ("id_ics","revisor","estado_asignacion","estado_objecion","estado_transmitools")
            VALUES %s
            ON CONFLICT ("id_ics")
            DO UPDATE SET
                "revisor" = CASE
                    WHEN {full_table}."revisor" IS NULL THEN EXCLUDED."revisor"
                    ELSE {full_table}."revisor"
                END,
                "estado_asignacion" = CASE
                    WHEN {full_table}."estado_asignacion" IS NULL THEN 0
                    ELSE {full_table}."estado_asignacion"
                END,
                "estado_objecion" = CASE
                    WHEN {full_table}."estado_objecion" IS NULL OR {full_table}."estado_objecion" = 0 THEN 0
                    ELSE {full_table}."estado_objecion"
                END,
                "estado_transmitools" = CASE
                    WHEN {full_table}."estado_transmitools" IS NULL OR {full_table}."estado_transmitools" = 0 THEN 0
                    ELSE {full_table}."estado_transmitools"
                END
        """

        total = 0
        with conn.cursor() as cur:
            for start in range(0, len(ids_unique), self.batch_size):
                chunk = ids_unique[start:start + self.batch_size]
                records = [(int(x), self.revisor_default_int, 0, 0, 0) for x in chunk]
                execute_values(cur, sql, records, page_size=len(records))
                total += len(records)

        return total

# =============================================================================
# MOTIVO/RESP — usando pool
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

    @staticmethod
    def _norm_series(s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .str.replace("\ufeff", "", regex=False)
             .str.strip()
             .str.upper()
             .str.replace(r"\s+", " ", regex=True)
        )

    def _get_catalog(self, conn) -> pd.DataFrame:
        sql = f'SELECT id, motivo, responsable FROM "{self.schema_motivos}"."{self.table_motivos}"'
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["id", "motivo", "responsable"]) if rows else pd.DataFrame(columns=["id", "motivo", "responsable"])
        if "motivo" not in df.columns:
            df["motivo"] = ""
        df["motivo_norm"] = self._norm_series(df["motivo"])
        return df

    def _fix_sequence(self, conn) -> None:
        full_table = f'"{self.schema_motivos}"."{self.table_motivos}"'
        sql = f"""
            SELECT setval(
                pg_get_serial_sequence('{full_table}', 'id'),
                COALESCE((SELECT MAX(id) FROM {full_table}), 0) + 1,
                false
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

        full_table = f'"{self.schema_motivos}"."{self.table_motivos}"'
        sql_ins = f'INSERT INTO {full_table} ("motivo","responsable") VALUES %s'

        items = list(missing_norm_to_raw.items())
        total = 0
        with conn.cursor() as cur:
            for start in range(0, len(items), self.batch_size):
                chunk = items[start:start + self.batch_size]
                records = [(raw, int(self.responsable_default)) for (_norm, raw) in chunk]
                execute_values(cur, sql_ins, records, page_size=len(records))
                total += len(records)

        self._fix_sequence(conn)
        return total

    def upsert_from_df(self, conn, df_sne: pd.DataFrame) -> int:
        if df_sne is None or df_sne.empty:
            print("⚠️ ics_motivo_resp: DF vacío.")
            return 0

        if "Id_ICS" not in df_sne.columns or "Motivo" not in df_sne.columns:
            print("⚠️ ics_motivo_resp: faltan columnas Id_ICS o Motivo.")
            return 0

        df = df_sne[["Id_ICS", "Motivo"]].copy()
        df["Id_ICS"] = pd.to_numeric(df["Id_ICS"], errors="coerce")
        df = df.dropna(subset=["Id_ICS"]).copy()

        df["Motivo"] = df["Motivo"].astype("string").fillna("").astype(str)
        df["Motivo"] = df["Motivo"].str.replace("\ufeff", "", regex=False).str.strip().str.replace(r"\s+", " ", regex=True)

        df["motivo_norm"] = self._norm_series(df["Motivo"])
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
            raw = str(r.Motivo).replace("\ufeff", "").strip()
            raw = re.sub(r"\s+", " ", raw)
            if norm and raw:
                missing_norm_to_raw[norm] = raw

        if missing_norm_to_raw:
            print(f"⚠️ motivos_eliminacion: se crearán {len(missing_norm_to_raw)} motivos nuevos (responsable={self.responsable_default}).")
            created = self._insert_missing_catalog(conn, missing_norm_to_raw)
            print(f"✅ motivos_eliminacion: insertados {created} nuevos motivos.")

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
# LOG (pool) + cálculo de fecha automática
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
        """
        Regla:
        - Si existe al menos un log con estado 'ok' => siguiente día a MAX(fecha)
        - Si no existe => fecha_semilla
        """
        full_table = f'"{self.schema_log}"."{self.table_log}"'
        sql = f"""
            SELECT MAX("fecha") AS max_fecha
            FROM {full_table}
            WHERE "id_reporte" = %s AND LOWER(TRIM("estado")) = 'ok'
        """
        with conn.cursor() as cur:
            cur.execute(sql, (id_reporte,))
            row = cur.fetchone()

        max_fecha = row[0] if row else None
        if max_fecha is None:
            return fecha_semilla
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

        with conn.cursor() as cur:
            cur.execute(sql_select, (id_reporte, fecha_reporte_date))
            row = cur.fetchone()

            # Si ya hay OK, no tocar
            if row and str(row[0]).strip().lower() == "ok":
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

# =============================================================================
# MAIN
# =============================================================================

def _get_azure_connection_string() -> str:
    env_conn = os.environ.get(AZURE_CONN_ENV, "").strip()
    if not env_conn:
        raise SystemExit(f"❌ Falta {AZURE_CONN_ENV} en .env / variables de entorno.")
    return env_conn

def _parse_semilla() -> date:
    return datetime.strptime(FECHA_SEMILLA_STR, "%d/%m/%Y").date()

def main() -> None:
    load_dotenv()
    start_perf = time.perf_counter()

    conn_azure = _get_azure_connection_string()
    fecha_semilla = _parse_semilla()

    estado = "ok"
    archivos_total = 1
    archivos_ok = 1
    archivos_error = 0
    registros_proce = 0

    fecha_dt: Optional[datetime] = None
    id_reporte: int = 1

    with get_db_connection() as conn:
        logger = ReportRunLogger()
        id_reporte = logger.get_id_reporte(conn, "ICS", default_id=1)

        # fecha automática desde log
        fecha_to_process = logger.get_next_fecha_to_process(conn, id_reporte=id_reporte, fecha_semilla=fecha_semilla)
        fecha_dt = datetime.combine(fecha_to_process, datetime.min.time())
        print(f"\n📅 Fecha a procesar (auto): {fecha_to_process} | id_reporte={id_reporte}")

        try:
            az = AzureBlobReader(AzureConfig(connection_string=conn_azure))
            builder = SNEFromAzureBuilder(az, filtro_zona_tipo=FILTRO_ZONA_TIPO)

            df_sne, _ = builder.build(fecha_dt)
            registros_proce = int(len(df_sne))

            print("\n" + "=" * 80)
            print("4) CARGANDO A POSTGRES sne.ics (UPSERT POR id_ics)")
            print("=" * 80)

            loader = PostgresLoader(schema=PG_SCHEMA_NAME, table=PG_TABLE_NAME, batch_size=PG_BATCH_SIZE)
            total_ics = loader.insert_df(conn, df_sne)
            print(f"✅ sne.ics upsert: {total_ics} filas")

            print("\n" + "=" * 80)
            print("5) ACTUALIZANDO sne.gestion_sne (IDs ICS únicos)")
            print("=" * 80)

            gestion_loader = GestionSNELoader(
                schema=PG_SCHEMA_GESTION,
                table=PG_TABLE_GESTION,
                batch_size=PG_BATCH_SIZE,
                revisor_default_int=GESTION_REVISOR_DEFAULT_INT,
            )
            total_gestion = gestion_loader.upsert_from_ics_ids(conn, df_sne)
            print(f"✅ sne.gestion_sne actualizado para {total_gestion} ids (únicos)")

            print("\n" + "=" * 80)
            print("6) CARGANDO sne.ics_motivo_resp (id_ics único + motivo/responsable)")
            print("=" * 80)

            motivo_resp_loader = IcsMotivoRespLoader(
                schema_motivo_resp=PG_SCHEMA_MOTIVO_RESP,
                table_motivo_resp=PG_TABLE_MOTIVO_RESP,
                schema_motivos=PG_SCHEMA_MOTIVOS,
                table_motivos=PG_TABLE_MOTIVOS,
                batch_size=PG_BATCH_SIZE,
                responsable_default=RESPONSABLE_DEFAULT,
            )
            total_motivo_resp = motivo_resp_loader.upsert_from_df(conn, df_sne)
            print(f"✅ sne.ics_motivo_resp upsert: {total_motivo_resp} filas")

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

            # OJO: fecha_dt siempre debería existir acá, pero lo defendemos:
            fecha_log = fecha_dt.date() if fecha_dt else fecha_semilla

            logger.write_log(
                conn=conn,
                id_reporte=id_reporte,
                fecha_reporte_date=fecha_log,
                estado=estado,
                ultima_ejecucion_ts=end_ts,
                duracion_seg=duracion_seg,
                archivos_total=archivos_total,
                archivos_ok=archivos_ok,
                archivos_error=archivos_error,
                registros_proce=registros_proce,
                fecha_actualizacion_ts=end_ts,
            )

            conn.commit()

            print(f"📌 log: {PG_SCHEMA_LOG}.{PG_TABLE_LOG} | id_reporte={id_reporte} | fecha={fecha_log} | estado={estado}")
            print(f"⏱️ duración_seg={duracion_seg} | registros_proce={registros_proce}")

    print("\n" + "=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)

if __name__ == "__main__":
    main()