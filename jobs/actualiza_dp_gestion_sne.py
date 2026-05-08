from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional in server runtimes
    load_dotenv = None

CURRENT_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from database.database_manager import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None


DEFAULT_CONTAINER = "e02-transmitools"
DEFAULT_SCHEMA = "sne"
DEFAULT_TABLE = "gestion_sne"
DEFAULT_START_YEAR = 2026
AZURE_QUERY_CHUNK_SIZE = 100

PHASE_RANK = {
    "": 0,
    "etapa 1": 1,
    "etapa1": 1,
    "etapa 2": 2,
    "etapa2": 2,
    "etapa 3": 3,
    "etapa3": 3,
    "etapa 4": 4,
    "etapa4": 4,
    "cerrado": 5,
}

REQUIRED_GESTION_COLUMNS = {
    "id_ics",
    "estado_asignacion",
    "procesa_dp",
    "fase_dp_actual",
    "estado_dp_actual",
    "fecha_cambio_dp",
    "km_aceptado",
}


@dataclass(frozen=True)
class SourceFile:
    path: str
    stage: int | None
    last_modified: datetime | None = None


@dataclass
class PendingIcs:
    id_ics: str
    current_phase: str = ""
    fecha_inicio_dp: datetime | None = None
    fecha_cierre_dp: datetime | None = None

    @property
    def current_rank(self) -> int:
        return phase_rank(self.current_phase)


@dataclass
class IcsUpdate:
    id_ics: str
    fase_dp_actual: str
    estado_dp_actual: str
    fecha_cambio_dp: datetime
    phase_rank: int
    source_path: str
    km_aceptado: Decimal | None = None


def normalize_text(value) -> str:
    return str(value or "").strip()


def normalize_key(value) -> str:
    return normalize_text(value).lower().replace("_", " ")


def phase_rank(value) -> int:
    return PHASE_RANK.get(normalize_key(value), 0)


def normalize_id(value) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        number = Decimal(text.replace(",", "."))
        if number == number.to_integral_value():
            return str(int(number))
    except InvalidOperation:
        pass
    return text


def parse_decimal(value) -> Decimal | None:
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace("\u00a0", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def pick_col(row: dict[str, str], aliases: Iterable[str]) -> str | None:
    lookup = {normalize_key(k): k for k in row.keys()}
    for alias in aliases:
        key = lookup.get(normalize_key(alias))
        if key is not None:
            return key
    return None


def clean_row(row: dict) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        cleaned[str(key).strip().lstrip("\ufeff")] = "" if value is None else str(value).strip()
    return cleaned


def iter_csv_rows_from_bytes(content: bytes) -> Iterator[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            text = content.decode(encoding)
            sample = text[:4096]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel
            reader = csv.DictReader(io.StringIO(text), dialect=dialect)
            for row in reader:
                yield clean_row(row)
            return
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error


def stage_from_path(path: str) -> int | None:
    match = re.search(r"(?:^|/)(?:\d+_)?etapa\s*([1-4])(?:/|$)", path, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r"(?:^|/)\d+_etapa([1-4])(?:/|$)", path, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def blob_date_from_name(path: str) -> date | None:
    name = Path(path).name
    match = re.search(r"(\d{8})", name)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    match = re.search(r"(\d{6})", name)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m").date()
        except ValueError:
            return None
    return None


class BaseSource:
    def list_stage_files(self, years: list[int]) -> dict[int, list[SourceFile]]:
        raise NotImplementedError

    def list_closed_files(self, years: list[int]) -> list[SourceFile]:
        raise NotImplementedError

    def read_bytes(self, path: str) -> bytes:
        raise NotImplementedError

    def iter_matching_rows(self, path: str, active_ids: set[str]) -> Iterator[dict[str, str]]:
        for row in iter_csv_rows_from_bytes(self.read_bytes(path)):
            id_col = pick_col(row, ["IdICS", "Id_ICS", "id_ics"])
            if not id_col:
                continue
            if normalize_id(row.get(id_col)) in active_ids:
                yield row


class LocalSource(BaseSource):
    def __init__(self, root: Path):
        self.root = root

    def list_stage_files(self, years: list[int]) -> dict[int, list[SourceFile]]:
        base = self.root / "10_ics_etapas"
        out = {1: [], 2: [], 3: [], 4: []}
        if not base.exists():
            return out
        for file_path in sorted(base.rglob("*.csv")):
            rel = file_path.relative_to(self.root).as_posix()
            stage = stage_from_path(rel)
            if stage in out:
                out[stage].append(SourceFile(str(file_path), stage, datetime.fromtimestamp(file_path.stat().st_mtime)))
        return out

    def list_closed_files(self, years: list[int]) -> list[SourceFile]:
        base = self.root / "20_ics_cerrado"
        if not base.exists():
            return []
        return [
            SourceFile(str(file_path), None, datetime.fromtimestamp(file_path.stat().st_mtime))
            for file_path in sorted(base.rglob("*.csv"))
        ]

    def read_bytes(self, path: str) -> bytes:
        return Path(path).read_bytes()


class AzureSource(BaseSource):
    def __init__(self, connection_string: str, container: str, date_from: date | None = None, date_to: date | None = None):
        try:
            from azure.storage.blob import BlobServiceClient
        except Exception as exc:  # pragma: no cover - depends on runtime
            raise SystemExit("Falta instalar azure-storage-blob.") from exc
        self.service = BlobServiceClient.from_connection_string(connection_string)
        self.container = self.service.get_container_client(container)
        self.date_from = date_from
        self.date_to = date_to

    def _list_files(self, prefix: str) -> list[SourceFile]:
        files: list[SourceFile] = []
        print(f"Listando Azure: {prefix}", flush=True)
        blobs = retry_azure(lambda: list(self.container.list_blobs(name_starts_with=prefix)), f"listar {prefix}")
        for blob in blobs:
            if not blob.name or blob.name.endswith("/") or not blob.name.lower().endswith(".csv"):
                continue
            files.append(SourceFile(blob.name, stage_from_path(blob.name), blob.last_modified))
        return sorted(files, key=lambda item: (item.last_modified or datetime.min, item.path))

    def list_stage_files(self, years: list[int]) -> dict[int, list[SourceFile]]:
        out = {1: [], 2: [], 3: [], 4: []}
        if self.date_from and self.date_to:
            folders = {1: "10_etapa1", 2: "20_etapa2", 3: "30_etapa3", 4: "40_etapa4"}
            for year in years:
                for stage, folder in folders.items():
                    prefix = f"{year}/11_ics_offline/10_ics_etapas/{folder}/"
                    for file_info in self._list_files(prefix):
                        file_date = blob_date_from_name(file_info.path)
                        if file_date and self.date_from <= file_date <= self.date_to:
                            out[stage].append(file_info)
            return out
        for year in years:
            prefix = f"{year}/11_ics_offline/10_ics_etapas/"
            for file_info in self._list_files(prefix):
                if file_info.stage in out:
                    out[file_info.stage].append(file_info)
        return out

    def list_closed_files(self, years: list[int]) -> list[SourceFile]:
        files: list[SourceFile] = []
        if self.date_from and self.date_to:
            start_month = date(self.date_from.year, self.date_from.month, 1)
            end_month = date(self.date_to.year, self.date_to.month, 1)
            for year in years:
                prefix = f"{year}/10_ics/20_ics_cerrado/"
                for file_info in self._list_files(prefix):
                    file_month = blob_date_from_name(file_info.path)
                    if file_month and start_month <= file_month <= end_month:
                        files.append(file_info)
            return files
        for year in years:
            prefix = f"{year}/10_ics/20_ics_cerrado/"
            files.extend(self._list_files(prefix))
        return files

    def exists(self, path: str) -> bool:
        try:
            retry_azure(lambda: self.container.get_blob_client(path).get_blob_properties(), f"validar {path}", attempts=2, delay_seconds=2)
            return True
        except Exception:
            return False

    def read_bytes(self, path: str) -> bytes:
        print(f"Leyendo Azure: {path}", flush=True)
        return retry_azure(
            lambda: self.container.get_blob_client(path).download_blob(
                timeout=600,
                max_concurrency=8,
                validate_content=False,
            ).readall(),
            f"descargar {path}",
        )

    def iter_matching_rows(self, path: str, active_ids: set[str]) -> Iterator[dict[str, str]]:
        if not active_ids:
            return
        try:
            yield from self._query_matching_rows(path, active_ids)
            return
        except Exception as exc:
            print(f"Azure Query falló para {path}; usando descarga completa. Detalle: {exc}", flush=True)
        yield from super().iter_matching_rows(path, active_ids)

    def _query_matching_rows(self, path: str, active_ids: set[str]) -> Iterator[dict[str, str]]:
        try:
            from azure.storage.blob import DelimitedTextDialect
        except Exception as exc:  # pragma: no cover - depends on runtime
            raise SystemExit("Falta azure-storage-blob con soporte de Blob Query.") from exc

        ids = sorted(active_ids)
        blob = self.container.get_blob_client(path)
        input_format = DelimitedTextDialect(delimiter=",", quotechar='"', lineterminator="\n", has_header=True)
        output_format = DelimitedTextDialect(delimiter=",", quotechar='"', lineterminator="\n", has_header=True)

        def _run_query(chunk: list[str], offset: int, total_ids: int):
            literals = ",".join(sql_literal(item) for item in chunk)
            query = f"SELECT * FROM BlobStorage WHERE IdICS IN ({literals})"
            print(f"Consultando Azure Query: {path} | ids {offset + 1}-{offset + len(chunk)} de {total_ids}", flush=True)
            return retry_azure(
                lambda: blob.query_blob(
                    query,
                    blob_format=input_format,
                    output_format=output_format,
                    timeout=180,
                ).readall(),
                f"query {path}",
            )

        for start in range(0, len(ids), AZURE_QUERY_CHUNK_SIZE):
            chunk = ids[start:start + AZURE_QUERY_CHUNK_SIZE]
            try:
                data = _run_query(chunk, start, len(ids))
                payloads = [data]
            except Exception as exc:
                msg = str(exc).lower()
                if "syntax error in query" not in msg and "query is invalid" not in msg and "out of memory" not in msg:
                    raise
                payloads = []
                sub_size = min(25, len(chunk)) or 1
                print(f"Azure Query grande no aceptado para {path}; reintentando en sub-bloques de {sub_size} ids.", flush=True)
                for sub_start in range(0, len(chunk), sub_size):
                    sub_chunk = chunk[sub_start:sub_start + sub_size]
                    sub_data = _run_query(sub_chunk, start + sub_start, len(ids))
                    payloads.append(sub_data)

            for data in payloads:
                if not data:
                    continue
                for row in iter_csv_rows_from_bytes(data):
                    id_col = pick_col(row, ["IdICS", "Id_ICS", "id_ics"])
                    if id_col and normalize_id(row.get(id_col)) in active_ids:
                        yield row


def retry_azure(action, label: str, attempts: int = 4, delay_seconds: int = 5):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if attempt == attempts:
                break
            print(f"Azure intento {attempt}/{attempts} falló al {label}; reintentando en {delay_seconds}s...")
            time.sleep(delay_seconds)
    raise last_error


def sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def get_azure_connection_string() -> str:
    for env_name in ("AZURE_STORAGE_CONNECTION_STRING", "AZURE_CONN_STRING_DATOSCENTROINFORMACION"):
        value = os.environ.get(env_name, "").strip()
        if value:
            return value
    raise SystemExit(
        "Falta connection string de Azure. Define AZURE_STORAGE_CONNECTION_STRING "
        "o AZURE_CONN_STRING_DATOSCENTROINFORMACION."
    )

def open_db_connection():
    if get_db_connection is not None:
        return get_db_connection()

    try:
        import psycopg2
    except Exception as exc:  # pragma: no cover - depends on runtime
        raise SystemExit("Falta instalar psycopg2 o psycopg2-binary.") from exc

    database_url = (
        os.environ.get("POSTGRES_CONN_STRING", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
    )
    connect_timeout = int(os.environ.get("PGCONNECT_TIMEOUT", "20"))
    if database_url:
        return psycopg2.connect(database_url, connect_timeout=connect_timeout)
    else:
        required = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise SystemExit(
                "No encontré database_manager ni variables de Postgres. "
                f"Faltan: {', '.join(missing)}. También puedes usar DATABASE_URL."
            )
        return psycopg2.connect(
            host=os.environ["PGHOST"],
            port=int(os.environ.get("PGPORT", "5432")),
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            sslmode=os.environ.get("PGSSLMODE", "prefer"),
            connect_timeout=connect_timeout,
        )


def validate_gestion_columns(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lower(column_name)
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        columns = {row[0] for row in cur.fetchall()}
    missing = sorted(REQUIRED_GESTION_COLUMNS - columns)
    if missing:
        raise SystemExit(
            f"Faltan columnas en {schema}.{table}: {', '.join(missing)}. "
            "Crea esas columnas antes de ejecutar el update."
        )


def load_pending_from_db(conn, schema: str, table: str) -> dict[str, PendingIcs]:
    sql = f"""
        SELECT
            "id_ics"::text,
            COALESCE("fase_dp_actual"::text, ''),
            "fecha_inicio_dp",
            "fecha_cierre_dp"
        FROM "{schema}"."{table}"
        WHERE "estado_asignacion" = 1
          AND COALESCE(lower("fase_dp_actual"::text), '') <> 'cerrado'
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {
        normalize_id(id_ics): PendingIcs(normalize_id(id_ics), fase, fecha_inicio, fecha_cierre)
        for id_ics, fase, fecha_inicio, fecha_cierre in rows
        if normalize_id(id_ics)
    }


def load_pending_from_args(values: list[str]) -> dict[str, PendingIcs]:
    pending: dict[str, PendingIcs] = {}
    for raw in values:
        text = normalize_text(raw)
        if not text:
            continue
        if ":" in text:
            id_part, phase_part = text.split(":", 1)
        else:
            id_part, phase_part = text, ""
        id_ics = normalize_id(id_part)
        if id_ics:
            pending[id_ics] = PendingIcs(id_ics, normalize_text(phase_part))
    return pending


def load_pending_from_file(path: Path) -> dict[str, PendingIcs]:
    pending: dict[str, PendingIcs] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        if "," in sample or ";" in sample or "\t" in sample:
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel
            reader = csv.DictReader(handle, dialect=dialect)
            for row in reader:
                clean = clean_row(row)
                id_col = pick_col(clean, ["id_ics", "IdICS", "Id_ICS"])
                phase_col = pick_col(clean, ["fase_dp_actual", "fase", "etapa"])
                if not id_col:
                    continue
                id_ics = normalize_id(clean.get(id_col))
                if id_ics:
                    pending[id_ics] = PendingIcs(id_ics, clean.get(phase_col, "") if phase_col else "")
        else:
            for line in handle:
                id_ics = normalize_id(line)
                if id_ics:
                    pending[id_ics] = PendingIcs(id_ics, "")
    return pending


def process_stage_file(
    source: BaseSource,
    file_info: SourceFile,
    stage: int,
    active_ids: set[str],
    run_timestamp: datetime,
) -> dict[str, IcsUpdate]:
    found: dict[str, IcsUpdate] = {}
    if not active_ids:
        return found

    for row in source.iter_matching_rows(file_info.path, active_ids):
        id_col = pick_col(row, ["IdICS", "Id_ICS", "id_ics"])
        estado_col = pick_col(row, ["Estado", "estado"])
        if not id_col or not estado_col:
            continue
        id_ics = normalize_id(row.get(id_col))
        if id_ics not in active_ids:
            continue
        estado = normalize_text(row.get(estado_col))
        if not estado:
            continue
        found[id_ics] = IcsUpdate(
            id_ics=id_ics,
            fase_dp_actual=f"etapa {stage}",
            estado_dp_actual=estado,
            fecha_cambio_dp=run_timestamp,
            phase_rank=stage,
            source_path=file_info.path,
        )
    return found


def process_closed_file(
    source: BaseSource,
    file_info: SourceFile,
    active_ids: set[str],
    run_timestamp: datetime,
) -> dict[str, IcsUpdate]:
    found: dict[str, IcsUpdate] = {}
    if not active_ids:
        return found

    for row in source.iter_matching_rows(file_info.path, active_ids):
        id_col = pick_col(row, ["IdICS", "Id_ICS", "id_ics"])
        estado_col = pick_col(row, ["Estado", "estado"])
        km_def_col = pick_col(row, ["KM Definitivo", "Km Definitivo", "km_definitivo"])
        km_exec_col = pick_col(row, ["Km Efectivamente Ejecutado", "km_efectivamente_ejecutado"])
        if not id_col or not estado_col:
            continue
        id_ics = normalize_id(row.get(id_col))
        if id_ics not in active_ids:
            continue
        estado = normalize_text(row.get(estado_col))
        km_def = parse_decimal(row.get(km_def_col)) if km_def_col else None
        km_exec = parse_decimal(row.get(km_exec_col)) if km_exec_col else None
        km_aceptado = None
        if km_def is not None and km_exec is not None:
            km_aceptado = km_def - km_exec
        found[id_ics] = IcsUpdate(
            id_ics=id_ics,
            fase_dp_actual="cerrado",
            estado_dp_actual=estado,
            fecha_cambio_dp=run_timestamp,
            phase_rank=5,
            source_path=file_info.path,
            km_aceptado=km_aceptado,
        )
        if len(found) >= len(active_ids):
            break
    return found


def find_updates(
    source: BaseSource,
    pending: dict[str, PendingIcs],
    years: list[int],
    run_timestamp: datetime,
) -> dict[str, IcsUpdate]:
    updates: dict[str, IcsUpdate] = {}
    current_rank = {id_ics: item.current_rank for id_ics, item in pending.items()}

    active_closed = {id_ics for id_ics, rank in current_rank.items() if rank < 5}
    closed_updates: dict[str, IcsUpdate] = {}
    for file_info in sorted(source.list_closed_files(years), key=lambda item: (item.last_modified or datetime.min, item.path), reverse=True):
        found = process_closed_file(source, file_info, active_closed, run_timestamp)
        for id_ics, update in found.items():
            if id_ics not in closed_updates:
                closed_updates[id_ics] = update
        active_closed.difference_update(found.keys())
        if not active_closed:
            break
    for id_ics, update in closed_updates.items():
        updates[id_ics] = update
        current_rank[id_ics] = update.phase_rank

    if current_rank and all(rank >= 5 for rank in current_rank.values()):
        return updates

    stage_files = source.list_stage_files(years)
    for stage in stage_files:
        stage_files[stage] = sorted(stage_files.get(stage, []), key=lambda item: (item.last_modified or datetime.min, item.path), reverse=True)

    for stage in (4, 3, 2, 1):
        active_ids = {id_ics for id_ics, rank in current_rank.items() if rank < stage}
        if not active_ids:
            continue
        stage_updates: dict[str, IcsUpdate] = {}
        for file_info in stage_files.get(stage, []):
            found = process_stage_file(source, file_info, stage, active_ids, run_timestamp)
            for id_ics, update in found.items():
                if id_ics not in stage_updates:
                    stage_updates[id_ics] = update
            active_ids.difference_update(found.keys())
            if not active_ids:
                break
        for id_ics, update in stage_updates.items():
            if update.phase_rank > current_rank.get(id_ics, 0):
                updates[id_ics] = update
                current_rank[id_ics] = update.phase_rank

    return updates


def apply_updates(conn, schema: str, table: str, updates: list[IcsUpdate], batch_size: int) -> int:
    if not updates:
        return 0
    try:
        from psycopg2.extras import execute_values
    except Exception as exc:  # pragma: no cover - depends on runtime
        raise SystemExit("Falta psycopg2.extras.execute_values.") from exc

    full_table = f'"{schema}"."{table}"'
    sql = f"""
        UPDATE {full_table} AS g
        SET
            "procesa_dp" = 1,
            "fase_dp_actual" = v.fase_dp_actual::text,
            "estado_dp_actual" = v.estado_dp_actual::text,
            "fecha_cambio_dp" = v.fecha_cambio_dp::timestamp,
            "km_aceptado" = CASE
                WHEN v.phase_rank::int = 5 THEN NULLIF(v.km_aceptado::text, '')::numeric
                ELSE g."km_aceptado"
            END
        FROM (VALUES %s) AS v(
            id_ics,
            fase_dp_actual,
            estado_dp_actual,
            fecha_cambio_dp,
            km_aceptado,
            phase_rank
        )
        WHERE g."id_ics"::text = v.id_ics::text
          AND g."estado_asignacion" = 1
          AND (
                CASE lower(COALESCE(g."fase_dp_actual"::text, ''))
                    WHEN 'cerrado' THEN 5
                    WHEN 'etapa 4' THEN 4
                    WHEN 'etapa4' THEN 4
                    WHEN 'etapa 3' THEN 3
                    WHEN 'etapa3' THEN 3
                    WHEN 'etapa 2' THEN 2
                    WHEN 'etapa2' THEN 2
                    WHEN 'etapa 1' THEN 1
                    WHEN 'etapa1' THEN 1
                    ELSE 0
                END
          ) < v.phase_rank::int
    """
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(updates), batch_size):
            chunk = updates[start:start + batch_size]
            records = [
                (
                    item.id_ics,
                    item.fase_dp_actual,
                    item.estado_dp_actual,
                    item.fecha_cambio_dp,
                    item.km_aceptado,
                    item.phase_rank,
                )
                for item in chunk
            ]
            execute_values(cur, sql, records, page_size=len(records))
            total += cur.rowcount
    return total



def parse_iso_date(value: str) -> date | None:
    text = normalize_text(value)
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d").date()


def derive_date_window(
    pending: dict[str, PendingIcs],
    run_timestamp: datetime,
    explicit_from: date | None,
    explicit_to: date | None,
    days_before: int,
) -> tuple[date | None, date | None]:
    if explicit_from or explicit_to:
        return explicit_from, explicit_to or run_timestamp.date()

    dates: list[date] = []
    for item in pending.values():
        for dt in (item.fecha_inicio_dp, item.fecha_cierre_dp):
            if isinstance(dt, datetime):
                dates.append(dt.date())
    if not dates:
        return None, None

    start = min(dates) - timedelta(days=max(days_before, 0))
    end = run_timestamp.date()
    return start, end


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Actualiza sne.gestion_sne con fase/estado DP usando CSV de ICS en Azure."
    )
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--date-from", default="", help="Fecha inicial YYYY-MM-DD para generar rutas Azure por fecha.")
    parser.add_argument("--date-to", default="", help="Fecha final YYYY-MM-DD para generar rutas Azure por fecha.")
    parser.add_argument("--no-date-window", action="store_true", help="Desactiva ventana por fecha y lista todos los blobs del a?o.")
    parser.add_argument("--date-window-days-before", type=int, default=2, help="D?as antes de la fecha m?nima DP para iniciar b?squeda Azure.")
    parser.add_argument("--pending-id", action="append", default=[], help="ID manual para pruebas. Formato opcional: id:fase")
    parser.add_argument("--pending-ids-file", default="", help="CSV/TXT con id_ics y fase_dp_actual opcional.")
    parser.add_argument("--limit-pending", type=int, default=0, help="Limita cantidad de pendientes para prueba.")
    parser.add_argument("--dry-run", action="store_true", help="Solo valida y no actualiza Postgres.")
    return parser


def main() -> int:
    if load_dotenv:
        load_dotenv()

    args = build_arg_parser().parse_args()
    years = list(range(args.start_year, args.end_year + 1))
    run_timestamp = datetime.now()

    pending: dict[str, PendingIcs] = {}
    if args.pending_id:
        pending.update(load_pending_from_args(args.pending_id))
    if args.pending_ids_file:
        pending.update(load_pending_from_file(Path(args.pending_ids_file)))

    if not pending:
        with open_db_connection() as conn:
            validate_gestion_columns(conn, args.schema, args.table)
            pending = load_pending_from_db(conn, args.schema, args.table)

    if args.limit_pending and args.limit_pending > 0:
        pending = dict(list(pending.items())[:args.limit_pending])

    print(f"Pendientes a buscar: {len(pending):,}")
    print(f"Fuente: azure | años: {years[0]}-{years[-1]}")

    if not pending:
        print("No hay registros pendientes con estado_asignacion = 1.")
        return 0

    date_from = None if args.no_date_window else parse_iso_date(args.date_from)
    date_to = None if args.no_date_window else parse_iso_date(args.date_to)
    if not args.no_date_window:
        date_from, date_to = derive_date_window(
            pending,
            run_timestamp,
            date_from,
            date_to,
            args.date_window_days_before,
        )
        if date_from and date_to:
            print(f"Ventana Azure por fecha: {date_from} a {date_to}")
        else:
            print("Ventana Azure por fecha no disponible; se listarán blobs por prefijo anual.")

    source: BaseSource = AzureSource(get_azure_connection_string(), args.container, date_from=date_from, date_to=date_to)

    updates_by_id = find_updates(source, pending, years, run_timestamp)
    updates = sorted(updates_by_id.values(), key=lambda item: (item.phase_rank, item.id_ics))

    counts: dict[tuple[str, str], int] = {}
    for item in updates:
        key = (item.fase_dp_actual, item.estado_dp_actual)
        counts[key] = counts.get(key, 0) + 1

    print(f"Encontrados para actualizar: {len(updates):,}")
    for (fase, estado), count in sorted(counts.items(), key=lambda kv: (phase_rank(kv[0][0]), kv[0][1])):
        print(f"  {fase} | {estado}: {count:,}")
    if args.dry_run:
        print("Dry-run: no se actualizó Postgres. Ejecuta sin --dry-run para guardar cambios.")
        return 0

    with open_db_connection() as conn:
        validate_gestion_columns(conn, args.schema, args.table)
        total = apply_updates(conn, args.schema, args.table, updates, args.batch_size)
        conn.commit()
    print(f"Filas actualizadas en {args.schema}.{args.table}: {total:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
