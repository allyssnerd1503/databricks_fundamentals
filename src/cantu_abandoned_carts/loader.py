from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg
import pyarrow.parquet as pq
from psycopg import Connection

SCHEMA_FILES = ("01_schema.sql", "02_views.sql")

TABLE_LOADS = [
    ("tb_regions", "csv", "tb_regions.csv", ["PK", "p_isocode", "p_isocodeshort"]),
    ("tb_paymentmodes", "csv", "tb_paymentmodes.csv", ["PK", "p_code"]),
    ("tb_cmssitelp", "csv", "tb_cmssitelp.csv", ["ITEMPK", "p_name"]),
    ("tb_paymentinfos", "parquet", "tb_paymentinfos.parquet", ["PK", "p_installments"]),
    ("tb_users", "csv", "tb_users.csv", ["PK", "p_uid"]),
    ("tb_addresses", "parquet", "tb_addresses.parquet", ["PK", "p_region", "p_postalcode"]),
    (
        "tb_carts",
        "parquet",
        "tb_carts.parquet",
        ["PK", "createdTS", "p_totalprice", "p_paymentaddress", "p_paymentinfo", "p_paymentmode", "p_user", "p_site"],
    ),
    (
        "tb_cartentries",
        "parquet",
        "tb_cartentries.parquet",
        ["PK", "createdTS", "p_order", "p_entrynumber", "p_product", "p_quantity", "p_totalprice"],
    ),
]

COLUMN_RENAMES = {
    "PK": "pk",
    "ITEMPK": "itempk",
    "createdTS": "createdts",
}

BIGINT_COLUMNS = {
    "pk",
    "itempk",
    "p_region",
    "p_paymentaddress",
    "p_paymentinfo",
    "p_paymentmode",
    "p_user",
    "p_site",
    "p_order",
    "p_product",
}

INTEGER_COLUMNS = {"p_installments", "p_entrynumber"}
NUMERIC_COLUMNS = {"p_totalprice", "p_quantity"}
TIMESTAMP_COLUMNS = {"createdts"}
TABLE_KEYS = {"tb_cmssitelp": "itempk"}


def table_key(table: str) -> str:
    return TABLE_KEYS.get(table, "pk")


def apply_schema(conn: Connection, sql_dir: Path) -> None:
    with conn.cursor() as cur:
        for filename in SCHEMA_FILES:
            cur.execute((sql_dir / filename).read_text(encoding="utf-8"))
    conn.commit()


def load_all(conn: Connection, data_dir: Path, batch_size: int = 100_000) -> None:
    for table, file_type, filename, columns in TABLE_LOADS:
        path = data_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {path}")
        print(f"Carregando {table} a partir de {path}...", flush=True)
        if file_type == "csv":
            load_csv(conn, table, path, columns, batch_size)
        else:
            load_parquet(conn, table, path, columns, batch_size)
        conn.commit()


def read_pipe_csv(path: Path, columns: list[str], batch_size: int):
    return pd.read_csv(
        path,
        sep="|",
        quotechar='"',
        usecols=columns,
        chunksize=batch_size,
        dtype="string",
        engine="python",
        on_bad_lines="warn",
    )


def load_csv(conn: Connection, table: str, path: Path, columns: list[str], batch_size: int) -> None:
    rows_loaded = 0
    for frame in read_pipe_csv(path, columns, batch_size):
        copy_frame(conn, table, normalize_frame(frame))
        rows_loaded += len(frame)
        print(f"  {table}: {rows_loaded:,} linhas processadas", flush=True)


def load_parquet(conn: Connection, table: str, path: Path, columns: list[str], batch_size: int) -> None:
    rows_loaded = 0
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        frame = batch.to_pandas()
        copy_frame(conn, table, normalize_frame(frame))
        rows_loaded += len(frame)
        print(f"  {table}: {rows_loaded:,} linhas processadas", flush=True)


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.rename(columns=COLUMN_RENAMES)
    frame.columns = [column.lower() for column in frame.columns]

    for column in frame.columns:
        if column in BIGINT_COLUMNS or column in INTEGER_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
        elif column in NUMERIC_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        elif column in TIMESTAMP_COLUMNS:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
        else:
            frame[column] = frame[column].astype("string")

    return frame.where(pd.notna(frame), None)


def copy_frame(conn: Connection, table: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return

    columns = list(frame.columns)
    key = table_key(table)
    temp_table = f"tmp_load_{table}"
    column_sql = ", ".join(columns)
    csv_buffer = io.StringIO()
    frame.to_csv(
        csv_buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
        quoting=csv.QUOTE_MINIMAL,
        date_format="%Y-%m-%d %H:%M:%S.%f",
    )
    csv_buffer.seek(0)

    copy_sql = f"copy {temp_table} ({column_sql}) from stdin with (format csv, delimiter E'\\t', null '\\N')"
    insert_sql = f"""
        insert into {table} ({column_sql})
        select distinct on ({key}) {column_sql}
        from {temp_table}
        where {key} is not null
        order by {key}
        on conflict ({key}) do nothing
    """

    with conn.cursor() as cur:
        cur.execute(f"drop table if exists {temp_table}")
        cur.execute(f"create temp table {temp_table} (like {table} including defaults) on commit drop")
        with cur.copy(copy_sql) as copy:
            copy.write(csv_buffer.getvalue())
        cur.execute(insert_sql)
        cur.execute(f"drop table if exists {temp_table}")


def table_counts(conn: Connection) -> Iterable[tuple[str, int]]:
    tables = [table for table, *_ in TABLE_LOADS]
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"select count(*) from {table}")
            yield table, cur.fetchone()[0]


def connect_from_dsn(dsn: str) -> Connection:
    return psycopg.connect(dsn)
