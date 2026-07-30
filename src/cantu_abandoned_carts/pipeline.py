from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import Connection

from . import queries
from .config import PostgresConfig
from .loader import apply_schema, connect_from_dsn, load_all


@dataclass(frozen=True)
class ProjectPaths:
    data_dir: Path
    output_dir: Path
    sql_dir: Path

    @classmethod
    def from_strings(cls, data_dir: str, output_dir: str, sql_dir: str = "sql/postgres") -> "ProjectPaths":
        return cls(Path(data_dir).resolve(), Path(output_dir).resolve(), Path(sql_dir).resolve())


def connect(config: PostgresConfig | None = None) -> Connection:
    return connect_from_dsn((config or PostgresConfig.from_env()).dsn())


def copy_chunk_to_text(data: str | bytes | memoryview) -> str:
    if isinstance(data, str):
        return data
    if isinstance(data, memoryview):
        return data.tobytes().decode("utf-8")
    return data.decode("utf-8")


def write_csv(conn: Connection, sql: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        with conn.cursor() as cur:
            with cur.copy(f"copy ({sql}) to stdout with (format csv, header true)") as copy:
                for data in copy:
                    file.write(copy_chunk_to_text(data))


def write_pipe_export(conn: Connection, path: Path, limit: int = 50) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(queries.TOP_CARTS_EXPORT.format(limit=limit)).fetchall()

    current_cart = None
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, delimiter="|", lineterminator="\n")
        for row in rows:
            (
                cart_id,
                created_ts,
                cart_total_price,
                user_uid,
                payment_mode_code,
                installments,
                site_name,
                postal_code,
                item_quantity_sum,
                item_count,
                product_id,
                quantity,
                total_price,
                _entry_number,
            ) = row

            if cart_id != current_cart:
                writer.writerow(
                    [
                        cart_id,
                        created_ts,
                        cart_total_price,
                        user_uid,
                        payment_mode_code,
                        installments,
                        site_name,
                        postal_code,
                        item_quantity_sum,
                        item_count,
                    ]
                )
                current_cart = cart_id

            if product_id is not None:
                writer.writerow([product_id, quantity, total_price, ""])


def generate_reports(conn: Connection, output_dir: Path, top_limit: int = 50) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    reports = [
        ("top_produtos_abandonados.csv", queries.TOP_ABANDONED_PRODUCTS.format(limit=top_limit)),
        ("top_duplas_produtos_abandonados.csv", queries.TOP_ABANDONED_PRODUCT_PAIRS.format(limit=top_limit)),
        ("produtos_com_aumento_abandono.csv", queries.PRODUCT_ABANDONMENT_INCREASE.format(limit=top_limit)),
        ("produtos_novos_primeiro_mes.csv", queries.NEW_PRODUCTS_FIRST_MONTH),
        ("abandonos_por_estado.csv", queries.ABANDONMENT_BY_STATE),
        ("relatorio_produtos_mes.csv", queries.MONTHLY_PRODUCT_REPORT),
        ("relatorio_diario.csv", queries.DAILY_REPORT),
    ]
    for filename, sql in reports:
        print(f"Gerando {filename}...", flush=True)
        write_csv(conn, sql, output_dir / filename)
        print(f"  {filename} concluido", flush=True)

    print("Gerando top_50_carrinhos.txt...", flush=True)
    write_pipe_export(conn, output_dir / "top_50_carrinhos.txt", limit=50)
    print("  top_50_carrinhos.txt concluido", flush=True)


def load_database(conn: Connection, paths: ProjectPaths, batch_size: int = 100_000) -> None:
    apply_schema(conn, paths.sql_dir)
    load_all(conn, paths.data_dir, batch_size=batch_size)


def run(paths: ProjectPaths, top_limit: int = 50, batch_size: int = 100_000) -> None:
    with connect() as conn:
        load_database(conn, paths, batch_size=batch_size)
        generate_reports(conn, paths.output_dir, top_limit=top_limit)
