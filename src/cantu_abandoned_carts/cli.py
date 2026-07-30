from __future__ import annotations

import argparse

from .config import PostgresConfig
from .loader import apply_schema, load_all, table_counts
from .pipeline import ProjectPaths, connect, generate_reports, run


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-dir", default="doc", help="Diretorio com os arquivos CSV/Parquet da prova.")
    parser.add_argument("--output-dir", default="output", help="Diretorio de saida dos relatorios.")
    parser.add_argument("--sql-dir", default="sql/postgres", help="Diretorio com os scripts SQL do PostgreSQL.")
    parser.add_argument("--top-limit", type=int, default=50, help="Limite dos rankings exploratorios.")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Tamanho dos lotes de carga.")


def add_spark_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-dir", default="doc", help="Diretorio com os arquivos CSV/Parquet da prova.")
    parser.add_argument("--output-dir", default="output", help="Diretorio de saida dos relatorios.")
    parser.add_argument("--top-limit", type=int, default=50, help="Limite dos rankings exploratorios.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline CantuStore com PostgreSQL.")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Recria schema, carrega dados e gera relatorios.")
    add_common_args(run_parser)

    load_parser = subparsers.add_parser("load", help="Recria schema e carrega os dados no PostgreSQL.")
    add_common_args(load_parser)

    reports_parser = subparsers.add_parser("reports", help="Gera relatorios usando dados ja carregados.")
    add_common_args(reports_parser)

    spark_parser = subparsers.add_parser("spark-run", help="Gera relatorios com PySpark, ideal para Databricks.")
    add_spark_args(spark_parser)

    subparsers.add_parser("counts", help="Mostra a quantidade de linhas por tabela carregada.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    command = args.command or "run"

    if command == "run":
        paths = ProjectPaths.from_strings(args.data_dir, args.output_dir, args.sql_dir)
        run(paths, top_limit=args.top_limit, batch_size=args.batch_size)
        print(f"Carga e relatorios concluidos em: {args.output_dir}")
        return

    if command == "spark-run":
        from pyspark.sql import SparkSession

        from . import spark_pipeline

        spark = SparkSession.builder.appName("cantu-abandoned-carts").getOrCreate()
        spark_pipeline.run(spark, data_dir=args.data_dir, output_dir=args.output_dir, top_limit=args.top_limit)
        print(f"Relatorios Spark gerados em: {args.output_dir}")
        return

    with connect(PostgresConfig.from_env()) as conn:
        if command == "load":
            paths = ProjectPaths.from_strings(args.data_dir, args.output_dir, args.sql_dir)
            apply_schema(conn, paths.sql_dir)
            load_all(conn, paths.data_dir, batch_size=args.batch_size)
            print("Carga concluida no PostgreSQL.")
        elif command == "reports":
            generate_reports(conn, ProjectPaths.from_strings(args.data_dir, args.output_dir, args.sql_dir).output_dir, args.top_limit)
            print(f"Relatorios gerados em: {args.output_dir}")
        elif command == "counts":
            for table, count in table_counts(conn):
                print(f"{table}: {count}")
        else:
            parser.print_help()


if __name__ == "__main__":
    main()
