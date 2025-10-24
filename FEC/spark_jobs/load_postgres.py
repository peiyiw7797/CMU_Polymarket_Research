"""Load existing parquet outputs into Postgres using Spark JDBC."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List

from pyspark.sql import SparkSession

from .config import JdbcConfig
from .publish_outputs import write_jdbc

LOGGER = logging.getLogger("fec.spark.load_postgres")

DEFAULT_TABLES = [
    "candidate_dim",
    "committee_dim",
    "candidate_committees",
    "candidate_cycle_summary",
    "candidate_committee_details",
    "top_contributors",
    "schedule_a_flat",
]


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load parquet datasets into Postgres via Spark JDBC")
    parser.add_argument("--warehouse-root", required=True, help="Path to parquet warehouse root")
    parser.add_argument("--jdbc-url", required=True, help="Postgres JDBC URL")
    parser.add_argument("--jdbc-user", required=True, help="Postgres username")
    parser.add_argument("--jdbc-password", required=True, help="Postgres password")
    parser.add_argument("--jdbc-driver", default="org.postgresql.Driver", help="JDBC driver class")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=DEFAULT_TABLES,
        help=f"Subset of tables to load (default: {', '.join(DEFAULT_TABLES)})",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(list(argv))


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_tables(warehouse_root: Path, jdbc: JdbcConfig, tables: List[str]) -> None:
    spark = SparkSession.builder.appName("fec-load-postgres").getOrCreate()
    try:
        for table in tables:
            path = (warehouse_root / table).resolve()
            if not path.exists():
                LOGGER.warning("Skipping %s; parquet path %s not found", table, path)
                continue
            LOGGER.info("Loading %s from %s", table, path)
            df = spark.read.parquet(str(path))
            write_jdbc(df, table, jdbc)
    finally:
        spark.stop()


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    configure_logging(args.log_level)
    warehouse_root = Path(args.warehouse_root).resolve()
    jdbc = JdbcConfig(
        url=args.jdbc_url,
        user=args.jdbc_user,
        password=args.jdbc_password,
        driver=args.jdbc_driver,
    )
    load_tables(warehouse_root, jdbc, args.tables)


if __name__ == "__main__":
    main()
