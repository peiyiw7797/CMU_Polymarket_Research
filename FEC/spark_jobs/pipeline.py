"""CLI entry point orchestrating the FEC Spark pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List
import os
from pyspark.sql import SparkSession

from . import build_facts
from .config import PipelineConfig
from .enrich_dimensions import build_candidate_committees, build_candidate_dim, build_committee_dim
from .ingest_master import load_candidate_committee_linkage, load_candidate_master, load_committee_master
from .ingest_schedule_a import build_schedule_a_flat, load_schedule_a_raw
from .publish_outputs import write_jdbc, write_parquet
from .schemas import SchemaRegistry
from .utils import configure_spark, ensure_dir

LOGGER = logging.getLogger("fec.spark.pipeline")


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FEC bulk data Spark pipeline")
    parser.add_argument("--cycles", nargs="*", type=int, help="Two-year cycles to process (e.g., 2024 2022)")
    parser.add_argument("--raw-root", default=None, help="Path to FEC bulk data directory")
    parser.add_argument("--staging-root", default=None, help="Path for intermediate parquet outputs")
    parser.add_argument("--warehouse-root", default=None, help="Path for curated parquet outputs")
    parser.add_argument("--schedule-a-header", default=None, help="Optional path to Schedule A header CSV")
    parser.add_argument("--schedule-b-header", default=None, help="Optional path to Schedule B header CSV")
    parser.add_argument("--schedule-e-header", default=None, help="Optional path to Schedule E header CSV")
    parser.add_argument("--jdbc-url", default=None, help="Postgres JDBC URL for loading curated tables")
    parser.add_argument("--jdbc-user", default=None, help="JDBC user")
    parser.add_argument("--jdbc-password", default=None, help="JDBC password")
    parser.add_argument("--jdbc-driver", default="org.postgresql.Driver", help="JDBC driver class")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(list(argv))


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_config(args: argparse.Namespace) -> PipelineConfig:
    config = PipelineConfig.from_env(
        cycles=args.cycles,
        raw_root=args.raw_root,
        staging_root=args.staging_root,
        warehouse_root=args.warehouse_root,
    )
    if args.schedule_a_header:
        config.schedule_a_header = Path(args.schedule_a_header).resolve()
    if args.schedule_b_header:
        config.schedule_b_header = Path(args.schedule_b_header).resolve()
    if args.schedule_e_header:
        config.schedule_e_header = Path(args.schedule_e_header).resolve()

    if args.jdbc_url and args.jdbc_user and args.jdbc_password:
        from .config import JdbcConfig

        config.jdbc = JdbcConfig(
            url=args.jdbc_url,
            user=args.jdbc_user,
            password=args.jdbc_password,
            driver=args.jdbc_driver,
        )
    return config


def run_pipeline(config: PipelineConfig) -> None:
    ensure_dir(config.staging_root)
    ensure_dir(config.warehouse_root)
    os.environ["PYSPARK_PYTHON"] = "/opt/anaconda3/bin/python3.13"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/anaconda3/bin/python3.13"

    builder = SparkSession.builder.appName("fec-spark-pipeline")
    spark = builder.getOrCreate()
    configure_spark(spark, config.spark_conf)
    registry = SchemaRegistry(config.raw_root)

    LOGGER.info("Loading master tables")
    candidate_master = load_candidate_master(spark, registry, config.raw_root, config.cycles)
    committee_master = load_committee_master(spark, registry, config.raw_root, config.cycles)
    linkage = load_candidate_committee_linkage(spark, registry, config.raw_root, config.cycles)

    candidate_dim = build_candidate_dim(candidate_master)
    committee_dim = build_committee_dim(committee_master)
    candidate_committees = build_candidate_committees(linkage, committee_dim, candidate_dim)

    LOGGER.info("Writing dimension tables to warehouse")
    write_parquet(candidate_dim, config.warehouse_root / "candidate_dim", partition_by="cycle")
    write_parquet(committee_dim, config.warehouse_root / "committee_dim", partition_by="cycle")
    write_parquet(candidate_committees, config.warehouse_root / "candidate_committees", partition_by="cycle")

    LOGGER.info("Loading Schedule A")
    schedule_a_raw = load_schedule_a_raw(
        spark,
        registry,
        config.schedule_a_dir,
        config.cycles,
        config.schedule_a_header,
    )
    schedule_a_flat = build_schedule_a_flat(schedule_a_raw, candidate_committees, candidate_dim, committee_dim)
    write_parquet(schedule_a_flat, config.warehouse_root / "schedule_a_flat", partition_by="two_year_transaction_period")

    LOGGER.info("Computing aggregated fact tables")
    candidate_summary = build_facts.compute_candidate_cycle_summary(schedule_a_flat)
    candidate_committee_details = build_facts.compute_candidate_committee_details(schedule_a_flat)
    top_contributors = build_facts.compute_top_contributors(schedule_a_flat)

    write_parquet(candidate_summary, config.warehouse_root / "candidate_cycle_summary", partition_by="cycle")
    write_parquet(
        candidate_committee_details,
        config.warehouse_root / "candidate_committee_details",
        partition_by="cycle",
    )
    write_parquet(top_contributors, config.warehouse_root / "top_contributors", partition_by="cycle")

    if config.jdbc:
        LOGGER.info("Loading curated tables into Postgres")
        write_jdbc(candidate_dim, "candidate", config.jdbc)
        write_jdbc(committee_dim, "committee", config.jdbc)
        write_jdbc(candidate_committees, "candidate_committee", config.jdbc)
        write_jdbc(candidate_summary, "candidate_cycle_summary", config.jdbc)
        write_jdbc(candidate_committee_details, "candidate_committee_details", config.jdbc)
        write_jdbc(top_contributors, "top_contributors", config.jdbc)

    spark.stop()


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    configure_logging(args.log_level)
    config = build_config(args)
    run_pipeline(config)


if __name__ == "__main__":
    main()
