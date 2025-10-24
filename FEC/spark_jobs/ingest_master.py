"""Ingestion routines for FEC master tables."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import SchemaRegistry
from .utils import add_candidate_name_parts, normalize_text, union_all

LOGGER = logging.getLogger("fec.spark.ingest")


def load_candidate_master(
    spark: SparkSession,
    registry: SchemaRegistry,
    raw_root: Path,
    cycles: Iterable[int],
) -> DataFrame:
    cycles = list(cycles)
    frames: List[DataFrame] = []
    for cycle in cycles:
        path = raw_root / "candidate_master" / f"candidate_master_{cycle}.txt"
        if not path.exists():
            LOGGER.warning("Missing candidate_master file for cycle %s at %s", cycle, path)
            continue
        schema = registry.candidate_master(cycle)
        df = (
            spark.read.options(delimiter="|")
            .schema(schema)
            .csv(str(path))
            .withColumn("cycle", F.lit(cycle))
        )
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No candidate_master files available for requested cycles.")
    combined = union_all(frames)
    enriched = add_candidate_name_parts(combined, column="CAND_NAME")
    normalize_udf = F.udf(normalize_text, "string")
    return enriched.withColumn("normalized_name", normalize_udf(F.col("CAND_NAME")))


def load_committee_master(
    spark: SparkSession,
    registry: SchemaRegistry,
    raw_root: Path,
    cycles: Iterable[int],
) -> DataFrame:
    cycles = list(cycles)
    frames: List[DataFrame] = []
    for cycle in cycles:
        path = raw_root / "committee_master" / f"committee_master_{cycle}.txt"
        if not path.exists():
            LOGGER.warning("Missing committee_master file for cycle %s at %s", cycle, path)
            continue
        schema = registry.committee_master(cycle)
        df = (
            spark.read.options(delimiter="|")
            .schema(schema)
            .csv(str(path))
            .withColumn("cycle", F.lit(cycle))
        )
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No committee_master files available for requested cycles.")
    return union_all(frames)


def load_candidate_committee_linkage(
    spark: SparkSession,
    registry: SchemaRegistry,
    raw_root: Path,
    cycles: Iterable[int],
) -> DataFrame:
    cycles = list(cycles)
    frames: List[DataFrame] = []
    for cycle in cycles:
        path = raw_root / "candidate_committee_linkage" / f"candidate_committee_linkage_{cycle}.txt"
        if not path.exists():
            LOGGER.warning("Missing candidate_committee_linkage file for cycle %s at %s", cycle, path)
            continue
        schema = registry.candidate_committee_linkage(cycle)
        df = (
            spark.read.options(delimiter="|")
            .schema(schema)
            .csv(str(path))
            .withColumn("cycle", F.lit(cycle))
        )
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No candidate_committee_linkage files available.")
    return union_all(frames)


def load_committees_to_candidates(
    spark: SparkSession,
    registry: SchemaRegistry,
    raw_root: Path,
    cycles: Iterable[int],
) -> DataFrame:
    cycles = list(cycles)
    frames: List[DataFrame] = []
    for cycle in cycles:
        path = raw_root / "committees_to_candidates" / f"committees_to_candidates_{cycle}.txt"
        if not path.exists():
            LOGGER.warning("Missing committees_to_candidates file for cycle %s at %s", cycle, path)
            continue
        schema = registry.committees_to_candidates(cycle)
        df = (
            spark.read.options(delimiter="|")
            .schema(schema)
            .csv(str(path))
            .withColumn("cycle", F.lit(cycle))
        )
        frames.append(df)
    if not frames:
        LOGGER.info("No committees_to_candidates files found; continuing without them.")
        schema = registry.committees_to_candidates(cycles[0])
        return spark.createDataFrame([], schema=schema)
    return union_all(frames)
