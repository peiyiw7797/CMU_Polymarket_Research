"""Utility functions shared across Spark ETL jobs."""

from __future__ import annotations

import functools
import logging
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

LOGGER = logging.getLogger("fec.spark")

SPACE_PATTERN = re.compile(r"\s+")
PUNCT_PATTERN = re.compile(r"[^\w\s]")


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = unicodedata.normalize("NFKD", value)
    text = text.upper()
    text = SPACE_PATTERN.sub(" ", text)
    return text.strip()


def strip_punctuation(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return PUNCT_PATTERN.sub(" ", value)


def name_udf():
    return F.udf(normalize_text, StringType())


def normalized_column(col: str) -> F.Column:
    return name_udf()(F.col(col))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_candidate_name(name: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not name:
        return None, None, None, None, None
    normalized = normalize_text(name)
    parts = [p.strip() for p in normalized.split(",")]
    if len(parts) == 1:
        tokens = parts[0].split()
        first = tokens[0] if tokens else None
        last = tokens[-1] if len(tokens) > 1 else None
        middle = " ".join(tokens[1:-1]) if len(tokens) > 2 else None
        return first, middle, last, None, None
    last_part = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    rest_tokens = rest.split()
    prefix = rest_tokens[0] if rest_tokens and rest_tokens[0] in {"DR", "HON", "GOV", "SEN", "REP"} else None
    if prefix:
        rest_tokens = rest_tokens[1:]
    first = rest_tokens[0] if rest_tokens else None
    suffix = rest_tokens[-1] if rest_tokens and rest_tokens[-1] in {"JR", "SR", "II", "III", "IV"} else None
    if suffix:
        rest_tokens = rest_tokens[:-1]
    middle = " ".join(rest_tokens[1:]) if len(rest_tokens) > 1 else None
    return first, middle, last_part or None, prefix, suffix


@functools.lru_cache(maxsize=2048)
def candidate_name_tuple(name: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    return parse_candidate_name(name)


def add_candidate_name_parts(df: DataFrame, column: str = "CAND_NAME") -> DataFrame:
    parse = F.udf(candidate_name_tuple, "struct<first:string,middle:string,last:string,prefix:string,suffix:string>")
    parts = parse(F.col(column))
    return (
        df.withColumn("first_name", parts["first"])
        .withColumn("middle_name", parts["middle"])
        .withColumn("last_name", parts["last"])
        .withColumn("prefix", parts["prefix"])
        .withColumn("suffix", parts["suffix"])
    )


def union_all(dfs: Sequence[DataFrame]) -> DataFrame:
    head, *tail = dfs
    result = head
    for frame in tail:
        result = result.unionByName(frame)
    return result


def configure_spark(spark: SparkSession, conf: dict) -> None:
    for key, value in conf.items():
        spark.conf.set(key, value)
