"""Writers for parquet datasets and optional JDBC loads."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame

from .config import JdbcConfig
from .utils import ensure_dir

LOGGER = logging.getLogger("fec.spark.publish")


def write_parquet(df: DataFrame, path: Path, partition_by: Optional[str] = None) -> None:
    ensure_dir(path.parent)
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    LOGGER.info("Writing parquet to %s", path)
    writer.parquet(str(path))


def write_jdbc(df: DataFrame, table: str, jdbc: JdbcConfig) -> None:
    options: Dict[str, str] = {
        "url": jdbc.url,
        "user": jdbc.user,
        "password": jdbc.password,
        "driver": jdbc.driver,
        "dbtable": table,
    }
    options.update(jdbc.options)
    LOGGER.info("Loading dataframe into %s via JDBC", table)
    (
        df.write.mode("overwrite")
        .format("jdbc")
        .options(**options)
        .save()
    )
