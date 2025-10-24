"""Configuration helpers for the FEC Spark pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass
class JdbcConfig:
    url: str
    user: str
    password: str
    driver: str = "org.postgresql.Driver"
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    raw_root: Path
    staging_root: Path
    warehouse_root: Path
    cycles: List[int]
    schedule_a_dir: Path
    schedule_a_header: Optional[Path] = None
    schedule_b_header: Optional[Path] = None
    schedule_e_header: Optional[Path] = None
    jdbc: Optional[JdbcConfig] = None
    spark_conf: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        cycles: Optional[Iterable[int]] = None,
        raw_root: Optional[str] = None,
        staging_root: Optional[str] = None,
        warehouse_root: Optional[str] = None,
    ) -> "PipelineConfig":
        env_cycles = os.environ.get("FEC_PIPELINE_CYCLES")
        if cycles is None and env_cycles:
            cycles = [int(x) for x in env_cycles.split(",") if x.strip()]
        if cycles is None:
            # Default to most recent three even cycles.
            from datetime import datetime

            year = datetime.utcnow().year
            even = year if year % 2 == 0 else year - 1
            cycles = [even, even - 2, even - 4]

        raw_root_path = Path(raw_root or os.environ.get("FEC_RAW_ROOT", "FEC_Data")).resolve()
        staging_path = Path(staging_root or os.environ.get("FEC_STAGING_ROOT", "data/staging")).resolve()
        warehouse_path = Path(
            warehouse_root or os.environ.get("FEC_WAREHOUSE_ROOT", "data/warehouse")
        ).resolve()

        schedule_a_dir = _resolve_schedule_a_dir(raw_root_path, os.environ.get("FEC_SCHEDULE_A_DIR"))
        schedule_a_header = _maybe_path(os.environ.get("FEC_SCHEDULE_A_HEADER"))
        if schedule_a_header is None:
            schedule_a_header = _default_schedule_a_header(schedule_a_dir)

        jdbc_url = os.environ.get("FEC_JDBC_URL")
        jdbc = None
        if jdbc_url:
            jdbc = JdbcConfig(
                url=jdbc_url,
                user=os.environ["FEC_JDBC_USER"],
                password=os.environ["FEC_JDBC_PASSWORD"],
                driver=os.environ.get("FEC_JDBC_DRIVER", "org.postgresql.Driver"),
            )

        default_conf = {
            "spark.sql.shuffle.partitions": os.environ.get("FEC_SPARK_SHUFFLE_PARTITIONS", "8"),
            "spark.sql.caseSensitive": "false",
            "spark.sql.legacy.timeParserPolicy": "LEGACY",
        }

        return cls(
            raw_root=raw_root_path,
            staging_root=staging_path,
            warehouse_root=warehouse_path,
            cycles=list(cycles),
            schedule_a_dir=schedule_a_dir,
            schedule_a_header=schedule_a_header,
            schedule_b_header=_maybe_path(os.environ.get("FEC_SCHEDULE_B_HEADER")),
            schedule_e_header=_maybe_path(os.environ.get("FEC_SCHEDULE_E_HEADER")),
            jdbc=jdbc,
            spark_conf=default_conf,
        )


def _maybe_path(value: Optional[str]) -> Optional[Path]:
    if not value:
        return None
    return Path(value).resolve()


def _resolve_schedule_a_dir(raw_root: Path, override: Optional[str]) -> Path:
    if override:
        candidate = Path(override).resolve()
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"FEC_SCHEDULE_A_DIR {candidate} does not exist")

    default = (raw_root / "schedule_a").resolve()
    if default.exists():
        return default

    alt = (raw_root / "scheduleA").resolve()
    if alt.exists():
        return alt

    # If neither exists yet, return default path so the pipeline can create it or fail later.
    return default


def _default_schedule_a_header(schedule_a_dir: Path) -> Optional[Path]:
    candidates = [
        schedule_a_dir / "indiv_header.csv",
        schedule_a_dir / "schedule_a_header.csv",
    ]
    for path in candidates:
        if path.exists():
            return path.resolve()
    return None
