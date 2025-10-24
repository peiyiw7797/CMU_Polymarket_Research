"""Schedule A ingestion and enrichment."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import SchemaRegistry
from .utils import union_all

LOGGER = logging.getLogger("fec.spark.schedule_a")

SCHEDULE_A_COLUMN_ALIASES = {
    "cmte_id": "committee_id",
    "cmte_nm": "committee_name",
    "rpt_yr": "report_year",
    "rpt_tp": "report_type",
    "image_num": "image_number",
    "form_tp_cd": "filing_form",
    "line_num": "line_number",
    "tran_id": "transaction_id",
    "file_num": "file_number",
    "entity_tp": "entity_type",
    "entity_tp_desc": "entity_type_desc",
    "name": "contributor_name",
    "contbr_nm": "contributor_name",
    "contbr_first_nm": "contributor_first_name",
    "contbr_mid_nm": "contributor_middle_name",
    "contbr_last_nm": "contributor_last_name",
    "contbr_prefix": "contributor_prefix",
    "contbr_suffix": "contributor_suffix",
    "contbr_st1": "contributor_street_1",
    "contbr_st2": "contributor_street_2",
    "contbr_city": "contributor_city",
    "contbr_st": "contributor_state",
    "contbr_zip": "contributor_zip",
    "contbr_employer": "contributor_employer",
    "contbr_occupation": "contributor_occupation",
    "contbr_id": "contributor_id",
    "receipt_tp": "receipt_type",
    "receipt_desc": "receipt_type_desc",
    "receipt_desc_full": "receipt_type_full",
    "memo_cd": "memo_code",
    "memo_cd_desc": "memo_code_full",
    "memo_text": "memo_text",
    "contb_receipt_dt": "contribution_receipt_date",
    "transaction_dt": "contribution_receipt_date",
    "contb_receipt_amt": "contribution_receipt_amount",
    "transaction_amt": "contribution_receipt_amount",
    "contb_aggreg_amt": "contributor_aggregate_ytd",
    "cand_id": "candidate_id",
    "cmte_tp": "recipient_committee_type",
    "cmte_dsgn": "recipient_committee_designation",
    "cmte_org_tp": "recipient_committee_org_type",
    "back_ref_tran_id": "back_reference_transaction_id",
    "back_ref_sched_nm": "back_reference_schedule_name",
    "election_tp": "election_type",
    "election_tp_desc": "election_type_full",
    "fec_election_tp_desc": "fec_election_type_desc",
}


def load_schedule_a_raw(
    spark: SparkSession,
    registry: SchemaRegistry,
    schedule_a_dir: Path,
    cycles: Iterable[int],
    header_path: Optional[Path],
) -> DataFrame:
    cycles = list(cycles)
    schema = registry.schedule_a(header_path)
    frames: List[DataFrame] = []
    for cycle in cycles:
        matched = list(schedule_a_dir.rglob(f"itcont_{cycle}*.txt"))
        if not matched:
            LOGGER.warning(
                "No Schedule A files found for cycle %s under %s", cycle, schedule_a_dir
            )
            continue
        for file_path in matched:
            df = (
                spark.read.options(delimiter="|")
                .schema(schema)
                .csv(str(file_path))
                .withColumn("two_year_transaction_period", F.lit(cycle))
            )
            standardized = _standardize_schedule_a_columns(_lowercase_columns(df))
            frames.append(standardized)
    if not frames:
        raise FileNotFoundError("No Schedule A files available for requested cycles.")
    return union_all(frames)


def build_schedule_a_flat(
    raw_schedule_a: DataFrame,
    candidate_committees: DataFrame,
    candidate_dim: DataFrame,
    committee_dim: DataFrame,
) -> DataFrame:
    receipts = (
        raw_schedule_a.alias("sa")
        .join(
            candidate_committees.select(
                "cand_id", "cmte_id", "cycle", "role", "principal_flag"
            ).alias("cc"),
            (F.col("sa.committee_id") == F.col("cc.cmte_id"))
            & (F.col("sa.two_year_transaction_period") == F.col("cc.cycle")),
            "left",
        )
        .join(
            candidate_dim.select(
                "cand_id",
                "cycle",
                "display_name",
                "first_name",
                "middle_name",
                "last_name",
                "prefix",
                "suffix",
                "office",
                "state",
                "district",
            ).alias("cand"),
            (F.coalesce(F.col("cc.cand_id"), F.col("sa.candidate_id")) == F.col("cand.cand_id"))
            & (F.col("sa.two_year_transaction_period") == F.col("cand.cycle")),
            "left",
        )
        .join(
            committee_dim.select("cmte_id", "cycle", "committee_type", "org_type", "designation").alias("cm"),
            (F.col("sa.committee_id") == F.col("cm.cmte_id"))
            & (F.col("sa.two_year_transaction_period") == F.col("cm.cycle")),
            "left",
        )
    )

    numeric_amount = F.col("contribution_receipt_amount").cast("double")

    override_cols = {
        "candidate_id",
        "recipient_committee_type",
        "recipient_committee_org_type",
        "recipient_committee_designation",
        "candidate_name",
        "candidate_first_name",
        "candidate_last_name",
        "candidate_middle_name",
        "candidate_prefix",
        "candidate_suffix",
        "candidate_office",
        "candidate_office_state",
        "candidate_office_district",
        "contribution_receipt_amount",
    }
    base_columns = [F.col(f"sa.{col}") for col in raw_schedule_a.columns if col not in override_cols]
    return receipts.select(
        *base_columns,
        F.coalesce(F.col("sa.candidate_id"), F.col("cc.cand_id")).alias("candidate_id"),
        F.coalesce(F.col("sa.recipient_committee_type"), F.col("cm.committee_type")).alias(
            "recipient_committee_type"
        ),
        F.coalesce(F.col("sa.recipient_committee_org_type"), F.col("cm.org_type")).alias(
            "recipient_committee_org_type"
        ),
        F.coalesce(F.col("sa.recipient_committee_designation"), F.col("cm.designation")).alias(
            "recipient_committee_designation"
        ),
        F.coalesce(F.col("sa.candidate_name"), F.col("cand.display_name")).alias("candidate_name"),
        F.coalesce(F.col("sa.candidate_first_name"), F.col("cand.first_name")).alias("candidate_first_name"),
        F.coalesce(F.col("sa.candidate_last_name"), F.col("cand.last_name")).alias("candidate_last_name"),
        F.coalesce(F.col("sa.candidate_middle_name"), F.col("cand.middle_name")).alias("candidate_middle_name"),
        F.coalesce(F.col("sa.candidate_prefix"), F.col("cand.prefix")).alias("candidate_prefix"),
        F.coalesce(F.col("sa.candidate_suffix"), F.col("cand.suffix")).alias("candidate_suffix"),
        F.coalesce(F.col("sa.candidate_office"), F.col("cand.office")).alias("candidate_office"),
        F.coalesce(F.col("sa.candidate_office_state"), F.col("cand.state")).alias("candidate_office_state"),
        F.coalesce(F.col("sa.candidate_office_district"), F.col("cand.district")).alias("candidate_office_district"),
        numeric_amount.alias("contribution_receipt_amount"),
    )


def _lowercase_columns(df: DataFrame) -> DataFrame:
    renamed = df
    for col in df.columns:
        renamed = renamed.withColumnRenamed(col, col.lower())
    return renamed


def _standardize_schedule_a_columns(df: DataFrame) -> DataFrame:
    normalized = df
    for source, target in SCHEDULE_A_COLUMN_ALIASES.items():
        if source in normalized.columns and target not in normalized.columns:
            normalized = normalized.withColumnRenamed(source, target)
    return normalized
