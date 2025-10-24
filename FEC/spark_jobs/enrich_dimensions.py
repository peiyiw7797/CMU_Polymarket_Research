"""Transforms raw master tables into analytical dimensions."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .utils import normalized_column

OFFICE_DESCRIPTIONS = {
    "P": "President",
    "S": "Senate",
    "H": "House",
}


def build_candidate_dim(candidate_master: DataFrame) -> DataFrame:
    office_entries = []
    for code, desc in OFFICE_DESCRIPTIONS.items():
        office_entries.extend([F.lit(code), F.lit(desc)])
    office_map = F.create_map(*office_entries)

    return (
        candidate_master.select(
            F.col("CAND_ID").alias("cand_id"),
            F.col("cycle"),
            F.col("CAND_NAME").alias("display_name"),
            F.col("first_name"),
            F.col("middle_name"),
            F.col("last_name"),
            F.col("prefix"),
            F.col("suffix"),
            F.col("CAND_PTY_AFFILIATION").alias("party"),
            F.col("CAND_OFFICE").alias("office"),
            F.col("CAND_OFFICE_ST").alias("state"),
            F.col("CAND_OFFICE_DISTRICT").alias("district"),
            F.col("CAND_ELECTION_YR").alias("candidate_election_year"),
            F.col("CAND_ICI").alias("incumbent_challenger_status"),
            F.col("CAND_STATUS").alias("status"),
            normalized_column("CAND_NAME").alias("normalized_name"),
            F.col("CAND_PCC").alias("principal_campaign_committee"),
        )
        .dropDuplicates(["cand_id", "cycle"])
        .withColumn(
            "search_text",
            F.concat_ws(" ", F.col("normalized_name"), F.col("display_name")),
        )
        .withColumn("office_full", office_map[F.col("office")])
    )


def build_committee_dim(committee_master: DataFrame) -> DataFrame:
    return (
        committee_master.select(
            F.col("CMTE_ID").alias("cmte_id"),
            F.col("cycle"),
            F.col("CMTE_NM").alias("name"),
            F.col("TRES_NM").alias("treasurer"),
            F.col("CMTE_ST1").alias("street_1"),
            F.col("CMTE_ST2").alias("street_2"),
            F.col("CMTE_CITY").alias("city"),
            F.col("CMTE_ST").alias("state"),
            F.col("CMTE_ZIP").alias("zip"),
            F.col("CMTE_DSGN").alias("designation"),
            F.col("CMTE_TP").alias("committee_type"),
            F.col("CMTE_PTY_AFFILIATION").alias("party_affiliation"),
            F.col("CMTE_FILING_FREQ").alias("filing_frequency"),
            F.col("ORG_TP").alias("org_type"),
            F.col("CONNECTED_ORG_NM").alias("connected_org"),
            F.col("CAND_ID").alias("principal_candidate_id"),
        )
        .dropDuplicates(["cmte_id", "cycle"])
    )


def build_candidate_committees(
    linkage: DataFrame,
    committees: DataFrame,
    candidate_dim: DataFrame,
) -> DataFrame:
    committee_roles = (
        linkage.select(
            F.col("CAND_ID").alias("cand_id"),
            F.col("CMTE_ID").alias("cmte_id"),
            F.col("cycle"),
            F.col("CMTE_TP").alias("committee_type_code"),
            F.col("CMTE_DSGN").alias("designation_code"),
        )
        .dropDuplicates(["cand_id", "cmte_id", "cycle"])
        .withColumn(
            "role",
            F.when(F.col("designation_code").isin("P", "A"), F.lit("principal"))
            .when(F.col("designation_code").isin("J"), F.lit("joint_fundraising"))
            .when(F.col("designation_code").isin("L"), F.lit("leadership"))
            .otherwise(F.lit("other")),
        )
    )

    enriched = (
        committee_roles.join(committees, ["cmte_id", "cycle"], "left")
        .join(candidate_dim.select("cand_id", "cycle"), ["cand_id", "cycle"], "left")
        .withColumn("principal_flag", F.col("role") == "principal")
        .withColumn(
            "committee_type",
            F.coalesce(F.col("committee_type"), F.col("committee_type_code")),
        )
        .withColumn(
            "designation",
            F.coalesce(F.col("designation"), F.col("designation_code")),
        )
        .drop("committee_type_code", "designation_code")
    )
    return enriched
