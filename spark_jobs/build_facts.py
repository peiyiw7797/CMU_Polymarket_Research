"""Compute aggregated fact tables from enriched datasets."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_candidate_cycle_summary(schedule_a_flat: DataFrame) -> DataFrame:
    return (
        schedule_a_flat.filter(F.col("candidate_id").isNotNull())
        .groupBy("candidate_id", "two_year_transaction_period")
        .agg(
            F.sum("contribution_receipt_amount").alias("total_receipts"),
            F.sum(
                F.when(F.col("entity_type") == "IND", F.col("contribution_receipt_amount")).otherwise(0.0)
            ).alias("individual_receipts"),
            F.sum(
                F.when(F.col("entity_type").isin("CCM", "COM"), F.col("contribution_receipt_amount")).otherwise(0.0)
            ).alias("committee_receipts"),
            F.sum(
                F.when(F.col("entity_type") == "PTY", F.col("contribution_receipt_amount")).otherwise(0.0)
            ).alias("party_receipts"),
            F.count("*").alias("transaction_count"),
        )
        .withColumnRenamed("two_year_transaction_period", "cycle")
    )


def compute_candidate_committee_details(schedule_a_flat: DataFrame) -> DataFrame:
    return (
        schedule_a_flat.filter(F.col("candidate_id").isNotNull())
        .groupBy(
            "candidate_id",
            "committee_id",
            "two_year_transaction_period",
            "report_year",
            "report_type",
        )
        .agg(
            F.sum("contribution_receipt_amount").alias("receipts"),
            F.count("*").alias("transactions"),
        )
        .withColumnRenamed("two_year_transaction_period", "cycle")
    )


def compute_top_contributors(schedule_a_flat: DataFrame, limit: int = 20) -> DataFrame:
    ranked = (
        schedule_a_flat.filter(F.col("candidate_id").isNotNull())
        .groupBy(
            "candidate_id",
            "two_year_transaction_period",
            "contributor_name",
            "entity_type",
        )
        .agg(F.sum("contribution_receipt_amount").alias("total_amount"))
        .withColumnRenamed("two_year_transaction_period", "cycle")
    )

    window = Window.partitionBy("candidate_id", "cycle").orderBy(F.col("total_amount").desc())
    return (
        ranked.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= limit)
        .drop("rank")
    )
