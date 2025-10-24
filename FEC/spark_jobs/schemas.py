"""Schema utilities for FEC bulk text files."""

from __future__ import annotations

import csv
import functools
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql.types import StringType, StructField, StructType


class SchemaRegistry:
    """Loads StructType definitions from FEC header CSV files."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self._cache: Dict[str, StructType] = {}

    def schema_from_header(self, header_path: Path) -> StructType:
        columns = _read_header_columns(header_path)
        return StructType([StructField(col, StringType(), True) for col in columns])

    def candidate_master(self, cycle: int) -> StructType:
        header = self.base_dir / "candidate_master" / "candidate_master_header_file.csv"
        return self._cached("candidate_master", header)

    def committee_master(self, cycle: int) -> StructType:
        header = self.base_dir / "committee_master" / "committee_master_header_file.csv"
        return self._cached("committee_master", header)

    def candidate_committee_linkage(self, cycle: int) -> StructType:
        header = self.base_dir / "candidate_committee_linkage" / "candidate_committee_linkage_header_file.csv"
        return self._cached("candidate_committee_linkage", header)

    def committees_to_candidates(self, cycle: int) -> StructType:
        header = self.base_dir / "committees_to_candidates" / "committees_to_candidates_header_file.csv"
        return self._cached("committees_to_candidates", header)

    def schedule_a(self, header_path: Optional[Path]) -> StructType:
        if header_path is None:
            return StructType([StructField(col, StringType(), True) for col in DEFAULT_SCHEDULE_A_COLUMNS])
        return self._cached(f"schedule_a:{header_path}", header_path)

    def schedule_b(self, header_path: Optional[Path]) -> StructType:
        if header_path is None:
            return StructType([StructField(col, StringType(), True) for col in DEFAULT_SCHEDULE_B_COLUMNS])
        return self._cached(f"schedule_b:{header_path}", header_path)

    def schedule_e(self, header_path: Optional[Path]) -> StructType:
        if header_path is None:
            return StructType([StructField(col, StringType(), True) for col in DEFAULT_SCHEDULE_E_COLUMNS])
        return self._cached(f"schedule_e:{header_path}", header_path)

    def _cached(self, key: str, header_path: Path) -> StructType:
        if key not in self._cache:
            self._cache[key] = self.schema_from_header(header_path)
        return self._cache[key]


@functools.lru_cache(maxsize=32)
def _read_header_columns(path: Path) -> List[str]:
    with path.open("r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                return [col.strip() for col in row]
    raise ValueError(f"No columns found in header file: {path}")


# Fallback columns derived from FEC Schedule documentation (subset used by pipeline).
DEFAULT_SCHEDULE_A_COLUMNS: List[str] = [
    "committee_id",
    "committee_name",
    "report_year",
    "report_type",
    "image_number",
    "filing_form",
    "link_id",
    "line_number",
    "transaction_id",
    "file_number",
    "entity_type",
    "entity_type_desc",
    "unused_contbr_id",
    "contributor_prefix",
    "contributor_name",
    "recipient_committee_type",
    "recipient_committee_org_type",
    "recipient_committee_designation",
    "contributor_first_name",
    "contributor_middle_name",
    "contributor_last_name",
    "contributor_suffix",
    "contributor_street_1",
    "contributor_street_2",
    "contributor_city",
    "contributor_state",
    "contributor_zip",
    "contributor_employer",
    "contributor_occupation",
    "contributor_id",
    "is_individual",
    "receipt_type",
    "receipt_type_desc",
    "receipt_type_full",
    "memo_code",
    "memo_code_full",
    "memo_text",
    "contribution_receipt_date",
    "contribution_receipt_amount",
    "contributor_aggregate_ytd",
    "candidate_id",
    "candidate_name",
    "candidate_first_name",
    "candidate_last_name",
    "candidate_middle_name",
    "candidate_prefix",
    "candidate_suffix",
    "candidate_office",
    "candidate_office_full",
    "candidate_office_state",
    "candidate_office_state_full",
    "candidate_office_district",
    "conduit_committee_id",
    "conduit_committee_name",
    "conduit_committee_street1",
    "conduit_committee_street2",
    "conduit_committee_city",
    "conduit_committee_state",
    "conduit_committee_zip",
    "donor_committee_name",
    "national_committee_nonfederal_account",
    "election_type",
    "election_type_full",
    "fec_election_type_desc",
    "fec_election_year",
    "two_year_transaction_period",
    "amendment_indicator",
    "amendment_indicator_desc",
    "schedule_type",
    "schedule_type_full",
    "increased_limit",
    "load_date",
    "sub_id",
    "original_sub_id",
    "back_reference_transaction_id",
    "back_reference_schedule_name",
    "pdf_url",
    "line_number_label",
]

DEFAULT_SCHEDULE_B_COLUMNS: List[str] = [
    "committee_id",
    "committee_name",
    "report_year",
    "report_type",
    "image_number",
    "filing_form",
    "line_number",
    "transaction_id",
    "entity_type",
    "entity_type_desc",
    "recipient_name",
    "recipient_city",
    "recipient_state",
    "recipient_zip",
    "recipient_organization",
    "recipient_principal",
    "disbursement_description",
    "disbursement_date",
    "disbursement_amount",
    "memo_code",
    "memo_text",
    "candidate_id",
    "candidate_name",
    "candidate_office",
    "candidate_state",
    "candidate_district",
    "two_year_transaction_period",
    "schedule_type",
    "load_date",
    "sub_id",
]

DEFAULT_SCHEDULE_E_COLUMNS: List[str] = [
    "committee_id",
    "committee_name",
    "report_year",
    "image_number",
    "filing_form",
    "line_number",
    "transaction_id",
    "candidate_id",
    "candidate_name",
    "candidate_party",
    "candidate_office",
    "candidate_state",
    "candidate_district",
    "payee_name",
    "payee_city",
    "payee_state",
    "payee_zip",
    "expenditure_date",
    "expenditure_amount",
    "expenditure_purpose",
    "support_oppose_code",
    "support_oppose_description",
    "election_type",
    "two_year_transaction_period",
    "load_date",
    "sub_id",
]
