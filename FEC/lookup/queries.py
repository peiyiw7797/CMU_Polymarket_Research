"""Parameterized SQL templates used by the lookup service."""

SEARCH_CANDIDATES = """
SELECT
    cand_id,
    cycle,
    display_name,
    normalized_name,
    party,
    office,
    office_full,
    state,
    district,
    search_text,
    candidate_election_year
FROM candidate_dim
WHERE search_text ILIKE :query
ORDER BY cycle DESC, display_name;
"""

CANDIDATE_SUMMARY = """
SELECT
    c.cand_id,
    c.cycle,
    c.display_name,
    c.party,
    c.office,
    c.office_full,
    c.state,
    c.district,
    s.total_receipts,
    s.individual_receipts,
    s.committee_receipts,
    s.party_receipts,
    s.transaction_count
FROM candidate c
LEFT JOIN candidate_cycle_summary s
  ON c.cand_id = s.candidate_id
 AND c.cycle = s.cycle
WHERE c.cand_id = :cand_id
ORDER BY c.cycle DESC;
"""

SCHEDULE_A_FLAT_BASE = """
SELECT
    *
FROM schedule_a_flat
WHERE candidate_id = :cand_id
{cycle_filter}
ORDER BY contribution_receipt_date DESC
LIMIT :limit OFFSET :offset;
"""

TOP_CONTRIBUTORS = """
SELECT
    candidate_id,
    cycle,
    contributor_name,
    entity_type,
    total_amount
FROM top_contributors
WHERE candidate_id = :cand_id
ORDER BY cycle DESC, total_amount DESC
LIMIT :limit;
"""
