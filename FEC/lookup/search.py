"""Business logic for candidate lookups."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .db import connection
from .queries import CANDIDATE_SUMMARY, SCHEDULE_A_FLAT_BASE, SEARCH_CANDIDATES, TOP_CONTRIBUTORS


@dataclass
class CandidateMatch:
    cand_id: str
    cycle: int
    display_name: str
    normalized_name: Optional[str]
    party: Optional[str]
    office: Optional[str]
    office_full: Optional[str]
    state: Optional[str]
    district: Optional[str]
    candidate_election_year: Optional[int]


def normalize_query(name: str) -> str:
    tokens = [part.strip() for part in name.replace(",", " ").split() if part.strip()]
    if not tokens:
        return "%"
    return "%" + "%".join(tokens) + "%"


def search_candidates(name: str, limit: Optional[int] = None) -> List[CandidateMatch]:
    params = {"query": normalize_query(name)}
    results: List[CandidateMatch] = []
    with connection() as conn:
        rows = conn.execute(text(SEARCH_CANDIDATES), params).fetchall()
    for row in rows:
        results.append(
            CandidateMatch(
                cand_id=row.cand_id,
                cycle=row.cycle,
                display_name=row.display_name,
                normalized_name=getattr(row, "normalized_name", None),
                party=row.party,
                office=row.office,
                office_full=getattr(row, "office_full", None),
                state=row.state,
                district=row.district,
                candidate_election_year=getattr(row, "candidate_election_year", None),
            )
        )
    if limit is not None:
        return results[:limit]
    return results


def candidate_summary(cand_id: str) -> List[dict]:
    with connection() as conn:
        rows = conn.execute(text(CANDIDATE_SUMMARY), {"cand_id": cand_id}).mappings().all()
    return [dict(row) for row in rows]


def schedule_a_history(
    cand_id: str,
    cycles: Optional[Iterable[int]] = None,
    limit: int = 200,
    page: int = 1,
) -> List[dict]:
    cycle_list = list(cycles) if cycles else None
    offset = (page - 1) * limit
    params = {"cand_id": cand_id, "limit": limit, "offset": offset}
    if cycle_list:
        cycle_placeholders = ", ".join(
            f":cycle_{idx}" for idx in range(len(cycle_list))
        )
        cycle_filter = f" AND two_year_transaction_period IN ({cycle_placeholders})"
        for idx, cycle in enumerate(cycle_list):
            params[f"cycle_{idx}"] = cycle
    else:
        cycle_filter = ""

    query = text(SCHEDULE_A_FLAT_BASE.format(cycle_filter=cycle_filter))
    with connection() as conn:
        rows = conn.execute(query, params).mappings().all()
    return [dict(row) for row in rows]


def top_contributors(cand_id: str, limit: int = 20) -> List[dict]:
    params = {"cand_id": cand_id, "limit": limit}
    with connection() as conn:
        rows = conn.execute(text(TOP_CONTRIBUTORS), params).mappings().all()
    return [dict(row) for row in rows]
