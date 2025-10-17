"""
Match Polymarket election markets to FEC itemized fundraising data.

For each relevant market row (United States or US state jurisdiction), the script:
* Parses candidate names from `candidate_names`.
* Queries the FEC API to locate matching candidates.
* Retrieves authorized committees for the most recent two election cycles.
* Identifies FEC candidate IDs (per market row) and aggregates them alongside market data.
* Writes an enriched dataset to `fec_financials_matched.csv` for downstream bulk matching.

The script relies on an FEC API key stored in the `FEC_API_KEY` environment
variable. If absent, it falls back to the public `DEMO_KEY` with tighter limits.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# Configure logging early so all modules share the same handler.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("fec_matcher")

FEC_API_BASE = "https://api.open.fec.gov/v1"
FEC_API_KEY = os.environ.get("FEC_API_KEY", "DEMO_KEY")


# Two most recent election cycles (even-numbered years).
def recent_cycles(count: int = 2) -> List[int]:
    current_year = datetime.utcnow().year
    even_year = current_year if current_year % 2 == 0 else current_year - 1
    cycles: List[int] = []
    for _ in range(count):
        cycles.append(even_year)
        even_year -= 2
    return cycles


# State helpers ---------------------------------------------------------------
US_STATE_TO_ABBREV: Dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}
US_STATES = set(US_STATE_TO_ABBREV.keys())


def state_from_jurisdiction(jurisdiction: Optional[str]) -> Optional[str]:
    """Extract a US state name when available."""
    if not jurisdiction:
        return None
    normalized = jurisdiction.strip()
    if normalized in US_STATES:
        return normalized
    if normalized.startswith("United States -"):
        suffix = normalized.split("-", 1)[1].strip()
        for state in US_STATES:
            if state.lower() == suffix.lower():
                return state
    if normalized == "United States":
        return None
    # Handle patterns like "United States - New York City".
    match = re.search(r"United States\s*-\s*([A-Za-z\s]+)", normalized)
    if match:
        candidate = match.group(1).strip()
        for state in US_STATES:
            if state.lower() == candidate.lower():
                return state
    return None


def infer_office_code(row: Dict[str, object]) -> Optional[str]:
    """Best-effort mapping from market text to FEC office codes (P, S, H)."""
    haystack_parts = [
        str(row.get("event_title", "")),
        str(row.get("market_question", "")),
        str(row.get("analytics_event_tags", "")),
    ]
    haystack = " ".join(haystack_parts).lower()
    if "president" in haystack or "white house" in haystack:
        return "P"
    if "senate" in haystack or "senator" in haystack:
        return "S"
    if "house" in haystack or "representative" in haystack or "congress" in haystack:
        return "H"
    if "governor" in haystack or "gubernatorial" in haystack:
        return "S"  # Governor races map closest to statewide (S) for filtering.
    return None


# API helpers -----------------------------------------------------------------
def _compute_rate_limit_sleep(response: requests.Response, fallback: float = 5.0) -> float:
    """Determine how long to sleep after a 429 based on headers."""
    wait_values: List[float] = []
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            wait_values.append(float(retry_after))
        except (TypeError, ValueError):
            pass
    reset_header = response.headers.get("X-RateLimit-Reset")
    if reset_header:
        try:
            reset_val = float(reset_header)
            now = time.time()
            # X-RateLimit-Reset may be either seconds-from-now or an epoch timestamp.
            if reset_val > now + 1:
                wait_values.append(reset_val - now)
            else:
                wait_values.append(reset_val)
        except (TypeError, ValueError):
            pass
    if wait_values:
        return max(fallback, max(wait_values))
    return fallback


def fetch_json(
    session: requests.Session,
    url: str,
    params: Dict[str, object],
    max_retries: int = 5,
    timeout: int = 30,
) -> Optional[Dict[str, object]]:
    """Backward-compatible wrapper around ``fetch_json_full``."""
    payload, _status = fetch_json_full(
        session=session,
        url=url,
        params=params,
        max_retries=max_retries,
        timeout=timeout,
    )
    return payload


def fetch_json_full(
    session: requests.Session,
    url: str,
    params: Dict[str, object],
    max_retries: int = 5,
    timeout: int = 30,
) -> Tuple[Optional[Dict[str, object]], Optional[int]]:
    """GET helper with pagination-aware exponential backoff."""
    backoff = 5.0
    last_status: Optional[int] = None
    last_response: Optional[requests.Response] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            last_response = response
        except requests.RequestException as exc:
            LOGGER.warning("Request error (%s) on %s params=%s", exc, url, params)
            time.sleep(backoff)
            backoff *= 2.0
            continue

        if response.status_code == 200:
            last_status = 200
            try:
                data = response.json()
            except ValueError:
                LOGGER.error("Failed to decode JSON response for %s", url)
                return None, 200
            time.sleep(0.75)
            return data, 200

        if response.status_code in {429, 500, 502, 503, 504}:
            last_status = response.status_code
            LOGGER.info(
                "FEC API rate/availability issue (status %s). Retry %s/%s.",
                response.status_code,
                attempt,
                max_retries,
            )
            sleep_for = backoff
            if response.status_code == 429:
                sleep_for = _compute_rate_limit_sleep(response, backoff)
                LOGGER.info("Sleeping %.1f seconds for rate limit reset.", sleep_for)
            time.sleep(sleep_for)
            backoff *= 2.0
            continue

        LOGGER.error(
            "FEC API returned status %s for %s params=%s",
            response.status_code,
            url,
            params,
        )
        return None, response.status_code
    LOGGER.error("Exceeded retry budget for %s params=%s", url, params)
    if last_response is not None and last_status == 429:
        sleep_for = _compute_rate_limit_sleep(last_response, 60.0)
        LOGGER.info("Cooling down %.1f seconds due to sustained rate limiting.", sleep_for)
        time.sleep(sleep_for)
    return None, last_status


SearchCacheKey = Tuple[str, Optional[str], Optional[str]]


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip()).lower()


def search_candidates(
    session: requests.Session,
    name: str,
    state: Optional[str],
    office: Optional[str],
    cache: Dict[SearchCacheKey, List[Dict[str, object]]],
) -> List[Dict[str, object]]:
    """Query FEC /candidates/ and return relevant rows."""
    state_abbrev = US_STATE_TO_ABBREV.get(state) if state else None

    name_parts = name.split()
    combinations: List[Tuple[Optional[str], Optional[str]]] = [(state_abbrev, office)]
    if len(name_parts) >= 2:
        if office:
            combinations.append((state_abbrev, None))
        if state_abbrev:
            combinations.append((None, office))
        combinations.append((None, None))

    unique_combinations: List[Tuple[Optional[str], Optional[str]]] = []
    seen: set[Tuple[Optional[str], Optional[str]]] = set()
    for combo in combinations:
        if combo in seen:
            continue
        seen.add(combo)
        unique_combinations.append(combo)
    combinations = unique_combinations

    for combo_state, combo_office in combinations:
        results = _search_candidates_with_filters(
            session=session,
            name=name,
            state_abbrev=combo_state,
            office=combo_office,
            cache=cache,
        )
        if results:
            if combo_state != state_abbrev or combo_office != office:
                LOGGER.info(
                    "Expanded FEC search for '%s' using state=%s office=%s",
                    name,
                    combo_state,
                    combo_office,
                )
            return results
        LOGGER.info(
            "No FEC results for '%s' with state=%s office=%s", name, combo_state, combo_office
        )
        if len(name_parts) < 2:
            break
    return []


def _search_candidates_with_filters(
    session: requests.Session,
    name: str,
    state_abbrev: Optional[str],
    office: Optional[str],
    cache: Dict[SearchCacheKey, List[Dict[str, object]]],
) -> List[Dict[str, object]]:
    """Internal helper to query candidates with a specific state/office filter."""
    cache_key: SearchCacheKey = (normalize_name(name), state_abbrev, office)
    if cache_key in cache:
        return cache[cache_key]

    params = {
        'q': name,
        'per_page': 50,
        'api_key': FEC_API_KEY,
    }

    if state_abbrev:
        params['state'] = state_abbrev
    if office:
        params['office'] = office

    url = f"{FEC_API_BASE}/candidates/"
    results: List[Dict[str, object]] = []
    page = 1
    while True:
        params["page"] = page
        payload, _status = fetch_json_full(session, url, params, max_retries=1)
        if not payload:
            break
        results.extend(payload.get("results", []))
        pagination = payload.get("pagination") or {}
        if page >= int(pagination.get("pages", 0) or 0):
            break
        page += 1

    # Post-filter by state/office/name exactness when possible.
    normalized_target = normalize_name(name)
    filtered: List[Dict[str, object]] = []
    for cand in results:
        cand_name = normalize_name(str(cand.get("name", "")))
        if cand_name != normalized_target:
            continue
        if state_abbrev and cand.get("state") != state_abbrev:
            continue
        if office and cand.get("office") != office:
            continue
        filtered.append(cand)

    if not filtered:
        filtered = results  # Fall back to best-effort list.

    cache[cache_key] = filtered
    return filtered


# Data orchestration ----------------------------------------------------------
def parse_candidate_names(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    parts = [part.strip() for part in raw_value.split(",")]
    return [part for part in parts if part]


def process_markets(
    input_path: str,
    output_path: str,
    row_limit: Optional[int] = None,
) -> None:
    df = pd.read_csv(input_path)
    if row_limit is not None:
        df = df.head(row_limit)

    cycles = recent_cycles(2)
    LOGGER.info("Using election cycles: %s", cycles)

    search_cache: Dict[SearchCacheKey, List[Dict[str, object]]] = {}
    stats = defaultdict(int)

    records: List[Dict[str, object]] = []
    with requests.Session() as session:
        session.headers.update({"User-Agent": "CMU-Polymarket-Research-FEC/1.0"})

        for idx, row in df.iterrows():
            jurisdiction = row.get("jurisdiction")
            state = state_from_jurisdiction(str(jurisdiction) if pd.notna(jurisdiction) else None)
            process_row = state is not None or str(jurisdiction).strip() == "United States"
            names_raw = row.get("candidate_names")
            candidate_names = parse_candidate_names(str(names_raw) if pd.notna(names_raw) else "")

            stats["rows_total"] += 1
            if not process_row or not candidate_names:
                stats["rows_skipped"] += 1
                enriched = row.to_dict()
                enriched["matched_fec_ids"] = []
                enriched["download_successful_names"] = []
                records.append(enriched)
                continue

            office = infer_office_code(row.to_dict())
            LOGGER.info(
                "Row %s: processing %d candidate(s) with jurisdiction '%s' (state=%s, office=%s)",
                idx,
                len(candidate_names),
                jurisdiction,
                state,
                office,
            )

            matched_ids: List[str] = []
            successful_names: List[str] = []

            for candidate_name in candidate_names:
                search_results = search_candidates(
                    session=session,
                    name=candidate_name,
                    state=state,
                    office=office,
                    cache=search_cache,
                )
                if not search_results:
                    LOGGER.warning("No FEC match for candidate '%s'", candidate_name)
                    stats["names_unmatched"] += 1
                    continue

                candidate_ids = [
                    cand.get("candidate_id")
                    for cand in search_results
                    if cand.get("candidate_id")
                ]
                if not candidate_ids:
                    LOGGER.warning("Candidate entry missing ID for '%s'", candidate_name)
                    stats["names_unmatched"] += 1
                    continue

                stats["names_matched"] += 1
                for candidate_id in candidate_ids:
                    if candidate_id not in matched_ids:
                        matched_ids.append(candidate_id)
                successful_names.append(candidate_name)

            enriched_row = row.to_dict()
            enriched_row["matched_fec_ids"] = matched_ids
            enriched_row["download_successful_names"] = successful_names
            records.append(enriched_row)

    LOGGER.info(
        "Processing complete. Rows=%s (skipped=%s). Names matched=%s, unmatched=%s.",
        stats["rows_total"],
        stats["rows_skipped"],
        stats["names_matched"],
        stats["names_unmatched"],
    )

    output_df = pd.DataFrame(records)
    # JSON-encode complex columns for CSV persistence.
    for column in ("matched_fec_ids", "download_successful_names"):
        output_df[column] = output_df[column].apply(json.dumps)
    output_df.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(output_df), output_path)


def main() -> None:
    parser = ArgumentParser(description="Match Polymarket markets to FEC candidate identifiers.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of market rows to process.",
    )
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "data", "polymarket_election_markets_closed_2y.csv")
    output_path = os.path.join(base_dir, "fec_financials_matched.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing input dataset at {input_path}")

    process_markets(
        input_path,
        output_path,
        row_limit=args.limit,
    )


if __name__ == "__main__":
    main()
