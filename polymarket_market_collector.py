"""
Collect high-liquidity, closed election markets from Polymarket's public API.

The script fetches closed events in batches, filters for election-related markets,
and exports the top candidates by notional volume. Supplementary metadata such as
liquidity, resolution details, and inferred winning outcomes is included so the
results can feed downstream analytics and merge pipelines.

Outputs
-------
new_research/data/polymarket_election_markets_top20.csv
new_research/data/polymarket_election_markets_top20.json
new_research/data/polymarket_price_history_top20.json

Notes
-----
* Price history data is retrieved via the `clob.polymarket.com/prices-history`
  endpoint with daily fidelity. Older FPMM-legacy markets sometimes return no
  observations; these cases are flagged for transparency.
* The script prioritises closed markets with strong liquidity and trading depth.
  Keyword heuristics are used to identify elections across global jurisdictions.
"""

from __future__ import annotations

import ast
import csv
import dataclasses
import json
import math
import pathlib
import re
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import requests


BASE_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

EVENTS_URL = "https://gamma-api.polymarket.com/events"
MARKET_URL = "https://gamma-api.polymarket.com/markets/{market_id}"
PRICE_HISTORY_URL = "https://clob.polymarket.com/prices-history"

# Keywords tuned to catch parliamentary, presidential, and sub-national contests.
ELECTION_KEYWORDS = {
    "election",
    "primary",
    "runoff",
    "ballot",
    "governor",
    "mayor",
    "parliament",
    "president",
    "assembly",
    "congress",
    "senate",
    "house",
    "bundestag",
    "reichstag",
    "nomination",
}

# Markets must refer to an electoral outcome to be included.
QUESTION_PATTERNS = [
    re.compile(r"\bwin(s)?\b", re.IGNORECASE),
    re.compile(r"\bwinner\b", re.IGNORECASE),
    re.compile(r"\bbe\s+elected\b", re.IGNORECASE),
    re.compile(r"\belected\b", re.IGNORECASE),
    re.compile(r"\bbe\s+inaugurated\b", re.IGNORECASE),
    re.compile(r"\binaugurated\b", re.IGNORECASE),
    re.compile(r"\bnominee\b", re.IGNORECASE),
    re.compile(r"\bnomination\b", re.IGNORECASE),
    re.compile(r"\bd[- ]?nom\b", re.IGNORECASE),
    re.compile(r"\bbe\s+the\s+next\b", re.IGNORECASE),
    re.compile(r"\bmajority\b", re.IGNORECASE),
    re.compile(r"\bseat\b", re.IGNORECASE),
    re.compile(r"\bcontrol\b", re.IGNORECASE),
    re.compile(r"\brunoff\b", re.IGNORECASE),
]

# Offset/limit settings recommended by the Polymarket API docs.
PAGE_LIMIT = 500
MAX_OFFSET = 20000  # Safety stop to avoid hammering the API.

# Cap the number of records we keep so downstream work stays focused.
TARGET_COUNT = 20
EXTRA_BUFFER = 10  # Fetch a few extra in case of downstream filtering losses.


@dataclasses.dataclass
class MarketRecord:
    event_id: str
    market_id: str
    platform: str
    event_title: str
    market_question: str
    jurisdiction: Optional[str]
    category: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    closed_time: Optional[str]
    total_volume: Optional[float]
    volume_24h: Optional[float]
    volume_1w: Optional[float]
    volume_1m: Optional[float]
    volume_1y: Optional[float]
    liquidity: Optional[float]
    liquidity_amm: Optional[float]
    liquidity_clob: Optional[float]
    open_interest: Optional[float]
    num_outcomes: int
    outcomes: List[str]
    winning_outcome: Optional[str]
    resolution_source: Optional[str]
    resolution_notes: Optional[str]
    clob_token_ids: List[str]
    price_history_available: bool

    def to_row(self) -> Dict[str, object]:
        """Convert the dataclass to a flat dict suitable for CSV/JSON export."""
        return {
            "event_id": self.event_id,
            "market_id": self.market_id,
            "platform": self.platform,
            "event_title": self.event_title,
            "market_question": self.market_question,
            "jurisdiction": self.jurisdiction,
            "category": self.category,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "closed_time": self.closed_time,
            "total_volume": self.total_volume,
            "volume_24h": self.volume_24h,
            "volume_1w": self.volume_1w,
            "volume_1m": self.volume_1m,
            "volume_1y": self.volume_1y,
            "liquidity": self.liquidity,
            "liquidity_amm": self.liquidity_amm,
            "liquidity_clob": self.liquidity_clob,
            "open_interest": self.open_interest,
            "num_outcomes": self.num_outcomes,
            "outcomes": self.outcomes,
            "winning_outcome": self.winning_outcome,
            "resolution_source": self.resolution_source,
            "resolution_notes": self.resolution_notes,
            "clob_token_ids": self.clob_token_ids,
            "price_history_available": self.price_history_available,
        }


def is_election_event(event: Dict[str, object]) -> bool:
    """Heuristic filter to keep only election-related events."""
    haystack = " ".join(
        str(part).lower()
        for part in (
            event.get("title"),
            event.get("description"),
            event.get("ticker"),
            event.get("slug"),
            event.get("category"),
        )
        if part
    )
    return any(keyword in haystack for keyword in ELECTION_KEYWORDS)


def parse_float(value: Optional[str]) -> Optional[float]:
    """Convert Polymarket stringified decimals to floats."""
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_winner(outcomes_raw: str, prices_raw: str) -> Optional[str]:
    """Infer the winning outcome from final settlement prices."""
    try:
        outcomes = ast.literal_eval(outcomes_raw)
        prices = [float(p) for p in ast.literal_eval(prices_raw)]
    except (ValueError, SyntaxError):
        return None

    if not outcomes or not prices or len(outcomes) != len(prices):
        return None

    # Markets typically settle at 0 or 1. Use a tolerance for defensive coding.
    tolerance = 0.8
    for outcome, price in zip(outcomes, prices):
        if price >= tolerance:
            return outcome
    return None


def parse_outcomes(outcomes_raw: str) -> List[str]:
    try:
        outcomes = ast.literal_eval(outcomes_raw)
    except (ValueError, SyntaxError):
        return []
    return [str(o) for o in outcomes]


def fetch_price_history(token_id: str) -> List[Dict[str, object]]:
    """Retrieve daily price history for a given CLOB token ID."""
    params = {
        "market": token_id,
        "interval": "max",
        "fidelity": 60 * 24,  # daily candles
    }
    try:
        response = requests.get(PRICE_HISTORY_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return []

    history = response.json().get("history", [])
    formatted = []
    for entry in history:
        timestamp = datetime.fromtimestamp(entry["t"], tz=timezone.utc).strftime("%Y-%m-%d")
        formatted.append({"token_id": token_id, "date": timestamp, "price": entry["p"]})
    return formatted


def is_election_market_question(question: str) -> bool:
    """Ensure the specific market question targets an electoral outcome."""
    if not question:
        return False
    return any(pattern.search(question) for pattern in QUESTION_PATTERNS)


def load_closed_election_markets() -> List[MarketRecord]:
    """Fetch and flatten Polymarket election markets into MarketRecord objects."""
    records: List[MarketRecord] = []
    offset = 0

    while offset <= MAX_OFFSET:
        params = {"closed": "true", "limit": PAGE_LIMIT, "offset": offset}
        try:
            response = requests.get(EVENTS_URL, params=params, timeout=45)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to fetch events at offset {offset}") from exc

        batch = response.json()
        if not batch:
            break

        for event in batch:
            if not is_election_event(event):
                continue

            event_id = str(event.get("id"))
            title = event.get("title", "")
            category = event.get("category")
            jurisdiction = None
            # Light heuristic: country name often appears in the title; keep it raw for analysts.
            # Analysts can apply NLP/geocoding downstream if richer tagging is required.
            if ":" in title:
                jurisdiction = title.split(":")[0].strip()

            liquidity_clob = parse_float(event.get("liquidityClob"))
            open_interest = parse_float(event.get("openInterest"))

            for market in event.get("markets", []):
                market_id = str(market.get("id"))
                volume = parse_float(market.get("volume"))
                resolution_source = market.get("resolutionSource") or event.get("resolutionSource")
                outcomes_list = parse_outcomes(market.get("outcomes", "[]"))
                clob_token_ids = parse_outcomes(market.get("clobTokenIds", "[]"))
                question = market.get("question", "")
                if not is_election_market_question(question):
                    continue

                record = MarketRecord(
                    event_id=event_id,
                    market_id=market_id,
                    platform="Polymarket",
                    event_title=title,
                    market_question=question,
                    jurisdiction=jurisdiction,
                    category=category,
                    start_date=event.get("startDate"),
                    end_date=event.get("endDate"),
                    closed_time=market.get("closedTime") or event.get("closedTime"),
                    total_volume=volume,
                    volume_24h=parse_float(market.get("volume24hr")),
                    volume_1w=parse_float(market.get("volume1wk")),
                    volume_1m=parse_float(market.get("volume1mo")),
                    volume_1y=parse_float(market.get("volume1yr")),
                    liquidity=parse_float(market.get("liquidity")),
                    liquidity_amm=parse_float(market.get("liquidityAmm") or event.get("liquidityAmm")),
                    liquidity_clob=liquidity_clob,
                    open_interest=open_interest,
                    num_outcomes=len(outcomes_list),
                    outcomes=outcomes_list,
                    winning_outcome=infer_winner(
                        market.get("outcomes", "[]"), market.get("outcomePrices", "[]")
                    ),
                    resolution_source=resolution_source,
                    resolution_notes=market.get("description") or event.get("description"),
                    clob_token_ids=clob_token_ids,
                    price_history_available=False,  # placeholder until we fetch it below
                )
                records.append(record)

        offset += PAGE_LIMIT
        if offset > MAX_OFFSET:
            break

        # Polymarket's API is public but we stay polite.
        time.sleep(0.4)

    return records


def main() -> None:
    records = load_closed_election_markets()

    # Focus on top volumes. Skip entries without meaningful volume data.
    ranked = [
        r for r in records
        if r.total_volume is not None and r.total_volume > 1_000
    ]

    ranked.sort(key=lambda r: r.total_volume or 0.0, reverse=True)
    top_records = ranked[: TARGET_COUNT + EXTRA_BUFFER]

    # Attempt to fetch price histories to support timeline merges.
    price_histories: Dict[str, List[Dict[str, object]]] = {}
    for record in top_records:
        histories_for_market: List[Dict[str, object]] = []
        for token_id in record.clob_token_ids:
            history = fetch_price_history(token_id)
            if history:
                histories_for_market.extend(history)
        if histories_for_market:
            record.price_history_available = True
            price_histories[record.market_id] = histories_for_market
        else:
            record.price_history_available = False

        # Friendly pacing.
        time.sleep(0.2)

    # Deduplicate top records by market ID and cap at target count.
    unique_records: Dict[str, MarketRecord] = {}
    for record in top_records:
        if record.market_id not in unique_records and len(unique_records) < TARGET_COUNT:
            unique_records[record.market_id] = record

    final_records = list(unique_records.values())

    if not final_records:
        raise RuntimeError("No election markets survived filtering; adjust parameters.")

    # Export CSV.
    csv_path = DATA_DIR / "polymarket_election_markets_top20.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        fieldnames = list(final_records[0].to_row().keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in final_records:
            writer.writerow(record.to_row())

    # Export JSON.
    json_path = DATA_DIR / "polymarket_election_markets_top20.json"
    with json_path.open("w", encoding="utf-8") as jsonfile:
        json.dump([record.to_row() for record in final_records], jsonfile, indent=2)

    # Export price histories for transparency; downstream scripts can join on market_id.
    final_market_ids = {record.market_id for record in final_records}
    filtered_histories = {
        market_id: price_histories[market_id]
        for market_id in final_market_ids
        if market_id in price_histories
    }

    history_path = DATA_DIR / "polymarket_price_history_top20.json"
    with history_path.open("w", encoding="utf-8") as history_file:
        json.dump(filtered_histories, history_file, indent=2)

    print(f"Wrote {len(final_records)} market records to {csv_path}")
    print(f"Wrote price history for {sum(bool(v) for v in filtered_histories.values())} markets")


if __name__ == "__main__":
    main()
