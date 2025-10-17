"""
Collect closed election markets from Polymarket and external aggregators.

The script expands the original snapshot to cover every election-focused market
that resolved within the past two years. In addition to Polymarket's public API,
it ingests metadata from PolymarketAnalytics to improve coverage of
jurisdiction details, liquidity snapshots, and closing prices. Jurisdictions are
tagged with authoritative data sources so downstream analysts can coordinate
result verification and archival data pulls.

Outputs
-------
* new_research/data/polymarket_election_markets_closed_2y.csv
* new_research/data/polymarket_election_markets_closed_2y.json
* new_research/data/polymarket_jurisdiction_sources.csv
* new_research/data/polymarket_jurisdiction_sources.json

Notes
-----
* Jurisdiction tagging relies on heuristics drawn from event titles, market
  questions, and PolymarketAnalytics metadata. These heuristics skew toward
  national and sub-national electoral contests in English-language markets.
"""

from __future__ import annotations

import ast
import csv
import dataclasses
from dataclasses import field
import json
import pathlib
import re
import string
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Tuple

import nltk
from nltk import ne_chunk, pos_tag
from nltk.tokenize import TreebankWordTokenizer
from nltk.tree import Tree
import requests


BASE_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

EVENTS_URL = "https://gamma-api.polymarket.com/events"
MARKET_URL = "https://gamma-api.polymarket.com/markets/{market_id}"
ANALYTICS_COMBINED_MARKETS_URL = "https://polymarketanalytics.com/api/combined-markets"

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
    "legislative",
    "minister",
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

# Two-year lookback window.
RECENT_WINDOW_DAYS = 365 * 2

# Feasibility scoring helper (higher is better).
FEASIBILITY_ORDER = {"Low": 1, "Medium": 2, "High": 3}

# Official election office URLs sourced from USAGov.
STATE_OFFICIAL_SITES = {
    "Alabama": "https://www.sos.alabama.gov/alabama-votes",
    "Alaska": "https://www.elections.alaska.gov/",
    "American Samoa": "https://aselectionoffice.gov/",
    "Arizona": "https://azsos.gov/elections",
    "Arkansas": "https://www.sos.arkansas.gov/elections",
    "California": "https://www.sos.ca.gov/elections",
    "Colorado": "https://www.sos.state.co.us/pubs/elections/main.html",
    "Connecticut": "https://portal.ct.gov/sots/common-elements/v5-template---redesign/elections--voting--home-page",
    "Delaware": "https://elections.delaware.gov/index.shtml",
    "District of Columbia": "https://dcboe.org/",
    "Federated States of Micronesia": "https://www.fsmned.fm/",
    "Florida": "https://www.dos.myflorida.com/elections/",
    "Georgia": "https://sos.ga.gov/elections-division-georgia-secretary-states-office",
    "Guam": "https://gec.guam.gov/",
    "Hawaii": "https://elections.hawaii.gov/",
    "Idaho": "https://voteidaho.gov/",
    "Illinois": "https://www.elections.il.gov/",
    "Indiana": "https://indianavoters.in.gov/",
    "Iowa": "https://sos.iowa.gov/elections/voterinformation/index.html",
    "Kansas": "https://sos.ks.gov/elections/elections.html",
    "Kentucky": "https://elect.ky.gov/Pages/default.aspx",
    "Louisiana": "https://www.sos.la.gov/ElectionsAndVoting/Pages/default.aspx",
    "Maine": "https://www.maine.gov/sos/cec/elec/index.html",
    "Maryland": "https://elections.maryland.gov/",
    "Massachusetts": "https://www.sec.state.ma.us/divisions/elections/elections-and-voting.htm",
    "Michigan": "https://www.michigan.gov/sos/elections",
    "Minnesota": "https://www.sos.state.mn.us/elections-voting/",
    "Mississippi": "https://www.sos.ms.gov/elections-voting",
    "Missouri": "https://www.sos.mo.gov/elections/",
    "Montana": "https://sosmt.gov/elections/",
    "Nebraska": "https://www.nebraska.gov/featured/elections-voting/",
    "Nevada": "https://www.nvsos.gov/sos/elections",
    "New Hampshire": "https://www.sos.nh.gov/elections/voters",
    "New Jersey": "https://www.nj.gov/state/elections/vote.shtml",
    "New Mexico": "https://www.sos.nm.gov/voting-and-elections/voter-information-portal-nmvote-org/",
    "New York": "https://www.elections.ny.gov/",
    "North Carolina": "https://www.ncsbe.gov/",
    "North Dakota": "https://vip.sos.nd.gov/PortalList.aspx",
    "Northern Mariana Islands": "https://www.votecnmi.gov.mp/",
    "Ohio": "https://www.sos.state.oh.us/elections/voters/",
    "Oklahoma": "https://oklahoma.gov/elections.html",
    "Oregon": "https://sos.oregon.gov/voting-elections/Pages/default.aspx",
    "Pennsylvania": "https://www.dos.pa.gov/VotingElections/Pages/default.aspx",
    "Puerto Rico": "https://ww2.ceepur.org/",
    "Republic of Palau": "https://palauelection.org/",
    "Rhode Island": "https://vote.sos.ri.gov/",
    "South Carolina": "https://www.scvotes.org/",
    "South Dakota": "https://sdsos.gov/elections-voting/default.aspx",
    "Tennessee": "https://sos.tn.gov/elections",
    "Texas": "https://www.sos.state.tx.us/elections/index.shtml",
    "U.S. Virgin Islands": "https://vivote.gov/",
    "Utah": "https://vote.utah.gov/",
    "Vermont": "https://sos.vermont.gov/elections/",
    "Virginia": "https://www.elections.virginia.gov/",
    "Washington": "https://www.sos.wa.gov/elections",
    "West Virginia": "https://sos.wv.gov/elections/pages/default.aspx",
    "Wisconsin": "https://myvote.wi.gov/en-us/",
    "Wyoming": "https://sos.wyo.gov/Elections/Default.aspx",
}

# States and territories with abbreviations for heuristics.
US_DIVISIONS: Tuple[Tuple[str, str], ...] = (
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AS", "American Samoa"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DE", "Delaware"),
    ("DC", "District of Columbia"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("GU", "Guam"),
    ("HI", "Hawaii"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("IA", "Iowa"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("ME", "Maine"),
    ("MD", "Maryland"),
    ("MA", "Massachusetts"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MS", "Mississippi"),
    ("MO", "Missouri"),
    ("MT", "Montana"),
    ("NE", "Nebraska"),
    ("NV", "Nevada"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NY", "New York"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("MP", "Northern Mariana Islands"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("PR", "Puerto Rico"),
    ("PW", "Republic of Palau"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VT", "Vermont"),
    ("VA", "Virginia"),
    ("VI", "U.S. Virgin Islands"),
    ("WA", "Washington"),
    ("WV", "West Virginia"),
    ("WI", "Wisconsin"),
    ("WY", "Wyoming"),
)

US_STATE_ABBREV_TO_NAME = {abbr: name for abbr, name in US_DIVISIONS}
US_STATE_NAME_TO_ABBREV = {name: abbr for abbr, name in US_DIVISIONS}
US_STATE_KEYWORDS = {name.lower(): name for _, name in US_DIVISIONS}

# Country-level heuristics.
COUNTRY_KEYWORDS = {
    "united states": ("United States", "national"),
    "usa": ("United States", "national"),
    "u.s.": ("United States", "national"),
    "america": ("United States", "national"),
    "united kingdom": ("United Kingdom", "national"),
    "uk": ("United Kingdom", "national"),
    "britain": ("United Kingdom", "national"),
    "british": ("United Kingdom", "national"),
    "scotland": ("United Kingdom - Scotland", "subnational"),
    "wales": ("United Kingdom - Wales", "subnational"),
    "northern ireland": ("United Kingdom - Northern Ireland", "subnational"),
    "canada": ("Canada", "national"),
    "mexico": ("Mexico", "national"),
    "brazil": ("Brazil", "national"),
    "argentina": ("Argentina", "national"),
    "france": ("France", "national"),
    "germany": ("Germany", "national"),
    "italy": ("Italy", "national"),
    "spain": ("Spain", "national"),
    "portugal": ("Portugal", "national"),
    "india": ("India", "national"),
    "pakistan": ("Pakistan", "national"),
    "israel": ("Israel", "national"),
    "turkey": ("Turkey", "national"),
    "taiwan": ("Taiwan", "national"),
    "china": ("China", "national"),
    "japan": ("Japan", "national"),
    "south korea": ("South Korea", "national"),
    "korea": ("South Korea", "national"),
    "philippines": ("Philippines", "national"),
    "indonesia": ("Indonesia", "national"),
    "nigeria": ("Nigeria", "national"),
    "south africa": ("South Africa", "national"),
    "kenya": ("Kenya", "national"),
    "australia": ("Australia", "national"),
    "new zealand": ("New Zealand", "national"),
    "european union": ("European Union", "supra-national"),
    "european parliament": ("European Union", "supra-national"),
    "europe": ("European Union", "supra-national"),
    "venezuela": ("Venezuela", "national"),
    "peru": ("Peru", "national"),
    "colombia": ("Colombia", "national"),
    "chile": ("Chile", "national"),
    "paraguay": ("Paraguay", "national"),
    "uruguay": ("Uruguay", "national"),
    "panama": ("Panama", "national"),
    "dominican republic": ("Dominican Republic", "national"),
    "ecuador": ("Ecuador", "national"),
    "bolivia": ("Bolivia", "national"),
    "guatemala": ("Guatemala", "national"),
    "honduras": ("Honduras", "national"),
    "el salvador": ("El Salvador", "national"),
}

# Country-specific data source templates.
COUNTRY_SOURCE_LIBRARY: Dict[str, Dict[str, object]] = {
    "United States": {
        "level": "national",
        "sources": [
            {
                "name": "Federal Election Commission – Certified Results and Filings",
                "url": "https://www.fec.gov/data/",
                "description": "Official certified federal election results, candidate filings, and independent expenditure reports.",
                "access": "Open government data",
                "update_frequency": "Daily during election periods; certified results after canvass (weeks).",
                "feasibility": "High",
                "feasibility_notes": "Legally authoritative with structured CSV/JSON downloads; lag tied to state certification but reliable for federal races.",
            },
            {
                "name": "Associated Press Elections API",
                "url": "https://developer.ap.org/ap-elections-api/",
                "description": "Real-time and historical race calls across federal and statewide contests.",
                "access": "Commercial license",
                "update_frequency": "Minutes during election night; periodic thereafter.",
                "feasibility": "High",
                "feasibility_notes": "Industry-standard feed for rapid projections; requires paid access and integration work.",
            },
            {
                "name": "MIT Election Data and Science Lab – Official Returns",
                "url": "https://electionlab.mit.edu/data",
                "description": "Aggregated precinct- and county-level results in machine-readable formats.",
                "access": "Open academic use",
                "update_frequency": "Released weeks to months after certification.",
                "feasibility": "Medium",
                "feasibility_notes": "High data quality but slower refresh cadence; excellent for retrospective validation.",
            },
        ],
    },
    "United Kingdom": {
        "level": "national",
        "sources": [
            {
                "name": "UK Electoral Commission – Results and Turnout",
                "url": "https://www.electoralcommission.org.uk/research-voting-and-elections",
                "description": "Official UK parliamentary and local election results with downloadable datasets.",
                "access": "Open government data",
                "update_frequency": "Certified results published within days of counts.",
                "feasibility": "High",
                "feasibility_notes": "Primary authority for certified vote totals; limited to official counts (no projections).",
            },
            {
                "name": "BBC Elections Live Results",
                "url": "https://www.bbc.com/news/election",
                "description": "Live constituency-level reporting with history and contextual analysis.",
                "access": "Open web (API unofficial)",
                "update_frequency": "Minutes on election night; archived pages post-event.",
                "feasibility": "Medium",
                "feasibility_notes": "Timely updates; scraping required for automation and subject to layout drift.",
            },
            {
                "name": "Press Association Elections Database",
                "url": "https://www.pressassociation.com/solutions/elections/",
                "description": "Syndicated constituency results feed consumed by major newsrooms.",
                "access": "Commercial license",
                "update_frequency": "Real-time on election night.",
                "feasibility": "High",
                "feasibility_notes": "Robust infrastructure with SLA-backed updates; procurement and per-seat pricing required.",
            },
        ],
    },
    "Canada": {
        "level": "national",
        "sources": [
            {
                "name": "Elections Canada – Official Voting Results",
                "url": "https://www.elections.ca/content.aspx?section=res&dir=rep/oth/ovr&document=index&lang=e",
                "description": "Certified federal election results down to riding level.",
                "access": "Open government data",
                "update_frequency": "Released after official tabulation (days to weeks).",
                "feasibility": "High",
                "feasibility_notes": "Authoritative with CSV and machine-readable formats; slower than media projections.",
            },
            {
                "name": "CBC News Elections Dashboard",
                "url": "https://newsinteractives.cbc.ca/elections/",
                "description": "Live results and historical context for federal and provincial races.",
                "access": "Open web",
                "update_frequency": "Minutes during election night; archived thereafter.",
                "feasibility": "Medium",
                "feasibility_notes": "Timely but requires scraping or manual download; APIs not public.",
            },
        ],
    },
    "Mexico": {
        "level": "national",
        "sources": [
            {
                "name": "Instituto Nacional Electoral (INE)",
                "url": "https://www.ine.mx/",
                "description": "Official PREP and certified results for federal elections.",
                "access": "Open government data",
                "update_frequency": "PREP updates every minutes on election night; certification weeks later.",
                "feasibility": "High",
                "feasibility_notes": "Near-real-time PREP feed available; documentation in Spanish and may require scraping.",
            },
            {
                "name": "Bloomberg Línea Mexico Elections Tracker",
                "url": "https://www.bloomberglinea.com/elecciones-mexico/",
                "description": "Media-run tracker with visualization and projections.",
                "access": "Open web",
                "update_frequency": "Minutes on election night.",
                "feasibility": "Medium",
                "feasibility_notes": "Useful backup feed; HTML structure subject to change.",
            },
        ],
    },
    "Brazil": {
        "level": "national",
        "sources": [
            {
                "name": "Tribunal Superior Eleitoral (TSE) Results Portal",
                "url": "https://resultados.tse.jus.br/oficial/app/index.html",
                "description": "Official election results for presidential, gubernatorial, and legislative races.",
                "access": "Open government data",
                "update_frequency": "Near real-time on election night.",
                "feasibility": "High",
                "feasibility_notes": "Robust official feed with public API endpoints; documentation in Portuguese.",
            },
            {
                "name": "Folha/UOL Real-Time Election Coverage",
                "url": "https://temas.folha.uol.com.br/eleicoes/",
                "description": "Major media outlet providing supplementary context and turnout tracking.",
                "access": "Open web",
                "update_frequency": "Minutes.",
                "feasibility": "Medium",
                "feasibility_notes": "Good redundancy but requires HTML parsing; consider language support.",
            },
        ],
    },
    "India": {
        "level": "national",
        "sources": [
            {
                "name": "Election Commission of India – Election Results",
                "url": "https://results.eci.gov.in/",
                "description": "Official results portal for Lok Sabha and Assembly elections.",
                "access": "Open government data",
                "update_frequency": "Minutes during counting days; final certification after canvass.",
                "feasibility": "High",
                "feasibility_notes": "Structured data with JSON endpoints; uptime maintained during counting.",
            },
            {
                "name": "PRS Legislative Research Election Atlas",
                "url": "https://prsindia.org/elections",
                "description": "Aggregated results with historical comparisons.",
                "access": "Open research use",
                "update_frequency": "Days after results",
                "feasibility": "Medium",
                "feasibility_notes": "Excellent for retrospective analysis; not real-time.",
            },
        ],
    },
    "Australia": {
        "level": "national",
        "sources": [
            {
                "name": "Australian Electoral Commission Tally Room",
                "url": "https://tallyroom.aec.gov.au/",
                "description": "Official results for federal elections and referendums.",
                "access": "Open government data",
                "update_frequency": "Minutes during counts; certification after preference distribution.",
                "feasibility": "High",
                "feasibility_notes": "Reliable JSON endpoints; timezone considerations for automation.",
            },
            {
                "name": "ABC News Elections Coverage",
                "url": "https://www.abc.net.au/news/elections/",
                "description": "Live race calls and swing analysis.",
                "access": "Open web",
                "update_frequency": "Minutes during election night.",
                "feasibility": "Medium",
                "feasibility_notes": "Good context and alternative feed; scraping or manual export required.",
            },
        ],
    },
}


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
    jurisdiction_level: Optional[str] = None
    jurisdiction_sources: List[str] = field(default_factory=list)
    jurisdiction_feasibility: Optional[str] = None
    analytics_market_volume: Optional[float] = None
    analytics_market_open_interest: Optional[float] = None
    analytics_outcome_yes_price: Optional[float] = None
    analytics_outcome_no_price: Optional[float] = None
    analytics_event_slug: Optional[str] = None
    analytics_source: Optional[str] = None
    analytics_event_tags: List[str] = field(default_factory=list)
    candidate_names: List[str] = field(default_factory=list)

    def to_row(self) -> Dict[str, object]:
        """Convert the dataclass to a flat dict suitable for CSV/JSON export."""
        return {
            "event_id": self.event_id,
            "market_id": self.market_id,
            "platform": self.platform,
            "event_title": self.event_title,
            "market_question": self.market_question,
            "jurisdiction": self.jurisdiction,
            "jurisdiction_level": self.jurisdiction_level,
            "jurisdiction_sources": self.jurisdiction_sources,
            "jurisdiction_feasibility": self.jurisdiction_feasibility,
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
            "analytics_market_volume": self.analytics_market_volume,
            "analytics_market_open_interest": self.analytics_market_open_interest,
            "analytics_outcome_yes_price": self.analytics_outcome_yes_price,
            "analytics_outcome_no_price": self.analytics_outcome_no_price,
            "analytics_event_slug": self.analytics_event_slug,
            "analytics_source": self.analytics_source,
            "analytics_event_tags": self.analytics_event_tags,
            "candidate_names": self.candidate_names,
        }


NAME_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z'.-]*")
NAME_SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}
NAME_EXCLUSION_TOKENS = {
    "all",
    "america",
    "american",
    "americans",
    "beautiful",
    "big",
    "bill",
    "breaking",
    "assembly",
    "australia",
    "ballot",
    "campaign",
    "caucus",
    "city",
    "control",
    "congress",
    "congressional",
    "delegates",
    "democrat",
    "democrats",
    "democratic",
    "election",
    "elections",
    "federal",
    "general",
    "global",
    "government",
    "gov",
    "governor",
    "gubernatorial",
    "house",
    "incumbent",
    "mentions",
    "mayor",
    "minister",
    "news",
    "nomination",
    "nominee",
    "nyc",
    "party",
    "parliament",
    "politics",
    "press",
    "president",
    "presidency",
    "primary",
    "referendum",
    "representative",
    "runoff",
    "senate",
    "senator",
    "shutdown",
    "state",
    "states",
    "united",
    "vote",
    "voter",
    "voting",
    "world",
}

NLTK_RESOURCE_GROUPS = [
    (
        ("averaged_perceptron_tagger_eng", "taggers/averaged_perceptron_tagger_eng"),
        ("averaged_perceptron_tagger", "taggers/averaged_perceptron_tagger"),
    ),
    (
        ("maxent_ne_chunker_tab", "chunkers/maxent_ne_chunker_tab/english_ace_multiclass/"),
        ("maxent_ne_chunker", "chunkers/maxent_ne_chunker"),
    ),
    (("words", "corpora/words"),),
]

TOKENIZER = TreebankWordTokenizer()


def _should_add_title_variant(tokens: List[str]) -> bool:
    """Detect lowercase name-like tokens that benefit from title-casing."""
    has_alpha = any(token.isalpha() for token in tokens)
    if not has_alpha:
        return False
    return all(token.islower() for token in tokens if token.isalpha())


def _resource_available(path: str) -> bool:
    """Return True if the given NLTK data path exists."""
    try:
        nltk.data.find(path)
        return True
    except LookupError:
        return False


def _download_nltk_resource(resource: str, path: str) -> bool:
    """Download the requested NLTK resource and confirm availability."""
    try:
        nltk.download(resource, quiet=True)
        nltk.data.find(path)
        return True
    except LookupError:
        return False


@lru_cache(maxsize=1)
def ensure_nltk_resources() -> None:
    """Install required NLTK models for tokenization, tagging, and NER."""
    for group in NLTK_RESOURCE_GROUPS:
        if any(_resource_available(path) for _resource, path in group):
            continue
        for resource, path in group:
            if _download_nltk_resource(resource, path):
                break
        else:
            resource, _ = group[0]
            raise RuntimeError(
                f"Missing NLTK resource '{resource}'. Run 'python -m nltk.downloader {resource}'."
            )


def clean_candidate_name(text: str) -> str:
    """Normalize candidate names to remove stray whitespace/punctuation."""
    cleaned = " ".join(str(text).split())
    return cleaned.strip(".,;:\"'()[]{}")


def tokenize_candidate_name(name: str) -> List[str]:
    """Split a candidate name into word-like tokens."""
    return NAME_TOKEN_PATTERN.findall(name)


def is_plausible_candidate(tokens: List[str]) -> bool:
    """Filter out entities that look like generic election phrases."""
    if not tokens:
        return False
    lowered = [token.lower() for token in tokens]
    # Ignore entries composed entirely of suffixes.
    if all(token in NAME_SUFFIX_TOKENS for token in lowered):
        return False
    meaningful_tokens = [
        token for token in lowered if token not in NAME_SUFFIX_TOKENS
    ]
    if not meaningful_tokens:
        return False
    if any(token in NAME_EXCLUSION_TOKENS for token in meaningful_tokens):
        return False
    return True


def extract_candidate_names(texts: Iterable[str]) -> List[str]:
    """Return unique PERSON entities detected within the provided text sources."""
    texts = [text for text in texts if text]
    if not texts:
        return []

    ensure_nltk_resources()
    seen: Dict[str, str] = {}
    ordered_names: List[str] = []
    token_cache: Dict[str, List[str]] = {}

    for text in texts:
        text_str = str(text)
        base_tokens = TOKENIZER.tokenize(text_str)
        variants = [text_str]
        if _should_add_title_variant(base_tokens):
            title_text = string.capwords(text_str)
            if title_text != text_str:
                variants.append(title_text)

        for variant in variants:
            tokens = TOKENIZER.tokenize(variant)
            if not tokens:
                continue
            tagged_tokens = pos_tag(tokens)
            chunked = ne_chunk(tagged_tokens)
            buffer: List[str] = []

            def flush_buffer() -> None:
                if not buffer:
                    return
                candidate = clean_candidate_name(" ".join(buffer))
                buffer.clear()
                if not candidate:
                    return
                candidate_tokens = tokenize_candidate_name(candidate)
                if not is_plausible_candidate(candidate_tokens):
                    return
                normalized = candidate.lower()
                if normalized in seen:
                    return
                seen[normalized] = candidate
                ordered_names.append(candidate)
                token_cache[candidate] = candidate_tokens

            for node in chunked:
                if isinstance(node, Tree) and node.label() == "PERSON":
                    buffer.extend(token for token, _tag in node.leaves())
                else:
                    flush_buffer()
            flush_buffer()

    if not ordered_names:
        return []

    tokenized = {
        name: token_cache.get(name) or tokenize_candidate_name(name) for name in ordered_names
    }
    multisets = [
        {token.lower() for token in tokens if token}
        for name, tokens in tokenized.items()
        if len(tokens) > 1
    ]

    filtered: List[str] = []
    for name in ordered_names:
        tokens = tokenized[name]
        if len(tokens) == 1:
            token_lower = tokens[0].lower()
            if any(token_lower in multi for multi in multisets):
                continue
        filtered.append(name)
    return filtered


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


def is_election_by_tags(tags: Optional[Iterable[str]]) -> bool:
    """Strictly decide if a market is an election (candidate) market using tags.

    Intentionally excludes broad politics topics and generic "vote" references
    so that we only keep contests for offices (president, senate, governor, etc.).
    """
    if not tags:
        return False
    lowered = [str(t).strip().lower() for t in tags if t]
    include = {
        # Core election concepts (candidate races)
        "election",
        "elections",
        "general election",
        "presidential",
        "president",
        "primary",
        "runoff",
        "by-election",
        "byelection",
        "leadership election",
        # Offices
        "senate",
        "house",
        "congress",
        "parliament",
        "assembly",
        "legislative",
        "governor",
        "gubernatorial",
        "mayor",
        "minister",
    }
    exclude = {"sports", "crypto", "finance", "stocks", "nfl", "nba", "tennis", "mlb", "nhl"}
    if any(t in exclude for t in lowered):
        # Require a strong election keyword if tags include excluded domains
        return any(t in include for t in lowered)
    # Otherwise still require a strong election keyword; do NOT accept generic 'politics'
    return any(t in include for t in lowered)


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

    tolerance = 0.8  # Markets typically settle near binary bounds.
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


def is_election_market_question(question: str) -> bool:
    """Ensure the specific market question targets an electoral outcome."""
    if not question:
        return False
    return any(pattern.search(question) for pattern in QUESTION_PATTERNS)


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a variety of ISO-ish strings into aware datetimes."""
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    if " " in cleaned and "T" not in cleaned:
        cleaned = cleaned.replace(" ", "T")
    if re.search(r"\+\d{2}$", cleaned):
        cleaned += ":00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def determine_closed_datetime(
    record: MarketRecord, analytics_entry: Optional[Dict[str, object]]
) -> Optional[datetime]:
    """Pick the best-available closed timestamp for recency filtering."""
    candidates = [
        record.closed_time,
        record.end_date,
        record.start_date,
    ]
    if analytics_entry:
        candidates.extend(
            [
                analytics_entry.get("market_close_date"),
                analytics_entry.get("event_end_date"),
            ]
        )
    for candidate in candidates:
        dt_value = parse_iso_datetime(candidate)
        if dt_value:
            return dt_value.astimezone(timezone.utc)
    return None


def load_closed_election_markets(
    analytics_lookup: Optional[Dict[str, Dict[str, object]]] = None,
) -> List[MarketRecord]:
    """Fetch and flatten Polymarket markets into MarketRecord objects.

    Election filtering prioritizes PolymarketAnalytics `event_tags` when
    available; falls back to text heuristics otherwise.
    """
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
            # Do not pre-filter by event text; rely on per-market tags/heuristics.

            event_id = str(event.get("id"))
            title = event.get("title", "")
            category = event.get("category")
            liquidity_clob = parse_float(event.get("liquidityClob"))
            open_interest = parse_float(event.get("openInterest"))

            for market in event.get("markets", []):
                market_id = str(market.get("id"))
                volume = parse_float(market.get("volume"))
                resolution_source = market.get("resolutionSource") or event.get("resolutionSource")
                outcomes_list = parse_outcomes(market.get("outcomes", "[]"))
                clob_token_ids = parse_outcomes(market.get("clobTokenIds", "[]"))
                question = market.get("question", "")
                # Election filtering: prefer tags; if analytics provided, require tag match
                if analytics_lookup is not None:
                    analytics_entry = analytics_lookup.get(market_id)
                    allow = bool(analytics_entry and is_election_by_tags(analytics_entry.get("event_tags")))
                else:
                    # Fallback heuristics without analytics
                    allow = is_election_event(event) and is_election_market_question(question)
                if not allow:
                    continue

                record = MarketRecord(
                    event_id=event_id,
                    market_id=market_id,
                    platform="Polymarket",
                    event_title=title,
                    market_question=question,
                    jurisdiction=None,
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
                )
                records.append(record)

        offset += PAGE_LIMIT
        if offset > MAX_OFFSET:
            break

        # Polymarket's API is public but we stay polite.
        time.sleep(0.4)

    return records


def fetch_polymarket_analytics_snapshot() -> Dict[str, Dict[str, object]]:
    """Pull market metadata from PolymarketAnalytics for enrichment."""
    payload = {
        "searchQuery": "",
        "selectedStatuses": ["Closed"],
        "selectedSources": ["polymarket"],
    }
    try:
        response = requests.post(
            ANALYTICS_COMBINED_MARKETS_URL,
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("Failed to retrieve data from PolymarketAnalytics") from exc

    data = response.json().get("data", [])
    analytics_index: Dict[str, Dict[str, object]] = {}
    for entry in data:
        market_id = str(entry.get("market_id"))
        analytics_index[market_id] = entry
    return analytics_index


def enrich_with_analytics(
    records: Iterable[MarketRecord],
    analytics_lookup: Dict[str, Dict[str, object]],
) -> None:
    """Merge PolymarketAnalytics metrics into MarketRecord objects."""
    for record in records:
        entry = analytics_lookup.get(record.market_id)
        if entry:
            record.analytics_market_volume = parse_float(entry.get("market_volume"))
            record.analytics_market_open_interest = parse_float(entry.get("market_open_interest"))
            record.analytics_outcome_yes_price = parse_float(entry.get("outcome_1_price"))
            record.analytics_outcome_no_price = parse_float(entry.get("outcome_2_price"))
            record.analytics_event_slug = entry.get("event_slug")
            record.analytics_source = entry.get("source")
            tags = entry.get("event_tags") or []
            if isinstance(tags, list):
                record.analytics_event_tags = [str(tag) for tag in tags]
            elif isinstance(tags, str):
                record.analytics_event_tags = [tag.strip() for tag in tags.split(",") if tag.strip()]

        candidate_inputs: List[str] = []
        candidate_inputs.extend(record.analytics_event_tags or [])
        if record.resolution_notes:
            candidate_inputs.append(record.resolution_notes)

        record.candidate_names = extract_candidate_names(candidate_inputs)


def match_us_state(text: str) -> Optional[Tuple[str, str]]:
    """Identify US state references from a blob of text."""
    lowered = text.lower()
    for state_key, state_name in US_STATE_KEYWORDS.items():
        if re.search(rf"\b{re.escape(state_key)}\b", lowered):
            return state_name, "state"

    district_match = re.search(r"\b([A-Z]{2})[-\u2013 ]?(\d{1,2})\b", text)
    if district_match:
        abbr = district_match.group(1)
        state_name = US_STATE_ABBREV_TO_NAME.get(abbr)
        if state_name:
            return state_name, "district"

    senate_pattern = re.search(r"\b([A-Z]{2})\b.*\bsenate\b", text, re.IGNORECASE)
    if senate_pattern:
        abbr = senate_pattern.group(1).upper()
        state_name = US_STATE_ABBREV_TO_NAME.get(abbr)
        if state_name:
            return state_name, "state"

    governor_pattern = re.search(r"\b([A-Z]{2})\b.*\bgovernor", text, re.IGNORECASE)
    if governor_pattern:
        abbr = governor_pattern.group(1).upper()
        state_name = US_STATE_ABBREV_TO_NAME.get(abbr)
        if state_name:
            return state_name, "state"
    return None


def infer_jurisdiction(
    record: MarketRecord,
    analytics_entry: Optional[Dict[str, object]],
) -> Tuple[str, str]:
    """Infer jurisdiction name and level from record metadata."""
    components = [
        record.event_title or "",
        record.market_question or "",
        record.category or "",
        record.resolution_notes or "",
    ]
    if analytics_entry:
        components.extend(
            [
                analytics_entry.get("event_title") or "",
                analytics_entry.get("event_description") or "",
            ]
        )
    text_blob = " ".join(component for component in components if component)
    if not text_blob:
        return "Unclassified", "unknown"

    state_match = match_us_state(text_blob)
    if state_match:
        return state_match

    lowered = text_blob.lower()
    for keyword, (jurisdiction, level) in COUNTRY_KEYWORDS.items():
        if keyword in lowered:
            return jurisdiction, level

    if "mayor" in lowered and "london" in lowered:
        return "United Kingdom - London", "city"
    if "mayor" in lowered and "new york" in lowered:
        return "United States - New York City", "city"

    if "senate" in lowered or "house" in lowered or "congress" in lowered:
        return "United States", "national"
    if "parliament" in lowered:
        return "European Union", "supra-national"

    return "Unclassified", "unknown"


def compute_recent_records(
    records: List[MarketRecord],
    analytics_lookup: Dict[str, Dict[str, object]],
    lookback_days: int = RECENT_WINDOW_DAYS,
) -> List[MarketRecord]:
    """Filter records to those closed within the lookback window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    filtered: List[MarketRecord] = []
    for record in records:
        closed_dt = determine_closed_datetime(record, analytics_lookup.get(record.market_id))
        if closed_dt and closed_dt >= cutoff:
            if not record.closed_time:
                record.closed_time = closed_dt.isoformat()
            filtered.append(record)
    return filtered


def make_source(
    name: str,
    url: str,
    description: str,
    access: str,
    update_frequency: str,
    feasibility: str,
    feasibility_notes: str,
) -> Dict[str, str]:
    return {
        "name": name,
        "url": url,
        "description": description,
        "access": access,
        "update_frequency": update_frequency,
        "feasibility": feasibility,
        "feasibility_notes": feasibility_notes,
    }


def build_state_assessment(state_name: str, level: str) -> Dict[str, object]:
    """Assemble data sources for a US state or territory."""
    official_url = STATE_OFFICIAL_SITES.get(state_name)
    sources: List[Dict[str, str]] = []
    if official_url:
        sources.append(
            make_source(
                name=f"{state_name} election office",
                url=official_url,
                description="Certified statewide and county results plus candidate filings.",
                access="Official (open web)",
                update_frequency="Nightly during canvass; certification per statutory calendar.",
                feasibility="High",
                feasibility_notes="Authoritative counts with predictable publication cadence; may require PDF/CSV parsing.",
            )
        )
    else:
        sources.append(
            make_source(
                name=f"{state_name} election authority (via NASS directory)",
                url="https://www.nass.org/can-i-vote",
                description="Official portal linking to state-managed election offices.",
                access="Open web",
                update_frequency="Updated each cycle as offices publish new info.",
                feasibility="Medium",
                feasibility_notes="One-click path to official data even if direct link unknown; manual navigation required.",
            )
        )

    state_slug = state_name.replace(" ", "_")
    sources.append(
        make_source(
            name=f"Ballotpedia – {state_name} elections portal",
            url=f"https://ballotpedia.org/{state_slug}",
            description="Aggregated election calendars, candidate rosters, and unofficial vote tallies.",
            access="Open web (CC BY-SA)",
            update_frequency="Daily during election periods; slower off-cycle.",
            feasibility="Medium",
            feasibility_notes="Rich historical context; manual scraping necessary for automated ingestion.",
        )
    )
    sources.append(
        make_source(
            name="Decision Desk HQ Results Feed",
            url="https://results.decisiondeskhq.com/",
            description="Paid API for precinct and county level tallies across US states.",
            access="Commercial API",
            update_frequency="Minutes on election night; hourly thereafter.",
            feasibility="High",
            feasibility_notes="Fast and structured but gated behind subscription and integration work.",
        )
    )
    return {"level": level, "sources": sources}


def build_generic_assessment(name: str, level: str) -> Dict[str, object]:
    """Fallback assessment when no explicit template exists."""
    sources = [
        make_source(
            name=f"Reuters Elections Coverage – {name}",
            url="https://www.reuters.com/world/",
            description="Global wire-service election reporting with near-real-time updates.",
            access="Open web",
            update_frequency="Minutes during major elections.",
            feasibility="Medium",
            feasibility_notes="Reliable narrative coverage; data extraction requires scraping and manual QA.",
        ),
        make_source(
            name=f"Local election commission ({name})",
            url="",
            description="Official jurisdiction election authority (URL to be verified per contest).",
            access="Varies",
            update_frequency="Certification timeline defined by statute.",
            feasibility="Low",
            feasibility_notes="Research required to identify authoritative feed; placeholder for analyst follow-up.",
        ),
    ]
    return {"level": level, "sources": sources}


def build_jurisdiction_assessment(jurisdiction: str, level: str) -> Dict[str, object]:
    """Return an assessment bundle for the requested jurisdiction."""
    if jurisdiction in COUNTRY_SOURCE_LIBRARY:
        return COUNTRY_SOURCE_LIBRARY[jurisdiction]
    if jurisdiction in STATE_OFFICIAL_SITES or jurisdiction in US_STATE_NAME_TO_ABBREV:
        return build_state_assessment(jurisdiction, level)
    if jurisdiction.startswith("United States -"):
        state_name = jurisdiction.split("-", 1)[1].strip()
        return build_state_assessment(state_name, "city" if "City" in jurisdiction else "state")
    return build_generic_assessment(jurisdiction, level)


def aggregate_feasibility(sources: List[Dict[str, str]]) -> Tuple[str, str]:
    """Compute aggregate feasibility rating with supporting note."""
    if not sources:
        return "Unknown", "No documented sources; manual research required."
    best_source = max(sources, key=lambda src: FEASIBILITY_ORDER.get(src["feasibility"], 0))
    rating = best_source["feasibility"]
    note = best_source["feasibility_notes"]
    return rating, note


def build_jurisdiction_mappings(
    records: Iterable[MarketRecord],
    analytics_lookup: Dict[str, Dict[str, object]],
) -> Dict[str, Dict[str, object]]:
    """Annotate records with jurisdiction metadata and build source catalog."""
    catalog: Dict[str, Dict[str, object]] = {}
    for record in records:
        analytics_entry = analytics_lookup.get(record.market_id)
        jurisdiction, level = infer_jurisdiction(record, analytics_entry)
        record.jurisdiction = jurisdiction
        record.jurisdiction_level = level

        if jurisdiction not in catalog:
            catalog[jurisdiction] = build_jurisdiction_assessment(jurisdiction, level)

        sources = catalog[jurisdiction]["sources"]
        record.jurisdiction_sources = [source["name"] for source in sources]
        rating, _notes = aggregate_feasibility(sources)
        record.jurisdiction_feasibility = rating

    return catalog


def write_market_dataset(
    path: pathlib.Path,
    records: Iterable[MarketRecord],
) -> None:
    """Serialize market data to CSV and JSON."""
    records_list = list(records)
    if not records_list:
        raise RuntimeError("No market records available for export.")

    csv_path = path.with_suffix(".csv")
    json_path = path.with_suffix(".json")

    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        fieldnames = list(records_list[0].to_row().keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records_list:
            row = record.to_row()
            row["jurisdiction_sources"] = "|".join(record.jurisdiction_sources or [])
            row["candidate_names"] = ", ".join(record.candidate_names or [])
            writer.writerow(row)

    with json_path.open("w", encoding="utf-8") as jsonfile:
        json.dump([record.to_row() for record in records_list], jsonfile, indent=2)


def write_jurisdiction_sources(
    catalog: Dict[str, Dict[str, object]],
) -> None:
    """Export jurisdiction source catalog to CSV and JSON."""
    json_path = DATA_DIR / "polymarket_jurisdiction_sources.json"
    csv_path = DATA_DIR / "polymarket_jurisdiction_sources.csv"

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=2)

    fieldnames = [
        "jurisdiction",
        "level",
        "source_name",
        "url",
        "description",
        "access",
        "update_frequency",
        "feasibility",
        "feasibility_notes",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for jurisdiction, data in catalog.items():
            level = data.get("level", "")
            for source in data.get("sources", []):
                writer.writerow(
                    {
                        "jurisdiction": jurisdiction,
                        "level": level,
                        "source_name": source["name"],
                        "url": source["url"],
                        "description": source["description"],
                        "access": source["access"],
                        "update_frequency": source["update_frequency"],
                        "feasibility": source["feasibility"],
                        "feasibility_notes": source["feasibility_notes"],
                    }
                )


def main() -> None:
    # Fetch analytics first so we can use event_tags for cleaner filtering
    analytics_lookup = fetch_polymarket_analytics_snapshot()
    records = load_closed_election_markets(analytics_lookup)
    enrich_with_analytics(records, analytics_lookup)

    recent_records = compute_recent_records(records, analytics_lookup, RECENT_WINDOW_DAYS)
    if not recent_records:
        raise RuntimeError("No recent election markets found in the last two years.")

    jurisdiction_catalog = build_jurisdiction_mappings(recent_records, analytics_lookup)
    write_jurisdiction_sources(jurisdiction_catalog)

    full_dataset_path = DATA_DIR / "polymarket_election_markets_closed_2y"
    write_market_dataset(full_dataset_path, recent_records)

    print(f"Wrote {len(recent_records)} market records to {full_dataset_path.with_suffix('.csv')}")


if __name__ == "__main__":
    main()
