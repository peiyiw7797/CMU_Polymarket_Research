"""
Map each target election market to authoritative campaign finance data sources.

The mapping references both public (openly accessible) and commercial (paid or
API-gated) datasets so analysts can align financial disclosures with market
price histories. Paid sources are explicitly flagged so procurement can manage
escalations before any spend is incurred.

Outputs
-------
new_research/data/finance_source_mapping.json
new_research/data/finance_source_mapping.csv
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MARKET_FILE = DATA_DIR / "polymarket_election_markets_top20.json"


def load_markets() -> Dict[str, Dict[str, object]]:
    with MARKET_FILE.open("r", encoding="utf-8") as handle:
        markets = json.load(handle)
    return {row["market_id"]: row for row in markets}


def make_source(
    name: str,
    url: str,
    license_note: str,
    access_notes: str,
    requires_escalation: bool = False,
) -> Dict[str, object]:
    return {
        "name": name,
        "url": url,
        "license": license_note,
        "access_notes": access_notes,
        "requires_escalation": requires_escalation,
    }


def build_mapping(markets: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    """Assemble finance data sources per market."""

    # Shared source libraries to keep entries consistent.
    us_federal_public = [
        make_source(
            "Federal Election Commission Data Portal",
            "https://www.fec.gov/data/",
            "U.S. government work (public domain under 17 U.S.C. §105).",
            "Includes committee- and candidate-level receipts, disbursements, and independent expenditure filings. Daily bulk CSV dumps allow reproducible merges.",
        ),
        make_source(
            "OpenSecrets Presidential Race Dashboard",
            "https://www.opensecrets.org/2024-presidential-race",
            "Creative Commons BY-NC 3.0; non-commercial redistribution with attribution.",
            "Aggregated fundraising and outside spending totals; API requires free account and enforces non-commercial restrictions.",
        ),
        make_source(
            "ProPublica Campaign Finance API",
            "https://www.propublica.org/datastore/api/propublica-campaign-finance-api",
            "Free for non-commercial use; registration and attribution required.",
            "Provides normalized FEC filings (Schedule A/B) with REST pagination for programmatic pulls.",
        ),
    ]

    us_federal_paid = [
        make_source(
            "AdImpact Political Advertising Intelligence",
            "https://www.adimpact.com/",
            "Commercial license; NDAs and seat fees required.",
            "Creative spend and impression estimates for TV, CTV, and digital placements. Use to contextualize ad blitzes around market price swings.",
            requires_escalation=True,
        ),
        make_source(
            "Kantar/CMAG Political Advertising Database",
            "https://www.kantar.com/north-america/expertise/media/planning/advertising-intelligence",
            "Commercial license; negotiated contracts.",
            "Legacy TV and radio ad tracking with flight dates. Useful for corroborating spend spikes.",
            requires_escalation=True,
        ),
    ]

    speaker_specific_public = [
        make_source(
            "Clerk of the U.S. House – Financial Disclosure Reports",
            "https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure",
            "U.S. government work (public domain).",
            "Annual disclosures covering leadership PACs and personal finances of House members, complementing FEC filings.",
        ),
    ]

    speaker_specific_paid = [
        make_source(
            "Quorum Federal Lobbying & PAC Contributions",
            "https://www.quorum.us/",
            "Commercial SaaS; contract required.",
            "Tracks leadership PAC receipts and corporate PAC activity tied to House leadership races.",
            requires_escalation=True,
        )
    ]

    pa_public = [
        make_source(
            "Pennsylvania DOS Campaign Finance Reporting",
            "https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Pages/default.aspx",
            "Official state portal; reuse permitted with attribution under Pennsylvania open records guidance.",
            "Provides CSV exports of statewide and federal filings attributed to Pennsylvania committees with reporting-period timestamps.",
        ),
        make_source(
            "FollowTheMoney – Pennsylvania State Overview",
            "https://www.followthemoney.org/tools/state/pennsylvania/",
            "CC BY 4.0 with attribution; mirrors state filings.",
            "Aggregated state-level receipts and independent expenditures, harmonized across election cycles.",
        ),
    ]

    pa_paid = [
        make_source(
            "TargetSmart VoterBase + Finance Enhancements",
            "https://targetsmart.com/",
            "Commercial; multi-year contract.",
            "Provides predictive donor scoring and near-real-time ingestion of Pennsylvania filings.",
            requires_escalation=True,
        )
    ]

    nv_public = [
        make_source(
            "Nevada Secretary of State – Campaign Finance",
            "https://www.nvsos.gov/sos/elections/campaign-finance",
            "Official state publication; reuse allowed with attribution per Nevada Revised Statutes 239.",
            "Candidate and committee filings with XML/CSV export, including filing acceptance timestamps.",
        ),
        make_source(
            "FollowTheMoney – Nevada State Overview",
            "https://www.followthemoney.org/tools/state/nevada/",
            "CC BY 4.0 with attribution.",
            "Normalizes Nevada committee filings and independent expenditures over time.",
        ),
    ]

    nv_paid = [
        make_source(
            "Civis Analytics – Battleground Finance Feeds",
            "https://www.civisanalytics.com/",
            "Commercial; custom data agreement.",
            "Combines Nevada filings with modeling of small-dollar donors; ingest schedule mirrored weekly.",
            requires_escalation=True,
        )
    ]

    wi_public = [
        make_source(
            "Wisconsin Campaign Finance Information System (CFIS)",
            "https://cfis.wi.gov",
            "State-hosted database; terms allow reuse for non-commercial analysis with attribution.",
            "Exports campaign finance filings with coverage period metadata in CSV and XML.",
        ),
        make_source(
            "Wisconsin Ethics Commission Open Data",
            "https://ethics.wi.gov/Pages/CampaignFinance/Opendata.aspx",
            "State open data licence; attribution required.",
            "Provides bulk CSV downloads for receipts and disbursements matched to CFIS identifiers.",
        ),
    ]

    wi_paid = [
        make_source(
            "L2 Political – Wisconsin Donor Files",
            "https://l2-data.com/",
            "Commercial; subscription required.",
            "Links CFIS filings to voter files for microtargeting analysis.",
            requires_escalation=True,
        )
    ]

    canada_public = [
        make_source(
            "Elections Canada – Political Financing Reports",
            "https://www.elections.ca/content.aspx?section=fin&document=index&lang=e",
            "Open Government Licence – Canada.",
            "Quarterly and event-based returns for registered parties, candidates, and leadership contestants with CSV exports.",
        ),
        make_source(
            "Elections Canada Open Data Portal",
            "https://www.elections.ca/content.aspx?section=res&dir=rec/dat&document=index&lang=e",
            "Open Government Licence – Canada.",
            "Machine-readable datasets covering contributions, loans, and expenses dating back to 1993.",
        ),
    ]

    canada_paid = [
        make_source(
            "iPolitics INTEL Fundraising Tracker",
            "https://www.ipolitics.ca/intel/",
            "Subscription service.",
            "Provides curated fundraising alerts and early access to Elections Canada filings.",
            requires_escalation=True,
        ),
        make_source(
            "Politico Pro Canada Elections Coverage",
            "https://www.politico.com/pro",
            "Subscription (enterprise).",
            "Delivers synthesized finance and lobbying intelligence around Canadian federal races.",
            requires_escalation=True,
        ),
    ]

    romania_public = [
        make_source(
            "Autoritatea Electorală Permanentă – Portalul Financiar",
            "https://finantarepartide.ro/",
            "Romanian public sector data; reuse allowed with attribution under Law 544/2001, but interface remains Romanian-only.",
            "Provides PDFs and XLS exports of income/expense statements by electoral period.",
        ),
        make_source(
            "Expert Forum (EFOR) – Banometru Dashboard",
            "https://expertforum.ro/banometru/",
            "Creative Commons BY 4.0.",
            "Civil-society aggregation of AEP disclosures with machine-readable timelines.",
        ),
    ]

    romania_paid = [
        make_source(
            "CEIC Data – Romania Government & Politics",
            "https://www.ceicdata.com/en",
            "Commercial subscription.",
            "Includes harmonized election finance and public subsidy series for Romania.",
            requires_escalation=True,
        )
    ]

    south_korea_public = [
        make_source(
            "National Election Commission – Political Funds Integrated Information System",
            "https://nef.nec.go.kr/",
            "Korean public sector licence (KOGL Type 1); attribution required.",
            "Campaign finance ledgers and subsidy disclosures with filing dates; site requires Korean interface navigation.",
        ),
        make_source(
            "Korean Statistical Information Service (KOSIS) – Political Funds",
            "https://kosis.kr/eng/",
            "KOGL Type 1; attribution required.",
            "Aggregated annual receipts/expenditures by party and election.",
        ),
    ]

    south_korea_paid = [
        make_source(
            "Yonhap Infomax Election Finance",
            "https://www.yonhapinfomax.co.kr/",
            "Commercial subscription.",
            "Real-time media monitoring and finance snapshots translated to English; essential for intra-day updates.",
            requires_escalation=True,
        )
    ]

    poland_public = [
        make_source(
            "National Electoral Commission (PKW) – Election Finance Disclosures",
            "https://wybory.gov.pl",
            "Official government publication; reuse permitted with attribution.",
            "Publishes committee financial statements and reimbursement reports post-election (PDF and XLS formats).",
        ),
        make_source(
            "OpenSpending Poland – Campaign Finance Explorer",
            "https://openspending.pl/",
            "CC BY 4.0 license.",
            "Civil society digitization of PKW filings with CSV exports.",
        ),
    ]

    poland_paid = [
        make_source(
            "Emerging Europe Monitor (Fitch Solutions)",
            "https://www.fitchsolutions.com/",
            "Commercial subscription.",
            "Provides capital-markets oriented election finance briefings for Central Europe.",
            requires_escalation=True,
        )
    ]

    alignment_notes = {
        "us_presidential": "Use FEC coverage_end_date to align quarterly and 48-hour filings with market price timestamps. After each report, forward-fill cumulative totals until the next filing to maintain daily continuity.",
        "us_state_pa": "Pennsylvania filings follow pre-primary, pre-general, and 24-hour schedules. Normalize by converting each filing's reporting_period_end_date to UTC and aligning with matching market dates; forward-fill between filings.",
        "us_state_nv": "Nevada requires pre-primary/general and 24-hour reports. Align using filing acceptance datetime, adjusting for Pacific Time when merging with UTC price data.",
        "us_state_wi": "Wisconsin CFIS updates nightly. Capture the `report_period_end_date` and expand across days until subsequent filings.",
        "us_speaker": "Candidate committees file monthly; leadership PACs file semi-annually. Join on FEC committee ID, using filing_received_date to sync with market moves around leadership announcements.",
        "canada_federal": "Elections Canada event returns include `ReportingPeriodEndDate`. Convert to UTC and align to the daily close in the price series. Supplement with interim leadership contestant filings as available.",
        "romania_presidential": "AEP posts pre-campaign and campaign expenditure statements stamped with submission dates. Manual OCR may be required; align to submission_date to observe lagged effects on market prices.",
        "south_korea_presidential": "NEC system exports monthly ledgers with `report_date`. Use Korean Standard Time when mapping to price data; expect updates within 7 days of filing deadlines.",
        "poland_presidential": "PKW releases final committee statements approximately 3 months post-election. Align using report publication date, noting that intra-campaign finance transparency is limited.",
    }

    mapping: List[Dict[str, object]] = []

    def add_entry(market_id: str, jurisdiction: str, office_level: str,
                  public_sources: List[Dict[str, object]],
                  commercial_sources: List[Dict[str, object]],
                  alignment_key: str,
                  additional_notes: str = "") -> None:
        market = markets[market_id]
        mapping.append(
            {
                "market_id": market_id,
                "market_question": market["market_question"],
                "jurisdiction": jurisdiction,
                "office_level": office_level,
                "public_sources": public_sources,
                "commercial_sources": commercial_sources,
                "timeline_alignment_guidance": alignment_notes[alignment_key],
                "additional_notes": additional_notes,
            }
        )

    # US presidential & VP nomination.
    for mid in ["511754", "511753", "504873", "504868"]:
        add_entry(
            market_id=mid,
            jurisdiction="United States – Federal",
            office_level="Presidential / Vice-Presidential nomination",
            public_sources=us_federal_public,
            commercial_sources=us_federal_paid,
            alignment_key="us_presidential",
            additional_notes="Pair presidential committee filings with joint fundraising committee data (e.g., Trump Save America, Biden Victory Fund) for completeness.",
        )

    # Pennsylvania margins.
    for mid in ["507242", "507237"]:
        add_entry(
            market_id=mid,
            jurisdiction="United States – Pennsylvania",
            office_level="Presidential statewide vote margin",
            public_sources=us_federal_public + pa_public,
            commercial_sources=us_federal_paid + pa_paid,
            alignment_key="us_state_pa",
            additional_notes="Blend statewide filings with county-level late contribution notices for final 72-hour adjustments.",
        )

    # Nevada margin.
    add_entry(
        market_id="509218",
        jurisdiction="United States – Nevada",
        office_level="Presidential statewide vote margin",
        public_sources=us_federal_public + nv_public,
        commercial_sources=us_federal_paid + nv_paid,
        alignment_key="us_state_nv",
        additional_notes="Nevada posts filings in Pacific Time; convert to UTC before merging with market timestamps.",
    )

    # Wisconsin margin.
    add_entry(
        market_id="509188",
        jurisdiction="United States – Wisconsin",
        office_level="Presidential statewide vote margin",
        public_sources=us_federal_public + wi_public,
        commercial_sources=us_federal_paid + wi_paid,
        alignment_key="us_state_wi",
        additional_notes="CFIS distinguishes between state and federal committees; filter on federal office to match presidential vote margins.",
    )

    # Speaker of the House contests.
    for mid in ["512246", "512245", "512248"]:
        add_entry(
            market_id=mid,
            jurisdiction="United States – U.S. House of Representatives",
            office_level="House leadership election",
            public_sources=us_federal_public + speaker_specific_public,
            commercial_sources=us_federal_paid + speaker_specific_paid,
            alignment_key="us_speaker",
            additional_notes="Leadership races also hinge on NRCC/DCCC transfers; capture party committee filings for a holistic view.",
        )

    # Canadian federal leadership markets.
    for mid in ["516767", "516769"]:
        add_entry(
            market_id=mid,
            jurisdiction="Canada – Federal",
            office_level="Prime Minister / party leadership",
            public_sources=canada_public,
            commercial_sources=canada_paid,
            alignment_key="canada_federal",
            additional_notes="Include Elections Canada event-specific expense reports once the writ is dropped for the 45th general election.",
        )

    # Romania presidential markets.
    for mid in ["512340", "519075", "519068"]:
        add_entry(
            market_id=mid,
            jurisdiction="Romania – National",
            office_level="Presidential election",
            public_sources=romania_public,
            commercial_sources=romania_paid,
            alignment_key="romania_presidential",
            additional_notes="AEP publishes reimbursements in PDF; plan for OCR or manual digitization to generate structured time series.",
        )

    # South Korea presidential markets.
    for mid in ["534189", "534197", "534220"]:
        add_entry(
            market_id=mid,
            jurisdiction="South Korea – National",
            office_level="Presidential election",
            public_sources=south_korea_public,
            commercial_sources=south_korea_paid,
            alignment_key="south_korea_presidential",
            additional_notes="NEC portal requires Korean language navigation; coordinate with bilingual analyst for data extraction.",
        )

    # Poland presidential market.
    add_entry(
        market_id="514139",
        jurisdiction="Poland – National",
        office_level="Presidential election",
        public_sources=poland_public,
        commercial_sources=poland_paid,
        alignment_key="poland_presidential",
        additional_notes="PKW releases deposits and reimbursement schedules; interim fundraising transparency is limited.",
    )

    return mapping


def write_outputs(mapping: List[Dict[str, object]]) -> None:
    json_path = DATA_DIR / "finance_source_mapping.json"
    csv_path = DATA_DIR / "finance_source_mapping.csv"

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(mapping, handle, indent=2, ensure_ascii=False)

    # Flatten nested fields for quick spreadsheet review.
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "market_id",
            "market_question",
            "jurisdiction",
            "office_level",
            "public_sources",
            "commercial_sources",
            "timeline_alignment_guidance",
            "additional_notes",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in mapping:
            public_flat = "; ".join(
                f"{src['name']} ({src['url']}; {src['license']})"
                for src in row["public_sources"]
            )
            commercial_flat = "; ".join(
                f"{src['name']} ({src['url']}; {'Escalation required' if src['requires_escalation'] else 'Standard access'})"
                for src in row["commercial_sources"]
            )
            writer.writerow(
                {
                    "market_id": row["market_id"],
                    "market_question": row["market_question"],
                    "jurisdiction": row["jurisdiction"],
                    "office_level": row["office_level"],
                    "public_sources": public_flat,
                    "commercial_sources": commercial_flat,
                    "timeline_alignment_guidance": row["timeline_alignment_guidance"],
                    "additional_notes": row["additional_notes"],
                }
            )

    print(f"Wrote finance mapping for {len(mapping)} markets to {json_path}")


def main() -> None:
    markets = load_markets()
    mapping = build_mapping(markets)
    write_outputs(mapping)


if __name__ == "__main__":
    main()
