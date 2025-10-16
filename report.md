# Closed Election Market Liquidity & Finance Mapping

## 1. Market Universe & Key Metrics

- Source data: `data/polymarket_election_markets_top20.csv` (generated via `polymarket_market_collector.py`).
- All markets are closed Polymarket contracts that cleared between Nov 2024 and Jun 2025; every entry includes the final resolved outcome and daily price history (`data/polymarket_price_history_top20.json`).
- Table highlights total notional volume (USD) and notes where Polymarket's legacy API omits liquidity fields for archived markets.

| Market ID | Question | Volume (USD m) | Liquidity | Liquidity (CLOB) | Resolution |
|---|---|---|---|---|---|
| 511754 | Will Donald Trump be inaugurated? | 400.4 |  |  | Yes |
| 512340 | Will Nicolae Ciucă win the 2024 Romanian Presidential election? | 326.5 |  |  | No |
| 507242 | Will the Democratic candidate win Pennsylvania by 1.5%-2.0%? | 109.1 |  |  | No |
| 504873 | Will Gavin Newsom be D-nom for VP on Election Day? | 92.9 |  |  | No |
| 534189 | Will Lee Jae-myung be elected the next president of South Korea? | 88.0 |  |  | Yes |
| 519075 | Will Hunor Kelemen win the Romanian presidential election? | 80.9 |  |  | No |
| 534197 | Will Kim Moon-soo be elected the next president of South Korea? | 73.2 |  |  | No |
| 511753 | Will Kamala Harris be inaugurated? | 72.2 |  |  | No |
| 516767 | Will Pierre Poilievre be the next Canadian Prime Minister? | 67.4 |  |  | No |
| 512246 | Will Austin Scott be the first elected Speaker of the House for the 119th congress? | 67.3 |  |  | No |
| 512245 | Will Jack Bergman be the first elected Speaker of the House for the 119th congress? | 58.6 |  |  | No |
| 512248 | Will Elise Stefanik be the first elected Speaker of the House for the 119th congress? | 54.2 |  |  | No |
| 534220 | Will Lee Jun-seok be elected the next president of South Korea? | 51.9 |  |  | No |
| 519068 | Will Nicușor Dan win the Romanian presidential election? | 51.0 |  |  | Yes |
| 509218 | Will the Democratic candidate win Nevada by 3.0%-4.0%?  | 47.8 |  |  | No |
| 514139 | Will Rafał Trzaskowski be the next President of Poland? | 40.9 |  |  | No |
| 509188 | Will the Democratic candidate win Wisconsin by 3%-4.0%?  | 37.4 |  |  | No |
| 516769 | Will Mark Carney be the next Canadian Prime Minister? | 36.4 |  |  | Yes |
| 504868 | Will Michelle Obama be D-nom for VP on Election Day? | 36.3 |  |  | No |
| 507237 | Will the Democratic candidate win Pennsylvania by 0.5%-1%? | 33.1 |  |  | No |

**Note:** Polymarket's gamma API (`https://gamma-api.polymarket.com`) exposes `liquidity` and `liquidityClob` as zero/`null` once an event is archived; volume figures remain reliable because the `volume` field is frozen at settlement.

## 2. Methodology & Tooling

- **Code assets created**:
  - `polymarket_market_collector.py`: pulls closed Polymarket events via the gamma API, filters for election-related questions using regex heuristics, captures resolution metadata, and fetches daily price candles from `https://clob.polymarket.com/prices-history`.
  - `finance_source_mapping.py`: attaches per-market finance data sources (public + commercial) and alignment notes, outputting JSON/CSV.
  - `merge_market_finance.py`: standardises price history into a wide format, generates finance-event templates, and produces merged daily datasets with gap diagnostics.
- **Python stack:** Python 3.13, `requests`, `pandas`, `BeautifulSoup` (for web citations), `csv`, `json`.
- **APIs / endpoints touched:**
  - Polymarket gamma event listing (`/events`, `/events/{id}`, `/markets/{id}`) and CLOB price history endpoint.
  - DuckDuckGo HTML endpoints for citation discovery.
  - External finance source homepages (FEC, Elections Canada, AEP, NEC, PKW) for licensing confirmation.
- **Heuristics & preprocessing:**
  - Election market detection uses keyword filters on titles/descriptions plus regex patterns (e.g., `\bwin\b`, `\bbe elected\b`, `\binaugurated\b`, `\bd-?nom\b`).
  - Only markets with minimum $1k total volume survive; final list prioritises highest notional volume to maximise liquidity.
  - Price histories are recorded for each outcome token; merged dataset pivots `Yes` and `No` legs for daily analytics.

## 3. Campaign Finance Data Mapping

Full details in `data/finance_source_mapping.(json|csv)`. Highlights by jurisdiction:

- **United States (federal/leadership/state margins):**
  - Public: Federal Election Commission data portal[^1], OpenSecrets presidential dashboard (CC BY-NC 3.0)[^2], ProPublica Campaign Finance API (free but gated)[^3].
  - State overlays: Pennsylvania DOS, Nevada SOS, Wisconsin CFIS portals supplemented by FollowTheMoney.org (CC BY 4.0).
  - Commercial (require escalation): AdImpact, Kantar/CMAG, TargetSmart, Civis Analytics, L2 Political, Quorum.
  - Alignment guidance: merge filings on `coverage_end_date` / `report_period_end_date` and forward-fill between reports.

- **Canada (Prime Minister scenarios):**
  - Public: Elections Canada political financing reports & open data (Open Government Licence – Canada)[^4].
  - Commercial: iPolitics INTEL, Politico Pro Canada.
  - Guidance: align `ReportingPeriodEndDate` with UTC close in price data.

- **Romania:**
  - Public: Autoritatea Electorală Permanentă portal (`finantarepartide.ro`), Expert Forum's Banometru dataset (CC BY 4.0).
  - Commercial: CEIC Data (regional political finance feeds).
  - Guidance: OCR-heavy PDFs; map on submission timestamp.

- **South Korea:**
  - Public: National Election Commission political funds system (KOGL Type 1), KOSIS political funds tables.
  - Commercial: Yonhap Infomax (subscription).
  - Guidance: convert Korean Standard Time filings to UTC before joining.

- **Poland:**
  - Public: PKW election finance disclosures (`wybory.gov.pl`), OpenSpending Poland (CC BY 4.0).
  - Commercial: Fitch Solutions – Emerging Europe Monitor.
  - Guidance: filings appear ~90 days post-election; use publication date as anchor.

Escalation flags are embedded for every paid dataset to route procurement before any spend is incurred.

## 4. Merge Pipeline & Templates

- Running `python merge_market_finance.py` now produces:
  - `data/finance_events/<market_id>.csv` templates (blank headers ready for OCR/ETL results).
  - `data/market_finance_merged.csv|json` with daily prices (`price_yes`, `price_no`), latest finance summary (if provided), and `finance_gap_flag` to highlight missing disclosures.
- Analysts can populate finance templates (e.g., FEC quarterly totals, AEP reimbursements) and re-run the merge to instantly align filings with market moves.
- Gap summary: all markets currently show `finance_gap_flag = True`, reflecting that finance event ingestion is pending (documented in §6).

## 5. Snapshot of Other Platforms (Closed / High-Liquidity)

- **Kalshi (CFTC-regulated):**
  - Flagship presidential contract surpassed $14M volume post-listing on 7 Oct 2024[^5].
  - Weekly platform volume peaked at ~$749M in the run-up to the 2024 U.S. presidential election, indicating comparable liquidity to Polymarket during closing weeks[^6].
  - Direct API access to historical volumes remains gated; tracking relies on Kalshi's public dashboards and media reporting.

- **PredictIt (CFTC no-action market, now sunset):**
  - Historical reporting notes >300M shares traded in 2017, with single-state primary markets (e.g., 2016 Michigan) clearing 2M+ trades despite regulatory caps[^7].
  - PredictIt is barred from releasing dollar turnover; analysts must proxy liquidity using share counts and trader cap metadata.

These supplemental figures can contextualise Polymarket liquidity when analysts compare across exchanges despite data-access asymmetries.

## 6. Transparency & Data Gaps

- **Polymarket data limitations:** archived markets zero-out liquidity fields; only total volume remains. Early FPMM markets lack price history, hence focus on recent CLOB-settled elections.
- **Campaign finance ingestion:** templates are empty pending OCR/API extraction. Non-U.S. sources (Romania, South Korea, Poland) deliver disclosures as PDFs in local languages; bilingual support and OCR tooling are required before structured merges.
- **Kalshi / PredictIt APIs:** Kalshi's trading API requires authenticated sessions; PredictIt withholds dollar volume due to CFTC consent agreement. Current strategy relies on reproducible web captures and cited media coverage.
- **Licensing:** some public sources (state portals, KOGL datasets) demand attribution; commercial feeds flagged for escalation before download.

## 7. Suggested Next Steps

1. Prioritise finance ETL for U.S. federal markets: pull FEC Form 3P filings for `Donald J. Trump for President (C00786876)` and `Biden for President (C00703975)` using the FEC API, populate templates, and re-run merge to validate alignment logic.
2. Coordinate translations/OCR for AEP (Romania) and NEC (South Korea) disclosures; store structured outputs under `data/finance_events/`.
3. Monitor Kalshi's public dashboards or CFTC filings for downloadable historical data; if unavailable, continue capturing volume milestones from reputable media.
4. Add automated validation notebooks to spot-check price/finance joins once finance data is ingested (e.g., ensure post-report price swings align with funding surprises).

## Citations

[^1]: Federal Election Commission. “Campaign finance data.” https://www.fec.gov/data/
[^2]: OpenSecrets. “2024 Presidential Race.” https://www.opensecrets.org/2024-presidential-race
[^3]: ProPublica. “Campaign Finance API.” https://www.propublica.org/datastore/api/propublica-campaign-finance-api
[^4]: Elections Canada. “Political Financing.” https://www.elections.ca/content.aspx?section=fin&document=index&lang=e
[^5]: FinanceFeeds. “Kalshi certifies US election betting contracts, topping $14M in volume.” https://financefeeds.com/kalshi-certifies-us-election-betting-contracts-topping-14m-in-volume/
[^6]: DL News. “Prediction market Kalshi overtakes Polymarket and sets off sports betting ‘land grab’.” https://www.dlnews.com/articles/markets/kalshi-overtakes-polymarket-trading-volume-and-sets-off-sports-betting-land-grab/
[^7]: PoliticalPredictionMarkets.com. “Volume and Liquidity on PredictIt.” https://politicalpredictionmarkets.com/volume-and-liquidity-on-predictit/
