# Polymarket Election Market Coverage & FEC Matching

This project tracks closed Polymarket election markets and links them to U.S. Federal Election Commission (FEC) identifiers for downstream fundraising analysis. The latest workstream expands coverage from a top‑20 slice to all election markets resolved within the last two years, enriches them with entity-extracted candidate names, and adds a lightweight FEC matcher to surface candidate IDs for bulk cross-referencing.

---

## Recent Progress

### 1. Full Two-Year Market Collection (`polymarket_market_collector.py`)
- **Scope expansion**: Collector now ingests *all* closed Polymarket election markets within the past two years (previously limited to top-20 by volume).
- **Name extraction**: Replaced the original spaCy pipeline with NLTK-based PERSON detection plus custom heuristics. Candidate names are deduplicated per market and appended as `candidate_names` during CSV/JSON export.
- **Outputs**: Updated datasets in `data/` now include the `candidate_names` column, allowing downstream finance matching without reprocessing text.

### 2. Candidate-to-FEC Mapping (`fec_financials_matcher.py`)
- **Purpose**: Reads the expanded market CSV, matches candidate names to FEC candidate records, and records the corresponding candidate IDs.
- **Debiasing logic**: Progressive filter relaxation—start with `(state, office)`; for multi-word names, optionally drop state/office to surface viable matches without spurious hits.
- **Result**: Produces `fec_financials_matched.csv`, which mirrors the market rows and adds:
  - `matched_fec_ids`: list of FEC candidate IDs tied to the market.
  - `download_successful_names`: candidate names that returned at least one FEC match.
- **Current behavior**: Only aggregates identifiers; does *not* download Schedule A (itemized) records, avoiding rate-limit and pagination overhead until bulk data is ready.

### 3. Logging & Rate-Limit Resilience
- All API calls use exponential backoff and honor `Retry-After`/`X-RateLimit-Reset` to stay within the FEC quota (1,000 calls/hour, 100 records/page).
- Partial progress is logged (e.g., unmatched names, expanded search attempts) to simplify manual QA.

---

## File Map
- `polymarket_market_collector.py`: Collects closed markets, enriches with candidate names, exports CSV/JSON in `data/`.
- `fec_financials_matcher.py`: Loads market CSV, resolves FEC candidate IDs, writes `fec_financials_matched.csv`.
- `data/`: Contains the refreshed comprehensive market dataset plus derived exports.
- `cache/`: (Previously) stored pagination state for Schedule A downloads; no longer populated in ID-only workflow.

---

## How to Reproduce the Current Outputs
```bash
# 1. Refresh market dataset (two-year window, tagged with candidate names)
python polymarket_market_collector.py

# 2. Match candidate names to FEC identifiers (optional --limit for smoke tests)
FEC_API_KEY=your_key_here python fec_financials_matcher.py --limit 50
```

`fec_financials_matched.csv` will live alongside the inputs and can be joined back to the Polymarket CSV via `market_id`/`event_id`.

---

## Future Steps
1. **Bulk FEC ingestion**: Download the full Schedule A (itemized contributions) bulk files for the cycles of interest directly from the FEC data portal.
2. **Historical matching**: For each market/candidate combination, filter the bulk data by the matched FEC IDs to reconstruct fundraising histories without live API calls.
3. **Temporal alignment**: Aggregate contributions by reporting period/coverage end date so they can be lined up with market resolution windows or campaign phases.
4. **Quality checks**: Validate name-to-ID matches against manual records (e.g., candidate hometown/state) before automating the merge with the bulk dataset.

---

## Environment & Dependencies
- Python 3.13
- Requests, pandas, nltk (with NE chunker resources pre-downloaded)

```bash
pip install requests pandas nltk
python -m nltk.downloader averaged_perceptron_tagger maxent_ne_chunker words
```

spaCy is no longer required; all PERSON extraction is handled via NLTK chunking.

---

## Version Notes
- **2025‑10‑17**:
  - Collector upgraded to full two-year sweep with NLTK PERSON detection.
  - FEC matcher simplified to identifier aggregation (no Schedule A downloads).
  - README initialized to track progress and next steps.
