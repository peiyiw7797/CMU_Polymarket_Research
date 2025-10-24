# FEC Candidate Funding Pipeline

## Objectives
- Normalize and unify FEC bulk text files into queryable datasets keyed by candidate across cycles.
- Provide near-instant lookups of a candidate's committees, receipts, disbursements, and incoming contributions (individual and committee).
- Support Polymarket research workflows by aligning FEC entities with market candidate names.

## High-Level Architecture
FEC bulk `.txt` files (Schedule A/B/E, master tables, linkage tables) → PySpark ETL (schema apply + cleansing) → Curated Parquet datasets in `data/warehouse/` → Postgres `fec_wh` database with indexed tables/materialized views → Python lookup service (CLI + API) backed by SQL queries.

> NOTE: Only a subset of FEC bulk files are currently checked in (`FEC_Data/`). The pipeline assumes schedule data (`itcont`, `oth`, `oppexp`, etc.) are downloaded to the same directory following FEC naming conventions.

## PySpark ETL

### Ingestion Layer
- Reads pipe-delimited bulk files using schema definitions from official header CSVs (e.g. `candidate_master_header_file.csv`).
- Stores raw tables as partitioned Parquet in `data/staging/{table}/{cycle}=YYYY/part=...`.
- Spark configuration tuned for local development by default (`spark.sql.shuffle.partitions=8`), with overrides for cluster execution.

### Core Tables
| Table | Source | Primary Keys | Notes |
|-------|--------|--------------|-------|
| `candidate_dim` | `candidate_master_*.txt` | `cand_id` | Normalizes names, party, office, state, election year. Adds helper columns: `normalized_name`, `cycle`. |
| `committee_dim` | `committee_master_*.txt` | `cmte_id` | Includes designation, committee type, treasurer, etc. |
| `candidate_committees` | `candidate_committee_linkage_*.txt` + `committees_to_candidates_*.txt` | (`cand_id`, `cmte_id`, `cycle`) | Merges principal + authorized committee relationships, flags joint fundraising committees. |
| `receipt_fact` | Schedule A (`itcont*.txt` / `pas2*.txt`) | (`cmte_id`, `transaction_id`) | Captures receipts with contributor detail, contributor type (individual, committee, party). |
| `disbursement_fact` | Schedule B (`oppexp*.txt` / `oth*.txt`) | (`cmte_id`, `transaction_id`) | Includes expenditure purpose, recipient name. |
| `committee_summary` | `weball*.txt` | (`cmte_id`, `cycle`) | Totals for receipts, disbursements, cash, debts. |
| `independent_expenditure` | Schedule E (`independent_expenditure*.txt`) | (`filer_cmte_id`, `transaction_id`) | Optional extension for outside spending. |

### Transformations
- **Name normalization**: strip punctuation, collapse whitespace, uppercase for joins. Maintain original casing for display.
- **Cycle assignment**: derive `cycle` from election year / reporting period; ensure contributions posted in odd years roll into the next even-year cycle.
- **Committee roll-ups**: mark committees as principal vs. joint vs. leadership using designation/type codes, enabling filtered aggregations.
- **Contribution attribution**:
  - Join `receipt_fact` to `candidate_committees` to attribute committee activity back to natural candidates.
  - Derive contributor buckets: `individual`, `committee`, `party`, `candidate self`, using FEC entity type fields.
- **Disbursement classification**: map purpose codes to high-level buckets (media, payroll, transfers) via configurable dictionary.
- **Fact indexing**: compute `checksum` hash on transaction rows for idempotent loads.

### Aggregated Parquet Outputs
1. `candidate_cycle_summary`: totals receipts, disbursements, cash on hand, debts, broken out by contributor bucket.
2. `candidate_committee_details`: per committee stats with time-series by filing quarter/month.
3. `contribution_topline`: top individual and committee contributors per candidate per cycle (limit N).
4. `disbursement_topline`: major vendors per candidate per cycle.
5. `schedule_a_flat`: fully denormalized Schedule A transactions enriched with candidate + committee data to satisfy lookup output (see below).

Each dataset partitioned by `cycle` and optionally `cand_id` to speed predicate pushdown.

## Postgres Warehouse

### Schema Overview (`fec_wh`)
- `candidate` (`cand_id` PK, `cycle`, `display_name`, `party`, `office`, `state`, `normalized_name`, `search_vector`)
- `committee` (`cmte_id` PK, `cycle`, `name`, `designation`, `type`, `principal_flag`)
- `candidate_committee` (`cand_id`, `cmte_id`, `cycle`, `role`)
- `candidate_cycle_summary` (`cand_id`, `cycle`, metrics…)
- `committee_time_series` (`cmte_id`, `cycle`, `period_start`, `period_end`, `receipts`, `disbursements`, `cash_on_hand`)
- `top_contributors` (`cand_id`, `cycle`, `contributor_name`, `contributor_type`, `total_amount`)
- `top_vendors` (`cand_id`, `cycle`, `recipient_name`, `total_amount`)
- `schedule_a_flat` (`two_year_transaction_period`, `transaction_id`, `cand_id`, `cmte_id`, …) materialized view delivering the wide record layout required by analysts.
- Indices:
  - `GIN` on `candidate.search_vector` (generated via `to_tsvector('simple', normalized_name || ' ' || display_name)`).
  - Btree on `(cand_id, cycle)` across summary tables.
  - Partial index on `candidate_committee(principal_flag)` to accelerate lookups.

### Loading Strategy
1. PySpark writes Parquet outputs to `/tmp/fec_exports/{table}/`.
2. Use `COPY` from Parquet (via `postgresql COPY FROM STDIN` + `spark.write.format("jdbc")`) or `spark.write.format("jdbc").option("truncate", "true")` for full refresh tables.
3. For large fact tables, stage into temp tables (`*_stg`) then swap with `ALTER TABLE ... RENAME` to keep lookups online.
4. Refresh materialized views (`REFRESH MATERIALIZED VIEW CONCURRENTLY candidate_mv`) after load.

### Funding History View
Create a materialized view (`mv_candidate_schedule_a`) that maps directly to the required output shape. Key points:
- Base table: `schedule_a_flat` Parquet export written by Spark.
- Enrich with candidate fields (first/last name, office, state) by joining `candidate_dim` via `cand_id` or through the committee linkage when missing.
- Populate election metadata (`fec_election_year`, `two_year_transaction_period`, `election_type`, etc.) using fields in Schedule A (`cycle`, `election_tp`, `fec_election_type_desc`).
- Include document metadata (`report_year`, `report_type`, `image_number`, `filing_form`, `pdf_url`) straight from the bulk file.
- Compute helper columns:
  - `is_individual`: `TRUE` when `entity_type = 'IND'`.
  - `receipt_type_desc`/`receipt_type_full`: join to FEC code reference table (load once into `dim_receipt_type`).
  - `candidate_name`, `candidate_first_name`, etc. derived from `candidate_dim`.
  - Aggregate YTD (`contributor_aggregate_ytd`) present in Schedule A; ensure numeric type.

Example DDL:
```sql
CREATE MATERIALIZED VIEW mv_candidate_schedule_a AS
SELECT
  sa.cmte_id AS committee_id,
  cm.name AS committee_name,
  sa.report_year,
  sa.report_type,
  sa.image_num AS image_number,
  sa.form_tp AS filing_form,
  sa.link_id,
  sa.line_num AS line_number,
  sa.transaction_id,
  sa.file_num AS file_number,
  sa.entity_type,
  et.description AS entity_type_desc,
  sa.contbr_id AS unused_contbr_id,
  sa.contbr_prefix AS contributor_prefix,
  sa.contbr_nm AS contributor_name,
  cm.committee_type AS recipient_committee_type,
  cm.org_type AS recipient_committee_org_type,
  cm.designation AS recipient_committee_designation,
  sa.contbr_first_nm AS contributor_first_name,
  sa.contbr_mid_nm AS contributor_middle_name,
  sa.contbr_last_nm AS contributor_last_name,
  sa.contbr_suffix AS contributor_suffix,
  sa.contbr_st1 AS contributor_street_1,
  sa.contbr_st2 AS contributor_street_2,
  sa.contbr_city AS contributor_city,
  sa.contbr_st AS contributor_state,
  sa.contbr_zip AS contributor_zip,
  sa.contbr_employer AS contributor_employer,
  sa.contbr_occupation AS contributor_occupation,
  sa.contbr_id,
  (sa.entity_type = 'IND') AS is_individual,
  sa.receipt_tp AS receipt_type,
  rt.short_desc AS receipt_type_desc,
  rt.long_desc AS receipt_type_full,
  sa.memo_cd AS memo_code,
  mc.description AS memo_code_full,
  sa.memo_text,
  sa.contb_receipt_dt AS contribution_receipt_date,
  sa.contb_receipt_amt AS contribution_receipt_amount,
  sa.contb_aggreg_amt AS contributor_aggregate_ytd,
  sa.cand_id AS candidate_id,
  cand.display_name AS candidate_name,
  cand.first_name AS candidate_first_name,
  cand.last_name AS candidate_last_name,
  cand.middle_name AS candidate_middle_name,
  cand.prefix AS candidate_prefix,
  cand.suffix AS candidate_suffix,
  cand.office AS candidate_office,
  cand.office_full AS candidate_office_full,
  cand.state AS candidate_office_state,
  cand.state_full AS candidate_office_state_full,
  cand.district AS candidate_office_district,
  sa.conduit_cmte_id AS conduit_committee_id,
  sa.conduit_cmte_nm AS conduit_committee_name,
  sa.conduit_street1 AS conduit_committee_street1,
  sa.conduit_street2 AS conduit_committee_street2,
  sa.conduit_city AS conduit_committee_city,
  sa.conduit_state AS conduit_committee_state,
  sa.conduit_zip AS conduit_committee_zip,
  sa.donor_cmte_nm AS donor_committee_name,
  sa.national_cmte_nonfed AS national_committee_nonfederal_account,
  sa.election_type,
  sa.election_tp_desc AS election_type_full,
  sa.fec_election_type_desc,
  sa.fec_election_yr AS fec_election_year,
  sa.two_year_transaction_period,
  sa.amnd_ind AS amendment_indicator,
  ai.description AS amendment_indicator_desc,
  sa.schedule_type,
  st.description AS schedule_type_full,
  sa.increased_limit,
  sa.load_date,
  sa.sub_id,
  sa.sub_id_original AS original_sub_id,
  sa.back_ref_tran_id AS back_reference_transaction_id,
  sa.back_ref_sch_nm AS back_reference_schedule_name,
  sa.pdf_url,
  sa.line_number_label
FROM schedule_a_flat sa
JOIN committee cm ON cm.cmte_id = sa.cmte_id AND cm.cycle = sa.two_year_transaction_period
LEFT JOIN candidate cand ON cand.cand_id = sa.cand_id AND cand.cycle = sa.two_year_transaction_period
LEFT JOIN dim_entity_type et ON et.code = sa.entity_type
LEFT JOIN dim_receipt_type rt ON rt.code = sa.receipt_tp
LEFT JOIN dim_memo_code mc ON mc.code = sa.memo_cd
LEFT JOIN dim_amendment_indicator ai ON ai.code = sa.amnd_ind
LEFT JOIN dim_schedule_type st ON st.code = sa.schedule_type;
```

Refresh cadence matches Schedule A load; include `WITH DATA` initial load, then `REFRESH ... CONCURRENTLY` nightly.

## Lookup Service

### Functional Requirements
- Input: candidate name (free text, partial matching supported).
- Output: JSON struct with candidate metadata, committee list, cycle summaries, top contributors, top vendors, and raw committee transactions (optional pagination).

### Implementation Outline
```text
lookup/
├── __init__.py
├── db.py             # SQLAlchemy engine + helper functions.
├── search.py         # Name normalization and candidate lookup queries.
├── service.py        # FastAPI or CLI wrapper.
└── queries.sql       # Parametrized SQL statements.
```

`search.py` workflow:
1. Normalize input (`normalized_name`).
2. Query `candidate` table using ILIKE + trigram or GIN text search to get candidate IDs.
3. For each candidate:
   - Fetch `candidate_cycle_summary` rows ordered by cycle desc.
   - Fetch associated `candidate_committee` + `committee_time_series`.
   - Fetch top contributors / vendors.
4. Assemble response with metadata and totals.

Provide both:
- `python -m lookup.service "Martha Washington"` (prints Markdown/JSON to stdout).
- REST endpoint: `GET /candidate?name=Martha Washington`.

### Performance Considerations
- Pre-compute `candidate_mv` materialized view combining core candidate fields and summary metrics for single-query fetch.
- Use caching layer (`functools.lru_cache` or Redis) for frequent lookups.
- Add daily ETL schedule (e.g., Airflow DAG) to refresh data; apply incremental loads using `Filing_Fec_Id` where available.

## Orchestration
- Entry point `spark_jobs/fec_pipeline.py` accepts args: `--cycles 2020 2022 2024`, `--staging-dir`, `--warehouse-dir`.
- DAG order:
  1. `ingest_master_tables`
  2. `ingest_schedule_tables`
  3. `build_candidate_dim`
  4. `build_committee_dim`
  5. `compute_facts`
  6. `publish_parquet`
  7. `load_postgres`
- Include data quality checks: count mismatches, null candidate IDs, duplicate transaction IDs.

## Integration with Existing Scripts
- `fec_financials_matcher.py` can feed candidate IDs into lookup service to retrieve aggregated metrics.
- Add CLI flag `--enrich` that calls the lookup service for each matched candidate and merges funding history into Polymarket datasets.

## Next Steps
1. Stand up local Spark environment (or Databricks job) and prototype ingestion for one cycle (e.g., 2024).
2. Implement dimension builders and ensure normalization matches lookup expectations.
3. Define SQL schemas in Alembic migration for Postgres.
4. Build lookup CLI + API, add tests using fixture database seeded from small sample.
5. Integrate with Polymarket enrichment workflow and document runbooks.
