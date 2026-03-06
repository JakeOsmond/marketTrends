---
title: 'Travel Insurance Market Intelligence Pipeline'
slug: 'travel-insurance-market-intel-pipeline'
created: '2026-03-06'
status: 'implementation-complete'
stepsCompleted: [1, 2, 3, 4]
tech_stack: ['Python 3.x', 'pytrends', 'gspread', 'pandas', 'requests', 'openpyxl', 'beautifulsoup4']
files_to_modify: []
code_patterns: ['modular ingestion per source', 'pandas DataFrame pipeline', 'spike detection/normalisation layer', 'Google Sheets API output']
test_patterns: ['unit tests per ingestion module', 'integration tests for normalisation', 'mock HTTP responses for source tests']
---

# Tech-Spec: Travel Insurance Market Intelligence Pipeline

**Created:** 2026-03-06

## Overview

### Problem Statement

There is no systematic way to track whether the UK travel insurance market is growing or shrinking. External data exists across multiple public sources (Google Trends, CAA, FCA, ABI, Eurocontrol, IATA, ONS) but isn't collected, normalised, or centralised. Without this, it's impossible to benchmark trading performance against the broader market or identify whether changes in traffic are company-specific or market-wide.

### Solution

A fully automated Python-based data pipeline that pulls from multiple external data sources covering both holiday intent and insurance purchase intent, applies spike detection and normalisation, and outputs a centralised Google Sheet with full historical backfill. The pipeline will run on a schedule to keep the dataset current with no manual data entry required.

### Scope

**In Scope:**
- Automated Google Trends extraction via pytrends for holiday intent terms (book holiday, cheap flights, package holiday, all inclusive, seasonal terms) and insurance intent terms (travel insurance, holiday insurance, annual travel insurance, single trip travel insurance, travel insurance comparison)
- Automated scraping/parsing of CAA international passenger data
- Automated scraping/parsing of FCA general insurance distribution data (travel insurance policies sold)
- Automated scraping/parsing of ABI quarterly travel insurance GWP data
- Automated ingestion of ONS overseas travel and tourism data
- Automated ingestion of Eurocontrol daily European flight data (UK departures)
- Automated ingestion of IATA monthly global passenger traffic data
- Spike detection using year-over-year comparison: each month is compared against the historical average for that same calendar month across prior years, with a 2 standard deviation threshold. This preserves seasonal patterns (summer peaks, winter troughs) while detecting genuine anomalies.
- Spike normalisation (replace anomalies with same-month historical average, flag and annotate known events)
- Historical backfill as far back as each source allows (target: 2015 onwards where possible)
- Automated output to Google Sheet via Google Sheets API
- Scheduling capability for ongoing daily/monthly ingestion depending on source freshness

**Out of Scope:**
- Forward airline booking indicators from public trading updates (TUI, Jet2, easyJet) — deferred to future phase as these are unstructured earnings reports requiring manual parsing
- Internal site visitor data or any business-specific metrics
- Derived metrics comparing company performance to market (user will overlay separately)
- BigQuery migration (future phase)
- Looker Studio or other dashboarding tools
- Alerting or notification systems
- Paid data sources (Consumer Intelligence, GlobalData, Defaqto)

## Context for Development

### Codebase Patterns

Greenfield project — confirmed clean slate. Modular Python pipeline with one ingestion module per data source, a shared normalisation layer, and a Google Sheets output module.

### Data Source Investigation Results

| Source | Format | Access Method | Auth Required | Historical Depth | Update Frequency | Status |
| ------ | ------ | ------------- | ------------- | ---------------- | ---------------- | ------ |
| Google Trends (pytrends) | pandas DataFrame | pytrends library (unofficial scraper) | No | Back to 2004 | Real-time | Available — fragile, needs rate limiting |
| CAA Passenger Data | CSV | Direct HTTP download from caa.co.uk | No | Monthly from mid-1990s | Monthly (~2mo lag) | Available |
| Eurocontrol Flights | CSV | Direct download from R&D Data Archive | No | Back to ~2014-2016 | Daily | Available — filter by UK departure state |
| ONS Overseas Travel | Excel (.xlsx) / CSV | Direct download or ONS beta API (JSON) | No | Back to 1993 (annual), mid-2000s (quarterly) | Quarterly (~3mo lag) | Available |
| FCA Value Measures | Excel / CSV | Direct download from fca.org.uk | No | Back to ~2018-2019 | Annual | Available — travel insurance included |
| ABI Travel GWP | Excel (members portal) | Members-only data portal | Yes (membership) | Years back | Quarterly | BLOCKED — detailed data requires paid ABI membership |
| IATA Passenger Traffic | PDF (free) / Excel (paid) | Free PDF summaries from iata.org | No (summaries) | ~5 years in free PDFs | Monthly | PARTIALLY BLOCKED — raw data is paid |
| ICAO Air Transport Monitor | Web/PDF | icao.int (free monthly stats) | No/Registration | Several years | Monthly | Available — free alternative to IATA |
| Eurostat Air Passengers | CSV/TSV | Eurostat API (JSON/SDMX) or bulk download | No | Extensive European coverage | Monthly | Available — free, covers European routes |
| World Bank Air Transport | CSV/Excel | data.worldbank.org API | No | Back to 1970s | Annual only | Available — global but annual granularity only |

### Risks and Mitigations

| Risk | Impact | Mitigation |
| ---- | ------ | ---------- |
| ABI detailed travel GWP data is members-only | Cannot get market value data without paid membership | Use FCA Value Measures as primary insurance market validation. Monitor ABI press releases/Key Facts PDFs for free summary figures. Flag as future enhancement if membership obtained. |
| IATA raw data is paid | No granular global passenger data | Replace with ICAO Air Transport Monitor (monthly, free) + Eurostat (European monthly, free API) + World Bank (annual global, free). Combined coverage is sufficient. |
| pytrends is unofficial and fragile | Pipeline could break when Google changes backend | Implement robust retry/backoff logic. Cache all successful responses. Consider SerpApi (~$50/mo) as paid fallback if pytrends becomes unreliable. |
| Google Trends returns relative indices (0-100) not absolute | Values not directly comparable across separate queries | Use anchor term technique — include one common term across all batches to normalise between them. Max 5 terms per request. |
| Eurostat UK data stops post-Brexit (Dec 2020) | UK no longer reports to Eurostat as an EU member. Monthly European air passenger data for UK likely truncated from 2021. | Eurostat treated as historical source (2015-2020). Module detects end of UK data, logs warning, records coverage end date. Post-2020 European coverage comes from CAA and Eurocontrol instead. |
| COVID period (Mar 2020 - Jun 2021) distorts all data | Trendlines become meaningless if drawn through COVID. COVID data would poison seasonal baselines if included. | Treat as structural break. COVID months quarantined from all same-month statistical pools. Each COVID month's normalised value uses same-month pre-COVID mean (e.g., Mar 2020 → mean of Mar 2015-2019). Post-COVID resumes normal YoY comparison excluding COVID months. |

### Technical Decisions

- **Language:** Python 3.x
- **Google Trends:** pytrends library with rate limiting (10-30s between requests), chunked time ranges (~5yr windows for weekly granularity), geo='GB' for UK-only. Up to 5 terms per request with anchor term for cross-batch normalisation.
- **Google Sheets:** gspread library for Sheet output via Google Sheets API. Requires Google Cloud service account with Sheets API enabled.
- **Data parsing:** pandas + openpyxl for Excel files, pandas for CSV, BeautifulSoup for any HTML scraping needed.
- **HTTP access:** requests library for direct file downloads from CAA, Eurocontrol, ONS, FCA.
- **Eurostat access:** Eurostat API (JSON/SDMX) or bulk CSV/TSV download for European air passenger data.
- **ONS access:** Try ONS beta API first (api.beta.ons.gov.uk/v1), fall back to direct Excel download if travel datasets not available via API.
- **Normalisation:** Applied in-pipeline before writing to Sheet. Both raw and normalised values stored. Spike detection uses **year-over-year (YoY) comparison** rather than a rolling average — this is critical because travel data is inherently seasonal (summer peaks, winter troughs are normal, not anomalous). For each month, the detector compares the value against the historical mean and standard deviation for that same calendar month across all available prior years. A spike is flagged when a value exceeds 2 standard deviations from the same-month historical mean. This requires at least 3 years of history per calendar month for meaningful statistics — pre-2018 data points have fewer comparison years and should be flagged as `low_confidence=True`. Known events (volcanic eruptions, airline collapses, pandemic, regulatory changes) annotated.
- **Monthly granularity** is the common denominator. Daily sources (Eurocontrol, Google Trends) aggregated to monthly. Weekly Google Trends data preserved where available.
- **Backfill strategy:** Pull all available history per source (target 2015+). Google Trends chunked into ~5yr windows then stitched. Cache all responses to avoid re-fetching.
- **Scheduling:** cron job or Cloud Function for ongoing ingestion. Frequency per source: daily (Eurocontrol, Google Trends), monthly (CAA, ICAO, Eurostat), quarterly (ONS, FCA annually).

## Implementation Plan

### Tasks

#### Phase 1: Project Setup and Infrastructure

- [x] Task 1: Initialize Python project structure
  - File: `src/` directory structure
  - Action: Create project skeleton with the following structure:
    ```
    marketTrends/
    ├── src/
    │   ├── __init__.py
    │   ├── main.py                    # Pipeline orchestrator
    │   ├── config.py                  # Configuration and constants
    │   ├── ingestion/
    │   │   ├── __init__.py
    │   │   ├── base.py                # Base ingestion class
    │   │   ├── google_trends.py       # Google Trends via pytrends
    │   │   ├── caa.py                 # CAA passenger data
    │   │   ├── eurocontrol.py         # Eurocontrol flight data
    │   │   ├── ons.py                 # ONS overseas travel
    │   │   ├── fca.py                 # FCA value measures
    │   │   ├── icao.py                # ICAO air transport monitor
    │   │   ├── eurostat.py            # Eurostat air passengers
    │   │   └── world_bank.py          # World Bank air transport
    │   ├── normalisation/
    │   │   ├── __init__.py
    │   │   └── spike_detector.py      # Spike detection and normalisation
    │   ├── output/
    │   │   ├── __init__.py
    │   │   └── google_sheets.py       # Google Sheets writer
    │   └── cache/
    │       ├── __init__.py
    │       └── file_cache.py          # Local file caching for API responses
    ├── tests/
    │   ├── __init__.py
    │   ├── test_google_trends.py
    │   ├── test_caa.py
    │   ├── test_eurocontrol.py
    │   ├── test_ons.py
    │   ├── test_fca.py
    │   ├── test_icao.py
    │   ├── test_eurostat.py
    │   ├── test_world_bank.py
    │   ├── test_spike_detector.py
    │   └── test_google_sheets.py
    ├── data/
    │   └── cache/                     # Cached API responses
    ├── requirements.txt
    ├── .env.example                  # Template for environment variables
    ├── .gitignore                     # Excludes credentials.json, .env, data/cache/
    └── README.md
    ```
  - Notes: Use a flat module structure. No unnecessary abstractions. The `.gitignore` must exclude `credentials.json`, `.env`, and `data/cache/` to prevent committing secrets or large cached files.

- [x] Task 2: Create configuration module
  - File: `src/config.py`
  - Action: Define all constants including:
    - Google Trends search terms (holiday intent: "book holiday", "cheap flights", "package holiday", "all inclusive holiday", "summer holiday", "winter sun"; insurance intent: "travel insurance", "holiday insurance", "annual travel insurance", "single trip travel insurance", "travel insurance comparison")
    - Anchor term for cross-batch normalisation (use "travel insurance" as it's the most stable)
    - Source URLs for CAA, Eurocontrol, ONS, FCA, ICAO, Eurostat, World Bank
    - Google Sheets spreadsheet ID loaded from environment variable `GOOGLE_SHEET_ID`
    - Google Cloud service account credentials path loaded from environment variable `GOOGLE_CREDENTIALS_PATH` (defaults to `credentials.json`)
    - Sheet tab names as constants
    - Rate limiting settings (pytrends delay: 15s between requests)
    - Backfill date range (2015-01-01 to present)
    - COVID structural break period (2020-03-01 to 2021-06-30)
    - Spike detection parameters (method: year-over-year same-month comparison, threshold: 2 std devs, minimum_years: 3 for statistical confidence)
    - Known event annotations (dict of date ranges to event descriptions)
  - Notes: Use dataclasses or simple module-level constants. No over-engineering.

- [x] Task 3: Create base ingestion class
  - File: `src/ingestion/base.py`
  - Action: Create a `BaseIngestionModule` class with:
    - `fetch()` method — retrieves raw data (HTTP download or API call) with built-in retry logic: 3 retries with exponential backoff (5s, 15s, 45s) on HTTP 429, 500, 502, 503, 504 errors and connection timeouts. Log each retry attempt. Subclasses can override retry parameters — Google Trends overrides to 5 retries starting at 30s (see Task 5) because Google rate-limits more aggressively than government data sites.
    - `parse()` method — transforms raw data into a standardised pandas DataFrame with columns: `date` (monthly), `source`, `metric_name`, `raw_value`
    - `validate()` method — checks parsed DataFrame against expected schema: verifies required columns exist, `date` column is datetime type, `raw_value` is numeric, no duplicate date+metric rows. For null values: if <20% of rows have null `raw_value`, drop them and log a warning. If >=20% are null, treat as a source failure (probable upstream format change) and raise `DataValidationError` — this circuit breaker prevents silently writing mostly-empty data.
    - `backfill(start_date, end_date)` method — fetches full historical range
    - `get_latest()` method — fetches most recent available data
    - Integration with file cache (check cache before fetching)
    - All HTTP requests use a shared `requests.Session` with a 30-second timeout
  - Notes: Keep it simple. Not every source will use every method — allow overrides. The retry and validation logic lives in the base class so every source inherits it automatically.

- [x] Task 4: Create file cache module
  - File: `src/cache/file_cache.py`
  - Action: Simple file-based caching that:
    - Stores raw API/download responses as files in `data/cache/` keyed by source name + date range
    - Checks if cached data exists and is within its TTL before re-fetching. TTL is configured per source in config.py:
      - Google Trends: 24 hours (daily refresh)
      - Eurocontrol: 24 hours (daily data)
      - CAA: 30 days (monthly publication, ~2 month lag)
      - ONS: 90 days (quarterly publication)
      - FCA: 365 days (annual publication)
      - ICAO: 30 days (monthly publication)
      - Eurostat: 30 days (monthly publication)
      - World Bank: 365 days (annual publication)
    - Allows forced refresh override via `--force-refresh` CLI flag
    - Stores raw responses in their original format (CSV as CSV, Excel as .xlsx, JSON as .json) so originals can be re-parsed if needed. Parsed DataFrames cached separately as CSV for quick loading.
  - Notes: No database, no Redis — just files on disk. Two-tier cache: raw originals + parsed CSVs.

#### Phase 2: Google Trends Ingestion (Highest Priority)

- [x] Task 5: Implement Google Trends ingestion module
  - File: `src/ingestion/google_trends.py`
  - Action: Implement `GoogleTrendsIngestion` class that:
    - Uses pytrends with `geo='GB'` for UK-only data
    - Fetches all 11 search terms (6 holiday intent + 5 insurance intent)
    - Batches into groups of 5 with "travel insurance" as anchor term in each batch
    - Chunks time ranges into ~5-year windows to get weekly granularity
    - Stitches chunks together using the anchor term to normalise across windows
    - Implements rate limiting (15s sleep between requests) with overridden retry config: 5 retries with exponential backoff starting at 30s (30s, 60s, 120s, 240s, 480s) on 429 errors — more aggressive than the base class defaults because Google throttles harder
    - Caches each successful response to disk
    - Aggregates weekly data to monthly (mean of weekly values per month)
    - Returns DataFrame with columns: date, term, raw_value, granularity
  - Notes: The anchor term normalisation is critical. "travel insurance" must appear in every batch of 5 terms. Values from different batches are scaled relative to the anchor term's value in each batch. If the anchor term value is 0 or below 5 in any time window, skip normalisation for that window and log a warning — the raw values are stored as-is with `is_anchor_normalised=False` in the output DataFrame. This flag must carry through to the Google Sheet so consumers can see which rows were cross-batch normalised and which are raw single-batch values.

- [x] Task 6: Implement Google Trends backfill
  - File: `src/ingestion/google_trends.py`
  - Action: Add `backfill()` method that:
    - Pulls data from 2015-01-01 to present in ~5-year chunks
    - For each chunk, fetches all term batches
    - Stitches temporal chunks using overlapping months
    - Validates continuity across chunk boundaries
    - Caches all responses — if interrupted, resumes from last cached chunk
  - Notes: This will make ~30+ API calls. With 15s delays, expect ~8-10 minutes for full backfill. Run once, cache forever.

#### Phase 3: Government/Authority Data Ingestion

- [x] Task 7: Implement CAA passenger data ingestion
  - File: `src/ingestion/caa.py`
  - Action: Implement `CAAIngestion` class that:
    - Downloads CSV files from caa.co.uk/data-and-analysis/uk-aviation-market/airports/uk-airport-data/
    - Parses international passenger totals by month
    - Handles the CSV format (identify correct table — likely "Table 12 - International and Domestic Passengers")
    - Extracts total UK international terminal passengers per month
    - Backfills from 2015 onwards
  - Notes: URL structure may need discovery. Download the index page, find CSV links, then fetch data files. Cache downloaded CSVs.

- [x] Task 8: Implement ONS overseas travel ingestion
  - File: `src/ingestion/ons.py`
  - Action: Implement `ONSIngestion` class that:
    - First attempts ONS beta API (api.beta.ons.gov.uk/v1) to find travel/tourism datasets
    - Falls back to direct Excel download if API doesn't have the dataset
    - Parses UK residents' total overseas visits by quarter
    - Stores as quarterly data only — do NOT interpolate to monthly. Each quarter's value is stored with the quarter-end month as the date (Mar, Jun, Sep, Dec). Add a `granularity="quarterly"` column so downstream consumers know these are not monthly observations.
    - Backfills from 2015 onwards
  - Notes: ONS Excel files have multiple sheets/tables. Need to identify the correct table for "UK residents visits abroad - total". Quarterly data must not be interpolated to monthly — fabricated monthly values would be misleading when displayed alongside genuinely monthly data from other sources. The Google Sheet will show gaps between quarterly data points, which is correct and honest.

- [x] Task 9: Implement FCA value measures ingestion
  - File: `src/ingestion/fca.py`
  - Action: Implement `FCAIngestion` class that:
    - Downloads Excel/CSV from fca.org.uk/data/general-insurance-value-measures-data
    - Extracts travel insurance specific rows (claims frequency, acceptance rates, average premiums)
    - Stores as annual data points (FCA publishes annually)
    - Backfills from 2018-2019 onwards (earliest available)
  - Notes: This is the primary insurance-market-specific validation source. Annual granularity only.

#### Phase 4: Aviation Data Ingestion

- [x] Task 10: Implement Eurocontrol flight data ingestion
  - File: `src/ingestion/eurocontrol.py`
  - Action: Implement `EurocontrolIngestion` class that:
    - Downloads CSV from Eurocontrol R&D Data Archive
    - Filters for UK departure state
    - Aggregates daily flight counts to monthly totals
    - Backfills from 2016 onwards (earliest available)
  - Notes: Daily granularity available. Aggregate to monthly for consistency but consider storing daily in a separate sheet tab for higher resolution analysis.

- [x] Task 11: Implement Eurostat air passenger ingestion
  - File: `src/ingestion/eurostat.py`
  - Action: Implement `EurostatIngestion` class that:
    - Uses Eurostat API (or bulk download) for dataset `avia_paoc` (air passengers by reporting country)
    - Filters for UK origin passengers for pre-2021 data
    - Monthly granularity natively available
    - Backfills from 2015 onwards
    - **Brexit handling:** UK data in Eurostat likely stops or degrades after December 2020 (end of EU transition period). The module must:
      - Fetch data up to the latest available UK data point
      - Log a warning if UK data stops before the current date
      - Add a `data_coverage_end` metadata field so downstream consumers know the series is truncated
      - Do NOT attempt to fill gaps with estimates — just end the series cleanly
    - For post-2020 European air passenger data, CAA and Eurocontrol provide the UK perspective. Eurostat remains valuable for the 2015-2020 historical baseline.
  - Notes: Eurostat API returns JSON or SDMX. pandas can read Eurostat TSV bulk downloads directly. This source is primarily historical — expect it to stop providing UK data from 2021 onwards.

- [x] Task 12: Implement ICAO air transport ingestion (best-effort)
  - File: `src/ingestion/icao.py`
  - Action: Implement `ICaoIngestion` class that:
    - Attempts to fetch monthly global air traffic statistics from ICAO Air Transport Monitor (icao.int)
    - Extracts key metrics: total passengers, RPKs, year-on-year growth percentage
    - **Implementation approach:** First check if ICAO provides structured data (CSV/JSON/API). If only PDF or heavily JavaScript-rendered pages are available, implement a minimal scraper for the key headline figures. If the data format is too unstable to scrape reliably, mark this source as `status=unavailable` in the Data Freshness tab and log a clear message.
    - Backfills as far as freely available data allows
  - Notes: This is a **best-effort** source. Unlike CAA/Eurocontrol/World Bank which have stable download formats, ICAO's free data may not be reliably automatable. The pipeline must not fail if ICAO ingestion fails — it should gracefully degrade. If ICAO proves unusable, the global aviation picture is still covered by World Bank (annual) and Eurostat (European monthly pre-2021). The developer should spend a maximum of 2 hours attempting ICAO automation before marking it as best-effort/manual.

- [x] Task 13: Implement World Bank air transport ingestion
  - File: `src/ingestion/world_bank.py`
  - Action: Implement `WorldBankIngestion` class that:
    - Uses World Bank API to fetch indicator IS.AIR.PSGR (air transport, passengers carried)
    - Fetches global and UK-specific annual totals
    - Backfills from 2015 onwards (data goes back to 1970s)
  - Notes: Annual only. Simple REST API, returns JSON. Well-documented and reliable.

#### Phase 5: Normalisation Layer

- [x] Task 14: Implement spike detection and normalisation
  - File: `src/normalisation/spike_detector.py`
  - Action: Implement `SpikeDetector` class that:
    - Takes a pandas DataFrame with date + raw_value columns (monthly granularity — all sources are aggregated to monthly before normalisation)
    - **Year-over-year same-month comparison (seasonality-aware):**
      - For each data point, group all historical values by calendar month (e.g., all Januaries, all Februaries)
      - Calculate the mean and standard deviation for that calendar month across all available years (excluding COVID period)
      - Flag a data point as a spike if it exceeds 2 standard deviations from its same-month historical mean
      - If fewer than 3 years of history exist for a given calendar month, mark the data point as `low_confidence=True` (insufficient comparison data)
    - For flagged spikes: checks against known events dict from config (volcanic eruptions, airline collapses, pandemic waves, regulatory news)
    - Applies normalisation: replaces spike values with the same-month historical mean (not a rolling average, which would destroy seasonality)
    - Adds columns: `normalised_value`, `is_spike` (boolean), `spike_event` (string, event name or empty), `is_normalised` (boolean), `low_confidence` (boolean — True if <3 years of comparison data for that calendar month)
    - **COVID period handling (Mar 2020 - Jun 2021):**
      - Flags entire period with `spike_event="COVID-19 structural break"` and `is_spike=True`
      - COVID months are **excluded from the same-month historical pools** — they must never pollute the baseline for future spike detection (e.g., March 2020 must not drag down the "March average" used to evaluate March 2022)
      - For `normalised_value` during COVID: use the same-month mean from pre-COVID years only (e.g., COVID March 2020 normalised to mean of March 2015-2019). This naturally handles seasonality — each COVID month gets its own seasonal baseline.
      - Post-COVID (Jul 2021 onwards): the same-month comparison resumes normally. Jul 2021 is compared against Jul 2015-2019 (excluding Jul 2020). As post-COVID years accumulate, they join the comparison pool naturally.
    - Returns DataFrame with both raw and normalised values
  - Notes: Normalisation should be idempotent — running it twice produces the same result. The year-over-year approach means summer peaks and winter troughs are treated as normal seasonal patterns, not anomalies. Only deviations from what's normal *for that time of year* are flagged. COVID months are quarantined from all statistical calculations to prevent contamination.

#### Phase 6: Google Sheets Output

- [x] Task 15: Implement Google Sheets writer
  - File: `src/output/google_sheets.py`
  - Action: Implement `GoogleSheetsWriter` class that:
    - Authenticates via Google Cloud service account (credentials JSON file)
    - Creates or opens target spreadsheet
    - Creates separate worksheet tabs:
      - "Holiday Intent" — all holiday-related Google Trends terms (raw + normalised)
      - "Insurance Intent" — all insurance-related Google Trends terms (raw + normalised)
      - "UK Passengers" — CAA, Eurocontrol monthly data (raw + normalised)
      - "Global Aviation" — ICAO, Eurostat, World Bank data (raw + normalised)
      - "Insurance Market" — FCA value measures data (raw + normalised)
      - "ONS Travel" — ONS overseas travel data (raw + normalised)
      - "Spike Log" — All detected spikes with dates, sources, event annotations
      - "Data Freshness" — Last updated date per source, expected next update
    - Writes full historical dataset on backfill using batch updates (gspread `update()` with full cell range rather than row-by-row) to minimise API calls. Batch writes into chunks of 500 rows per API call to stay within Google Sheets API quota (300 requests/min per project, 60 requests/min per user). Add 1-second delay between batch writes.
    - On incremental updates: reads the last date value from each Sheet tab to determine the watermark, then appends only rows with dates after that watermark. The watermark check prevents duplicates without needing to scan the full sheet. If the sheet is empty (first run), writes all data.
    - Includes retry logic for Sheets API calls: 3 retries with exponential backoff (2s, 4s, 8s) on 429 and 500-series errors to handle transient quota exhaustion
    - Formats headers, freezes top row, auto-sizes columns
  - Notes: gspread handles Google Sheets API. Service account must be shared as editor on the target spreadsheet. A full backfill across all tabs will make ~50-100 API calls — well within limits if batched properly.

#### Phase 7: Pipeline Orchestration

- [x] Task 16: Implement pipeline orchestrator
  - File: `src/main.py`
  - Action: Implement main pipeline that:
    - Accepts CLI arguments: `--backfill` (full historical pull), `--update` (incremental latest data), `--source <name>` (run single source only), `--force-refresh` (ignore cache TTL)
    - Creates a lock file (`data/.pipeline.lock`) on start, removes on exit (including on crash via `atexit` or `try/finally`). If lock file already exists, exit with error message. Include PID in lock file for debugging stale locks.
    - Configures Python `logging` module: INFO level to stdout, WARNING+ to `data/pipeline.log` with rotation (5MB max, 3 backups). Each log entry includes timestamp, source name, and level.
    - On `--backfill`: runs all ingestion modules with full date range, validates each source output, applies normalisation, writes to Google Sheet
    - On `--update`: runs all ingestion modules for latest period only, validates, applies normalisation, appends to Google Sheet
    - Handles errors per source gracefully — if one source fails, continue with others and log the failure
    - Outputs summary: sources fetched, rows added, any failures
    - Updates "Data Freshness" tab with last run timestamps
  - Notes: Keep orchestration simple. Sequential execution is fine — no need for async/parallel for ~8 sources.

- [x] Task 17: Create requirements.txt and setup
  - File: `requirements.txt`
  - Action: Pin dependencies with upper bounds to prevent breaking changes on unattended installs:
    ```
    pytrends>=4.9.2,<5.0
    gspread>=5.0.0,<7.0
    google-auth>=2.0.0,<3.0
    pandas>=2.0.0,<3.0
    openpyxl>=3.1.0,<4.0
    requests>=2.28.0,<3.0
    beautifulsoup4>=4.12.0,<5.0
    numpy>=1.24.0,<2.0
    python-dotenv>=1.0.0,<2.0
    ```
    Specify `python_requires=">=3.10"` (minimum for modern type hints and match statement support).
  - Notes: Include a brief README.md with setup instructions (create venv, install deps, copy `.env.example` to `.env`, configure service account credentials path and spreadsheet ID). Include `.env.example` with `GOOGLE_SHEET_ID=` and `GOOGLE_CREDENTIALS_PATH=credentials.json`.

### Acceptance Criteria

#### Google Trends
- [x] AC 1: Given pytrends is configured with geo='GB', when fetching "travel insurance" for the last month, then a DataFrame is returned with date index and integer values between 0-100.
- [x] AC 2: Given 11 search terms configured, when running a full backfill from 2015, then all terms are fetched in batches of 5 with "travel insurance" as anchor, values are normalised across batches, and weekly data is aggregated to monthly.
- [x] AC 3: Given a rate limit (429) response from Google, when the ingestion module encounters it, then it applies exponential backoff (starting at 30s, max 5 retries) before failing gracefully.

#### CAA
- [x] AC 4: Given CAA CSV data is available at caa.co.uk, when the CAA ingestion module runs, then it downloads and parses monthly UK international terminal passenger totals into a DataFrame with date and passenger_count columns.
- [x] AC 5: Given CAA data is cached locally, when the module runs again within the same month, then it returns cached data without re-downloading.

#### Eurocontrol
- [x] AC 6: Given Eurocontrol R&D archive CSV is available, when the Eurocontrol module runs, then it downloads daily flight data, filters for UK departures, and aggregates to monthly flight counts.

#### ONS
- [x] AC 7: Given ONS overseas travel data is available, when the ONS module runs, then it retrieves quarterly UK residents' overseas visit totals and stores them with quarterly date granularity.

#### FCA
- [x] AC 8: Given FCA value measures data is available, when the FCA module runs, then it extracts travel insurance specific metrics (claims frequency, acceptance rates, average premiums) as annual data points.

#### ICAO / Eurostat / World Bank
- [x] AC 9: Given ICAO, Eurostat, and World Bank data sources are available, when their respective modules run, then each returns a DataFrame with date and passenger/flight metrics at their native granularity (monthly for ICAO/Eurostat, annual for World Bank).

#### Spike Detection
- [x] AC 10: Given a July 2023 value that is 2.5 standard deviations above the mean of all previous Julys (2015-2022, excluding Jul 2020), when spike detection runs, then that data point is flagged as `is_spike=True` and `normalised_value` is set to the July historical mean.
- [x] AC 11: Given a normal seasonal summer peak (e.g., August 2023 is high but within 1.5 standard deviations of the August historical mean), when spike detection runs, then it is NOT flagged as a spike — seasonal patterns are preserved.
- [x] AC 12: Given a data point during the COVID period (Mar 2020 - Jun 2021), when spike detection runs, then it is flagged with `spike_event="COVID-19 structural break"` and `normalised_value` is set to the same-month pre-COVID mean (e.g., March 2020 normalised to mean of March 2015-2019).
- [x] AC 13: Given the same-month historical pool for any calendar month, when COVID months are included in the calculation, then the test FAILS — COVID months (Mar 2020 - Jun 2021) must be excluded from all statistical baselines.
- [x] AC 14: Given a data point with fewer than 3 years of same-month history available, when spike detection runs, then `low_confidence=True` is set on that row.
- [x] AC 15: Given a spike matching a known event in the config (e.g., Thomas Cook collapse Sep 2019), when spike detection runs, then the `spike_event` field is populated with the event name.

#### Google Sheets Output
- [x] AC 16: Given normalised data from all sources, when the Google Sheets writer runs, then it creates/updates the target spreadsheet with separate tabs per data category, including both raw and normalised values, plus `is_normalised`, `is_anchor_normalised`, and `low_confidence` flag columns where applicable.
- [x] AC 17: Given an incremental update with new data, when the writer reads the last date from each tab as a watermark, then only rows with dates after the watermark are appended, preventing duplicates.
- [x] AC 18: Given the pipeline completes, when checking the "Data Freshness" tab, then each source shows its last updated timestamp, the date of its most recent data point, and data_coverage_end if the source has a known truncation (e.g., Eurostat UK post-Brexit).

#### Data Validation
- [x] AC 19: Given a source returns a DataFrame missing required columns (date, source, metric_name, raw_value), when validation runs, then a `DataValidationError` is raised and the source is skipped with the error logged.
- [x] AC 20: Given a source returns a DataFrame with null values in `raw_value` affecting less than 20% of rows, when validation runs, then null rows are dropped and a warning is logged with the count of dropped rows.
- [x] AC 21: Given a source returns a DataFrame with null values in `raw_value` affecting 20% or more of rows, when validation runs, then the entire source is treated as a failure (not a data quality issue), the source is skipped, and an error is logged indicating probable upstream format change or parsing failure.
- [x] AC 22: Given a source returns a DataFrame with duplicate date+metric_name rows, when validation runs, then duplicates are dropped (keeping first) and a warning is logged.

#### Pipeline Orchestration
- [x] AC 23: Given the pipeline is run with `--backfill`, when all sources are fetched, then the Google Sheet contains historical data from 2015 (or earliest available per source) to present for all configured sources.
- [x] AC 24: Given one source fails during pipeline execution, when the orchestrator handles the error, then remaining sources continue processing and the failure is logged with source name and error details.
- [x] AC 25: Given the pipeline is run with `--update`, when only new data is available from some sources, then only those sources append new rows and the Data Freshness tab reflects the update.
- [x] AC 26: Given a pipeline run is already in progress, when a second run is attempted, then the second run exits immediately with a message indicating a lock file exists, preventing concurrent execution.

## Additional Context

### Dependencies

**Python Packages:**
- `pytrends` — Google Trends extraction
- `gspread` + `google-auth` — Google Sheets API output
- `pandas` — Data manipulation and aggregation
- `openpyxl` — Excel file parsing (ONS, FCA)
- `requests` — HTTP downloads (CAA, Eurocontrol, ONS, FCA)
- `beautifulsoup4` — HTML parsing where needed
- `numpy` — Statistical calculations for spike detection
- `python-dotenv` — Environment variable loading from `.env` file

**External Services:**
- Google Cloud service account with Google Sheets API enabled (credentials JSON file, path set via `GOOGLE_CREDENTIALS_PATH` env var)
- Target Google Sheet ID set via `GOOGLE_SHEET_ID` env var
- Google Trends (via pytrends — no account needed, but rate-limited)

### Testing Strategy

- **Unit tests per ingestion module:** Mock HTTP responses for each data source, verify parsing produces expected DataFrame schema
- **Normalisation tests:** Test spike detection with known anomalous data, verify normalisation produces expected output
- **Integration test:** End-to-end pipeline run with cached/mock data, verify Google Sheet output format
- **Backfill test:** Verify historical data stitching and anchor term normalisation for Google Trends

### Notes

- COVID period (March 2020 - June 2021) should be treated as a structural break. Data will be collected but flagged, and normalisation should handle this period carefully.
- Google Trends returns relative indices (0-100), not absolute numbers. Correlation validation against harder data (CAA, FCA) is important.
- CAA data has ~2 month lag, FCA annual, ONS quarterly with ~3mo lag. The pipeline must handle sources updating at different frequencies.
- ABI detailed data is members-only — excluded from initial build. Free ABI summary data from press releases/Key Facts PDFs can be added manually if desired.
- IATA replaced with ICAO (monthly, free) + Eurostat (European monthly, free) + World Bank (annual global, free).
- pytrends fragility is the biggest technical risk. All successful API responses should be cached locally. SerpApi is the paid fallback option (~$50/mo).
