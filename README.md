# Travel Insurance Market Intelligence Pipeline

Automated data pipeline that tracks whether the UK travel insurance market is growing or shrinking, using free public data sources.

## Data Sources

| Source | Data | Frequency |
|--------|------|-----------|
| Google Trends | Holiday + insurance intent search terms (GB) | Weekly → monthly |
| CAA | UK airport international passengers | Monthly |
| ONS | UK overseas travel statistics | Quarterly |
| FCA | Travel insurance value measures | Annual |
| Eurocontrol | UK departure flight counts | Daily → monthly |
| Eurostat | UK air passengers (pre-Brexit) | Monthly |
| ICAO | Global air passengers (best-effort) | Variable |
| World Bank | Global + UK air transport passengers | Annual |

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Google Sheets credentials:**
   - Create a Google Cloud service account with Sheets + Drive API access
   - Download the credentials JSON file
   - Share your target Google Sheet with the service account email

3. **Environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your values:
   # GOOGLE_SHEET_ID=your_sheet_id_here
   # GOOGLE_CREDENTIALS_PATH=path/to/credentials.json
   ```

## Usage

**Full historical backfill (2015–present):**
```bash
python -m src.main --backfill
```

**Incremental update (latest data only):**
```bash
python -m src.main --update
```

**Single source:**
```bash
python -m src.main --update --source google_trends
```

**Force refresh (bypass cache):**
```bash
python -m src.main --update --force-refresh
```

**Dry run (no Sheets writes):**
```bash
python -m src.main --backfill --dry-run --verbose
```

## Google Sheet Tabs

| Tab | Content |
|-----|---------|
| Holiday Intent | Google Trends holiday search terms |
| Insurance Intent | Google Trends insurance search terms |
| UK Passengers | CAA + Eurocontrol + Eurostat data |
| Global Aviation | World Bank + ICAO data |
| Insurance Market | FCA value measures |
| ONS Travel | ONS overseas travel statistics |
| Spike Log | All detected anomalies with annotations |
| Data Freshness | Source status and last-updated timestamps |

## Normalisation

- **Spike detection:** Year-over-year same-month comparison (2 std dev threshold)
- **COVID handling:** Mar 2020–Jun 2021 quarantined; normalised to pre-COVID same-month means
- **Known events:** Thomas Cook collapse, Eyjafjallajokull eruption auto-annotated
- **Low confidence flag:** Added when fewer than 3 years of history for a given month

## Project Structure

```
src/
  config.py              # Central configuration
  main.py                # Pipeline orchestrator (CLI entry point)
  ingestion/
    base.py              # Base class with retry, validation, circuit breaker
    google_trends.py     # Anchor-term normalised Google Trends
    caa.py               # CAA airport passenger data
    ons.py               # ONS overseas travel
    fca.py               # FCA insurance value measures
    eurocontrol.py       # Eurocontrol flight data
    eurostat.py          # Eurostat air passengers (Brexit-aware)
    icao.py              # ICAO global air passengers (best-effort)
    world_bank.py        # World Bank air transport
  cache/
    file_cache.py        # Two-tier file cache with per-source TTL
  normalisation/
    spike_detector.py    # Seasonality-aware spike detection
  output/
    google_sheets.py     # Google Sheets writer with batching
data/
  cache/                 # Cached raw + parsed data (gitignored)
  pipeline.log           # Rotating log file
```
