import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Project root: directory containing this config file's parent package
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Google Sheets (Apps Script webhook)
APPS_SCRIPT_WEBHOOK_URL = os.environ.get("APPS_SCRIPT_WEBHOOK_URL", "")

# Sheet tab names
SHEET_HOLIDAY_INTENT = "Holiday Intent"
SHEET_INSURANCE_INTENT = "Insurance Intent"
SHEET_UK_PASSENGERS = "UK Passengers"
SHEET_GLOBAL_AVIATION = "Global Aviation"
SHEET_INSURANCE_MARKET = "Insurance Market"
SHEET_ONS_TRAVEL = "ONS Travel"
SHEET_SPIKE_LOG = "Spike Log"
SHEET_MARKET_SUMMARY = "Market Demand Summary"
SHEET_DATA_FRESHNESS = "Data Freshness"

# Google Trends search terms
HOLIDAY_INTENT_TERMS = [
    "book holiday",
    "cheap flights",
    "package holiday",
    "all inclusive holiday",
    "summer holiday",
    "winter sun",
]

INSURANCE_INTENT_TERMS = [
    "travel insurance",
    "holiday insurance",
    "annual travel insurance",
    "single trip travel insurance",
    "travel insurance comparison",
]

ANCHOR_TERM = "travel insurance"
ANCHOR_MIN_VALUE = 5  # skip normalisation if anchor below this

# All terms combined
ALL_TERMS = HOLIDAY_INTENT_TERMS + INSURANCE_INTENT_TERMS

# Google Trends settings
PYTRENDS_GEO = "GB"
PYTRENDS_DELAY_SECONDS = 15
PYTRENDS_MAX_RETRIES = 5
PYTRENDS_INITIAL_BACKOFF_SECONDS = 30
PYTRENDS_CHUNK_YEARS = 5
PYTRENDS_MAX_TERMS_PER_BATCH = 5

# Base ingestion retry settings
BASE_MAX_RETRIES = 3
BASE_INITIAL_BACKOFF_SECONDS = 5
BASE_BACKOFF_MULTIPLIER = 3
HTTP_TIMEOUT_SECONDS = 30
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Backfill date range
BACKFILL_START_DATE = date(2015, 1, 1)

# COVID structural break period
COVID_START = date(2020, 3, 1)
COVID_END = date(2021, 6, 30)

# Spike detection parameters
SPIKE_THRESHOLD_STD_DEVS = 2
SPIKE_MIN_YEARS_FOR_CONFIDENCE = 3

# Known event annotations: (start_date, end_date) -> event description
KNOWN_EVENTS = {
    (date(2010, 4, 14), date(2010, 4, 20)): "Eyjafjallajokull volcanic eruption",
    (date(2019, 9, 23), date(2019, 10, 31)): "Thomas Cook collapse",
    (date(2020, 3, 1), date(2021, 6, 30)): "COVID-19 structural break",
}

# Source URLs (best-known as of spec creation; may need discovery at runtime)
SOURCE_URLS = {
    "caa": "https://www.caa.co.uk/data-and-analysis/uk-aviation-market/airports/uk-airport-data/",
    "eurocontrol": "https://www.eurocontrol.int/dashboard/rnd-data-archive",
    "ons": "https://api.beta.ons.gov.uk/v1",
    "ons_visits_abroad": "https://www.ons.gov.uk/peoplepopulationandcommunity/leisureandtourism/datasets/ukresidentsvisitsabroad",
    "fca": "https://www.fca.org.uk/data/general-insurance-value-measures",
    "icao": "https://www.icao.int/sustainability/Pages/Air-Traffic-Monitor.aspx",
    "eurostat": "https://ec.europa.eu/eurostat/databrowser/view/avia_paoc/default/table",
    "world_bank": "https://api.worldbank.org/v2/country/all/indicator/IS.AIR.PSGR",
}

# Cache TTLs in seconds per source
CACHE_TTL = {
    "google_trends": 24 * 3600,       # 24 hours
    "eurocontrol": 24 * 3600,          # 24 hours
    "caa": 30 * 24 * 3600,             # 30 days
    "ons": 90 * 24 * 3600,             # 90 days
    "fca": 365 * 24 * 3600,            # 365 days
    "icao": 30 * 24 * 3600,            # 30 days
    "eurostat": 30 * 24 * 3600,        # 30 days
    "world_bank": 365 * 24 * 3600,     # 365 days
}

# Validation
NULL_THRESHOLD_PERCENT = 20  # >=20% nulls = source failure

# Logging
LOG_FILE = str(PROJECT_ROOT / "data" / "pipeline.log")
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5MB
LOG_BACKUP_COUNT = 3

# Lock file
LOCK_FILE = str(PROJECT_ROOT / "data" / ".pipeline.lock")

