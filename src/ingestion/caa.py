import io
import logging
import re
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)

CAA_BASE = "https://www.caa.co.uk"
CAA_INDEX_URL = config.SOURCE_URLS["caa"]

# Month names for building page URLs
MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


class CAAIngestion(BaseIngestionModule):
    source_name = "caa"

    def _find_passenger_csv(self, page_url: str) -> str | None:
        """Fetch a CAA data page and find the Table 09 or Table 10 CSV link."""
        try:
            response = self.fetch_url(page_url)
        except Exception:
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True).lower()
            href = link["href"]
            # Table 09 = Terminal and Transit Passengers (total UK pax)
            if "table 09" in text or "terminal and transit" in text.lower():
                if not href.startswith("http"):
                    href = f"{CAA_BASE}{href}"
                return href
        return None

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "caa_passenger_data"
        cached = self.cache.get_raw(cache_id, ext=".csv", force_refresh=force_refresh)
        if cached is not None:
            return cached

        # Strategy: fetch the index page, find year pages, then find monthly
        # data pages within each year, and download Table 09 CSVs.
        index_response = self.fetch_url(CAA_INDEX_URL)
        index_soup = BeautifulSoup(index_response.content, "html.parser")

        # Find year page links (e.g., /uk-airport-data-2024/)
        year_links = []
        for link in index_soup.find_all("a", href=True):
            href = link["href"]
            match = re.search(r"uk-airport-data-(\d{4})/?$", href)
            if match:
                year = int(match.group(1))
                if year >= 2015:
                    if not href.startswith("http"):
                        href = f"{CAA_BASE}{href}"
                    year_links.append((year, href))

        if not year_links:
            logger.error("%s: No year pages found on CAA index", self.source_name)
            return b""

        year_links.sort(key=lambda x: x[0])
        all_csvs = []

        for year, year_url in year_links:
            # Try annual data page first (has full year in one file)
            annual_url = f"{year_url.rstrip('/')}/annual-{year}/"
            csv_link = self._find_passenger_csv(annual_url)

            if csv_link:
                try:
                    data = self.fetch_url(csv_link).content
                    all_csvs.append((year, "annual", data))
                    logger.info("%s: Downloaded annual data for %d", self.source_name, year)
                    continue
                except Exception as exc:
                    logger.info("%s: Annual CSV download failed for %d: %s", self.source_name, year, exc)

            # Fallback: try individual monthly pages
            for month_idx, month_name in enumerate(MONTHS, 1):
                month_url = f"{year_url.rstrip('/')}/{month_name}-{year}/"
                csv_link = self._find_passenger_csv(month_url)
                if csv_link:
                    try:
                        data = self.fetch_url(csv_link).content
                        all_csvs.append((year, month_name, data))
                    except Exception:
                        continue

        if not all_csvs:
            logger.error("%s: No CSV files downloaded from any year", self.source_name)
            return b""

        # Use the most recent annual file if available, otherwise combine monthly
        # For simplicity, just use the latest annual CSV we got
        # (annual files contain all months for that year)
        annual_csvs = [(y, l, d) for y, l, d in all_csvs if l == "annual"]

        if annual_csvs:
            # Combine all annual CSVs
            combined = b""
            for i, (y, _, data) in enumerate(sorted(annual_csvs)):
                if i == 0:
                    combined = data
                else:
                    # Skip the header line on subsequent files
                    lines = data.split(b"\n", 1)
                    if len(lines) > 1:
                        combined += b"\n" + lines[1]
            self.cache.put_raw(cache_id, combined, ext=".csv")
            return combined
        else:
            # Just return the first monthly CSV as a starting point
            _, _, data = all_csvs[0]
            self.cache.put_raw(cache_id, data, ext=".csv")
            return data

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        text = raw_data.decode("utf-8", errors="replace")
        df = pd.read_csv(io.StringIO(text))
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # CAA annual CSVs have: this_period (year), rpt_apt_name, term_pax_tp
        # We aggregate all airports per year to get UK total
        period_col = None
        pax_col = None

        for col in df.columns:
            if col in ("this_period", "period"):
                period_col = col
            if col == "term_pax_tp" or ("terminal" in col and "pax" in col):
                pax_col = col
            if pax_col is None and "total_pax_tp" in col:
                pax_col = col

        if period_col is None or pax_col is None:
            logger.error(
                "%s: Could not identify period/pax columns. Columns: %s",
                self.source_name, list(df.columns),
            )
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        df["_pax"] = pd.to_numeric(df[pax_col].astype(str).str.replace(",", ""), errors="coerce")
        df["_period"] = df[period_col].astype(str).str.strip()

        # Group by period (year) and sum all airport passengers
        grouped = df.groupby("_period")["_pax"].sum().reset_index()

        rows = []
        for _, row in grouped.iterrows():
            try:
                year = int(float(row["_period"]))
                if 1990 <= year <= 2030:
                    rows.append({
                        "date": pd.Timestamp(year=year, month=12, day=31),
                        "source": self.source_name,
                        "metric_name": "uk_terminal_passengers",
                        "raw_value": row["_pax"],
                    })
            except (ValueError, TypeError):
                continue

        if not rows:
            logger.error("%s: No parseable year data found", self.source_name)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or config.BACKFILL_START_DATE
        end_date = end_date or date.today()
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        df = self.validate(df)
        if df.empty:
            return df
        mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
        return df[mask].reset_index(drop=True)

    def get_latest(self, force_refresh: bool = False) -> pd.DataFrame:
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        return self.validate(df)
