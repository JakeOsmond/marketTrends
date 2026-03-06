import logging
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)


class ICaoIngestion(BaseIngestionModule):
    """Best-effort ICAO ingestion. May return empty data if scraping fails."""
    source_name = "icao"

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "icao_air_transport"
        cached = self.cache.get_raw(cache_id, ext=".html", force_refresh=force_refresh)
        if cached is not None:
            return cached

        try:
            url = config.SOURCE_URLS["icao"]
            response = self.fetch_url(url)
            data = response.content
            self.cache.put_raw(cache_id, data, ext=".html")
            return data
        except Exception as exc:
            logger.warning(
                "%s: Failed to fetch ICAO data (best-effort source): %s",
                self.source_name, exc,
            )
            return b""

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            logger.info("%s: No data available (best-effort source)", self.source_name)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        soup = BeautifulSoup(raw_data, "html.parser")
        rows = []

        # Attempt to extract data from tables on the page
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if len(cells) >= 2:
                    # Look for rows with date-like and numeric content
                    for i, cell in enumerate(cells):
                        try:
                            dt = pd.to_datetime(cell, errors="coerce")
                            if pd.notna(dt):
                                # Try to find a numeric value in adjacent cells
                                for j, other_cell in enumerate(cells):
                                    if j != i:
                                        val_str = other_cell.replace(",", "").replace("%", "").strip()
                                        try:
                                            val = float(val_str)
                                            rows.append({
                                                "date": dt,
                                                "source": self.source_name,
                                                "metric_name": "global_air_passengers_icao",
                                                "raw_value": val,
                                            })
                                            break
                                        except ValueError:
                                            continue
                                break
                        except Exception:
                            continue

        if not rows:
            logger.info(
                "%s: Could not extract structured data from ICAO page (best-effort source). "
                "This source may require manual data collection.",
                self.source_name,
            )
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        return pd.DataFrame(rows)

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or config.BACKFILL_START_DATE
        end_date = end_date or date.today()
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        if df.empty:
            return df
        df = self.validate(df)
        mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
        return df[mask].reset_index(drop=True)

    def get_latest(self, force_refresh: bool = False) -> pd.DataFrame:
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        if df.empty:
            return df
        return self.validate(df)
