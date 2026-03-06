import io
import logging
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)


class EurocontrolIngestion(BaseIngestionModule):
    source_name = "eurocontrol"

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "eurocontrol_flights"
        cached = self.cache.get_raw(cache_id, ext=".csv", force_refresh=force_refresh)
        if cached is not None:
            return cached

        # Fetch the R&D data archive page to find CSV download links
        page_url = config.SOURCE_URLS["eurocontrol"]
        response = self.fetch_url(page_url)
        soup = BeautifulSoup(response.content, "html.parser")

        csv_link = None
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".csv" in href.lower():
                if not href.startswith("http"):
                    href = f"https://www.eurocontrol.int{href}"
                csv_link = href
                break

        if not csv_link:
            logger.error("%s: No CSV download found on Eurocontrol data archive page", self.source_name)
            return b""

        data = self.fetch_url(csv_link).content
        self.cache.put_raw(cache_id, data, ext=".csv")
        return data

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        text = raw_data.decode("utf-8", errors="replace")
        df = pd.read_csv(io.StringIO(text))
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Find state/entity column and filter for UK
        state_col = None
        for col in df.columns:
            if any(kw in col for kw in ("state", "entity", "country")):
                state_col = col
                break

        if state_col:
            uk_variants = ["united kingdom", "uk", "gb", "great britain"]
            mask = df[state_col].str.lower().str.strip().isin(uk_variants)
            df = df[mask]

        # Find date and flight count columns
        date_col = None
        flight_col = None
        for col in df.columns:
            if any(kw in col for kw in ("date", "day", "period")):
                date_col = col
            if any(kw in col for kw in ("flight", "movement", "ifr")):
                flight_col = col

        if date_col is None or flight_col is None:
            logger.error(
                "%s: Could not identify columns. Found: %s", self.source_name, list(df.columns)
            )
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["_flights"] = pd.to_numeric(df[flight_col], errors="coerce")
        df = df.dropna(subset=["_date", "_flights"])

        # Aggregate daily to monthly, excluding the current incomplete month
        df["_month"] = df["_date"].dt.to_period("M")
        current_month = pd.Timestamp.today().to_period("M")
        df = df[df["_month"] < current_month]
        monthly = df.groupby("_month")["_flights"].sum().reset_index()
        monthly["date"] = monthly["_month"].dt.to_timestamp()

        result = pd.DataFrame({
            "date": monthly["date"],
            "source": self.source_name,
            "metric_name": "uk_departure_flights",
            "raw_value": monthly["_flights"],
        })

        return result

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or date(2016, 1, 1)
        end_date = end_date or date.today()
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        df = self.validate(df)
        mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
        return df[mask].reset_index(drop=True)

    def get_latest(self, force_refresh: bool = False) -> pd.DataFrame:
        raw = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw)
        return self.validate(df)
