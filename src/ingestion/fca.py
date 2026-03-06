import io
import logging
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)

# Known direct download URLs for FCA value measures data
FCA_DATA_URLS = [
    "https://www.fca.org.uk/publication/data/gi-value-measures-data-august-2019.xlsx",
]

# Pages that may contain download links
FCA_PAGES = [
    "https://www.fca.org.uk/data/general-insurance-value-measures",
    "https://www.fca.org.uk/data/general-insurance-value-measures-data-year-ending-31-august-2019",
    "https://www.fca.org.uk/data/general-insurance-value-measures-data-year-ending-31-august-2018",
]


class FCAIngestion(BaseIngestionModule):
    source_name = "fca"

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "fca_value_measures"
        cached = self.cache.get_raw(cache_id, ext=".xlsx", force_refresh=force_refresh)
        if cached is not None:
            return cached

        # Try known direct download URLs first
        for url in FCA_DATA_URLS:
            try:
                response = self.fetch_url(url)
                data = response.content
                self.cache.put_raw(cache_id, data, ext=".xlsx")
                return data
            except Exception as exc:
                logger.info("%s: Direct download failed for %s: %s", self.source_name, url, exc)

        # Fallback: scrape pages for download links
        for page_url in FCA_PAGES:
            try:
                response = self.fetch_url(page_url)
                soup = BeautifulSoup(response.content, "html.parser")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if ".xlsx" in href.lower() or ".xls" in href.lower() or ".csv" in href.lower():
                        if not href.startswith("http"):
                            href = f"https://www.fca.org.uk{href}"
                        data = self.fetch_url(href).content
                        ext = ".csv" if ".csv" in href.lower() else ".xlsx"
                        self.cache.put_raw(cache_id, data, ext=ext)
                        return data
            except Exception as exc:
                logger.info("%s: Page scrape failed for %s: %s", self.source_name, page_url, exc)

        logger.error("%s: No data download found from any FCA source", self.source_name)
        return b""

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        try:
            xls = pd.ExcelFile(io.BytesIO(raw_data))
            all_rows = []

            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                text = df.to_string().lower()
                if "travel" not in text:
                    continue

                df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

                # Look for travel insurance rows
                for _, row in df.iterrows():
                    row_text = " ".join(str(v).lower() for v in row.values)
                    if "travel" in row_text:
                        for col in df.columns:
                            val = row[col]
                            if isinstance(val, (int, float)) and pd.notna(val):
                                try:
                                    year = int(str(col).strip()[:4])
                                    if 2015 <= year <= 2030:
                                        all_rows.append({
                                            "date": pd.Timestamp(year=year, month=12, day=31),
                                            "source": self.source_name,
                                            "metric_name": "travel_insurance_value_measure",
                                            "raw_value": float(val),
                                        })
                                except (ValueError, TypeError):
                                    continue

            if all_rows:
                return pd.DataFrame(all_rows)

        except Exception as exc:
            logger.warning("%s: Excel parsing failed (%s), trying CSV", self.source_name, exc)
            text = raw_data.decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(text))
            df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        logger.warning("%s: Could not extract travel insurance data from FCA file", self.source_name)
        return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

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
