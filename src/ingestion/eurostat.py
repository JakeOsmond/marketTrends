import io
import logging
import re
from datetime import date

import pandas as pd

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)

EUROSTAT_API_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/avia_paoc"


class EurostatIngestion(BaseIngestionModule):
    source_name = "eurostat"

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "eurostat_avia_paoc_uk"
        cached = self.cache.get_raw(cache_id, ext=".tsv", force_refresh=force_refresh)
        if cached is not None:
            return cached

        # Try Eurostat SDMX API for UK data
        try:
            api_url = f"{EUROSTAT_API_URL}/M.PAS_CRD.UK.TOTAL?format=TSV"
            response = self.fetch_url(api_url)
            data = response.content
            self.cache.put_raw(cache_id, data, ext=".tsv")
            return data
        except Exception as exc:
            logger.warning("%s: API request failed (%s), trying bulk download", self.source_name, exc)

        # Fallback: try bulk TSV download
        try:
            bulk_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/avia_paoc?format=TSV&compressed=false"
            response = self.fetch_url(bulk_url)
            data = response.content
            self.cache.put_raw(cache_id, data, ext=".tsv")
            return data
        except Exception as exc:
            logger.error("%s: Bulk download also failed: %s", self.source_name, exc)
            return b""

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "data_coverage_end"])

        text = raw_data.decode("utf-8", errors="replace")
        df = pd.read_csv(io.StringIO(text), sep="\t")
        df.columns = [c.strip() for c in df.columns]

        # Eurostat TSV format has time periods as columns
        # Try to parse into long format
        rows = []
        for _, row in df.iterrows():
            row_text = " ".join(str(v).lower() for v in row.values[:3])
            if "uk" not in row_text and "united kingdom" not in row_text:
                continue

            for col in df.columns:
                col_str = str(col).strip()
                # Match patterns like "2020M01" or "2020-01" (4-digit year + separator + 2-digit month)
                if re.match(r"^\d{4}[M\-]\d{2}$", col_str):
                    try:
                        period = col_str.replace("M", "-")
                        dt = pd.to_datetime(period + "-01", errors="coerce")
                        if pd.isna(dt):
                            continue
                        val_str = str(row[col]).strip().replace(" ", "").rstrip("bepsu")
                        val = float(val_str)
                        rows.append({
                            "date": dt,
                            "source": self.source_name,
                            "metric_name": "uk_air_passengers_eurostat",
                            "raw_value": val,
                        })
                    except (ValueError, TypeError):
                        continue

        if not rows:
            logger.warning("%s: No UK data found in Eurostat response", self.source_name)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "data_coverage_end"])

        result = pd.DataFrame(rows)
        result = result.sort_values("date").reset_index(drop=True)

        # Brexit handling: detect if UK data stops
        last_date = result["date"].max()
        brexit_cutoff = pd.Timestamp("2021-01-01")
        if last_date < pd.Timestamp("today") - pd.DateOffset(months=6):
            logger.warning(
                "%s: UK data appears to end at %s (likely Brexit-related truncation)",
                self.source_name, last_date.strftime("%Y-%m"),
            )
            result["data_coverage_end"] = last_date.strftime("%Y-%m")
        else:
            result["data_coverage_end"] = ""

        return result

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or config.BACKFILL_START_DATE
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
