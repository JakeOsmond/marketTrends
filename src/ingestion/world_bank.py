import logging
from datetime import date

import pandas as pd

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)

WB_API_BASE = "https://api.worldbank.org/v2"


class WorldBankIngestion(BaseIngestionModule):
    source_name = "world_bank"

    def fetch(self, force_refresh: bool = False) -> bytes:
        cache_id = "world_bank_air_transport"
        cached = self.cache.get_raw(cache_id, ext=".json", force_refresh=force_refresh)
        if cached is not None:
            return cached

        import json

        all_records = []
        # Fetch UK and World separately to avoid pagination issues
        # ("WLD" is the World Bank aggregate code for global totals)
        for country_code in ("GBR", "WLD"):
            page = 1
            while True:
                url = (
                    f"{WB_API_BASE}/country/{country_code}/indicator/IS.AIR.PSGR"
                    f"?format=json&per_page=1000&date=2015:2026&page={page}"
                )
                response = self.fetch_url(url)
                try:
                    payload = json.loads(response.content)
                except json.JSONDecodeError:
                    break

                if not isinstance(payload, list) or len(payload) < 2:
                    break

                records = payload[1]
                if not records:
                    break

                all_records.extend(records)

                metadata = payload[0]
                total_pages = metadata.get("pages", 1)
                if page >= total_pages:
                    break
                page += 1

        data = json.dumps(all_records).encode("utf-8")
        self.cache.put_raw(cache_id, data, ext=".json")
        return data

    def parse(self, raw_data: bytes) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        import json
        try:
            records = json.loads(raw_data)
        except json.JSONDecodeError as exc:
            logger.error("%s: Failed to parse JSON: %s", self.source_name, exc)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        if not isinstance(records, list) or not records:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value"])

        rows = []
        for record in records:
            country_code = record.get("country", {}).get("id", "")
            year = record.get("date")
            value = record.get("value")

            if year and value is not None:
                metric = f"air_passengers_{'uk' if country_code == 'GBR' else 'global'}"
                rows.append({
                    "date": pd.Timestamp(year=int(year), month=12, day=31),
                    "source": self.source_name,
                    "metric_name": metric,
                    "raw_value": float(value),
                })

        return pd.DataFrame(rows)

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
