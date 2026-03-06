import io
import logging
import re
from datetime import date

import pandas as pd

from src import config
from src.ingestion.base import BaseIngestionModule

logger = logging.getLogger(__name__)

# Direct download URLs for UK residents' visits abroad
ONS_URLS = [
    # 2019-2023 edition (has quarterly time series in sheet 1)
    (
        "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/leisureandtourism/"
        "datasets/ukresidentsvisitsabroad/2019to2023/traveltrendssection3ukresidentsvisitsabroad.xlsx"
    ),
    # 2024 edition
    (
        "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/leisureandtourism/"
        "datasets/ukresidentsvisitsabroad/2024/annualukresidents2024.xlsx"
    ),
]


class ONSIngestion(BaseIngestionModule):
    source_name = "ons"

    def fetch(self, force_refresh: bool = False) -> list[bytes]:
        """Fetch both ONS files (historical + latest)."""
        results = []
        for i, url in enumerate(ONS_URLS):
            cache_id = f"ons_visits_abroad_{i}"
            cached = self.cache.get_raw(cache_id, ext=".xlsx", force_refresh=force_refresh)
            if cached is not None:
                results.append(cached)
                continue
            try:
                response = self.fetch_url(url)
                data = response.content
                self.cache.put_raw(cache_id, data, ext=".xlsx")
                results.append(data)
            except Exception as exc:
                logger.warning("%s: Failed to fetch %s: %s", self.source_name, url, exc)
        return results

    def parse(self, raw_data_list: list[bytes]) -> pd.DataFrame:
        if not raw_data_list:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "granularity"])

        all_rows = []
        for raw_data in raw_data_list:
            if not raw_data:
                continue
            rows = self._parse_file(raw_data)
            all_rows.extend(rows)

        if not all_rows:
            logger.error("%s: No usable data found", self.source_name)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "granularity"])

        df = pd.DataFrame(all_rows)
        # Deduplicate: keep the latest file's value for any given date
        df = df.drop_duplicates(subset=["date", "metric_name"], keep="last")
        return df.sort_values("date").reset_index(drop=True)

    def _parse_file(self, raw_data: bytes) -> list[dict]:
        """Parse a single ONS Excel file for quarterly visit data."""
        rows = []
        try:
            xls = pd.ExcelFile(io.BytesIO(raw_data))
        except Exception as exc:
            logger.warning("%s: Could not open Excel file: %s", self.source_name, exc)
            return rows

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

            # Find the header row — individual cells must match (not title rows)
            header_row = None
            for i in range(min(10, len(df))):
                cell_vals = [str(v).strip().lower() for v in df.iloc[i] if pd.notna(v)]
                # Header row should have "year" as a standalone cell value
                if "year" in cell_vals and any("visit" in c for c in cell_vals):
                    header_row = i
                    break

            if header_row is None:
                continue

            headers = [str(v).strip() for v in df.iloc[header_row]]

            # Find Year, Quarter, and Visits columns
            year_col = quarter_col = visits_col = None
            for j, h in enumerate(headers):
                h_lower = h.lower()
                if h_lower == "year":
                    year_col = j
                elif h_lower == "quarter":
                    quarter_col = j
                elif "visit" in h_lower and "change" not in h_lower and "season" not in h_lower:
                    if visits_col is None:
                        visits_col = j

            if year_col is None or visits_col is None:
                continue

            # Parse data rows
            for i in range(header_row + 1, len(df)):
                try:
                    year_val = df.iloc[i, year_col]
                    year = int(float(str(year_val).strip()))
                    if year < 1990 or year > 2030:
                        continue
                except (ValueError, TypeError):
                    continue

                visits_val = df.iloc[i, visits_col]
                try:
                    visits = float(str(visits_val).strip())
                except (ValueError, TypeError):
                    continue

                if quarter_col is not None:
                    q_val = str(df.iloc[i, quarter_col]).strip().lower()
                    if q_val == "total" or q_val == "nan":
                        # Annual total row
                        rows.append({
                            "date": pd.Timestamp(year=year, month=12, day=31),
                            "source": self.source_name,
                            "metric_name": "uk_visits_abroad_annual",
                            "raw_value": visits,
                            "granularity": "annual",
                        })
                        continue
                    try:
                        quarter = int(float(q_val))
                    except (ValueError, TypeError):
                        continue

                    month = {1: 3, 2: 6, 3: 9, 4: 12}[quarter]
                    dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
                    rows.append({
                        "date": dt,
                        "source": self.source_name,
                        "metric_name": "uk_visits_abroad",
                        "raw_value": visits,
                        "granularity": "quarterly",
                    })
                else:
                    # No quarter column — annual data
                    rows.append({
                        "date": pd.Timestamp(year=year, month=12, day=31),
                        "source": self.source_name,
                        "metric_name": "uk_visits_abroad_annual",
                        "raw_value": visits,
                        "granularity": "annual",
                    })

            if rows:
                break  # Found data in this sheet, no need to check others

        return rows

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or config.BACKFILL_START_DATE
        end_date = end_date or date.today()
        raw_list = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw_list)
        df = self.validate(df)
        if df.empty:
            return df
        mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
        return df[mask].reset_index(drop=True)

    def get_latest(self, force_refresh: bool = False) -> pd.DataFrame:
        raw_list = self.fetch(force_refresh=force_refresh)
        df = self.parse(raw_list)
        return self.validate(df)
