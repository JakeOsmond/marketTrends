import logging
import time
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd
import requests

from src import config
from src.cache.file_cache import FileCache

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    pass


class BaseIngestionModule(ABC):
    source_name: str = ""
    max_retries: int = config.BASE_MAX_RETRIES
    initial_backoff: float = config.BASE_INITIAL_BACKOFF_SECONDS
    backoff_multiplier: float = config.BASE_BACKOFF_MULTIPLIER

    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = config.HTTP_TIMEOUT_SECONDS
        self.cache = FileCache(self.source_name)

    def fetch_url(self, url: str, **kwargs) -> requests.Response:
        backoff = self.initial_backoff
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=config.HTTP_TIMEOUT_SECONDS, **kwargs)
                if response.status_code in config.RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    logger.warning(
                        "%s: HTTP %d from %s, retry %d/%d in %.0fs",
                        self.source_name, response.status_code, url,
                        attempt + 1, self.max_retries, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= self.backoff_multiplier
                    continue
                response.raise_for_status()
                return response
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    logger.warning(
                        "%s: %s fetching %s, retry %d/%d in %.0fs",
                        self.source_name, type(exc).__name__, url,
                        attempt + 1, self.max_retries, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= self.backoff_multiplier
                    continue
                raise

        if last_exception:
            raise last_exception
        raise requests.HTTPError(f"Failed after {self.max_retries} retries")

    @abstractmethod
    def fetch(self, force_refresh: bool = False) -> bytes | str:
        """Retrieve raw data from the source."""

    @abstractmethod
    def parse(self, raw_data) -> pd.DataFrame:
        """Parse raw data into standardised DataFrame with columns:
        date, source, metric_name, raw_value
        """

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = {"date", "source", "metric_name", "raw_value"}
        missing = required_columns - set(df.columns)
        if missing:
            raise DataValidationError(
                f"{self.source_name}: Missing required columns: {missing}"
            )

        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            try:
                df["date"] = pd.to_datetime(df["date"])
            except Exception:
                raise DataValidationError(
                    f"{self.source_name}: 'date' column cannot be converted to datetime"
                )

        df["raw_value"] = pd.to_numeric(df["raw_value"], errors="coerce")

        null_count = df["raw_value"].isna().sum()
        total_count = len(df)

        if total_count > 0:
            null_pct = (null_count / total_count) * 100
            if null_pct >= config.NULL_THRESHOLD_PERCENT:
                raise DataValidationError(
                    f"{self.source_name}: {null_pct:.1f}% of rows have null raw_value "
                    f"({null_count}/{total_count}). Probable upstream format change."
                )
            if null_count > 0:
                logger.warning(
                    "%s: Dropped %d null rows (%.1f%% of %d total)",
                    self.source_name, null_count, null_pct, total_count,
                )
                df = df.dropna(subset=["raw_value"])

        dupes = df.duplicated(subset=["date", "metric_name"], keep="first")
        dupe_count = dupes.sum()
        if dupe_count > 0:
            logger.warning(
                "%s: Dropped %d duplicate date+metric_name rows",
                self.source_name, dupe_count,
            )
            df = df.drop_duplicates(subset=["date", "metric_name"], keep="first")

        return df.reset_index(drop=True)

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
