import logging
import time
from datetime import date, timedelta

import pandas as pd
from pytrends.request import TrendReq

from src import config
from src.cache.file_cache import FileCache

logger = logging.getLogger(__name__)


class GoogleTrendsIngestion:
    source_name = "google_trends"

    def __init__(self):
        self.cache = FileCache(self.source_name)
        self.pytrends = TrendReq(hl="en-GB", tz=0)

    def _build_batches(self) -> list[list[str]]:
        """Split all terms into batches of 5, each including the anchor term."""
        anchor = config.ANCHOR_TERM
        other_terms = [t for t in config.ALL_TERMS if t != anchor]
        batches = []
        batch_size = config.PYTRENDS_MAX_TERMS_PER_BATCH - 1  # reserve 1 slot for anchor

        for i in range(0, len(other_terms), batch_size):
            batch = [anchor] + other_terms[i:i + batch_size]
            batches.append(batch)

        return batches

    def _build_time_chunks(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Split date range into ~5-year chunks with 1-month overlap for stitching."""
        chunks = []
        chunk_start = start_date
        chunk_years = config.PYTRENDS_CHUNK_YEARS

        while chunk_start < end_date:
            # Use 1st of month to avoid leap-year day issues
            target_year = chunk_start.year + chunk_years
            target_month = chunk_start.month
            try:
                target_date = date(target_year, target_month, chunk_start.day)
            except ValueError:
                # e.g. Feb 29 + 5 years = Feb 28 in non-leap year
                target_date = date(target_year, target_month, 28)
            chunk_end = min(target_date, end_date)
            chunks.append((chunk_start, chunk_end))
            if chunk_end >= end_date:
                break
            # Next chunk starts 1 month before this chunk ends for overlap
            chunk_start = date(chunk_end.year, chunk_end.month, 1) - timedelta(days=1)
            chunk_start = date(chunk_start.year, chunk_start.month, 1)

        return chunks

    def _fetch_batch(self, terms: list[str], timeframe: str, force_refresh: bool = False) -> pd.DataFrame | None:
        """Fetch a single batch of terms for a timeframe."""
        cache_id = f"{'-'.join(sorted(terms))}_{timeframe}"
        cached = self.cache.get_parsed(cache_id, force_refresh=force_refresh)
        if cached is not None:
            return cached

        backoff = config.PYTRENDS_INITIAL_BACKOFF_SECONDS
        for attempt in range(config.PYTRENDS_MAX_RETRIES + 1):
            try:
                self.pytrends.build_payload(terms, cat=0, timeframe=timeframe, geo=config.PYTRENDS_GEO)
                df = self.pytrends.interest_over_time()
                if df.empty:
                    logger.warning("%s: Empty response for terms=%s timeframe=%s", self.source_name, terms, timeframe)
                    return None

                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])

                df = df.reset_index()
                self.cache.put_parsed(cache_id, df)
                time.sleep(config.PYTRENDS_DELAY_SECONDS)
                return df

            except Exception as exc:
                if attempt < config.PYTRENDS_MAX_RETRIES:
                    logger.warning(
                        "%s: Error fetching (attempt %d/%d), backoff %.0fs: %s",
                        self.source_name, attempt + 1, config.PYTRENDS_MAX_RETRIES, backoff, exc,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                logger.error("%s: Failed after %d retries: %s", self.source_name, config.PYTRENDS_MAX_RETRIES, exc)
                raise

    def _normalise_across_batches(self, batch_dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Normalise values across batches using the anchor term."""
        anchor = config.ANCHOR_TERM
        if not batch_dfs:
            return pd.DataFrame()

        # First batch is the reference
        result = batch_dfs[0].copy()

        for batch_df in batch_dfs[1:]:
            if anchor not in batch_df.columns or anchor not in result.columns:
                logger.warning("%s: Anchor term missing in batch, skipping normalisation", self.source_name)
                for col in batch_df.columns:
                    if col not in result.columns and col != "date":
                        result[col] = batch_df[col]
                continue

            # Merge on date to align anchor values
            merged = result[["date", anchor]].merge(
                batch_df[["date", anchor]],
                on="date",
                suffixes=("_ref", "_batch"),
            )

            ref_mean = merged[f"{anchor}_ref"].mean()
            batch_mean = merged[f"{anchor}_batch"].mean()

            if batch_mean < config.ANCHOR_MIN_VALUE:
                logger.warning(
                    "%s: Anchor term mean=%.1f (below %d) in batch, storing unnormalised",
                    self.source_name, batch_mean, config.ANCHOR_MIN_VALUE,
                )
                scale = 1.0
                is_normalised = False
            else:
                scale = ref_mean / batch_mean
                is_normalised = True

            for col in batch_df.columns:
                if col not in result.columns and col != "date":
                    scaled = batch_df.set_index("date")[col] * scale
                    temp = batch_df[["date"]].copy()
                    temp[col] = scaled.values
                    result = result.merge(temp, on="date", how="outer")

        return result

    def _stitch_time_chunks(self, chunk_dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Stitch temporal chunks using overlapping months."""
        if not chunk_dfs:
            return pd.DataFrame()
        if len(chunk_dfs) == 1:
            return chunk_dfs[0]

        result = chunk_dfs[0].copy()

        for chunk_df in chunk_dfs[1:]:
            overlap = result.merge(chunk_df, on="date", suffixes=("_prev", "_next"))
            if overlap.empty:
                result = pd.concat([result, chunk_df], ignore_index=True)
                continue

            anchor = config.ANCHOR_TERM
            prev_col = f"{anchor}_prev"
            next_col = f"{anchor}_next"

            if prev_col in overlap.columns and next_col in overlap.columns:
                prev_mean = overlap[prev_col].mean()
                next_mean = overlap[next_col].mean()

                if next_mean >= config.ANCHOR_MIN_VALUE:
                    scale = prev_mean / next_mean
                else:
                    scale = 1.0

                non_date_cols = [c for c in chunk_df.columns if c != "date"]
                scaled_chunk = chunk_df.copy()
                for col in non_date_cols:
                    scaled_chunk[col] = scaled_chunk[col] * scale

                # Remove overlap dates from new chunk, then append
                overlap_dates = set(overlap["date"])
                new_rows = scaled_chunk[~scaled_chunk["date"].isin(overlap_dates)]
                result = pd.concat([result, new_rows], ignore_index=True)
            else:
                new_dates = ~chunk_df["date"].isin(result["date"])
                result = pd.concat([result, chunk_df[new_dates]], ignore_index=True)

        return result.sort_values("date").reset_index(drop=True)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate Google Trends output using same rules as BaseIngestionModule."""
        required_columns = {"date", "source", "metric_name", "raw_value"}
        missing = required_columns - set(df.columns)
        if missing:
            logger.error("%s: Missing required columns: %s", self.source_name, missing)
            return pd.DataFrame(columns=list(required_columns))

        df["raw_value"] = pd.to_numeric(df["raw_value"], errors="coerce")

        null_count = df["raw_value"].isna().sum()
        total_count = len(df)
        if total_count > 0:
            null_pct = (null_count / total_count) * 100
            if null_pct >= config.NULL_THRESHOLD_PERCENT:
                logger.error(
                    "%s: %.1f%% of rows have null raw_value (%d/%d)",
                    self.source_name, null_pct, null_count, total_count,
                )
                return pd.DataFrame(columns=list(required_columns))
            if null_count > 0:
                logger.warning(
                    "%s: Dropped %d null rows (%.1f%%)",
                    self.source_name, null_count, null_pct,
                )
                df = df.dropna(subset=["raw_value"])

        dupes = df.duplicated(subset=["date", "metric_name"], keep="first")
        if dupes.sum() > 0:
            logger.warning("%s: Dropped %d duplicate rows", self.source_name, dupes.sum())
            df = df.drop_duplicates(subset=["date", "metric_name"], keep="first")

        return df.reset_index(drop=True)

    def _to_standard_format(self, wide_df: pd.DataFrame, is_anchor_normalised: bool = True) -> pd.DataFrame:
        """Convert wide DataFrame to standard long format."""
        if wide_df.empty:
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "granularity", "is_anchor_normalised"])

        non_date_cols = [c for c in wide_df.columns if c != "date"]
        rows = []
        for _, row in wide_df.iterrows():
            for col in non_date_cols:
                rows.append({
                    "date": row["date"],
                    "source": self.source_name,
                    "metric_name": col,
                    "raw_value": row[col],
                    "granularity": "monthly",
                    "is_anchor_normalised": is_anchor_normalised,
                })
        return pd.DataFrame(rows)

    def _aggregate_to_monthly(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate weekly/daily data to monthly means."""
        if df.empty:
            return df

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.to_period("M")

        non_date_cols = [c for c in df.columns if c not in ("date", "month")]
        monthly = df.groupby("month")[non_date_cols].mean().reset_index()
        monthly["date"] = monthly["month"].dt.to_timestamp()
        monthly = monthly.drop(columns=["month"])

        return monthly

    def backfill(self, start_date: date | None = None, end_date: date | None = None, force_refresh: bool = False) -> pd.DataFrame:
        start_date = start_date or config.BACKFILL_START_DATE
        end_date = end_date or date.today()

        batches = self._build_batches()
        time_chunks = self._build_time_chunks(start_date, end_date)

        logger.info(
            "%s: Backfill %s to %s — %d batches x %d chunks = %d requests",
            self.source_name, start_date, end_date,
            len(batches), len(time_chunks), len(batches) * len(time_chunks),
        )

        chunk_results = []

        for chunk_start, chunk_end in time_chunks:
            timeframe = f"{chunk_start.isoformat()} {chunk_end.isoformat()}"
            batch_dfs = []

            for batch_terms in batches:
                df = self._fetch_batch(batch_terms, timeframe, force_refresh=force_refresh)
                if df is not None:
                    batch_dfs.append(df)

            if batch_dfs:
                normalised = self._normalise_across_batches(batch_dfs)
                monthly = self._aggregate_to_monthly(normalised)
                chunk_results.append(monthly)

        if not chunk_results:
            logger.warning("%s: No data retrieved during backfill", self.source_name)
            return pd.DataFrame(columns=["date", "source", "metric_name", "raw_value", "granularity", "is_anchor_normalised"])

        stitched = self._stitch_time_chunks(chunk_results)
        result = self._to_standard_format(stitched)
        return self.validate(result)

    def get_latest(self, force_refresh: bool = False) -> pd.DataFrame:
        """Fetch the last 2 years of data.

        Uses a 2-year window so that:
        - YoY comparisons are valid (both years in the same query = same scale)
        - Fewer API calls than a full backfill (avoids rate limiting)
        - No cross-query stitching needed (Google Trends normalises 0-100 per query)
        """
        end_date = date.today()
        start_date = date(end_date.year - 2, end_date.month, 1)
        return self.backfill(start_date, end_date, force_refresh=force_refresh)
