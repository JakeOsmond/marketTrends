import logging
from datetime import date

import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


class SpikeDetector:
    def __init__(self):
        self.threshold = config.SPIKE_THRESHOLD_STD_DEVS
        self.min_years = config.SPIKE_MIN_YEARS_FOR_CONFIDENCE
        self.covid_start = pd.Timestamp(config.COVID_START)
        self.covid_end = pd.Timestamp(config.COVID_END)
        self.known_events = config.KNOWN_EVENTS

    def _is_covid_period(self, dt: pd.Timestamp) -> bool:
        return self.covid_start <= dt <= self.covid_end

    def _find_known_event(self, dt: pd.Timestamp) -> str:
        dt_date = dt.date() if hasattr(dt, "date") else dt
        for (start, end), event_name in self.known_events.items():
            if start <= dt_date <= end:
                return event_name
        return ""

    def detect_and_normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply year-over-year same-month spike detection and normalisation.

        Input DataFrame must have columns: date, raw_value (and optionally others).
        Adds columns: normalised_value, is_spike, spike_event, is_normalised, low_confidence.
        """
        if df.empty:
            for col in ["normalised_value", "is_spike", "spike_event", "is_normalised", "low_confidence"]:
                df[col] = pd.Series(dtype="object")
            return df

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["_month"] = df["date"].dt.month
        df["_year"] = df["date"].dt.year

        # Build same-month historical pools excluding COVID, indexed by (month, year)
        non_covid = df[~df["date"].between(self.covid_start, self.covid_end)]

        # Group non-COVID values by month for leave-one-out evaluation
        month_pools = {}
        for month in range(1, 13):
            month_rows = non_covid[non_covid["_month"] == month][["_year", "raw_value"]].dropna(subset=["raw_value"])
            month_pools[month] = dict(zip(month_rows["_year"], month_rows["raw_value"]))

        # Pre-COVID only stats for COVID normalisation
        pre_covid = df[df["date"] < self.covid_start]
        pre_covid_month_means = {}
        for month in range(1, 13):
            month_data = pre_covid[pre_covid["_month"] == month]["raw_value"]
            if month_data.notna().any():
                pre_covid_month_means[month] = month_data.mean()

        # Apply spike detection row by row
        normalised_values = []
        is_spike_flags = []
        spike_events = []
        is_normalised_flags = []
        low_confidence_flags = []

        for _, row in df.iterrows():
            dt = row["date"]
            raw = row["raw_value"]
            month = row["_month"]
            year = row["_year"]

            # COVID period: always flag, use pre-COVID same-month mean
            if self._is_covid_period(dt):
                pre_covid_mean = pre_covid_month_means.get(month, raw)
                normalised_values.append(pre_covid_mean)
                is_spike_flags.append(True)
                spike_events.append("COVID-19 structural break")
                is_normalised_flags.append(True)
                low_confidence_flags.append(False)
                continue

            # Leave-one-out: exclude current row from same-month stats
            pool = month_pools.get(month, {})
            others = [v for y, v in pool.items() if y != year]
            n_years = len(others)

            if n_years >= 2:
                mean = np.mean(others)
                std = np.std(others, ddof=1)
            elif n_years == 1:
                mean = others[0]
                std = 0
            else:
                mean = np.nan
                std = 0

            low_conf = n_years < self.min_years

            # Check for known event
            event = self._find_known_event(dt)

            # Spike detection: value > mean + threshold * std
            if pd.notna(mean) and std > 0 and pd.notna(raw):
                deviation = abs(raw - mean) / std
                is_spike = deviation > self.threshold
            else:
                is_spike = False

            if is_spike or event:
                normalised_values.append(mean if pd.notna(mean) else raw)
                is_spike_flags.append(True)
                spike_events.append(event if event else "statistical anomaly")
                is_normalised_flags.append(True)
            else:
                normalised_values.append(raw)
                is_spike_flags.append(False)
                spike_events.append("")
                is_normalised_flags.append(False)

            low_confidence_flags.append(low_conf)

        df["normalised_value"] = normalised_values
        df["is_spike"] = is_spike_flags
        df["spike_event"] = spike_events
        df["is_normalised"] = is_normalised_flags
        df["low_confidence"] = low_confidence_flags

        df = df.drop(columns=["_month", "_year"])

        spike_count = df["is_spike"].sum()
        covid_count = (df["spike_event"] == "COVID-19 structural break").sum()
        logger.info(
            "Spike detection: %d spikes found (%d COVID) out of %d data points",
            spike_count, covid_count, len(df),
        )

        return df
