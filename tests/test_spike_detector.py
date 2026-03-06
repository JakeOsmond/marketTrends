"""Tests for the SpikeDetector class."""

from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

# Patch config values before importing SpikeDetector so __init__ reads our values.
CONFIG_PATCH = {
    "src.config.SPIKE_THRESHOLD_STD_DEVS": 2,
    "src.config.SPIKE_MIN_YEARS_FOR_CONFIDENCE": 3,
    "src.config.COVID_START": date(2020, 3, 1),
    "src.config.COVID_END": date(2021, 6, 30),
    "src.config.KNOWN_EVENTS": {
        (date(2019, 9, 23), date(2019, 10, 31)): "Thomas Cook collapse",
        (date(2020, 3, 1), date(2021, 6, 30)): "COVID-19 structural break",
    },
}


def _make_detector():
    """Create a SpikeDetector with controlled config values."""
    with patch("src.config.SPIKE_THRESHOLD_STD_DEVS", 2), \
         patch("src.config.SPIKE_MIN_YEARS_FOR_CONFIDENCE", 3), \
         patch("src.config.COVID_START", date(2020, 3, 1)), \
         patch("src.config.COVID_END", date(2021, 6, 30)), \
         patch("src.config.KNOWN_EVENTS", CONFIG_PATCH["src.config.KNOWN_EVENTS"]):
        from src.normalisation.spike_detector import SpikeDetector
        return SpikeDetector()


def _build_df(rows):
    """Build a DataFrame from a list of (date_str, raw_value) tuples."""
    return pd.DataFrame(rows, columns=["date", "raw_value"])


# --------------------------------------------------------------------------- #
# 1. Empty DataFrame
# --------------------------------------------------------------------------- #

class TestEmptyDataFrame:
    def test_empty_dataframe(self):
        detector = _make_detector()
        df = pd.DataFrame(columns=["date", "raw_value"])
        result = detector.detect_and_normalise(df)

        assert result.empty
        expected_cols = {"date", "raw_value", "normalised_value", "is_spike",
                         "spike_event", "is_normalised", "low_confidence"}
        assert expected_cols.issubset(set(result.columns))


# --------------------------------------------------------------------------- #
# 2. No spikes - normal data within 2 std devs
# --------------------------------------------------------------------------- #

class TestNoSpikes:
    def test_no_spikes(self):
        """Values that sit close to the same-month mean should not be flagged."""
        detector = _make_detector()
        # 5 years of January data with natural variance — all within 2 std devs of each other
        rows = [
            ("2016-01-15", 90),
            ("2017-01-15", 105),
            ("2018-01-15", 100),
            ("2019-01-15", 95),
            ("2022-01-15", 110),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        assert not result["is_spike"].any()
        assert not result["is_normalised"].any()
        assert (result["spike_event"] == "").all()


# --------------------------------------------------------------------------- #
# 3. Spike detected - value far from same-month mean
# --------------------------------------------------------------------------- #

class TestSpikeDetected:
    def test_spike_detected(self):
        """A value far beyond the mean +/- 2*std should be flagged as a spike."""
        detector = _make_detector()
        # Several years of March data with some natural variance, then an extreme outlier
        rows = [
            ("2016-03-15", 98),
            ("2017-03-15", 102),
            ("2018-03-15", 100),
            ("2019-03-15", 104),
            # Skip COVID period for March
            ("2022-03-15", 96),
            ("2023-03-15", 100),
            ("2024-03-15", 500),  # extreme spike
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        spike_row = result[result["date"] == pd.Timestamp("2024-03-15")].iloc[0]
        assert spike_row["is_spike"] is True or spike_row["is_spike"] == True
        assert spike_row["is_normalised"] is True or spike_row["is_normalised"] == True
        assert spike_row["spike_event"] == "statistical anomaly"
        # Leave-one-out: normalised value is the mean of OTHER March years
        expected_mean = (98 + 102 + 100 + 104 + 96 + 100) / 6  # = 100.0
        assert spike_row["normalised_value"] == pytest.approx(expected_mean, abs=1)


# --------------------------------------------------------------------------- #
# 4. COVID period normalised
# --------------------------------------------------------------------------- #

class TestCovidPeriodNormalised:
    def test_covid_period_normalised(self):
        """Dates in the COVID window should be normalised to pre-COVID same-month mean."""
        detector = _make_detector()
        rows = [
            # Pre-COVID April data
            ("2016-04-15", 80),
            ("2017-04-15", 90),
            ("2018-04-15", 100),
            ("2019-04-15", 110),
            # COVID April - should be normalised
            ("2020-04-15", 10),
            # Post-COVID
            ("2022-04-15", 95),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        covid_row = result[result["date"] == pd.Timestamp("2020-04-15")].iloc[0]
        assert covid_row["is_spike"] is True or covid_row["is_spike"] == True
        assert covid_row["is_normalised"] is True or covid_row["is_normalised"] == True
        assert covid_row["spike_event"] == "COVID-19 structural break"
        # Pre-COVID April mean = (80 + 90 + 100 + 110) / 4 = 95
        assert covid_row["normalised_value"] == pytest.approx(95.0)


# --------------------------------------------------------------------------- #
# 5. Known event flagged
# --------------------------------------------------------------------------- #

class TestKnownEventFlagged:
    def test_known_event_flagged(self):
        """Dates within a known event range should be flagged with the event name."""
        detector = _make_detector()
        rows = [
            # Stable October data across years
            ("2016-10-01", 100),
            ("2017-10-01", 100),
            ("2018-10-01", 100),
            # Thomas Cook collapse window: 2019-09-23 to 2019-10-31
            ("2019-10-01", 105),
            ("2022-10-01", 100),
            ("2023-10-01", 100),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        event_row = result[result["date"] == pd.Timestamp("2019-10-01")].iloc[0]
        assert event_row["is_spike"] is True or event_row["is_spike"] == True
        assert event_row["spike_event"] == "Thomas Cook collapse"
        assert event_row["is_normalised"] is True or event_row["is_normalised"] == True


# --------------------------------------------------------------------------- #
# 6. Low confidence flag
# --------------------------------------------------------------------------- #

class TestLowConfidenceFlag:
    def test_low_confidence_flag(self):
        """Fewer than 3 years of non-COVID data for a month triggers low_confidence."""
        detector = _make_detector()
        # Only 2 years of February data (below min_years=3)
        rows = [
            ("2022-02-15", 100),
            ("2023-02-15", 105),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        assert result["low_confidence"].all()

    def test_sufficient_data_not_low_confidence(self):
        """With enough years of data (>= min_years others after leave-one-out), low_confidence is False."""
        detector = _make_detector()
        # 5 rows: each row has 4 others to compare against, well above min_years=3
        rows = [
            ("2016-06-15", 100),
            ("2017-06-15", 102),
            ("2018-06-15", 101),
            ("2019-06-15", 99),
            ("2022-06-15", 100),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        assert not result["low_confidence"].any()


# --------------------------------------------------------------------------- #
# 7. COVID excluded from stats
# --------------------------------------------------------------------------- #

class TestCovidExcludedFromStats:
    def test_covid_excluded_from_stats(self):
        """COVID-period data should not affect the mean/std used for non-COVID rows."""
        detector = _make_detector()
        # Stable May data pre- and post-COVID, with a wild COVID May value
        rows = [
            ("2016-05-15", 100),
            ("2017-05-15", 100),
            ("2018-05-15", 100),
            ("2019-05-15", 100),
            # COVID May - extreme value that would skew stats if included
            ("2020-05-15", 5),
            ("2021-05-15", 10),
            # Post-COVID normal value
            ("2022-05-15", 101),
        ]
        df = _build_df(rows)
        result = detector.detect_and_normalise(df)

        # The post-COVID row should NOT be a spike; if COVID data polluted the
        # stats the std would be huge or the mean shifted, but 101 is near 100.
        post_covid_row = result[result["date"] == pd.Timestamp("2022-05-15")].iloc[0]
        assert post_covid_row["is_spike"] is False or post_covid_row["is_spike"] == False
        assert post_covid_row["normalised_value"] == pytest.approx(101.0)
        assert post_covid_row["is_normalised"] is False or post_covid_row["is_normalised"] == False
