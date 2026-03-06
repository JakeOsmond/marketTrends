"""Tests for src.ingestion.eurocontrol.EurocontrolIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create an EurocontrolIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.eurocontrol import EurocontrolIngestion
        obj = EurocontrolIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseAggregatesDailyToMonthly:
    def test_daily_rows_grouped_by_month(self, ingestion):
        """Daily flight rows should be summed into monthly totals."""
        csv_bytes = (
            b"date,state,flights\n"
            b"2023-01-01,United Kingdom,100\n"
            b"2023-01-02,United Kingdom,150\n"
            b"2023-01-03,United Kingdom,200\n"
            b"2023-02-01,United Kingdom,300\n"
            b"2023-02-02,United Kingdom,400\n"
        )
        result = ingestion.parse(csv_bytes)

        assert len(result) == 2
        jan = result[result["date"] == pd.Timestamp("2023-01-01")]
        assert jan["raw_value"].iloc[0] == 450  # 100 + 150 + 200

        feb = result[result["date"] == pd.Timestamp("2023-02-01")]
        assert feb["raw_value"].iloc[0] == 700  # 300 + 400


class TestParseFiltersUK:
    def test_only_uk_rows_kept(self, ingestion):
        """When a state column is present, only UK rows should be retained."""
        csv_bytes = (
            b"date,state,flights\n"
            b"2023-01-01,United Kingdom,100\n"
            b"2023-01-01,France,500\n"
            b"2023-01-01,Germany,600\n"
            b"2023-02-01,United Kingdom,200\n"
            b"2023-02-01,France,700\n"
        )
        result = ingestion.parse(csv_bytes)

        # Should have 2 monthly entries for UK only
        assert len(result) == 2
        assert (result["source"] == "eurocontrol").all()
        assert (result["metric_name"] == "uk_departure_flights").all()
        # Jan UK = 100, Feb UK = 200
        assert result["raw_value"].tolist() == [100.0, 200.0]


class TestParseEmpty:
    def test_empty_bytes_returns_empty_df(self, ingestion):
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns
        assert "raw_value" in result.columns
