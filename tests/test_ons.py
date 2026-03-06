"""Tests for src.ingestion.ons.ONSIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create an ONSIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.ons import ONSIngestion
        obj = ONSIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseEmpty:
    def test_empty_bytes_returns_empty_df_with_granularity(self, ingestion):
        """Empty input should return empty df that includes a granularity column."""
        result = ingestion.parse(b"")
        assert result.empty
        assert "granularity" in result.columns
        assert "date" in result.columns


class TestParseCSVFormat:
    def test_csv_with_period_and_visits(self, ingestion):
        """CSV with period and visits columns parses correctly with quarterly granularity."""
        csv_bytes = (
            b"period,visits\n"
            b"2022-03-31,1500\n"
            b"2022-06-30,2000\n"
            b"2022-09-30,2500\n"
            b"2022-12-31,1800\n"
        )
        result = ingestion.parse(csv_bytes)

        assert len(result) == 4
        assert "granularity" in result.columns
        assert (result["granularity"] == "quarterly").all()
        assert (result["source"] == "ons").all()
        assert (result["metric_name"] == "uk_overseas_visits").all()
        assert result["raw_value"].tolist() == [1500.0, 2000.0, 2500.0, 1800.0]
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
