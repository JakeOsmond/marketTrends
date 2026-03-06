"""Tests for src.ingestion.caa.CAAIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create a CAAIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.caa import CAAIngestion
        obj = CAAIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseValidCSV:
    def test_parse_valid_csv(self, ingestion):
        """CSV with date and total_passengers columns should parse correctly."""
        csv_bytes = (
            b"date,total_passengers,airport\n"
            b"2023-01-01,500000,Heathrow\n"
            b"2023-02-01,600000,Heathrow\n"
            b"2023-03-01,700000,Gatwick\n"
        )
        result = ingestion.parse(csv_bytes)

        assert len(result) == 3
        assert set(result.columns) >= {"date", "source", "metric_name", "raw_value"}
        assert (result["source"] == "caa").all()
        assert (result["metric_name"] == "uk_international_passengers").all()
        assert result["raw_value"].tolist() == [500000.0, 600000.0, 700000.0]

    def test_dates_parsed_as_datetime(self, ingestion):
        csv_bytes = (
            b"date,total_passengers\n"
            b"2023-06-15,123456\n"
        )
        result = ingestion.parse(csv_bytes)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])


class TestParseEmpty:
    def test_parse_empty_bytes(self, ingestion):
        """Empty bytes input should return an empty DataFrame."""
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns
        assert "raw_value" in result.columns


class TestParseMissingColumns:
    def test_unidentifiable_columns_returns_empty(self, ingestion):
        """CSV whose columns cannot be matched to date/passenger returns empty df."""
        csv_bytes = (
            b"foo,bar,baz\n"
            b"1,2,3\n"
            b"4,5,6\n"
        )
        result = ingestion.parse(csv_bytes)
        assert result.empty
        assert "date" in result.columns
