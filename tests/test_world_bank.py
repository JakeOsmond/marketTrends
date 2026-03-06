"""Tests for src.ingestion.world_bank.WorldBankIngestion."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create a WorldBankIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.world_bank import WorldBankIngestion
        obj = WorldBankIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseValidJSON:
    def test_standard_api_response(self, ingestion):
        """Flat records list parses into the correct format."""
        records = [
            {
                "country": {"id": "GBR", "value": "United Kingdom"},
                "date": "2022",
                "value": 150000000,
            },
            {
                "country": {"id": "USA", "value": "United States"},
                "date": "2022",
                "value": 900000000,
            },
            {
                "country": {"id": "GBR", "value": "United Kingdom"},
                "date": "2021",
                "value": 80000000,
            },
        ]
        raw = json.dumps(records).encode("utf-8")
        result = ingestion.parse(raw)

        assert len(result) == 3
        assert set(result.columns) >= {"date", "source", "metric_name", "raw_value"}
        assert (result["source"] == "world_bank").all()

    def test_dates_are_year_end(self, ingestion):
        """Each record date should be stored as Dec 31 of that year."""
        records = [
            {
                "country": {"id": "GBR", "value": "UK"},
                "date": "2020",
                "value": 100000,
            },
        ]
        raw = json.dumps(records).encode("utf-8")
        result = ingestion.parse(raw)

        assert result["date"].iloc[0] == pd.Timestamp("2020-12-31")


class TestParseEmpty:
    def test_empty_bytes(self, ingestion):
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns

    def test_empty_records_list(self, ingestion):
        raw = json.dumps([]).encode("utf-8")
        result = ingestion.parse(raw)
        assert result.empty

    def test_null_value_records_skipped(self, ingestion):
        """Records with null values should be skipped."""
        records = [
            {
                "country": {"id": "GBR", "value": "UK"},
                "date": "2020",
                "value": None,
            },
        ]
        raw = json.dumps(records).encode("utf-8")
        result = ingestion.parse(raw)
        assert result.empty


class TestParseFiltersUKAndGlobal:
    def test_uk_gets_uk_metric_others_get_global(self, ingestion):
        """GBR records should get metric 'air_passengers_uk',
        all other country codes get 'air_passengers_global'."""
        records = [
            {
                "country": {"id": "GBR", "value": "United Kingdom"},
                "date": "2022",
                "value": 150000000,
            },
            {
                "country": {"id": "USA", "value": "United States"},
                "date": "2022",
                "value": 900000000,
            },
            {
                "country": {"id": "DEU", "value": "Germany"},
                "date": "2022",
                "value": 200000000,
            },
        ]
        raw = json.dumps(records).encode("utf-8")
        result = ingestion.parse(raw)

        uk_rows = result[result["metric_name"] == "air_passengers_uk"]
        global_rows = result[result["metric_name"] == "air_passengers_global"]

        assert len(uk_rows) == 1
        assert uk_rows["raw_value"].iloc[0] == 150000000.0

        assert len(global_rows) == 2
        assert set(global_rows["raw_value"]) == {900000000.0, 200000000.0}
