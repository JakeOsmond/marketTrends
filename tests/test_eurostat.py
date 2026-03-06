"""Tests for src.ingestion.eurostat.EurostatIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create an EurostatIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.eurostat import EurostatIngestion
        obj = EurostatIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseEmpty:
    def test_empty_bytes_returns_empty_df(self, ingestion):
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns
        assert "data_coverage_end" in result.columns


class TestParseTSVWithUKData:
    def test_tsv_with_uk_rows_and_monthly_columns(self, ingestion):
        """TSV where the first column contains UK identifier and subsequent
        columns are monthly periods like '2020M01' should parse correctly."""
        # Eurostat TSV: tab-separated, first column has geo info, rest are period columns
        tsv_bytes = (
            b"freq,unit,tra_meas,geo\\TIME_PERIOD\t2020M01\t2020M02\t2020M03\n"
            b"M,PAS,PAS_CRD,UK\t10000\t12000\t15000\n"
            b"M,PAS,PAS_CRD,DE\t20000\t22000\t25000\n"
        )
        result = ingestion.parse(tsv_bytes)

        # Only UK rows should appear
        assert len(result) == 3
        assert (result["source"] == "eurostat").all()
        assert (result["metric_name"] == "uk_air_passengers_eurostat").all()
        assert result["raw_value"].tolist() == [10000.0, 12000.0, 15000.0]

        dates = result["date"].tolist()
        assert dates[0] == pd.Timestamp("2020-01-01")
        assert dates[1] == pd.Timestamp("2020-02-01")
        assert dates[2] == pd.Timestamp("2020-03-01")


class TestBrexitDetection:
    def test_old_last_date_sets_data_coverage_end(self, ingestion):
        """When the last date in the data is old (>6 months ago), the
        data_coverage_end column should be populated."""
        tsv_bytes = (
            b"freq,unit,tra_meas,geo\\TIME_PERIOD\t2019M06\t2019M12\n"
            b"M,PAS,PAS_CRD,UK\t5000\t6000\n"
        )
        result = ingestion.parse(tsv_bytes)

        assert len(result) == 2
        assert "data_coverage_end" in result.columns
        # Data ends at 2019-12 which is well over 6 months ago
        assert result["data_coverage_end"].iloc[0] != ""
        assert result["data_coverage_end"].iloc[0] == "2019-12"
