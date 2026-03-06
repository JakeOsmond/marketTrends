"""Tests for src.ingestion.fca.FCAIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create an FCAIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.fca import FCAIngestion
        obj = FCAIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseEmpty:
    def test_empty_bytes_returns_empty_df(self, ingestion):
        """Empty input should return an empty DataFrame with standard columns."""
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns
        assert "raw_value" in result.columns
        assert "source" in result.columns
        assert "metric_name" in result.columns

    def test_invalid_bytes_returns_empty_df(self, ingestion):
        """Non-Excel, non-CSV garbage bytes should not raise but return empty."""
        result = ingestion.parse(b"\x00\x01\x02not-valid-data")
        assert result.empty
