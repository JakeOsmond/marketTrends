"""Tests for src.ingestion.icao.ICaoIngestion."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create an ICaoIngestion with mocked cache and session."""
    with patch("src.ingestion.base.FileCache") as MockCache, \
         patch("src.ingestion.base.requests.Session"):
        MockCache.return_value = MagicMock(
            get_raw=MagicMock(return_value=None),
            get_parsed=MagicMock(return_value=None),
        )
        from src.ingestion.icao import ICaoIngestion
        obj = ICaoIngestion()
        obj.cache = MockCache.return_value
        yield obj


class TestParseEmpty:
    def test_empty_bytes_returns_empty_df_with_columns(self, ingestion):
        """Empty input returns empty df with correct standard columns."""
        result = ingestion.parse(b"")
        assert result.empty
        assert "date" in result.columns
        assert "source" in result.columns
        assert "metric_name" in result.columns
        assert "raw_value" in result.columns


class TestParseNoTables:
    def test_html_with_no_useful_tables(self, ingestion):
        """HTML page containing no parseable table data returns empty df."""
        html = b"""
        <html>
        <body>
            <h1>ICAO Air Transport Monitor</h1>
            <p>Some descriptive text with no tables.</p>
            <div>More content here.</div>
        </body>
        </html>
        """
        result = ingestion.parse(html)
        assert result.empty
        assert "date" in result.columns

    def test_html_with_table_but_no_dates(self, ingestion):
        """HTML with a table that contains no date-like cells returns empty df."""
        html = b"""
        <html><body>
        <table>
            <tr><th>Region</th><th>Status</th></tr>
            <tr><td>Europe</td><td>Active</td></tr>
            <tr><td>Asia</td><td>Active</td></tr>
        </table>
        </body></html>
        """
        result = ingestion.parse(html)
        assert result.empty
