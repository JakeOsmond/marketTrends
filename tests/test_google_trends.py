"""Tests for src.ingestion.google_trends.GoogleTrendsIngestion."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def ingestion():
    """Create a GoogleTrendsIngestion with mocked pytrends and cache."""
    with patch("src.ingestion.google_trends.TrendReq") as MockTrendReq, \
         patch("src.ingestion.google_trends.FileCache") as MockCache:
        mock_pytrends = MagicMock()
        MockTrendReq.return_value = mock_pytrends
        MockCache.return_value = MagicMock(get_parsed=MagicMock(return_value=None))

        from src.ingestion.google_trends import GoogleTrendsIngestion
        obj = GoogleTrendsIngestion()
        obj.pytrends = mock_pytrends
        yield obj


class TestBuildBatches:
    def test_anchor_in_every_batch(self, ingestion):
        """The anchor term must appear in every batch."""
        from src import config
        batches = ingestion._build_batches()

        for batch in batches:
            assert config.ANCHOR_TERM in batch

    def test_max_five_terms_per_batch(self, ingestion):
        """No batch may exceed PYTRENDS_MAX_TERMS_PER_BATCH (5) terms."""
        from src import config
        batches = ingestion._build_batches()

        for batch in batches:
            assert len(batch) <= config.PYTRENDS_MAX_TERMS_PER_BATCH

    def test_all_terms_covered(self, ingestion):
        """Every term in ALL_TERMS must appear in at least one batch."""
        from src import config
        batches = ingestion._build_batches()
        all_in_batches = set()
        for batch in batches:
            all_in_batches.update(batch)

        for term in config.ALL_TERMS:
            assert term in all_in_batches


class TestBuildTimeChunks:
    def test_chunks_cover_full_range(self, ingestion):
        start = date(2015, 1, 1)
        end = date(2025, 6, 1)
        chunks = ingestion._build_time_chunks(start, end)

        assert chunks[0][0] == start
        assert chunks[-1][1] == end

    def test_roughly_five_year_windows(self, ingestion):
        start = date(2010, 1, 1)
        end = date(2025, 1, 1)
        chunks = ingestion._build_time_chunks(start, end)

        for chunk_start, chunk_end in chunks:
            span_years = (chunk_end - chunk_start).days / 365.25
            # Each chunk should be at most ~5 years (plus small tolerance)
            assert span_years <= 6

    def test_overlap_between_chunks(self, ingestion):
        start = date(2010, 1, 1)
        end = date(2025, 1, 1)
        chunks = ingestion._build_time_chunks(start, end)

        if len(chunks) > 1:
            for i in range(len(chunks) - 1):
                # Next chunk starts before the previous chunk ends (overlap)
                assert chunks[i + 1][0] < chunks[i][1]


class TestAggregateToMonthly:
    def test_weekly_data_becomes_monthly_means(self, ingestion):
        """Weekly rows within the same month should be averaged."""
        weekly = pd.DataFrame({
            "date": pd.to_datetime([
                "2023-01-01", "2023-01-08", "2023-01-15", "2023-01-22",
                "2023-02-05", "2023-02-12",
            ]),
            "term_a": [10, 20, 30, 40, 50, 60],
            "term_b": [100, 200, 300, 400, 500, 600],
        })

        result = ingestion._aggregate_to_monthly(weekly)

        assert len(result) == 2
        jan = result[result["date"] == pd.Timestamp("2023-01-01")]
        assert jan["term_a"].iloc[0] == pytest.approx(25.0)  # mean(10,20,30,40)
        assert jan["term_b"].iloc[0] == pytest.approx(250.0)

        feb = result[result["date"] == pd.Timestamp("2023-02-01")]
        assert feb["term_a"].iloc[0] == pytest.approx(55.0)

    def test_empty_df_returns_empty(self, ingestion):
        empty = pd.DataFrame(columns=["date", "term_a"])
        result = ingestion._aggregate_to_monthly(empty)
        assert result.empty


class TestToStandardFormat:
    def test_wide_to_long(self, ingestion):
        """Wide df with date + N term columns becomes long with source/metric_name."""
        wide = pd.DataFrame({
            "date": pd.to_datetime(["2023-01-01", "2023-02-01"]),
            "travel insurance": [50, 60],
            "cheap flights": [70, 80],
        })

        result = ingestion._to_standard_format(wide)

        assert "source" in result.columns
        assert "metric_name" in result.columns
        assert "raw_value" in result.columns
        assert (result["source"] == "google_trends").all()
        assert set(result["metric_name"]) == {"travel insurance", "cheap flights"}
        assert len(result) == 4  # 2 dates x 2 metrics

    def test_empty_wide_returns_empty_long(self, ingestion):
        empty = pd.DataFrame(columns=["date", "term_a"])
        result = ingestion._to_standard_format(empty)
        assert result.empty
        assert "metric_name" in result.columns


class TestNormaliseAcrossBatches:
    def test_scaling_applied_via_anchor(self, ingestion):
        """Second batch values should be scaled by anchor ratio from first batch."""
        from src import config
        anchor = config.ANCHOR_TERM

        batch1 = pd.DataFrame({
            "date": pd.to_datetime(["2023-01-01", "2023-02-01"]),
            anchor: [100, 100],
            "term_a": [50, 60],
        })
        batch2 = pd.DataFrame({
            "date": pd.to_datetime(["2023-01-01", "2023-02-01"]),
            anchor: [50, 50],
            "term_b": [30, 40],
        })

        result = ingestion._normalise_across_batches([batch1, batch2])

        # anchor mean in batch1 = 100, in batch2 = 50, so scale = 2.0
        assert "term_b" in result.columns
        assert result["term_b"].iloc[0] == pytest.approx(60.0)  # 30 * 2
        assert result["term_b"].iloc[1] == pytest.approx(80.0)  # 40 * 2

    def test_empty_input(self, ingestion):
        result = ingestion._normalise_across_batches([])
        assert result.empty
