"""Tests for GoogleSheetsWriter (Apps Script webhook version)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests


@pytest.fixture
def writer():
    """Build a GoogleSheetsWriter with mocked webhook URL."""
    with patch("src.output.google_sheets.config") as mock_config:
        mock_config.APPS_SCRIPT_WEBHOOK_URL = "https://script.google.com/fake"
        mock_config.HTTP_TIMEOUT_SECONDS = 30
        mock_config.RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
        mock_config.SHEET_DATA_FRESHNESS = "Data Freshness"
        from src.output.google_sheets import GoogleSheetsWriter
        return GoogleSheetsWriter()


class TestGoogleSheetsWriter:

    # ------------------------------------------------------------------
    # 1. Constructor raises when URL is not set
    # ------------------------------------------------------------------
    def test_init_raises_without_url(self):
        with patch("src.output.google_sheets.config") as mock_config:
            mock_config.APPS_SCRIPT_WEBHOOK_URL = ""
            from src.output.google_sheets import GoogleSheetsWriter
            with pytest.raises(RuntimeError, match="APPS_SCRIPT_WEBHOOK_URL"):
                GoogleSheetsWriter()

    # ------------------------------------------------------------------
    # 2. _post retries on 429 then succeeds
    # ------------------------------------------------------------------
    def test_post_retry_on_429(self, writer):
        mock_resp_fail = MagicMock()
        mock_resp_fail.status_code = 429
        mock_resp_fail.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_resp_fail
        )

        mock_resp_ok = MagicMock()
        mock_resp_ok.raise_for_status.return_value = None
        mock_resp_ok.json.return_value = {"status": "ok"}

        with patch("src.output.google_sheets.requests.post", side_effect=[mock_resp_fail, mock_resp_ok]):
            with patch("src.output.google_sheets.time.sleep"):
                result = writer._post({"action": "test"})

        assert result == {"status": "ok"}

    # ------------------------------------------------------------------
    # 3. _post raises after max retries
    # ------------------------------------------------------------------
    def test_post_raises_after_max_retries(self, writer):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_resp
        )

        with patch("src.output.google_sheets.requests.post", return_value=mock_resp):
            with patch("src.output.google_sheets.time.sleep"):
                with pytest.raises(requests.exceptions.HTTPError):
                    writer._post({"action": "test"})

    # ------------------------------------------------------------------
    # 4. write_tab full write posts correct payload
    # ------------------------------------------------------------------
    def test_write_tab_full_write(self, writer):
        df = pd.DataFrame({"date": ["2025-01-01", "2025-01-02"], "value": [10, 20]})

        with patch.object(writer, "_post", return_value={"status": "written"}) as mock_post:
            writer.write_tab("TestTab", df, append_only=False)

        mock_post.assert_called_once_with({
            "action": "write",
            "tab": "TestTab",
            "headers": ["date", "value"],
            "rows": [["2025-01-01", "10"], ["2025-01-02", "20"]],
        })

    # ------------------------------------------------------------------
    # 5. write_tab append filters by watermark
    # ------------------------------------------------------------------
    def test_write_tab_append_only(self, writer):
        df = pd.DataFrame({
            "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "value": [10, 20, 30],
        })

        def mock_post(payload):
            if payload["action"] == "get_watermark":
                return {"watermark": "2025-01-02"}
            return {"status": "appended"}

        with patch.object(writer, "_post", side_effect=mock_post) as mp:
            writer.write_tab("TestTab", df, append_only=True)

        # Should have made 2 calls: get_watermark + append
        assert mp.call_count == 2
        append_call = mp.call_args_list[1]
        assert append_call[0][0]["action"] == "append"
        assert append_call[0][0]["rows"] == [["2025-01-03", "30"]]

    # ------------------------------------------------------------------
    # 6. write_tab with empty DataFrame is a no-op
    # ------------------------------------------------------------------
    def test_write_tab_empty_df(self, writer):
        with patch.object(writer, "_post") as mock_post:
            writer.write_tab("EmptyTab", pd.DataFrame())

        mock_post.assert_not_called()

    # ------------------------------------------------------------------
    # 7. write_tab append skips when watermark parse fails
    # ------------------------------------------------------------------
    def test_write_tab_append_bad_watermark(self, writer):
        df = pd.DataFrame({"date": ["2025-01-01"], "value": [10]})

        def mock_post(payload):
            if payload["action"] == "get_watermark":
                return {"watermark": "not-a-date"}
            return {"status": "appended"}

        with patch.object(writer, "_post", side_effect=mock_post) as mp:
            writer.write_tab("TestTab", df, append_only=True)

        # Only get_watermark should be called — append skipped due to bad watermark
        assert mp.call_count == 1

    # ------------------------------------------------------------------
    # 8. write_data_freshness posts correct payload
    # ------------------------------------------------------------------
    def test_write_data_freshness(self, writer):
        source_status = {
            "SourceA": {
                "last_updated": "2025-03-01",
                "latest_data_point": "2025-02-28",
                "data_coverage_end": "2025-02-28",
                "status": "ok",
            },
            "SourceB": {
                "last_updated": "2025-03-02",
                "latest_data_point": "2025-03-01",
                "data_coverage_end": "2025-03-01",
                "status": "stale",
            },
        }

        with patch.object(writer, "_post", return_value={"status": "written"}) as mock_post:
            writer.write_data_freshness(source_status)

        mock_post.assert_called_once()
        payload = mock_post.call_args[0][0]
        assert payload["action"] == "write"
        assert payload["tab"] == "Data Freshness"
        assert payload["headers"] == ["Source", "Last Updated", "Latest Data Point", "Data Coverage End", "Status"]
        assert len(payload["rows"]) == 2
        assert payload["rows"][0][0] == "SourceA"
        assert payload["rows"][1][0] == "SourceB"
