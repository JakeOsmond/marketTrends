import logging
import time

import pandas as pd
import requests

from src import config

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2


class GoogleSheetsWriter:
    def __init__(self):
        self.webhook_url = config.APPS_SCRIPT_WEBHOOK_URL
        if not self.webhook_url:
            raise RuntimeError(
                "APPS_SCRIPT_WEBHOOK_URL is not set. Deploy the Apps Script "
                "web app and add the URL to your .env file."
            )

    def _post(self, payload: dict) -> dict:
        """POST JSON to the Apps Script webhook with retry logic.

        Google Apps Script redirects POST /exec to a one-time URL via 302.
        requests converts POST->GET on redirect, losing the body.
        We disable auto-redirect and re-POST to the redirect URL.
        """
        import json as _json

        backoff = INITIAL_BACKOFF_SECONDS
        data_bytes = _json.dumps(payload).encode("utf-8")

        for attempt in range(MAX_RETRIES + 1):
            try:
                # Apps Script POST flow:
                # 1. POST to /exec -> 302 redirect
                # 2. GET the redirect URL to retrieve the JSON response
                resp = requests.post(
                    self.webhook_url,
                    data=data_bytes,
                    timeout=config.HTTP_TIMEOUT_SECONDS,
                    headers={"Content-Type": "application/json"},
                    allow_redirects=False,
                )

                # Follow redirect with GET (Apps Script returns response via GET)
                if resp.status_code in (301, 302, 303, 307, 308):
                    redirect_url = resp.headers.get("Location")
                    if redirect_url:
                        resp = requests.get(
                            redirect_url,
                            timeout=config.HTTP_TIMEOUT_SECONDS,
                            allow_redirects=True,
                        )

                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else 0
                if status_code in config.RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                    logger.warning(
                        "Webhook %d error, retry %d/%d in %.0fs",
                        status_code, attempt + 1, MAX_RETRIES, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
            except requests.exceptions.RequestException:
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "Webhook request failed, retry %d/%d in %.0fs",
                        attempt + 1, MAX_RETRIES, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise

    def _get_watermark(self, tab_name: str) -> str | None:
        """Get the last value in column A of a tab."""
        result = self._post({"action": "get_watermark", "tab": tab_name})
        return result.get("watermark")

    def write_tab(self, tab_name: str, df: pd.DataFrame, append_only: bool = False):
        """Write data to a named tab via the Apps Script webhook."""
        if df.empty:
            logger.info("No data to write for tab '%s'", tab_name)
            return

        headers = list(df.columns)
        rows = df.astype(str).values.tolist()

        if append_only:
            watermark = self._get_watermark(tab_name)
            if watermark and "date" in df.columns:
                try:
                    watermark_date = pd.to_datetime(watermark)
                    df = df[pd.to_datetime(df["date"]) > watermark_date]
                except Exception as exc:
                    logger.warning(
                        "Could not parse watermark '%s' for tab '%s': %s. "
                        "Skipping append to prevent duplication.",
                        watermark, tab_name, exc,
                    )
                    return

            if df.empty:
                logger.info("No new data to append for tab '%s'", tab_name)
                return

            rows = df.astype(str).values.tolist()

            # Send in batches
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                self._post({
                    "action": "append",
                    "tab": tab_name,
                    "headers": headers,
                    "rows": batch,
                })
                if i + BATCH_SIZE < len(rows):
                    time.sleep(1)

            logger.info("Appended %d rows to '%s'", len(rows), tab_name)
        else:
            # Full write in batches
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                if i == 0:
                    # First batch: full write (clears sheet, writes headers + data)
                    self._post({
                        "action": "write",
                        "tab": tab_name,
                        "headers": headers,
                        "rows": batch,
                    })
                else:
                    # Subsequent batches: append
                    self._post({
                        "action": "append",
                        "tab": tab_name,
                        "headers": headers,
                        "rows": batch,
                    })
                if i + BATCH_SIZE < len(rows):
                    time.sleep(1)

            logger.info("Wrote %d rows to '%s'", len(rows), tab_name)

    def write_data_freshness(self, source_status: dict):
        """Update the Data Freshness tab with source status info."""
        headers = ["Source", "Last Updated", "Latest Data Point", "Data Coverage End", "Status"]
        rows = []

        for source_name, status in source_status.items():
            rows.append([
                source_name,
                status.get("last_updated", ""),
                status.get("latest_data_point", ""),
                status.get("data_coverage_end", ""),
                status.get("status", "ok"),
            ])

        self._post({
            "action": "write",
            "tab": config.SHEET_DATA_FRESHNESS,
            "headers": headers,
            "rows": rows,
        })
