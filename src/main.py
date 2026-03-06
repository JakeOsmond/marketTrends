import argparse
import fcntl
import logging
import os
import sys
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from src import config
from src.ingestion.google_trends import GoogleTrendsIngestion
from src.ingestion.caa import CAAIngestion
from src.ingestion.ons import ONSIngestion
from src.ingestion.fca import FCAIngestion
from src.ingestion.eurocontrol import EurocontrolIngestion
from src.ingestion.eurostat import EurostatIngestion
from src.ingestion.icao import ICaoIngestion
from src.ingestion.world_bank import WorldBankIngestion
from src.normalisation.spike_detector import SpikeDetector
from src.output.google_sheets import GoogleSheetsWriter
from src.output.market_summary import build_market_summary

logger = logging.getLogger("src")

# Source registry: name -> (class, sheet_tab, is_best_effort)
SOURCE_REGISTRY = {
    "google_trends": (GoogleTrendsIngestion, None, False),  # written to two tabs
    "caa": (CAAIngestion, config.SHEET_UK_PASSENGERS, False),
    "ons": (ONSIngestion, config.SHEET_ONS_TRAVEL, False),
    "fca": (FCAIngestion, config.SHEET_INSURANCE_MARKET, True),
    "eurocontrol": (EurocontrolIngestion, config.SHEET_UK_PASSENGERS, True),
    "eurostat": (EurostatIngestion, config.SHEET_UK_PASSENGERS, False),
    "icao": (ICaoIngestion, config.SHEET_GLOBAL_AVIATION, True),
    "world_bank": (WorldBankIngestion, config.SHEET_GLOBAL_AVIATION, False),
}

# Google Trends metrics go to specific tabs
TRENDS_TAB_MAPPING = {
    "holiday_intent": config.SHEET_HOLIDAY_INTENT,
    "insurance_intent": config.SHEET_INSURANCE_INTENT,
}


def setup_logging(verbose: bool = False):
    log_dir = Path(config.LOG_FILE).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger("src")
    root.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s", datefmt="%H:%M:%S"))
    root.addHandler(console)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        config.LOG_FILE, maxBytes=config.LOG_MAX_BYTES, backupCount=config.LOG_BACKUP_COUNT,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s"))
    root.addHandler(file_handler)


def acquire_lock() -> int:
    lock_dir = Path(config.LOCK_FILE).parent
    lock_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(config.LOCK_FILE, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        logger.error("Another pipeline instance is already running (lock file: %s)", config.LOCK_FILE)
        sys.exit(1)
    return fd


def release_lock(fd: int):
    # Do not delete the lock file — just unlock and close.
    # Deleting after unlock creates a race where another process can lock
    # the old inode while a third creates a new file at the same path.
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def _classify_trends_metric(metric_name: str) -> str:
    """Determine which tab a Google Trends metric belongs to."""
    holiday_terms = {t.replace(" ", "_") for t in config.HOLIDAY_INTENT_TERMS}
    # Normalise metric name for comparison
    if metric_name.replace(" ", "_") in holiday_terms or metric_name in config.HOLIDAY_INTENT_TERMS:
        return "holiday_intent"
    return "insurance_intent"


def run_source(source_name: str, backfill: bool, force_refresh: bool, spike_detector: SpikeDetector) -> tuple[pd.DataFrame, dict]:
    """Run a single source. Returns (processed_df, status_dict)."""
    cls, tab, is_best_effort = SOURCE_REGISTRY[source_name]

    status = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "latest_data_point": "",
        "data_coverage_end": "",
        "status": "ok",
    }

    try:
        instance = cls()
        if backfill:
            df = instance.backfill(force_refresh=force_refresh)
        else:
            df = instance.get_latest(force_refresh=force_refresh)

        if df.empty:
            status["status"] = "no_data"
            return df, status

        # Record latest data point
        if "date" in df.columns:
            latest = df["date"].max()
            status["latest_data_point"] = latest.strftime("%Y-%m-%d") if pd.notna(latest) else ""

        # Record data coverage end if present
        if "data_coverage_end" in df.columns:
            coverage_vals = df["data_coverage_end"].dropna().unique()
            non_empty = [v for v in coverage_vals if v]
            if non_empty:
                status["data_coverage_end"] = non_empty[0]

        # Apply spike detection per metric
        processed_parts = []
        for metric_name, metric_df in df.groupby("metric_name"):
            detected = spike_detector.detect_and_normalise(metric_df)
            processed_parts.append(detected)

        processed = pd.concat(processed_parts, ignore_index=True) if processed_parts else df

        return processed, status

    except Exception as exc:
        if is_best_effort:
            logger.warning("Best-effort source '%s' failed (non-fatal): %s", source_name, exc)
            status["status"] = f"failed (best-effort): {exc}"
            return pd.DataFrame(), status
        else:
            logger.error("Source '%s' failed: %s", source_name, exc, exc_info=True)
            status["status"] = f"failed: {exc}"
            return pd.DataFrame(), status


def write_to_sheets(source_name: str, df: pd.DataFrame, writer: GoogleSheetsWriter, backfill: bool):
    """Write processed data to the appropriate Google Sheets tab(s)."""
    if df.empty:
        return

    append_only = not backfill

    if source_name == "google_trends":
        # Split into holiday intent and insurance intent tabs
        holiday_mask = df["metric_name"].apply(lambda m: _classify_trends_metric(m) == "holiday_intent")

        holiday_df = df[holiday_mask]
        insurance_df = df[~holiday_mask]

        if not holiday_df.empty:
            writer.write_tab(config.SHEET_HOLIDAY_INTENT, holiday_df, append_only=append_only)
        if not insurance_df.empty:
            writer.write_tab(config.SHEET_INSURANCE_INTENT, insurance_df, append_only=append_only)
    else:
        _, tab, _ = SOURCE_REGISTRY[source_name]
        if tab:
            writer.write_tab(tab, df, append_only=append_only)


def write_spike_log(all_dfs: list[pd.DataFrame], writer: GoogleSheetsWriter):
    """Write all detected spikes to the Spike Log tab."""
    if not all_dfs:
        return

    combined = pd.concat(all_dfs, ignore_index=True)
    if "is_spike" not in combined.columns:
        return

    spikes = combined[combined["is_spike"] == True]
    if spikes.empty:
        logger.info("No spikes to log")
        return

    spike_cols = ["date", "source", "metric_name", "raw_value", "normalised_value", "spike_event", "low_confidence"]
    available_cols = [c for c in spike_cols if c in spikes.columns]
    writer.write_tab(config.SHEET_SPIKE_LOG, spikes[available_cols], append_only=False)
    logger.info("Wrote %d spikes to Spike Log", len(spikes))


def main():
    parser = argparse.ArgumentParser(description="Travel Insurance Market Intelligence Pipeline")
    parser.add_argument("--backfill", action="store_true", help="Full historical backfill from 2015")
    parser.add_argument("--update", action="store_true", help="Incremental update with latest data")
    parser.add_argument("--source", type=str, help="Run a single source (e.g. google_trends, caa)")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass cache and re-fetch all data")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and process data but skip writing to Google Sheets")
    args = parser.parse_args()

    if not args.backfill and not args.update:
        parser.error("Specify either --backfill or --update")

    setup_logging(verbose=args.verbose)

    lock_fd = acquire_lock()
    try:
        _run_pipeline(args)
    finally:
        release_lock(lock_fd)


def _run_pipeline(args):
    logger.info("Pipeline started — mode=%s force_refresh=%s", "backfill" if args.backfill else "update", args.force_refresh)

    spike_detector = SpikeDetector()

    # Determine which sources to run
    if args.source:
        if args.source not in SOURCE_REGISTRY:
            logger.error("Unknown source: %s. Available: %s", args.source, ", ".join(SOURCE_REGISTRY))
            sys.exit(1)
        sources = [args.source]
    else:
        sources = list(SOURCE_REGISTRY.keys())

    # Initialise Sheets writer (unless dry run)
    writer = None
    if not args.dry_run:
        try:
            writer = GoogleSheetsWriter()
        except Exception as exc:
            logger.error("Failed to connect to Google Sheets: %s", exc)
            sys.exit(1)

    all_processed = []
    source_status = {}

    for source_name in sources:
        logger.info("Processing source: %s", source_name)
        df, status = run_source(source_name, args.backfill, args.force_refresh, spike_detector)
        source_status[source_name] = status

        if not df.empty:
            all_processed.append(df)
            if writer:
                try:
                    write_to_sheets(source_name, df, writer, args.backfill)
                except Exception as exc:
                    logger.error("Failed to write '%s' to Sheets: %s", source_name, exc)
                    source_status[source_name]["status"] = f"write_failed: {exc}"

    # Write spike log, market summary, and data freshness
    if writer:
        try:
            write_spike_log(all_processed, writer)
        except Exception as exc:
            logger.error("Failed to write Spike Log: %s", exc)

        try:
            summary_df = build_market_summary(all_processed)
            if not summary_df.empty:
                writer.write_tab(config.SHEET_MARKET_SUMMARY, summary_df, append_only=False)
                logger.info("Wrote %d quarters to Market Demand Summary", len(summary_df))
            else:
                logger.warning("No data available for Market Demand Summary")
        except Exception as exc:
            logger.error("Failed to write Market Demand Summary: %s", exc)

        try:
            writer.write_data_freshness(source_status)
        except Exception as exc:
            logger.error("Failed to write Data Freshness: %s", exc)

    # Summary
    ok_count = sum(1 for s in source_status.values() if s["status"] == "ok")
    fail_count = len(source_status) - ok_count
    logger.info("Pipeline complete — %d/%d sources OK, %d failed", ok_count, len(source_status), fail_count)

    if fail_count > 0:
        for name, status in source_status.items():
            if status["status"] != "ok" and status["status"] != "no_data":
                logger.warning("  %s: %s", name, status["status"])


if __name__ == "__main__":
    main()
