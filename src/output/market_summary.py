"""Build a Market Demand Summary tab from all processed source data.

Indexes every metric to 2019 = 100, resamples to quarterly, and computes
a combined demand index across all available signals.
"""

import logging

import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)

BASELINE_YEAR = 2019

# Which Google Trends metrics map to which index
HOLIDAY_TERMS = set(config.HOLIDAY_INTENT_TERMS)
INSURANCE_TERMS = set(config.INSURANCE_INTENT_TERMS)


def _to_quarterly(df: pd.DataFrame, value_col: str, agg: str = "mean") -> pd.DataFrame:
    """Resample a date-indexed series to calendar quarters."""
    ts = df.set_index("date")[[value_col]].copy()
    ts.index = pd.DatetimeIndex(ts.index)
    quarterly = ts.resample("QE").agg(agg).dropna()
    quarterly = quarterly.reset_index()
    quarterly["quarter"] = quarterly["date"].dt.to_period("Q").astype(str)
    return quarterly


def _index_to_baseline(df: pd.DataFrame, value_col: str, baseline_year: int = BASELINE_YEAR) -> pd.DataFrame:
    """Normalise values so that the baseline year average = 100."""
    baseline_mask = df["date"].dt.year == baseline_year
    baseline_vals = df.loc[baseline_mask, value_col]
    if baseline_vals.empty or baseline_vals.mean() == 0:
        return df
    baseline_mean = baseline_vals.mean()
    df = df.copy()
    df[value_col] = (df[value_col] / baseline_mean) * 100
    return df


def build_market_summary(all_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine all source DataFrames into a quarterly market demand summary.

    Returns a DataFrame with columns:
        Quarter, Holiday_Intent_Index, Insurance_Intent_Index,
        UK_Passengers_Index, Visits_Abroad_Index, Global_Aviation_Index,
        Combined_Demand_Index
    """
    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    if "normalised_value" not in combined.columns or "date" not in combined.columns:
        return pd.DataFrame()

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date", "normalised_value"])

    # --- 1. Holiday Intent Index (average of all holiday search terms) ---
    holiday_df = combined[combined["metric_name"].isin(HOLIDAY_TERMS)]
    holiday_q = _build_index(holiday_df, "Holiday_Intent_Index")

    # --- 2. Insurance Intent Index (average of all insurance search terms) ---
    insurance_df = combined[combined["metric_name"].isin(INSURANCE_TERMS)]
    insurance_q = _build_index(insurance_df, "Insurance_Intent_Index")

    # --- 3. UK Passengers Index (CAA terminal passengers) ---
    uk_pax_df = combined[combined["metric_name"] == "uk_terminal_passengers"]
    uk_pax_q = _build_index(uk_pax_df, "UK_Passengers_Index")

    # --- 4. Visits Abroad Index (ONS quarterly visits) ---
    visits_df = combined[combined["metric_name"] == "uk_visits_abroad"]
    visits_q = _build_index(visits_df, "Visits_Abroad_Index")

    # --- 6. Global Aviation Index (World Bank) ---
    global_df = combined[combined["metric_name"] == "air_passengers_global"]
    global_q = _build_index(global_df, "Global_Aviation_Index")

    # --- Merge all into one quarterly table ---
    indices = [holiday_q, insurance_q, uk_pax_q, visits_q, global_q]
    indices = [idx for idx in indices if not idx.empty]

    if not indices:
        return pd.DataFrame()

    result = indices[0]
    for idx in indices[1:]:
        result = result.merge(idx, on="quarter", how="outer")

    # Sort by quarter
    result = result.sort_values("quarter").reset_index(drop=True)

    # --- Combined Demand Index: mean of all available indices per quarter ---
    index_cols = [c for c in result.columns if c.endswith("_Index")]
    result["Combined_Demand_Index"] = result[index_cols].mean(axis=1).round(1)

    # Round all index columns
    for col in index_cols:
        result[col] = result[col].round(1)

    # Add a YoY change column
    result["YoY_Change_%"] = ""
    for i, row in result.iterrows():
        q_str = row["quarter"]
        # Find same quarter previous year
        try:
            prev_q = q_str[:4]
            prev_year = str(int(prev_q) - 1)
            prev_quarter = prev_year + q_str[4:]
            prev_row = result[result["quarter"] == prev_quarter]
            if not prev_row.empty:
                prev_val = prev_row.iloc[0]["Combined_Demand_Index"]
                curr_val = row["Combined_Demand_Index"]
                if prev_val and prev_val != 0:
                    change = ((curr_val - prev_val) / prev_val) * 100
                    result.at[i, "YoY_Change_%"] = f"{change:+.1f}%"
        except (ValueError, TypeError):
            continue

    return result


def _build_index(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """From a filtered DataFrame, compute a quarterly index (2019=100)."""
    if df.empty:
        return pd.DataFrame(columns=["quarter", col_name])

    # Average across metrics if multiple (e.g. multiple search terms)
    monthly = df.groupby("date")["normalised_value"].mean().reset_index()
    monthly.columns = ["date", "value"]

    # Resample to quarterly
    quarterly = _to_quarterly(monthly, "value", agg="mean")

    # Index to 2019 baseline
    quarterly = _index_to_baseline(quarterly, "value", BASELINE_YEAR)

    return quarterly[["quarter", "value"]].rename(columns={"value": col_name})
