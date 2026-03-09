#!/usr/bin/env python3
"""Weekly cache warmer for HX Insurance Pulse dashboard.

Run via cron every day at 6am:
    0 6 * * * cd /Users/jake.osmond/marketTrends && /usr/bin/python3 warm_cache.py >> _cache/warm.log 2>&1

This pre-generates ALL data and AI calls so the dashboard is instant for everyone.
Cache lasts 24 hours — cron should run daily.
"""

import hashlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv()

from src import config
from src.ingestion.caa import CAAIngestion
from src.ingestion.eurostat import EurostatIngestion
from src.ingestion.google_trends import GoogleTrendsIngestion
from src.ingestion.ons import ONSIngestion
from src.ingestion.world_bank import WorldBankIngestion
from src.normalisation.spike_detector import SpikeDetector

# ---------------------------------------------------------------------------
# Disk cache (mirrors dashboard.py logic)
# ---------------------------------------------------------------------------
CACHE_DIR = Path(os.environ.get("CACHE_DIR", Path(__file__).resolve().parent / "_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "ai").mkdir(exist_ok=True)
(CACHE_DIR / "images").mkdir(exist_ok=True)
CACHE_TTL_SECS = 86400  # 24 hours


def _disk_cache_get(subfolder, key):
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    path = CACHE_DIR / subfolder / h
    if path.exists():
        age = time.time() - path.stat().st_mtime
        if age < CACHE_TTL_SECS:
            if subfolder == "images":
                return path.read_bytes()
            return path.read_text(encoding="utf-8")
    return None


def _disk_cache_put(subfolder, key, value):
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    path = CACHE_DIR / subfolder / h
    if isinstance(value, bytes):
        path.write_bytes(value)
    else:
        path.write_text(value, encoding="utf-8")


# ---------------------------------------------------------------------------
# AI engine (standalone — no Streamlit dependency)
# ---------------------------------------------------------------------------
_BAD_RESPONSE_MARKERS = [
    "i don't have live web access", "i don't have live access",
    "i can't browse", "i'm unable to browse", "enable browsing",
    "i cannot access", "i don't have access to real-time",
    "i don't have the ability to browse", "can't reliably pull",
    "without risking inaccuracies", "if you can enable browsing",
    "i'm not able to search", "i can't search the web",
    "i don't have internet access", "i cannot browse",
    "unable to access the web", "i can't access the internet",
    "share links/articles",
]


def _is_bad_response(text):
    lower = text.lower()[:300]
    return any(m in lower for m in _BAD_RESPONSE_MARKERS)


def _get_client():
    from openai import OpenAI
    key = os.environ.get("OPENAI_API_KEY", "")
    return OpenAI(api_key=key) if key else None


def _call_with_web_search(system, user):
    client = _get_client()
    if not client:
        log("  SKIP — no OPENAI_API_KEY")
        return ""

    # Attempt 1: Responses API with web search
    try:
        resp = client.responses.create(
            model="gpt-5",
            tools=[{"type": "web_search_preview"}],
            instructions=system,
            input=user,
        )
        result = getattr(resp, "output_text", "") or ""
        if result and not _is_bad_response(result):
            return result
    except Exception as e:
        log(f"  Responses API failed: {e}")

    # Attempt 2: Plain chat (no web — fallback)
    try:
        resp = client.chat.completions.create(
            model="gpt-5",
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_completion_tokens=4000)
        content = resp.choices[0].message.content or ""
        if not _is_bad_response(content):
            return content
    except Exception as e:
        log(f"  Plain chat failed: {e}")

    return ""


def cached_ai(cache_key, system, user):
    disk_key = hashlib.sha256(f"{cache_key}|{system}|{user}".encode()).hexdigest()[:24]
    cached = _disk_cache_get("ai", disk_key)
    if cached and not _is_bad_response(cached):
        log(f"  CACHED (disk hit)")
        return cached
    result = _call_with_web_search(system, user)
    if result and not _is_bad_response(result):
        _disk_cache_put("ai", disk_key, result)
        return result
    return ""


def call_ai(question, system_prompt):
    cache_key = hashlib.sha256(question.encode()).hexdigest()[:16]
    return cached_ai(cache_key, system_prompt, question)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data loading (mirrors dashboard.py)
# ---------------------------------------------------------------------------
import numpy as np
import pandas as pd

BASELINE_YEAR = 2019
HOLIDAY_TERMS = set(config.HOLIDAY_INTENT_TERMS)

COMPETITOR_TERMS = ["Staysure", "AllClear travel insurance", "Post Office travel insurance",
                    "Compare the Market insurance", "travel supermarket"]
PRICE_SENSITIVITY_TERMS = ["cheap travel insurance", "travel insurance deals",
                           "cheapest travel insurance", "budget travel insurance"]
WHITE_LABEL_PARTNERS = ["Carnival cruise", "Fred Olsen cruise"]
PARKING_CROSSSELL = ["airport parking", "airport parking UK", "meet and greet parking"]

# Bespoke trend terms per section — give AI real-world context for each
SECTION_TREND_TERMS = {
    "market_demand": ["cheap flights", "travel chaos UK", "airline strikes", "holiday deals", "passport renewal UK"],
    "divergence": ["GHIC card", "travel insurance comparison", "book holiday 2026", "do I need travel insurance", "EHIC card UK"],
    "channels": ["airport parking deals", "compare travel insurance", "cruise holiday deals", "buy travel insurance online", "Holiday Extras"],
    "competitors": ["Staysure reviews", "AllClear medical travel insurance", "cheapest travel insurance UK", "travel insurance over 70", "Defaqto travel insurance"],
    "seasonal": ["summer holiday booking", "half term holidays", "ski holidays", "Easter holiday deals", "last minute holidays"],
    "yoy": ["cost of living UK", "UK passport applications", "package holiday UK", "travel insurance price", "UK outbound travel"],
}

HX_STRATEGY_CONTEXT = """
HOLIDAY EXTRAS MARKETING STRATEGY (frame all advice against these priorities):
1. TAKE MARKET SHARE -- Increase GWP, grow market share, drive profitable new customer acquisition, improve AMT and Medical mix
2. MAXIMISE CUSTOMER LIFETIME VALUE -- Increase renewal rate, improve cross-sell (Insurance <-> Distribution), grow GP per customer, increase AMT penetration
3. DELIVER BRAND PROMISE -- Reduce buying friction, improve trip capture (especially AMT), increase add-on attachment, move renewal beyond email-only, simplify claims/support
4. BUILD BRAND -- Increase brand awareness, improve consideration within HX database, improve acquisition efficiency (CPA/CPC over time)

CHANNELS: Direct (parking cross-sell), PPC/SEO (new acquisition), White Labels (Carnival, Fred Olsen, retail partners), Aggregators (Compare the Market, CYTI)
"""

ANALYST_PROMPT = f"""You brief the Holiday Extras insurance team. They're busy. Every word must earn its place.

{HX_STRATEGY_CONTEXT}

RULES:
- Reply in plain English a 12-year-old could understand. No jargon.
- NEVER use asterisks, markdown formatting, bullet points, or special symbols. Use <b> tags if you need bold.
- Never say "index", "SA", "normalised", "basis points". Say "up 15% vs last year".
- All data is Google search volume, NOT sales. More searches does not mean more HX customers. Say how to CAPTURE demand.
- Insurance search volume is the primary KPI. The gap between insurance and holiday searches is a secondary consideration.
- Name specifics: airlines, destinations, news events. Vague is useless.
- End with ONE suggested action: who does what, which channel. Never include specific date deadlines like "by Friday" or "this week".
- Write in natural flowing sentences. Never use bullet points or numbered lists. Keep it to 3-4 sentences maximum. No filler. No preamble."""


def fetch_section_trends() -> dict:
    """Fetch bespoke Google Trends for each dashboard section."""
    from pytrends.request import TrendReq
    pytrends = TrendReq(hl="en-GB", tz=0)
    results = {}
    rate_limited = False
    for section, terms in SECTION_TREND_TERMS.items():
        section_data = {}
        if rate_limited:
            results[section] = section_data
            continue
        try:
            pytrends.build_payload(terms[:5], cat=0, timeframe="today 12-m", geo="GB")
            time.sleep(3)
            interest = pytrends.interest_over_time()
            if not interest.empty:
                for term in terms:
                    if term in interest.columns:
                        recent = interest[term].tail(4).mean()
                        earlier = interest[term].head(4).mean()
                        change = ((recent - earlier) / earlier * 100) if earlier > 0 else 0
                        section_data[term] = {
                            "current": int(interest[term].iloc[-1]),
                            "peak": int(interest[term].max()),
                            "change_pct": round(change, 1),
                            "trending": "up" if change > 10 else ("down" if change < -10 else "flat"),
                        }
        except Exception as e:
            if "429" in str(e):
                log(f"  Google Trends rate-limited (429) — skipping remaining section trends")
                rate_limited = True
        results[section] = section_data
    return results


def _format_section_trends(section_key: str, section_trends: dict) -> str:
    """Format bespoke trend data as context for AI prompts."""
    data = section_trends.get(section_key, {})
    if not data:
        return ""
    lines = ["RELATED GOOGLE TRENDS (UK, last 12 months):"]
    for term, stats in data.items():
        lines.append(f'  "{term}": {stats["current"]}/100 (peak {stats["peak"]}), '
                     f'{stats["trending"]} {stats["change_pct"]:+.1f}%')
    return "\n".join(lines)


def load_all_data():
    sd = SpikeDetector()
    sources = {}
    for name, cls in {
        "google_trends": GoogleTrendsIngestion, "caa": CAAIngestion,
        "ons": ONSIngestion, "eurostat": EurostatIngestion, "world_bank": WorldBankIngestion,
    }.items():
        try:
            inst = cls()
            df = inst.backfill()
            if not df.empty:
                parts = [sd.detect_and_normalise(mdf) for _, mdf in df.groupby("metric_name")]
                sources[name] = pd.concat(parts, ignore_index=True)
        except Exception as e:
            log(f"  {name} failed: {e}")
    return sources


def build_weekly_trends(sources):
    gt = sources.get("google_trends")
    if gt is None or gt.empty:
        return pd.DataFrame()
    gt = gt.copy()
    gt["date"] = pd.to_datetime(gt["date"])
    gt["category"] = gt["metric_name"].apply(lambda m: "holiday" if m in HOLIDAY_TERMS else "insurance")
    gt["week"] = gt["date"].dt.to_period("W").dt.start_time
    weekly = gt.groupby(["week", "category"])["normalised_value"].mean().reset_index()
    weekly = weekly.pivot(index="week", columns="category", values="normalised_value").reset_index()
    weekly.columns.name = None
    weekly = weekly.rename(columns={"week": "date"})
    for col in ["holiday", "insurance"]:
        if col in weekly.columns:
            baseline = weekly.loc[weekly["date"].dt.year == BASELINE_YEAR, col].mean()
            if baseline and baseline > 0:
                weekly[col] = (weekly[col] / baseline) * 100
    idx_cols = [c for c in ["holiday", "insurance"] if c in weekly.columns]
    if idx_cols:
        weekly["combined"] = weekly[idx_cols].mean(axis=1)
    return weekly


def _trend_pct(df, col=None):
    if df is None or df.empty:
        return 0.0
    if col is None:
        col = df.columns[0] if len(df.columns) == 1 else df.columns[-1]
    if col not in df.columns:
        return 0.0
    vals = df[col].dropna()
    if len(vals) < 12:
        return 0.0
    recent = vals.tail(4).mean()
    earlier = vals.tail(52).head(4).mean()
    if earlier == 0:
        return 0.0
    return ((recent - earlier) / earlier) * 100


def load_extra_trends():
    """Fetch competitor and price sensitivity trends via pytrends directly."""
    from pytrends.request import TrendReq
    pytrends = TrendReq(hl="en-GB", tz=0)
    result = {}
    for key, terms in [("competitors", COMPETITOR_TERMS), ("price_sensitivity", PRICE_SENSITIVITY_TERMS),
                       ("white_label", WHITE_LABEL_PARTNERS)]:
        try:
            pytrends.build_payload(terms[:5], cat=0, timeframe="today 12-m", geo="GB")
            time.sleep(3)
            df = pytrends.interest_over_time()
            if not df.empty:
                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])
                result[key] = df
        except Exception as e:
            if "429" in str(e):
                log(f"  Google Trends rate-limited — skipping remaining extra trends")
                break
    return result


def load_hx_trends():
    """Fetch HX channel trends via pytrends directly."""
    from pytrends.request import TrendReq
    pytrends = TrendReq(hl="en-GB", tz=0)
    result = {}
    try:
        pytrends.build_payload(PARKING_CROSSSELL[:5], cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(3)
        df = pytrends.interest_over_time()
        if not df.empty:
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            result["parking"] = df
    except Exception as e:
        if "429" in str(e):
            log(f"  Google Trends rate-limited — skipping HX trends")
    return result


def build_context(sa_weekly, sources):
    lines = []
    latest = sa_weekly.iloc[-1]
    for col in ["combined", "holiday", "insurance"]:
        sa_col = f"{col}_sa" if f"{col}_sa" in sa_weekly.columns else col
        if sa_col in sa_weekly.columns:
            now = float(latest.get(sa_col, 0))
            lines.append(f"{col}: {now:.0f}")
    if len(sa_weekly) > 52:
        ago = sa_weekly.iloc[-53]
        for col in ["combined", "holiday", "insurance"]:
            sa_col = f"{col}_sa" if f"{col}_sa" in sa_weekly.columns else col
            if sa_col in sa_weekly.columns:
                ly = float(ago.get(sa_col, 0))
                lines.append(f"{col}_last_year: {ly:.0f}")
    return "\n".join(lines)


def build_full_context(ctx, extra_trends, hx_trends):
    parts = [ctx]
    for label, key in [("Competitors", "competitors"), ("Price sensitivity", "price_sensitivity"),
                       ("White labels", "white_label")]:
        df = extra_trends.get(key)
        if df is not None and not df.empty:
            parts.append(f"\n{label}:")
            for t in df.columns:
                parts.append(f"  {t}: {_trend_pct(df, t):+.0f}%")
    park = hx_trends.get("parking")
    if park is not None and not park.empty:
        parts.append("\nParking cross-sell:")
        for t in park.columns:
            parts.append(f"  {t}: {_trend_pct(park, t):+.0f}%")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Google Sheets export (for Apps Script dashboard)
# ---------------------------------------------------------------------------
def _post_to_sheets(tab_name, headers, rows):
    """Write data to Google Sheets via the Apps Script webhook."""
    import requests
    url = os.environ.get("APPS_SCRIPT_WEBHOOK_URL", "")
    if not url:
        log(f"  SKIP sheets write — no APPS_SCRIPT_WEBHOOK_URL")
        return
    payload = {"action": "write", "tab": tab_name, "headers": headers, "rows": rows}
    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        log(f"  Wrote {len(rows)} rows to '{tab_name}'")
    except Exception as e:
        log(f"  Failed to write '{tab_name}': {e}")


def _export_chart_data(weekly, sa_weekly, section_trends, extra_trends, hx_trends,
                       c_now, h_now, i_now, c_last_year, h_last_year, i_last_year,
                       yoy, wow, gap):
    """Export chart data + metrics to Google Sheets (fast — no AI dependency)."""

    # Dashboard Weekly — the time series for charts
    if not weekly.empty:
        headers = ["date", "combined", "holiday", "insurance"]
        rows = []
        for _, r in weekly.iterrows():
            d = r["date"].strftime("%Y-%m-%d") if hasattr(r["date"], "strftime") else str(r["date"])
            rows.append([d,
                         round(float(r.get("combined", 0)), 1),
                         round(float(r.get("holiday", 0)), 1),
                         round(float(r.get("insurance", 0)), 1)])
        _post_to_sheets("Dashboard Weekly", headers, rows)

    # Dashboard Metrics — key headline numbers
    gap_last_year = i_last_year - h_last_year
    h_yoy = ((h_now - h_last_year) / h_last_year * 100) if h_last_year else 0
    i_yoy = ((i_now - i_last_year) / i_last_year * 100) if i_last_year else 0
    metrics_rows = [
        ["yoy", round(yoy, 1), "up" if yoy > 0 else "down", "Overall market demand vs last year"],
        ["wow", round(wow, 1), "up" if wow > 0 else "down", "4-week momentum"],
        ["c_now", round(c_now, 1), "", "Combined index current value"],
        ["h_now", round(h_now, 1), "", "Holiday searches current"],
        ["i_now", round(i_now, 1), "", "Insurance searches current"],
        ["c_last_year", round(c_last_year, 1), "", "Combined index last year"],
        ["h_last_year", round(h_last_year, 1), "", "Holiday searches last year"],
        ["i_last_year", round(i_last_year, 1), "", "Insurance searches last year"],
        ["h_yoy", round(h_yoy, 1), "up" if h_yoy > 0 else "down", "Holiday dreamers vs last year"],
        ["i_yoy", round(i_yoy, 1), "up" if i_yoy > 0 else "down", "Insurance shoppers vs last year"],
        ["gap", round(gap, 1), "up" if gap > 0 else "down", "Insurance minus holiday searches"],
        ["gap_last_year", round(gap_last_year, 1), "", "Gap last year"],
    ]
    _post_to_sheets("Dashboard Metrics", ["metric_key", "value", "direction", "description"], metrics_rows)

    # Dashboard Section Trends
    trend_rows = []
    for section_key, terms_data in section_trends.items():
        for term, stats in terms_data.items():
            trend_rows.append([
                section_key, term,
                stats.get("current", 0), stats.get("peak", 0),
                stats.get("change_pct", 0), stats.get("trending", "flat")
            ])
    if trend_rows:
        _post_to_sheets("Dashboard Section Trends",
                        ["section", "term", "current", "peak", "change_pct", "trending"],
                        trend_rows)

    # Dashboard Competitors
    comp_rows = []
    for key in ("competitors", "price_sensitivity"):
        df = extra_trends.get(key)
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                d = r.name if hasattr(r.name, "strftime") else str(r.name)
                if hasattr(d, "strftime"):
                    d = d.strftime("%Y-%m-%d")
                for col in df.columns:
                    comp_rows.append([str(d), key, col, round(float(r[col]), 1)])
    if comp_rows:
        _post_to_sheets("Dashboard Competitors",
                        ["date", "category", "term", "value"],
                        comp_rows)

    # Dashboard Channels
    chan_rows = []
    for key, src in [("parking", hx_trends), ("white_label", extra_trends)]:
        df = src.get(key) if isinstance(src, dict) else None
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                d = r.name if hasattr(r.name, "strftime") else str(r.name)
                if hasattr(d, "strftime"):
                    d = d.strftime("%Y-%m-%d")
                for col in df.columns:
                    chan_rows.append([str(d), key, col, round(float(r[col]), 1)])
    if chan_rows:
        _post_to_sheets("Dashboard Channels",
                        ["date", "category", "term", "value"],
                        chan_rows)

    log("  Chart data export complete.")


def _export_ai_insights(matters_q, dd_q, trend_q, div_q, ch_q, seasonal_q, yoy_q, xsrc_q,
                        news_system, news_user):
    """Export AI insights from disk cache to Google Sheets."""
    from datetime import datetime as _dt
    now_str = _dt.now().strftime("%Y-%m-%d %H:%M")
    ai_rows = []
    insight_queries = {
        "what_matters": (matters_q, ANALYST_PROMPT),
        "deep_dive": (dd_q, ANALYST_PROMPT),
        "trend": (trend_q, ANALYST_PROMPT),
        "divergence": (div_q, ANALYST_PROMPT),
        "channels": (ch_q, ANALYST_PROMPT),
        "seasonal": (seasonal_q, ANALYST_PROMPT),
        "yoy": (yoy_q, ANALYST_PROMPT),
        "quarterly": (xsrc_q, ANALYST_PROMPT),
        "news": (news_user, news_system),
    }
    for section_key, (user_q, system_q) in insight_queries.items():
        if section_key == "news":
            ck = hashlib.sha256(f"{system_q[:50]}_{user_q[:50]}|{system_q}|{user_q}".encode()).hexdigest()[:24]
        else:
            ck = hashlib.sha256(f"{hashlib.sha256(user_q.encode()).hexdigest()[:16]}|{system_q}|{user_q}".encode()).hexdigest()[:24]
        cached = _disk_cache_get("ai", ck)
        ai_rows.append([section_key, cached or "", now_str])

    _post_to_sheets("AI Insights", ["section_key", "insight_text", "generated_at"], ai_rows)
    log("  AI insights export complete.")


# ---------------------------------------------------------------------------
# Main cache warming
# ---------------------------------------------------------------------------
def main():
    start = time.time()
    log("=" * 60)
    log("HX Insurance Pulse — Weekly Cache Warm")
    log("=" * 60)

    # 1. Load data
    log("Loading pipeline data...")
    sources = load_all_data()
    log(f"  Sources loaded: {list(sources.keys())}")

    weekly = build_weekly_trends(sources)
    if weekly.empty:
        log("ERROR: No weekly data. Run pipeline first: python3 -m src.main --backfill")
        return

    # Simple SA placeholder (the dashboard uses add_all_sa but we just need the values)
    sa_weekly = weekly.copy()

    latest = sa_weekly.iloc[-1]
    c_now = float(latest.get("combined", 0))
    h_now = float(latest.get("holiday", 0))
    i_now = float(latest.get("insurance", 0))
    gap = i_now - h_now

    yoy = wow = 0.0
    c_last_year = h_last_year = i_last_year = c_now
    if len(sa_weekly) > 52:
        ago = sa_weekly.iloc[-53]
        c_last_year = float(ago.get("combined", c_now))
        h_last_year = float(ago.get("holiday", h_now))
        i_last_year = float(ago.get("insurance", i_now))
        if c_last_year:
            yoy = ((c_now - c_last_year) / c_last_year) * 100
    if len(sa_weekly) > 4:
        p4 = float(sa_weekly.iloc[-5].get("combined", c_now))
        if p4:
            wow = ((c_now - p4) / p4) * 100

    log(f"  YoY: {yoy:+.1f}%, WoW: {wow:+.1f}%, Gap: {gap:+.1f}")

    # 2. Extra trends + section trends
    log("Loading competitor & channel data...")
    extra_trends = load_extra_trends()
    hx_trends = load_hx_trends()
    log(f"  Extra: {list(extra_trends.keys())}, HX: {list(hx_trends.keys())}")

    log("Fetching section-specific Google Trends...")
    section_trends = fetch_section_trends()
    log(f"  Section trends: {[k for k, v in section_trends.items() if v]}")

    # 3. Export chart data to Google Sheets FIRST (fast — before slow AI step)
    log("Exporting chart data to Google Sheets...")
    _export_chart_data(weekly, sa_weekly, section_trends, extra_trends, hx_trends,
                       c_now, h_now, i_now, c_last_year, h_last_year, i_last_year,
                       yoy, wow, gap)

    # 4. Build contexts
    _ctx = build_context(sa_weekly, sources)
    _full_ctx = build_full_context(_ctx, extra_trends, hx_trends)

    # 5. Pre-generate ALL AI calls (slow — can timeout)
    log("Generating AI insights...")

    # 4a. "What Matters Now"
    log("  What Matters Now...")
    all_trends_ctx = "\n".join(_format_section_trends(k, section_trends) for k in SECTION_TREND_TERMS if _format_section_trends(k, section_trends))
    matters_q = (
        f"DATA:\n{_full_ctx}\n\nThe biggest market signals right now.\n"
        f"\n{all_trends_ctx}\n\n"
        f"HX priorities: 1) Take market share 2) Maximise CLTV 3) Deliver brand promise 4) Build brand."
        f"\n\nIn 2-3 sentences, explain what these signals mean for Holiday Extras. "
        f"Use the Google Trends data to explain WHY these things are happening. "
        f"REMEMBER: More people searching does NOT mean more HX customers. "
        f"What specific action should the team take to capture the opportunity? "
        f"Be specific: who does what, on which channel.")
    call_ai(matters_q, ANALYST_PROMPT)

    # 4b. Deep dive
    log("  Deep Dive...")
    dd_q = (f"DATA:\n{_full_ctx}\n\n{all_trends_ctx}\n\n"
            f"Do a deep investigation. What's REALLY driving the UK travel insurance "
            f"market right now? Use the Google Trends data above and search the web for current news. "
            f"Name specific airlines, destinations, events. "
            f"Remember: more searches doesn't mean more Holiday Extras customers — "
            f"how can HX capture this demand across their channels?")
    call_ai(dd_q, ANALYST_PROMPT)

    # 4c. News
    log("  News...")
    news_system = f"""Search the web for UK travel insurance news from the last 2 weeks. Only include stories that directly affect Holiday Extras.
{HX_STRATEGY_CONTEXT}
RULES:
- Reply in plain English a 12-year-old could understand. No asterisks, no markdown, no special symbols.
- Use <b> tags for emphasis. NEVER use ** or *.
- Write in flowing sentences, not bullet points or lists.
- MAX 600 characters total.

Write 3-4 short paragraphs about the most important news items. Each paragraph should name the headline, explain what happened in one sentence, and suggest what HX should consider doing about it. Never include specific date deadlines. Skip anything generic."""
    news_user = ("Search the web for the most important UK travel insurance news in the last 2-4 weeks. "
                  "Include real headlines and sources. Focus on anything affecting travel demand, "
                  "insurance pricing, airline disruption, or competitor moves in the UK market.")
    cached_ai(f"{news_system[:50]}_{news_user[:50]}", news_system, news_user)

    # 4d. Trend analysis
    log("  Trend analysis...")
    recent = sa_weekly.tail(8)
    trend_str = " -> ".join(f"{r['date'].strftime('%b %Y')}: {float(r.get('combined', 0)):.0f}"
                            for _, r in recent.iterrows())
    md_trends = _format_section_trends("market_demand", section_trends)
    trend_q = (f"DATA:\n{_ctx}\n\nRecent weekly search activity: {trend_str}. "
               f"Currently {yoy:+.1f}% vs last year, 4-week momentum {wow:+.1f}%. "
               f"\n{md_trends}\n\n"
               f"Using the Google Trends above (cheap flights, travel chaos, passport renewals etc), "
               f"explain WHY travel insurance demand is {'rising' if yoy > 2 else 'falling' if yoy < -2 else 'flat'}. "
               f"What real-world event or behaviour is driving this? "
               f"One specific action HX should take to capture this demand.")
    call_ai(trend_q, ANALYST_PROMPT)

    # 4e. Divergence (buyers vs dreamers)
    log("  Buyers vs Dreamers...")
    gap_last_year = i_last_year - h_last_year
    if (gap > 0) != (gap_last_year > 0):
        gap_story = "flipped"
    elif abs(gap) > abs(gap_last_year) + 5:
        gap_story = "widened"
    elif abs(gap) < abs(gap_last_year) - 5:
        gap_story = "narrowed"
    else:
        gap_story = "stable"

    div_trends = _format_section_trends("divergence", section_trends)
    div_q = (f"DATA:\n{_ctx}\n\n{'Insurance searches lead holiday searches' if gap > 0 else 'Holiday searches lead insurance searches'} "
             f"(insurance {i_now:.0f}, holidays {h_now:.0f}, gap {gap:+.0f}). "
             f"Gap has {gap_story} vs last year (was {gap_last_year:+.0f}). "
             f"\n{div_trends}\n\n"
             f"Using the trends above (GHIC cards, 'do I need travel insurance', booking patterns), "
             f"explain why {'insurance' if gap > 0 else 'holiday'} searches are leading. "
             f"Are people closer to buying or still dreaming? "
             f"One specific thing HX should do to convert these searchers into customers.")
    call_ai(div_q, ANALYST_PROMPT)

    # 4f. Channels
    log("  Channels...")
    park_df = hx_trends.get("parking")
    wl_df = extra_trends.get("white_label")
    comp_df = extra_trends.get("competitors")
    park_ctx = ""
    if park_df is not None and not park_df.empty:
        park_ctx = "\nAirport parking trends (last 12m):\n"
        for t in park_df.columns:
            park_ctx += f"  {t}: {_trend_pct(park_df, t):+.0f}% trend\n"
    wl_ctx = ""
    if wl_df is not None and not wl_df.empty:
        wl_ctx = "\nWhite label partner brand searches:\n"
        for t in wl_df.columns:
            wl_ctx += f"  {t}: {_trend_pct(wl_df, t):+.0f}% trend\n"
    comp_ctx = ""
    if comp_df is not None and not comp_df.empty:
        comp_ctx = "\nCompetitor brand searches:\n"
        for t in comp_df.columns:
            comp_ctx += f"  {t}: {_trend_pct(comp_df, t):+.0f}%\n"

    ch_trends = _format_section_trends("channels", section_trends)
    park_ch = _trend_pct(park_df) if park_df is not None else 0
    wl_ch = _trend_pct(wl_df) if wl_df is not None else 0
    ch_q = (f"DATA:\n{_ctx}\n{park_ctx}\n{wl_ctx}\n{comp_ctx}\n\n{ch_trends}\n\n"
            f"HX has 4 channels: Direct (airport parking cross-sell), PPC/SEO (new customers from search), "
            f"White Labels (Carnival Cruises, Fred Olsen), Aggregators (Compare the Market, CYTI). "
            f"Parking searches are {park_ch:+.0f}%, white label partner interest is {wl_ch:+.0f}%. "
            f"Using the Google Trends above, which channel has the biggest opportunity? "
            f"One specific action per channel that's moving. Skip channels with nothing noteworthy.")
    call_ai(ch_q, ANALYST_PROMPT)

    # 4g. Competitors
    log("  Competitors...")
    comp_context = ""
    price_df = extra_trends.get("price_sensitivity")
    for label, df in [("Competitor brands", comp_df), ("Price sensitivity", price_df)]:
        if df is not None and not df.empty:
            comp_context += f"\n{label}:\n"
            for t in df.columns:
                comp_context += f"  {t}: {_trend_pct(df, t):+.0f}% trend\n"
    comp_trends = _format_section_trends("competitors", section_trends)
    price_ch = _trend_pct(price_df) if price_df is not None else 0
    comp_q = (f"DATA:\n{_ctx}\n{comp_context}\n\n{comp_trends}\n\n"
              f"Price sensitivity is {price_ch:+.0f}%. "
              f"Using the Google Trends above (Staysure reviews, AllClear, cheapest TI, over-70s, Defaqto), "
              f"which competitors are gaining or losing? Is price shopping rising or falling? "
              f"Name specific competitors and say what HX should do — pricing, ad copy, or product positioning. "
              f"Skip any competitor with no meaningful movement.")
    call_ai(comp_q, ANALYST_PROMPT)

    # 4h. Seasonal
    log("  Seasonal...")
    month = latest["date"].strftime("%B %Y") if hasattr(latest["date"], "strftime") else "this month"
    seasonal_trends = _format_section_trends("seasonal", section_trends)
    seasonal_q = (f"DATA:\n{_ctx}\n\nIt's {month}. Market is {yoy:+.1f}% vs last year. "
                  f"\n{seasonal_trends}\n\n"
                  f"Using the trends above (summer bookings, half term, ski holidays, Easter, last minute), "
                  f"what's driving seasonal demand right now? "
                  f"What happens to travel insurance demand in the next 6-8 weeks? "
                  f"Name specific dates (school holidays, bank holidays, booking deadlines) and "
                  f"one thing HX should prepare to capture the next wave.")
    call_ai(seasonal_q, ANALYST_PROMPT)

    # 4i. Year-on-year
    log("  Year-on-Year...")
    h_yoy = ((h_now - h_last_year) / h_last_year * 100) if h_last_year else 0
    i_yoy = ((i_now - i_last_year) / i_last_year * 100) if i_last_year else 0
    yoy_trends = _format_section_trends("yoy", section_trends)
    yoy_q = (f"DATA:\n{_ctx}\n\nOverall searches {yoy:+.1f}% vs last year. "
             f"Holiday searches: {h_yoy:+.0f}%. Insurance searches: {i_yoy:+.0f}%. "
             f"\n{yoy_trends}\n\n"
             f"Using the trends above (cost of living, passport applications, package holidays, outbound travel), "
             f"explain the year-on-year change. Is it more travellers, different search behaviour, or economic shifts? "
             f"What does this mean specifically for HX sales — not just market size but customer acquisition?")
    call_ai(yoy_q, ANALYST_PROMPT)

    # 4j. Cross-source validation
    log("  Cross-Source...")
    xsrc_q = (f"DATA:\n{_ctx}\n\n"
              f"5 data sources: Google Trends (weekly), UK CAA (annual), ONS (quarterly), Eurostat (monthly), World Bank (annual). "
              f"Do they agree or conflict on the direction of travel? "
              f"Which sources are most up-to-date? Is this a real market shift or noise? "
              f"One-line confidence verdict for HX decision-makers.")
    call_ai(xsrc_q, ANALYST_PROMPT)

    # 6. Export AI insights to Google Sheets
    log("Exporting AI insights to Google Sheets...")
    _export_ai_insights(matters_q, dd_q, trend_q, div_q, ch_q, seasonal_q, yoy_q, xsrc_q,
                        news_system, news_user)

    # 7. Done
    elapsed = time.time() - start
    log(f"Cache warm complete in {elapsed:.0f}s")
    log("=" * 60)


if __name__ == "__main__":
    main()
