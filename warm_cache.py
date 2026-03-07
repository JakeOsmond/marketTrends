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
    disk_key = f"{cache_key}|{system[:80]}|{user[:80]}"
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
    return cached_ai(question[:100], system_prompt, question)


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
- Lead with the ONE thing that matters most. Bold it.
- Plain English only. No jargon. No "index", "SA", "normalised", "basis points".
- Say "up 15% vs last year" — never "index at 115".
- All data is Google search volume, NOT sales. More searches ≠ more HX customers. Say how to CAPTURE demand, not just report it.
- Name specifics: airlines, dates, destinations, news events. Vague = useless.
- End with ONE action: who does what, which channel, by when.
- MAX 3 sentences. If you can say it in fewer, do. No filler. No preamble.
- Bold the key facts for skimmers."""


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
    from src.ingestion.google_trends import GoogleTrendsIngestion
    gi = GoogleTrendsIngestion()
    result = {}
    for key, terms in [("competitors", COMPETITOR_TERMS), ("price_sensitivity", PRICE_SENSITIVITY_TERMS),
                       ("white_label", WHITE_LABEL_PARTNERS)]:
        try:
            df = gi.fetch_terms(terms, timeframe="today 12-m")
            if not df.empty:
                result[key] = df
        except Exception:
            pass
    return result


def load_hx_trends():
    from src.ingestion.google_trends import GoogleTrendsIngestion
    gi = GoogleTrendsIngestion()
    result = {}
    try:
        df = gi.fetch_terms(PARKING_CROSSSELL, timeframe="today 12-m")
        if not df.empty:
            result["parking"] = df
    except Exception:
        pass
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

    # 2. Extra trends
    log("Loading competitor & channel data...")
    extra_trends = load_extra_trends()
    hx_trends = load_hx_trends()
    log(f"  Extra: {list(extra_trends.keys())}, HX: {list(hx_trends.keys())}")

    # 3. Build contexts
    _ctx = build_context(sa_weekly, sources)
    _full_ctx = build_full_context(_ctx, extra_trends, hx_trends)

    # 4. Pre-generate ALL AI calls
    log("Generating AI insights...")

    # 4a. "What Matters Now"
    log("  What Matters Now...")
    matters_q = (
        f"DATA:\n{_full_ctx}\n\nThe biggest market signals right now.\n"
        f"HX priorities: 1) Take market share 2) Maximise CLTV 3) Deliver brand promise 4) Build brand."
        f"\n\nIn 2-3 sentences, explain what these signals mean for Holiday Extras. "
        f"REMEMBER: More people searching does NOT mean more HX customers. "
        f"What specific action should the team take THIS WEEK to capture the opportunity? "
        f"Be specific: who does what, on which channel, by when.")
    call_ai(matters_q, ANALYST_PROMPT)

    # 4b. Deep dive
    log("  Deep Dive...")
    dd_q = (f"DATA:\n{_full_ctx}\n\nDo a deep investigation. What's REALLY driving the UK travel insurance "
            f"market right now? Search the web for current news. Name specific airlines, destinations, "
            f"events. Remember: more searches doesn't mean more Holiday Extras customers — "
            f"how can HX capture this demand across their channels?")
    call_ai(dd_q, ANALYST_PROMPT)

    # 4c. News
    log("  News...")
    news_system = f"""Search the web for UK travel insurance news from the last 2 weeks. Only include stories that directly affect Holiday Extras.
{HX_STRATEGY_CONTEXT}
FORMAT: Return 3-4 items MAX. Only the most important. For each:
**[Headline]** — [1 sentence what happened]. **HX action:** [1 sentence what to do].

Skip anything generic. Every item must have a clear "so what" for Holiday Extras."""
    news_user = "Search the web for the most important UK travel insurance news in the last 2-4 weeks. Include real headlines and sources."
    cached_ai(f"{news_system[:50]}_{news_user[:50]}", news_system, news_user)

    # 4d. Trend analysis
    log("  Trend analysis...")
    recent = sa_weekly.tail(8)
    trend_str = " -> ".join(f"{r['date'].strftime('%b %Y')}: {float(r.get('combined', 0)):.0f}"
                            for _, r in recent.iterrows())
    trend_q = (f"DATA:\n{_ctx}\n\nRecent weekly search activity: {trend_str}. "
               f"Currently {yoy:+.1f}% vs last year. "
               f"Growing, shrinking, or flat — and why? One line on what to expect next month. "
               f"One specific action HX should take to capture this demand (not just report it).")
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

    div_q = (f"DATA:\n{_ctx}\n\n{'Insurance searches lead holiday searches' if gap > 0 else 'Holiday searches lead insurance searches'}. "
             f"Gap has {gap_story} vs last year. "
             f"Why — what real-world event or behaviour shift explains this? "
             f"One specific thing HX should do right now to convert these searchers.")
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

    ch_q = (f"DATA:\n{_ctx}\n{park_ctx}\n{wl_ctx}\n{comp_ctx}\n\n"
            f"4 channels: Direct (parking cross-sell), PPC/SEO, White Labels (Carnival, Fred Olsen), "
            f"Aggregators (Compare the Market). Which channel has the biggest opportunity right now and why? "
            f"One specific action per channel. Be blunt — skip any channel with nothing noteworthy.")
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
    comp_q = (f"DATA:\n{_ctx}\n{comp_context}\n\nWhich competitors are gaining or losing search share? "
              f"Is 'cheap travel insurance' rising? Name names and say what HX should do — "
              f"pricing, ad copy, or product. Skip competitors with no meaningful movement.")
    call_ai(comp_q, ANALYST_PROMPT)

    # 4h. Seasonal
    log("  Seasonal...")
    month = latest["date"].strftime("%B %Y") if hasattr(latest["date"], "strftime") else "this month"
    seasonal_q = (f"DATA:\n{_ctx}\n\nIt's {month}. Market is {yoy:+.1f}% vs last year. "
                  f"What happens to travel insurance demand in the next 6-8 weeks? "
                  f"Key dates (school holidays, booking windows) and one thing HX should prepare now.")
    call_ai(seasonal_q, ANALYST_PROMPT)

    # 4i. Year-on-year
    log("  Year-on-Year...")
    h_yoy = ((h_now - h_last_year) / h_last_year * 100) if h_last_year else 0
    i_yoy = ((i_now - i_last_year) / i_last_year * 100) if i_last_year else 0
    yoy_q = (f"DATA:\n{_ctx}\n\nOverall searches {yoy:+.1f}% vs last year. "
             f"Holiday searches: {h_yoy:+.0f}%. Insurance searches: {i_yoy:+.0f}%. "
             f"Why — more travellers, or different search behaviour? "
             f"What does this mean for HX sales specifically (not just market size)?")
    call_ai(yoy_q, ANALYST_PROMPT)

    # 4j. Cross-source validation
    log("  Cross-Source...")
    xsrc_q = (f"DATA:\n{_ctx}\n\n5 data sources checked. Do they agree or conflict? "
              f"Is this a real market shift or noise? One-line confidence verdict for HX decision-makers.")
    call_ai(xsrc_q, ANALYST_PROMPT)

    # 5. Done
    elapsed = time.time() - start
    log(f"Cache warm complete in {elapsed:.0f}s")
    log("=" * 60)


if __name__ == "__main__":
    main()
