"""Travel Insurance Market Intelligence Dashboard — Holiday Extras.

Insight-first, dynamic, AI-powered dashboard for weekly market demand meetings.
Run with: python3 -m streamlit run dashboard.py
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# On Streamlit Cloud, secrets come from st.secrets instead of .env
try:
    for key in ["OPENAI_API_KEY", "APPS_SCRIPT_WEBHOOK_URL"]:
        if key not in os.environ and hasattr(st, "secrets") and key in st.secrets:
            os.environ[key] = st.secrets[key]
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src import config
from src.ingestion.caa import CAAIngestion
from src.ingestion.eurostat import EurostatIngestion
from src.ingestion.google_trends import GoogleTrendsIngestion
from src.ingestion.ons import ONSIngestion
from src.ingestion.world_bank import WorldBankIngestion
from src.normalisation.spike_detector import SpikeDetector

# ---------------------------------------------------------------------------
# Tracking terms
# ---------------------------------------------------------------------------
COMPETITOR_TERMS = ["Staysure", "AllClear travel insurance", "Post Office travel insurance",
                    "Compare the Market insurance", "travel supermarket"]
PRICE_SENSITIVITY_TERMS = ["cheap travel insurance", "travel insurance deals",
                           "cheapest travel insurance", "budget travel insurance"]
WHITE_LABEL_PARTNERS = ["Carnival cruise", "Fred Olsen cruise"]
PARKING_CROSSSELL = ["airport parking", "airport parking UK", "meet and greet parking"]

# ---------------------------------------------------------------------------
# Holiday Extras official brand palette
# ---------------------------------------------------------------------------
# Core palette
HX_PURPLE = "#542E91"       # Primary brand purple
HX_YELLOW = "#FDDC06"       # Primary brand yellow
HX_WHITE = "#FFFFFF"
HX_OFFWHITE = "#F0F0F0"
HX_BLACK = "#232323"

# Functional palette (UI elements only)
HX_BLUE = "#3AA6FF"
HX_GREEN = "#00B0A6"
HX_ORANGE = "#FFB55F"
HX_RED = "#FF5F68"

# Special use (sparingly)
HX_VIOLET = "#925FFF"

# Derived for dark dashboard
HX_PURPLE_LIGHT = "#9B72CF" # Purple for text on dark backgrounds
HX_PURPLE_MED = "#7B52C1"   # Purple for hover states

# Legacy aliases for chart palette
BLUE = HX_BLUE
PURPLE = HX_PURPLE
MAGENTA = "#DC267F"
ORANGE = HX_ORANGE
GOLD = HX_YELLOW
TEAL = HX_GREEN
GREY = "#8D99AE"
PALETTE = [HX_PURPLE_LIGHT, HX_ORANGE, HX_GREEN, MAGENTA, HX_YELLOW, HX_BLUE, GREY]

BG_PRIMARY = "#0F0B18"      # Very dark with purple tint
BG_CARD = "#1A1528"          # Card background — dark purple-grey
TEXT_PRIMARY = HX_OFFWHITE
TEXT_SECONDARY = "#B0A8C0"   # Slightly purple-tinted secondary text
TEXT_MUTED = "#9AA3B5"       # Improved contrast (WCAG AA)
ACCENT_UP = HX_GREEN         # Functional green for positive
ACCENT_DOWN = HX_RED         # Functional red for negative
BASELINE_YEAR = 2019

HX_LOGO_URL = "https://dmy0b9oeprz0f.cloudfront.net/holidayextras.co.uk/brand-guidelines/logo-tags/png/deck-chair.png"

# ---------------------------------------------------------------------------
# Page config & CSS
# ---------------------------------------------------------------------------
st.set_page_config(page_title="HX Insurance Pulse — Weekly Market View", page_icon="🟣",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;600;700;800&display=swap');

    /* === HOLIDAY EXTRAS BRAND === */
    /* Apply Nunito to block-level text elements only — never touch spans (Streamlit icons use spans) */
    body, p, h1, h2, h3, h4, h5, h6, div, td, th, li, a, label, input, textarea, button {
        font-family: 'Nunito', system-ui, sans-serif !important;
    }
    .stApp { background-color: #0F0B18; }
    [data-testid="stHeader"] { background-color: #0F0B18; }
    [data-testid="stSidebar"] { background-color: #1A1528; }
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    /* Scroll section markers — hidden, used by JS to find sections */
    .scroll-section-marker { display: none; }

    /* === SCROLL REVEAL — clean, one-shot fade + rise === */
    .scroll-section {
        opacity: 0;
        transform: translateY(24px);
        transition: opacity 0.5s cubic-bezier(0.25, 0.46, 0.45, 0.94),
                    transform 0.5s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        padding: 2.5rem 0;
        margin: 0.5rem 0;
        border-bottom: 1px solid rgba(45, 36, 69, 0.3);
    }
    .scroll-section:last-child { border-bottom: none; }
    .scroll-section.revealed {
        opacity: 1;
        transform: translateY(0);
    }

    /* 3-tier stagger: header → body → supplemental */
    .scroll-section .section-header,
    .scroll-section .section-row,
    .scroll-section .section-image,
    .scroll-section .section-summary,
    .scroll-section .section-content,
    .scroll-section .ai-insight,
    .scroll-section .ai-loading {
        opacity: 0;
        transform: translateY(16px);
        transition: opacity 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94),
                    transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }
    .scroll-section.revealed .section-header {
        opacity: 1; transform: translateY(0); transition-delay: 0s;
    }
    .scroll-section.revealed .section-row,
    .scroll-section.revealed .section-image,
    .scroll-section.revealed .section-summary,
    .scroll-section.revealed .section-content {
        opacity: 1; transform: translateY(0); transition-delay: 0.06s;
    }
    .scroll-section.revealed .ai-insight,
    .scroll-section.revealed .ai-loading {
        opacity: 1; transform: translateY(0); transition-delay: 0.12s;
    }

    /* Progress dot nav */
    .scroll-nav {
        position: fixed; right: 18px; top: 50%; transform: translateY(-50%);
        z-index: 100; display: flex; flex-direction: column; gap: 10px;
        padding: 8px 4px; opacity: 0; transition: opacity 0.4s ease;
    }
    .scroll-nav.show { opacity: 1; }
    .scroll-nav .nav-dot {
        width: 6px; height: 6px; border-radius: 50%;
        background: rgba(155, 114, 207, 0.25);
        transition: background 0.3s ease, transform 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        cursor: pointer;
    }
    .scroll-nav .nav-dot.active {
        background: #9B72CF; transform: scale(1.35);
    }
    .scroll-nav .nav-dot:hover {
        background: rgba(155, 114, 207, 0.5); transform: scale(1.25);
    }

    /* Reduced motion */
    @media (prefers-reduced-motion: reduce) {
        .scroll-section { transition: none; opacity: 1; transform: none; }
        .scroll-section .section-header,
        .scroll-section .section-row,
        .scroll-section .section-image,
        .scroll-section .section-summary,
        .scroll-section .section-content,
        .scroll-section .ai-insight,
        .scroll-section .ai-loading { transition: none; opacity: 1; transform: none; }
    }

    /* Hero top area — generous breathing room */
    .hero-area {
        padding: 2rem 0 3rem 0;
        margin-bottom: 1rem;
    }

    /* Metric cards */
    .metric-card {
        background: #1A1528; border-radius: 10px; padding: 1.25rem 1.5rem;
        border: 1px solid #2D2445;
        box-shadow: 0 1px 3px rgba(0,0,0,0.3), 0 4px 12px rgba(0,0,0,0.15);
        transition: border-color 0.25s ease, transform 0.25s ease, box-shadow 0.25s ease;
    }
    .metric-card:hover {
        border-color: #542E91;
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(84,46,145,0.2), 0 8px 24px rgba(0,0,0,0.25);
    }
    .metric-label {
        font-size: 0.75rem; color: #B0A8C0; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.35rem;
    }
    .metric-value { font-size: 2.2rem; font-weight: 800; color: #F0F2F6; line-height: 1.1; letter-spacing: -0.02em; }
    .metric-delta-up { font-size: 0.9rem; color: #00B0A6; font-weight: 700; margin-top: 0.15rem; }
    .metric-delta-down { font-size: 0.9rem; color: #FF5F68; font-weight: 700; margin-top: 0.15rem; }
    .metric-subtitle { font-size: 0.9rem; color: #9AA3B5; margin-top: 0.25rem; }

    /* Section headers */
    .section-header {
        font-size: 0.85rem; color: #9B72CF; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.14em; margin: 0 0 1.25rem 0; padding-bottom: 0;
        border-bottom: none;
    }
    .chart-explainer { font-size: 0.95rem; color: #9AA3B5; line-height: 1.7; margin: -0.25rem 0 1rem 0; }

    /* AI insight boxes */
    .ai-insight {
        background: #140F20; border-left: 4px solid #542E91; border-radius: 0 10px 10px 0;
        padding: 1.25rem 1.5rem; margin: 1.25rem 0; font-size: 0.95rem;
        color: #F0F2F6; line-height: 1.75; box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        transition: box-shadow 0.3s ease;
    }
    .ai-insight:hover {
        box-shadow: 0 4px 20px rgba(0,0,0,0.4), 0 0 0 1px rgba(84,46,145,0.15);
    }
    .ai-insight .ai-label {
        font-size: 0.8rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.1em; margin-bottom: 0.5rem;
        display: flex; align-items: center; gap: 0.4rem;
    }
    .ai-insight .ai-label .dot {
        width: 8px; height: 8px; border-radius: 50%; display: inline-block;
    }
    .insight-box {
        background: #1A1528; border-left: 4px solid #542E91; border-radius: 0 10px 10px 0;
        padding: 1.25rem 1.5rem; margin: 1rem 0; font-size: 0.95rem;
        color: #F0F2F6; line-height: 1.75;
    }
    .insight-box .label {
        font-size: 0.8rem; color: #9B72CF; text-transform: uppercase;
        letter-spacing: 0.08em; font-weight: 700; margin-bottom: 0.4rem;
    }

    /* Methodology */
    .methodology {
        font-size: 0.9rem; color: #B0A8C0; line-height: 1.7;
        background: #140F20; border-radius: 10px; padding: 1.5rem; margin: 1rem 0;
    }
    .methodology strong { color: #F0F2F6; }

    /* Hide Streamlit chrome */
    #MainMenu { visibility: hidden; } footer { visibility: hidden; }
    [data-testid="stStatusWidget"] { visibility: hidden; }

    /* Expander — nuclear fix: hide Streamlit's icon text, use CSS chevron */
    [data-testid="stExpander"] details {
        border-color: #2D2445 !important;
        background: #140F20 !important;
        border-radius: 12px !important;
        margin: 0.75rem 0 !important;
        transition: border-color 0.3s ease !important;
    }
    [data-testid="stExpander"] details:hover {
        border-color: #542E91 !important;
    }
    [data-testid="stExpander"] summary {
        font-family: 'Nunito', system-ui, sans-serif !important;
        font-weight: 600 !important;
        color: #B0A8C0 !important;
        padding: 0.75rem 1rem !important;
    }
    /* Hide the Material Symbols icon text completely */
    [data-testid="stExpanderToggleIcon"],
    [data-testid="stExpander"] summary [data-testid="stExpanderToggleIcon"] {
        font-size: 0 !important;
        width: 20px; height: 20px;
        display: inline-flex !important;
        align-items: center; justify-content: center;
        position: relative;
    }
    /* Replace with a pure CSS chevron */
    [data-testid="stExpanderToggleIcon"]::after {
        content: '›';
        font-size: 1.4rem !important;
        font-family: system-ui, sans-serif !important;
        color: #9B72CF;
        display: block;
        transition: transform 0.3s ease;
    }
    [data-testid="stExpander"] details[open] [data-testid="stExpanderToggleIcon"]::after {
        transform: rotate(90deg);
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
    .stTabs [data-baseweb="tab"] { font-size: 0.95rem; font-weight: 600; color: #B0A8C0; }
    .stTabs [aria-selected="true"] { color: #9B72CF; }

    /* Focus */
    *:focus-visible { outline: 2px solid #542E91; outline-offset: 2px; }

    /* Buttons — HX purple */
    .stButton > button[kind="secondary"] {
        border: 1px solid #542E91 !important; color: #9B72CF !important;
        font-weight: 700 !important; letter-spacing: 0.02em;
        transition: all 0.2s cubic-bezier(0.4,0,0.2,1) !important;
    }
    .stButton > button[kind="secondary"]:hover {
        background: rgba(84,46,145,0.15) !important; border-color: #9B72CF !important;
        color: #F0F2F6 !important;
    }
    .stButton > button:active { transform: scale(0.97) !important; }

    /* Recommendation banner — kept for backward compat but hidden, merged into headline */
    .reco-banner { display: none; }

    /* Deep dive */
    .deep-dive-box {
        background: #140F20; border: 1px dashed #542E91; border-radius: 10px;
        padding: 1.25rem 1.5rem; margin: 1rem 0;
    }
    .deep-dive-box .dd-title { font-size: 1rem; color: #9B72CF; font-weight: 700; margin-bottom: 0.5rem; }
    .deep-dive-box .dd-desc { font-size: 0.9rem; color: #B0A8C0; line-height: 1.6; margin-bottom: 0.75rem; }

    /* Primary action buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #542E91 0%, #7B52C1 100%) !important;
        color: white !important; border: none !important;
        font-weight: 700 !important; border-radius: 10px !important;
        box-shadow: 0 4px 14px rgba(84,46,145,0.3);
        transition: all 0.3s cubic-bezier(0.4,0,0.2,1) !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #7B52C1 0%, #9B72CF 100%) !important;
        box-shadow: 0 6px 20px rgba(84,46,145,0.4);
        transform: translateY(-1px);
    }

    /* Channel cards */
    .channel-card {
        background: #1A1528; border-radius: 10px; padding: 1.25rem;
        border: 1px solid #2D2445; margin-bottom: 0.5rem;
    }
    .channel-card .ch-name { font-size: 1rem; font-weight: 700; margin-bottom: 0.25rem; }
    .channel-card .ch-desc { font-size: 0.85rem; color: #9AA3B5; }

    /* News */
    .news-section {
        background: #140F20; border-radius: 10px; padding: 1.5rem;
        border: 1px solid #2D2445; margin: 1rem 0;
    }
    .news-section .news-title { font-size: 1rem; color: #FDDC06; font-weight: 700; margin-bottom: 0.75rem; }

    /* Priority badge */
    .priority-badge {
        display: inline-block; font-size: 0.7rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.1em; padding: 0.15rem 0.5rem; border-radius: 4px; margin-left: 0.5rem;
    }

    /* Section row — image beside content on desktop */
    .section-row {
        display: flex; gap: 1.25rem; align-items: flex-start; margin: 0.5rem 0 0.75rem 0;
    }
    .section-image {
        flex-shrink: 0; width: 33.33%; aspect-ratio: 2 / 1;
        border-radius: 10px; overflow: hidden;
        border: 1px solid #2D2445;
    }
    .section-image img {
        width: 100%; height: 100%; object-fit: cover; display: block;
        opacity: 0.55; filter: saturate(0.7);
        transition: opacity 0.3s ease, transform 0.5s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        transform: translateY(0px) scale(1.03);
    }
    .section-image:hover img { opacity: 0.75; }
    .section-content { flex: 1; min-width: 0; }
    @media (max-width: 768px) {
        .section-row { flex-direction: column; }
        .section-image { width: 100%; aspect-ratio: 3 / 1; }
    }

    /* Section loading shimmer */
    @keyframes shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }
    .section-loading {
        background: linear-gradient(90deg, #1A1528 25%, #2D2445 50%, #1A1528 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s ease-in-out infinite;
        border-radius: 10px; height: 80px; margin: 1rem 0;
    }
    .section-loading-text {
        text-align: center; color: #9AA3B5; font-size: 0.85rem; padding-top: 0.5rem;
        animation: pulse-glow 1.5s ease-in-out infinite;
    }

    /* AI loading skeleton */
    @keyframes ai-shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }
    @keyframes ai-dot-pulse {
        0%, 80%, 100% { opacity: 0.2; transform: scale(0.8); }
        40% { opacity: 1; transform: scale(1); }
    }
    @keyframes ai-label-pulse {
        0%, 100% { opacity: 0.5; }
        50% { opacity: 1; }
    }
    .ai-loading {
        background: #140F20;
        border-left: 4px solid #542E91;
        border-radius: 0 10px 10px 0;
        padding: 1.25rem 1.5rem;
        margin: 1.25rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    .ai-loading .ai-loading-label {
        font-size: 0.8rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.1em; margin-bottom: 0.75rem;
        display: flex; align-items: center; gap: 0.5rem;
        color: #9B72CF; animation: ai-label-pulse 2s ease-in-out infinite;
    }
    .ai-loading .ai-loading-dots { display: flex; gap: 4px; align-items: center; }
    .ai-loading .ai-loading-dots span {
        width: 6px; height: 6px; border-radius: 50%; background: #9B72CF;
        display: inline-block; animation: ai-dot-pulse 1.4s ease-in-out infinite;
    }
    .ai-loading .ai-loading-dots span:nth-child(2) { animation-delay: 0.2s; }
    .ai-loading .ai-loading-dots span:nth-child(3) { animation-delay: 0.4s; }
    .ai-loading .ai-shimmer-line {
        height: 12px; border-radius: 6px; margin-bottom: 0.6rem;
        background: linear-gradient(90deg, #1A1528 25%, #2D2445 37%, #3D2F5A 50%, #2D2445 63%, #1A1528 75%);
        background-size: 400% 100%; animation: ai-shimmer 1.8s ease-in-out infinite;
    }
    .ai-loading .ai-shimmer-line:nth-child(1) { width: 92%; }
    .ai-loading .ai-shimmer-line:nth-child(2) { width: 78%; animation-delay: 0.15s; }
    .ai-loading .ai-shimmer-line:nth-child(3) { width: 65%; animation-delay: 0.3s; margin-bottom: 0; }

    /* Loading screen */
    @keyframes pulse-glow {
        0%, 100% { opacity: 0.4; }
        50% { opacity: 1; }
    }
    @keyframes slide-in {
        from { transform: translateX(-8px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes bar-fill {
        from { width: 0%; }
        to { width: 100%; }
    }
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(16px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .loading-screen {
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        min-height: 60vh; text-align: center;
    }
    .loading-logo {
        display: flex; align-items: center; justify-content: center; gap: 0.75rem;
        margin-bottom: 0.5rem;
    }
    .loading-logo img {
        width: 56px; height: 56px; object-fit: contain;
    }
    .loading-logo-text {
        font-size: 2.8rem; font-weight: 800; letter-spacing: -0.02em;
        background: linear-gradient(135deg, #542E91 0%, #9B72CF 50%, #FDDC06 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .loading-subtitle {
        font-size: 1rem; color: #9AA3B5; font-weight: 400; margin-bottom: 2.5rem;
        letter-spacing: 0.04em;
    }
    .loading-steps {
        display: flex; flex-direction: column; gap: 0.75rem; width: 320px; text-align: left;
    }
    .loading-step {
        display: flex; align-items: center; gap: 0.75rem;
        font-size: 0.9rem; color: #B0A8C0; animation: slide-in 0.4s ease both;
    }
    .loading-step .step-dot {
        width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
        background: #2D2445; transition: background 0.3s;
    }
    .loading-step.active .step-dot { background: #542E91; animation: pulse-glow 1.2s ease-in-out infinite; }
    .loading-step.done .step-dot { background: #00B0A6; }
    .loading-step.done { color: #F0F2F6; }
    .loading-bar-wrap {
        width: 320px; height: 3px; background: #1A1528; border-radius: 2px;
        margin-top: 2rem; overflow: hidden;
    }
    .loading-bar {
        height: 100%; border-radius: 2px;
        background: linear-gradient(90deg, #542E91, #9B72CF, #FDDC06);
        animation: bar-fill 8s ease-out forwards;
    }

    /* Section summary */
    .section-summary {
        font-size: 1.15rem; color: #F0F2F6; font-weight: 400; line-height: 1.7;
        margin: 0.5rem 0 1rem 0; padding: 0;
        background: none; border-radius: 0; border-left: none;
    }
    .section-summary strong { font-weight: 700; }
    .section-summary .up { color: #00B0A6; }
    .section-summary .down { color: #FF5F68; }
    .section-summary .neutral { color: #FFB55F; }

    /* Top headline — hero style */
    .top-headline {
        background: none;
        border: none; border-radius: 0;
        padding: 3rem 0 2rem 0; margin: 0; text-align: center;
    }
    .top-headline .hl-main {
        font-size: 2rem; font-weight: 800; color: #F0F2F6; line-height: 1.35;
        margin-bottom: 1rem; letter-spacing: -0.02em;
    }
    .top-headline .hl-sub {
        font-size: 1.05rem; color: #B0A8C0; line-height: 1.7; max-width: 640px; margin: 0 auto;
    }
    .top-headline .hl-sub strong { color: #F0F2F6; }
    .top-headline .up { color: #00B0A6; } .top-headline .down { color: #FF5F68; }
    .top-headline .neutral { color: #FFB55F; }
    .top-headline .hl-reco {
        display: inline-block; margin-top: 1.25rem; padding: 0.5rem 1.25rem;
        background: rgba(84,46,145,0.12); border: 1px solid rgba(84,46,145,0.25);
        border-radius: 8px; font-size: 0.9rem; color: #B0A8C0; line-height: 1.5;
    }
    .top-headline .hl-reco strong { color: #FDDC06; }

    /* Gap YoY change badge */
    .gap-change-badge {
        display: inline-block; padding: 0.2rem 0.6rem; border-radius: 6px;
        font-size: 0.85rem; font-weight: 600; margin-left: 0.5rem;
    }
    .gap-change-badge.widened { background: rgba(0,176,166,0.15); color: #00B0A6; }
    .gap-change-badge.narrowed { background: rgba(255,95,104,0.15); color: #FF5F68; }
    .gap-change-badge.flipped { background: rgba(220,38,127,0.15); color: #DC267F; }
    .gap-change-badge.stable { background: rgba(141,153,174,0.15); color: #8D99AE; }

    /* Entrance animations — metric cards stagger on load, sections use scroll observer */
    .metric-card {
        animation: fadeInUp 0.5s ease-out both;
    }
    /* Stagger metric cards */
    [data-testid="stHorizontalBlock"] > div:nth-child(1) .metric-card { animation-delay: 0.1s; }
    [data-testid="stHorizontalBlock"] > div:nth-child(2) .metric-card { animation-delay: 0.2s; }
    [data-testid="stHorizontalBlock"] > div:nth-child(3) .metric-card { animation-delay: 0.3s; }
    [data-testid="stHorizontalBlock"] > div:nth-child(4) .metric-card { animation-delay: 0.4s; }

    /* Chart containers */
    [data-testid="stPlotlyChart"] {
        border-radius: 10px; overflow: hidden;
        border: 1px solid rgba(45, 36, 69, 0.5);
        transition: border-color 0.3s ease;
    }
    [data-testid="stPlotlyChart"]:hover {
        border-color: rgba(84,46,145,0.3);
    }

    /* Custom scrollbar */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #0F0B18; }
    ::-webkit-scrollbar-thumb { background: #2D2445; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #3D3455; }

    /* Brand bar — minimal, top-aligned */
    .hx-brand-bar {
        display: flex; align-items: center; gap: 0.6rem;
        padding: 0; margin: 0;
    }
    .hx-brand-bar .hx-logo {
        display: flex; align-items: center; gap: 0.4rem;
        font-size: 1rem; font-weight: 700; letter-spacing: -0.01em;
        color: #B0A8C0;
    }
    .hx-brand-bar .hx-logo img {
        width: 24px; height: 24px; object-fit: contain;
    }
    .hx-brand-bar .hx-divider {
        width: 1px; height: 16px; background: #2D2445;
    }
    .hx-brand-bar .hx-title {
        font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.12em; color: #9B72CF;
    }
    .hx-brand-bar .hx-date {
        margin-left: auto; font-size: 0.75rem; color: #9AA3B5; font-weight: 400;
    }

    @media (max-width: 768px) {
        .metric-card { padding: 1rem; }
        .metric-value { font-size: 1.8rem; }
        .top-headline { padding: 2rem 0 1.5rem 0; }
        .top-headline .hl-main { font-size: 1.4rem; }
        .loading-steps, .loading-bar-wrap { width: 260px; }
        .section-row { flex-direction: column; }
        .section-image { width: 100%; aspect-ratio: 3 / 1; }
        .scroll-section { padding: 1.5rem 0; }
        .scroll-section .section-header,
        .scroll-section .section-row,
        .scroll-section .section-image,
        .scroll-section .section-summary,
        .scroll-section .section-content,
        .scroll-section .ai-insight,
        .scroll-section .ai-loading { transition-delay: 0s !important; }
        .scroll-nav { display: none; }
    }
</style>
""", unsafe_allow_html=True)

# Inject scroll engine via st.html (st.markdown strips <script> tags)
st.html("""
<script>
(function() {
    'use strict';
    const doc = window.parent.document;

    // Dot nav
    if (doc.querySelector('.scroll-nav')) return; // already injected
    const nav = doc.createElement('div');
    nav.className = 'scroll-nav';
    doc.body.appendChild(nav);

    // Observe scroll-section containers (found via marker spans)
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('revealed');
                observer.unobserve(entry.target);
            }
        });
        updateNav();
    }, { threshold: 0.15, rootMargin: '0px 0px -60px 0px' });

    // Active section tracking for dot nav
    let activeDot = -1;
    function updateNav() {
        const sections = doc.querySelectorAll('.scroll-section');
        const vh = window.parent.innerHeight;
        let bestIdx = -1, bestDist = Infinity;
        sections.forEach((s, i) => {
            const rect = s.getBoundingClientRect();
            const dist = Math.abs(rect.top + rect.height / 2 - vh * 0.45);
            if (rect.top < vh && rect.bottom > 0 && dist < bestDist) {
                bestDist = dist; bestIdx = i;
            }
        });
        if (bestIdx !== activeDot) {
            activeDot = bestIdx;
            nav.querySelectorAll('.nav-dot').forEach((d, i) => {
                d.classList.toggle('active', i === bestIdx);
            });
        }
    }

    // Subtle image parallax only (single layer, 12px)
    let ticking = false;
    function updateParallax() {
        const vh = window.parent.innerHeight;
        doc.querySelectorAll('.scroll-section.revealed .section-image img').forEach(img => {
            const rect = img.getBoundingClientRect();
            const ratio = Math.max(-1, Math.min(1, (rect.top + rect.height/2 - vh/2) / (vh/2)));
            img.style.transform = 'translateY(' + (ratio * 12) + 'px) scale(1.03)';
        });
        ticking = false;
    }
    window.parent.addEventListener('scroll', function() {
        if (!ticking) { ticking = true; requestAnimationFrame(updateParallax); }
        updateNav();
    }, { passive: true });

    // Find marker spans, apply scroll-section class to their nearest stVerticalBlock parent
    let lastCount = 0;
    function scan() {
        doc.querySelectorAll('.scroll-section-marker').forEach(marker => {
            // Walk up to find the st.container's vertical block wrapper
            let el = marker.closest('[data-testid="stVerticalBlock"]');
            if (!el) el = marker.parentElement;
            // Walk one more level up to get the container wrapper (not the inner block)
            if (el && el.parentElement && el.parentElement.getAttribute('data-testid') !== 'stAppViewBlockContainer') {
                el = el.parentElement;
            }
            if (el && !el.classList.contains('scroll-section')) {
                el.classList.add('scroll-section');
            }
        });
        doc.querySelectorAll('.scroll-section:not(.scroll-observed)').forEach(el => {
            el.classList.add('scroll-observed');
            observer.observe(el);
        });
        const all = doc.querySelectorAll('.scroll-section');
        if (all.length !== lastCount) {
            lastCount = all.length;
            nav.innerHTML = '';
            all.forEach((section) => {
                const dot = doc.createElement('div');
                dot.className = 'nav-dot';
                dot.addEventListener('click', () => {
                    section.scrollIntoView({ behavior: 'smooth', block: 'center' });
                });
                nav.appendChild(dot);
            });
        }
        const shouldShow = window.parent.scrollY > 300;
        nav.classList.toggle('show', shouldShow);
    }
    scan();
    new MutationObserver(() => requestAnimationFrame(scan)).observe(doc.body, { childList: true, subtree: true });
})();
</script>
""", unsafe_allow_javascript=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner="Loading pipeline data...")
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
        except Exception:
            pass
    return sources


def build_weekly_trends(sources: dict) -> pd.DataFrame:
    gt = sources.get("google_trends")
    if gt is None or gt.empty:
        return pd.DataFrame()
    gt = gt.copy()
    gt["date"] = pd.to_datetime(gt["date"])
    holiday_terms = set(config.HOLIDAY_INTENT_TERMS)
    gt["category"] = gt["metric_name"].apply(lambda m: "holiday" if m in holiday_terms else "insurance")
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
    return weekly.sort_values("date").reset_index(drop=True)


def build_quarterly_summary(sources: dict) -> pd.DataFrame:
    all_dfs = [df for df in sources.values() if not df.empty]
    if not all_dfs:
        return pd.DataFrame()
    combined = pd.concat(all_dfs, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date", "normalised_value"])
    holiday_terms = set(config.HOLIDAY_INTENT_TERMS)
    insurance_terms = set(config.INSURANCE_INTENT_TERMS)
    metric_groups = {
        "Holiday Intent": combined[combined["metric_name"].isin(holiday_terms)],
        "Insurance Intent": combined[combined["metric_name"].isin(insurance_terms)],
        "UK Passengers": combined[combined["metric_name"] == "uk_terminal_passengers"],
        "Visits Abroad": combined[combined["metric_name"] == "uk_visits_abroad"],
        "Global Aviation": combined[combined["metric_name"] == "air_passengers_global"],
    }
    indices = {}
    for label, df in metric_groups.items():
        if df.empty:
            continue
        monthly = df.groupby("date")["normalised_value"].mean().reset_index()
        monthly.columns = ["date", "value"]
        ts = monthly.set_index("date")[["value"]].resample("QE").mean().dropna().reset_index()
        ts["quarter"] = ts["date"].dt.to_period("Q").astype(str)
        baseline = ts.loc[ts["date"].dt.year == BASELINE_YEAR, "value"].mean()
        if baseline and baseline > 0:
            ts["value"] = (ts["value"] / baseline) * 100
        indices[label] = ts[["quarter", "value"]].rename(columns={"value": label})
    if not indices:
        return pd.DataFrame()
    result = list(indices.values())[0]
    for idx_df in list(indices.values())[1:]:
        result = result.merge(idx_df, on="quarter", how="outer")
    result = result.sort_values("quarter").reset_index(drop=True)
    idx_cols = [c for c in result.columns if c != "quarter"]
    result["Combined"] = result[idx_cols].mean(axis=1)
    return result


@st.cache_data(ttl=86400, show_spinner="Loading competitor & price data...")
def load_extra_trends():
    from pytrends.request import TrendReq
    results = {}
    try:
        pytrends = TrendReq(hl="en-GB", tz=0)
        pytrends.build_payload(COMPETITOR_TERMS[:5], cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(3)
        comp = pytrends.interest_over_time()
        if not comp.empty:
            results["competitors"] = comp.drop(columns=["isPartial"], errors="ignore")
        pytrends.build_payload(PRICE_SENSITIVITY_TERMS[:5], cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(3)
        price = pytrends.interest_over_time()
        if not price.empty:
            results["price_sensitivity"] = price.drop(columns=["isPartial"], errors="ignore")
    except Exception:
        pass
    return results


@st.cache_data(ttl=86400, show_spinner="Loading Holiday Extras signals...")
def load_hx_trends():
    from pytrends.request import TrendReq
    results = {}
    try:
        pytrends = TrendReq(hl="en-GB", tz=0)
        pytrends.build_payload(PARKING_CROSSSELL[:3], cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(3)
        park = pytrends.interest_over_time()
        if not park.empty:
            results["parking"] = park.drop(columns=["isPartial"], errors="ignore")
        pytrends.build_payload(WHITE_LABEL_PARTNERS[:5], cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(3)
        wl = pytrends.interest_over_time()
        if not wl.empty:
            results["white_labels"] = wl.drop(columns=["isPartial"], errors="ignore")
    except Exception:
        pass
    return results


# ---------------------------------------------------------------------------
# Seasonal adjustment
# ---------------------------------------------------------------------------
def seasonal_adjust(weekly: pd.DataFrame, metric: str) -> pd.DataFrame:
    df = weekly.copy()
    if metric not in df.columns:
        return df
    df["week_num"] = df["date"].dt.isocalendar().week.astype(int)
    seasonal = df.groupby("week_num")[metric].mean()
    overall_mean = df[metric].mean()
    if overall_mean == 0:
        return df
    si = seasonal / overall_mean
    df = df.merge(si.rename("_si"), left_on="week_num", right_index=True, how="left")
    df[f"{metric}_sa"] = df[metric] / df["_si"]
    df = df.drop(columns=["week_num", "_si"])
    return df


def add_all_sa(weekly: pd.DataFrame) -> pd.DataFrame:
    df = weekly.copy()
    for col in ["combined", "holiday", "insurance"]:
        if col in df.columns:
            df = seasonal_adjust(df, col)
    return df


# ---------------------------------------------------------------------------
# Disk cache — survives restarts, shared across devices via Google Drive
# ---------------------------------------------------------------------------
import hashlib

# Set CACHE_DIR env var to a Google Drive folder for cross-device sharing,
# e.g. CACHE_DIR=/Users/jake.osmond/Google Drive/My Drive/hx-dashboard-cache
CACHE_DIR = Path(os.environ.get("CACHE_DIR", Path(__file__).resolve().parent / "_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "ai").mkdir(exist_ok=True)
(CACHE_DIR / "images").mkdir(exist_ok=True)

CACHE_TTL_SECS = 86400  # 24 hours


def _disk_cache_get(subfolder: str, key: str) -> str | bytes | None:
    """Read from disk cache if fresh (< 24h old)."""
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    path = CACHE_DIR / subfolder / h
    if path.exists():
        age = time.time() - path.stat().st_mtime
        if age < CACHE_TTL_SECS:
            if subfolder == "images":
                return path.read_bytes()
            return path.read_text(encoding="utf-8")
    return None


def _disk_cache_put(subfolder: str, key: str, value: str | bytes):
    """Write to disk cache."""
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    path = CACHE_DIR / subfolder / h
    if isinstance(value, bytes):
        path.write_bytes(value)
    else:
        path.write_text(value, encoding="utf-8")


# ---------------------------------------------------------------------------
# AI Engine
# ---------------------------------------------------------------------------
HX_STRATEGY_CONTEXT = """
HOLIDAY EXTRAS MARKETING STRATEGY (frame all advice against these priorities):
1. TAKE MARKET SHARE — Increase GWP, grow market share, drive profitable new customer acquisition, improve AMT and Medical mix
2. MAXIMISE CUSTOMER LIFETIME VALUE — Increase renewal rate, improve cross-sell (Insurance <-> Distribution), grow GP per customer, increase AMT penetration
3. DELIVER BRAND PROMISE — Reduce buying friction, improve trip capture (especially AMT), increase add-on attachment, move renewal beyond email-only, simplify claims/support
4. BUILD BRAND — Increase brand awareness, improve consideration within HX database, improve acquisition efficiency (CPA/CPC over time)

CHANNELS: Direct (parking cross-sell), PPC/SEO (new acquisition), White Labels (Carnival, Fred Olsen, retail partners), Aggregators (Compare the Market, CYTI)
"""

ANALYST_PROMPT = f"""You brief the Holiday Extras insurance team. They're busy. Every word must earn its place.

{HX_STRATEGY_CONTEXT}

RULES:
- Reply in plain English a 12-year-old could understand. Short sentences. No jargon.
- NEVER use asterisks, markdown formatting, or special symbols. Use <b> tags if you need bold.
- Never say "index", "SA", "normalised", "basis points". Say "up 15% vs last year".
- All data is Google search volume, NOT sales. More searches does not mean more HX customers. Say how to CAPTURE demand.
- Name specifics: airlines, dates, destinations, news events. Vague is useless.
- End with ONE action: who does what, which channel, by when.
- MAX 280 characters total. Tweet-length. No filler. No preamble."""


_BAD_RESPONSE_MARKERS = [
    "i don't have live web access",
    "i don't have live access",
    "i can't browse",
    "i'm unable to browse",
    "enable browsing",
    "i cannot access",
    "i don't have access to real-time",
    "i don't have the ability to browse",
    "can't reliably pull",
    "without risking inaccuracies",
    "if you can enable browsing",
    "i'm not able to search",
    "i can't search the web",
    "i don't have internet access",
    "i cannot browse",
    "unable to access the web",
    "i can't access the internet",
    "share links/articles",
]


def _is_bad_response(text: str) -> bool:
    """Check if AI response indicates it couldn't use web search."""
    lower = text.lower()[:300]
    return any(marker in lower for marker in _BAD_RESPONSE_MARKERS)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_ai(cache_key: str, system: str, user: str) -> str:
    """AI call with 7-day disk + memory cache. Shared across devices if CACHE_DIR is on Google Drive."""
    disk_key = f"{cache_key}|{system[:80]}|{user[:80]}"
    cached = _disk_cache_get("ai", disk_key)
    if cached and not _is_bad_response(cached):
        return cached
    result = _call_with_web_search_uncached(system, user)
    if result and not _is_bad_response(result):
        _disk_cache_put("ai", disk_key, result)
        return result
    # Bad or empty response — return empty so callers skip rendering
    return ""


def _get_client():
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return None
    return OpenAI(api_key=api_key)


def _call_with_web_search_uncached(system: str, user: str) -> str:
    """Call OpenAI with web search enabled. Tries Responses API first, then fallbacks."""
    client = _get_client()
    if not client:
        return "AI insights unavailable -- add OPENAI_API_KEY to your .env file."

    # Track which level we used (for debugging in sidebar)
    level_used = "none"

    # Attempt 1: Responses API with web_search_preview (best — has real web access)
    try:
        resp = client.responses.create(
            model="gpt-5",
            tools=[{"type": "web_search_preview"}],
            instructions=system,
            input=user,
        )
        # Use output_text accessor — handles all response item types cleanly
        result = getattr(resp, "output_text", "") or ""
        if result and not _is_bad_response(result):
            level_used = "responses_api_web"
            if "ai_levels" not in st.session_state:
                st.session_state["ai_levels"] = []
            st.session_state["ai_levels"].append(level_used)
            return result
    except Exception:
        pass

    # Attempt 3: Plain chat completions (no web search — last resort)
    try:
        resp = client.chat.completions.create(
            model="gpt-5",
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_completion_tokens=4000)
        content = resp.choices[0].message.content or ""
        # Even last resort must pass the bad response check
        if _is_bad_response(content):
            return ""
        level_used = "chat_plain"
        if "ai_levels" not in st.session_state:
            st.session_state["ai_levels"] = []
        st.session_state["ai_levels"].append(level_used)
        return content
    except Exception:
        return ""


def _call_openai(question: str) -> str:
    """Main AI call — uses web search, cached 24h."""
    return _cached_ai(question[:100], ANALYST_PROMPT, question)


def _call_openai_raw(system: str, user: str) -> str:
    """AI call with custom system prompt — uses web search, cached 24h."""
    return _cached_ai(f"{system[:50]}_{user[:50]}", system, user)


def _call_openai_fresh(question: str) -> str:
    """Uncached AI call for on-demand buttons."""
    return _call_with_web_search_uncached(ANALYST_PROMPT, question)


def _call_openai_with_timeout(question: str, timeout_secs: int = 10) -> str | None:
    """Call AI with a timeout. Returns None if it takes too long."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_call_openai, question)
        try:
            return future.result(timeout=timeout_secs)
        except FuturesTimeout:
            return None


@st.cache_data(ttl=86400, show_spinner=False)
def _generate_section_image(cache_key: str, description: str) -> str | None:
    """Generate a photorealistic image via DALL-E 3. Cached to disk for cross-device sharing."""
    import base64
    import requests as _req

    # Check disk cache first
    disk_key = f"img|{cache_key}|{description[:60]}"
    cached_bytes = _disk_cache_get("images", disk_key)
    if cached_bytes:
        b64 = base64.b64encode(cached_bytes).decode()
        return f"data:image/png;base64,{b64}"

    client = _get_client()
    if not client:
        return None
    try:
        resp = client.images.generate(
            model="dall-e-3",
            prompt=(f"Photorealistic, high quality editorial photograph for a business dashboard. "
                    f"Clean, modern, professional. NO text, NO charts, NO numbers, NO words in the image. "
                    f"Subject: {description}. "
                    f"Style: soft natural lighting, shallow depth of field, magazine quality."),
            size="1792x1024",  # landscape ratio — displayed as thin 120px banner via CSS crop
            quality="standard",
            n=1,
        )
        url = resp.data[0].url
        img_bytes = _req.get(url, timeout=15).content
        # Save to disk cache for cross-device sharing
        _disk_cache_put("images", disk_key, img_bytes)
        b64 = base64.b64encode(img_bytes).decode()
        return f"data:image/png;base64,{b64}"
    except Exception:
        return None


def deep_dive_investigation(context: str) -> str:
    """3-step investigation: AI topics -> Google Trends -> AI narrative."""
    from pytrends.request import TrendReq

    topic_prompt = """You are a market research analyst. Given the data below about the UK travel
insurance market, suggest exactly 5 Google search terms that would help explain WHY
the market is behaving this way.

IMPORTANT: These terms will be searched on Google Trends. They must be SHORT (2-3 words max)
and POPULAR enough that real people actually search for them. Do NOT include years or dates.

Good examples: "cheap flights", "easyjet", "spain holiday", "cost of living", "passport renewal"
Bad examples: "UK airline news February 2026", "impact of inflation on travel 2026"

Return ONLY a JSON array of 5 terms, nothing else. Example:
["easyjet", "spain holiday", "cost of living", "passport renewal", "ryanair"]"""

    topics_raw = _call_openai_raw(topic_prompt, f"DATA:\n{context}")
    if not topics_raw:
        return "Could not generate investigation topics."

    try:
        cleaned = topics_raw.replace("```json", "").replace("```", "").strip()
        start = cleaned.find("[")
        end = cleaned.rfind("]") + 1
        if start == -1 or end == 0:
            return "AI did not return valid search topics."
        topics = json.loads(cleaned[start:end])
        if not isinstance(topics, list) or len(topics) == 0:
            return "AI did not return valid search topics."
        topics = [str(t).strip() for t in topics[:5]]
    except (json.JSONDecodeError, ValueError):
        return f"Could not parse AI topics: {topics_raw}"

    trends_results = []
    try:
        pytrends = TrendReq(hl="en-GB", tz=0)
        pytrends.build_payload(topics, cat=0, timeframe="today 12-m", geo="GB")
        time.sleep(2)
        interest = pytrends.interest_over_time()
        if not interest.empty:
            for topic in topics:
                if topic in interest.columns:
                    recent_avg = interest[topic].tail(4).mean()
                    earlier_avg = interest[topic].head(4).mean()
                    change = ((recent_avg - earlier_avg) / earlier_avg * 100) if earlier_avg > 0 else 0
                    current = int(interest[topic].iloc[-1])
                    peak = int(interest[topic].max())
                    trends_results.append(
                        f'"{topic}": current interest {current}/100, '
                        f'peak was {peak}/100, trending {change:+.0f}% over the last 12 months')
                else:
                    trends_results.append(f'"{topic}": no data found')
        else:
            trends_results = [f'"{t}": Google Trends returned no data' for t in topics]
    except Exception as e:
        trends_results = [f"Google Trends search failed: {e}"]

    trends_summary = "\n".join(trends_results)

    narrative_prompt = f"""Brief Holiday Extras on what's driving the UK travel insurance market RIGHT NOW.
{HX_STRATEGY_CONTEXT}

RULES:
- Reply in plain English a 12-year-old could understand. Short punchy sentences.
- NEVER use asterisks, markdown formatting, or special symbols. Use <b> tags if you need bold.
- Name specifics: airlines, destinations, events, dates. No vague statements.
- Data is Google search volume, NOT sales. Say how HX can capture the opportunity.
- End with ONE thing to watch or do next week.
- MAX 400 characters total. No bullet points. No jargon. No filler."""

    narrative_input = (
        f"MARKET DATA:\n{context}\n\n"
        f"INVESTIGATION TOPICS: {', '.join(topics)}\n\n"
        f"GOOGLE TRENDS RESULTS (UK, last 12 months):\n{trends_summary}")

    narrative = _call_openai_raw(narrative_prompt, narrative_input)
    if not narrative:
        return f"Topics: {', '.join(topics)}\n\nTrends:\n{trends_summary}\n\n(AI narrative failed)"

    return (
        f"<strong>Topics investigated:</strong> {', '.join(topics)}<br><br>"
        f"<strong>What Google Trends shows:</strong><br>"
        f"{'<br>'.join(trends_results)}<br><br>"
        f"<strong>The story:</strong><br>{narrative}")


def build_context(weekly: pd.DataFrame, sources: dict) -> str:
    if weekly.empty:
        return ""
    sa = add_all_sa(weekly)
    latest = sa.iloc[-1]
    d = latest["date"].strftime("%d %B %Y")
    c = float(latest.get("combined_sa", latest.get("combined", 0)))
    h = float(latest.get("holiday_sa", latest.get("holiday", 0)))
    ins = float(latest.get("insurance_sa", latest.get("insurance", 0)))
    lines = [
        f"Date: week ending {d}",
        f"(All numbers are seasonally adjusted -- calendar effects removed)",
        f"Overall demand (SA): {c:.0f}",
        f"Holiday search interest (SA): {h:.0f}",
        f"Insurance search interest (SA): {ins:.0f}",
        f"Gap: insurance is {ins - h:+.0f} points vs holiday",
    ]
    if len(sa) > 52:
        ago = sa.iloc[-53]
        pc = float(ago.get("combined_sa", ago.get("combined", c)))
        yoy_val = ((c - pc) / pc * 100) if pc else 0
        lines.append(f"vs same period last year: {yoy_val:+.1f}% (was {pc:.0f}, now {c:.0f})")
    if len(sa) > 4:
        p4 = float(sa.iloc[-5].get("combined_sa", sa.iloc[-5].get("combined", c)))
        wow_val = ((c - p4) / p4 * 100) if p4 else 0
        lines.append(f"4-week change: {wow_val:+.1f}%")
    recent = sa.tail(8)
    trend = " -> ".join(
        f"{r['date'].strftime('%b %Y')}: {float(r.get('combined_sa', r.get('combined', 0))):.0f}"
        for _, r in recent.iterrows())
    lines.append(f"Recent trend (SA): {trend}")
    gt = sources.get("google_trends")
    if gt is not None and not gt.empty:
        gt = gt.copy()
        gt["date"] = pd.to_datetime(gt["date"])
        last = gt[gt["date"] >= gt["date"].max() - pd.Timedelta(days=60)]
        prev = gt[(gt["date"] >= gt["date"].max() - pd.Timedelta(days=120)) &
                  (gt["date"] < gt["date"].max() - pd.Timedelta(days=60))]
        if not prev.empty:
            la = last.groupby("metric_name")["normalised_value"].mean()
            pa = prev.groupby("metric_name")["normalised_value"].mean()
            lines.append("\nSearch term changes (last 2 months vs prior 2 months):")
            for t in la.index:
                if t in pa.index and pa[t] > 0:
                    ch = (la[t] - pa[t]) / pa[t] * 100
                    lines.append(f'  "{t}": {ch:+.0f}%')
    return "\n".join(lines)


def build_full_context(ctx: str, extra_trends: dict, hx_trends: dict) -> str:
    """Enrich base context with all available signal data."""
    sections = [ctx]
    for label, key in [("Competitor brand searches", "competitors"),
                       ("Price sensitivity searches", "price_sensitivity")]:
        df = extra_trends.get(key)
        if df is not None and not df.empty:
            sections.append(f"\n{label.upper()} (Google Trends, last 12m):")
            for t in df.columns:
                r, e = df[t].tail(4).mean(), df[t].head(4).mean()
                ch = ((r - e) / e * 100) if e > 0 else 0
                sections.append(f"  {t}: current {r:.0f}/100, {ch:+.0f}% trend")
    for label, key in [("Airport parking searches (direct cross-sell signal)", "parking"),
                       ("White label partner brand searches", "white_labels")]:
        df = hx_trends.get(key)
        if df is not None and not df.empty:
            sections.append(f"\n{label.upper()}:")
            for t in df.columns:
                r, e = df[t].tail(4).mean(), df[t].head(4).mean()
                ch = ((r - e) / e * 100) if e > 0 else 0
                sections.append(f"  {t}: current {r:.0f}/100, {ch:+.0f}% trend")
    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------
def _hex_to_rgba(h: str, a: float) -> str:
    h = h.lstrip("#")
    return f"rgba({int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)},{a})"

CHART_LAYOUT = dict(
    template="plotly_dark", paper_bgcolor=BG_PRIMARY, plot_bgcolor=BG_PRIMARY,
    font=dict(family="Nunito, system-ui, sans-serif", color=TEXT_PRIMARY),
    hoverlabel=dict(bgcolor=BG_CARD, font_size=13, bordercolor=BG_CARD),
    xaxis=dict(gridcolor="#1E2230", showgrid=False, tickfont=dict(size=12, color=TEXT_SECONDARY)),
    yaxis=dict(gridcolor="#1E2230", gridwidth=1, tickfont=dict(size=12, color=TEXT_SECONDARY), title=None),
)


def _strip_markdown(text: str) -> str:
    """Remove markdown artifacts from AI responses, keeping HTML tags."""
    if not text:
        return text
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<b>\1</b>', text)
    text = text.replace('*', '')
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    text = text.replace('`', '')
    return text.strip()


def ai_box(label: str, content: str, color: str = BLUE) -> str:
    content = _strip_markdown(content)
    return f"""
    <div class="ai-insight" style="border-left-color: {color};" role="region" aria-label="{label}">
        <div class="ai-label" style="color: {color};">
            <span class="dot" style="background: {color};"></span> {label}
        </div>
        <div>{content}</div>
    </div>"""


def ai_loading_box(label: str, color: str = HX_PURPLE_LIGHT) -> str:
    """Skeleton loader that matches ai_box dimensions."""
    return f"""
    <div class="ai-loading" style="border-left-color: {color};">
        <div class="ai-loading-label" style="color: {color};">
            <div class="ai-loading-dots">
                <span style="background: {color};"></span>
                <span style="background: {color};"></span>
                <span style="background: {color};"></span>
            </div>
            {label}
        </div>
        <div class="ai-shimmer-line"></div>
        <div class="ai-shimmer-line"></div>
        <div class="ai-shimmer-line"></div>
    </div>"""


def make_trend_chart(weekly, show_holiday=True, show_insurance=True,
                     show_combined=True, date_range=None, use_sa=True):
    df = weekly.copy()
    if date_range:
        df = df[(df["date"] >= date_range[0]) & (df["date"] <= date_range[1])]
    h_col = "holiday_sa" if use_sa and "holiday_sa" in df.columns else "holiday"
    i_col = "insurance_sa" if use_sa and "insurance_sa" in df.columns else "insurance"
    c_col = "combined_sa" if use_sa and "combined_sa" in df.columns else "combined"
    fig = go.Figure()
    fig.add_vrect(x0="2020-03-01", x1="2021-06-30", fillcolor="#FFFFFF", opacity=0.03,
                  line_width=0, annotation_text="COVID period", annotation_position="top left",
                  annotation_font_color=TEXT_MUTED, annotation_font_size=11)
    if show_holiday and h_col in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df[h_col],
            name="Holiday searches", line=dict(color=BLUE, width=1.5), opacity=0.5,
            hovertemplate="Holiday: %{y:.0f}<extra></extra>"))
    if show_insurance and i_col in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df[i_col],
            name="Insurance searches", line=dict(color=TEAL, width=1.5), opacity=0.5,
            hovertemplate="Insurance: %{y:.0f}<extra></extra>"))
    if show_combined and c_col in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df[c_col],
            name="Overall demand", line=dict(color=GOLD, width=3),
            hovertemplate="Demand: %{y:.0f}<extra></extra>"))
    fig.update_layout(**CHART_LAYOUT, height=420, margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="top", y=1.14, xanchor="left", x=0,
                    font=dict(size=12, color=TEXT_SECONDARY), bgcolor="rgba(0,0,0,0)"),
        hovermode="x unified")
    return fig


def make_sparkline(values, color=BLUE, height=60):
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=values, mode="lines", line=dict(color=color, width=2),
        fill="tozeroy", fillcolor=_hex_to_rgba(color, 0.08), hoverinfo="skip"))
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)", height=height, margin=dict(l=0,r=0,t=0,b=0),
        xaxis=dict(visible=False), yaxis=dict(visible=False), showlegend=False)
    return fig


def make_quarterly_bars(summary):
    fig = go.Figure()
    cols = [c for c in summary.columns if c not in ("quarter", "Combined")]
    for i, col in enumerate(cols):
        fig.add_trace(go.Bar(x=summary["quarter"], y=summary[col], name=col,
            marker_color=PALETTE[i % len(PALETTE)], opacity=0.85,
            hovertemplate=f"{col}: %{{y:.0f}}<extra></extra>"))
    fig.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=0,r=0,t=10,b=0), barmode="group",
        legend=dict(orientation="h", yanchor="top", y=1.15, xanchor="left", x=0,
                    font=dict(size=12, color=TEXT_SECONDARY), bgcolor="rgba(0,0,0,0)"))
    fig.update_xaxes(gridcolor="#1E2230", showgrid=False,
                     tickfont=dict(size=11, color=TEXT_SECONDARY), tickangle=-45)
    return fig


def make_yoy_chart(weekly, use_sa=True):
    df = weekly.copy()
    col = "combined_sa" if use_sa and "combined_sa" in df.columns else "combined"
    if col not in df.columns or len(df) < 53:
        return go.Figure()
    df = df.set_index("date").sort_index()
    df["yoy"] = df[col].pct_change(periods=52) * 100
    df = df.dropna(subset=["yoy"]).reset_index()
    colors = [ACCENT_UP if v >= 0 else ACCENT_DOWN for v in df["yoy"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["yoy"], marker_color=colors, showlegend=False,
        hovertemplate="%{y:+.1f}% vs last year<extra></extra>"))
    fig.add_hline(y=0, line_color=TEXT_MUTED, line_width=1)
    fig.update_layout(**CHART_LAYOUT, height=260, margin=dict(l=0,r=0,t=10,b=0), showlegend=False)
    fig.update_yaxes(gridcolor="#1E2230", tickfont=dict(size=11, color=TEXT_SECONDARY),
                     title=None, ticksuffix="%")
    return fig


def make_divergence_chart(weekly, use_sa=True):
    df = weekly.copy()
    h = "holiday_sa" if use_sa and "holiday_sa" in df.columns else "holiday"
    i = "insurance_sa" if use_sa and "insurance_sa" in df.columns else "insurance"
    if h not in df.columns or i not in df.columns:
        return go.Figure()
    df["gap"] = df[i] - df[h]
    df["gap_smooth"] = df["gap"].rolling(12, min_periods=4).mean()
    colors = [TEAL if v >= 0 else BLUE for v in df["gap_smooth"].fillna(0)]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["gap_smooth"], marker_color=colors, showlegend=False,
        hovertemplate="Insurance %{y:+.0f} pts vs holiday<extra></extra>"))
    fig.add_hline(y=0, line_color=TEXT_MUTED, line_width=1)
    fig.update_layout(**CHART_LAYOUT, height=280, margin=dict(l=0,r=0,t=10,b=0), showlegend=False)
    fig.update_yaxes(gridcolor="#1E2230", tickfont=dict(size=11, color=TEXT_SECONDARY), title=None)
    return fig


def make_seasonal_overlay(weekly, metric="combined"):
    df = weekly.copy()
    if metric not in df.columns:
        return go.Figure()
    df["year"] = df["date"].dt.year
    df["week_num"] = df["date"].dt.isocalendar().week.astype(int)
    year_counts = df.groupby("year")["week_num"].count()
    valid_years = year_counts[year_counts >= 26].index.tolist()
    df = df[df["year"].isin(valid_years)]
    if df.empty:
        return go.Figure()
    current_year = df["year"].max()
    hist_range = df[df["year"] < current_year].groupby("week_num")[metric].agg(["min","max"]).reset_index()
    hist_avg = df[df["year"] < current_year].groupby("week_num")[metric].mean().reset_index()
    hist_avg.columns = ["week_num", "avg"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=hist_range["week_num"], y=hist_range["max"],
        mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=hist_range["week_num"], y=hist_range["min"],
        mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        fill="tonexty", fillcolor=_hex_to_rgba(GREY, 0.12)))
    fig.add_trace(go.Scatter(x=hist_avg["week_num"], y=hist_avg["avg"],
        name="Historical avg", line=dict(color=GREY, width=2, dash="dash"),
        hovertemplate="Week %{x}: Avg %{y:.0f}<extra></extra>"))
    prev_year = current_year - 1
    if prev_year in valid_years:
        prev = df[df["year"] == prev_year]
        fig.add_trace(go.Scatter(x=prev["week_num"], y=prev[metric],
            name=str(prev_year), line=dict(color=BLUE, width=1.5), opacity=0.6,
            hovertemplate=f"Wk %{{x}} ({prev_year}): %{{y:.0f}}<extra></extra>"))
    curr = df[df["year"] == current_year]
    fig.add_trace(go.Scatter(x=curr["week_num"], y=curr[metric],
        name=str(current_year), line=dict(color=GOLD, width=3),
        hovertemplate=f"Wk %{{x}} ({current_year}): %{{y:.0f}}<extra></extra>"))
    ms = {1:"Jan",5:"Feb",9:"Mar",14:"Apr",18:"May",22:"Jun",
          27:"Jul",31:"Aug",35:"Sep",40:"Oct",44:"Nov",48:"Dec"}
    fig.update_layout(**CHART_LAYOUT, height=350, margin=dict(l=0,r=0,t=10,b=0),
        legend=dict(orientation="h", yanchor="top", y=1.12, xanchor="left", x=0,
                    font=dict(size=12, color=TEXT_SECONDARY), bgcolor="rgba(0,0,0,0)"),
        hovermode="x unified")
    fig.update_xaxes(gridcolor="#1E2230", showgrid=False,
                     tickfont=dict(size=11, color=TEXT_SECONDARY),
                     tickvals=list(ms.keys()), ticktext=list(ms.values()), title=None)
    return fig


def make_trends_line(df, title_map=None, height=220, palette_offset=0):
    """Generic line chart for a Google Trends dataframe."""
    fig = go.Figure()
    for idx, col_name in enumerate(df.columns):
        display = title_map.get(col_name, col_name) if title_map else col_name
        fig.add_trace(go.Scatter(
            x=df.index, y=df[col_name], name=display,
            line=dict(color=PALETTE[(idx + palette_offset) % len(PALETTE)], width=2),
            hovertemplate=f"{display}: %{{y}}/100<extra></extra>"))
    fig.update_layout(**CHART_LAYOUT, height=height, margin=dict(l=0,r=0,t=10,b=0),
        legend=dict(orientation="h", yanchor="top", y=1.2, xanchor="left", x=0,
                    font=dict(size=10, color=TEXT_SECONDARY), bgcolor="rgba(0,0,0,0)"))
    fig.update_yaxes(gridcolor="#1E2230", tickfont=dict(size=10, color=TEXT_SECONDARY), title=None)
    return fig


LOADING_STEPS = [
    "Connecting to data sources",
    "Loading Google Trends & search data",
    "Fetching competitor signals",
    "Loading Holiday Extras channel data",
    "Crunching the numbers",
    "Generating AI insights",
    "Creating section images",
    "Building your dashboard",
]


def loading_screen(step: int = 0) -> str:
    """Return HTML for a branded loading screen with progress steps and percentage."""
    total = len(LOADING_STEPS)
    pct = int((step / total) * 100)

    step_html = ""
    for idx, label in enumerate(LOADING_STEPS):
        if idx < step:
            cls = "done"
        elif idx == step:
            cls = "active"
        else:
            cls = ""
        delay = f"animation-delay: {idx * 0.1}s;" if cls else f"animation-delay: {idx * 0.1}s; opacity: 0.3;"
        step_html += f"""<div class="loading-step {cls}" style="{delay}">
            <span class="step-dot"></span> {label}
        </div>"""

    return f"""<div class="loading-screen">
        <div class="loading-logo"><img src="{HX_LOGO_URL}" alt="Holiday Extras"><span class="loading-logo-text">Holiday Extras</span></div>
        <div class="loading-subtitle">Insurance Pulse — Weekly Market View</div>
        <div style="font-size: 2rem; font-weight: 800; color: #9B72CF; margin-bottom: 1.5rem;">{pct}%</div>
        <div class="loading-steps">{step_html}</div>
        <div class="loading-bar-wrap">
            <div class="loading-bar" style="animation: none; width: {pct}%; transition: width 0.5s ease;"></div>
        </div>
    </div>"""


def metric_card(label, value, delta="", subtitle="", delta_direction="neutral"):
    dcls = {"up":"metric-delta-up","down":"metric-delta-down"}.get(delta_direction,"metric-subtitle")
    dh = f'<div class="{dcls}">{delta}</div>' if delta else ""
    sh = f'<div class="metric-subtitle">{subtitle}</div>' if subtitle else ""
    return f"""<div class="metric-card" role="figure" aria-label="{label}: {value}">
        <div class="metric-label">{label}</div><div class="metric-value">{value}</div>{dh}{sh}</div>"""


def _trend_pct(df, col=None):
    """Compute recent vs earlier pct change for a trends df."""
    if df is None or df.empty:
        return 0.0
    if col:
        series = df[col] if col in df.columns else df.iloc[:, 0]
    else:
        series = df.mean(axis=1)
    r, e = series.tail(4).mean(), series.head(4).mean()
    return ((r - e) / e * 100) if e > 0 else 0.0


# ---------------------------------------------------------------------------
# Dynamic section priority
# ---------------------------------------------------------------------------
def compute_priorities(yoy, gap, wow, extra_trends, hx_trends):
    """Return a list of (section_key, score, reason) sorted by importance."""
    signals = []

    # Market trend -- big moves get attention
    trend_score = 50 + abs(yoy) * 2
    if abs(yoy) > 10:
        signals.append(("trend", trend_score, f"Market {('up' if yoy > 0 else 'down')} {abs(yoy):.0f}% vs last year"))
    else:
        signals.append(("trend", trend_score, ""))

    # Divergence -- large gap means opportunity or risk
    div_score = 40 + abs(gap) * 1.5
    if abs(gap) > 15:
        signals.append(("divergence", div_score, f"Insurance {'leads' if gap > 0 else 'trails'} holiday by {abs(gap):.0f} pts"))
    else:
        signals.append(("divergence", div_score, ""))

    # Channel signals -- parking
    park_ch = _trend_pct(hx_trends.get("parking"))
    ch_score = 45 + abs(park_ch) * 0.5
    if abs(park_ch) > 15:
        signals.append(("channels", ch_score, f"Airport parking {'surging' if park_ch > 0 else 'falling'} {abs(park_ch):.0f}%"))
    else:
        signals.append(("channels", ch_score, ""))

    # Competitors -- anyone surging?
    comp_df = extra_trends.get("competitors")
    comp_score = 35
    if comp_df is not None and not comp_df.empty:
        max_comp_ch = max(abs(_trend_pct(comp_df, c)) for c in comp_df.columns)
        comp_score += max_comp_ch * 0.4
        if max_comp_ch > 20:
            signals.append(("competitors", comp_score, "Competitor brand surging"))
        else:
            signals.append(("competitors", comp_score, ""))
    else:
        signals.append(("competitors", comp_score, ""))

    # YoY chart -- boost when year-on-year shift is dramatic
    yoy_score = 25 + abs(yoy) * 0.8
    signals.append(("yoy", yoy_score, ""))

    # Seasonal -- boost near key booking windows (Jan, May-Jun, Sep)
    import datetime as _dt
    _month = _dt.date.today().month
    seasonal_boost = 15 if _month in (1, 5, 6, 9) else 0
    signals.append(("seasonal", 30 + seasonal_boost, ""))

    # Quarterly -- data confidence, always last
    signals.append(("quarterly", 15, ""))

    # News -- always important
    signals.append(("news", 55, ""))

    signals.sort(key=lambda x: -x[1])
    return signals


# ---------------------------------------------------------------------------
# Section renderers -- each is a standalone function
# ---------------------------------------------------------------------------
def _section_with_image(header: str, img_key: str, img_desc: str, summary: str):
    """Render a section header with optional image beside the summary."""
    st.markdown(f'<div class="section-header">{header}</div>', unsafe_allow_html=True)
    img_url = _generate_section_image(img_key, img_desc)
    if img_url:
        st.markdown(f"""<div class="section-row">
            <div class="section-image"><img src="{img_url}" alt="{header}"></div>
            <div class="section-content"><div class="section-summary">{summary}</div></div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="section-summary">{summary}</div>', unsafe_allow_html=True)


def render_trend(sa_weekly, _ctx, c_now, yoy, wow, max_date):
    # Build summary and image
    if yoy > 5:
        summary = f'More people are searching for travel insurance — <strong class="up">up {abs(yoy):.0f}%</strong> vs last year. In the last 4 weeks alone, searches have moved <strong class="{"up" if wow > 0 else "down"}">{wow:+.1f}%</strong>.'
        img_desc = "busy airport terminal with families and travellers, suitcases and departure boards, vibrant and optimistic"
    elif yoy < -5:
        summary = f'Fewer people are searching for travel insurance — <strong class="down">down {abs(yoy):.0f}%</strong> vs last year. In the last 4 weeks, searches moved <strong class="{"up" if wow > 0 else "down"}">{wow:+.1f}%</strong>.'
        img_desc = "quiet airport terminal with empty seats, fewer travellers, subdued lighting"
    else:
        summary = f'Search activity for travel insurance is <strong class="neutral">about the same as last year</strong> ({yoy:+.1f}%). In the last 4 weeks it\'s moved <strong>{wow:+.1f}%</strong>.'
        img_desc = "normal airport scene with moderate passenger flow, everyday travel"
    _section_with_image("Market Demand", f"trend_{int(yoy)}", img_desc, summary)

    with st.expander("View demand chart and controls"):
        ctrl_cols = st.columns([2, 1, 1, 1])
        with ctrl_cols[0]:
            time_range = st.select_slider("Time range", options=["1Y","2Y","3Y","5Y","All"], value="3Y",
                                          label_visibility="collapsed")
        with ctrl_cols[1]:
            show_h = st.toggle("Holiday", value=True)
        with ctrl_cols[2]:
            show_i = st.toggle("Insurance", value=True)
        with ctrl_cols[3]:
            show_c = st.toggle("Combined", value=True)

        rmap = {"1Y":52,"2Y":104,"3Y":156,"5Y":260,"All":len(sa_weekly)}
        nw = rmap.get(time_range, 156)
        min_date = max_date - pd.Timedelta(weeks=nw) if time_range != "All" else sa_weekly["date"].min()
        fig = make_trend_chart(sa_weekly, show_h, show_i, show_c, (min_date, max_date), use_sa=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        st.caption(f"Gold = overall demand. Blue = holiday searches. Teal = insurance searches. All seasonally adjusted.")

    # Auto-generate AI insight (cached 24h, 10s timeout)
    recent = sa_weekly.tail(8)
    trend_str = " -> ".join(f"{r['date'].strftime('%b %Y')}: {float(r.get('combined_sa', r.get('combined',0))):.0f}"
                            for _, r in recent.iterrows())
    q = (f"DATA:\n{_ctx}\n\nRecent weekly search activity: {trend_str}. "
         f"Currently {yoy:+.1f}% vs last year. "
         f"Growing, shrinking, or flat — and why? One line on what to expect next month. "
         f"One specific action HX should take to capture this demand (not just report it).")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing market trend", HX_PURPLE_LIGHT), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — What's Driving This", ai_result, HX_PURPLE_LIGHT), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", HX_PURPLE_LIGHT), unsafe_allow_html=True)
    return time_range


def render_divergence(sa_weekly, _ctx, i_now, h_now, gap, i_last_year, h_last_year):
    gap_last_year = i_last_year - h_last_year
    gap_change = gap - gap_last_year

    if gap > 0 and gap_last_year <= 0:
        gap_story = "flipped"
    elif gap <= 0 and gap_last_year > 0:
        gap_story = "flipped"
    elif abs(gap) > abs(gap_last_year) + 5:
        gap_story = "widened"
    elif abs(gap) < abs(gap_last_year) - 5:
        gap_story = "narrowed"
    else:
        gap_story = "stable"

    # Build image description
    if gap > 10:
        div_img_desc = "close up of someone comparing insurance policies on a laptop screen, focused and ready to purchase, warm lighting"
    elif gap < -10:
        div_img_desc = "person browsing holiday destinations on a tablet, beach photos on screen, relaxed daydreaming mood"
    else:
        div_img_desc = "split image of someone looking at beach photos and insurance documents, decision making"

    # One-liner summary
    who_leads = "Insurance" if gap > 0 else "Holiday"
    lead_color = "up" if gap > 0 else "down"
    who_led_ly = "insurance" if gap_last_year > 0 else "holiday"

    if gap > 0:
        buying_msg = "More people are <strong class=\"up\">searching for travel insurance</strong> than dreaming about holidays"
    else:
        buying_msg = "More people are <strong class=\"down\">dreaming about holidays</strong> than searching for travel insurance"

    if gap_story == "flipped":
        summary = (f'{buying_msg} — this has <strong class="down">flipped</strong> from last year when {who_led_ly.lower()} searches were ahead.')
    elif gap_story == "widened":
        summary = (f'{buying_msg}. This gap has <strong>grown bigger</strong> compared to last year.')
    elif gap_story == "narrowed":
        summary = (f'{buying_msg}. The gap is <strong>closing</strong> compared to last year.')
    else:
        summary = (f'{buying_msg} — about the same as this time last year.')
    _section_with_image("Insurance Searchers vs Holiday Dreamers", f"div_{int(gap)}", div_img_desc, summary)

    with st.expander("View gap chart and detail"):
        st.markdown(f"""<div class="chart-explainer">
            <strong style="color:{TEAL};">Teal</strong> = insurance leads.
            <strong style="color:{BLUE};">Blue</strong> = holiday leads. 12-week average, SA.<br>
            <strong>Holiday searches</strong> = people Googling holidays (they'll need insurance soon). <strong>Insurance searches</strong> = people Googling travel insurance (closer to purchasing).
            When insurance leads, people are actively looking for cover. When holidays lead, insurance searches usually follow in 2-4 weeks.
        </div>""", unsafe_allow_html=True)
        div_fig = make_divergence_chart(sa_weekly, use_sa=True)
        if div_fig.data:
            st.plotly_chart(div_fig, use_container_width=True, config={"displayModeBar": False})

    q = (f"DATA:\n{_ctx}\n\n{'Insurance searches lead holiday searches' if gap > 0 else 'Holiday searches lead insurance searches'}. "
         f"Gap has {gap_story} vs last year. "
         f"Why — what real-world event or behaviour shift explains this? "
         f"One specific thing HX should do right now to convert these searchers.")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing searchers vs dreamers", TEAL), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — Insurance Searchers vs Dreamers", ai_result, TEAL), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", TEAL), unsafe_allow_html=True)


def render_channels(hx_trends, extra_trends, _ctx, comp_df):
    # Build one-liner from data (plain English)
    park_ch = _trend_pct(hx_trends.get("parking"))
    wl_ch = _trend_pct(hx_trends.get("white_labels"))
    parts = []
    if park_ch > 10:
        parts.append(f'<strong class="up">{abs(park_ch):.0f}% more people</strong> are searching for airport parking — more potential insurance cross-sells coming')
    elif park_ch < -10:
        parts.append(f'<strong class="down">{abs(park_ch):.0f}% fewer</strong> airport parking searches — fewer people in our direct sales funnel')
    else:
        parts.append("Airport parking search traffic is <strong>steady</strong>")
    if wl_ch > 10:
        parts.append(f'our cruise partners (Carnival, Fred Olsen) are getting <strong class="up">{abs(wl_ch):.0f}% more</strong> search interest')
    elif wl_ch < -10:
        parts.append(f'cruise partner search interest is <strong class="down">down {abs(wl_ch):.0f}%</strong>')
    else:
        parts.append("cruise partner brands are <strong>steady</strong>")
    _section_with_image(
        "Holiday Extras — Channel Signals", "channels",
        "aerial view of airport car park with planes in background, cars driving in, bright sunny day",
        f"{parts[0]}, and {parts[1]}.")

    with st.expander("View channel detail"):
        ch1, ch2 = st.columns(2)
        park_df = hx_trends.get("parking")
        with ch1:
            st.markdown(f"""<div class="channel-card">
                <div class="ch-name" style="color:{BLUE};">Direct — Airport Parking Cross-Sell</div>
                <div class="ch-desc">When parking searches rise, your cross-sell window opens.</div>
            </div>""", unsafe_allow_html=True)
            if park_df is not None and not park_df.empty:
                st.plotly_chart(make_trends_line(park_df, height=220), use_container_width=True, config={"displayModeBar": False})
                p_ch = _trend_pct(park_df)
                if p_ch > 10:
                    st.caption(f"Parking demand UP {p_ch:.0f}% — cross-sell window opening.")
                elif p_ch < -10:
                    st.caption(f"Parking demand DOWN {abs(p_ch):.0f}% — fewer customers in funnel.")
                else:
                    st.caption(f"Parking demand stable ({p_ch:+.0f}%).")
            else:
                st.caption("Parking data loading...")

        wl_df = hx_trends.get("white_labels")
        with ch2:
            st.markdown(f"""<div class="channel-card">
                <div class="ch-name" style="color:{ORANGE};">White Labels — Partner Brand Health</div>
                <div class="ch-desc">When a partner brand grows, your white label sales follow.</div>
            </div>""", unsafe_allow_html=True)
            if wl_df is not None and not wl_df.empty:
                st.plotly_chart(make_trends_line(wl_df, height=220, palette_offset=2), use_container_width=True, config={"displayModeBar": False})
                for col_name in wl_df.columns:
                    ch = _trend_pct(wl_df, col_name)
                    icon = "up" if ch > 5 else ("down" if ch < -5 else "flat")
                    st.caption(f"{'📈' if icon == 'up' else '📉' if icon == 'down' else '➡️'} {col_name}: {ch:+.0f}% over 12 months")
            else:
                st.caption("Partner data loading...")

    # Auto-generated channel AI analysis (no buttons — just generates)
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
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing channels", HX_PURPLE_LIGHT), unsafe_allow_html=True)
    ch_result = _call_openai_with_timeout(ch_q, timeout_secs=10)
    if ch_result:
        ai_slot.markdown(ai_box("AI — Channel Opportunities", ch_result, HX_PURPLE_LIGHT), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", HX_PURPLE_LIGHT), unsafe_allow_html=True)


def render_competitors(extra_trends, _ctx):
    comp_df = extra_trends.get("competitors")
    price_df = extra_trends.get("price_sensitivity")

    # One-liner summary (no jargon)
    comp_parts = []
    if comp_df is not None and not comp_df.empty:
        changes = {t: _trend_pct(comp_df, t) for t in comp_df.columns}
        top = max(changes, key=lambda k: abs(changes[k]))
        top_ch = changes[top]
        if abs(top_ch) > 15:
            comp_parts.append(f'<strong class="{"up" if top_ch > 0 else "down"}">{top}</strong> is getting {"more" if top_ch > 0 else "less"} popular ({abs(top_ch):.0f}% {"more" if top_ch > 0 else "fewer"} searches)')
        else:
            comp_parts.append("No competitor is making big moves right now")
    price_ch = _trend_pct(price_df)
    if price_ch > 10:
        comp_parts.append(f'more people are searching for <strong class="up">cheap insurance</strong> ({abs(price_ch):.0f}% more)')
    elif price_ch < -10:
        comp_parts.append(f'fewer people are shopping on price ({abs(price_ch):.0f}% less) — <strong>less price pressure</strong>')
    else:
        comp_parts.append("price shopping is at normal levels")
    _section_with_image(
        "Competitors & Price Shopping", "competitors",
        "comparison shopping on a laptop showing multiple insurance quotes side by side, professional office setting",
        f"{'. '.join(comp_parts).capitalize()}.")

    with st.expander("View competitor and price charts"):
        comp_col, price_col = st.columns(2)
        with comp_col:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Competitor Brand Searches</div>
                <div class="metric-subtitle">If a competitor grows faster than generic "travel insurance", they're winning share.</div>
            </div>""", unsafe_allow_html=True)
            if comp_df is not None and not comp_df.empty:
                st.plotly_chart(make_trends_line(comp_df, height=250), use_container_width=True, config={"displayModeBar": False})
                changes = {t: _trend_pct(comp_df, t) for t in comp_df.columns}
                if changes:
                    top = max(changes, key=changes.get)
                    st.caption(f"Fastest growing: {top} ({changes[top]:+.0f}% over 12 months)")
            else:
                st.caption("Competitor data loading...")

        with price_col:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">Price Sensitivity Searches</div>
                <div class="metric-subtitle">Rising = people want cover but are shopping hard on price.</div>
            </div>""", unsafe_allow_html=True)
            if price_df is not None and not price_df.empty:
                st.plotly_chart(make_trends_line(price_df, height=250, palette_offset=3), use_container_width=True, config={"displayModeBar": False})
                p_ch = _trend_pct(price_df)
                if p_ch > 10:
                    st.caption(f"Price sensitivity UP {p_ch:.0f}% — consumers shopping harder on price.")
                elif p_ch < -10:
                    st.caption(f"Price sensitivity DOWN {abs(p_ch):.0f}% — consumers care less about price.")
                else:
                    st.caption(f"Price sensitivity stable ({p_ch:+.0f}%).")
            else:
                st.caption("Price sensitivity data loading...")

    comp_context = ""
    for label, df in [("Competitor brands", comp_df), ("Price sensitivity", price_df)]:
        if df is not None and not df.empty:
            comp_context += f"\n{label}:\n"
            for t in df.columns:
                comp_context += f"  {t}: {_trend_pct(df, t):+.0f}% trend\n"
    q = (f"DATA:\n{_ctx}\n{comp_context}\n\nWhich competitors are gaining or losing search share? "
         f"Is 'cheap travel insurance' rising? Name names and say what HX should do — "
         f"pricing, ad copy, or product. Skip competitors with no meaningful movement.")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing competitors", ORANGE), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — Competitor Watch", ai_result, ORANGE), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", ORANGE), unsafe_allow_html=True)

    return comp_df


NEWS_SYSTEM_PROMPT = f"""Search the web for UK travel insurance news from the last 2 weeks. Only include stories that directly affect Holiday Extras.
{HX_STRATEGY_CONTEXT}
RULES:
- Reply in plain English a 12-year-old could understand. No asterisks, no markdown, no special symbols.
- Use <b> tags for emphasis. NEVER use ** or *.
- MAX 600 characters total.

FORMAT: Return 3-4 items MAX. Only the most important. For each:
<b>[Headline]</b> -- [1 sentence what happened]. <b>HX action:</b> [1 sentence what to do].

Skip anything generic. Every item must have a clear "so what" for Holiday Extras."""

NEWS_USER_PROMPT = "Search the web for the most important UK travel insurance news in the last 2-4 weeks. Include real headlines and sources."


def pre_generate_news() -> str:
    """Pre-generate news during loading phase so it's cached before render."""
    return _call_openai_raw(NEWS_SYSTEM_PROMPT, NEWS_USER_PROMPT)


def render_news(pre_result: str | None = None):
    _section_with_image(
        "News & Market Intelligence", "news",
        "stack of newspapers and a tablet showing travel headlines, morning coffee scene, warm editorial lighting",
        "What's happening in the real world that affects travel insurance — updated daily from live news sources.")

    result = pre_result
    if result and not _is_bad_response(result):
        result = _strip_markdown(result)
        st.markdown(f"""<div class="news-section">
            <div class="news-title">What's Happening Right Now</div>
            <div style="color: #F0F2F6; line-height: 1.8; font-size: 0.95rem;">{result}</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown(ai_loading_box("News still loading — refresh shortly", "#FDDC06"), unsafe_allow_html=True)


def render_channel_table(yoy, extra_trends, hx_trends):
    st.markdown('<div class="section-header">What To Do On Each Channel</div>', unsafe_allow_html=True)
    rows = []

    # Direct
    park_ch = _trend_pct(hx_trends.get("parking"))
    if park_ch > 10:
        rows.append({"Channel": "Direct", "Signal": f"Parking UP {park_ch:.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Ramp up parking-to-insurance campaigns"})
    elif park_ch < -10:
        rows.append({"Channel": "Direct", "Signal": f"Parking DOWN {abs(park_ch):.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Re-engage existing parkers via email"})
    else:
        rows.append({"Channel": "Direct", "Signal": "Parking stable",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Maintain current cross-sell cadence"})

    # PPC/SEO
    price_ch = _trend_pct(extra_trends.get("price_sensitivity"))
    if price_ch > 10:
        rows.append({"Channel": "PPC / SEO", "Signal": f"Price shopping UP {price_ch:.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Highlight value, not just price"})
    elif price_ch < -10:
        rows.append({"Channel": "PPC / SEO", "Signal": f"Price shopping DOWN {abs(price_ch):.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Test higher bids -- quality over price"})
    else:
        rows.append({"Channel": "PPC / SEO", "Signal": "Price sensitivity stable",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Hold current bids"})

    # White Labels
    wl_ch = _trend_pct(hx_trends.get("white_labels"))
    if wl_ch > 10:
        rows.append({"Channel": "White Labels", "Signal": f"Partner brands UP {wl_ch:.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Negotiate better placement"})
    elif wl_ch < -10:
        rows.append({"Channel": "White Labels", "Signal": f"Partner brands DOWN {abs(wl_ch):.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Prospect new partners"})
    else:
        rows.append({"Channel": "White Labels", "Signal": "Partners stable",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Maintain relationships"})

    # Aggregators
    comp_df = extra_trends.get("competitors")
    ctm_ch = 0
    if comp_df is not None and "Compare the Market insurance" in comp_df.columns:
        ctm_ch = _trend_pct(comp_df, "Compare the Market insurance")
    if ctm_ch > 15:
        rows.append({"Channel": "Aggregators", "Signal": f"Agg traffic UP {ctm_ch:.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Ensure pricing is competitive"})
    elif ctm_ch < -10:
        rows.append({"Channel": "Aggregators", "Signal": f"Agg traffic DOWN {abs(ctm_ch):.0f}%",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Less competition -- hold margins"})
    else:
        rows.append({"Channel": "Aggregators", "Signal": "Aggregator traffic stable",
                     "Demand vs LY": f"{yoy:+.1f}%", "Action": "Hold pricing"})

    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def render_seasonal(weekly, latest_date, _ctx, yoy):
    month = latest_date.strftime("%B") if hasattr(latest_date, "strftime") else "this month"
    st.markdown('<div class="section-header">Seasonal Pattern</div>', unsafe_allow_html=True)

    if yoy > 5:
        summary = f'For <strong>{month}</strong>, the market is <strong class="up">busier than usual</strong> — {abs(yoy):.0f}% more searches than the same time last year.'
    elif yoy < -5:
        summary = f'For <strong>{month}</strong>, the market is <strong class="down">quieter than usual</strong> — {abs(yoy):.0f}% fewer searches than the same time last year.'
    else:
        summary = f'For <strong>{month}</strong>, the market is <strong class="neutral">about average</strong> for this time of year ({yoy:+.1f}% vs last year).'
    st.markdown(f'<div class="section-summary">{summary}</div>', unsafe_allow_html=True)

    with st.expander("View seasonal overlay charts"):
        st.caption("Grey band = range across all previous years. Gold = this year. Blue = last year.")
        stabs = st.tabs(["Overall Demand", "Holiday Searches", "Insurance Searches"])
        for tab, lbl, key in zip(stabs, ["Overall","Holiday","Insurance"], ["combined","holiday","insurance"]):
            with tab:
                if key in weekly.columns:
                    sf = make_seasonal_overlay(weekly, metric=key)
                    if sf.data:
                        st.plotly_chart(sf, use_container_width=True, config={"displayModeBar": False})
                    else:
                        st.caption("Not enough data.")
                else:
                    st.caption(f"No {lbl.lower()} data.")

    q = (f"DATA:\n{_ctx}\n\nIt's {month}. Market is {yoy:+.1f}% vs last year. "
         f"What happens to travel insurance demand in the next 6-8 weeks? "
         f"Key dates (school holidays, booking windows) and one thing HX should prepare now.")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing seasonal patterns", ORANGE), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — What's Coming Next", ai_result, ORANGE), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", ORANGE), unsafe_allow_html=True)


def render_yoy(sa_weekly, _ctx, c_now, c_last_year, h_now, h_last_year, i_now, i_last_year, yoy):
    st.markdown('<div class="section-header">Year-on-Year Growth</div>', unsafe_allow_html=True)

    h_yoy = ((h_now - h_last_year) / h_last_year * 100) if h_last_year else 0
    i_yoy = ((i_now - i_last_year) / i_last_year * 100) if i_last_year else 0
    summary = (f'Compared to this time last year, overall search activity is <strong class="{"up" if yoy > 0 else "down"}">{yoy:+.1f}%</strong>. '
               f'Holiday browsing is <strong>{h_yoy:+.0f}%</strong> and insurance shopping is <strong>{i_yoy:+.0f}%</strong>.')
    st.markdown(f'<div class="section-summary">{summary}</div>', unsafe_allow_html=True)

    with st.expander("View year-on-year chart"):
        st.caption("Teal = higher than last year. Orange = lower. All seasonally adjusted.")
        yoy_fig = make_yoy_chart(sa_weekly, use_sa=True)
        if yoy_fig.data:
            st.plotly_chart(yoy_fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("Not enough data.")

    q = (f"DATA:\n{_ctx}\n\nOverall searches {yoy:+.1f}% vs last year. "
         f"Holiday searches: {h_yoy:+.0f}%. Insurance searches: {i_yoy:+.0f}%. "
         f"Why — more travellers, or different search behaviour? "
         f"What does this mean for HX sales specifically (not just market size)?")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI analysing year-on-year", MAGENTA), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — Year-on-Year Story", ai_result, MAGENTA), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", MAGENTA), unsafe_allow_html=True)


def render_quarterly(quarterly, time_range, max_date, _ctx):
    st.markdown('<div class="section-header">Cross-Source Validation</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-summary">We track <strong>5 independent data sources</strong>. When they all move in the same direction, the trend is real.</div>', unsafe_allow_html=True)

    with st.expander("View quarterly source comparison"):
        if not quarterly.empty:
            qdf = quarterly.copy()
            if time_range != "All":
                cy = max_date.year - int(time_range[0])
                qdf = qdf[qdf["quarter"] >= f"{cy}Q1"]
            st.plotly_chart(make_quarterly_bars(qdf), use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No quarterly data available.")

    q = (f"DATA:\n{_ctx}\n\n5 data sources checked. Do they agree or conflict? "
         f"Is this a real market shift or noise? One-line confidence verdict for HX decision-makers.")
    ai_slot = st.empty()
    ai_slot.markdown(ai_loading_box("AI checking data confidence", HX_PURPLE_LIGHT), unsafe_allow_html=True)
    ai_result = _call_openai_with_timeout(q, timeout_secs=10)
    if ai_result:
        ai_slot.markdown(ai_box("AI — Data Confidence Check", ai_result, HX_PURPLE_LIGHT), unsafe_allow_html=True)
    else:
        ai_slot.markdown(ai_loading_box("AI still loading — refresh shortly", HX_PURPLE_LIGHT), unsafe_allow_html=True)


def render_signals(sources):
    st.markdown('<div class="section-header">Individual Data Sources</div>', unsafe_allow_html=True)
    signal_configs = [
        ("Holiday Search Trends", "google_trends", list(config.HOLIDAY_INTENT_TERMS), BLUE,
         "UK Google searches for holidays."),
        ("Insurance Search Trends", "google_trends", list(config.INSURANCE_INTENT_TERMS), TEAL,
         "UK Google searches for travel insurance."),
        ("UK Airport Passengers", "caa", ["uk_terminal_passengers"], ORANGE,
         "Passengers through UK airports (CAA)."),
        ("UK Residents Going Abroad", "ons", ["uk_visits_abroad"], MAGENTA,
         "UK residents travelling abroad (ONS)."),
        ("Global Air Travel", "world_bank", ["air_passengers_global"], GOLD,
         "Worldwide air passengers (World Bank)."),
    ]
    for i in range(0, len(signal_configs), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(signal_configs):
                break
            label, src_key, metrics, color, desc = signal_configs[idx]
            src_df = sources.get(src_key)
            with col:
                st.markdown(f"""<div class="metric-card" role="figure" aria-label="{label}">
                    <div class="metric-label">{label}</div>
                    <div class="metric-subtitle">{desc}</div>
                </div>""", unsafe_allow_html=True)
                if src_df is not None and not src_df.empty:
                    filtered = src_df[src_df["metric_name"].isin(metrics)]
                    if not filtered.empty:
                        ts = filtered.groupby("date")["normalised_value"].mean().sort_index()
                        vals = [float(v) for v in ts.values]
                        st.plotly_chart(make_sparkline(vals, color=color, height=100),
                                        use_container_width=True, config={"displayModeBar": False})
                        lv = float(ts.iloc[-1])
                        ld = pd.Timestamp(ts.index[-1])
                        st.caption(f"Latest: {lv:,.0f} ({ld.strftime('%b %Y')})")
                else:
                    st.caption("No data available.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    # =====================================================================
    # LOADING PHASE — show branded loading screen, nothing else visible
    # =====================================================================
    is_cold_start = "data_loaded" not in st.session_state
    if is_cold_start:
        loader = st.empty()
        loader.markdown(loading_screen(0), unsafe_allow_html=True)

    # Step 1: Core data
    if is_cold_start:
        loader.markdown(loading_screen(1), unsafe_allow_html=True)
    sources = load_all_data()

    # Step 2: Build weekly/quarterly
    weekly = build_weekly_trends(sources)
    quarterly = build_quarterly_summary(sources)

    if weekly.empty:
        if is_cold_start:
            loader.empty()
        st.error("No data available. Run the pipeline first: `python3 -m src.main --backfill`")
        return

    sa_weekly = add_all_sa(weekly)

    # Step 3: Competitor signals
    if is_cold_start:
        loader.markdown(loading_screen(2), unsafe_allow_html=True)
    extra_trends = load_extra_trends()

    # Step 4: HX channel data
    if is_cold_start:
        loader.markdown(loading_screen(3), unsafe_allow_html=True)
    hx_trends = load_hx_trends()

    # Step 5: Crunch numbers
    if is_cold_start:
        loader.markdown(loading_screen(4), unsafe_allow_html=True)

    # Current metrics
    latest = sa_weekly.iloc[-1]
    latest_date = latest["date"]
    date_str = latest_date.strftime("%d %B %Y") if hasattr(latest_date, "strftime") else str(latest_date)

    c_now = float(latest.get("combined_sa", latest.get("combined", 0)))
    h_now = float(latest.get("holiday_sa", latest.get("holiday", 0)))
    i_now = float(latest.get("insurance_sa", latest.get("insurance", 0)))
    gap = i_now - h_now

    yoy = 0.0
    c_last_year = h_last_year = i_last_year = c_now
    if len(sa_weekly) > 52:
        ago = sa_weekly.iloc[-53]
        c_last_year = float(ago.get("combined_sa", ago.get("combined", c_now)))
        h_last_year = float(ago.get("holiday_sa", ago.get("holiday", h_now)))
        i_last_year = float(ago.get("insurance_sa", ago.get("insurance", i_now)))
        if c_last_year:
            yoy = ((c_now - c_last_year) / c_last_year) * 100

    wow = 0.0
    if len(sa_weekly) > 4:
        p4 = float(sa_weekly.iloc[-5].get("combined_sa", sa_weekly.iloc[-5].get("combined", c_now)))
        if p4:
            wow = ((c_now - p4) / p4) * 100

    if yoy > 2:
        direction, arrow, word = "up", "↑", "growing"
    elif yoy < -2:
        direction, arrow, word = "down", "↓", "shrinking"
    else:
        direction, arrow, word = "neutral", "→", "roughly flat"
    wow_dir = "up" if wow > 0 else "down"

    _ctx = build_context(sa_weekly, sources)
    _full_ctx = build_full_context(_ctx, extra_trends, hx_trends)

    # =====================================================================
    # Compute dynamic section order
    # =====================================================================
    priorities = compute_priorities(yoy, gap, wow, extra_trends, hx_trends)
    priority_reasons = {k: reason for k, _, reason in priorities}
    section_order = [k for k, _, _ in priorities]

    # Step 6: Pre-generate "What Matters Now" AI (so it's cached before render)
    if is_cold_start:
        loader.markdown(loading_screen(5), unsafe_allow_html=True)
    top_signals = [(k, r) for k, _, r in priorities[:3] if r]
    matters_result = None
    if top_signals:
        matters_q = (
            f"DATA:\n{_full_ctx}\n\nThe biggest signals right now are:\n"
            + "\n".join(f"- {r}" for _, r in top_signals)
            + f"\n\nHX priorities: 1) Take market share 2) Maximise CLTV 3) Deliver brand promise 4) Build brand."
            f"\n\nIn 2-3 sentences, explain what these signals mean for Holiday Extras. "
            f"REMEMBER: More people searching does NOT mean more HX customers. "
            f"What specific action should the team take THIS WEEK to capture the opportunity? "
            f"Be specific: who does what, on which channel, by when.")
        matters_result = _call_openai_with_timeout(matters_q, timeout_secs=15)

    # Step 6b: Pre-generate news + deep dive (so they're cached before render)
    news_result = pre_generate_news()
    dd_q = (f"DATA:\n{_full_ctx}\n\nDo a deep investigation. What's REALLY driving the UK travel insurance "
            f"market right now? Search the web for current news. Name specific airlines, destinations, "
            f"events. Remember: more searches doesn't mean more Holiday Extras customers — "
            f"how can HX capture this demand across their channels?")
    dd_result = _call_openai_with_timeout(dd_q, timeout_secs=15)

    # Step 7: Pre-generate section images (cached to disk)
    if is_cold_start:
        loader.markdown(loading_screen(6), unsafe_allow_html=True)
    # Warm the image cache — these are all cached so only generate on first run
    if yoy > 5:
        _generate_section_image(f"trend_{int(yoy)}", "busy airport terminal with families and travellers, suitcases and departure boards, vibrant and optimistic")
    elif yoy < -5:
        _generate_section_image(f"trend_{int(yoy)}", "quiet airport terminal with empty seats, fewer travellers, subdued lighting")

    # Step 8: Ready
    if is_cold_start:
        loader.markdown(loading_screen(7), unsafe_allow_html=True)
        import time as _t; _t.sleep(0.4)
        loader.empty()
        st.session_state["data_loaded"] = True

    # =====================================================================
    # BRAND BAR + TOP HEADLINE — always first, always fixed
    # =====================================================================
    st.markdown(f"""<div class="hx-brand-bar">
        <div class="hx-logo"><img src="{HX_LOGO_URL}" alt="HX">Holiday Extras</div>
        <div class="hx-divider"></div>
        <div class="hx-title">Insurance Pulse</div>
        <div class="hx-date">Week ending {date_str}</div>
    </div>""", unsafe_allow_html=True)

    # Build the one-sentence headline a CEO can understand
    if yoy > 10:
        hl_main = f'The travel insurance market is <span class="up">growing strongly</span> — up {abs(yoy):.0f}% vs last year.'
    elif yoy > 2:
        hl_main = f'The travel insurance market is <span class="up">growing</span> — up {abs(yoy):.1f}% vs last year.'
    elif yoy < -5:
        hl_main = f'The travel insurance market is <span class="down">shrinking</span> — down {abs(yoy):.0f}% vs last year.'
    elif yoy < -2:
        hl_main = f'The travel insurance market is <span class="down">softening</span> — down {abs(yoy):.1f}% vs last year.'
    else:
        hl_main = f'The travel insurance market is <span class="neutral">flat</span> vs last year ({yoy:+.1f}%).'

    # Sub-headline with the key details (no jargon)
    gap_last_year = i_last_year - h_last_year
    hl_parts = []
    if abs(gap) > 10:
        if gap > 0:
            hl_parts.append("People are <strong>actively searching for travel insurance</strong>, not just dreaming about holidays")
        else:
            hl_parts.append("People are <strong>dreaming about holidays</strong> but haven't started searching for travel insurance yet")
    if abs(wow) > 3:
        hl_parts.append(f'search activity is <strong>{"picking up" if wow > 0 else "slowing down"}</strong> ({wow:+.1f}% over the last 4 weeks)')
    hl_sub = ". ".join(hl_parts) + f'. <span style="color:{TEXT_MUTED};">Week ending {date_str}.</span>' if hl_parts else f'<span style="color:{TEXT_MUTED};">Week ending {date_str}.</span>'

    # Recommendation — inline in the headline, not a separate banner
    # CRITICAL: demand growing ≠ HX getting those customers. Actions must be about CAPTURING demand.
    if yoy > 10 and gap > 15:
        reco_text = "Market is <strong>growing fast</strong> with lots of insurance searches. <strong>Capture share now</strong> — check PPC bids, push premium products."
    elif yoy > 10 and gap < -15:
        reco_text = "Lots of <strong>holiday dreamers</strong> — insurance demand is coming. <strong>Get in front of them</strong> — retarget, push insurance at booking."
    elif yoy < -5:
        reco_text = "Fewer searches than last year. <strong>Protect what you have</strong> — renewals, multi-trip, cross-sell to parking customers."
    elif gap > 25:
        reco_text = "Strong <strong>insurance search activity</strong>. <strong>Be visible</strong> — check aggregator rankings, test higher PPC bids."
    elif gap < -20:
        reco_text = "People <strong>dreaming about holidays</strong> but not searching for insurance yet. <strong>Plant the seed</strong> — awareness campaigns, parking confirmation emails."
    else:
        reco_text = "Market is <strong>steady</strong>. <strong>Focus on efficiency</strong> — optimise campaigns, improve conversion rates."

    st.markdown(f"""<div class="top-headline">
        <div class="hl-main">{hl_main}</div>
        <div class="hl-sub">{hl_sub}</div>
        <div class="hl-reco">{reco_text}</div>
    </div>""", unsafe_allow_html=True)

    # =====================================================================
    # METRIC CARDS
    # =====================================================================
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(metric_card("Market Demand", f"{yoy:+.0f}%",
            f"{arrow} vs last year",
            "How much people are searching for travel insurance overall", direction), unsafe_allow_html=True)
    with c2:
        h_yoy = ((h_now - h_last_year) / h_last_year * 100) if h_last_year else 0
        hd = "up" if h_yoy > 0 else "down"
        st.markdown(metric_card("Holiday Dreamers", f"{h_yoy:+.0f}%",
            f"{'↑' if h_yoy>0 else '↓'} vs last year",
            "Searching for holidays — they'll need travel insurance soon", hd), unsafe_allow_html=True)
    with c3:
        i_yoy = ((i_now - i_last_year) / i_last_year * 100) if i_last_year else 0
        id_ = "up" if i_yoy > 0 else "down"
        st.markdown(metric_card("Insurance Shoppers", f"{i_yoy:+.0f}%",
            f"{'↑' if i_yoy>0 else '↓'} vs last year",
            "Googling travel insurance — closest to actually purchasing", id_), unsafe_allow_html=True)
    with c4:
        gap_last_year = i_last_year - h_last_year
        gap_change = gap - gap_last_year
        gd = "up" if gap > 0 else "down"
        if gap > 0:
            gap_sub = "More insurance searches than holiday searches — people are closer to purchasing"
        else:
            gap_sub = "More holiday searches than insurance searches — demand is building"
        st.markdown(metric_card("Searching vs Dreaming", "Insurance leads" if gap > 0 else "Holidays lead",
            f"Gap {'widened' if abs(gap) > abs(gap_last_year) else 'narrowed'} vs last year",
            gap_sub, gd), unsafe_allow_html=True)

    st.markdown('<div style="height: 2rem;"></div>', unsafe_allow_html=True)

    # =====================================================================
    # AI NARRATIVE — first section after metrics
    # =====================================================================
    with st.container():
        st.markdown('<span class="scroll-section-marker"></span>', unsafe_allow_html=True)
        if matters_result:
            st.markdown(ai_box("What Matters Right Now", matters_result, HX_PURPLE_LIGHT), unsafe_allow_html=True)
        if dd_result:
            st.markdown(ai_box("The Full Story", dd_result, HX_PURPLE_LIGHT), unsafe_allow_html=True)

    # =====================================================================
    # DYNAMIC SECTION RENDERING — ordered by importance
    # =====================================================================
    comp_df = extra_trends.get("competitors")
    time_range = "3Y"  # default

    section_names = {
        "trend": "Market Demand", "divergence": "Holiday vs Insurance",
        "channels": "Channel Signals", "competitors": "Competitors",
        "news": "News", "seasonal": "Seasonal", "yoy": "Year-on-Year",
        "quarterly": "Cross-Source",
    }

    # Each section in its own st.container with a marker for JS scroll detection
    for idx, section_key in enumerate(section_order):
        with st.container():
            st.markdown('<span class="scroll-section-marker"></span>', unsafe_allow_html=True)

            if section_key == "trend":
                time_range = render_trend(sa_weekly, _ctx, c_now, yoy, wow, sa_weekly["date"].max())

            elif section_key == "divergence":
                render_divergence(sa_weekly, _ctx, i_now, h_now, gap, i_last_year, h_last_year)

            elif section_key == "channels":
                render_channels(hx_trends, extra_trends, _ctx, comp_df)
                render_channel_table(yoy, extra_trends, hx_trends)

            elif section_key == "competitors":
                comp_df = render_competitors(extra_trends, _ctx)

            elif section_key == "news":
                render_news(pre_result=news_result)

            elif section_key == "seasonal":
                render_seasonal(weekly, latest_date, _ctx, yoy)

            elif section_key == "yoy":
                render_yoy(sa_weekly, _ctx, c_now, c_last_year, h_now, h_last_year, i_now, i_last_year, yoy)

            elif section_key == "quarterly":
                render_quarterly(quarterly, time_range, sa_weekly["date"].max(), _ctx)

    # =====================================================================
    # INDIVIDUAL SIGNALS & METHODOLOGY (always last)
    # =====================================================================
    with st.container():
        st.markdown('<span class="scroll-section-marker"></span>', unsafe_allow_html=True)
        render_signals(sources)

    with st.expander("How this dashboard works"):
        st.markdown(f"""<div class="methodology">
            <strong>What is this?</strong><br>
            This dashboard shows how busy the UK travel insurance market is, so Holiday Extras
            can make better decisions. It pulls data from 5 different sources and uses AI to
            explain what's happening and what to do about it.<br><br>
            <strong>The numbers account for the time of year</strong> — January is always busy,
            so we strip out that pattern. Every change you see reflects something real happening
            in the market, not just the calendar.<br><br>
            <strong>Everything compares to last year</strong> — that's the most useful benchmark.<br><br>
            <strong>Sections are ordered by importance</strong> — if the market is moving fast,
            that comes first. If a competitor is surging, that gets promoted.<br><br>
            <strong>AI analyses generate automatically</strong> and are cached for 24 hours.
            They search the web for real news to explain what's driving the numbers.<br><br>
            <strong>Images are AI-generated</strong> to match what the data is showing — they
            update when the market picture changes.<br><br>
            <strong>Important:</strong> More people searching does NOT mean more Holiday Extras
            customers. The dashboard shows the size of the opportunity — it's up to us to capture it.<br><br>
            <strong>Data sources:</strong> Google Trends (weekly), UK CAA (annual), ONS (quarterly),
            Eurostat (monthly), World Bank (annual).
        </div>""", unsafe_allow_html=True)

    # Data freshness
    st.markdown('<div class="section-header">Data Freshness</div>', unsafe_allow_html=True)
    freshness = []
    for key, (label, freq) in {
        "google_trends": ("Google Trends", "Weekly"), "caa": ("UK CAA", "Annual"),
        "ons": ("ONS", "Quarterly"), "eurostat": ("Eurostat", "Monthly (ends 2020)"),
        "world_bank": ("World Bank", "Annual"),
    }.items():
        df = sources.get(key)
        if df is not None and not df.empty and "date" in df.columns:
            freshness.append({"Source": label, "Frequency": freq,
                              "Latest": pd.Timestamp(df["date"].max()).strftime("%b %Y"),
                              "Records": len(df)})
        else:
            freshness.append({"Source": label, "Frequency": freq, "Latest": "—", "Records": 0})
    st.dataframe(pd.DataFrame(freshness), hide_index=True, use_container_width=True)

    st.markdown("")
    if st.button("Refresh All Data", type="secondary"):
        st.cache_data.clear()
        st.session_state.pop("data_loaded", None)
        st.rerun()

    # (sections are now wrapped in st.container, no manual div close needed)


if __name__ == "__main__":
    main()
