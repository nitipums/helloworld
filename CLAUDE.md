# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a **Thai Stock Minervini Scanner** — a two-component GCP application that tracks SET/MAI-listed Thai stocks and classifies them by Minervini stage analysis.

## Architecture

Two independent services share a single Firestore collection (`set50`):

1. **Web app** (`hello.py` + root `Dockerfile`) — Flask app deployed as a **Cloud Run service**. Reads from Firestore, applies Minervini stage classification, and renders an HTML dashboard.

2. **Fetcher** (`fetcher/main.py` + `fetcher/Dockerfile`) — Python script deployed as a **Cloud Run Job**, triggered daily at 18:00 Bangkok time (Mon–Fri) via Cloud Scheduler. Pulls ~2 years of daily OHLCV candles from the Settrade Open API and writes them to Firestore.

### Data flow

```
Settrade Open API → fetcher/main.py → Firestore (set50/{TICKER}) → hello.py → HTML dashboard
```

### Firestore document schema (`set50/{TICKER}`)

Fields written by the fetcher: `symbol`, `ticker` (e.g. `ADVANC.BK`), `ohlcv` / `prices` (array of `{date, open, high, low, close, volume}`), `last_price`, `count`, `lastUpdated`.

### Stage classification (`classify()` in `hello.py`)

Scores 9 Minervini criteria (price vs MA50/150/200, MA slope, 52-week range) to assign one of four stages: Stage 1 (Basing), Stage 2 (Advancing), Stage 3 (Topping), Stage 4 (Declining). Stage 2 accordion is open by default.

### In-memory cache (`hello.py`)

A module-level `_cache` dict + threading lock holds fetched stocks for 1 hour. Force-clear via `GET /refresh`; inspect raw Firestore state via `GET /debug`.

## Running locally

```bash
# Web app — requires Application Default Credentials with Firestore access
pip install -r requirements.txt
python hello.py          # runs on port 8080 by default

# Fetcher — requires Settrade API credentials
cd fetcher
pip install -r requirements.txt
SETTRADE_APP_ID=... SETTRADE_APP_SECRET=... SETTRADE_BROKER_ID=... SETTRADE_APP_CODE=... python main.py
```

## Deployment

CI/CD is GitHub Actions → Cloud Run (region `asia-southeast1`):

- Pushing to `main` excluding `fetcher/**` → deploys the web service (`helloworld`)
- Pushing to `main` with changes in `fetcher/**` → deploys/updates the Cloud Run Job (`set50-fetcher`) and upserts the Cloud Scheduler trigger

Required GitHub secrets: `GCP_SA_KEY`, `GCP_PROJECT_ID`, `GCP_SA_EMAIL`, `SETTRADE_APP_ID`, `SETTRADE_APP_SECRET`, `SETTRADE_BROKER_ID`, `SETTRADE_APP_CODE`.

## Key conventions

- The web app HTML is a single inline `render_template_string` template inside `hello.py` — there are no separate template files.
- Ticker symbols are stored without the `.BK` suffix in the display but with it in Firestore (`ticker` field). The `analyze_doc()` function handles both `ohlcv`/`prices` and `ticker`/`symbol` field name variants for backwards compatibility.
- The fetcher sleeps 0.3 s between tickers to respect Settrade API rate limits.
- Minimum 50 OHLCV rows required for a stock to appear; MA150/MA200 are `None` if insufficient history.
