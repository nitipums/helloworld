"""
Signalix — Signal Detection Module
Detects VCP (Volatility Contraction Pattern) and Pivot Breakout setups
based on Mark Minervini's SEPA methodology.
Joins fundamental data (D/E, RE/EQ, SET100) as a soft-filter badge.
"""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd


# ── Fundamental badge (soft filter) ──────────────────────────────────────────

def make_fund_badge(fund_doc: dict | None) -> dict:
    """
    Build a soft-filter badge from a fundamentals/ Firestore doc.
    quality: 'good' | 'warn' | 'na'
      good = D/E < 2 AND RE/EQ > 2
      warn = fails at least one criterion
      na   = data unavailable (signal still shown)
    """
    if not fund_doc:
        return {"quality": "na", "de_ratio": None, "re_ratio": None,
                "in_set100": False, "de_ok": None, "re_ok": None}

    de   = fund_doc.get("de_ratio")
    re   = fund_doc.get("re_ratio")
    s100 = bool(fund_doc.get("in_set100", False))

    de_ok = (de is not None and de < 2.0)
    re_ok = (re is not None and re > 2.0)

    if de is None and re is None:
        quality = "na"
    elif de_ok and re_ok:
        quality = "good"
    else:
        quality = "warn"

    return {
        "quality":   quality,
        "de_ratio":  round(de, 2) if de is not None else None,
        "re_ratio":  round(re, 2) if re is not None else None,
        "in_set100": s100,
        "de_ok":     de_ok,
        "re_ok":     re_ok,
    }


def load_fund_map(db) -> dict:
    """Load all fundamentals/{symbol} docs into a {symbol: doc} dict."""
    try:
        docs = db.collection("fundamentals").stream()
        return {d.id: d.to_dict() for d in docs}
    except Exception as e:
        print(f"[signals] fund_map load error: {e}")
        return {}


# ── helpers ──────────────────────────────────────────────────────────────────

def _atr(df: pd.DataFrame, period: int = 14) -> float:
    """Average True Range of the given slice."""
    if len(df) < period + 1:
        return 0.0
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat(
        [(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1
    ).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    return float(val) if pd.notna(val) else 0.0


def _pivot_high(df: pd.DataFrame, lookback: int = 25) -> float:
    """Highest close in the base (last `lookback` bars, excluding current)."""
    return float(df["close"].iloc[-(lookback + 1) : -1].max())


def _base_low(df: pd.DataFrame, lookback: int = 15) -> float:
    """Lowest low in the base (last `lookback` bars)."""
    return float(df["low"].tail(lookback).min())


# ── VCP ───────────────────────────────────────────────────────────────────────

def detect_vcp(df: pd.DataFrame, stock: dict) -> dict | None:
    """
    Volatility Contraction Pattern criteria:
      - Stage 2, Minervini score >= 7
      - Price within 15% of 52-week high
      - ATR has contracted >= 30% vs 20 bars ago (tighter base)
      - Risk per trade <= 15% (stop is within range)
    """
    if stock.get("stage") != 2 or stock.get("score", 0) < 7:
        return None
    if len(df) < 45:
        return None
    if stock["price"] < stock["high52"] * 0.85:
        return None  # too far from 52W high

    atr_now    = _atr(df.tail(22))
    atr_before = _atr(df.iloc[-44:-22])
    if atr_before == 0 or atr_now >= atr_before * 0.70:
        return None  # volatility not contracting enough

    pivot  = _pivot_high(df, 25)
    entry  = round(pivot * 1.005, 2)          # just above pivot
    stop   = round(_base_low(df, 15), 2)
    risk   = entry - stop
    if risk <= 0 or risk / entry > 0.15:
        return None

    target = round(entry + risk * 3, 2)
    return {
        "signal_type": "VCP",
        "entry":    entry,
        "stop":     stop,
        "target":   target,
        "rr":       round((target - entry) / risk, 1),
        "atr":      round(atr_now, 2),
        "pivot":    round(pivot, 2),
        "risk_pct": round(risk / entry * 100, 1),
    }


# ── Breakout ──────────────────────────────────────────────────────────────────

def detect_breakout(df: pd.DataFrame, stock: dict) -> dict | None:
    """
    Pivot Breakout criteria:
      - Stage 2, Minervini score >= 7
      - Today's close is AT or just above the 5-week pivot (within +3%)
      - Volume >= 150% of 20-day average (conviction)
      - Risk per trade <= 12%
    """
    if stock.get("stage") != 2 or stock.get("score", 0) < 7:
        return None
    if len(df) < 30:
        return None
    if "volume" not in df.columns:
        return None

    pivot = _pivot_high(df, 25)
    price = stock["price"]
    if price < pivot or price > pivot * 1.03:
        return None  # not at breakout point

    vol_now   = float(df["volume"].iloc[-1])
    vol_avg20 = float(df["volume"].iloc[-21:-1].mean())
    if vol_avg20 == 0 or vol_now < vol_avg20 * 1.5:
        return None  # no volume confirmation

    entry = round(price, 2)
    stop  = round(float(df["low"].iloc[-16:-1].min()), 2)
    risk  = entry - stop
    if risk <= 0 or risk / entry > 0.12:
        return None

    target = round(entry + risk * 3, 2)
    return {
        "signal_type": "Breakout",
        "entry":     entry,
        "stop":      stop,
        "target":    target,
        "rr":        round((target - entry) / risk, 1),
        "atr":       round(_atr(df), 2),
        "pivot":     round(pivot, 2),
        "risk_pct":  round(risk / entry * 100, 1),
        "vol_ratio": round(vol_now / vol_avg20, 1),
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def compute_signals(
    analyzed: list[dict],
    raw_by_symbol: dict[str, list],
    fund_map: dict | None = None,
) -> list[dict]:
    """
    Run signal detection on all analyzed stocks.

    Parameters
    ----------
    analyzed       : list of stock dicts produced by analyze_doc()
    raw_by_symbol  : {symbol: ohlcv_list}  (raw Firestore data)
    fund_map       : {symbol: fundamentals_doc}  optional — for badge only

    Returns
    -------
    list of signal dicts, sorted by R:R descending.
    Each dict includes a 'fund' badge (soft filter — never blocks signals).
    """
    if fund_map is None:
        fund_map = {}

    results = []
    now = datetime.now(timezone.utc).isoformat()

    for stock in analyzed:
        sym   = stock["ticker"]
        ohlcv = raw_by_symbol.get(sym)
        if not ohlcv:
            continue
        try:
            df = pd.DataFrame(ohlcv)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

            sig = detect_vcp(df, stock) or detect_breakout(df, stock)
            if sig:
                results.append({
                    "symbol":      sym,
                    "price":       stock["price"],
                    "chg":         stock["chg"],
                    "stage":       stock["stage"],
                    "score":       stock["score"],
                    "ma50":        stock["ma50"],
                    "high52":      stock["high52"],
                    "detected_at": now,
                    "fund":        make_fund_badge(fund_map.get(sym)),
                    **sig,
                })
        except Exception:
            continue

    # Sort: VCP first, then by R:R descending
    results.sort(key=lambda x: (0 if x["signal_type"] == "VCP" else 1, -x.get("rr", 0)))
    return results
