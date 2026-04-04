"""
Signalix — Market Breadth Module
Computes breadth indicators and overall market condition assessment.
"""
from __future__ import annotations

from datetime import datetime, timezone


# ── Condition mapping ─────────────────────────────────────────────────────────

CONDITION_META = {
    "Confirmed Uptrend":      {"color": "#15803d", "bg": "#f0fdf4", "emoji": "🟢"},
    "Uptrend Under Pressure": {"color": "#d97706", "bg": "#fffbeb", "emoji": "🟡"},
    "Rally Attempt":          {"color": "#ea580c", "bg": "#fff7ed", "emoji": "🟠"},
    "Downtrend":              {"color": "#dc2626", "bg": "#fef2f2", "emoji": "🔴"},
}


def _market_condition(pct_above_200: float, stage2_pct: float) -> str:
    """
    Classify overall market condition using breadth thresholds.
    Loosely mirrors IBD Market Status methodology.
    """
    if pct_above_200 >= 50 and stage2_pct >= 25:
        return "Confirmed Uptrend"
    elif pct_above_200 >= 35:
        return "Uptrend Under Pressure"
    elif pct_above_200 >= 20:
        return "Rally Attempt"
    else:
        return "Downtrend"


# ── Main entry point ──────────────────────────────────────────────────────────

def compute_breadth(analyzed: list[dict]) -> dict:
    """
    Compute market breadth from a list of analyzed stock dicts.

    Parameters
    ----------
    analyzed : list of dicts from analyze_doc()

    Returns
    -------
    breadth snapshot dict
    """
    total = len(analyzed)
    if total == 0:
        return {
            "total": 0,
            "market_condition": "No Data",
            "above_ma50_pct":  0,
            "above_ma150_pct": 0,
            "above_ma200_pct": 0,
            "stage1_count": 0, "stage2_count": 0,
            "stage3_count": 0, "stage4_count": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    above_50  = sum(1 for s in analyzed if s.get("ma50")  and s["price"] > s["ma50"])
    above_150 = sum(1 for s in analyzed if s.get("ma150") and s["price"] > s["ma150"])
    above_200 = sum(1 for s in analyzed if s.get("ma200") and s["price"] > s["ma200"])

    # Denominator for MA150/200: only stocks with enough history
    has_150 = sum(1 for s in analyzed if s.get("ma150")) or total
    has_200 = sum(1 for s in analyzed if s.get("ma200")) or total

    pct50  = round(above_50  / total  * 100, 1)
    pct150 = round(above_150 / has_150 * 100, 1)
    pct200 = round(above_200 / has_200 * 100, 1)

    stage_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for s in analyzed:
        stage_counts[s.get("stage", 1)] += 1

    stage2_pct = round(stage_counts[2] / total * 100, 1)
    condition  = _market_condition(pct200, stage2_pct)

    # Advance / Decline
    advancing = sum(1 for s in analyzed if s.get("chg", 0) > 0)
    declining = sum(1 for s in analyzed if s.get("chg", 0) < 0)

    return {
        "above_ma50_pct":  pct50,
        "above_ma150_pct": pct150,
        "above_ma200_pct": pct200,
        "stage1_count":    stage_counts[1],
        "stage2_count":    stage_counts[2],
        "stage3_count":    stage_counts[3],
        "stage4_count":    stage_counts[4],
        "stage2_pct":      stage2_pct,
        "advancing":       advancing,
        "declining":       declining,
        "total":           total,
        "market_condition": condition,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }
