"""
Signalix — Fundamentals Fetcher (Cloud Run Job)
Runs weekly (Monday 08:00 BKK) via Cloud Scheduler.

1. Fetches SET100 constituent list → Firestore meta/set100
2. For each stock in Firestore stocks/, fetches balance sheet data
   from SET public API → computes D/E ratio and RE/EQ ratio
3. Saves result to Firestore fundamentals/{symbol}
"""
from __future__ import annotations

import os
import time
from datetime import datetime

import firebase_admin
import requests
from firebase_admin import firestore

SET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.set.or.th/th/market/product/stock/list",
}


# ── SET100 ────────────────────────────────────────────────────────────────────

def fetch_set100_members() -> list[str]:
    """Fetch SET100 constituent symbols from SET API."""
    try:
        url = "https://www.set.or.th/api/set/index/SET100/constituent?lang=en"
        r   = requests.get(url, headers=SET_HEADERS, timeout=30)
        r.raise_for_status()
        data    = r.json()
        symbols = []
        # Response shape may vary — handle list or dict
        if isinstance(data, list):
            symbols = [item.get("symbol") for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            constituents = (data.get("constituent")
                            or data.get("securities")
                            or data.get("securitySymbols")
                            or [])
            if constituents and isinstance(constituents[0], dict):
                symbols = [c.get("symbol") for c in constituents]
            else:
                symbols = [str(c) for c in constituents]
        symbols = [s for s in symbols if s]
        print(f"  SET100: {len(symbols)} members")
        return symbols
    except Exception as e:
        print(f"  SET100 fetch error: {e}")
        return []


# ── Financial statements ──────────────────────────────────────────────────────

def fetch_financials(symbol: str) -> dict | None:
    """
    Fetch latest balance sheet from SET API.
    Extracts total liabilities, shareholders' equity, and retained earnings.
    Computes D/E = total_liabilities / equity and RE/EQ = retained_earnings / equity.
    """
    url = (f"https://www.set.or.th/api/set/stock/{symbol}"
           f"/financialstatement?type=balance-sheet&lang=en")
    try:
        r = requests.get(url, headers=SET_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  {symbol}: fetch error — {e}")
        return None

    # Navigate to latest period data (structure varies; try common shapes)
    try:
        periods = (data.get("periods")
                   or data.get("financialStatement")
                   or data.get("data")
                   or [])

        if not periods:
            return None

        # Use most recent period (last element)
        latest = periods[-1] if isinstance(periods, list) else None
        if not latest:
            return None

        fiscal_quarter = (latest.get("periodEndDate")
                          or latest.get("quarter")
                          or latest.get("period", ""))

        items = (latest.get("items") or latest.get("financialItems") or [])

        # Build a quick lookup by account name (case-insensitive partial match)
        account_map: dict[str, float] = {}
        for item in items:
            name  = (item.get("accountName") or item.get("name") or "").lower()
            value = item.get("value") or item.get("amount")
            if value is not None:
                try:
                    account_map[name] = float(value)
                except (TypeError, ValueError):
                    pass

        def _find(*keywords: str) -> float | None:
            """Find first matching account by keyword(s)."""
            for kw in keywords:
                kw_l = kw.lower()
                for name, val in account_map.items():
                    if kw_l in name:
                        return val
            return None

        equity   = _find("total shareholders", "total equity", "shareholders' equity")
        liab     = _find("total liabilities")
        retained = _find("retained earnings", "unappropriated retained")

        if equity is None or equity == 0:
            return None

        de_ratio = round(liab / equity, 2)       if liab     is not None else None
        re_ratio = round(retained / equity, 2)   if retained is not None else None

        return {
            "de_ratio":       de_ratio,
            "re_ratio":       re_ratio,
            "fiscal_quarter": str(fiscal_quarter)[:10],
        }

    except Exception as e:
        print(f"  {symbol}: parse error — {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    firebase_admin.initialize_app()
    db = firestore.client()

    # 1. Fetch SET100 members
    print(f"\n[{datetime.utcnow().isoformat()}] Fetching SET100 members…")
    set100 = set(fetch_set100_members())

    if set100:
        db.collection("meta").document("set100").set({
            "members":    sorted(set100),
            "count":      len(set100),
            "updated_at": datetime.utcnow().isoformat(),
        })
        print(f"  ✅ Saved {len(set100)} SET100 members → meta/set100")
    else:
        # Load from cache if fetch failed
        doc = db.collection("meta").document("set100").get()
        if doc.exists:
            set100 = set(doc.to_dict().get("members", []))
            print(f"  ⚠️  Using cached SET100 ({len(set100)} members)")

    # 2. Get all stock symbols from Firestore
    print(f"\n[{datetime.utcnow().isoformat()}] Loading stock list…")
    try:
        symbols = [d.id for d in db.collection("stocks").stream()]
        print(f"  {len(symbols)} stocks found")
    except Exception as e:
        print(f"  Error loading stocks: {e}")
        return

    # 3. Fetch fundamentals for each symbol
    print(f"\n[{datetime.utcnow().isoformat()}] Fetching fundamentals…")
    success = fail = skip = 0

    for symbol in symbols:
        fin = fetch_financials(symbol)
        if fin is None:
            fail += 1
            time.sleep(0.3)
            continue

        doc = {
            "symbol":         symbol,
            "in_set100":      symbol in set100,
            "updated_at":     datetime.utcnow().isoformat(),
        }
        if fin.get("de_ratio") is not None:
            doc["de_ratio"]       = fin["de_ratio"]
        if fin.get("re_ratio") is not None:
            doc["re_ratio"]       = fin["re_ratio"]
        if fin.get("fiscal_quarter"):
            doc["fiscal_quarter"] = fin["fiscal_quarter"]

        db.collection("fundamentals").document(symbol).set(doc, merge=True)

        de_s  = f"D/E {fin.get('de_ratio', '—')}"
        re_s  = f"RE/EQ {fin.get('re_ratio', '—')}"
        s100s = "SET100" if symbol in set100 else ""
        print(f"  ✅ {symbol}: {de_s}  {re_s}  {s100s}")
        success += 1
        time.sleep(0.4)   # rate-limit: ~150 req/min

    print(f"\n{'='*40}")
    print(f"Fundamentals done — ok: {success}, failed: {fail}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
