"""
Thai Stock Fetcher — Cloud Run Job (Settrade Open API)
1. ดึง ticker list จาก set.or.th (SET + MAI) หรือ fallback
2. ดาวน์โหลดราคาย้อนหลัง 2 ปี จาก Settrade Open API
3. บันทึกลง Firestore  collection: set50 / document: {TICKER}
"""
import os
import time
import requests
import firebase_admin
from firebase_admin import firestore
from datetime import datetime
from settrade_v2 import Investor

# ── Fallback list ────────────────────────────────────────────────────────────
FALLBACK_TICKERS = [
    "ADVANC","AOT","AWC","BANPU","BAY","BBL","BCP","BDMS","BEM","BGRIM",
    "BH","BJC","BTS","CBG","CENTEL","COM7","CPALL","CPF","CPN","CRC",
    "DELTA","EA","EGCO","GLOBAL","GPSC","GULF","HMPRO","INTUCH","IVL","KBANK",
    "KTB","KTC","LH","MINT","MTC","OR","OSP","PTT","PTTEP","PTTGC",
    "RATCH","SAWAD","SCB","SCC","SCGP","TISCO","TOP","TRUE","TTB","WHA",
    "AEONTS","AMATA","AP","BAM","BIG","BLA","BTG","CHG","CK","CKP",
    "CPAXT","DOHOME","EASTW","ERW","GFPT","GGC","IRPC","JMT","JWD","KCE",
    "KKP","LHFG","MAJOR","MAKRO","MALEE","NMG","ORI","PLANB","PRM","PSH",
    "PSL","PTG","ROBINS","RS","SAT","SPRC","STA","STGT","SUPER","SVI",
    "SYNEX","TFG","THAI","THANI","TIDLOR","TOA","TQM","TU","VGI","WHAUP",
    "AAV","ACE","AIT","AJ","ASK","BAFS","BCH","BEC","BEAUTY","BLAND",
    "BOL","BROOK","CIMBT","COL","DEMCO","DUSIT","EPG","ESSO","EVER","FARM",
    "FORTH","FVC","GEL","GEN","GMM","GOLD","HANA","HUMAN","ICHI","INET",
    "IOD","ITD","JMART","K","KBS","KDH","KSL","KTIS","LALIN","LPN",
    "M","MACO","MATCH","MBK","MDX","MILL","MK","MONO","NOBLE","NOK",
    "OCC","OISHI","ONEE","PAP","PB","PCSGH","PIZZA","PKG","PL","PMTA",
    "PT","PYLON","RAIMON","RCL","RPH","S","SABINA","SAMART","SAUCE","SC",
    "SCCC","SEAFCO","SFP","SHR","SINGER","SKR","SOLAR","SPALI","STEC","SSL",
    "SSP","STANLY","STEEL","SUC","SUN","SVH","SVOA","T","TCAP","TFD",
    "TFI","TGH","THANA","THE","TIPCO","TK","TNP","TOG","TPAC","TRC",
    "TRT","TSC","TTA","TTO","TWZ","U","UNIQ","UV","XO","YUASA","ZEN",
]

SET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.set.or.th/th/market/product/stock/list",
}


def fetch_tickers_from_set() -> list[str] | None:
    symbols = set()
    for market in ["SET", "mai"]:
        url = f"https://www.set.or.th/api/set/stock/list?market={market}&language=en"
        try:
            r = requests.get(url, headers=SET_HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                raw = [item.get("symbol") or item.get("ticker") for item in data if isinstance(item, dict)]
            elif isinstance(data, dict):
                raw = data.get("securitySymbols") or data.get("symbols") or []
                if raw and isinstance(raw[0], dict):
                    raw = [s.get("symbol") for s in raw]
            else:
                raw = []
            added = sum(1 for s in raw if s and symbols.add(s) is None)
            print(f"  {market.upper()}: {added} tickers")
        except Exception as e:
            print(f"  {market.upper()} API error: {e}")
    return sorted(symbols) if symbols else None


def get_investor() -> Investor:
    return Investor(
        app_id=os.environ["SETTRADE_APP_ID"],
        app_secret=os.environ["SETTRADE_APP_SECRET"],
        broker_id=os.environ["SETTRADE_BROKER_ID"],
        app_code=os.environ["SETTRADE_APP_CODE"],
        is_auto_queue=False,
    )


def fetch_and_store(investor: Investor, symbol: str, db) -> bool:
    """ดึง candlestick รายวันย้อนหลัง ~2 ปี แล้วบันทึก Firestore"""
    try:
        # limit=520 ≈ 2 ปีของวันทำการ (252 วัน/ปี)
        market_data = investor.MarketData()
        candles = market_data.get_candlestick(symbol=symbol, interval="1D", limit=520)

        if not candles or len(candles) == 0:
            print(f"  ⚠ {symbol}: no data")
            return False

        ohlcv = [
            {
                "date":   c.get("datetime", c.get("date", ""))[:10],
                "open":   round(float(c.get("open",   0)), 2),
                "high":   round(float(c.get("high",   0)), 2),
                "low":    round(float(c.get("low",    0)), 2),
                "close":  round(float(c.get("close",  0)), 2),
                "volume": int(c.get("volume", 0)),
            }
            for c in candles
            if c.get("close") and float(c.get("close", 0)) > 0
        ]

        if len(ohlcv) < 50:
            print(f"  ⚠ {symbol}: only {len(ohlcv)} rows")
            return False

        db.collection("set50").document(symbol).set({
            "symbol":      symbol,
            "ticker":      f"{symbol}.BK",
            "ohlcv":       ohlcv,
            "prices":      ohlcv,
            "last_price":  ohlcv[-1]["close"] if ohlcv else 0,
            "count":       len(ohlcv),
            "lastUpdated": datetime.utcnow().isoformat(),
        })
        print(f"  ✅ {symbol}: {len(ohlcv)} days")
        return True

    except Exception as e:
        print(f"  ❌ {symbol}: {e}")
        return False


def main():
    firebase_admin.initialize_app()
    db = firestore.client()

    # ── init Settrade ──────────────────────────────────────────────────────
    print("Connecting to Settrade Open API…")
    try:
        investor = get_investor()
        print("  ✓ Connected")
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        return

    # ── ดึง ticker list ────────────────────────────────────────────────────
    print(f"\n[{datetime.utcnow().isoformat()}] Fetching ticker list…")
    symbols = fetch_tickers_from_set()
    if symbols:
        print(f"  ✓ SET API: {len(symbols)} tickers")
    else:
        symbols = FALLBACK_TICKERS
        print(f"  ⚠ Fallback: {len(symbols)} tickers")

    # ── download & store ───────────────────────────────────────────────────
    start   = time.time()
    success = fail = 0

    print(f"\nDownloading {len(symbols)} tickers…")
    for symbol in symbols:
        if fetch_and_store(investor, symbol, db):
            success += 1
        else:
            fail += 1
        time.sleep(0.3)

    duration = time.time() - start
    print(f"\n{'='*40}")
    print(f"Done — saved: {success}, failed: {fail}")
    print(f"Duration: {duration:.1f}s")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
