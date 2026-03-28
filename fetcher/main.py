"""
Thai Stock Fetcher — Cloud Run Job
1. ดึง ticker list จาก set.or.th (SET + MAI) หรือ fallback
2. ดาวน์โหลดราคาย้อนหลัง 2 ปี จาก yfinance (sequential + sleep เพื่อหลีกเลี่ยง rate limit)
3. บันทึกลง Firestore  collection: set50 / document: {TICKER} (ไม่มี .BK)
"""
import time
import yfinance as yf
import firebase_admin
import requests
from firebase_admin import firestore
from datetime import datetime

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer":         "https://www.set.or.th/th/market/product/stock/list",
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


def fetch_and_store(symbol: str, db) -> bool:
    """ดึงข้อมูล 1 ตัวจาก Yahoo Finance แล้วบันทึก Firestore (retry 3 ครั้ง)"""
    sym = symbol.replace(".BK", "")

    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol)
            hist   = ticker.history(period="2y")

            if hist.empty:
                print(f"  ⚠ {sym}: no data")
                return False

            # ดึงชื่อและ sector (optional)
            try:
                info      = ticker.info
                full_name = info.get("longName", sym)
                sector    = info.get("sector", "N/A")
            except Exception:
                full_name = sym
                sector    = "N/A"

            ohlcv = [
                {
                    "date":   index.strftime("%Y-%m-%d"),
                    "open":   round(float(row["Open"]),   2),
                    "high":   round(float(row["High"]),   2),
                    "low":    round(float(row["Low"]),    2),
                    "close":  round(float(row["Close"]),  2),
                    "volume": int(row["Volume"]),
                }
                for index, row in hist.iterrows()
            ]

            db.collection("set50").document(sym).set({
                "symbol":      sym,
                "ticker":      symbol,
                "full_name":   full_name,
                "sector":      sector,
                "ohlcv":       ohlcv,
                "prices":      ohlcv,
                "last_price":  round(float(hist["Close"].iloc[-1]), 2),
                "count":       len(ohlcv),
                "lastUpdated": datetime.utcnow().isoformat(),
            })
            print(f"  ✅ {sym}: {len(ohlcv)} days")
            return True

        except Exception as e:
            wait = 2 ** (attempt + 1)   # 2s, 4s, 8s
            print(f"  ⚠ {sym} attempt {attempt+1}/3: {e} — retry in {wait}s")
            time.sleep(wait)

    print(f"  ❌ {sym}: failed after 3 attempts")
    return False


def main():
    firebase_admin.initialize_app()
    db = firestore.client()

    # ── connectivity test ──────────────────────────────────────────────────
    print("Testing connectivity…")
    for test_sym in ["AAPL", "ADVANC.BK"]:
        try:
            t = yf.Ticker(test_sym).history(period="5d")
            print(f"  {test_sym}: {len(t)} rows ✓")
        except Exception as e:
            print(f"  {test_sym}: ERROR — {e}")

    # ── ดึง ticker list ────────────────────────────────────────────────────
    print(f"\n[{datetime.utcnow().isoformat()}] Fetching ticker list…")
    symbols = fetch_tickers_from_set()
    if symbols:
        print(f"  ✓ SET API: {len(symbols)} tickers")
    else:
        symbols = FALLBACK_TICKERS
        print(f"  ⚠ Fallback: {len(symbols)} tickers")

    # ── download & store (sequential + sleep 0.5s) ─────────────────────────
    start = time.time()
    print(f"\nStarting download for {len(symbols)} tickers…")
    success = fail = 0

    for symbol in symbols:
        formatted = symbol if symbol.endswith(".BK") else f"{symbol}.BK"
        if fetch_and_store(formatted, db):
            success += 1
        else:
            fail += 1
        time.sleep(1.5)   # ป้องกัน Yahoo Finance rate limit

    duration = time.time() - start
    print(f"\n{'='*40}")
    print(f"Done — saved: {success}, failed: {fail}")
    print(f"Duration: {duration:.1f}s")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
