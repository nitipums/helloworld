"""
Thai Stock Fetcher — Cloud Run Job
1. ดึง ticker list จาก set.or.th (SET + MAI)
2. ดาวน์โหลดราคาย้อนหลัง 2 ปี จาก yfinance
3. บันทึกลง Firestore  collection: set50 / document: {TICKER.BK}
"""
import yfinance as yf
import firebase_admin
import requests
from firebase_admin import firestore
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Fallback list (ใช้เมื่อ SET API ไม่ตอบสนอง) ─────────────────────────────
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

BATCH_SIZE = 100

SET_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer":         "https://www.set.or.th/th/market/product/stock/list",
    "Origin":          "https://www.set.or.th",
}


def fetch_tickers_from_set() -> list[str] | None:
    """
    ดึง ticker list จาก SET API (SET + MAI)
    คืนค่าเป็น list เช่น ["ADVANC.BK", "AOT.BK", ...]
    คืน None ถ้า API ไม่ตอบสนอง
    """
    symbols = set()
    for market in ["SET", "mai"]:
        url = (
            f"https://www.set.or.th/api/set/stock/list"
            f"?market={market}&language=en"
        )
        try:
            r = requests.get(url, headers=SET_HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()

            # รองรับทั้ง 2 format ที่ SET อาจส่งมา
            if isinstance(data, list):
                raw_symbols = [
                    item.get("symbol") or item.get("ticker")
                    for item in data if isinstance(item, dict)
                ]
            elif isinstance(data, dict):
                raw_symbols = data.get("securitySymbols") or data.get("symbols") or []
                if raw_symbols and isinstance(raw_symbols[0], dict):
                    raw_symbols = [s.get("symbol") for s in raw_symbols]
            else:
                raw_symbols = []

            added = sum(1 for s in raw_symbols if s and symbols.add(f"{s}.BK") is None)
            print(f"  {market.upper()}: {added} tickers")

        except Exception as e:
            print(f"  {market.upper()} API error: {e}")

    return sorted(symbols) if symbols else None


def get_ticker_df(raw, ticker, n_tickers):
    """ดึง DataFrame ของ ticker เดียวจาก raw อย่าง robust"""
    if n_tickers == 1:
        return raw  # single ticker = flat DataFrame

    if raw.columns.nlevels == 1:
        return None  # unexpected structure

    lvl0 = raw.columns.get_level_values(0)
    lvl1 = raw.columns.get_level_values(1)

    if ticker in lvl1:      # yfinance 0.2.x default: (Price, Ticker)
        return raw.xs(ticker, axis=1, level=1)
    if ticker in lvl0:      # group_by='ticker': (Ticker, Price)
        return raw[ticker]
    return None


def process_batch(raw, tickers, db, batch, ops):
    ok = skip = 0

    # log column structure เพื่อ debug
    if raw is not None and not raw.empty:
        print(f"  [debug] nlevels={raw.columns.nlevels} "
              f"lvl0={list(raw.columns.get_level_values(0)[:4])} "
              f"lvl1={list(raw.columns.get_level_values(1)[:4]) if raw.columns.nlevels > 1 else []}")
    else:
        print(f"  [debug] raw is empty or None")
        return batch, ops, 0, len(tickers)

    for ticker in tickers:
        sym = ticker.replace(".BK", "")
        try:
            df = get_ticker_df(raw, ticker, len(tickers))

            if df is None or df.empty:
                print(f"  MISS {sym}: not in columns")
                skip += 1
                continue

            df = df.dropna(subset=["Close"]).sort_index()
            if len(df) < 50:
                print(f"  SHORT {sym}: only {len(df)} rows")
                skip += 1
                continue

            prices = [
                {
                    "date":   str(d.date()),
                    "open":   round(float(r["Open"]),  4),
                    "high":   round(float(r["High"]),  4),
                    "low":    round(float(r["Low"]),   4),
                    "close":  round(float(r["Close"]), 4),
                    "volume": int(r["Volume"]),
                }
                for d, r in df.iterrows()
            ]

            ref = db.collection("set50").document(ticker)
            batch.set(ref, {
                "ticker":      ticker,
                "prices":      prices,
                "lastUpdated": datetime.utcnow().isoformat(),
                "count":       len(prices),
            })
            ops += 1
            ok  += 1
            print(f"  OK  {sym}: {len(prices)} days")

            if ops >= 400:
                batch.commit()
                batch = db.batch()
                ops   = 0

        except Exception as e:
            print(f"  ERR {sym}: {e}")
            skip += 1

    return batch, ops, ok, skip


def download_one(ticker: str):
    """ดาวน์โหลด ticker เดียว — ใช้ Ticker.history() แทน bulk download"""
    sym = ticker.replace(".BK", "")
    try:
        df = yf.Ticker(ticker).history(period="2y", auto_adjust=True)
        if df.empty:
            return ticker, None, f"empty"
        df = df.dropna(subset=["Close"]).sort_index()
        if len(df) < 50:
            return ticker, None, f"only {len(df)} rows"
        return ticker, df, None
    except Exception as e:
        return ticker, None, str(e)


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
    print(f"\n[{datetime.utcnow().isoformat()}] Fetching ticker list from set.or.th…")
    tickers = fetch_tickers_from_set()
    if tickers:
        print(f"  ✓ SET API: {len(tickers)} tickers")
    else:
        tickers = [f"{t}.BK" for t in FALLBACK_TICKERS]
        print(f"  ⚠ Fallback list: {len(tickers)} tickers")

    # ── download แบบ parallel (Ticker.history ต่อตัว) ─────────────────────
    print(f"\nDownloading {len(tickers)} tickers (10 workers)…")
    total_ok = total_skip = 0
    batch = db.batch()
    ops   = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(download_one, t): t for t in tickers}
        done = 0
        for future in as_completed(futures):
            done += 1
            ticker, df, err = future.result()
            sym = ticker.replace(".BK", "")

            if df is None:
                print(f"  SKIP {sym}: {err}")
                total_skip += 1
                continue

            prices = [
                {
                    "date":   str(d.date()),
                    "open":   round(float(r["Open"]),   4),
                    "high":   round(float(r["High"]),   4),
                    "low":    round(float(r["Low"]),    4),
                    "close":  round(float(r["Close"]),  4),
                    "volume": int(r["Volume"]),
                }
                for d, r in df.iterrows()
            ]

            ref = db.collection("set50").document(ticker)
            batch.set(ref, {
                "ticker":      ticker,
                "prices":      prices,
                "lastUpdated": datetime.utcnow().isoformat(),
                "count":       len(prices),
            })
            ops       += 1
            total_ok  += 1
            print(f"  OK  {sym}: {len(prices)} days  [{done}/{len(tickers)}]")

            if ops >= 400:
                batch.commit()
                batch = db.batch()
                ops   = 0

    if ops > 0:
        batch.commit()

    print(f"\n{'='*40}")
    print(f"Done — saved: {total_ok}, skipped: {total_skip}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
