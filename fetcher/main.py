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


def process_batch(raw, tickers, db, batch, ops):
    ok = skip = 0
    for ticker in tickers:
        sym = ticker.replace(".BK", "")
        try:
            if len(tickers) == 1:
                df = raw
            else:
                lvl = raw.columns.get_level_values(0)
                df  = raw[ticker] if ticker in lvl else None

            if df is None or df.empty:
                skip += 1
                continue

            df = df.dropna(subset=["Close"]).sort_index()
            if len(df) < 50:
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


def main():
    firebase_admin.initialize_app()
    db = firestore.client()

    # 1. ดึง ticker list จาก SET
    print(f"[{datetime.utcnow().isoformat()}] Fetching ticker list from set.or.th…")
    tickers = fetch_tickers_from_set()

    if tickers:
        print(f"  ✓ SET API: {len(tickers)} tickers total")
    else:
        tickers = [f"{t}.BK" for t in FALLBACK_TICKERS]
        print(f"  ⚠ Using fallback list: {len(tickers)} tickers")

    # 2. Download ราคาย้อนหลัง 2 ปี (batch ละ 100)
    chunks     = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total_ok   = total_skip = 0
    batch      = db.batch()
    ops        = 0

    print(f"\nDownloading {len(tickers)} tickers in {len(chunks)} batches…")
    for i, chunk in enumerate(chunks, 1):
        print(f"\nBatch {i}/{len(chunks)} ({len(chunk)} tickers)")
        try:
            raw = yf.download(
                " ".join(chunk), period="2y",
                group_by="ticker", auto_adjust=True,
                progress=False, threads=True,
            )
            batch, ops, ok, skip = process_batch(raw, chunk, db, batch, ops)
            total_ok   += ok
            total_skip += skip
        except Exception as e:
            print(f"  Batch error: {e}")

    if ops > 0:
        batch.commit()

    print(f"\n{'='*40}")
    print(f"Done — saved: {total_ok}, skipped: {total_skip}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
