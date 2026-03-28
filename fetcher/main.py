"""
SET50 Fetcher — Cloud Run Job
ดึงข้อมูลหุ้น SET50 ย้อนหลัง 2 ปี แล้วบันทึกลง Firestore
collection: set50  /  document: {TICKER.BK}
"""
import yfinance as yf
import firebase_admin
from firebase_admin import firestore
from datetime import datetime

SET50 = [
    "ADVANC.BK", "AOT.BK",    "AWC.BK",    "BANPU.BK", "BAY.BK",
    "BBL.BK",    "BCP.BK",    "BDMS.BK",   "BEM.BK",   "BGRIM.BK",
    "BH.BK",     "BJC.BK",    "BTS.BK",    "CBG.BK",   "CENTEL.BK",
    "COM7.BK",   "CPALL.BK",  "CPF.BK",    "CPN.BK",   "CRC.BK",
    "DELTA.BK",  "EA.BK",     "EGCO.BK",   "GLOBAL.BK","GPSC.BK",
    "GULF.BK",   "HMPRO.BK",  "INTUCH.BK", "IVL.BK",   "KBANK.BK",
    "KTB.BK",    "KTC.BK",    "LH.BK",     "MINT.BK",  "MTC.BK",
    "OR.BK",     "OSP.BK",    "PTT.BK",    "PTTEP.BK", "PTTGC.BK",
    "RATCH.BK",  "SAWAD.BK",  "SCB.BK",    "SCC.BK",   "SCGP.BK",
    "TISCO.BK",  "TOP.BK",    "TRUE.BK",   "TTB.BK",   "WHA.BK",
]


def main():
    firebase_admin.initialize_app()
    db = firestore.client()

    print(f"[{datetime.utcnow().isoformat()}] Downloading SET50 (2y)…")
    raw = yf.download(
        " ".join(SET50),
        period="2y",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    ok, skip = 0, 0
    batch = db.batch()
    ops = 0

    for ticker in SET50:
        try:
            df = raw[ticker] if ticker in raw.columns.get_level_values(0) else None
            if df is None or df.empty:
                print(f"  SKIP {ticker}: no data")
                skip += 1
                continue

            df = df.dropna(subset=["Close"]).sort_index()

            prices = [
                {
                    "date":   str(date.date()),
                    "open":   round(float(row["Open"]),   4),
                    "high":   round(float(row["High"]),   4),
                    "low":    round(float(row["Low"]),    4),
                    "close":  round(float(row["Close"]),  4),
                    "volume": int(row["Volume"]),
                }
                for date, row in df.iterrows()
            ]

            ref = db.collection("set50").document(ticker)
            batch.set(ref, {
                "ticker":      ticker,
                "prices":      prices,
                "lastUpdated": datetime.utcnow().isoformat(),
                "count":       len(prices),
            })
            ops += 1
            ok += 1
            print(f"  OK  {ticker}: {len(prices)} days")

            # Firestore batch limit = 500 ops
            if ops >= 400:
                batch.commit()
                batch = db.batch()
                ops = 0

        except Exception as e:
            print(f"  ERR {ticker}: {e}")
            skip += 1

    if ops > 0:
        batch.commit()

    print(f"\nDone — saved: {ok}, skipped: {skip}")


if __name__ == "__main__":
    main()
