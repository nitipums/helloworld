"""
Thai Stock Fetcher — Cloud Run Job
ดึงข้อมูลหุ้นไทยทุกตัวย้อนหลัง 2 ปี แล้วบันทึกลง Firestore
collection: set50  /  document: {TICKER.BK}
"""
import yfinance as yf
import firebase_admin
from firebase_admin import firestore
from datetime import datetime

# หุ้นไทยทุกตัวบน SET + MAI (~300 ตัว)
ALL_THAI = [
    # SET50
    "ADVANC","AOT","AWC","BANPU","BAY","BBL","BCP","BDMS","BEM","BGRIM",
    "BH","BJC","BTS","CBG","CENTEL","COM7","CPALL","CPF","CPN","CRC",
    "DELTA","EA","EGCO","GLOBAL","GPSC","GULF","HMPRO","INTUCH","IVL","KBANK",
    "KTB","KTC","LH","MINT","MTC","OR","OSP","PTT","PTTEP","PTTGC",
    "RATCH","SAWAD","SCB","SCC","SCGP","TISCO","TOP","TRUE","TTB","WHA",
    # SET100 (non-SET50)
    "AEONTS","AMATA","AP","BAM","BIG","BLA","BTG","CHG","CK","CKP",
    "CPAXT","DOHOME","EASTW","ERW","GFPT","GGC","IRPC","JMT","JWD","KCE",
    "KKP","LHFG","MAJOR","MAKRO","MALEE","NMG","ORI","PLANB","PRM","PSH",
    "PSL","PTG","ROBINS","RS","SAT","SPRC","STA","STGT","SUPER","SVI",
    "SYNEX","TFG","THAI","THANI","TIDLOR","TOA","TQM","TU","VGI","WHAUP",
    # SET ขนาดกลาง-เล็ก
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
    "TRT","TSC","TTA","TTO","TWZ","U","UNIQ","UV","XO","YUASA",
    "ZEN","SAPPE","TKN","NPS","NER","NCL","LIT","GPC","EMC","HFT",
    "IEC","INN","IRC","LEE","MCS","MJD","NCH","NEW","NYT","PAW",
    "PIMO","PPP","PRIN","PSTC","RICHY","ROH","SGP","SIS","SKE","SLIC",
    "SMIT","SMK","SNP","STEL","SW","SYMC","TCJ","TH","THEP","TIC",
    "TIW","TMT","TNITY","TNL","TPP","TRITN","TRU","TSR","TTT","TYM",
    "UPF","UTP","VIBHA","WINNER","WR","FMT","FER","DCC","CMO","CNT",
    "BUI","HYDRO","KBS","MUANGTHAI","PDG","PCSGH","SARACH","SAWANG","SITHAI",
    "SKT","SPEEDA","SPG","SRICHA","SRN","SSF","SST","SSUP","STPI","SWC",
    "TCOAT","THAIVIVAT","THIP","THRE","TLGF","TMI","TMW","TOG","TPRICE",
    "TTO","TVDH","UMS","UP","GJS","GL","GLAND","GLG","GMMZ","GOLDEI",
    "GYT","JSP","KAMART","LHHOTEL","LUXF","MBAX","MBK","MBKET","META","MIDA",
    "MJLF","MKTH","MMC","MOUNT","MPIC","MTI","NAIINS","NEQ","NETBAY","NRF",
    "NSI","NTV","OGC","PACE","PAP","PATO","PDI","PERM","PEA","PKG",
    "PLCL","PPC","PRAKIT","PTGC","PTL","Q-CON","RAMA","RBF","RINENG","RML",
    "ROH","RPC","RPCL","RUSSEL","SAMCO","SANITHAI","SCG","SEAOIL","SECURE",
    "SITHAI","SKN","SLICT","SMPC","SOLAR","SORKON","SPPT","SUSCO","SVH",
    "VGI","WHAUP","YCI",
]

# ลบ duplicate
ALL_THAI = list(dict.fromkeys(ALL_THAI))
TICKERS   = [f"{t}.BK" for t in ALL_THAI]
BATCH_SIZE = 100


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

    total_ok = total_skip = 0
    batch = db.batch()
    ops   = 0

    # แบ่ง batch เพื่อไม่ให้ timeout
    chunks = [TICKERS[i:i+BATCH_SIZE] for i in range(0, len(TICKERS), BATCH_SIZE)]
    print(f"[{datetime.utcnow().isoformat()}] {len(TICKERS)} tickers → {len(chunks)} batches")

    for i, chunk in enumerate(chunks, 1):
        print(f"\nBatch {i}/{len(chunks)} ({len(chunk)} tickers)…")
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

    print(f"\nDone — saved: {total_ok}, skipped: {total_skip}")


if __name__ == "__main__":
    main()
