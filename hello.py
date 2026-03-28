from flask import Flask, render_template_string
import yfinance as yf
import numpy as np
import threading
from datetime import datetime

app = Flask(__name__)

SP100 = [
    "AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","BRK-B","UNH","XOM",
    "JNJ","JPM","V","PG","MA","HD","CVX","MRK","ABBV","LLY",
    "AVGO","COST","PEP","KO","WMT","MCD","CSCO","BAC","ABT","ACN",
    "CRM","TMO","DHR","NEE","TXN","LIN","PM","ADBE","VZ","NFLX",
    "WFC","NKE","BMY","RTX","ORCL","QCOM","HON","UPS","AMGN","IBM",
    "SBUX","CAT","GE","INTU","AMD","DE","BA","NOW","LOW","SPGI",
    "GS","ELV","BLK","AXP","MS","MDLZ","T","GILD","ADI","TJX",
    "C","CVS","ISRG","PLD","BKNG","SYK","MO","ZTS","TMUS","CB",
    "ADP","REGN","SO","MMC","EOG","BSX","CI","USB","DUK","PNC",
    "MU","VRTX","CL","ICE","TGT","MCO","CME","FCX","EMR","PANW",
]

STAGE_INFO = {
    1: {"label": "Stage 1 — Basing",       "color": "#60a5fa", "bg": "rgba(96,165,250,0.08)",  "border": "rgba(96,165,250,0.3)"},
    2: {"label": "Stage 2 — Advancing ✦",  "color": "#34d399", "bg": "rgba(52,211,153,0.08)",  "border": "rgba(52,211,153,0.3)"},
    3: {"label": "Stage 3 — Topping",       "color": "#fbbf24", "bg": "rgba(251,191,36,0.08)",  "border": "rgba(251,191,36,0.3)"},
    4: {"label": "Stage 4 — Declining",     "color": "#f87171", "bg": "rgba(248,113,113,0.08)", "border": "rgba(248,113,113,0.3)"},
}

_cache = {"stocks": None, "updated": None}
_lock  = threading.Lock()


def ma_slope(series, n=20):
    s = series.dropna()
    if len(s) < n:
        return 0.0
    y = s.iloc[-n:].values
    x = np.arange(n)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope / y[-1]) if y[-1] else 0.0


def classify(price, ma50, ma150, ma200, slope200, low52, high52):
    c1  = price  > ma200  if ma200  else False
    c2  = price  > ma150  if ma150  else False
    c3  = ma150  > ma200  if (ma150 and ma200) else False
    c4  = slope200 > 0
    c5  = ma50   > ma150  if ma150  else False
    c6  = ma50   > ma200  if ma200  else False
    c7  = price  > ma50
    c8  = price  >= low52  * 1.30
    c9  = price  >= high52 * 0.75
    score = sum([c1,c2,c3,c4,c5,c6,c7,c8,c9])

    if score >= 7 and c1 and c3:
        stage = 2
    elif c1 and c3 and not c7:
        stage = 3
    elif not c1 and slope200 < -0.0005:
        stage = 4
    elif not c1:
        stage = 1
    else:
        stage = 3
    return stage, score


def fetch_all():
    raw = yf.download(
        " ".join(SP100), period="1y",
        group_by="ticker", auto_adjust=True,
        progress=False, threads=True,
    )
    results = []
    for ticker in SP100:
        try:
            df = raw[ticker] if ticker in raw.columns.get_level_values(0) else None
            if df is None or df.empty:
                continue
            close = df["Close"].dropna()
            if len(close) < 60:
                continue

            price   = float(close.iloc[-1])
            prev    = float(close.iloc[-2]) if len(close) > 1 else price
            chg     = round((price / prev - 1) * 100, 2)
            high52  = float(df["High"].tail(252).max())
            low52   = float(df["Low"].tail(252).min())
            from_h  = round((price / high52 - 1) * 100, 1)

            ma50  = float(close.rolling(50).mean().iloc[-1])
            ma150 = float(close.rolling(150).mean().iloc[-1]) if len(close) >= 150 else None
            ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
            sl200 = ma_slope(close.rolling(200).mean(), 20) if ma200 else 0.0

            stage, score = classify(price, ma50, ma150, ma200, sl200, low52, high52)

            results.append({
                "ticker":   ticker,
                "price":    round(price, 2),
                "chg":      chg,
                "ma50":     round(ma50, 2),
                "ma150":    round(ma150, 2) if ma150 else None,
                "ma200":    round(ma200, 2) if ma200 else None,
                "high52":   round(high52, 2),
                "low52":    round(low52, 2),
                "from_h":   from_h,
                "score":    score,
                "stage":    stage,
            })
        except Exception:
            continue
    return sorted(results, key=lambda x: (-x["stage"] if x["stage"] != 2 else -99, -x["score"]))


def get_stocks():
    with _lock:
        now = datetime.now()
        stale = _cache["updated"] is None or (now - _cache["updated"]).seconds > 3600
        if stale:
            _cache["stocks"] = fetch_all()
            _cache["updated"] = now
        return _cache["stocks"], _cache["updated"]


HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Minervini Scanner — S&P 100</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0a0a0f;
      color: #e2e8f0;
      font-family: 'Segoe UI', system-ui, sans-serif;
      min-height: 100vh;
    }

    /* ── header ── */
    header {
      padding: 28px 32px 20px;
      border-bottom: 1px solid rgba(255,255,255,0.07);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }
    .logo { display: flex; align-items: center; gap: 12px; }
    .logo-icon {
      width: 40px; height: 40px; border-radius: 10px;
      background: linear-gradient(135deg,#a78bfa,#34d399);
      display: flex; align-items: center; justify-content: center;
      font-size: 20px;
    }
    h1 { font-size: 20px; font-weight: 700; color: #f1f5f9; }
    h1 span { color: #a78bfa; }
    .meta { font-size: 12px; color: #64748b; }

    /* ── tabs ── */
    .tabs {
      display: flex; gap: 8px;
      padding: 20px 32px 0;
      flex-wrap: wrap;
    }
    .tab {
      padding: 7px 18px; border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.1);
      background: transparent; color: #94a3b8;
      font-size: 13px; cursor: pointer;
      transition: all .2s;
    }
    .tab:hover, .tab.active {
      background: rgba(167,139,250,0.15);
      border-color: rgba(167,139,250,0.4);
      color: #a78bfa;
    }

    /* ── stage section ── */
    .stage-section { padding: 28px 32px; }
    .stage-header {
      display: flex; align-items: center; gap: 10px;
      margin-bottom: 16px;
    }
    .stage-dot {
      width: 10px; height: 10px; border-radius: 50%;
    }
    .stage-title { font-size: 15px; font-weight: 600; }
    .stage-count {
      font-size: 12px; color: #64748b;
      background: rgba(255,255,255,0.05);
      padding: 2px 8px; border-radius: 999px;
    }

    /* ── grid ── */
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 12px;
    }

    /* ── card ── */
    .card {
      border-radius: 14px;
      border: 1px solid var(--border);
      background: var(--bg);
      padding: 16px;
      transition: transform .15s, box-shadow .15s;
      cursor: default;
    }
    .card:hover {
      transform: translateY(-2px);
      box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }
    .card-top {
      display: flex; justify-content: space-between;
      align-items: flex-start; margin-bottom: 10px;
    }
    .ticker { font-size: 18px; font-weight: 800; color: var(--color); }
    .score-badge {
      font-size: 11px; padding: 2px 8px;
      border-radius: 999px;
      background: rgba(255,255,255,0.07);
      color: #94a3b8;
    }
    .price { font-size: 22px; font-weight: 700; color: #f1f5f9; margin-bottom: 4px; }
    .chg { font-size: 13px; font-weight: 600; }
    .chg.pos { color: #34d399; }
    .chg.neg { color: #f87171; }
    .stats {
      margin-top: 12px;
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 4px 8px; font-size: 11px; color: #64748b;
    }
    .stat-val { color: #94a3b8; font-weight: 500; }

    /* ── progress bar (score) ── */
    .score-bar { margin-top: 12px; }
    .score-bar-bg {
      height: 4px; border-radius: 999px;
      background: rgba(255,255,255,0.06);
      overflow: hidden;
    }
    .score-bar-fill {
      height: 100%; border-radius: 999px;
      background: var(--color);
      transition: width .4s ease;
    }

    /* ── from high pill ── */
    .from-high {
      display: inline-block; margin-top: 8px;
      font-size: 11px; padding: 2px 8px;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      color: #94a3b8;
    }

    /* ── divider ── */
    .divider {
      border: none;
      border-top: 1px solid rgba(255,255,255,0.05);
      margin: 0 32px;
    }

    /* ── empty state ── */
    .empty { color: #334155; font-size: 14px; padding: 8px 0; }

    /* ── summary bar ── */
    .summary {
      display: flex; gap: 24px; flex-wrap: wrap;
      padding: 16px 32px;
      border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .sum-item { display: flex; align-items: center; gap: 8px; }
    .sum-dot { width: 8px; height: 8px; border-radius: 50%; }
    .sum-label { font-size: 13px; color: #64748b; }
    .sum-val { font-size: 13px; font-weight: 700; }
  </style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">📈</div>
    <div>
      <h1><span>Minervini</span> Stage Scanner</h1>
      <div class="meta">S&P 100 · {{ total }} stocks · Updated {{ updated }}</div>
    </div>
  </div>
  <div class="meta">Trend Template Score /9</div>
</header>

<!-- summary -->
<div class="summary">
  {% for s, info in stage_info.items() %}
  <div class="sum-item">
    <div class="sum-dot" style="background:{{ info.color }}"></div>
    <span class="sum-label">Stage {{ s }}</span>
    <span class="sum-val" style="color:{{ info.color }}">{{ counts[s] }}</span>
  </div>
  {% endfor %}
</div>

<!-- tabs -->
<div class="tabs">
  <button class="tab active" onclick="filter(this, 0)">All</button>
  {% for s in [2,1,3,4] %}
  <button class="tab" onclick="filter(this, {{ s }})" style="--tc:{{ stage_info[s].color }}">
    Stage {{ s }}
  </button>
  {% endfor %}
</div>

<!-- stages — order: 2 first, then 1, 3, 4 -->
{% for s in [2,1,3,4] %}
{% set info = stage_info[s] %}
{% set group = stocks_by_stage[s] %}
<section class="stage-section" data-stage="{{ s }}">
  <div class="stage-header">
    <div class="stage-dot" style="background:{{ info.color }}"></div>
    <span class="stage-title" style="color:{{ info.color }}">{{ info.label }}</span>
    <span class="stage-count">{{ group|length }} stocks</span>
  </div>
  {% if group %}
  <div class="grid">
    {% for st in group %}
    <div class="card" style="--color:{{ info.color }};--bg:{{ info.bg }};--border:{{ info.border }}">
      <div class="card-top">
        <span class="ticker">{{ st.ticker }}</span>
        <span class="score-badge">{{ st.score }}/9</span>
      </div>
      <div class="price">${{ "%.2f"|format(st.price) }}</div>
      <span class="chg {{ 'pos' if st.chg >= 0 else 'neg' }}">
        {{ '+' if st.chg >= 0 else '' }}{{ st.chg }}%
      </span>
      <div class="stats">
        <span>MA50</span>   <span class="stat-val">${{ st.ma50 }}</span>
        <span>MA150</span>  <span class="stat-val">{{ '$'+st.ma150|string if st.ma150 else '—' }}</span>
        <span>MA200</span>  <span class="stat-val">{{ '$'+st.ma200|string if st.ma200 else '—' }}</span>
        <span>52W H</span>  <span class="stat-val">${{ st.high52 }}</span>
      </div>
      <div class="from-high">{{ st.from_h }}% from high</div>
      <div class="score-bar">
        <div class="score-bar-bg">
          <div class="score-bar-fill" style="width:{{ (st.score/9*100)|round }}%"></div>
        </div>
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
  <p class="empty">No stocks in this stage</p>
  {% endif %}
</section>
{% if not loop.last %}<hr class="divider">{% endif %}
{% endfor %}

<script>
function filter(btn, stage) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.stage-section').forEach(sec => {
    sec.style.display = (stage === 0 || +sec.dataset.stage === stage) ? '' : 'none';
  });
}
</script>
</body>
</html>
"""


@app.route("/")
def index():
    stocks, updated = get_stocks()
    by_stage = {1: [], 2: [], 3: [], 4: []}
    for s in stocks:
        by_stage.setdefault(s["stage"], []).append(s)
    # sort stage 2 by score desc
    for v in by_stage.values():
        v.sort(key=lambda x: -x["score"])

    return render_template_string(
        HTML,
        stocks_by_stage=by_stage,
        stage_info=STAGE_INFO,
        counts={k: len(v) for k, v in by_stage.items()},
        total=len(stocks),
        updated=updated.strftime("%d %b %Y %H:%M") if updated else "—",
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
