import threading
from datetime import datetime

import firebase_admin
import numpy as np
import pandas as pd
from firebase_admin import firestore as fs_admin
from flask import Flask, render_template_string

if not firebase_admin._apps:
    firebase_admin.initialize_app()
_db = fs_admin.client()

app = Flask(__name__)

STAGE_INFO = {
    1: {"label": "Stage 1 — Basing",      "color": "#2563eb", "light": "#eff6ff", "border": "#bfdbfe", "text": "#1d4ed8"},
    2: {"label": "Stage 2 — Advancing",   "color": "#16a34a", "light": "#f0fdf4", "border": "#bbf7d0", "text": "#15803d"},
    3: {"label": "Stage 3 — Topping",     "color": "#d97706", "light": "#fffbeb", "border": "#fde68a", "text": "#b45309"},
    4: {"label": "Stage 4 — Declining",   "color": "#dc2626", "light": "#fef2f2", "border": "#fecaca", "text": "#b91c1c"},
}

_cache = {"stocks": None, "updated": None, "fetched": None}
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
    c1 = bool(ma200 and price > ma200)
    c2 = bool(ma150 and price > ma150)
    c3 = bool(ma150 and ma200 and ma150 > ma200)
    c4 = slope200 > 0
    c5 = bool(ma150 and ma50 > ma150)
    c6 = bool(ma200 and ma50 > ma200)
    c7 = price > ma50
    c8 = price >= low52  * 1.30
    c9 = price >= high52 * 0.75
    score = sum([c1, c2, c3, c4, c5, c6, c7, c8, c9])

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


def analyze_doc(data):
    try:
        ticker  = data["ticker"]
        prices  = data["prices"]
        fetched = data.get("lastUpdated", "")

        df = pd.DataFrame(prices)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        close = df["close"]
        if len(close) < 50:
            return None

        price  = float(close.iloc[-1])
        prev   = float(close.iloc[-2]) if len(close) > 1 else price
        chg    = round((price / prev - 1) * 100, 2)
        high52 = float(df["high"].tail(252).max())
        low52  = float(df["low"].tail(252).min())
        from_h = round((price / high52 - 1) * 100, 1)

        ma50  = float(close.rolling(50).mean().iloc[-1])
        ma150 = float(close.rolling(150).mean().iloc[-1]) if len(close) >= 150 else None
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
        sl200 = ma_slope(close.rolling(200).mean(), 20) if ma200 else 0.0

        stage, score = classify(price, ma50, ma150, ma200, sl200, low52, high52)

        return {
            "ticker":  ticker.replace(".BK", ""),
            "price":   round(price, 2),
            "chg":     chg,
            "ma50":    round(ma50, 2),
            "ma150":   round(ma150, 2) if ma150 else None,
            "ma200":   round(ma200, 2) if ma200 else None,
            "high52":  round(high52, 2),
            "low52":   round(low52, 2),
            "from_h":  from_h,
            "score":   score,
            "stage":   stage,
            "fetched": fetched,
        }
    except Exception:
        return None


def load_from_firestore():
    docs    = _db.collection("set50").stream()
    results = []
    last_fetched = None
    for doc in docs:
        data   = doc.to_dict()
        result = analyze_doc(data)
        if result:
            results.append(result)
            ts = data.get("lastUpdated", "")
            if last_fetched is None or ts > last_fetched:
                last_fetched = ts
    return results, last_fetched


def get_stocks():
    with _lock:
        now   = datetime.now()
        stale = (
            _cache["stocks"] is None
            or len(_cache["stocks"]) == 0
            or (now - _cache["updated"]).seconds > 3600
        )
        if stale:
            stocks, fetched      = load_from_firestore()
            _cache["stocks"]     = stocks
            _cache["updated"]    = now
            _cache["fetched"]    = fetched
        return _cache["stocks"], _cache["updated"], _cache["fetched"]


HTML = """
<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Thai Stock Minervini Scanner</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #f1f5f9;
      color: #1e293b;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      min-height: 100vh;
    }

    /* ── header ── */
    header {
      background: #fff;
      border-bottom: 1px solid #e2e8f0;
      padding: 18px 32px;
      display: flex; align-items: center;
      justify-content: space-between; flex-wrap: wrap; gap: 12px;
      position: sticky; top: 0; z-index: 100;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    .logo { display: flex; align-items: center; gap: 12px; }
    .logo-icon {
      width: 38px; height: 38px; border-radius: 10px;
      background: linear-gradient(135deg,#2563eb,#16a34a);
      display: flex; align-items: center; justify-content: center;
      font-size: 18px;
    }
    h1 { font-size: 18px; font-weight: 700; color: #0f172a; }
    h1 span { color: #2563eb; }
    .meta { font-size: 12px; color: #94a3b8; margin-top: 2px; }

    /* ── summary pills ── */
    .summary {
      display: flex; gap: 8px; flex-wrap: wrap;
    }
    .pill {
      display: flex; align-items: center; gap: 6px;
      padding: 6px 14px; border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--bg);
      font-size: 13px;
    }
    .pill-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--color); }
    .pill-label { color: #64748b; }
    .pill-val   { font-weight: 700; color: var(--color); }

    /* ── main content ── */
    main { max-width: 1400px; margin: 0 auto; padding: 24px 24px; }

    /* ── stage accordion ── */
    .stage-group {
      background: #fff;
      border: 1px solid #e2e8f0;
      border-radius: 14px;
      margin-bottom: 12px;
      overflow: hidden;
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }

    .stage-header {
      display: flex; align-items: center; gap: 12px;
      padding: 16px 20px;
      cursor: pointer;
      user-select: none;
      transition: background .15s;
    }
    .stage-header:hover { background: #f8fafc; }

    .stage-badge {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 12px; border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--light);
      font-size: 13px; font-weight: 600;
      color: var(--text);
    }
    .stage-badge-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--color); }

    .stage-title { font-size: 15px; font-weight: 600; color: #0f172a; flex: 1; }

    .stage-count {
      font-size: 12px; color: #94a3b8;
      background: #f1f5f9;
      padding: 3px 10px; border-radius: 999px;
      border: 1px solid #e2e8f0;
    }

    .chevron {
      font-size: 12px; color: #94a3b8;
      transition: transform .2s;
    }
    .stage-group.open .chevron { transform: rotate(180deg); }

    /* ── grid body ── */
    .stage-body {
      border-top: 1px solid #f1f5f9;
      padding: 16px 20px 20px;
      display: none;
    }
    .stage-group.open .stage-body { display: block; }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(185px, 1fr));
      gap: 10px;
    }

    /* ── card ── */
    .card {
      background: #fff;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 14px;
      transition: box-shadow .15s, border-color .15s;
    }
    .card:hover {
      box-shadow: 0 4px 16px rgba(0,0,0,0.08);
      border-color: var(--color);
    }

    .card-top {
      display: flex; justify-content: space-between;
      align-items: center; margin-bottom: 8px;
    }
    .ticker-link {
      font-size: 16px; font-weight: 800;
      color: var(--color);
      text-decoration: none;
    }
    .ticker-link:hover { text-decoration: underline; }

    .score-badge {
      font-size: 11px; padding: 2px 8px;
      border-radius: 999px;
      background: var(--light);
      color: var(--text);
      border: 1px solid var(--border);
      font-weight: 600;
    }

    .price { font-size: 19px; font-weight: 700; color: #0f172a; }
    .chg   { font-size: 12px; font-weight: 600; margin-top: 1px; }
    .pos   { color: #16a34a; }
    .neg   { color: #dc2626; }

    .stats {
      margin-top: 10px;
      display: grid; grid-template-columns: auto 1fr;
      gap: 3px 10px; font-size: 11px;
    }
    .stat-key { color: #94a3b8; }
    .stat-val { color: #475569; font-weight: 500; text-align: right; }

    .from-high {
      margin-top: 8px; font-size: 11px;
      color: #94a3b8;
    }

    .score-bar    { margin-top: 8px; }
    .score-bar-bg {
      height: 3px; border-radius: 999px;
      background: #f1f5f9;
    }
    .score-bar-fill {
      height: 100%; border-radius: 999px;
      background: var(--color);
    }

    /* ── no data ── */
    .no-data {
      text-align: center; padding: 80px 32px;
      background: #fff; border-radius: 14px;
      border: 1px solid #e2e8f0;
    }
    .no-data h2 { font-size: 18px; color: #475569; margin-bottom: 8px; }
    .no-data p  { font-size: 14px; color: #94a3b8; }
  </style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <h1><span>Thai Stock</span> Minervini Scanner</h1>
      <div class="meta">{{ total }} หุ้น · ข้อมูลจาก {{ fetched or '—' }}</div>
    </div>
  </div>

  <div class="summary">
    {% for s in [2,1,3,4] %}
    {% set info = stage_info[s] %}
    <div class="pill" style="--color:{{ info.color }};--bg:{{ info.light }};--border:{{ info.border }}">
      <div class="pill-dot"></div>
      <span class="pill-label">Stage {{ s }}</span>
      <span class="pill-val">{{ counts[s] }}</span>
    </div>
    {% endfor %}
  </div>
</header>

<main>
{% if total == 0 %}
<div class="no-data">
  <h2>ยังไม่มีข้อมูล</h2>
  <p>รัน Fetcher Job ก่อนเพื่อดึงข้อมูลหุ้นไทยลง Firestore</p>
</div>
{% else %}

{% for s in [2,1,3,4] %}
{% set info  = stage_info[s] %}
{% set group = stocks_by_stage[s] %}
<div class="stage-group {{ 'open' if s == 2 else '' }}"
     id="stage-{{ s }}"
     style="--color:{{ info.color }};--light:{{ info.light }};--border:{{ info.border }};--text:{{ info.text }}">

  <div class="stage-header" onclick="toggle({{ s }})">
    <div class="stage-badge">
      <div class="stage-badge-dot"></div>
      Stage {{ s }}
    </div>
    <span class="stage-title">{{ info.label }}</span>
    <span class="stage-count">{{ group|length }} หุ้น</span>
    <span class="chevron">▼</span>
  </div>

  <div class="stage-body">
    {% if group %}
    <div class="grid">
      {% for st in group %}
      <div class="card">
        <div class="card-top">
          <a class="ticker-link"
             href="https://www.tradingview.com/symbols/SET-{{ st.ticker }}/"
             target="_blank" rel="noopener">{{ st.ticker }}</a>
          <span class="score-badge">{{ st.score }}/9</span>
        </div>
        <div class="price">฿{{ "%.2f"|format(st.price) }}</div>
        <div class="chg {{ 'pos' if st.chg >= 0 else 'neg' }}">
          {{ '+' if st.chg >= 0 else '' }}{{ st.chg }}%
        </div>
        <div class="stats">
          <span class="stat-key">MA50</span>
          <span class="stat-val">฿{{ st.ma50 }}</span>
          <span class="stat-key">MA150</span>
          <span class="stat-val">{{ '฿'+st.ma150|string if st.ma150 else '—' }}</span>
          <span class="stat-key">MA200</span>
          <span class="stat-val">{{ '฿'+st.ma200|string if st.ma200 else '—' }}</span>
          <span class="stat-key">52W H</span>
          <span class="stat-val">฿{{ st.high52 }}</span>
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
    <p style="color:#94a3b8;font-size:14px">ไม่มีหุ้นใน stage นี้</p>
    {% endif %}
  </div>
</div>
{% endfor %}

{% endif %}
</main>

<script>
function toggle(s) {
  const el = document.getElementById('stage-' + s);
  el.classList.toggle('open');
}
</script>
</body>
</html>
"""


@app.route("/")
def index():
    stocks, updated, fetched = get_stocks()

    by_stage = {1: [], 2: [], 3: [], 4: []}
    for s in stocks:
        by_stage[s["stage"]].append(s)
    for v in by_stage.values():
        v.sort(key=lambda x: -x["score"])

    fetched_str = None
    if fetched:
        try:
            fetched_str = datetime.fromisoformat(fetched).strftime("%d %b %Y %H:%M UTC")
        except Exception:
            fetched_str = fetched

    return render_template_string(
        HTML,
        stocks_by_stage=by_stage,
        stage_info=STAGE_INFO,
        counts={k: len(v) for k, v in by_stage.items()},
        total=len(stocks),
        fetched=fetched_str,
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
