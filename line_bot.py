"""
Signalix — LINE Bot Module
Handles webhook events, push notifications, LIFF HTML templates,
and Rich Menu setup via LINE Messaging API v3.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

import requests

# ── Config ────────────────────────────────────────────────────────────────────

CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LIFF_ID              = os.environ.get("LIFF_ID", "")

_API_BASE = "https://api.line.me/v2/bot"
_HEADERS  = lambda: {
    "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
    "Content-Type":  "application/json",
}


# ── Signature verification ────────────────────────────────────────────────────

def verify_signature(body: bytes, signature: str) -> bool:
    """Validate X-Line-Signature header."""
    if not CHANNEL_SECRET:
        return True  # dev mode: skip
    mac = hmac.new(CHANNEL_SECRET.encode(), body, hashlib.sha256).digest()
    from base64 import b64encode
    return hmac.compare_digest(b64encode(mac).decode(), signature)


# ── Low-level send helpers ────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> bool:
    try:
        r = requests.post(
            f"{_API_BASE}{path}",
            headers=_HEADERS(),
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[LINE] {path} error: {e}")
        return False


def reply(reply_token: str, messages: list[dict]) -> bool:
    return _post("/message/reply", {"replyToken": reply_token, "messages": messages})


def push(user_id: str, messages: list[dict]) -> bool:
    return _post("/message/push", {"to": user_id, "messages": messages})


def multicast(user_ids: list[str], messages: list[dict]) -> bool:
    """Send to up to 500 users at once."""
    if not user_ids:
        return True
    for i in range(0, len(user_ids), 500):
        batch = user_ids[i : i + 500]
        _post("/message/multicast", {"to": batch, "messages": messages})
    return True


def get_profile(user_id: str) -> dict:
    try:
        r = requests.get(
            f"{_API_BASE}/profile/{user_id}",
            headers=_HEADERS(),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


# ── Flex Message builders ─────────────────────────────────────────────────────

def _bar(pct: float, color: str = "#16a34a") -> dict:
    """Progress bar component (0–100)."""
    filled = max(0, min(100, int(pct)))
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            {"type": "filler"},
        ],
        "height": "6px",
        "borderWidth": "0px",
        "cornerRadius": "3px",
        "backgroundColor": "#e2e8f0",
        "paddingAll": "0px",
        "action": None,
        "_comment": f"bar-{filled}pct",
        # Inline bar via offsetStart trick not available; use text fallback
    }


def _fund_rows(fund: dict | None) -> list[dict]:
    """Build fundamental badge rows for a Flex Message body."""
    if not fund or fund.get("quality") == "na":
        return [{"type": "text", "text": "Fund  —",
                 "size": "xs", "color": "#94a3b8", "margin": "sm"}]

    de   = fund.get("de_ratio")
    re   = fund.get("re_ratio")
    s100 = fund.get("in_set100", False)

    de_icon  = "✅" if fund.get("de_ok") else "⚠️"
    re_icon  = "✅" if fund.get("re_ok") else "⚠️"
    de_text  = f"D/E {de}" if de is not None else "D/E —"
    re_text  = f"RE/EQ {re}x" if re is not None else "RE/EQ —"

    rows = [
        {
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": "Fund",
                 "size": "xs", "color": "#94a3b8", "flex": 2},
                {"type": "text",
                 "text": f"{de_icon} {de_text}  {re_icon} {re_text}",
                 "size": "xs", "color": "#0f172a", "flex": 7,
                 "weight": "bold", "wrap": True},
            ],
            "paddingTop": "4px",
        },
    ]
    if s100:
        rows.append({
            "type": "text", "text": "🏆 SET100",
            "size": "xs", "color": "#2563eb", "weight": "bold",
            "paddingTop": "2px",
        })
    return rows


def _score_text(score: int, total: int = 9) -> str:
    filled = round(score / total * 8)
    return "█" * filled + "░" * (8 - filled) + f"  {score}/{total}"


def signal_flex(sig: dict) -> dict:
    """Build a Flex Message bubble for a single signal."""
    color = "#16a34a" if sig["signal_type"] == "VCP" else "#2563eb"
    icon  = "⚡" if sig["signal_type"] == "VCP" else "🚀"
    chg_s = f"+{sig['chg']:.1f}%" if sig["chg"] >= 0 else f"{sig['chg']:.1f}%"
    chg_c = "#16a34a" if sig["chg"] >= 0 else "#dc2626"
    tv_url = f"https://www.tradingview.com/symbols/SET-{sig['symbol']}/"
    liff_url = f"https://liff.line.me/{LIFF_ID}/stock/{sig['symbol']}" if LIFF_ID else tv_url

    def row(label: str, value: str, val_color: str = "#0f172a") -> dict:
        return {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": label, "size": "sm",
                 "color": "#64748b", "flex": 4},
                {"type": "text", "text": value, "size": "sm",
                 "color": val_color, "weight": "bold", "flex": 5, "align": "end"},
            ],
            "paddingTop": "4px",
        }

    return {
        "type": "bubble",
        "size": "kilo",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": color,
            "paddingAll": "14px",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"{icon} {sig['signal_type']}",
                         "color": "#ffffff", "size": "sm", "weight": "bold", "flex": 0},
                        {"type": "filler"},
                        {"type": "text", "text": f"Stage 2 · {sig['score']}/9",
                         "color": "#ffffff", "size": "xs", "flex": 0},
                    ],
                },
                {"type": "text", "text": sig["symbol"],
                 "color": "#ffffff", "size": "xl", "weight": "bold", "margin": "sm"},
            ],
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "14px",
            "contents": [
                row("ราคาปัจจุบัน", f"฿{sig['price']:.2f}  {chg_s}", chg_c),
                {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
                row("จุดเข้า (Entry)",  f"฿{sig['entry']:.2f}"),
                row("Stop Loss",        f"฿{sig['stop']:.2f}  (-{sig['risk_pct']:.1f}%)", "#dc2626"),
                row("Target",           f"฿{sig['target']:.2f}", "#16a34a"),
                row("Risk/Reward",      f"{sig['rr']}x", color),
                row("ATR",              f"฿{sig['atr']:.2f}"),
                {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
                *_fund_rows(sig.get("fund")),
                {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
                {"type": "text",
                 "text": _score_text(sig["score"]),
                 "size": "xs", "color": "#64748b",
                 "margin": "sm", "wrap": True},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "horizontal",
            "spacing": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {"type": "uri", "label": "รายละเอียด",
                               "uri": liff_url},
                    "style": "primary", "color": color, "height": "sm",
                },
                {
                    "type": "button",
                    "action": {"type": "uri", "label": "TradingView",
                               "uri": tv_url},
                    "style": "secondary", "height": "sm",
                },
            ],
        },
    }


def signals_carousel(signals: list[dict]) -> dict:
    """Carousel of up to 10 signal bubbles."""
    bubbles = [signal_flex(s) for s in signals[:10]]
    return {
        "type": "flex",
        "altText": f"⚡ {len(signals)} Signal{'s' if len(signals)>1 else ''} วันนี้",
        "contents": {
            "type": "carousel",
            "contents": bubbles,
        },
    }


def breadth_bubble(b: dict) -> dict:
    """Flex Message bubble for market breadth summary."""
    cond  = b.get("market_condition", "—")
    emoji = {"Confirmed Uptrend": "🟢", "Uptrend Under Pressure": "🟡",
             "Rally Attempt": "🟠", "Downtrend": "🔴"}.get(cond, "⚪")
    color = {"Confirmed Uptrend": "#15803d", "Uptrend Under Pressure": "#d97706",
             "Rally Attempt": "#ea580c",     "Downtrend": "#dc2626"}.get(cond, "#64748b")

    def pct_row(label: str, pct: float) -> dict:
        bar_count = round(pct / 12.5)  # 0–8 blocks
        bar = "█" * bar_count + "░" * (8 - bar_count)
        return {
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": label, "size": "xs",
                 "color": "#64748b", "flex": 4},
                {"type": "text",
                 "text": f"{bar}  {pct:.1f}%",
                 "size": "xs", "color": "#0f172a", "flex": 6,
                 "weight": "bold", "align": "end"},
            ],
            "paddingTop": "4px",
        }

    liff_url = f"https://liff.line.me/{LIFF_ID}/market" if LIFF_ID else "https://signalix.app"

    return {
        "type": "flex",
        "altText": f"{emoji} {cond}",
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": color,
                "paddingAll": "14px",
                "contents": [
                    {"type": "text", "text": f"{emoji} {cond}",
                     "color": "#ffffff", "size": "md", "weight": "bold"},
                    {"type": "text",
                     "text": f"หุ้นทั้งหมด {b.get('total', 0)} ตัว",
                     "color": "#ffffff", "size": "xs", "margin": "xs"},
                ],
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "14px",
                "contents": [
                    {"type": "text", "text": "% เหนือเส้นค่าเฉลี่ย",
                     "size": "xs", "color": "#94a3b8", "weight": "bold"},
                    {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
                    pct_row("Above MA50",  b.get("above_ma50_pct",  0)),
                    pct_row("Above MA150", b.get("above_ma150_pct", 0)),
                    pct_row("Above MA200", b.get("above_ma200_pct", 0)),
                    {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
                    {
                        "type": "box", "layout": "horizontal",
                        "contents": [
                            {"type": "text",
                             "text": f"Stage 2: {b.get('stage2_count',0)} ตัว",
                             "size": "xs", "color": "#16a34a", "weight": "bold"},
                            {"type": "filler"},
                            {"type": "text",
                             "text": f"Stage 4: {b.get('stage4_count',0)} ตัว",
                             "size": "xs", "color": "#dc2626", "weight": "bold"},
                        ],
                        "paddingTop": "6px",
                    },
                ],
            },
            "footer": {
                "type": "box", "layout": "vertical",
                "contents": [{
                    "type": "button",
                    "action": {"type": "uri", "label": "ดูรายละเอียด",
                               "uri": liff_url},
                    "style": "primary", "color": color, "height": "sm",
                }],
            },
        },
    }


def stock_flex(stock: dict, signal: dict | None = None) -> dict:
    """Flex Message bubble for a single stock analysis."""
    stage_color = {"1": "#2563eb", "2": "#16a34a",
                   "3": "#d97706", "4": "#dc2626"}.get(str(stock.get("stage")), "#64748b")
    chg = stock.get("chg", 0)
    chg_s = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
    chg_c = "#16a34a" if chg >= 0 else "#dc2626"
    tv_url   = f"https://www.tradingview.com/symbols/SET-{stock['ticker']}/"
    liff_url = (f"https://liff.line.me/{LIFF_ID}/stock/{stock['ticker']}"
                if LIFF_ID else tv_url)

    def row(label, val, color="#475569"):
        return {"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": label, "size": "xs", "color": "#94a3b8", "flex": 4},
            {"type": "text", "text": str(val), "size": "xs",
             "color": color, "weight": "bold", "flex": 5, "align": "end"},
        ], "paddingTop": "3px"}

    body_rows = [
        row("MA50",  f"฿{stock['ma50']:.2f}"),
        row("MA200", f"฿{stock['ma200']:.2f}" if stock.get("ma200") else "—"),
        row("52W High", f"฿{stock['high52']:.2f}"),
        row("52W Low",  f"฿{stock['low52']:.2f}"),
        {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
        {"type": "text", "text": _score_text(stock["score"]),
         "size": "xs", "color": "#64748b", "margin": "sm", "wrap": True},
    ]

    if signal:
        body_rows += [
            {"type": "separator", "margin": "sm", "color": "#f1f5f9"},
            {"type": "text", "text": f"⚡ {signal['signal_type']} Signal",
             "size": "xs", "color": stage_color, "weight": "bold", "margin": "sm"},
            row("Entry",  f"฿{signal['entry']:.2f}"),
            row("Stop",   f"฿{signal['stop']:.2f}", "#dc2626"),
            row("Target", f"฿{signal['target']:.2f}", "#16a34a"),
            row("R:R",    f"{signal['rr']}x", stage_color),
        ]

    return {
        "type": "flex",
        "altText": f"{stock['ticker']} — Stage {stock['stage']} · {stock['score']}/9",
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box", "layout": "vertical",
                "backgroundColor": stage_color, "paddingAll": "14px",
                "contents": [
                    {
                        "type": "box", "layout": "horizontal",
                        "contents": [
                            {"type": "text", "text": stock["ticker"],
                             "color": "#ffffff", "size": "xl", "weight": "bold"},
                            {"type": "filler"},
                            {"type": "text", "text": chg_s,
                             "color": "#ffffff", "size": "sm",
                             "weight": "bold", "align": "end"},
                        ],
                    },
                    {"type": "text",
                     "text": f"฿{stock['price']:.2f}   Stage {stock['stage']}",
                     "color": "#ffffff", "size": "sm", "margin": "xs"},
                ],
            },
            "body": {"type": "box", "layout": "vertical",
                     "paddingAll": "14px", "contents": body_rows},
            "footer": {
                "type": "box", "layout": "horizontal", "spacing": "sm",
                "contents": [
                    {"type": "button",
                     "action": {"type": "uri", "label": "รายละเอียด", "uri": liff_url},
                     "style": "primary", "color": stage_color, "height": "sm"},
                    {"type": "button",
                     "action": {"type": "uri", "label": "Chart", "uri": tv_url},
                     "style": "secondary", "height": "sm"},
                ],
            },
        },
    }


def text_message(text: str) -> dict:
    return {"type": "text", "text": text}


def help_message() -> dict:
    liff_url = f"https://liff.line.me/{LIFF_ID}" if LIFF_ID else "#"
    return text_message(
        "⚡ Signalix — คำสั่งที่ใช้ได้\n\n"
        "📊 signals — สัญญาณหุ้นวันนี้\n"
        "📈 market — ภาพรวมตลาด\n"
        "🔍 ADVANC — วิเคราะห์หุ้นรายตัว\n"
        "👁 watchlist — ดู watchlist ของฉัน\n"
        "➕ watchlist add ADVANC — เพิ่มหุ้น\n"
        "➖ watchlist remove ADVANC — ลบหุ้น\n"
        "📋 history — signal ล่าสุด 5 รายการ\n\n"
        f"🌐 เปิดแอปใน LINE: {liff_url}"
    )


# ── Webhook event dispatcher ──────────────────────────────────────────────────

def handle_webhook(body_bytes: bytes, signature: str, db, get_stocks_fn, get_signals_fn, get_breadth_fn) -> None:
    """
    Parse LINE webhook events and dispatch to handlers.
    db             : Firestore client
    get_stocks_fn  : () -> (analyzed, raw_by_symbol)
    get_signals_fn : () -> list[dict]
    get_breadth_fn : () -> dict
    """
    if not verify_signature(body_bytes, signature):
        print("[LINE] Invalid signature")
        return

    try:
        payload = json.loads(body_bytes)
    except json.JSONDecodeError:
        return

    for event in payload.get("events", []):
        etype   = event.get("type")
        user_id = event.get("source", {}).get("userId", "")
        rtoken  = event.get("replyToken", "")

        if etype == "follow":
            _on_follow(user_id, rtoken, db)
        elif etype == "unfollow":
            _on_unfollow(user_id, db)
        elif etype == "message":
            _on_message(event, user_id, rtoken, db,
                        get_stocks_fn, get_signals_fn, get_breadth_fn)
        elif etype == "postback":
            _on_postback(event, user_id, rtoken, db,
                         get_stocks_fn, get_signals_fn, get_breadth_fn)


def _on_follow(user_id: str, reply_token: str, db) -> None:
    profile = get_profile(user_id)
    # Upsert user document
    db.collection("users").document(user_id).set({
        "lineUserId":    user_id,
        "displayName":   profile.get("displayName", ""),
        "pictureUrl":    profile.get("pictureUrl", ""),
        "tier":          "free",
        "watchlist":     [],
        "active":        True,
        "joinedAt":      _now(),
        "lastActive":    _now(),
    }, merge=True)

    liff_url = f"https://liff.line.me/{LIFF_ID}" if LIFF_ID else "https://signalix.app"
    reply(reply_token, [text_message(
        f"ยินดีต้อนรับสู่ ⚡ Signalix!\n\n"
        "สัญญาณหุ้นไทยสไตล์ Mark Minervini\n"
        "รับ alert อัตโนมัติทุกวันจันทร์–ศุกร์ หลัง 18:00 น.\n\n"
        f"🌐 เปิดแอป: {liff_url}\n\n"
        "พิมพ์ help เพื่อดูคำสั่งทั้งหมด"
    )])


def _on_unfollow(user_id: str, db) -> None:
    db.collection("users").document(user_id).set(
        {"active": False, "lastActive": _now()}, merge=True
    )


def _on_message(event, user_id, reply_token, db,
                get_stocks_fn, get_signals_fn, get_breadth_fn) -> None:
    msg = event.get("message", {})
    if msg.get("type") != "text":
        return

    _touch_user(user_id, db)
    text = msg.get("text", "").strip()
    cmd  = text.lower()

    if cmd in ("help", "ช่วยเหลือ"):
        reply(reply_token, [help_message()])

    elif cmd in ("signals", "สัญญาณ", "signal"):
        sigs = get_signals_fn()
        if not sigs:
            reply(reply_token, [text_message("ยังไม่มีสัญญาณวันนี้ ลองใหม่พรุ่งนี้ครับ")])
        else:
            reply(reply_token, [signals_carousel(sigs)])

    elif cmd in ("market", "ตลาด", "breadth"):
        b = get_breadth_fn()
        if not b or not b.get("total"):
            reply(reply_token, [text_message("ยังไม่มีข้อมูลตลาดวันนี้ครับ")])
        else:
            reply(reply_token, [breadth_bubble(b)])

    elif cmd in ("watchlist", "wl"):
        _handle_watchlist_show(user_id, reply_token, db, get_stocks_fn)

    elif cmd.startswith("watchlist add ") or cmd.startswith("wl add "):
        sym = text.split()[-1].upper()
        _handle_watchlist_add(user_id, reply_token, db, sym)

    elif cmd.startswith("watchlist remove ") or cmd.startswith("wl rm "):
        sym = text.split()[-1].upper()
        _handle_watchlist_remove(user_id, reply_token, db, sym)

    elif cmd in ("history", "ประวัติ"):
        _handle_history(user_id, reply_token, db)

    else:
        # Try to look up as stock symbol
        sym = text.upper().strip()
        if sym.isalpha() and 2 <= len(sym) <= 6:
            _handle_stock_lookup(sym, reply_token, db, get_stocks_fn, get_signals_fn)
        else:
            reply(reply_token, [text_message("ไม่เข้าใจคำสั่งครับ พิมพ์ help เพื่อดูคำสั่ง")])


def _on_postback(event, user_id, reply_token, db,
                 get_stocks_fn, get_signals_fn, get_breadth_fn) -> None:
    data = event.get("postback", {}).get("data", "")
    if data == "signals":
        sigs = get_signals_fn()
        reply(reply_token, [signals_carousel(sigs)] if sigs
              else [text_message("ยังไม่มีสัญญาณวันนี้")])
    elif data == "market":
        b = get_breadth_fn()
        reply(reply_token, [breadth_bubble(b)] if b and b.get("total")
              else [text_message("ยังไม่มีข้อมูลตลาด")])
    elif data == "watchlist":
        _handle_watchlist_show(user_id, reply_token, db, get_stocks_fn)
    elif data == "help":
        reply(reply_token, [help_message()])
    elif data.startswith("stock:"):
        sym = data.split(":", 1)[1].upper()
        _handle_stock_lookup(sym, reply_token, db, get_stocks_fn, get_signals_fn)


# ── Command handlers ──────────────────────────────────────────────────────────

def _handle_stock_lookup(sym, reply_token, db, get_stocks_fn, get_signals_fn):
    analyzed, _ = get_stocks_fn()
    stock = next((s for s in analyzed if s["ticker"] == sym), None)
    if not stock:
        reply(reply_token, [text_message(f"ไม่พบหุ้น {sym} ในฐานข้อมูลครับ")])
        return
    sigs = get_signals_fn()
    sig  = next((s for s in sigs if s["symbol"] == sym), None)
    reply(reply_token, [stock_flex(stock, sig)])


def _handle_watchlist_show(user_id, reply_token, db, get_stocks_fn):
    doc   = db.collection("users").document(user_id).get()
    if not doc.exists:
        reply(reply_token, [text_message("ยังไม่มี watchlist ครับ ลอง 'watchlist add ADVANC'")])
        return
    wl = doc.to_dict().get("watchlist", [])
    if not wl:
        reply(reply_token, [text_message("Watchlist ว่างอยู่ครับ\nเพิ่มด้วย: watchlist add ADVANC")])
        return
    analyzed, _ = get_stocks_fn()
    stock_map = {s["ticker"]: s for s in analyzed}
    lines = ["👁 Watchlist ของคุณ\n"]
    for sym in wl:
        s = stock_map.get(sym)
        if s:
            chg = f"+{s['chg']:.1f}%" if s["chg"] >= 0 else f"{s['chg']:.1f}%"
            stage_label = {1:"Basing",2:"Advancing",3:"Topping",4:"Declining"}.get(s["stage"],"")
            lines.append(f"• {sym}  ฿{s['price']:.2f}  {chg}  Stage {s['stage']} {stage_label}")
        else:
            lines.append(f"• {sym}  (ไม่มีข้อมูล)")
    reply(reply_token, [text_message("\n".join(lines))])


def _handle_watchlist_add(user_id, reply_token, db, sym):
    ref = db.collection("users").document(user_id)
    doc = ref.get()
    if not doc.exists:
        ref.set({"lineUserId": user_id, "watchlist": [sym], "tier": "free",
                 "active": True, "joinedAt": _now(), "lastActive": _now()})
    else:
        wl = doc.to_dict().get("watchlist", [])
        if sym not in wl:
            if len(wl) >= 20:
                reply(reply_token, [text_message("Watchlist เต็มแล้ว (สูงสุด 20 ตัวสำหรับ Free tier)")])
                return
            wl.append(sym)
            ref.update({"watchlist": wl, "lastActive": _now()})
    reply(reply_token, [text_message(f"✅ เพิ่ม {sym} ใน Watchlist แล้วครับ")])


def _handle_watchlist_remove(user_id, reply_token, db, sym):
    ref = db.collection("users").document(user_id)
    doc = ref.get()
    if doc.exists:
        wl = doc.to_dict().get("watchlist", [])
        if sym in wl:
            wl.remove(sym)
            ref.update({"watchlist": wl, "lastActive": _now()})
    reply(reply_token, [text_message(f"🗑 ลบ {sym} ออกจาก Watchlist แล้วครับ")])


def _handle_history(user_id, reply_token, db):
    """Show last 5 signal records from Firestore."""
    from datetime import date, timedelta
    messages = []
    for days_back in range(7):
        d = (date.today() - timedelta(days=days_back)).isoformat()
        doc = db.collection("signals").document(d).get()
        if doc.exists:
            sigs = doc.to_dict().get("signals", [])
            if sigs:
                messages.append(f"📅 {d}: {len(sigs)} signal(s)")
        if len(messages) >= 5:
            break
    if messages:
        reply(reply_token, [text_message("📋 Signal History\n\n" + "\n".join(messages))])
    else:
        reply(reply_token, [text_message("ยังไม่มีประวัติ signal ครับ")])


# ── Rich Menu ─────────────────────────────────────────────────────────────────

def create_rich_menu() -> str | None:
    """
    Create a 6-button Rich Menu programmatically.
    Returns richMenuId if successful.
    Call this once during initial setup.
    """
    liff_base = f"https://liff.line.me/{LIFF_ID}" if LIFF_ID else "https://signalix.app"

    payload = {
        "size": {"width": 2500, "height": 843},
        "selected": True,
        "name": "Signalix Main Menu",
        "chatBarText": "⚡ Signalix Menu",
        "areas": [
            # Row 1
            {"bounds": {"x": 0,    "y": 0, "width": 833, "height": 421},
             "action": {"type": "postback", "data": "signals", "displayText": "signals"}},
            {"bounds": {"x": 833,  "y": 0, "width": 834, "height": 421},
             "action": {"type": "postback", "data": "market",  "displayText": "market"}},
            {"bounds": {"x": 1667, "y": 0, "width": 833, "height": 421},
             "action": {"type": "uri",      "uri": f"{liff_base}/scanner"}},
            # Row 2
            {"bounds": {"x": 0,    "y": 421, "width": 833, "height": 422},
             "action": {"type": "uri",      "uri": f"{liff_base}/"}},
            {"bounds": {"x": 833,  "y": 421, "width": 834, "height": 422},
             "action": {"type": "postback", "data": "watchlist", "displayText": "watchlist"}},
            {"bounds": {"x": 1667, "y": 421, "width": 833, "height": 422},
             "action": {"type": "postback", "data": "help", "displayText": "help"}},
        ],
    }
    try:
        r = requests.post(
            "https://api.line.me/v2/bot/richmenu",
            headers=_HEADERS(), json=payload, timeout=10
        )
        r.raise_for_status()
        return r.json().get("richMenuId")
    except Exception as e:
        print(f"[LINE] create_rich_menu error: {e}")
        return None


def set_default_rich_menu(rich_menu_id: str) -> bool:
    try:
        r = requests.post(
            f"https://api.line.me/v2/bot/user/all/richmenu/{rich_menu_id}",
            headers=_HEADERS(), timeout=10
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[LINE] set_default_rich_menu error: {e}")
        return False


# ── Morning Briefing ──────────────────────────────────────────────────────────

def morning_briefing_message(breadth: dict, signals: list[dict]) -> dict:
    """Quick morning push with yesterday's market snapshot."""
    cond  = breadth.get("market_condition", "—")
    emoji = {"Confirmed Uptrend": "🟢", "Uptrend Under Pressure": "🟡",
             "Rally Attempt": "🟠", "Downtrend": "🔴"}.get(cond, "⚪")
    sig_line = f"⚡ Signals: {len(signals)} รายการ" if signals else "ยังไม่มี signal"
    liff_url = f"https://liff.line.me/{LIFF_ID}" if LIFF_ID else "https://signalix.app"
    return text_message(
        f"☀️ Good morning — Signalix\n\n"
        f"{emoji} {cond}\n"
        f"Above MA200:  {breadth.get('above_ma200_pct', 0):.1f}%\n"
        f"Above MA50:   {breadth.get('above_ma50_pct', 0):.1f}%\n"
        f"Stage 2:      {breadth.get('stage2_count', 0)} หุ้น\n\n"
        f"{sig_line}\n\n"
        f"🌐 {liff_url}"
    )


# ── Utilities ─────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _touch_user(user_id: str, db) -> None:
    try:
        db.collection("users").document(user_id).set(
            {"lastActive": _now(), "active": True}, merge=True
        )
    except Exception:
        pass


def get_active_user_ids(db) -> list[str]:
    """Fetch all active subscriber LINE user IDs from Firestore."""
    try:
        docs = db.collection("users").where("active", "==", True).stream()
        return [d.id for d in docs]
    except Exception:
        return []
