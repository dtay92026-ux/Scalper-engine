"""
ScalpRules Engine
=================
Control panel + web API for ScalpRulesEA.mq5

  * PLAY / PAUSE the EA from the browser
  * Configuration page: lot size, SL, TP, Telegram
  * Activity log: EA connected / running, trades executed (time + prices)
  * Telegram notification whenever the EA executes a trade

Run:
    pip install -r requirements.txt
    python app.py
Then open  http://127.0.0.1:5000

Environment variables (all optional):
    ENGINE_HOST          default 127.0.0.1  (use 0.0.0.0 to open it from your phone)
    ENGINE_PORT          default 5000
    ENGINE_UI_PASSWORD   if set, the web panel asks for this password
    ENGINE_TOKEN         shared secret between the EA and the engine
                         (auto-generated and shown in Configuration if not set)
"""

import html
import json
import os
import secrets
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request

# ----------------------------------------------------------------------------
# Paths / settings
# ----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "engine_data"
DATA_DIR.mkdir(exist_ok=True)
CONFIG_FILE = DATA_DIR / "config.json"
EVENTS_FILE = DATA_DIR / "events.jsonl"
TRADES_FILE = DATA_DIR / "trades.jsonl"

HOST = os.environ.get("ENGINE_HOST", "127.0.0.1")
PORT = int(os.environ.get("ENGINE_PORT", "5000"))
UI_PASSWORD = os.environ.get("ENGINE_UI_PASSWORD", "")

app = Flask(__name__, static_folder=str(BASE_DIR / "static"), static_url_path="/static")
_lock = threading.RLock()

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
DEFAULTS = {
    "run": False,
    "lot": 0.01,
    "sl_pts": 5000,
    "tp_pts": 6000,
    "ea_token": "",
    "tg_enabled": True,
    "tg_token": "",
    "tg_chat": "",
}


def _load_config():
    cfg = dict(DEFAULTS)
    if CONFIG_FILE.exists():
        try:
            cfg.update(json.loads(CONFIG_FILE.read_text("utf-8")))
        except Exception:
            pass
    if os.environ.get("ENGINE_TOKEN"):
        cfg["ea_token"] = os.environ["ENGINE_TOKEN"]
    if not cfg.get("ea_token"):
        cfg["ea_token"] = secrets.token_urlsafe(16)
    cfg["run"] = False  # always start paused for safety
    return cfg


config = _load_config()


def save_config():
    with _lock:
        CONFIG_FILE.write_text(json.dumps(config, indent=2), "utf-8")


save_config()

# ----------------------------------------------------------------------------
# Events (activity log) and trades
# ----------------------------------------------------------------------------
events = deque(maxlen=1000)
trades = deque(maxlen=500)


def _load_jsonl(path, dq):
    if not path.exists():
        return
    try:
        for line in path.read_text("utf-8").splitlines():
            line = line.strip()
            if line:
                dq.append(json.loads(line))
    except Exception:
        pass


def _rewrite_jsonl(path, dq):
    try:
        path.write_text("".join(json.dumps(x) + "\n" for x in dq), "utf-8")
    except Exception:
        pass


def _append_jsonl(path, obj):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj) + "\n")
    except Exception:
        pass


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(kind, msg, level="info", data=None):
    """kind: system | control | ea | trade | telegram   level: ok | info | warn | error"""
    entry = {"ts": now_str(), "kind": kind, "level": level, "msg": msg}
    if data:
        entry["data"] = data
    with _lock:
        events.append(entry)
        _append_jsonl(EVENTS_FILE, entry)


_load_jsonl(EVENTS_FILE, events)
_load_jsonl(TRADES_FILE, trades)
_rewrite_jsonl(EVENTS_FILE, events)
_rewrite_jsonl(TRADES_FILE, trades)

# ----------------------------------------------------------------------------
# EA connection tracking
# ----------------------------------------------------------------------------
ea = {"last_seen": 0.0, "connected": False, "running": None, "info": {}}


def ea_timeout():
    try:
        poll = float(ea["info"].get("poll", 2))
    except Exception:
        poll = 2.0
    return max(10.0, 3.0 * poll)


def update_connection(reason=None):
    with _lock:
        alive = ea["last_seen"] > 0 and (time.time() - ea["last_seen"]) <= ea_timeout()
        if alive and not ea["connected"]:
            ea["connected"] = True
            i = ea["info"]
            log("ea",
                f"EA connected · {i.get('symbol', '?')} · account {i.get('account', '?')} · {i.get('server', '')}",
                "ok")
        elif not alive and ea["connected"]:
            ea["connected"] = False
            ea["running"] = None
            log("ea", reason or "EA disconnected · no heartbeat received", "error")

        if ea["connected"]:
            running = bool(ea["info"].get("running"))
            if ea["running"] is None or running != ea["running"]:
                ea["running"] = running
                if running:
                    log("ea", "EA is RUNNING · accepting new trade signals", "ok")
                else:
                    log("ea", "EA is PAUSED · not opening new trades", "warn")


def _monitor():
    while True:
        try:
            update_connection()
        except Exception:
            pass
        time.sleep(1)


_monitor_started = False


def start_background():
    global _monitor_started
    if _monitor_started:
        return
    _monitor_started = True
    threading.Thread(target=_monitor, daemon=True).start()


# ----------------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------------
def tg_send(text, token=None, chat=None):
    token = token or config["tg_token"]
    chat = chat or config["tg_chat"]
    if not token or not chat:
        return False, "Telegram bot token / chat ID not set"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10,
        )
        try:
            j = r.json()
        except Exception:
            j = {}
        if r.ok and j.get("ok"):
            return True, "sent"
        return False, j.get("description") or f"HTTP {r.status_code}"
    except Exception as e:  # never leak the bot token in logs
        return False, str(e).replace(token, "***")


def fmt_trade_message(t):
    e = html.escape
    side = str(t.get("side", "")).upper()
    icon = "🟢" if side == "BUY" else "🔴"
    lines = [
        f"{icon} <b>Trade executed · {e(side)} {e(str(t.get('symbol', '')))}</b>",
        f"🕒 Time: <code>{e(str(t.get('time', '')))}</code>",
        f"💰 Entry: <code>{e(str(t.get('price', '')))}</code>",
        f"🛑 SL: <code>{e(str(t.get('sl', '')))}</code>",
        f"🎯 TP: <code>{e(str(t.get('tp', '')))}</code>",
        f"📦 Lots: <code>{e(str(t.get('lots', '')))}</code>",
    ]
    if t.get("pattern"):
        lines.append(f"📊 Setup: {e(str(t['pattern']))}")
    if t.get("ticket") and str(t["ticket"]) != "0":
        lines.append(f"🎫 Ticket: <code>{e(str(t['ticket']))}</code>")
    return "\n".join(lines)


def notify_trade(t):
    if not config["tg_enabled"]:
        return

    def work():
        ok, info = tg_send(fmt_trade_message(t))
        if ok:
            log("telegram", "Telegram notification sent", "ok")
        elif "not set" in info:
            log("telegram", "Telegram not configured · notification skipped", "warn")
        else:
            log("telegram", f"Telegram failed · {info}", "error")

    threading.Thread(target=work, daemon=True).start()


# ----------------------------------------------------------------------------
# Auth
# ----------------------------------------------------------------------------
def ea_auth():
    tok = request.headers.get("X-Engine-Token", "")
    return secrets.compare_digest(tok.encode("utf-8"), config["ea_token"].encode("utf-8"))


@app.before_request
def ui_guard():
    if request.path.startswith("/api/ea/") or not UI_PASSWORD:
        return None
    a = request.authorization
    if a and secrets.compare_digest((a.password or "").encode("utf-8"), UI_PASSWORD.encode("utf-8")):
        return None
    return Response("Authentication required", 401,
                    {"WWW-Authenticate": 'Basic realm="ScalpRules Engine"'})


# ----------------------------------------------------------------------------
# EA-facing API  (called by the EA through WebRequest)
# ----------------------------------------------------------------------------
@app.post("/api/ea/poll")
def ea_poll():
    if not ea_auth():
        return jsonify(error="unauthorized"), 401
    body = request.get_json(force=True, silent=True) or {}
    with _lock:
        ea["last_seen"] = time.time()
        ea["info"] = body
    update_connection()
    resp = {
        "run": 1 if config["run"] else 0,
        "lot": config["lot"],
        "sl_pts": int(config["sl_pts"]),
        "tp_pts": int(config["tp_pts"]),
    }
    return Response(json.dumps(resp, separators=(",", ":")), mimetype="application/json")


@app.post("/api/ea/event")
def ea_event():
    if not ea_auth():
        return jsonify(error="unauthorized"), 401
    b = request.get_json(force=True, silent=True) or {}
    typ = b.get("type")

    if typ == "trade":
        ticket = str(b.get("ticket", ""))
        with _lock:
            if ticket not in ("", "0") and any(str(t.get("ticket")) == ticket for t in trades):
                return jsonify(ok=1, duplicate=1)
        t = {
            "ts": now_str(),
            "time": b.get("time"),
            "symbol": b.get("symbol"),
            "side": str(b.get("side", "")).upper(),
            "lots": b.get("lots"),
            "price": b.get("price"),
            "sl": b.get("sl"),
            "tp": b.get("tp"),
            "ticket": ticket,
            "pattern": b.get("pattern", ""),
            "spread": b.get("spread"),
        }
        with _lock:
            trades.append(t)
            _append_jsonl(TRADES_FILE, t)
        log("trade",
            f"TRADE EXECUTED · {t['side']} {t['lots']} {t['symbol']} @ {t['price']} · "
            f"SL {t['sl']} · TP {t['tp']} · {t['time']}",
            "ok", t)
        notify_trade(t)

    elif typ == "order_failed":
        log("trade",
            f"Order FAILED · {b.get('side', '')} {b.get('symbol', '')} · "
            f"{b.get('msg', '')} (retcode {b.get('retcode', '?')})",
            "error")

    elif typ == "info":
        lvl = b.get("level") if b.get("level") in ("ok", "info", "warn", "error") else "info"
        log("ea", str(b.get("msg", ""))[:300], lvl)

    elif typ == "bye":
        with _lock:
            ea["last_seen"] = 0.0
        update_connection("EA stopped · removed from chart or MT5 closed")

    return jsonify(ok=1)


# ----------------------------------------------------------------------------
# Panel API
# ----------------------------------------------------------------------------
def public_config():
    tok = config["tg_token"]
    hint = (tok[:4] + "…" + tok[-4:]) if len(tok) > 10 else ("saved" if tok else "")
    return {
        "run": config["run"],
        "lot": config["lot"],
        "sl_pts": config["sl_pts"],
        "tp_pts": config["tp_pts"],
        "tg_enabled": config["tg_enabled"],
        "tg_chat": config["tg_chat"],
        "tg_token_set": bool(tok),
        "tg_token_hint": hint,
        "ea_token": config["ea_token"],
        "ea_url": f"http://127.0.0.1:{PORT}",
    }


@app.get("/api/state")
def api_state():
    with _lock:
        return jsonify({
            "server_time": now_str(),
            "engine_run": config["run"],
            "ea_connected": ea["connected"],
            "ea_running": ea["running"] if ea["connected"] else None,
            "ea": ea["info"],
            "config": public_config(),
            "events": list(events)[-300:],
            "trades": list(trades)[-100:],
        })


@app.post("/api/control")
def api_control():
    b = request.get_json(silent=True) or {}
    act = b.get("action")
    if act not in ("play", "pause"):
        return jsonify(error="action must be play or pause"), 400
    with _lock:
        config["run"] = (act == "play")
        save_config()
    if act == "play":
        log("control", "Engine PLAY · the EA may open new trades", "ok")
    else:
        log("control", "Engine PAUSED · no new trades (open trades keep their SL/TP)", "warn")
    return jsonify(ok=True, run=config["run"])


@app.post("/api/config")
def api_config():
    b = request.get_json(silent=True) or {}
    new = {}
    try:
        if "lot" in b:
            lot = float(b["lot"])
            if not 0.01 <= lot <= 100:
                raise ValueError("Lot size must be between 0.01 and 100")
            new["lot"] = round(lot, 2)
        for key, label in (("sl_pts", "Stop loss"), ("tp_pts", "Take profit")):
            if key in b:
                v = int(float(b[key]))
                if not 1 <= v <= 1_000_000:
                    raise ValueError(f"{label} must be between 1 and 1,000,000 points")
                new[key] = v
        if "tg_enabled" in b:
            new["tg_enabled"] = bool(b["tg_enabled"])
        if "tg_chat" in b:
            new["tg_chat"] = str(b["tg_chat"]).strip()
        if str(b.get("tg_token", "")).strip():
            new["tg_token"] = str(b["tg_token"]).strip()
    except (TypeError, ValueError) as e:
        msg = str(e)
        if msg.startswith("could not convert") or msg.startswith("invalid literal"):
            msg = "Please enter valid numbers"
        return jsonify(error=msg), 400

    with _lock:
        changed = {k: v for k, v in new.items() if config.get(k) != v}
        config.update(new)
        save_config()
    trading = [k for k in changed if k in ("lot", "sl_pts", "tp_pts")]
    if trading:
        log("control",
            f"Configuration saved · lot {config['lot']} · SL {config['sl_pts']} pts · TP {config['tp_pts']} pts",
            "info")
    if any(k.startswith("tg_") for k in changed):
        log("control", "Telegram settings updated", "info")
    return jsonify(ok=True, config=public_config())


@app.post("/api/telegram/test")
def api_tg_test():
    ok, info = tg_send("✅ <b>ScalpRules Engine</b>\nTelegram is connected. You will be notified when a trade is executed.")
    if ok:
        log("telegram", "Telegram test message sent", "ok")
        return jsonify(ok=True)
    log("telegram", f"Telegram test failed · {info}", "error")
    return jsonify(error=info), 400


@app.post("/api/log/clear")
def api_log_clear():
    with _lock:
        events.clear()
        _rewrite_jsonl(EVENTS_FILE, events)
    log("system", "Log cleared", "info")
    return jsonify(ok=True)


# ----------------------------------------------------------------------------
# Web panel
# ----------------------------------------------------------------------------
@app.get("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#04060c">
<title>ScalpRules Engine</title>
<link rel="icon" href="/static/robot.jpg">
<style>
:root{
  --bg:#04060c; --card:#0a111f; --card2:#0d1626; --line:#15233c; --line2:#1d3254;
  --text:#e8f0ff; --muted:#7d92b6; --blue:#2f80ff; --blue2:#1747c9; --cyan:#22d3ee;
  --green:#19f26b; --green2:#12c455; --red:#ff4d6a; --amber:#ffb020;
}
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-text-size-adjust:100%}
body{
  background:
    radial-gradient(1100px 560px at 85% -10%,rgba(47,128,255,.16),transparent 60%),
    radial-gradient(900px 520px at -10% 105%,rgba(25,242,107,.09),transparent 60%),
    var(--bg);
  color:var(--text);min-height:100vh;
  font-family:Inter,"Segoe UI",system-ui,-apple-system,Roboto,Helvetica,Arial,sans-serif;
}
.app{display:grid;grid-template-columns:236px minmax(0,1fr);min-height:100vh}
/* sidebar */
.side{position:sticky;top:0;height:100vh;padding:22px 16px;border-right:1px solid var(--line);
  background:linear-gradient(180deg,rgba(10,17,31,.9),rgba(4,6,12,.95));display:flex;flex-direction:column;gap:22px}
.brand{display:flex;align-items:center;gap:12px;padding:0 6px}
.brand img{width:42px;height:42px;border-radius:12px;object-fit:cover;object-position:50% 25%;
  border:1px solid var(--line2);box-shadow:0 0 18px rgba(25,242,107,.35)}
.brand b{display:block;font-size:15px;letter-spacing:.02em}
.brand span{font-size:11px;color:var(--muted);letter-spacing:.14em}
.nav{display:flex;flex-direction:column;gap:6px}
.nav a{display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:12px;color:var(--muted);
  text-decoration:none;font-weight:600;font-size:14px;border:1px solid transparent;cursor:pointer;transition:.15s}
.nav a svg{width:18px;height:18px;flex:none}
.nav a:hover{color:var(--text);background:rgba(47,128,255,.07)}
.nav a.active{color:#fff;background:linear-gradient(90deg,rgba(47,128,255,.22),rgba(47,128,255,.04));
  border-color:rgba(47,128,255,.35);box-shadow:inset 3px 0 0 var(--blue)}
.side .foot{margin-top:auto;font-size:11px;color:var(--muted);line-height:1.6;padding:0 6px}
/* main */
main{padding:26px 30px 40px;max-width:1280px;width:100%}
.view{display:none}.view.active{display:block;animation:fade .25s ease}
@keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
h2.title{font-size:20px;margin-bottom:4px}
p.sub{color:var(--muted);font-size:13px;margin-bottom:18px}
/* hero */
.hero{position:relative;overflow:hidden;border:1px solid var(--line2);border-radius:22px;min-height:310px;
  background:linear-gradient(120deg,#08101f 0%,#050913 60%);margin-bottom:18px;
  box-shadow:0 0 0 1px rgba(47,128,255,.06),0 20px 60px rgba(0,0,0,.5)}
.hero-img{position:absolute;top:0;right:0;bottom:0;width:60%}
.hero-img img{width:100%;height:100%;object-fit:cover;object-position:50% 30%;
  -webkit-mask-image:linear-gradient(to left,#000 38%,transparent 100%);mask-image:linear-gradient(to left,#000 38%,transparent 100%)}
.hero::after{content:"";position:absolute;inset:0;pointer-events:none;
  background:radial-gradient(500px 260px at 100% 0%,rgba(47,128,255,.18),transparent 70%)}
.hero-text{position:relative;z-index:2;padding:30px 32px;max-width:600px}
.eyebrow{font-size:11px;letter-spacing:.34em;color:var(--green);font-weight:700;margin-bottom:12px}
.hero h1{font-size:34px;line-height:1.1;letter-spacing:-.01em;margin-bottom:8px}
.hero h1 em{font-style:normal;background:linear-gradient(90deg,var(--blue),var(--cyan));-webkit-background-clip:text;background-clip:text;color:transparent}
.hero p{color:var(--muted);font-size:14px;margin-bottom:16px}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}
.chip{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;font-size:11.5px;
  font-weight:700;letter-spacing:.06em;background:rgba(8,14,26,.75);border:1px solid var(--line2);backdrop-filter:blur(6px)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex:none}
.dot.ok{background:var(--green);box-shadow:0 0 10px var(--green);animation:pulse 2s infinite}
.dot.bad{background:var(--red);box-shadow:0 0 10px var(--red)}
.dot.warn{background:var(--amber);box-shadow:0 0 10px var(--amber)}
.dot.blue{background:var(--blue);box-shadow:0 0 10px var(--blue);animation:pulse 2s infinite}
@keyframes pulse{50%{opacity:.45}}
.controls{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.play{display:inline-flex;align-items:center;gap:12px;padding:16px 30px;border-radius:14px;border:0;cursor:pointer;
  font-weight:800;letter-spacing:.14em;font-size:14px;color:#03150a;transition:.18s;
  background:linear-gradient(135deg,var(--green),var(--green2));box-shadow:0 0 34px rgba(25,242,107,.38)}
.play svg{width:20px;height:20px}
.play.running{color:#fff;background:linear-gradient(135deg,var(--blue),var(--blue2));box-shadow:0 0 34px rgba(47,128,255,.45)}
.play:hover{transform:translateY(-1px);filter:brightness(1.08)}
.play:active{transform:scale(.98)}
.hint{font-size:12.5px;color:var(--muted);max-width:280px;line-height:1.5}
/* cards / stats */
.grid{display:grid;gap:14px}
.g4{grid-template-columns:repeat(4,minmax(0,1fr))}
.g3{grid-template-columns:repeat(3,minmax(0,1fr))}
.g2{grid-template-columns:repeat(2,minmax(0,1fr))}
.card{background:linear-gradient(180deg,var(--card),#080e19);border:1px solid var(--line);border-radius:18px;padding:18px 20px}
.card h3{font-size:12px;letter-spacing:.16em;color:var(--muted);font-weight:700;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center}
.stat .v{font-size:24px;font-weight:800;letter-spacing:-.01em}
.stat .s{font-size:12px;color:var(--muted);margin-top:4px}
.v.green{color:var(--green)}.v.red{color:var(--red)}.v.amber{color:var(--amber)}.v.blue{color:var(--blue)}
.section{margin-top:18px}
/* table */
.tw{overflow-x:auto;margin:0 -6px}
table{width:100%;border-collapse:collapse;font-size:13px;min-width:640px}
th{color:var(--muted);font-size:10.5px;letter-spacing:.14em;text-align:left;padding:8px 10px;border-bottom:1px solid var(--line);font-weight:700}
td{padding:11px 10px;border-bottom:1px solid #0e1a2f;white-space:nowrap;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:0}
.badge{display:inline-block;padding:3px 10px;border-radius:7px;font-size:11px;font-weight:800;letter-spacing:.08em}
.badge.buy{background:rgba(25,242,107,.12);color:var(--green);border:1px solid rgba(25,242,107,.35)}
.badge.sell{background:rgba(255,77,106,.12);color:var(--red);border:1px solid rgba(255,77,106,.35)}
.empty{color:var(--muted);font-size:13px;padding:22px 0;text-align:center}
/* log */
.log{max-height:360px;overflow-y:auto;border:1px solid var(--line);border-radius:12px;background:#060a13}
.log.tall{max-height:70vh}
.ll{display:grid;grid-template-columns:104px 1fr;gap:12px;padding:9px 14px;border-bottom:1px solid #0d1626;
  font:12.5px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;border-left:3px solid transparent}
.ll:last-child{border-bottom:0}
.ll .t{color:var(--muted)}
.ll.ok{border-left-color:var(--green)}.ll.ok .m{color:#b9ffd3}
.ll.info{border-left-color:var(--blue)}
.ll.warn{border-left-color:var(--amber)}.ll.warn .m{color:#ffd88a}
.ll.error{border-left-color:var(--red)}.ll.error .m{color:#ff9db0}
.ll .k{display:inline-block;font-size:10px;letter-spacing:.1em;padding:1px 7px;border-radius:5px;margin-right:8px;
  background:rgba(47,128,255,.14);color:#8db8ff;font-weight:700}
.ll.ok .k{background:rgba(25,242,107,.13);color:var(--green)}
.ll.error .k{background:rgba(255,77,106,.14);color:var(--red)}
.ll.warn .k{background:rgba(255,176,32,.13);color:var(--amber)}
/* forms */
label{display:block;font-size:12px;color:var(--muted);font-weight:600;margin-bottom:6px;letter-spacing:.03em}
.f{margin-bottom:16px}
input[type=text],input[type=number],input[type=password]{width:100%;padding:13px 14px;border-radius:12px;color:var(--text);
  background:#060b15;border:1px solid var(--line2);font-size:15px;outline:none;transition:.15s;font-family:inherit}
input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(47,128,255,.18)}
.note{font-size:12px;color:var(--muted);margin-top:6px;line-height:1.5}
.row{display:flex;gap:10px;align-items:center}
.btn{padding:12px 20px;border-radius:12px;border:1px solid var(--line2);background:#0c1626;color:var(--text);
  font-weight:700;font-size:13px;cursor:pointer;transition:.15s;font-family:inherit;white-space:nowrap}
.btn:hover{border-color:var(--blue);background:#0f1c33}
.btn.primary{background:linear-gradient(135deg,var(--blue),var(--blue2));border-color:transparent;color:#fff;box-shadow:0 6px 22px rgba(47,128,255,.3)}
.btn.green{background:linear-gradient(135deg,var(--green),var(--green2));border-color:transparent;color:#03150a}
.btn.sm{padding:7px 12px;font-size:12px}
.switch{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;border:1px solid var(--line2);border-radius:12px;background:#060b15}
.switch input{appearance:none;width:44px;height:25px;border-radius:99px;background:#1a2740;position:relative;cursor:pointer;transition:.2s;flex:none}
.switch input::after{content:"";position:absolute;top:3px;left:3px;width:19px;height:19px;border-radius:50%;background:#8aa0c6;transition:.2s}
.switch input:checked{background:var(--green2)}
.switch input:checked::after{left:22px;background:#fff}
code.k{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px;color:var(--cyan);background:#060b15;border:1px solid var(--line2);padding:10px 12px;border-radius:10px;display:block;word-break:break-all}
ol.steps{margin:6px 0 0 18px;color:var(--muted);font-size:13px;line-height:1.8}
ol.steps b{color:var(--text)}
.banner{display:none;margin-bottom:14px;padding:12px 16px;border-radius:12px;background:rgba(255,77,106,.1);border:1px solid rgba(255,77,106,.4);color:#ffb3c0;font-size:13px;font-weight:600}
.toast{position:fixed;right:20px;bottom:24px;z-index:50;padding:13px 18px;border-radius:12px;font-weight:600;font-size:13.5px;
  background:#0b1424;border:1px solid var(--line2);box-shadow:0 14px 40px rgba(0,0,0,.6);transform:translateY(20px);opacity:0;pointer-events:none;transition:.25s;max-width:calc(100vw - 40px)}
.toast.show{transform:none;opacity:1}
.toast.ok{border-color:rgba(25,242,107,.5)}.toast.err{border-color:rgba(255,77,106,.55);color:#ffb3c0}
::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-thumb{background:#1a2b49;border-radius:8px}
/* responsive */
@media(max-width:980px){.g4{grid-template-columns:repeat(2,minmax(0,1fr))}.g3{grid-template-columns:1fr}.g2{grid-template-columns:1fr}}
@media(max-width:860px){
  .app{grid-template-columns:1fr}
  .side{position:fixed;z-index:30;bottom:0;left:0;right:0;top:auto;height:auto;flex-direction:row;padding:8px 10px calc(8px + env(safe-area-inset-bottom));
    border-right:0;border-top:1px solid var(--line2);background:rgba(5,8,15,.96);backdrop-filter:blur(10px)}
  .brand,.side .foot{display:none}
  .nav{flex-direction:row;width:100%;justify-content:space-around}
  .nav a{flex-direction:column;gap:4px;padding:8px 14px;font-size:11px;box-shadow:none!important}
  main{padding:16px 14px 110px}
  .hero{min-height:0}
  .hero-img{width:100%;opacity:.38}
  .hero-img img{-webkit-mask-image:linear-gradient(to bottom,#000 30%,transparent);mask-image:linear-gradient(to bottom,#000 30%,transparent)}
  .hero-text{padding:22px 18px}.hero h1{font-size:26px}
  .play{width:100%;justify-content:center}
  .ll{grid-template-columns:1fr;gap:2px}
}
</style>
</head>
<body>
<div class="app">
  <aside class="side">
    <div class="brand">
      <img src="/static/robot.jpg" alt="" onerror="this.style.display='none'">
      <div><b>ScalpRules</b><span>ENGINE</span></div>
    </div>
    <nav class="nav">
      <a data-view="dashboard" class="active">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9" rx="1.5"/><rect x="14" y="3" width="7" height="5" rx="1.5"/><rect x="14" y="12" width="7" height="9" rx="1.5"/><rect x="3" y="16" width="7" height="5" rx="1.5"/></svg>
        Dashboard</a>
      <a data-view="config">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/><circle cx="9" cy="6" r="2.2" fill="#04060c"/><circle cx="15" cy="12" r="2.2" fill="#04060c"/><circle cx="8" cy="18" r="2.2" fill="#04060c"/></svg>
        Configuration</a>
      <a data-view="logs">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 5h16M4 10h16M4 15h10M4 20h7"/></svg>
        Activity Log</a>
    </nav>
    <div class="foot">M5 pattern · M1 confirmation<br>Trendline confluence<br><span id="clock"></span></div>
  </aside>

  <main>
    <div class="banner" id="banner">Engine unreachable · retrying…</div>

    <!-- DASHBOARD -->
    <section class="view active" id="view-dashboard">
      <div class="hero">
        <div class="hero-img"><img src="/static/robot.jpg" alt="" onerror="this.style.display='none'"></div>
        <div class="hero-text">
          <div class="eyebrow">ANALYZE · EXECUTE · PROFIT</div>
          <h1>ScalpRules <em>Engine</em></h1>
          <p>Controls your EA over web requests. Press play and the EA is allowed to trade your rules.</p>
          <div class="chips" id="chips"></div>
          <div class="controls">
            <button class="play" id="toggle"></button>
            <div class="hint" id="toggleHint"></div>
          </div>
        </div>
      </div>

      <div class="grid g4">
        <div class="card stat"><h3>EA STATUS</h3><div class="v" id="stEa">-</div><div class="s" id="stEaSub">-</div></div>
        <div class="card stat"><h3>SYMBOL</h3><div class="v" id="stSym">-</div><div class="s" id="stSymSub">-</div></div>
        <div class="card stat"><h3>OPEN POSITIONS</h3><div class="v" id="stPos">-</div><div class="s" id="stPosSub">-</div></div>
        <div class="card stat"><h3>TRADES TODAY</h3><div class="v" id="stToday">0</div><div class="s" id="stTotal">-</div></div>
      </div>

      <div class="grid g3 section">
        <div class="card stat"><h3>LOT SIZE</h3><div class="v blue" id="sLot">-</div></div>
        <div class="card stat"><h3>STOP LOSS</h3><div class="v red" id="sSL">-</div><div class="s" id="sSLs"></div></div>
        <div class="card stat"><h3>TAKE PROFIT</h3><div class="v green" id="sTP">-</div><div class="s" id="sTPs"></div></div>
      </div>

      <div class="card section">
        <h3>TRADES EXECUTED</h3>
        <div class="tw"><table>
          <thead><tr><th>TIME</th><th>SYMBOL</th><th>SIDE</th><th>LOTS</th><th>ENTRY</th><th>SL</th><th>TP</th><th>SETUP</th><th>TICKET</th></tr></thead>
          <tbody id="tradeRows"></tbody>
        </table></div>
        <div class="empty" id="tradeEmpty">No trades executed yet.</div>
      </div>

      <div class="card section">
        <h3>LIVE LOG <a class="btn sm" data-view="logs" style="text-decoration:none">VIEW ALL</a></h3>
        <div class="log" id="miniLog"></div>
      </div>
    </section>

    <!-- CONFIG -->
    <section class="view" id="view-config">
      <h2 class="title">Configuration</h2>
      <p class="sub">These values are sent to the EA every couple of seconds. Changes apply to new trades.</p>
      <div class="grid g2">
        <div class="card">
          <h3>TRADING</h3>
          <div class="f"><label for="cfgLot">LOT SIZE</label><input type="number" id="cfgLot" step="0.01" min="0.01" inputmode="decimal"></div>
          <div class="f"><label for="cfgSL">STOP LOSS (points)</label><input type="number" id="cfgSL" step="1" min="1" inputmode="numeric">
            <div class="note" id="slNote"></div></div>
          <div class="f"><label for="cfgTP">TAKE PROFIT (points)</label><input type="number" id="cfgTP" step="1" min="1" inputmode="numeric">
            <div class="note" id="tpNote"></div></div>
          <div class="note">Pause stops new entries only. Trades already open keep the SL/TP they were opened with.</div>
        </div>

        <div class="card">
          <h3>TELEGRAM</h3>
          <div class="f switch"><span style="font-weight:600;font-size:14px">Notify me when a trade is executed</span><input type="checkbox" id="cfgTgOn"></div>
          <div class="f"><label for="cfgTgToken">BOT TOKEN</label><input type="password" id="cfgTgToken" autocomplete="off" placeholder="123456:ABC-DEF…">
            <div class="note">Create a bot with @BotFather and paste its token. Leave blank to keep the saved one.</div></div>
          <div class="f"><label for="cfgTgChat">CHAT ID</label><input type="text" id="cfgTgChat" placeholder="e.g. 123456789" inputmode="numeric">
            <div class="note">Send any message to your bot first, then get your chat ID from @userinfobot.</div></div>
          <button class="btn" id="btnTest">SAVE &amp; SEND TEST MESSAGE</button>
        </div>
      </div>

      <div class="row section"><button class="btn primary" id="btnSave">SAVE CONFIGURATION</button></div>

      <div class="card section">
        <h3>EA CONNECTION</h3>
        <div class="grid g2">
          <div><label>ENGINE URL (EA input: Engine URL)</label><code class="k" id="cUrl"></code>
            <div class="row" style="margin-top:8px"><button class="btn sm" data-copy="cUrl">COPY</button></div></div>
          <div><label>ENGINE TOKEN (EA input: Engine token)</label><code class="k" id="cTok"></code>
            <div class="row" style="margin-top:8px"><button class="btn sm" data-copy="cTok">COPY</button></div></div>
        </div>
        <ol class="steps">
          <li>In MT5: <b>Tools → Options → Expert Advisors → Allow WebRequest for listed URL</b> and add the Engine URL.</li>
          <li>Attach <b>ScalpRulesEA</b> to the chart, paste the Engine URL and token in its inputs, enable <b>Algo Trading</b>.</li>
          <li>The status chips on the Dashboard turn green when the EA connects. Press <b>PLAY</b> to let it trade.</li>
        </ol>
      </div>
    </section>

    <!-- LOGS -->
    <section class="view" id="view-logs">
      <div class="row" style="justify-content:space-between;margin-bottom:6px">
        <div><h2 class="title">Activity Log</h2><p class="sub" style="margin-bottom:12px">EA connection, run state, trades and Telegram delivery.</p></div>
        <button class="btn sm" id="btnClear">CLEAR LOG</button>
      </div>
      <div class="log tall" id="fullLog"></div>
    </section>
  </main>
</div>
<div class="toast" id="toast"></div>

<script>
const $ = s => document.querySelector(s);
const $$ = s => Array.from(document.querySelectorAll(s));
let S = null, formLoaded = false, failCount = 0;

function esc(s){ return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

async function api(path, body){
  const opt = body === undefined ? {} : {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)};
  const r = await fetch(path, opt);
  let j = {}; try { j = await r.json(); } catch(e){}
  if(!r.ok) throw new Error(j.error || r.statusText || 'Request failed');
  return j;
}

let toastTimer;
function toast(msg, ok=true){
  const t = $('#toast'); t.textContent = msg; t.className = 'toast show ' + (ok ? 'ok' : 'err');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.className = 'toast', 3200);
}

/* navigation */
function showView(v){
  if(!['dashboard','config','logs'].includes(v)) v = 'dashboard';
  $$('.view').forEach(e => e.classList.toggle('active', e.id === 'view-' + v));
  $$('.nav a').forEach(a => a.classList.toggle('active', a.dataset.view === v));
  history.replaceState(null, '', '#' + v);
  window.scrollTo(0, 0);
}
document.addEventListener('click', e => {
  const a = e.target.closest('[data-view]'); if(a){ e.preventDefault(); showView(a.dataset.view); }
  const c = e.target.closest('[data-copy]');
  if(c){ const el = $('#' + c.dataset.copy); navigator.clipboard.writeText(el.textContent).then(() => toast('Copied')).catch(() => toast('Copy failed', false)); }
});

/* render helpers */
function chip(cls, label){ return `<span class="chip"><i class="dot ${cls}"></i>${label}</span>`; }
function fmtNum(v, digits){
  if(v === null || v === undefined || v === '') return '-';
  const n = Number(v); if(isNaN(n)) return esc(v);
  return (digits !== undefined && digits !== null) ? n.toFixed(digits) : String(n);
}
function logHtml(list){
  if(!list.length) return '<div class="empty">No activity yet.</div>';
  return list.map(e => `<div class="ll ${esc(e.level)}"><div class="t">${esc((e.ts||'').slice(5))}</div>
    <div class="m"><span class="k">${esc((e.kind||'').toUpperCase())}</span>${esc(e.msg)}</div></div>`).join('');
}

function render(){
  const c = S.config, ea = S.ea || {}, conn = S.ea_connected, digits = (ea.digits !== undefined ? Number(ea.digits) : null);
  const point = Number(ea.point) || 0;

  // chips
  const tgReady = c.tg_token_set && c.tg_chat;
  $('#chips').innerHTML =
    chip(conn ? 'ok' : 'bad', 'EA ' + (conn ? 'CONNECTED' : 'DISCONNECTED')) +
    chip(!conn ? 'bad' : (S.ea_running ? 'ok' : 'warn'), 'EA ' + (!conn ? 'OFFLINE' : (S.ea_running ? 'RUNNING' : 'PAUSED'))) +
    chip(S.engine_run ? 'blue' : 'warn', 'ENGINE ' + (S.engine_run ? 'PLAYING' : 'PAUSED')) +
    chip(!tgReady ? 'bad' : (c.tg_enabled ? 'ok' : 'warn'), 'TELEGRAM ' + (!tgReady ? 'NOT SET' : (c.tg_enabled ? 'READY' : 'OFF')));

  // play / pause
  const btn = $('#toggle');
  btn.className = 'play' + (S.engine_run ? ' running' : '');
  btn.innerHTML = S.engine_run
    ? '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="5" y="4" width="5" height="16" rx="1.2"/><rect x="14" y="4" width="5" height="16" rx="1.2"/></svg>PAUSE'
    : '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15a1 1 0 0 0 1.5.86l12-7.5a1 1 0 0 0 0-1.72l-12-7.5A1 1 0 0 0 7 4.5z"/></svg>PLAY';
  $('#toggleHint').textContent = !S.engine_run
    ? 'Paused · the EA will not open new trades.'
    : (conn ? 'Playing · the EA is trading your rules.' : 'Playing, but the EA is offline · attach it to a chart.');

  // stats
  const st = $('#stEa');
  st.textContent = !conn ? 'OFFLINE' : (S.ea_running ? 'RUNNING' : 'PAUSED');
  st.className = 'v ' + (!conn ? 'red' : (S.ea_running ? 'green' : 'amber'));
  $('#stEaSub').textContent = conn ? ('Account ' + (ea.account || '?') + ' · ' + (ea.server || '')) : 'No heartbeat from the EA';
  $('#stSym').textContent = conn ? (ea.symbol || '-') : '-';
  $('#stSymSub').textContent = conn ? ('Spread ' + fmtNum(ea.spread) + ' pts') : 'Waiting for EA';
  const ob = Number(ea.open_buy || 0), os = Number(ea.open_sell || 0);
  $('#stPos').textContent = conn ? (ob + os) : '-';
  $('#stPosSub').textContent = conn ? (ob + ' buy · ' + os + ' sell') : 'Waiting for EA';
  const today = (S.server_time || '').slice(0, 10);
  $('#stToday').textContent = S.trades.filter(t => (t.ts || '').startsWith(today)).length;
  $('#stTotal').textContent = S.trades.length + ' total';

  $('#sLot').textContent = c.lot;
  $('#sSL').textContent = c.sl_pts + ' pts';
  $('#sTP').textContent = c.tp_pts + ' pts';
  $('#sSLs').textContent = point ? ('≈ ' + (c.sl_pts * point).toFixed(Math.max(2, digits || 2)) + ' price distance') : '';
  $('#sTPs').textContent = point ? ('≈ ' + (c.tp_pts * point).toFixed(Math.max(2, digits || 2)) + ' price distance') : '';

  // trades
  const tr = S.trades.slice().reverse();
  $('#tradeEmpty').style.display = tr.length ? 'none' : 'block';
  $('#tradeRows').innerHTML = tr.map(t => `<tr>
    <td>${esc(t.time || t.ts)}</td><td><b>${esc(t.symbol)}</b></td>
    <td><span class="badge ${t.side === 'BUY' ? 'buy' : 'sell'}">${esc(t.side)}</span></td>
    <td>${fmtNum(t.lots)}</td><td><b>${fmtNum(t.price, digits)}</b></td>
    <td style="color:var(--red)">${fmtNum(t.sl, digits)}</td><td style="color:var(--green)">${fmtNum(t.tp, digits)}</td>
    <td>${esc(t.pattern || '-')}</td><td>${esc(t.ticket || '-')}</td></tr>`).join('');

  // logs
  const ev = S.events.slice().reverse();
  $('#miniLog').innerHTML = logHtml(ev.slice(0, 10));
  $('#fullLog').innerHTML = logHtml(ev);

  // config form (fill once, then only after saving)
  if(!formLoaded){ fillForm(c); formLoaded = true; }
  $('#cUrl').textContent = c.ea_url;
  $('#cTok').textContent = c.ea_token;
  if(c.tg_token_set) $('#cfgTgToken').placeholder = 'Saved (' + c.tg_token_hint + ') · leave blank to keep';
  updateNotes(point, digits);
  $('#clock').textContent = (S.server_time || '').slice(11);
}

function fillForm(c){
  $('#cfgLot').value = c.lot; $('#cfgSL').value = c.sl_pts; $('#cfgTP').value = c.tp_pts;
  $('#cfgTgOn').checked = !!c.tg_enabled; $('#cfgTgChat').value = c.tg_chat || ''; $('#cfgTgToken').value = '';
}
function updateNotes(point, digits){
  const d = Math.max(2, digits || 2);
  const sl = Number($('#cfgSL').value), tp = Number($('#cfgTP').value);
  $('#slNote').textContent = point && sl ? '≈ ' + (sl * point).toFixed(d) + ' price distance from entry' : 'Distance from entry, in broker points';
  $('#tpNote').textContent = point && tp ? '≈ ' + (tp * point).toFixed(d) + ' price distance from entry' : 'Distance from entry, in broker points';
}
['cfgSL','cfgTP'].forEach(id => document.addEventListener('input', e => { if(e.target.id === id && S) updateNotes(Number((S.ea||{}).point)||0, (S.ea||{}).digits); }));

async function saveConfig(){
  const body = {
    lot: $('#cfgLot').value, sl_pts: $('#cfgSL').value, tp_pts: $('#cfgTP').value,
    tg_enabled: $('#cfgTgOn').checked, tg_chat: $('#cfgTgChat').value, tg_token: $('#cfgTgToken').value
  };
  const j = await api('/api/config', body);
  fillForm(j.config);
  await refresh();
}

/* actions */
$('#toggle').addEventListener('click', async () => {
  if(!S) return;
  try { await api('/api/control', {action: S.engine_run ? 'pause' : 'play'}); await refresh(); }
  catch(e){ toast(e.message, false); }
});
$('#btnSave').addEventListener('click', async () => {
  try { await saveConfig(); toast('Configuration saved'); } catch(e){ toast(e.message, false); }
});
$('#btnTest').addEventListener('click', async () => {
  try { await saveConfig(); await api('/api/telegram/test', {}); toast('Test message sent to Telegram'); }
  catch(e){ toast('Telegram: ' + e.message, false); }
  refresh();
});
$('#btnClear').addEventListener('click', async () => {
  if(!confirm('Clear the activity log? Trade history is kept.')) return;
  try { await api('/api/log/clear', {}); await refresh(); } catch(e){ toast(e.message, false); }
});

async function refresh(){
  try {
    S = await api('/api/state'); failCount = 0; $('#banner').style.display = 'none'; render();
  } catch(e){
    if(++failCount >= 2) $('#banner').style.display = 'block';
  }
}

showView((location.hash || '#dashboard').slice(1));
refresh(); setInterval(refresh, 2000);
</script>
</body>
</html>
"""

start_background()
log("system", "Engine started · PAUSED. Press PLAY to let the EA trade.", "info")

if __name__ == "__main__":
    print("=" * 60)
    print(" ScalpRules Engine")
    print(f" Panel : http://127.0.0.1:{PORT}")
    print(f" EA URL: http://127.0.0.1:{PORT}")
    print(f" Token : {config['ea_token']}")
    print("=" * 60)
    app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False)
