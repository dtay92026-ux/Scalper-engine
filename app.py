"""
ScalpRules Engine
=================
Control panel + web API for ScalpRulesEA.mq5

  * PLAY / PAUSE the EA from the browser
  * Settings: lot size, SL and TP in MONEY, Telegram
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
    ENGINE_TZ            hours from UTC for the clock/log, default 2 (GMT+2)
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
from datetime import datetime, timedelta, timezone
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
try:
    TZ_HOURS = float(os.environ.get("ENGINE_TZ", "2"))   # shown/logged time zone, default GMT+2
except ValueError:
    TZ_HOURS = 2.0
TZ = timezone(timedelta(hours=TZ_HOURS))
TZ_LABEL = "GMT" + f"{TZ_HOURS:+g}"

app = Flask(__name__, static_folder=str(BASE_DIR / "static"), static_url_path="/static")
_lock = threading.RLock()

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
DEFAULTS = {
    "run": False,
    "lot": 0.01,
    "sl_money": 5.0,
    "tp_money": 6.0,
    "max_trades": 1,
    "repeat": True,
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
    return {k: cfg.get(k, DEFAULTS[k]) for k in DEFAULTS}


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
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


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
        "sl_money": float(config["sl_money"]),
        "tp_money": float(config["tp_money"]),
        "max_trades": int(config["max_trades"]),
        "repeat": 1 if config["repeat"] else 0,
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
def _public_url():
    """The address the EA should use (works locally and when hosted online)."""
    try:
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        return f"{proto}://{request.host}"
    except RuntimeError:
        return f"http://127.0.0.1:{PORT}"


def public_config():
    tok = config["tg_token"]
    hint = (tok[:4] + "…" + tok[-4:]) if len(tok) > 10 else ("saved" if tok else "")
    return {
        "run": config["run"],
        "lot": config["lot"],
        "sl_money": config["sl_money"],
        "tp_money": config["tp_money"],
        "max_trades": config["max_trades"],
        "repeat": bool(config["repeat"]),
        "tg_enabled": config["tg_enabled"],
        "tg_chat": config["tg_chat"],
        "tg_token_set": bool(tok),
        "tg_token_hint": hint,
        "ea_token": config["ea_token"],
        "ea_url": _public_url(),
    }


@app.get("/api/state")
def api_state():
    with _lock:
        return jsonify({
            "server_time": now_str(),
            "tz": TZ_LABEL,
            "ea_age": (int(time.time() - ea["last_seen"]) if ea["last_seen"] > 0 else None),
            "engine_run": config["run"],
            "ea_connected": ea["connected"],
            "ea_running": ea["running"] if ea["connected"] else None,
            "ea": ea["info"],
            "ea_age": (time.time() - ea["last_seen"]) if ea["connected"] else None,
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
        if "max_trades" in b:
            n = int(float(b["max_trades"]))
            if not 1 <= n <= 100:
                raise ValueError("Number of trades must be between 1 and 100")
            new["max_trades"] = n
        for key, label in (("sl_money", "Stop loss"), ("tp_money", "Take profit")):
            if key in b:
                v = float(b[key])
                if not 0.01 <= v <= 1_000_000:
                    raise ValueError(f"{label} amount must be at least 0.01")
                new[key] = round(v, 2)
        if "repeat" in b:
            new["repeat"] = bool(b["repeat"])
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
    trading = [k for k in changed if k in ("lot", "max_trades", "repeat", "sl_money", "tp_money")]
    if trading:
        log("control",
            f"Settings saved · lot {config['lot']} · {config['max_trades']} trade(s) · {'keep trading' if config['repeat'] else 'one batch per START'} · SL {config['sl_money']} · TP {config['tp_money']} (money per trade)",
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
<meta name="theme-color" content="#000000">
<title>ScalpRules Engine</title>
<link rel="icon" href="/static/robot.jpg">
<style>
:root{
  --card:#05080f; --line:#13203a; --line2:#1c2d4d; --text:#dfe8f7; --muted:#6f83a6;
  --blue:#2f80ff; --cyan:#22d3ee; --green:#1fe07f; --red:#ff4d6a; --amber:#ffb020;
}
*{box-sizing:border-box;margin:0;padding:0;min-width:0}
html,body{max-width:100%;overflow-x:hidden}
body{
  background:radial-gradient(700px 380px at 50% -6%,rgba(47,128,255,.14),transparent 65%),
             radial-gradient(500px 300px at 100% 100%,rgba(31,224,127,.07),transparent 70%),#000;
  color:var(--text);min-height:100vh;-webkit-tap-highlight-color:transparent;
  font-family:Inter,"Segoe UI",system-ui,-apple-system,Roboto,Helvetica,Arial,sans-serif;
}
.wrap{max-width:560px;margin:0 auto;padding:calc(18px + env(safe-area-inset-top)) 16px calc(44px + env(safe-area-inset-bottom))}

/* header */
.top{display:flex;align-items:center;gap:14px}
.logo{width:58px;height:58px;border-radius:16px;object-fit:cover;object-position:50% 26%;flex:none;
  border:1px solid var(--line2);box-shadow:0 0 22px rgba(31,224,127,.3)}
.name b{display:block;font-size:21px;letter-spacing:.26em;font-weight:700;color:#e9f0ff;white-space:nowrap}
.name b i{font-style:normal;color:var(--blue)}
.name span{display:block;margin-top:4px;font-size:11px;letter-spacing:.3em;color:var(--muted)}
.bar{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-top:18px;padding-bottom:16px;border-bottom:1px solid var(--line)}
.pill{display:inline-flex;align-items:center;gap:9px;padding:9px 15px;border-radius:999px;font-size:12px;
  letter-spacing:.2em;font-weight:600;border:1px solid var(--line2);color:var(--muted);white-space:nowrap}
.pill i{width:8px;height:8px;border-radius:50%;background:var(--muted);flex:none}
.pill.on{color:var(--green);border-color:rgba(31,224,127,.35);background:rgba(31,224,127,.06)}
.pill.on i{background:var(--green);box-shadow:0 0 10px var(--green)}
.pill.off{color:var(--red);border-color:rgba(255,77,106,.35);background:rgba(255,77,106,.06)}
.pill.off i{background:var(--red);box-shadow:0 0 10px var(--red)}
.clock{font-size:17px;font-weight:600;letter-spacing:.04em;white-space:nowrap;font-variant-numeric:tabular-nums}
.clock small{font-size:11px;color:var(--muted);letter-spacing:.14em;margin-left:4px}

/* tabs */
.tabs{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:16px 0 4px}
.tabs button{padding:12px 4px;border-radius:14px;border:1px solid var(--line);background:transparent;color:var(--muted);
  font-weight:600;font-size:11.5px;line-height:1;letter-spacing:.2em;cursor:pointer;font-family:inherit}
.tabs button.active{color:#fff;border-color:rgba(47,128,255,.55);background:linear-gradient(180deg,rgba(47,128,255,.2),rgba(47,128,255,.05))}
.view{display:none}.view.active{display:block;animation:fade .25s ease}
@keyframes fade{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}

/* power card */
.power{margin-top:14px;border:1px solid var(--line);border-radius:30px;padding:22px 18px 18px;text-align:center;
  background:linear-gradient(180deg,#05080f,#020409)}
.lbl{display:block;text-align:left;font-size:12px;letter-spacing:.3em;color:var(--muted);font-weight:600}
.orb{position:relative;display:block;width:min(64vw,250px);aspect-ratio:1/1;margin:20px auto 20px;border-radius:50%;
  border:1px solid var(--line);background:#000;cursor:pointer;overflow:hidden;transition:box-shadow .3s;
  box-shadow:0 0 70px 4px rgba(47,128,255,.22)}
.orb img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;object-position:50% 28%;opacity:.16;
  filter:grayscale(1);transition:.3s;-webkit-mask-image:radial-gradient(circle,#000 35%,transparent 72%);mask-image:radial-gradient(circle,#000 35%,transparent 72%)}
.orb svg{position:relative;width:38%;height:38%;color:var(--blue);transition:.3s}
.orb{display:flex;align-items:center;justify-content:center}
.orb.on{box-shadow:0 0 80px 8px rgba(31,224,127,.28);border-color:rgba(31,224,127,.3)}
.orb.on svg{color:var(--green)}
.orb.on img{opacity:.3;filter:none}
.orb:active{transform:scale(.985)}
.state{font-size:28px;font-weight:700;letter-spacing:.34em;margin-left:.34em;color:#c9d4e8}
.state.on{color:var(--green)}
.sub{margin-top:10px;font-size:14.5px;color:var(--muted);line-height:1.5;padding:0 6px}
.cmd{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--line2);border-radius:18px;margin:18px 0 14px;text-align:left}
.cmd>div{padding:14px 16px}
.cmd>div+div{border-left:1px solid var(--line)}
.cmd small{display:block;font-size:10.5px;letter-spacing:.22em;color:var(--muted);margin-bottom:7px;font-weight:600}
.cmd b{font-size:18px;letter-spacing:.1em}
.tap{font-size:12px;letter-spacing:.3em;color:var(--muted);font-weight:600}
.g{color:var(--green)}.r{color:var(--red)}.a{color:var(--amber)}.b{color:var(--blue)}

/* cards */
.card{margin-top:14px;border:1px solid var(--line);border-radius:24px;padding:18px;background:var(--card)}
.card h3{font-size:12px;letter-spacing:.26em;color:var(--muted);font-weight:600;margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;gap:8px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px}
.stat{border:1px solid var(--line);border-radius:20px;padding:15px 16px;background:var(--card)}
.stat small{display:block;font-size:10.5px;letter-spacing:.22em;color:var(--muted);font-weight:600;margin-bottom:8px}
.stat b{display:block;font-size:20px;letter-spacing:.02em;overflow-wrap:anywhere}
.stat em{display:block;font-style:normal;font-size:12px;color:var(--muted);margin-top:4px}

/* trades */
.tr{border:1px solid var(--line);border-radius:16px;padding:13px 14px;margin-bottom:10px;background:#03060c}
.tr:last-child{margin-bottom:0}
.tr .h{display:flex;justify-content:space-between;align-items:center;gap:8px}
.tr .h span{font-size:12px;color:var(--muted)}
.badge{display:inline-block;padding:4px 11px;border-radius:8px;font-size:11px;font-weight:800;letter-spacing:.1em;margin-right:8px}
.badge.buy{background:rgba(31,224,127,.12);color:var(--green);border:1px solid rgba(31,224,127,.35)}
.badge.sell{background:rgba(255,77,106,.12);color:var(--red);border:1px solid rgba(255,77,106,.35)}
.tr .p{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:11px}
.tr .p small{display:block;font-size:9.5px;letter-spacing:.18em;color:var(--muted);font-weight:600;margin-bottom:3px}
.tr .p b{font-size:14px;font-variant-numeric:tabular-nums}
.tr .f{margin-top:10px;font-size:11.5px;color:var(--muted);overflow-wrap:anywhere}
.empty{color:var(--muted);font-size:13.5px;text-align:center;padding:14px 0}

/* log */
.log{border:1px solid var(--line);border-radius:16px;background:#03060c;overflow:hidden}
.ll{padding:10px 13px;border-bottom:1px solid #0c1527;border-left:3px solid transparent}
.ll:last-child{border-bottom:0}
.ll .t{font-size:11px;color:var(--muted);letter-spacing:.06em;margin-bottom:3px;display:flex;gap:8px;align-items:center}
.ll .k{font-size:9.5px;letter-spacing:.14em;padding:1px 7px;border-radius:5px;background:rgba(47,128,255,.14);color:#8db8ff;font-weight:700}
.ll .m{font-size:13px;line-height:1.5;overflow-wrap:anywhere}
.ll.ok{border-left-color:var(--green)}.ll.ok .m{color:#b9ffd8}.ll.ok .k{background:rgba(31,224,127,.13);color:var(--green)}
.ll.info{border-left-color:var(--blue)}
.ll.warn{border-left-color:var(--amber)}.ll.warn .m{color:#ffd88a}.ll.warn .k{background:rgba(255,176,32,.13);color:var(--amber)}
.ll.error{border-left-color:var(--red)}.ll.error .m{color:#ff9db0}.ll.error .k{background:rgba(255,77,106,.14);color:var(--red)}
.scroll{max-height:60vh;overflow-y:auto}

/* forms */
label{display:block;font-size:11px;color:var(--muted);font-weight:600;margin-bottom:7px;letter-spacing:.18em}
.f{margin-bottom:18px}
.inp{position:relative}
.inp input{padding-right:62px}
.inp span{position:absolute;right:14px;top:50%;transform:translateY(-50%);font-size:12px;color:var(--muted);letter-spacing:.1em;font-weight:600}
input[type=text],input[type=number],input[type=password]{width:100%;padding:14px;border-radius:14px;color:var(--text);
  background:#02050b;border:1px solid var(--line2);font-size:16px;outline:none;font-family:inherit}
input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(47,128,255,.2)}
.note{font-size:12px;color:var(--muted);margin-top:7px;line-height:1.5}
.btn{width:100%;padding:15px;border-radius:14px;border:1px solid var(--line2);background:#0a1322;color:var(--text);
  font-weight:700;font-size:12px;line-height:1;font-family:inherit;letter-spacing:.2em;cursor:pointer;margin-top:4px}
.btn:active{transform:scale(.99)}
.btn.primary{background:linear-gradient(135deg,var(--blue),#1747c9);border-color:transparent;color:#fff;box-shadow:0 8px 24px rgba(47,128,255,.28)}
.btn.sm{width:auto;padding:8px 14px;font-size:10.5px;margin:0}
.switch{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 14px;border:1px solid var(--line2);border-radius:14px;background:#02050b;font-size:14px;font-weight:600}
.switch input{appearance:none;-webkit-appearance:none;width:46px;height:26px;border-radius:99px;background:#17233b;position:relative;cursor:pointer;flex:none;transition:.2s}
.switch input::after{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;background:#8aa0c6;transition:.2s}
.switch input:checked{background:#12a85f}
.switch input:checked::after{left:23px;background:#fff}
code.k{display:block;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12.5px;color:var(--cyan);background:#02050b;
  border:1px solid var(--line2);padding:12px;border-radius:12px;overflow-wrap:anywhere;word-break:break-all}
.copyrow{display:flex;justify-content:flex-end;margin-top:8px}
ol.steps{margin:14px 0 0 18px;color:var(--muted);font-size:13px;line-height:1.8}
ol.steps b{color:var(--text)}
.banner{display:none;margin-top:14px;padding:12px 14px;border-radius:14px;background:rgba(255,77,106,.1);border:1px solid rgba(255,77,106,.4);color:#ffb3c0;font-size:13px;font-weight:600}
.toast{position:fixed;left:16px;right:16px;bottom:calc(20px + env(safe-area-inset-bottom));z-index:50;padding:14px 16px;border-radius:14px;font-weight:600;font-size:14px;
  background:#0b1424;border:1px solid var(--line2);box-shadow:0 14px 40px rgba(0,0,0,.7);transform:translateY(20px);opacity:0;pointer-events:none;transition:.25s;text-align:center;max-width:528px;margin:0 auto}
.toast.show{transform:none;opacity:1}
.toast.ok{border-color:rgba(31,224,127,.5)}.toast.err{border-color:rgba(255,77,106,.55);color:#ffb3c0}
</style>
</head>
<body>
<div class="wrap">

  <div class="top">
    <img class="logo" src="/static/robot.jpg" alt="" onerror="this.style.display='none'">
    <div class="name"><b>SCALP<i>RULES</i></b><span>EA CONTROL ENGINE</span></div>
  </div>
  <div class="bar">
    <div class="pill off" id="pill"><i></i><span id="pillTxt">EA OFFLINE</span></div>
    <div class="clock"><span id="clk">--:--:--</span><small id="gmt"></small></div>
  </div>

  <div class="banner" id="banner">Engine unreachable · retrying…</div>

  <div class="tabs">
    <button data-tab="home" class="active">HOME</button>
    <button data-tab="settings">SETTINGS</button>
    <button data-tab="log">LOG</button>
  </div>

  <!-- HOME -->
  <section class="view active" id="view-home">
    <div class="power">
      <span class="lbl">ENGINE</span>
      <button class="orb" id="orb" aria-label="Start or stop the engine">
        <img src="/static/robot.jpg" alt="" onerror="this.style.display='none'">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M12 3v8"/><path d="M6.6 6.6a8 8 0 1 0 10.8 0"/></svg>
      </button>
      <div class="state" id="state">STOPPED</div>
      <div class="sub" id="stateSub">Stopped · no new trades</div>
      <div class="cmd">
        <div><small>COMMANDED</small><b id="cmdV">STOP</b></div>
        <div><small>EA STATE</small><b id="eaV">OFFLINE</b></div>
      </div>
      <div class="tap" id="tap">TAP TO START</div>
    </div>

    <div class="grid2">
      <div class="stat"><small>SYMBOL</small><b id="stSym">-</b><em id="stSpr">Waiting for EA</em></div>
      <div class="stat"><small>OPEN</small><b id="stPos">-</b><em id="stPosS">Waiting for EA</em></div>
      <div class="stat"><small>TRADES TODAY</small><b id="stToday">0</b><em id="stTotal">0 total</em></div>
      <div class="stat"><small>TELEGRAM</small><b id="stTg">NOT SET</b><em id="stTgS">Add it in Settings</em></div>
    </div>

    <div class="card">
      <h3>TRADES EXECUTED</h3>
      <div id="trades"></div>
    </div>
  </section>

  <!-- SETTINGS -->
  <section class="view" id="view-settings">
    <div class="card">
      <h3>TRADING</h3>
      <div class="f"><label for="cfgLot">LOT SIZE</label>
        <div class="inp"><input type="number" id="cfgLot" step="0.01" min="0.01" inputmode="decimal"><span>LOTS</span></div></div>
      <div class="f"><label for="cfgTrades">NUMBER OF TRADES</label>
        <div class="inp"><input type="number" id="cfgTrades" step="1" min="1" inputmode="numeric"><span>TRADES</span></div>
        <div class="note">How many trades the EA opens when you press START (and keeps open while it keeps trading).</div></div>
      <div class="f switch"><span>Keep trading until I press STOP</span><input type="checkbox" id="cfgRepeat"></div>
      <div class="note" style="margin:-8px 0 18px">On: the EA keeps that many trades open and opens new ones as they close. Off: it opens them once per START.</div>
      <div class="f"><label for="cfgSL">STOP LOSS · MONEY</label>
        <div class="inp"><input type="number" id="cfgSL" step="0.01" min="0.01" inputmode="decimal"><span class="cur">USD</span></div>
        <div class="note">The most you are willing to lose on one trade.</div></div>
      <div class="f"><label for="cfgTP">TAKE PROFIT · MONEY</label>
        <div class="inp"><input type="number" id="cfgTP" step="0.01" min="0.01" inputmode="decimal"><span class="cur">USD</span></div>
        <div class="note">The profit at which a trade closes. The EA converts both amounts into prices using the lot size.</div></div>
    </div>

    <div class="card">
      <h3>TELEGRAM</h3>
      <div class="f switch"><span>Notify me on every trade</span><input type="checkbox" id="cfgTgOn"></div>
      <div class="f"><label for="cfgTgToken">BOT TOKEN</label>
        <input type="password" id="cfgTgToken" autocomplete="off" placeholder="123456:ABC-DEF…">
        <div class="note">From @BotFather. Leave blank to keep the saved one.</div></div>
      <div class="f"><label for="cfgTgChat">CHAT ID</label>
        <input type="text" id="cfgTgChat" inputmode="numeric" placeholder="e.g. 123456789">
        <div class="note">Message your bot first, then get your ID from @userinfobot.</div></div>
      <button class="btn" id="btnTest">SAVE &amp; SEND TEST</button>
    </div>

    <button class="btn primary" id="btnSave" style="margin-top:14px">SAVE SETTINGS</button>

    <div class="card">
      <h3>EA CONNECTION</h3>
      <div class="f"><label>ENGINE URL</label><code class="k" id="cUrl"></code>
        <div class="copyrow"><button class="btn sm" data-copy="cUrl">COPY</button></div></div>
      <div class="f"><label>ENGINE TOKEN</label><code class="k" id="cTok"></code>
        <div class="copyrow"><button class="btn sm" data-copy="cTok">COPY</button></div></div>
      <ol class="steps">
        <li>MT5 → <b>Tools → Options → Expert Advisors</b> → Allow WebRequest → add the <b>Engine URL</b>.</li>
        <li>Paste the <b>URL</b> and <b>token</b> into the EA inputs.</li>
      </ol>
    </div>
  </section>

  <!-- LOG -->
  <section class="view" id="view-log">
    <div class="card">
      <h3>ACTIVITY LOG <button class="btn sm" id="btnClear">CLEAR</button></h3>
      <div class="log scroll" id="logBox"></div>
    </div>
  </section>

</div>
<div class="toast" id="toast"></div>

<script>
const $ = s => document.querySelector(s);
const $$ = s => Array.from(document.querySelectorAll(s));
let S = null, loaded = false, fails = 0;

function esc(s){ return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
async function api(path, body){
  const opt = body === undefined ? {} : {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)};
  const r = await fetch(path, opt);
  let j = {}; try { j = await r.json(); } catch(e){}
  if(!r.ok) throw new Error(j.error || r.statusText || 'Request failed');
  return j;
}
let tt;
function toast(msg, ok=true){
  const t = $('#toast'); t.textContent = msg; t.className = 'toast show ' + (ok ? 'ok' : 'err');
  clearTimeout(tt); tt = setTimeout(() => t.className = 'toast', 3000);
}

/* tabs */
function tab(v){
  if(!['home','settings','log'].includes(v)) v = 'home';
  $$('.view').forEach(e => e.classList.toggle('active', e.id === 'view-' + v));
  $$('.tabs button').forEach(b => b.classList.toggle('active', b.dataset.tab === v));
  history.replaceState(null, '', '#' + v);
}
$$('.tabs button').forEach(b => b.addEventListener('click', () => tab(b.dataset.tab)));
document.addEventListener('click', e => {
  const c = e.target.closest('[data-copy]');
  if(c){ navigator.clipboard.writeText($('#' + c.dataset.copy).textContent).then(() => toast('Copied')).catch(() => toast('Copy failed', false)); }
});

/* clock */
function tick(){
  const d = new Date(), p = n => String(n).padStart(2, '0');
  $('#clk').textContent = p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  const off = -d.getTimezoneOffset() / 60;
  $('#gmt').textContent = 'GMT' + (off >= 0 ? '+' : '-') + Math.abs(off);
}
setInterval(tick, 1000); tick();

function fmt(v, d){
  if(v === null || v === undefined || v === '') return '-';
  const n = Number(v); if(isNaN(n)) return esc(v);
  return (d !== null && d !== undefined && !isNaN(d)) ? n.toFixed(d) : String(n);
}
function money(v, cur){ const n = Number(v); return isNaN(n) ? '-' : n.toFixed(2) + ' ' + cur; }

function render(){
  const c = S.config, ea = S.ea || {}, conn = S.ea_connected;
  const cur = ea.currency || 'USD';
  const digits = ea.digits !== undefined ? Number(ea.digits) : null;

  // pill
  const pill = $('#pill');
  pill.className = 'pill ' + (conn ? 'on' : 'off');
  $('#pillTxt').textContent = conn ? 'EA ONLINE · ' + Math.max(0, Math.round(S.ea_age || 0)) + 's' : 'EA OFFLINE';

  // power card
  const run = S.engine_run;
  $('#orb').className = 'orb' + (run ? ' on' : '');
  const st = $('#state');
  st.textContent = run ? 'RUNNING' : 'STOPPED';
  st.className = 'state' + (run ? ' on' : '');
  $('#stateSub').textContent = !run ? 'Stopped · no new trades'
    : (!conn ? 'Armed · waiting for the EA to connect'
    : (S.ea_running ? 'Armed · EA is live' : 'Armed · EA is syncing'));
  $('#cmdV').textContent = run ? 'START' : 'STOP';
  $('#cmdV').className = run ? 'g' : 'a';
  const ev = $('#eaV');
  ev.textContent = !conn ? 'OFFLINE' : (S.ea_running ? 'RUNNING' : 'PAUSED');
  ev.className = !conn ? 'r' : (S.ea_running ? 'g' : 'a');
  $('#tap').textContent = run ? 'TAP TO STOP' : 'TAP TO START';

  // stats
  $('#stSym').textContent = conn ? (ea.symbol || '-') : '-';
  $('#stSpr').textContent = conn ? ('Spread ' + fmt(ea.spread) + ' pts') : 'Waiting for EA';
  const ob = Number(ea.open_buy || 0), os = Number(ea.open_sell || 0);
  $('#stPos').textContent = conn ? (ob + os) : '-';
  $('#stPosS').textContent = conn ? (ob + ' buy · ' + os + ' sell') : 'Waiting for EA';
  const today = (S.server_time || '').slice(0, 10);
  $('#stToday').textContent = S.trades.filter(t => (t.ts || '').startsWith(today)).length;
  $('#stTotal').textContent = S.trades.length + ' total';
  const tgReady = c.tg_token_set && c.tg_chat;
  const tg = $('#stTg');
  tg.textContent = !tgReady ? 'NOT SET' : (c.tg_enabled ? 'READY' : 'OFF');
  tg.className = !tgReady ? 'r' : (c.tg_enabled ? 'g' : 'a');
  $('#stTgS').textContent = !tgReady ? 'Add it in Settings' : (c.tg_enabled ? 'Alerts on every trade' : 'Alerts switched off');

  // trades
  const tr = S.trades.slice().reverse().slice(0, 30);
  $('#trades').innerHTML = tr.length ? tr.map(t => `
    <div class="tr">
      <div class="h"><div><span class="badge ${t.side === 'BUY' ? 'buy' : 'sell'}">${esc(t.side)}</span><b>${esc(t.symbol)}</b></div><span>${esc(t.time || t.ts)}</span></div>
      <div class="p">
        <div><small>ENTRY</small><b>${fmt(t.price, digits)}</b></div>
        <div><small>SL</small><b class="r">${fmt(t.sl, digits)}</b></div>
        <div><small>TP</small><b class="g">${fmt(t.tp, digits)}</b></div>
      </div>
      <div class="f">${fmt(t.lots)} lots${t.pattern ? ' · ' + esc(t.pattern) : ''}${t.ticket && t.ticket !== '0' ? ' · #' + esc(t.ticket) : ''}</div>
    </div>`).join('') : '<div class="empty">No trades executed yet.</div>';

  // log
  const evs = S.events.slice().reverse();
  $('#logBox').innerHTML = evs.length ? evs.map(e => `
    <div class="ll ${esc(e.level)}"><div class="t"><span class="k">${esc((e.kind || '').toUpperCase())}</span>${esc((e.ts || '').slice(5))}</div>
    <div class="m">${esc(e.msg)}</div></div>`).join('') : '<div class="empty">No activity yet.</div>';

  // settings
  if(!loaded){ fillForm(c); loaded = true; }
  $$('.cur').forEach(e => e.textContent = cur);
  $('#cUrl').textContent = c.ea_url;
  $('#cTok').textContent = c.ea_token;
  if(c.tg_token_set) $('#cfgTgToken').placeholder = 'Saved (' + c.tg_token_hint + ') · blank keeps it';
}

function fillForm(c){
  $('#cfgLot').value = c.lot; $('#cfgTrades').value = c.max_trades; $('#cfgRepeat').checked = !!c.repeat; $('#cfgSL').value = c.sl_money; $('#cfgTP').value = c.tp_money;
  $('#cfgTgOn').checked = !!c.tg_enabled; $('#cfgTgChat').value = c.tg_chat || ''; $('#cfgTgToken').value = '';
}
async function saveConfig(){
  const j = await api('/api/config', {
    lot: $('#cfgLot').value, max_trades: $('#cfgTrades').value, repeat: $('#cfgRepeat').checked, sl_money: $('#cfgSL').value, tp_money: $('#cfgTP').value,
    tg_enabled: $('#cfgTgOn').checked, tg_chat: $('#cfgTgChat').value, tg_token: $('#cfgTgToken').value
  });
  fillForm(j.config);
  await refresh();
}

$('#orb').addEventListener('click', async () => {
  if(!S) return;
  try { await api('/api/control', {action: S.engine_run ? 'pause' : 'play'}); await refresh(); }
  catch(e){ toast(e.message, false); }
});
$('#btnSave').addEventListener('click', async () => {
  try { await saveConfig(); toast('Settings saved'); } catch(e){ toast(e.message, false); }
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
  try { S = await api('/api/state'); fails = 0; $('#banner').style.display = 'none'; render(); }
  catch(e){ if(++fails >= 2) $('#banner').style.display = 'block'; }
}
tab((location.hash || '#home').slice(1));
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
    print(f" Panel : http://127.0.0.1:{PORT}  (local run only)")
    print(f" Token : {config['ea_token']}")
    print("=" * 60)
    app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False)
