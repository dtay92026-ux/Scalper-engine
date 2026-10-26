"""
BigMove Engine
==============
Remote START / STOP for the MT5 instant M1 scalper (ScalpRules_M1_EA).

  * Same panel as before: one big START / STOP button, status, trades and activity log.
  * The engine holds the trade settings and sends them to the EA on every poll:
    lot size, number of trades, keep trading until STOP, stop loss and take profit in USD.
  * The EA reports its status every poll, every executed trade, every close and any
    order problem. Each executed trade goes to the activity log and, if enabled, to Telegram.

Run locally:
    pip install -r requirements.txt
    python app.py            then open http://127.0.0.1:5000

Environment variables (set these on Render):
    ENGINE_TOKEN          secret the EA sends in the X-Engine-Token header. Set it yourself
                          so it does not change when the server restarts.
    ENGINE_UI_PASSWORD    password for this panel. Set it online: START / STOP is live trading.
    ENGINE_LOT            lot size sent to the EA                  (default 0.01)
    ENGINE_TRADES         number of trades kept open               (default 1)
    ENGINE_KEEP_TRADING   keep trading until STOP                  (default on)
    ENGINE_SL_USD         stop loss per trade, USD                 (default 5)
    ENGINE_TP_USD         take profit per trade, USD               (default 6)
    ENGINE_NOTIFY_TRADES  Telegram message for every trade         (default on)
    TG_BOT_TOKEN          optional Telegram bot token
    TG_CHAT_ID            optional Telegram chat ID
    ENGINE_HOST           default 0.0.0.0 on Render, 127.0.0.1 locally
    ENGINE_PORT           default $PORT on Render, otherwise 5000
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
# Paths and settings
# ----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "engine_data"
DATA_DIR.mkdir(exist_ok=True)
EVENTS_FILE = DATA_DIR / "events.jsonl"
TRADES_FILE = DATA_DIR / "trades.jsonl"

HOST = os.environ.get("ENGINE_HOST", "0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1")
PORT = int(os.environ.get("ENGINE_PORT") or os.environ.get("PORT") or "5000")
UI_PASSWORD = os.environ.get("ENGINE_UI_PASSWORD", "")
TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT = os.environ.get("TG_CHAT_ID", "").strip()
EA_TOKEN = os.environ.get("ENGINE_TOKEN", "").strip() or secrets.token_urlsafe(16)


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return int(default)


def _env_bool(name, default):
    v = os.environ.get(name)
    if v is None or not str(v).strip():
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# Trade settings sent to the EA on every poll. Defaults match the earlier engine.
# Change them with the ENGINE_* variables on Render; the EA picks them up on its next poll.
SETTINGS = {
    "lot": max(0.01, _env_float("ENGINE_LOT", 0.01)),
    "trades": min(10, max(1, _env_int("ENGINE_TRADES", 1))),
    "keep_trading": _env_bool("ENGINE_KEEP_TRADING", True),
    "sl_usd": max(0.01, _env_float("ENGINE_SL_USD", 5.0)),
    "tp_usd": max(0.01, _env_float("ENGINE_TP_USD", 6.0)),
    "notify_trades": _env_bool("ENGINE_NOTIFY_TRADES", True),
}

app = Flask(__name__, static_folder=str(BASE_DIR / "static"), static_url_path="/static")
_lock = threading.RLock()

# START / STOP. The engine always starts STOPPED for safety.
# "seq" changes on every START press, so the EA sees a fresh START
# even if it missed a short STOP in between.
state = {"run": False, "seq": 0}
events = deque(maxlen=500)
trades = deque(maxlen=300)
ea = {"last_seen": 0.0, "connected": False, "running": None, "halted": None, "info": {}}


# ----------------------------------------------------------------------------
# Storage helpers
# ----------------------------------------------------------------------------
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


def log(kind, msg, level="info"):
    """kind: system | control | ea | trade | telegram   level: ok | info | warn | error"""
    entry = {"ts": now_str(), "kind": kind, "level": level, "msg": msg}
    with _lock:
        events.append(entry)
        _append_jsonl(EVENTS_FILE, entry)


def settings_text():
    s = SETTINGS
    return (f"{s['lot']:.2f} lots · {s['trades']} trade(s) · keep trading "
            f"{'ON' if s['keep_trading'] else 'OFF'} · SL ${s['sl_usd']:g} · TP ${s['tp_usd']:g} · "
            f"Telegram every trade {'ON' if s['notify_trades'] else 'OFF'}")


_load_jsonl(EVENTS_FILE, events)
_load_jsonl(TRADES_FILE, trades)
_rewrite_jsonl(EVENTS_FILE, events)
_rewrite_jsonl(TRADES_FILE, trades)


# ----------------------------------------------------------------------------
# EA connection tracking
# ----------------------------------------------------------------------------
def ea_timeout():
    try:
        poll = float(ea["info"].get("poll", 3))
    except Exception:
        poll = 3.0
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
            ea["halted"] = None
            log("ea", reason or "EA disconnected · no heartbeat received", "error")

        if ea["connected"]:
            running = bool(ea["info"].get("running"))
            if ea["running"] is None or running != ea["running"]:
                ea["running"] = running
                if running:
                    log("ea", "EA is RUNNING · trading on M1", "ok")
                else:
                    log("ea", "EA is STOPPED · no new trades", "warn")

            halted = bool(ea["info"].get("halted"))
            if ea["halted"] is None or halted != ea["halted"]:
                ea["halted"] = halted
                if halted:
                    log("ea", "EA HALTED · trading stopped by the EA. Press STOP, then START.", "error")


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
def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        return False, "not set"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
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
    except Exception as e:  # never leak the bot token into the log
        return False, str(e).replace(TG_TOKEN, "***")


def notify(text):
    if not (TG_TOKEN and TG_CHAT):
        return

    def work():
        ok, info = tg_send(text)
        if ok:
            log("telegram", "Telegram notification sent", "ok")
        else:
            log("telegram", f"Telegram failed · {info}", "error")

    threading.Thread(target=work, daemon=True).start()


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


def fmt_halt_message(msg):
    return "🚨 <b>EA HALTED</b>\n" + html.escape(msg) + "\nPress STOP then START on the engine to resume."


# ----------------------------------------------------------------------------
# Auth
# ----------------------------------------------------------------------------
def ea_auth():
    tok = request.headers.get("X-Engine-Token", "")
    return secrets.compare_digest(tok.encode("utf-8"), EA_TOKEN.encode("utf-8"))


@app.before_request
def ui_guard():
    if request.path.startswith("/api/ea/") or not UI_PASSWORD:
        return None
    a = request.authorization
    if a and secrets.compare_digest((a.password or "").encode("utf-8"), UI_PASSWORD.encode("utf-8")):
        return None
    return Response("Authentication required", 401,
                    {"WWW-Authenticate": 'Basic realm="BigMove Engine"'})


# ----------------------------------------------------------------------------
# EA-facing API (called by the EA through WebRequest)
# ----------------------------------------------------------------------------
@app.post("/api/ea/poll")
def ea_poll():
    if not ea_auth():
        return jsonify(error="unauthorized"), 401
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        body = {}
    with _lock:
        ea["last_seen"] = time.time()
        ea["info"] = body
        reply = {"run": 1 if state["run"] else 0, "seq": state["seq"], **SETTINGS}
    update_connection()
    return jsonify(reply)


@app.post("/api/ea/event")
def ea_event():
    if not ea_auth():
        return jsonify(error="unauthorized"), 401
    b = request.get_json(force=True, silent=True)
    if not isinstance(b, dict):
        b = {}
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
            trades.append(t)
            _append_jsonl(TRADES_FILE, t)
        log("trade",
            f"TRADE EXECUTED · {t['side']} {t['lots']} {t['symbol']} @ {t['price']} · "
            f"SL {t['sl']} · TP {t['tp']} · {t['time']}",
            "ok")
        if SETTINGS["notify_trades"]:
            notify(fmt_trade_message(t))

    elif typ == "close":
        lvl = b.get("level") if b.get("level") in ("ok", "info", "warn", "error") else "info"
        try:
            pnl = f"{float(b.get('profit')):+.2f}"
        except (TypeError, ValueError):
            pnl = str(b.get("profit", ""))
        log("trade",
            f"TRADE CLOSED · {str(b.get('side', '')).upper()} {b.get('lots', '')} {b.get('symbol', '')} "
            f"@ {b.get('price', '')} · {b.get('reason', '')} · {pnl} {b.get('currency', '')}",
            lvl)

    elif typ == "order_failed":
        log("trade",
            f"Order FAILED · {b.get('side', '')} {b.get('symbol', '')} · "
            f"{b.get('msg', '')} (retcode {b.get('retcode', '?')})",
            "error")

    elif typ == "halt":
        msg = str(b.get("msg", "Trading halted"))[:300]
        log("ea", "EA HALTED · " + msg, "error")
        notify(fmt_halt_message(msg))

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


@app.get("/api/state")
def api_state():
    with _lock:
        return jsonify({
            "server_time": now_str(),
            "engine_run": state["run"],
            "seq": state["seq"],
            "ea_connected": ea["connected"],
            "ea_running": ea["running"] if ea["connected"] else None,
            "ea_halted": ea["halted"] if ea["connected"] else None,
            "ea_age": (time.time() - ea["last_seen"]) if ea["connected"] else None,
            "ea": ea["info"],
            "ea_url": _public_url(),
            "ea_token": EA_TOKEN,
            "settings": SETTINGS,
            "tg_ready": bool(TG_TOKEN and TG_CHAT),
            "events": list(events)[-200:],
            "trades": list(trades)[-60:],
        })


@app.post("/api/control")
def api_control():
    b = request.get_json(silent=True) or {}
    act = str(b.get("action", "")).lower()
    if act not in ("start", "stop"):
        return jsonify(error="action must be start or stop"), 400
    with _lock:
        state["run"] = (act == "start")
        if state["run"]:
            state["seq"] = int(time.time() * 1000)
    if act == "start":
        log("control", "START pressed · EA armed", "ok")
    else:
        log("control", "STOP pressed · no new trades (open trades keep their stops and exits)", "warn")
    return jsonify(ok=True, run=state["run"])


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
<meta name="theme-color" content="#060102">
<title>BigMove Engine</title>
<link rel="icon" href="/static/robot.jpg">
<style>
:root{
  --bg:#060102; --card:#0c0407; --card2:#130709; --line:#2a0f16; --line2:#4d1b28;
  --text:#ffe9ee; --muted:#9b6e78; --red:#ff2d55; --hot:#ff5f7e; --amber:#ff9a3c;
}
*{box-sizing:border-box;margin:0;padding:0;min-width:0}
html,body{max-width:100%;overflow-x:hidden}
body{
  background:
    linear-gradient(rgba(255,45,85,.045) 1px,transparent 1px) 0 0/100% 34px,
    linear-gradient(90deg,rgba(255,45,85,.04) 1px,transparent 1px) 0 0/34px 100%,
    radial-gradient(800px 420px at 50% -8%,rgba(255,45,85,.22),transparent 65%),
    var(--bg);
  color:var(--text);min-height:100vh;-webkit-tap-highlight-color:transparent;
  font-family:Inter,"Segoe UI",system-ui,-apple-system,Roboto,Helvetica,Arial,sans-serif;
}
.wrap{max-width:560px;margin:0 auto;padding:calc(16px + env(safe-area-inset-top)) 16px calc(40px + env(safe-area-inset-bottom))}

/* header */
.top{display:flex;align-items:center;gap:14px}
.logo{width:56px;height:56px;border-radius:16px;object-fit:cover;object-position:50% 26%;flex:none;
  border:1px solid var(--line2);filter:hue-rotate(240deg) saturate(1.35);box-shadow:0 0 24px rgba(255,45,85,.35)}
.brand b{display:block;font-size:22px;letter-spacing:.24em;font-weight:800;white-space:nowrap}
.brand b i{font-style:normal;color:var(--red);text-shadow:0 0 14px rgba(255,45,85,.75)}
.brand span{display:block;margin-top:5px;font-size:10.5px;letter-spacing:.3em;color:var(--muted);white-space:nowrap}
.bar{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-top:18px;padding-bottom:16px;border-bottom:1px solid var(--line)}
.pills{display:flex;gap:8px;flex-wrap:wrap}
.pill{display:inline-flex;align-items:center;gap:8px;padding:8px 13px;border-radius:999px;font-size:11px;
  letter-spacing:.18em;font-weight:600;border:1px solid var(--line2);color:var(--muted);white-space:nowrap}
.pill i{width:7px;height:7px;border-radius:50%;background:var(--muted);flex:none}
.pill.on{color:var(--text);border-color:rgba(255,45,85,.5);background:rgba(255,45,85,.08)}
.pill.on i{background:var(--red);box-shadow:0 0 10px var(--red);animation:blink 2s infinite}
.pill.off{color:var(--muted)}
.clock{font-size:16px;font-weight:600;letter-spacing:.05em;white-space:nowrap;font-variant-numeric:tabular-nums}
.clock small{font-size:10.5px;color:var(--muted);letter-spacing:.14em;margin-left:4px}
@keyframes blink{50%{opacity:.4}}

/* power */
.power{margin-top:16px;border:1px solid var(--line);border-radius:30px;padding:20px 16px 18px;text-align:center;
  background:linear-gradient(180deg,#0a0306,#030102);box-shadow:inset 0 1px 0 rgba(255,45,85,.1)}
.lbl{display:block;text-align:left;font-size:11.5px;letter-spacing:.3em;color:var(--muted);font-weight:600}
.orb{position:relative;display:block;width:min(70vw,268px);aspect-ratio:1/1;margin:14px auto 6px;border:0;background:none;
  cursor:pointer;padding:0}
.orb:active{transform:scale(.985)}
.orb .ring{position:absolute;inset:-10px;border-radius:50%;opacity:.35;transition:opacity .4s;
  background:conic-gradient(from 0deg,transparent 0 30%,rgba(255,45,85,.95) 42%,transparent 55%,transparent 100%);
  -webkit-mask:radial-gradient(circle,transparent 62%,#000 63%);mask:radial-gradient(circle,transparent 62%,#000 63%)}
.orb.on .ring{opacity:1;animation:spin 2.6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.orb .core{position:absolute;inset:14px;border-radius:50%;display:flex;align-items:center;justify-content:center;overflow:hidden;
  background:radial-gradient(circle at 50% 30%,#2a0a12,#040102 72%);border:1px solid var(--line2);
  box-shadow:inset 0 0 40px rgba(255,45,85,.12),0 0 40px rgba(255,45,85,.1);transition:box-shadow .4s}
.orb.on .core{box-shadow:inset 0 0 60px rgba(255,45,85,.35),0 0 80px rgba(255,45,85,.45)}
.orb .core img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;object-position:50% 28%;opacity:.14;
  filter:hue-rotate(240deg) saturate(1.5);-webkit-mask-image:radial-gradient(circle,#000 30%,transparent 70%);
  mask-image:radial-gradient(circle,#000 30%,transparent 70%);transition:opacity .4s}
.orb.on .core img{opacity:.3}
.orb .core svg{position:relative;width:34%;height:34%;color:var(--red);filter:drop-shadow(0 0 10px rgba(255,45,85,.8));transition:.3s}
.orb.on .core svg{color:#ff8aa0}
.state{font-size:30px;font-weight:800;letter-spacing:.34em;margin-left:.34em;color:#8c6870;transition:color .3s}
.state.on{color:var(--red);text-shadow:0 0 18px rgba(255,45,85,.7)}
.state.halt{color:var(--amber);text-shadow:0 0 18px rgba(255,154,60,.6)}
.sub{margin-top:10px;font-size:14px;color:var(--muted);line-height:1.5;padding:0 6px;min-height:42px}
.cmd{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--line2);border-radius:18px;margin:16px 0 14px;text-align:left;background:#080305}
.cmd>div{padding:13px 15px}
.cmd>div+div{border-left:1px solid var(--line)}
.cmd small{display:block;font-size:10px;letter-spacing:.22em;color:var(--muted);margin-bottom:6px;font-weight:600}
.cmd b{font-size:17px;letter-spacing:.1em}
.tap{font-size:11.5px;letter-spacing:.3em;color:var(--muted);font-weight:600}
.hot{color:var(--red);text-shadow:0 0 10px rgba(255,45,85,.5)}
.dim{color:var(--muted)}
.warn{color:var(--amber)}

/* cards */
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px}
.stat{border:1px solid var(--line);border-radius:18px;padding:14px 15px;background:linear-gradient(180deg,var(--card2),var(--card))}
.stat small{display:block;font-size:10px;letter-spacing:.22em;color:var(--muted);font-weight:600;margin-bottom:7px}
.stat b{display:block;font-size:19px;letter-spacing:.02em;overflow-wrap:anywhere;font-variant-numeric:tabular-nums}
.stat em{display:block;font-style:normal;font-size:11.5px;color:var(--muted);margin-top:4px;overflow-wrap:anywhere}
.card{margin-top:14px;border:1px solid var(--line);border-radius:22px;padding:17px;background:linear-gradient(180deg,var(--card2),var(--card));
  box-shadow:inset 0 1px 0 rgba(255,45,85,.1)}
.card h3{font-size:11.5px;letter-spacing:.26em;color:var(--muted);font-weight:700;margin-bottom:14px;
  display:flex;justify-content:space-between;align-items:center;gap:8px}
.card h3::before{content:"";display:inline-block;width:14px;height:2px;background:var(--red);box-shadow:0 0 8px var(--red);margin-right:10px;vertical-align:middle}
.card h3 span{flex:1}

/* trades */
.tr{border:1px solid var(--line);border-radius:15px;padding:12px 13px;margin-bottom:10px;background:#050205}
.tr:last-child{margin-bottom:0}
.tr .h{display:flex;justify-content:space-between;align-items:center;gap:8px}
.tr .h span{font-size:11.5px;color:var(--muted);text-align:right}
.badge{display:inline-block;padding:4px 10px;border-radius:7px;font-size:10.5px;font-weight:800;letter-spacing:.12em;margin-right:8px}
.badge.buy{background:rgba(255,255,255,.06);color:#ffe9ee;border:1px solid rgba(255,233,238,.4)}
.badge.sell{background:rgba(255,45,85,.14);color:var(--red);border:1px solid rgba(255,45,85,.5)}
.tr .p{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px}
.tr .p small{display:block;font-size:9px;letter-spacing:.18em;color:var(--muted);font-weight:600;margin-bottom:3px}
.tr .p b{font-size:13.5px;font-variant-numeric:tabular-nums}
.tr .f{margin-top:9px;font-size:11px;color:var(--muted);overflow-wrap:anywhere}
.empty{color:var(--muted);font-size:13px;text-align:center;padding:14px 0}

/* log */
.log{border:1px solid var(--line);border-radius:14px;background:#040104;max-height:52vh;overflow-y:auto}
.ll{padding:9px 12px;border-bottom:1px solid #15070b;border-left:3px solid transparent}
.ll:last-child{border-bottom:0}
.ll .t{font-size:10.5px;color:var(--muted);letter-spacing:.06em;margin-bottom:3px;display:flex;gap:8px;align-items:center}
.ll .k{font-size:9px;letter-spacing:.14em;padding:1px 7px;border-radius:5px;background:rgba(255,45,85,.12);color:var(--hot);font-weight:700}
.ll .m{font-size:12.5px;line-height:1.5;overflow-wrap:anywhere}
.ll.ok{border-left-color:#ffe9ee}.ll.ok .m{color:#ffe9ee}
.ll.info{border-left-color:var(--muted)}
.ll.warn{border-left-color:var(--amber)}.ll.warn .m{color:#ffc790}.ll.warn .k{background:rgba(255,154,60,.13);color:var(--amber)}
.ll.error{border-left-color:var(--red)}.ll.error .m{color:#ff9db0}

/* buttons and code */
label{display:block;font-size:10.5px;color:var(--muted);font-weight:600;margin:12px 0 6px;letter-spacing:.2em}
code.k{display:block;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:#ffb3c1;background:#040104;
  border:1px solid var(--line2);padding:11px;border-radius:12px;overflow-wrap:anywhere;word-break:break-all}
.btn{padding:9px 14px;border-radius:11px;border:1px solid var(--line2);background:#120609;color:var(--text);
  font-weight:700;font-size:10.5px;letter-spacing:.18em;cursor:pointer;font-family:inherit;margin-top:8px}
.btn:active{transform:scale(.98)}
.note{font-size:12px;color:var(--muted);margin-top:12px;line-height:1.55}
.banner{display:none;margin-top:14px;padding:12px 14px;border-radius:14px;background:rgba(255,45,85,.1);border:1px solid rgba(255,45,85,.45);color:#ffb3c0;font-size:13px;font-weight:600}
.toast{position:fixed;left:16px;right:16px;bottom:calc(20px + env(safe-area-inset-bottom));z-index:50;padding:13px 16px;border-radius:14px;font-weight:600;font-size:14px;
  background:#0d0408;border:1px solid var(--line2);box-shadow:0 14px 40px rgba(0,0,0,.7);transform:translateY(20px);opacity:0;pointer-events:none;transition:.25s;text-align:center;max-width:528px;margin:0 auto}
.toast.show{transform:none;opacity:1}
.toast.ok{border-color:rgba(255,45,85,.55)}.toast.err{border-color:rgba(255,45,85,.7);color:#ffb3c0}
</style>
<link rel="stylesheet" href="/static/cover.css">
</head>
<body>
<div class="wrap">

  <div class="top">
    <img class="logo" src="/static/robot.jpg" alt="" onerror="this.remove()">
    <div class="brand"><b>BIG<i>MOVE</i></b><span>TREND ENGINE · W1 D1 H4</span></div>
  </div>

  <div class="bar">
    <div class="pills">
      <div class="pill off" id="pill"><i></i><span id="pillTxt">EA OFFLINE</span></div>
      <div class="pill off" id="tg"><i></i><span id="tgTxt">TELEGRAM OFF</span></div>
    </div>
    <div class="clock"><span id="clk">--:--:--</span><small id="gmt"></small></div>
  </div>

  <div class="banner" id="banner">Engine unreachable · retrying…</div>

  <div class="power">
    <span class="lbl">ENGINE</span>
    <button class="orb" id="orb" aria-label="Start or stop the engine">
      <span class="ring"></span>
      <span class="core">
        <img src="/static/robot.jpg" alt="" onerror="this.remove()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M12 3v8"/><path d="M6.4 6.4a8 8 0 1 0 11.2 0"/></svg>
      </span>
    </button>
    <div class="state" id="state">STOPPED</div>
    <div class="sub" id="sub">Waiting for the EA…</div>
    <div class="cmd">
      <div><small>COMMANDED</small><b id="cmdV" class="dim">STOP</b></div>
      <div><small>EA STATE</small><b id="eaV" class="dim">OFFLINE</b></div>
    </div>
    <div class="tap" id="tap">TAP TO START</div>
  </div>

  <div class="grid2">
    <div class="stat"><small>POSITIONS</small><b id="stPos">-</b><em id="stPosS">Waiting for EA</em></div>
    <div class="stat"><small>TREND</small><b id="stTrend">-</b><em id="stAdx"></em></div>
    <div class="stat"><small>BALANCE</small><b id="stBal">-</b></div>
    <div class="stat"><small>EQUITY</small><b id="stEq">-</b></div>
  </div>

  <div class="card">
    <h3><span>TRADES EXECUTED</span></h3>
    <div id="trades"></div>
  </div>

  <div class="card">
    <h3><span>ACTIVITY LOG</span> <button class="btn" id="btnClear" style="margin:0">CLEAR</button></h3>
    <div class="log" id="logBox"></div>
  </div>

  <div class="card">
    <h3><span>EA CONNECTION</span></h3>
    <label>ENGINE URL</label>
    <code class="k" id="cUrl">-</code>
    <button class="btn" data-copy="cUrl">COPY URL</button>
    <label>ENGINE TOKEN</label>
    <code class="k" id="cTok">-</code>
    <button class="btn" data-copy="cTok">COPY TOKEN</button>
    <p class="note">Paste the URL and token into the EA inputs. In MT5, add the URL under Tools &gt; Options &gt; Expert Advisors &gt; Allow WebRequest.</p>
  </div>

</div>
<div class="toast" id="toast"></div>

<script>
const $ = s => document.querySelector(s);
let S = null, fails = 0;

function esc(s){ return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function num(v, d){ const n = Number(v); if(v === null || v === undefined || v === '' || isNaN(n)) return '-'; return d === undefined ? String(n) : n.toFixed(d); }
function money(v, cur){ const n = Number(v); return (v === null || v === undefined || isNaN(n)) ? '-' : n.toFixed(2) + ' ' + (cur || ''); }
function bias(b){ return Number(b) > 0 ? '▲' : (Number(b) < 0 ? '▼' : '■'); }

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

document.addEventListener('click', e => {
  const c = e.target.closest('[data-copy]');
  if(c){
    navigator.clipboard.writeText($('#' + c.dataset.copy).textContent)
      .then(() => toast('Copied')).catch(() => toast('Copy failed', false));
  }
});

function tick(){
  const d = new Date(), p = n => String(n).padStart(2, '0');
  $('#clk').textContent = p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  const off = -d.getTimezoneOffset() / 60;
  $('#gmt').textContent = 'GMT' + (off >= 0 ? '+' : '') + off;
}
setInterval(tick, 1000); tick();

function render(){
  const ea = S.ea || {}, conn = !!S.ea_connected, run = !!S.engine_run, halted = !!S.ea_halted;

  const pill = $('#pill');
  pill.className = 'pill ' + (conn ? 'on' : 'off');
  $('#pillTxt').textContent = conn ? 'EA ONLINE · ' + Math.round(S.ea_age || 0) + 's' : 'EA OFFLINE';
  $('#tg').className = 'pill ' + (S.tg_ready ? 'on' : 'off');
  $('#tgTxt').textContent = S.tg_ready ? 'TELEGRAM ON' : 'TELEGRAM OFF';

  $('#orb').classList.toggle('on', run);
  const st = $('#state');
  st.textContent = run ? (halted ? 'HALTED' : 'RUNNING') : 'STOPPED';
  st.className = 'state ' + (run ? (halted ? 'halt' : 'on') : '');
  $('#sub').textContent = !conn ? 'EA offline · start it on the VM' : (ea.status || 'Connected');

  $('#cmdV').textContent = run ? 'START' : 'STOP';
  $('#cmdV').className = run ? 'hot' : 'dim';
  const ev = $('#eaV');
  ev.textContent = !conn ? 'OFFLINE' : (halted ? 'HALTED' : (ea.active ? 'ACTIVE' : 'IDLE'));
  ev.className = (conn && ea.active && !halted) ? 'hot' : (halted ? 'warn' : 'dim');
  $('#tap').textContent = run ? 'TAP TO STOP' : 'TAP TO START';

  const n = (Number(ea.open_buy) || 0) + (Number(ea.open_sell) || 0);
  $('#stPos').textContent = conn ? n + ' / ' + (ea.max_positions || 5) : '-';
  $('#stPosS').textContent = conn ? (ea.open_buy || 0) + ' buy · ' + (ea.open_sell || 0) + ' sell' : 'Waiting for EA';
  $('#stTrend').textContent = conn ? 'W1 ' + bias(ea.bias_w) + '  D1 ' + bias(ea.bias_d) : '-';
  $('#stAdx').textContent = conn ? 'ADX ' + num(ea.adx, 1) + ' · ATR ' + num(ea.atr, 2) : '';
  $('#stBal').textContent = conn ? money(ea.balance, ea.currency) : '-';
  $('#stEq').textContent = conn ? money(ea.equity, ea.currency) : '-';

  const tr = S.trades.slice().reverse().slice(0, 30);
  $('#trades').innerHTML = tr.length ? tr.map(t => `
    <div class="tr">
      <div class="h"><div><span class="badge ${t.side === 'BUY' ? 'buy' : 'sell'}">${esc(t.side)}</span><b>${esc(t.symbol)}</b></div><span>${esc(t.time || t.ts)}</span></div>
      <div class="p">
        <div><small>ENTRY</small><b>${num(t.price)}</b></div>
        <div><small>SL</small><b class="hot">${num(t.sl)}</b></div>
        <div><small>TP</small><b>${num(t.tp)}</b></div>
      </div>
      <div class="f">${num(t.lots)} lots${t.pattern ? ' · ' + esc(t.pattern) : ''}${t.ticket && t.ticket !== '0' ? ' · #' + esc(t.ticket) : ''}</div>
    </div>`).join('') : '<div class="empty">No trades executed yet.</div>';

  const evs = S.events.slice().reverse();
  $('#logBox').innerHTML = evs.length ? evs.map(e => `
    <div class="ll ${esc(e.level)}"><div class="t"><span class="k">${esc((e.kind || '').toUpperCase())}</span>${esc((e.ts || '').slice(5))}</div>
    <div class="m">${esc(e.msg)}</div></div>`).join('') : '<div class="empty">No activity yet.</div>';

  $('#cUrl').textContent = S.ea_url || '-';
  $('#cTok').textContent = S.ea_token || '-';
}

$('#orb').addEventListener('click', async () => {
  if(!S) return;
  try { await api('/api/control', {action: S.engine_run ? 'stop' : 'start'}); await refresh(); }
  catch(e){ toast(e.message, false); }
});
$('#btnClear').addEventListener('click', async () => {
  if(!confirm('Clear the activity log? Trade history is kept.')) return;
  try { await api('/api/log/clear', {}); await refresh(); } catch(e){ toast(e.message, false); }
});

async function refresh(){
  try {
    S = await api('/api/state'); fails = 0; $('#banner').style.display = 'none'; render();
  } catch(e){
    if(++fails >= 2) $('#banner').style.display = 'block';
  }
}
refresh(); setInterval(refresh, 2000);
</script>
</body>
</html>
"""

start_background()
log("system", "Engine started · STOPPED. Press START to arm the EA.", "info")
log("system", "Trade settings: " + settings_text(), "info")

if __name__ == "__main__":
    print("=" * 60)
    print(" BigMove Engine")
    print(f" Panel : http://127.0.0.1:{PORT}  (local run)")
    print(f" Token : {EA_TOKEN}")
    print("=" * 60)
    app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False)
