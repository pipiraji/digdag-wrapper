#!/usr/bin/env python3
"""
digdag_dashboard.py
===================
Digdag server monitoring dashboard — FastAPI + WebSocket

Run:
    pip install fastapi uvicorn
    python digdag_dashboard.py

Access:
    http://<host>:8765

Features:
    - Shows all digdag servers across all users on the machine
    - WebSocket push: auto-refresh every 10 seconds
    - One session per OS user (duplicate tabs are allowed, duplicate users are not)
    - Kill server with confirmation modal
    - Browse: opens digdag UI URL in a new browser tab

Environment:
    DIGDAG_SH       path to digdag wrapper  (default: /user/qarepo/usr/local/bin/digdag.sh)
    DASHBOARD_PORT  web server port         (default: 8765)
    DASHBOARD_HOST  bind address            (default: "0.0.0.0")
"""

import asyncio
import getpass
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# ── settings ─────────────────────────────────────────────────
DIGDAG_SH      = os.environ.get(
    "DIGDAG_SH",
    "/user/qarepo/usr/local/bin/digdag.sh",
)
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8765"))
DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
SCAN_INTERVAL  = 10   # seconds
DEBUG          = os.environ.get("DIGDAG_DEBUG", "0") == "1"

# ── tmp directory (mirrors digdag.sh convention) ──────────────
# /tmp/digdag_<user>/          ← base dir (shared with digdag.sh)
# /tmp/digdag_<user>/dashboard/ ← dashboard exclusive subdir
_USER           = getpass.getuser()
HOSTNAME        = subprocess.check_output(["hostname"], text=True).strip()
DIGDAG_TMP_DIR  = f"/tmp/digdag_{_USER}"
DASHBOARD_DIR   = f"{DIGDAG_TMP_DIR}/dashboard"
DASHBOARD_PIDFILE = f"{DASHBOARD_DIR}/dashboard.pid"   # "<PID> <PORT>"
DASHBOARD_LOG   = f"{DASHBOARD_DIR}/dashboard.log"

# Create dirs at import time (chmod 700 — owner only)
import stat as _stat
for _d in (DIGDAG_TMP_DIR, DASHBOARD_DIR):
    os.makedirs(_d, exist_ok=True)
    os.chmod(_d, _stat.S_IRWXU)

# ─────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════
#  Data
# ══════════════════════════════════════════════════════════════
@dataclass
class ServerInfo:
    no:      str
    pid:     str
    port:    str
    url:     str
    user:    str                        # OS user who owns the process
    running: list = field(default_factory=list)
    owner:   str  = ""                  # same as user; explicit for frontend


# ══════════════════════════════════════════════════════════════
#  Server discovery — parse "digdag list_server" per user
#  OR scan /proc directly for all users
# ══════════════════════════════════════════════════════════════
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_ROW  = re.compile(
    r"^\|\s*(?P<no>[^|]*)\|\s*(?P<pid>[^|]*)\|\s*(?P<port>[^|]*)"
    r"\|\s*(?P<url>[^|]*)\|\s*(?P<run>[^|]*)\|"
)


def _parse_list_server(raw: str, user: str) -> list[ServerInfo]:
    servers: list[ServerInfo] = []
    current: Optional[ServerInfo] = None

    for line in raw.splitlines():
        line = _ANSI.sub("", line).strip()
        m = _ROW.match(line)
        if not m:
            continue

        no_v   = m.group("no").strip()
        pid_v  = m.group("pid").strip()
        port_v = m.group("port").strip()
        url_v  = m.group("url").strip()   # keep URL as-is from list_server
        run_v  = m.group("run").strip()

        if no_v.lower() in ("no.", "no") or pid_v.lower() == "pid":
            continue

        if no_v:
            current = ServerInfo(no=no_v, pid=pid_v, port=port_v,
                                 url=url_v, user=user, running=[], owner=user)
            servers.append(current)
            if run_v and run_v not in ("(none)", "(없음)"):
                current.running.append(run_v)
        elif current and run_v and run_v != "(none)":
            current.running.append(run_v)

    import sys
    if DEBUG:
        print(f"[DEBUG] _parse_list_server: {len(servers)} server(s) parsed", file=sys.stderr)
        for s in servers:
            print(f"  → no={s.no} pid={s.pid} port={s.port} url={s.url} running={s.running}", file=sys.stderr)
    return servers


def _run_list_server_as(user: str) -> list[ServerInfo]:
    """
    Run 'digdag list_server' as the given user.
    If the current process IS that user, run directly.
    Otherwise use 'su -c'.
    """
    current_user = getpass.getuser()

    if user == current_user:
        cmd = [DIGDAG_SH, "list_server"]
    else:
        cmd = ["su", "-", user, "-c", f"{DIGDAG_SH} list_server"]

    try:
        import sys
        if DEBUG:
            print(f"\n[DEBUG] cmd = {cmd}", file=sys.stderr)

        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, timeout=20)

        if DEBUG:
            print(f"[DEBUG] returncode = {r.returncode}", file=sys.stderr)
            print(f"[DEBUG] stdout ({len(r.stdout)} bytes):\n{r.stdout[:800]}", file=sys.stderr)
            print(f"[DEBUG] stderr ({len(r.stderr)} bytes):\n{r.stderr[:800]}", file=sys.stderr)

        raw = r.stderr or r.stdout

        no_server_patterns = [
            "No running Digdag server found",
            "기동 중인 Digdag 서버가 없습니다",
        ]
        if not raw.strip():
            if DEBUG:
                print("[DEBUG] raw output is EMPTY → returning []", file=sys.stderr)
            return []
        if any(p in raw for p in no_server_patterns):
            if DEBUG:
                print(f"[DEBUG] matched no-server pattern → returning []", file=sys.stderr)
            return []

        servers = _parse_list_server(raw, user)
        if DEBUG:
            print(f"[DEBUG] parsed {len(servers)} server(s): {[s.pid for s in servers]}", file=sys.stderr)
        return servers

    except Exception as e:
        import sys
        print(f"[ERROR] list_server failed for user={user!r}: {e}", file=sys.stderr)
        return []


def fetch_all_servers(username: str) -> list[ServerInfo]:
    """
    Collect digdag servers from ALL users on the machine.
    Each ServerInfo carries an 'owner' field.
    'username' is the connected browser user (used to mark own servers).
    """
    all_users = _find_digdag_users()
    result: list[ServerInfo] = []
    for user in all_users:
        result.extend(_run_list_server_as(user))
    return result


def _find_digdag_users() -> list[str]:
    """Find all OS users currently running a digdag server process."""
    users: set[str] = set()
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "user,args", "--no-headers"],
            text=True, stderr=subprocess.DEVNULL,
        )
        jar = os.path.basename(
            "/user/qarepo/usr/local/digdag-0.10.5.1.jar"
        )
        for line in out.splitlines():
            parts = line.split(None, 1)
            if len(parts) == 2:
                user, args = parts
                if ("server" in args
                        and not any(k in args for k in
                                    ("run","push","start","retry","kill","check"))):
                    users.add(user)
    except Exception:
        pass
    # Always include current user even if no server running yet
    users.add(getpass.getuser())
    return sorted(users)


# ══════════════════════════════════════════════════════════════
#  Kill
# ══════════════════════════════════════════════════════════════
def kill_server(pid: str) -> dict:
    try:
        subprocess.run(["kill", "--", f"-{pid}"],
                       timeout=10, check=False)
        return {"ok": True, "message": f"SIGTERM sent to PID {pid}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}


# ══════════════════════════════════════════════════════════════
#  Session manager — one active WS per OS user
# ══════════════════════════════════════════════════════════════
class SessionManager:
    def __init__(self):
        # user -> WebSocket
        self._sessions: dict[str, WebSocket] = {}

    async def connect(self, user: str, ws: WebSocket) -> bool:
        """Returns True if connection accepted, False if duplicate."""
        await ws.accept()
        if user in self._sessions:
            old = self._sessions[user]
            try:
                await old.send_json({"type": "kicked",
                                     "message": "Another session opened."})
                await old.close()
            except Exception:
                pass
        self._sessions[user] = ws
        return True

    def disconnect(self, user: str, ws: WebSocket):
        if self._sessions.get(user) is ws:
            del self._sessions[user]

    async def broadcast(self, data: dict):
        dead = []
        for user, ws in list(self._sessions.items()):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(user)
        for u in dead:
            self._sessions.pop(u, None)

    @property
    def connected_users(self) -> list[str]:
        return list(self._sessions.keys())


sessions = SessionManager()


# ══════════════════════════════════════════════════════════════
#  Background scanner
# ══════════════════════════════════════════════════════════════
async def scanner_loop():
    while True:
        # Fetch all servers once (all users on machine)
        all_servers = await asyncio.get_event_loop().run_in_executor(
            None, fetch_all_servers, ""
        )
        payload_servers = [asdict(s) for s in all_servers]
        ts = datetime.now().strftime("%H:%M:%S")

        # Send same server list to every connected session
        # Frontend uses 'owner' field to decide Kill button visibility
        for uname, ws in list(sessions._sessions.items()):
            try:
                await ws.send_json({
                    "type":      "update",
                    "timestamp": ts,
                    "servers":   payload_servers,
                    "viewer":    uname,   # who is watching
                })
            except Exception:
                pass
        await asyncio.sleep(SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════
#  FastAPI app
# ══════════════════════════════════════════════════════════════
app = FastAPI()


@app.on_event("startup")
async def startup():
    asyncio.create_task(scanner_loop())
    asyncio.create_task(pidfile_touch_loop())


async def pidfile_touch_loop():
    """
    Keep pidfile alive by touching it every 5 seconds.
    Mirrors digdag.sh watcher pattern:
      while server alive: sleep 5; touch lock/info files
    Pidfile path: /tmp/digdag_dashboard_<user>.pid
    """
    pidfile = DASHBOARD_PIDFILE
    while True:
        try:
            if os.path.exists(pidfile):
                os.utime(pidfile, None)   # touch
        except OSError:
            pass
        await asyncio.sleep(5)


@app.get("/whoami")
async def whoami():
    """Return the OS username running this server process."""
    from fastapi.responses import JSONResponse
    return JSONResponse({"user": getpass.getuser(), "host": HOSTNAME})


@app.get("/debug/{username}")
async def debug_endpoint(username: str):
    """
    Run list_server for the given user and return raw result.
    Usage: http://hostname:8765/debug/<your_username>
    """
    from fastapi.responses import PlainTextResponse
    import sys

    lines = []
    lines.append(f"=== Digdag Dashboard Debug ===")
    lines.append(f"OS user (server process) : {getpass.getuser()}")
    lines.append(f"Requested user           : {username}")
    lines.append(f"DIGDAG_SH                : {DIGDAG_SH}")
    lines.append(f"DIGDAG_SH exists         : {os.path.isfile(DIGDAG_SH)}")
    lines.append(f"DIGDAG_TMP_DIR           : {DIGDAG_TMP_DIR}")
    lines.append(f"DASHBOARD_DIR            : {DASHBOARD_DIR}")
    lines.append(f"DASHBOARD_PIDFILE        : {DASHBOARD_PIDFILE}")
    lines.append("")

    # Run list_server with forced debug
    import subprocess, sys
    current_user = getpass.getuser()
    if username == current_user:
        cmd = [DIGDAG_SH, "list_server"]
    else:
        cmd = ["su", "-", username, "-c", f"{DIGDAG_SH} list_server"]

    lines.append(f"CMD: {' '.join(cmd)}")
    lines.append("")

    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, timeout=20)
        lines.append(f"returncode : {r.returncode}")
        lines.append(f"--- stdout ({len(r.stdout)} bytes) ---")
        lines.append(r.stdout or "(empty)")
        lines.append(f"--- stderr ({len(r.stderr)} bytes) ---")
        lines.append(r.stderr or "(empty)")
        lines.append("")

        raw = r.stderr or r.stdout
        servers = _parse_list_server(raw, username)
        lines.append(f"=== Parsed result: {len(servers)} server(s) ===")
        for s in servers:
            lines.append(f"  no={s.no} pid={s.pid} port={s.port} url={s.url}")
            lines.append(f"  running={s.running}")
    except Exception as e:
        lines.append(f"EXCEPTION: {e}")

    return PlainTextResponse("\n".join(lines))


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # Username is always the OS user running this dashboard process
    username = getpass.getuser()
    await sessions.connect(username, ws)
    # Send immediate snapshot
    servers = await asyncio.get_event_loop().run_in_executor(
        None, fetch_all_servers, ""
    )
    await ws.send_json({
        "type":      "update",
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "servers":   [asdict(s) for s in servers],
        "viewer":    username,
    })
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("action") == "kill":
                pid   = msg.get("pid", "")
                owner = msg.get("owner", "")
                # Security: only allow kill if requester owns the server
                # Empty owner field = allow (backward compat / own server)
                if owner and owner != username:
                    await ws.send_json({
                        "type":    "kill_result",
                        "ok":      False,
                        "message": f"Permission denied: server owned by '{owner}', connected as '{username}'",
                    })
                else:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, kill_server, pid
                    )
                    await ws.send_json({"type": "kill_result", **result})
                # Trigger immediate rescan
                await asyncio.sleep(2)
                servers = await asyncio.get_event_loop().run_in_executor(
                    None, fetch_all_servers, username
                )
                await ws.send_json({
                    "type":      "update",
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "servers":   [asdict(s) for s in servers],
                })
    except WebSocketDisconnect:
        sessions.disconnect(username, ws)


# ══════════════════════════════════════════════════════════════
#  HTML — single-file dashboard
# ══════════════════════════════════════════════════════════════
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Digdag Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #0d1117;
    --surface: #161b22;
    --border:  #30363d;
    --text:    #e6edf3;
    --muted:   #8b949e;
    --green:   #3fb950;
    --red:     #f85149;
    --amber:   #e3b341;
    --blue:    #58a6ff;
    --accent:  #238636;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }
  .mono { font-family: 'JetBrains Mono', monospace; }

  /* pulse dot */
  @keyframes pulse {
    0%,100% { opacity: 1; }
    50%      { opacity: .3; }
  }
  .pulse { animation: pulse 2s ease-in-out infinite; }

  /* row fade-in */
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
  }
  .fade-in { animation: fadeIn .25s ease forwards; }

  /* countdown ring */
  .ring-track { stroke: var(--border); }
  .ring-fill  {
    stroke: var(--green);
    stroke-linecap: round;
    transition: stroke-dashoffset 1s linear;
  }

  /* modal backdrop */
  .modal-bg {
    position: fixed; inset: 0;
    background: rgba(0,0,0,.7);
    display: flex; align-items: center; justify-content: center;
    z-index: 50;
  }
  .modal {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 28px 32px;
    width: 420px;
    max-width: 95vw;
  }
  .btn {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 5px 14px; border-radius: 6px; font-size: 13px;
    font-weight: 500; cursor: pointer; border: 1px solid transparent;
    transition: opacity .15s;
  }
  .btn:hover { opacity: .8; }
  .btn-red    { background: #da3633; color: #fff; }
  .btn-gray   { background: transparent; color: var(--muted);
                border-color: var(--border); }
  .btn-green  { background: var(--accent); color: #fff; }

  td, th { padding: 10px 14px; }
  thead th {
    font-size: 11px; letter-spacing: .08em; text-transform: uppercase;
    color: var(--muted); border-bottom: 1px solid var(--border);
    font-family: 'JetBrains Mono', monospace; font-weight: 500;
  }
  tbody tr {
    border-bottom: 1px solid var(--border);
    transition: background .1s;
  }
  tbody tr:hover { background: rgba(255,255,255,.03); }
  tbody tr:last-child { border-bottom: none; }
</style>
</head>
<body>

<!-- ── Header ─────────────────────────────────────────── -->
<header style="border-bottom:1px solid var(--border); padding:16px 32px;
               display:flex; align-items:center; justify-content:space-between;">
  <div style="display:flex; align-items:center; gap:12px;">
    <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
      <rect width="28" height="28" rx="7" fill="#238636"/>
      <path d="M7 14h14M14 7v14" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/>
    </svg>
    <span style="font-size:18px; font-weight:600; font-family:'JetBrains Mono',monospace;">
      digdag<span style="color:var(--green);">.</span>dashboard
    </span>
  </div>

  <div style="display:flex; align-items:center; gap:20px;">
    <!-- countdown ring -->
    <div style="display:flex; align-items:center; gap:8px;">
      <svg width="32" height="32" style="transform:rotate(-90deg)">
        <circle class="ring-track" cx="16" cy="16" r="12"
                stroke-width="2.5" fill="none"/>
        <circle id="ring" class="ring-fill" cx="16" cy="16" r="12"
                stroke-width="2.5" fill="none"
                stroke-dasharray="75.4" stroke-dashoffset="0"/>
      </svg>
      <span id="countdown" class="mono" style="font-size:13px; color:var(--muted);">
        10s
      </span>
    </div>

    <!-- live dot + last updated -->
    <div style="display:flex; align-items:center; gap:6px;">
      <span id="live-dot" class="pulse" style="
        width:8px; height:8px; border-radius:50%;
        background:var(--green); display:inline-block;">
      </span>
      <span id="last-update" style="font-size:12px; color:var(--muted);">
        connecting…
      </span>
    </div>

    <!-- user badge -->
    <span id="user-badge" style="
      font-size:12px; font-family:'JetBrains Mono',monospace;
      background:rgba(56,139,253,.15); color:var(--blue);
      padding:3px 10px; border-radius:20px; border:1px solid rgba(56,139,253,.3);">
    </span>
  </div>
</header>

<!-- ── Summary cards ──────────────────────────────────── -->
<div style="padding:24px 32px 0; display:grid;
            grid-template-columns:repeat(auto-fit, minmax(180px,1fr)); gap:14px;">

  <div style="background:var(--surface); border:1px solid var(--border);
              border-radius:10px; padding:16px 20px;">
    <p style="font-size:11px; color:var(--muted); text-transform:uppercase;
              letter-spacing:.08em; margin-bottom:6px;">Total servers</p>
    <p id="stat-total" class="mono" style="font-size:28px; font-weight:700;">—</p>
  </div>

  <div style="background:var(--surface); border:1px solid var(--border);
              border-radius:10px; padding:16px 20px;">
    <p style="font-size:11px; color:var(--muted); text-transform:uppercase;
              letter-spacing:.08em; margin-bottom:6px;">Running workflows</p>
    <p id="stat-running" class="mono"
       style="font-size:28px; font-weight:700; color:var(--green);">—</p>
  </div>

  <div style="background:var(--surface); border:1px solid var(--border);
              border-radius:10px; padding:16px 20px;">
    <p style="font-size:11px; color:var(--muted); text-transform:uppercase;
              letter-spacing:.08em; margin-bottom:6px;">Idle servers</p>
    <p id="stat-idle" class="mono"
       style="font-size:28px; font-weight:700; color:var(--muted);">—</p>
  </div>

  <div style="background:var(--surface); border:1px solid var(--border);
              border-radius:10px; padding:16px 20px;">
    <p style="font-size:11px; color:var(--muted); text-transform:uppercase;
              letter-spacing:.08em; margin-bottom:6px;">Signed in as</p>
    <p id="stat-users" class="mono"
       style="font-size:18px; font-weight:700; color:var(--blue);
              overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">—</p>
  </div>

</div>

<!-- ── Table ──────────────────────────────────────────── -->
<div style="padding:24px 32px;">
  <div style="background:var(--surface); border:1px solid var(--border);
              border-radius:10px; overflow:hidden;">

    <div style="padding:14px 20px; border-bottom:1px solid var(--border);
                display:flex; align-items:center; justify-content:space-between;">
      <span style="font-size:14px; font-weight:600;">Server List</span>
      <span id="kill-all-btn" onclick="confirmKillAll()"
            class="btn btn-red" style="display:none;">
        ☠ Kill All
      </span>
    </div>

    <div style="overflow-x:auto;">
      <table style="width:100%; border-collapse:collapse;">
        <thead>
          <tr>
            <th style="text-align:left;">No.</th>
            <th style="text-align:left;">PID</th>
            <th style="text-align:left;">Port</th>
            <th style="text-align:left;">URL</th>
            <th style="text-align:left;">Running (project/workflow)</th>
            <th style="text-align:center;">Actions</th>
          </tr>
        </thead>
        <tbody id="server-tbody">
          <tr>
            <td colspan="6" style="text-align:center; color:var(--muted);
                                   padding:40px; font-size:14px;">
              Connecting to server…
            </td>
          </tr>
        </tbody>
      </table>
    </div>

  </div>
</div>

<!-- ── Toast ──────────────────────────────────────────── -->
<div id="toast" style="
  position:fixed; bottom:28px; right:28px;
  background:var(--surface); border:1px solid var(--border);
  border-radius:8px; padding:12px 18px;
  font-size:13px; max-width:340px;
  transform:translateY(80px); opacity:0;
  transition:all .3s ease; pointer-events:none; z-index:100;">
</div>

<!-- ── Kill confirm modal ─────────────────────────────── -->
<div id="modal" class="modal-bg" style="display:none;"
     onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <h3 style="font-size:16px; font-weight:600; margin-bottom:8px;">
      Confirm Kill
    </h3>
    <p id="modal-body" style="font-size:13px; color:var(--muted);
                               line-height:1.6; margin-bottom:20px;"></p>
    <div style="display:flex; gap:10px; justify-content:flex-end;">
      <button class="btn btn-gray" onclick="closeModal()">Cancel</button>
      <button id="modal-confirm" class="btn btn-red">Kill Server</button>
    </div>
  </div>
</div>

<script>
// ── State ──────────────────────────────────────────────
// username is fixed to the OS user running this dashboard (no URL param)
let USERNAME = "";
let ws;
let servers  = [];
let viewer   = "";
let remainSec = 10;
let countdownInterval;
let pendingKillPids  = [];
let pendingKillOwner = "";



// ── WebSocket ──────────────────────────────────────────
function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById("live-dot").style.background = "var(--green)";
    startCountdown();
  };

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === "update") {
      servers = msg.servers;
      if (msg.viewer) viewer = msg.viewer;
      renderTable(servers);
      updateStats(servers);
      document.getElementById("last-update").textContent =
        "Updated " + msg.timestamp;
      resetCountdown();
    }
    if (msg.type === "kill_result") {
      toast(msg.ok ? "✓ " + msg.message : "✗ " + msg.message,
            msg.ok ? "var(--green)" : "var(--red)");
    }
    if (msg.type === "kicked") {
      toast("Session replaced by new connection.", "var(--amber)");
      ws.close();
    }
  };

  ws.onclose = () => {
    document.getElementById("live-dot").style.background = "var(--red)";
    document.getElementById("live-dot").classList.remove("pulse");
    clearInterval(countdownInterval);
    // Reconnect after 3s
    setTimeout(connect, 3000);
  };
}

// ── Render table ───────────────────────────────────────
function renderTable(list) {
  const tbody = document.getElementById("server-tbody");

  if (!list.length) {
    tbody.innerHTML = `<tr><td colspan="7"
      style="text-align:center;color:var(--muted);padding:40px;font-size:14px;">
      No running Digdag servers found.</td></tr>`;
    document.getElementById("kill-all-btn").style.display = "none";
    return;
  }

  // Show Kill All only if viewer owns at least one server
  const myServers = list.filter(s => s.owner === viewer);
  document.getElementById("kill-all-btn").style.display =
    myServers.length ? "inline-flex" : "none";

  // Group by owner for visual separation
  const ownerOrder = [...new Set(list.map(s => s.owner))].sort(o =>
    o === viewer ? -1 : 1
  );

  let rows = "";
  ownerOrder.forEach(owner => {
    const group = list.filter(s => s.owner === owner);
    const isMe  = owner === viewer;   // for section header

    // Owner section header
    rows += `
    <tr>
      <td colspan="7" style="
        background:${isMe ? "rgba(63,185,80,.06)" : "rgba(255,255,255,.02)"};
        padding:6px 14px; border-bottom:1px solid var(--border);">
        <span style="font-size:11px; font-family:'JetBrains Mono',monospace;
          color:${isMe ? "var(--green)" : "var(--muted)"};">
          ${isMe ? "▶ " : "  "}${owner}${isMe ? "  (you)" : "  (read-only)"}
        </span>
      </td>
    </tr>`;

    group.forEach((s, i) => {
      const hasRunning = s.running.length > 0;
      const runHtml = hasRunning
        ? s.running.map(r =>
            `<span style="display:inline-block;background:rgba(63,185,80,.12);
             color:var(--green);border:1px solid rgba(63,185,80,.25);
             border-radius:4px;padding:1px 8px;font-size:11px;
             font-family:'JetBrains Mono',monospace;margin:2px 2px 2px 0;">
             ${r}</span>`
          ).join("")
        : `<span style="color:var(--muted);font-size:12px;">(idle)</span>`;

      // Kill button: only active for own servers
      // Fallback: if owner field is empty, treat as own server
      const serverOwner = s.owner || s.user || viewer;
      const isMine = serverOwner === viewer;
      const killBtn = isMine
        ? `<button class="btn btn-red" style="font-size:11px;padding:3px 10px;"
             onclick="confirmKill('${s.pid}','${s.port}','${serverOwner}')">
             🗡 Kill
           </button>`
        : `<button class="btn btn-gray" style="font-size:11px;padding:3px 10px;
             opacity:.35;cursor:not-allowed;" disabled title="Owned by ${serverOwner}">
             🗡 Kill
           </button>`;

      rows += `
      <tr class="fade-in" style="animation-delay:${i * 40}ms;
        ${isMe ? "" : "opacity:.75;"}">
        <td class="mono" style="color:var(--muted);font-size:12px;">${s.no}</td>
        <td class="mono" style="font-weight:700;font-size:13px;">${s.pid}</td>
        <td>
          <span style="background:rgba(227,179,65,.1);color:var(--amber);
            border:1px solid rgba(227,179,65,.2);border-radius:4px;
            padding:2px 8px;font-size:12px;font-family:'JetBrains Mono',monospace;">
            ${s.port}
          </span>
        </td>
        <td class="mono" style="font-size:11px;color:var(--muted);">
          <a href="${s.url}" target="_blank"
             style="color:var(--blue);text-decoration:none;"
             onmouseover="this.style.textDecoration='underline'"
             onmouseout="this.style.textDecoration='none'">
            ${s.url}
          </a>
        </td>
        <td>${runHtml}</td>
        <td style="text-align:center;white-space:nowrap;">
          <button class="btn btn-gray" style="font-size:11px;padding:3px 10px;margin-right:4px;"
                  onclick="window.open('${s.url}','_blank')">
            🌐 Open
          </button>
          ${killBtn}
        </td>
      </tr>`;
    });
  });

  tbody.innerHTML = rows;
}

// ── Stats ──────────────────────────────────────────────
function updateStats(list) {
  const total   = list.length;
  const running = list.filter(s => s.running.length > 0).length;
  const idle    = total - running;
  document.getElementById("stat-total").textContent   = total;
  document.getElementById("stat-running").textContent = running;
  document.getElementById("stat-idle").textContent    = idle;
  document.getElementById("stat-users").textContent   = USERNAME;
}

// ── Kill confirm ───────────────────────────────────────
function confirmKill(pid, port, owner) {
  pendingKillPids  = [pid];
  pendingKillOwner = owner;
  document.getElementById("modal-body").innerHTML =
    `Terminate this server?<br>
     <span class="mono" style="font-size:12px; color:var(--muted);">
       PID: ${pid} &nbsp; PORT: ${port} &nbsp; owner: ${owner}
     </span>`;
  document.getElementById("modal-confirm").textContent = "Kill Server";
  document.getElementById("modal-confirm").onclick = executeKill;
  document.getElementById("modal").style.display = "flex";
}

function confirmKillAll() {
  const myServers  = servers.filter(s => s.owner === viewer);
  pendingKillPids  = myServers.map(s => s.pid);
  pendingKillOwner = viewer;
  document.getElementById("modal-body").innerHTML =
    `Terminate <strong>your ${myServers.length} server(s)</strong>?<br>
     <span style="font-size:12px; color:var(--muted);">
       ${myServers.map(s => "PORT:" + s.port).join(" &nbsp; ")}
     </span>`;
  document.getElementById("modal-confirm").textContent = "Kill All";
  document.getElementById("modal-confirm").onclick = executeKillAll;
  document.getElementById("modal").style.display = "flex";
}

function executeKill() {
  closeModal();
  ws.send(JSON.stringify({
    action: "kill",
    pid:    pendingKillPids[0],
    owner:  pendingKillOwner,
  }));
}

function executeKillAll() {
  closeModal();
  pendingKillPids.forEach(pid =>
    ws.send(JSON.stringify({ action: "kill", pid, owner: pendingKillOwner }))
  );
}

function closeModal() {
  document.getElementById("modal").style.display = "none";
}

// ── Countdown ring ─────────────────────────────────────
const CIRCUMFERENCE = 75.4;  // 2π×12

function startCountdown() {
  remainSec = 10;
  clearInterval(countdownInterval);
  countdownInterval = setInterval(() => {
    remainSec = Math.max(0, remainSec - 1);
    const offset = CIRCUMFERENCE * (1 - remainSec / 10);
    document.getElementById("ring").style.strokeDashoffset = offset;
    document.getElementById("countdown").textContent = remainSec + "s";
  }, 1000);
}

function resetCountdown() {
  remainSec = 10;
  document.getElementById("ring").style.strokeDashoffset = 0;
  document.getElementById("countdown").textContent = "10s";
}

// ── Toast ──────────────────────────────────────────────
function toast(msg, color) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.style.borderLeftColor = color;
  el.style.borderLeftWidth = "3px";
  el.style.transform = "translateY(0)";
  el.style.opacity   = "1";
  setTimeout(() => {
    el.style.transform = "translateY(80px)";
    el.style.opacity   = "0";
  }, 3000);
}

// ── Keyboard ───────────────────────────────────────────
document.addEventListener("keydown", e => {
  if (e.key === "Escape") closeModal();
});

// ── Boot ───────────────────────────────────────────────
// Always fetch OS username from server, then connect
fetch("/whoami")
  .then(r => r.json())
  .then(data => {
    USERNAME = data.user;
    viewer   = data.user;
    document.getElementById("user-badge").textContent =
      data.user + "@" + (data.host || location.hostname);
    connect();
  })
  .catch(() => {
    USERNAME = "unknown";
    viewer   = "unknown";
    document.getElementById("user-badge").textContent =
      "unknown@" + location.hostname;
    connect();
  });
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    host_name = HOSTNAME
    debug_hint = "  DEBUG: DIGDAG_DEBUG=1 python digdag_dashboard.py" if not DEBUG else "  [DEBUG MODE ON]"
    print(f"""
╔══════════════════════════════════════════════════════╗
║          Digdag Dashboard                            ║
╠══════════════════════════════════════════════════════╣
║  URL   : http://{host_name}:{DASHBOARD_PORT}/
║  Debug : http://{host_name}:{DASHBOARD_PORT}/debug/{getpass.getuser()}
║  SH    : {DIGDAG_SH}
║  TMP   : {DASHBOARD_DIR}
╚══════════════════════════════════════════════════════╝
{debug_hint}
""")
    uvicorn.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT, log_level="warning")