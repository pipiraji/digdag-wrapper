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
from dataclasses import dataclass, field
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
class AttemptInfo:
    """A single attempt returned from /api/attempts."""
    id:          str
    project:     str
    workflow:    str
    status:      str   # running / success / error / killed
    created_at:  str
    finished_at: str

    @property
    def url(self) -> str:
        return ""   # filled in by ServerInfo.attempt_url()


@dataclass
class ServerInfo:
    pid:      str
    port:     str
    user:     str          # OS user who owns this server
    owner:    str  = ""    # same as user; for frontend permission check
    attempts: list = field(default_factory=list)   # list[AttemptInfo]

    @property
    def base_url(self) -> str:
        return f"http://{HOSTNAME}:{self.port}"

    def attempt_url(self, attempt_id: str) -> str:
        return f"{self.base_url}/attempts/{attempt_id}"

    @property
    def running_attempts(self) -> list:
        return [a for a in self.attempts if a.status == "running"]

    def to_dict(self) -> dict:
        """Custom serialization for JSON (replaces asdict for nested objects)."""
        base = self.base_url
        running = self.running_attempts
        return {
            "pid":      self.pid,
            "port":     self.port,
            "user":     self.user,
            "owner":    self.owner or self.user,
            "base_url": base,
            # URL logic:
            #   0 running → base URL
            #   1 running → /attempts/{id}
            #   2+ running → base URL
            "url": (
                self.attempt_url(running[0].id)
                if len(running) == 1
                else base
            ),
            "running": [
                f"{a.project}/{a.workflow}"
                for a in running
            ],
            "attempts": [
                {
                    "id":          a.id,
                    "project":     a.project,
                    "workflow":    a.workflow,
                    "status":      a.status,
                    "created_at":  a.created_at,
                    "finished_at": a.finished_at,
                    "url":         self.attempt_url(a.id),
                }
                for a in self.attempts
            ],
        }


# ══════════════════════════════════════════════════════════════
#  Server discovery — parse "digdag list_server" per user
#  OR scan /proc directly for all users
# ══════════════════════════════════════════════════════════════
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_ROW  = re.compile(
    r"^\|\s*(?P<no>[^|]*)\|\s*(?P<pid>[^|]*)\|\s*(?P<port>[^|]*)"
    r"\|\s*(?P<url>[^|]*)\|\s*(?P<run>[^|]*)\|"
)


def _find_digdag_server_pids() -> list[str]:
    """
    Find PIDs of 'digdag server' processes owned by the current user.
    Mirrors digdag.sh find_my_digdag_server_pid().
    """
    import re as _re
    jar_name = os.path.basename(DIGDAG_SH)   # use script name as filter key
    try:
        out = subprocess.check_output(
            ["ps", "-u", _USER, "-f"],
            text=True, stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return []

    skip = {"run", "push", "start", "retry", "kill", "check"}
    pids = []
    for line in out.splitlines():
        # Match lines containing the digdag script and "server" subcommand
        if DIGDAG_SH in line and "server" in line:
            if not any(k in line for k in skip):
                parts = line.split()
                if len(parts) >= 2:
                    pids.append(parts[1])
    return pids


def _find_port_by_pid(pid: str) -> Optional[str]:
    """
    Find listening port for a given PID.
    Strategy (mirrors digdag.sh find_port_by_pid):
      1. Check /tmp/digdag_<user>/server.info
      2. Check /tmp/digdag_<user>/once.*/server.info
      3. Fallback: ss -tlnp
    """
    # 1. start_server info file
    info_file = f"{DIGDAG_TMP_DIR}/server.info"
    if os.path.isfile(info_file):
        try:
            info = dict(
                line.split("=", 1)
                for line in open(info_file).read().splitlines()
                if "=" in line
            )
            if info.get("PID") == pid:
                return info.get("PORT")
        except Exception:
            pass

    # 2. --once server.info files
    try:
        import glob
        for once_info in glob.glob(f"{DIGDAG_TMP_DIR}/once.*/server.info"):
            info = dict(
                line.split("=", 1)
                for line in open(once_info).read().splitlines()
                if "=" in line
            )
            if info.get("PID") == pid:
                return info.get("PORT")
    except Exception:
        pass

    # 3. Fallback: ss -tlnp
    try:
        out = subprocess.check_output(
            ["ss", "-tlnp"],
            text=True, stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            if f"pid={pid}," in line:
                parts = line.split()
                for part in parts:
                    if ":" in part:
                        port = part.rsplit(":", 1)[-1]
                        if port.isdigit():
                            return port
    except Exception:
        pass

    return None


# ══════════════════════════════════════════════════════════════
#  Digdag REST API client
# ══════════════════════════════════════════════════════════════

def _api_get(base_url: str, path: str, timeout: int = 8) -> Optional[object]:
    """GET {base_url}{path} → parsed JSON or None."""
    import urllib.request, urllib.error
    req = urllib.request.Request(
        f"{base_url}{path}",
        headers={"Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _fetch_attempts_from_api(base_url: str) -> list[AttemptInfo]:
    """
    GET /api/attempts?pageSize=100
    Returns all attempts (all statuses) for this server.
    """
    data = _api_get(base_url, "/api/attempts?pageSize=100")
    if not data:
        return []

    raw_list = data if isinstance(data, list) else data.get("attempts", [])
    result = []
    for a in raw_list:
        result.append(AttemptInfo(
            id          = str(a.get("id", "")),
            project     = a.get("project", {}).get("name", ""),
            workflow    = a.get("workflow", {}).get("name", ""),
            status      = a.get("status", ""),
            created_at  = (a.get("createdAt")  or "")[:19].replace("T", " "),
            finished_at = (a.get("finishedAt") or "")[:19].replace("T", " "),
        ))
    return result


# ══════════════════════════════════════════════════════════════
#  Main data collector
# ══════════════════════════════════════════════════════════════

def fetch_all_servers(username: str = "") -> list[ServerInfo]:
    """
    1. Find digdag server PIDs via ps
    2. Resolve port via server.info / ss
    3. Call /api/attempts for each server
    Returns list[ServerInfo] with full attempt data.
    """
    pids = _find_digdag_server_pids()
    result: list[ServerInfo] = []

    for pid in pids:
        port = _find_port_by_pid(pid)
        if not port:
            continue

        srv = ServerInfo(pid=pid, port=port, user=_USER, owner=_USER)

        # Fetch all attempts from REST API
        srv.attempts = _fetch_attempts_from_api(srv.base_url)

        if DEBUG:
            import sys
            print(
                f"[DEBUG] pid={pid} port={port} "
                f"attempts={len(srv.attempts)} "
                f"running={len(srv.running_attempts)}",
                file=sys.stderr,
            )

        result.append(srv)

    return result


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
        payload_servers = [s.to_dict() for s in all_servers]
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

    # Find servers via ps/ss
    pids = _find_digdag_server_pids()
    lines.append(f"Found PIDs: {pids}")
    lines.append("")

    for pid in pids:
        port = _find_port_by_pid(pid)
        lines.append(f"PID={pid}  PORT={port or '(not found)'}")
        if port:
            base_url = f"http://{HOSTNAME}:{port}"
            attempts = _fetch_attempts_from_api(base_url)
            lines.append(f"  /api/attempts → {len(attempts)} attempt(s)")
            for a in attempts:
                lines.append(
                    f"    id={a.id:>6}  [{a.status:<10}]  "
                    f"{a.project}/{a.workflow}"
                )
        lines.append("")
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
        "servers":   [s.to_dict() for s in servers],
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
                    "servers":   [s.to_dict() for s in servers],
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
    tbody.innerHTML = `<tr><td colspan="6"
      style="text-align:center;color:var(--muted);padding:40px;font-size:14px;">
      No running Digdag servers found.</td></tr>`;
    document.getElementById("kill-all-btn").style.display = "none";
    return;
  }

  const myServers = list.filter(s => (s.owner || s.user) === viewer);
  document.getElementById("kill-all-btn").style.display =
    myServers.length ? "inline-flex" : "none";

  // Section order: own servers first
  const ownerOrder = [...new Set(list.map(s => s.owner || s.user))].sort(
    o => o === viewer ? -1 : 1
  );

  let rows = "";

  ownerOrder.forEach(owner => {
    const group = list.filter(s => (s.owner || s.user) === owner);
    const isMe  = owner === viewer;

    // Owner section header
    rows += `
    <tr>
      <td colspan="6" style="
        background:${isMe ? "rgba(63,185,80,.06)" : "rgba(255,255,255,.02)"};
        padding:5px 14px; border-bottom:1px solid var(--border);">
        <span style="font-size:11px; font-family:'JetBrains Mono',monospace;
          color:${isMe ? "var(--green)" : "var(--muted)"};">
          ${isMe ? "▶" : "  "} ${owner}${isMe ? "  (you)" : "  (read-only)"}
        </span>
      </td>
    </tr>`;

    group.forEach(s => {
      const serverOwner = s.owner || s.user || viewer;
      const baseUrl     = s.base_url || s.url;

      // ── Kill server button ────────────────────────────────
      const killBtn = isMe
        ? `<button class="btn btn-red"
             style="font-size:11px;padding:3px 8px;"
             onclick="confirmKill('${s.pid}','${s.port}','${serverOwner}')">
             🗡 Kill
           </button>`
        : `<button class="btn btn-gray"
             style="font-size:11px;padding:3px 8px;opacity:.35;cursor:not-allowed;"
             disabled title="Owned by ${serverOwner}">🗡 Kill</button>`;

      // ── Port badge (shared across rows) ───────────────────
      const portBadge = `
        <span style="background:rgba(227,179,65,.1);color:var(--amber);
          border:1px solid rgba(227,179,65,.2);border-radius:4px;
          padding:2px 8px;font-size:12px;
          font-family:'JetBrains Mono',monospace;">${s.port}</span>`;

      const runCount = (s.running || []).length;

      if (runCount === 0) {
        // ── No attempts: 1 row, server base URL, (idle) ──────
        rows += `
        <tr class="fade-in" style="${isMe ? "" : "opacity:.8;"}">
          <td class="mono" style="color:var(--muted);font-size:12px;">${s.no}</td>
          <td class="mono" style="font-weight:700;font-size:13px;">${s.pid}</td>
          <td>${portBadge}</td>
          <td class="mono" style="font-size:11px;">
            <a href="${baseUrl}" target="_blank"
               style="color:var(--muted);text-decoration:none;"
               onmouseover="this.style.textDecoration='underline'"
               onmouseout="this.style.textDecoration='none'">${baseUrl}</a>
          </td>
          <td style="color:var(--muted);font-size:12px;">(idle)</td>
          <td style="text-align:center;white-space:nowrap;">
            <button class="btn btn-gray"
              style="font-size:11px;padding:3px 8px;margin-right:4px;"
              onclick="window.open('${baseUrl}','_blank')">🌐 Open</button>
            ${killBtn}
          </td>
        </tr>`;

      } else if (runCount === 1) {
        // ── 1 attempt: 1 row, /attempts/{id} URL, proj/wf badge
        const projWf  = s.running[0];
        const dispUrl = s.url;   // already has /attempts/{id} from list_server
        const badge   = `<span style="display:inline-block;
          background:rgba(63,185,80,.12);color:var(--green);
          border:1px solid rgba(63,185,80,.25);border-radius:4px;
          padding:1px 8px;font-size:11px;
          font-family:'JetBrains Mono',monospace;">${projWf}</span>`;

        rows += `
        <tr class="fade-in" style="${isMe ? "" : "opacity:.8;"}">
          <td class="mono" style="color:var(--muted);font-size:12px;">${s.no}</td>
          <td class="mono" style="font-weight:700;font-size:13px;">${s.pid}</td>
          <td>${portBadge}</td>
          <td class="mono" style="font-size:11px;">
            <a href="${dispUrl}" target="_blank"
               style="color:var(--blue);text-decoration:none;"
               onmouseover="this.style.textDecoration='underline'"
               onmouseout="this.style.textDecoration='none'">${dispUrl}</a>
          </td>
          <td>${badge}</td>
          <td style="text-align:center;white-space:nowrap;">
            <button class="btn btn-gray"
              style="font-size:11px;padding:3px 8px;margin-right:4px;"
              onclick="window.open('${dispUrl}','_blank')">🌐 Open</button>
            ${killBtn}
          </td>
        </tr>`;

      } else {
        // ── 2+ attempts: 1 row, base URL, all proj/wf badges ─
        const badges = s.running.map(r =>
          `<span style="display:inline-block;
            background:rgba(63,185,80,.12);color:var(--green);
            border:1px solid rgba(63,185,80,.25);border-radius:4px;
            padding:1px 8px;font-size:11px;margin:2px 2px 2px 0;
            font-family:'JetBrains Mono',monospace;">${r}</span>`
        ).join("");

        rows += `
        <tr class="fade-in" style="${isMe ? "" : "opacity:.8;"}">
          <td class="mono" style="color:var(--muted);font-size:12px;">${s.no}</td>
          <td class="mono" style="font-weight:700;font-size:13px;">${s.pid}</td>
          <td>${portBadge}</td>
          <td class="mono" style="font-size:11px;">
            <a href="${baseUrl}" target="_blank"
               style="color:var(--blue);text-decoration:none;"
               onmouseover="this.style.textDecoration='underline'"
               onmouseout="this.style.textDecoration='none'">${baseUrl}</a>
          </td>
          <td>${badges}</td>
          <td style="text-align:center;white-space:nowrap;">
            <button class="btn btn-gray"
              style="font-size:11px;padding:3px 8px;margin-right:4px;"
              onclick="window.open('${baseUrl}','_blank')">🌐 Open</button>
            ${killBtn}
          </td>
        </tr>`;
      }
    });
  });

  tbody.innerHTML = rows;
}


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