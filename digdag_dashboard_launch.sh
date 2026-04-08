#!/bin/bash
# ==============================================================
#  digdag_dashboard_launch.sh
#
#  Launch a per-user Digdag dashboard web server and open browser.
#
#  Each OS user gets their own independent dashboard process
#  on an automatically assigned port.
#
#  Behavior on re-run:
#    - Server already running → just open browser (no restart)
#    - Server not running     → find free port, start, open browser
#    - --restart flag         → kill existing server, then restart
#    - --stop flag            → stop server only
#
#  Usage:
#    ./digdag_dashboard_launch.sh             # start or reuse
#    ./digdag_dashboard_launch.sh --restart   # force restart
#    ./digdag_dashboard_launch.sh --stop      # stop server only
#
#  Per-user isolation:
#    pidfile : /tmp/digdag_dashboard_<user>.pid
#    logfile : /tmp/digdag_dashboard_<user>.log
#    port    : auto-assigned from DASHBOARD_BASE_PORT (default: 8765)
#              each user gets their own port, stored in pidfile
#
#  Requirements:
#    pip install fastapi uvicorn
# ==============================================================

# ── Settings ──────────────────────────────────────────────────
DIGDAG_SH="${DIGDAG_SH:-/user/qarepo/usr/local/bin/digdag.sh}"
DASHBOARD_BASE_PORT="${DASHBOARD_BASE_PORT:-8765}"   # start scanning from here
DASHBOARD_HOST="${DASHBOARD_HOST:-0.0.0.0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_PY="${SCRIPT_DIR}/digdag_dashboard.py"
BROWSER="${BROWSER:-firefox}"
HOST_NAME=$(hostname)

# ── tmp directory (mirrors digdag.sh convention) ──────────────
# /tmp/digdag_<user>/            ← base (shared with digdag.sh)
# /tmp/digdag_<user>/dashboard/  ← dashboard exclusive subdir
DIGDAG_TMP_DIR="/tmp/digdag_${USER}"
DASHBOARD_DIR="${DIGDAG_TMP_DIR}/dashboard"
PIDFILE="${DASHBOARD_DIR}/dashboard.pid"   # "<PID> <PORT>"
LOGFILE="${DASHBOARD_DIR}/dashboard.log"
# ──────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

_log()  { echo -e "$@"; }
_ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
_err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
_info() { echo -e "${CYAN}[INFO]${NC} $*"; }

# ── pidfile format: "<PID> <PORT>" ───────────────────────────
# ── Check if dashboard server is running ──────────────────────
is_running() {
    if [ -f "$PIDFILE" ]; then
        local pid
        pid=$(awk '{print $1}' "$PIDFILE" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            return 0   # running
        fi
        rm -f "$PIDFILE"   # stale
    fi
    return 1
}

get_pid()  { awk '{print $1}' "$PIDFILE" 2>/dev/null; }
get_port() { awk '{print $2}' "$PIDFILE" 2>/dev/null; }

# ── Find a free port starting from base ───────────────────────
find_free_port() {
    local port=$DASHBOARD_BASE_PORT
    local max_try=50
    for (( i=0; i<max_try; i++ )); do
        # Check if any process (not just ours) uses this port
        if ! (echo > /dev/tcp/${HOST_NAME}/$port) 2>/dev/null; then
            echo "$port"
            return 0
        fi
        ((port++))
    done
    echo ""   # none found
    return 1
}

# ── Open browser ──────────────────────────────────────────────
open_browser() {
    local port
    port=$(get_port)
    local url="http://${HOST_NAME}:${port}/"
    _info "Opening browser: ${BOLD}${url}${NC}"
    if command -v "$BROWSER" >/dev/null 2>&1; then
        "$BROWSER" "$url" >/dev/null 2>&1 &
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$url" >/dev/null 2>&1 &
    else
        _warn "Browser not found. Open manually: ${CYAN}${url}${NC}"
    fi
}

# ── Start server ──────────────────────────────────────────────
start_server() {
    # Validate dashboard script exists
    if [ ! -f "$DASHBOARD_PY" ]; then
        _err "Dashboard script not found: $DASHBOARD_PY"
        exit 1
    fi

    # Ensure dashboard tmp directory exists (chmod 700 — owner only)
    mkdir -p "$DASHBOARD_DIR"
    chmod 700 "$DIGDAG_TMP_DIR" "$DASHBOARD_DIR"

    # Validate DIGDAG_SH
    if [ ! -f "$DIGDAG_SH" ]; then
        _err "DIGDAG_SH not found: $DIGDAG_SH"
        _err "Set the DIGDAG_SH environment variable to the correct path."
        exit 1
    fi

    # Validate Python dependencies
    if ! python3 -c "import fastapi, uvicorn" 2>/dev/null; then
        _err "Missing dependencies. Run:"
        _err "  pip install fastapi uvicorn"
        exit 1
    fi

    # Find a free port for this user
    local port
    port=$(find_free_port)
    if [ -z "$port" ]; then
        _err "No free port found (tried ${DASHBOARD_BASE_PORT}+50)"
        exit 1
    fi

    _info "Starting dashboard server (user: ${BOLD}${USER}${NC}, port: ${BOLD}${port}${NC})..."
    _info "Log: ${LOGFILE}"

    # Launch in background, fully detached
    nohup env \
        DIGDAG_SH="$DIGDAG_SH" \
        DASHBOARD_PORT="$port" \
        DASHBOARD_HOST="$DASHBOARD_HOST" \
        python3 "$DASHBOARD_PY" \
        > "$LOGFILE" 2>&1 &

    local server_pid=$!
    disown $server_pid

    # Wait for server to be ready (up to 15s)
    _log -n "  Waiting for server to start "
    local ready=false
    for i in $(seq 1 15); do
        sleep 1
        _log -n "."
        if (echo > /dev/tcp/${HOST_NAME}/$port) 2>/dev/null; then
            ready=true
            break
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            _log ""
            _err "Server process exited unexpectedly."
            _err "Check log: $LOGFILE"
            tail -n 20 "$LOGFILE" >&2
            exit 1
        fi
    done

    if ! $ready; then
        _log ""
        _err "Server did not start within 15 seconds."
        _err "Check log: $LOGFILE"
        tail -n 10 "$LOGFILE" >&2
        exit 1
    fi

    # Save PID and PORT together in pidfile
    echo "$server_pid $port" > "$PIDFILE"

    _log " Done!"
    _ok "Server started (PID: ${server_pid}, PORT: ${port})"
}

# ── Stop server ───────────────────────────────────────────────
stop_server() {
    if ! is_running; then
        _warn "No running dashboard server found."
        return 0
    fi

    local pid port
    pid=$(get_pid)
    port=$(get_port)

    if [ -n "$pid" ]; then
        _info "Stopping server (PID: $pid, PORT: ${port:-unknown})..."
        kill "$pid" 2>/dev/null
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null
        fi
        rm -f "$PIDFILE"
        _ok "Server stopped."
    else
        _warn "PID not found in $PIDFILE"
    fi
}

# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════
echo ""
_log "${BOLD}  Digdag Dashboard Launcher${NC}"
_log "  ────────────────────────────────────"
echo ""

case "${1:-}" in

    # ── Force restart ────────────────────────────────────────
    --restart)
        _info "Restarting dashboard server..."
        stop_server
        sleep 1
        start_server
        open_browser
        ;;

    # ── Stop only ────────────────────────────────────────────
    --stop)
        stop_server
        ;;

    # ── Default: reuse or start ──────────────────────────────
    *)
        if is_running; then
            local_pid=$(get_pid)
            local_port=$(get_port)
            _ok "Dashboard already running (PID: ${local_pid:-unknown}, PORT: ${local_port:-unknown})"
            _info "Reusing existing server — no restart needed."
            open_browser
        else
            start_server
            open_browser
        fi
        ;;
esac

echo ""
_FINAL_PORT=$(get_port)
_log "  URL  : ${CYAN}${BOLD}http://${HOST_NAME}:${_FINAL_PORT}/${NC}"
_log "  SH   : ${DIGDAG_SH}"
_log "  TMP  : ${DASHBOARD_DIR}"
_log "  Log  : ${LOGFILE}"
_log "  PID  : $(get_pid)"
_log "  Stop : ${BOLD}$0 --stop${NC}"
echo ""
