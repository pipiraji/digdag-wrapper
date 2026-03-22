#!/bin/bash
# ============================================================
#  digdag.sh  (wrapper)
#
#  Extends digdag with custom subcommands while keeping all original commands.
#  Manages per-user Digdag servers in a multi-user LSF HPC environment.
#
# ────────────────────────────────────────────────────────────
#  Custom command list
# ────────────────────────────────────────────────────────────
#
#  start_server
#    Only one server per user. Reuses existing server or starts a new one.
#    Multiple servers only allowed via run_workflow --once (disposable mode).
#    Usage: digdag start_server
#
#  kill_server
#    Stops my Digdag server.
#    Prompts for confirmation, then kills server + all child processes via kill -- -$pid.
#    Usage: digdag kill_server
#
#  list_server
#    Displays all running servers in a horizontal table.
#    Includes running project/workflow list per server.
#    Usage: digdag list_server
#
#  run_workflow <project> <workflow> [options]
#    Boot server (reuse or new) -> push -> start.
#    --once: new dedicated server -> push -> start
#                   -> wait for workflow completion -> auto shutdown server (disposable).
#    --once uses one dedicated server per project for full isolation.
#    Options:
#    --once                   : Disposable server mode.
#    --log, -L <file>         : --once only. Background monitoring log file.
#      --project, -d <dir>      : Project directory (default: current directory).
#      --params-file, -P <file> : External parameter file.
#    Usage: digdag run_workflow my_project etl_workflow
#            digdag run_workflow --once my_project etl_workflow
#            digdag run_workflow --project /path/to -P p.yml my_project etl_workflow
#
#  list_job [options]
#    Auto-selects if one server; prompts selection if multiple. Displays attempts table.
#    Options:
#      --all        : Show all statuses (default: running only).
#      -p <project> : Project filter.
#      -w <workflow>: Workflow filter.
#    Usage: digdag list_job
#            digdag list_job --all -p my_project
#
#  kill_job [options]
#    Select server -> show running attempts -> kill by ID or kill all.
#    Options:
#      --all        : Kill all matching attempts immediately.
#      -p <project> : Project filter.
#      -w <workflow>: Workflow filter.
#    Usage: digdag kill_job
#            digdag kill_job --all -p my_project
#
#  browse
#    Opens Digdag UI in browser (xdg-open / firefox).
#    Usage: digdag browse
#
# ────────────────────────────────────────────────────────────
#  Server management design
# ────────────────────────────────────────────────────────────
#  - Multiple servers per account supported (auto port assignment).
#  - setsid + disown fully detaches server from parent process.
#    (Server persists after script exit / terminal close / LSF job end.)
#  - kill -- -$pid terminates server + all child processes in the group.
#  - Watcher process detects server shutdown via port check -> auto-removes lock.
#  - Race condition prevention: noclobber lock ensures only one server starts at a time.
#  - All files stored under /tmp/digdag_$USER/ (saves NFS home quota).
#
#  File structure:
#    /tmp/digdag_<user>/
#      ├── server.log.<PID>   : Server log
#      ├── server.info        : PORT / PID / URL / STARTED
#      ├── server.lock        : Race condition prevention lock
#      ├── task-logs/         : Task execution logs
#      └── jvm-tmp/           : JVM temp directory
# ============================================================

# ── Settings ────────────────────────────────────────────────────
BASE_PORT=65432
MAX_RETRIES=50
BOOT_TIMEOUT=15
LOCK_TIMEOUT=60            # Max wait time for follower processes (seconds)
USER_NAME=$(id -un)
HOST_NAME=$(hostname)
WORK_DIR="$(pwd)"
# Uses local /tmp on each compute node
#  - Saves home directory quota (NFS shared home 1GiB limit)
#  - Independent space per server (LSF multi-compute-farm env)
#  - Auto-cleaned on reboot (server dies with the node)
DIGDAG_TMP_DIR="/tmp/digdag_${USER_NAME}"          # start_server exclusive
DIGDAG_JVM_TMP="${DIGDAG_TMP_DIR}/jvm-tmp"          # JVM temp directory
LOG_FILE="${DIGDAG_TMP_DIR}/server.log"
TASK_LOG_DIR="${DIGDAG_TMP_DIR}/task-logs"
INFO_FILE="${DIGDAG_TMP_DIR}/server.info"
LOCK_FILE="${DIGDAG_TMP_DIR}/server.lock"
# --once exclusive dir: PID-based uniqueness, fully separated from start_server
# (finalized at runtime via $$, empty here)
ONCE_TMP_DIR=""
DIGDAG_JAR="/user/qarepo/usr/local/digdag-0.10.5.1.jar"  # Update this path when changing digdag version
# ────────────────────────────────────────────────────────────

# ── Color definitions ────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;34m'   # Blue (good visibility on both white/black backgrounds)
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'
# ────────────────────────────────────────────────────────────

# ── Common helpers ────────────────────────────────────────────────
log() { echo -e "$@" >&2; }
print_divider() { log "${CYAN}----------------------------------------------------${NC}"; }

# Port check using pure Bash /dev/tcp
port_in_use() {
    (echo > /dev/tcp/$HOST_NAME/$1) >/dev/null 2>&1
}

# Find only my "digdag server" processes
# Filter by jar filename for accuracy (excludes run/push/start etc.)
find_my_digdag_server_pid() {
    local jar_name
    jar_name=$(basename "$DIGDAG_JAR")
    ps -u "$USER_NAME" -f 2>/dev/null | awk -v jar="$jar_name" '$0 ~ jar && /server/ && !/run/ && !/push/ && !/start/ && !/retry/ && !/kill/ && !/check/ { print $2 }'
}

# Extract port from PID
# ss -tlnp may omit users column under newgrp,
# so read /proc/<pid>/net/tcp directly (group-independent).
find_port_by_pid() {
    local pid="$1"
    # /proc/<pid>/net/tcp: local address column(2) is hex-encoded
    # Extract port from LISTEN(0A) sockets only
    local port_hex port_dec
    port_hex=$(cat "/proc/$pid/net/tcp" 2>/dev/null \
        | awk 'NR>1 && $4=="0A" {print $2}' \
        | awk -F: '{print $2}' \
        | head -1)
    [ -z "$port_hex" ] && return 1
    # Convert hex to decimal
    port_dec=$(printf '%d' "0x${port_hex}" 2>/dev/null)
    [ -n "$port_dec" ] && echo "$port_dec"
}
# ── Security 1: Validate DIGDAG_JAR and build exec command ────────
# Direct jar path avoids which-based lookup
# Explicit java -jar ensures fixed version

if [ ! -f "$DIGDAG_JAR" ]; then
    log "${RED}[ERROR] DIGDAG_JAR not found: $DIGDAG_JAR${NC}"
    exit 1
fi

# Check java availability
if ! command -v java >/dev/null 2>&1; then
    log "${RED}[ERROR] java command not found. Please check JDK/JRE installation.${NC}"
    exit 1
fi

# DIGDAG_BIN: use this var for all digdag executions
# Usage: "${DIGDAG_BIN[@]}" server / push / start ...
DIGDAG_BIN=(java -Djava.io.tmpdir="$DIGDAG_JVM_TMP" -jar "$DIGDAG_JAR")
# ────────────────────────────────────────────────────────────

# ── Security 2: Cleanup orphan processes on interrupt (INT/TERM only) ──────────
BOOTING_PID=""
BOOT_SUCCESS=false

cleanup_on_exit() {
    if [ -n "$BOOTING_PID" ] && ! $BOOT_SUCCESS; then
        log "\n${YELLOW}[WARN] Execution interrupted. Cleaning up booting server (PID: $BOOTING_PID)${NC}"
        kill -9 "$BOOTING_PID" 2>/dev/null
        rm -f "$LOCK_FILE"
    fi
}
trap cleanup_on_exit INT TERM
# ────────────────────────────────────────────────────────────

# ── Initialize work directories (always on script entry) ────────────
# DIGDAG_JVM_TMP needed even for non-server commands like run/push
mkdir -p "$DIGDAG_TMP_DIR" "$TASK_LOG_DIR" "$DIGDAG_JVM_TMP"
chmod 700 "$DIGDAG_TMP_DIR" "$TASK_LOG_DIR" "$DIGDAG_JVM_TMP"
# ────────────────────────────────────────────────────────────

# ── Check server alive -> stdout: port / exit 0=alive 1=none ───
# start_server exclusive: cross-check PID with server.info to exclude --once servers
check_server_alive() {
    local pid port

    # If server.info exists, use its PID (start_server exclusive)
    if [ -f "$INFO_FILE" ]; then
        pid=$(grep '^PID=' "$INFO_FILE" 2>/dev/null | cut -d= -f2)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            port=$(grep '^PORT=' "$INFO_FILE" | cut -d= -f2)
            [ -n "$port" ] && port_in_use "$port" && { echo "$port"; return 0; }
        fi
        # If info file exists but process is gone -> clean up and return none
        rm -f "$INFO_FILE" "$LOCK_FILE"
        return 1
    fi

    # No server.info -> no start_server running
    return 1
}

# ── Boot server -> stdout: port / exit 0=OK 1=fail ─────────
#
#  Always boots a new server (regardless of existing servers)
#  Race condition prevention design:
#   - Creates lock file atomically via noclobber (set -C)
#     -> only first process acquires lock; followers wait then reuse
#   - Lock file lifetime = digdag server process lifetime
#
start_server() {
    # Save server log with PID-based filename (server.log.<PID>)
    # Accumulates without rotation; auto-removed on reboot via /tmp
    LOG_FILE="${DIGDAG_TMP_DIR}/server.log.$$"

    # ── Attempt lock acquisition (atomic: noclobber) ──────────────────
    if (set -C; echo $$ > "$LOCK_FILE") 2>/dev/null; then

        # ── First process: lock acquired -> boot server ─────
        log "  [LOCK] Lock acquired -> starting server boot (PID: $$)"

        local port=$BASE_PORT

        for (( i=1; i<=MAX_RETRIES; i++ )); do
            log "  ${YELLOW}[Attempt $i/$MAX_RETRIES]${NC} Port ${BOLD}$port${NC} checking..."

            if port_in_use $port; then
                log "  [ERROR] Port $port in use -> try next port"
                ((port++)); continue
            fi

            log "  [OK] Port $port available -> booting server..."

            # setsid: new session -> no SIGHUP on parent exit
            # disown: removed from bash job table -> fully independent
            setsid "${DIGDAG_BIN[@]}" server \
                --bind 0.0.0.0 \
                --port $port \
                --memory \
                --task-log "$TASK_LOG_DIR" \
                > "$LOG_FILE" 2>&1 &

            BOOTING_PID=$!
            disown $BOOTING_PID

            # polling: wait until port is actually open
            log -n "  [WAIT] Booting "
            for (( j=1; j<=BOOT_TIMEOUT; j++ )); do
                sleep 1; log -n "."
                ! kill -0 "$BOOTING_PID" 2>/dev/null && { log " Process exit detected"; break; }
                if port_in_use $port; then
                    log " Done! (${j}s)"
                    BOOT_SUCCESS=true; break
                fi
            done

            if $BOOT_SUCCESS; then
                # Security 3: Protect server.info permissions (owner read/write only)
                cat > "$INFO_FILE" <<EOF
PORT=$port
PID=$BOOTING_PID
URL=http://$HOST_NAME:$port
STARTED=$(date '+%Y-%m-%d %H:%M:%S')
EOF
                chmod 600 "$INFO_FILE"

                # ── Watcher process ────────────────────────────
                # kill -0 polling detects server exit -> auto-remove lock
                # Lock file lifetime = digdag server process lifetime
                (
                    exec >/dev/null 2>&1
                    local watch_port="$port"
                    local watch_lock="$LOCK_FILE"
                    local watch_info="$INFO_FILE"
                    local jar_name
                    jar_name=$(basename "$DIGDAG_JAR")
                    # Prevent PID reuse: verify server existence via find_my_digdag_server_pid
                    # Port closed = server dead (most reliable indicator)
                    while (echo > /dev/tcp/$HOST_NAME/$watch_port) >/dev/null 2>&1; do
                        sleep 5
                        touch "$watch_lock" "$watch_info" 2>/dev/null
                    done
                    rm -f "$watch_lock"
                ) &
                disown $!

                echo "$port"
                return 0
            fi

            log "  [WARN] Boot failed -> try next port"
            kill -9 "$BOOTING_PID" 2>/dev/null
            BOOTING_PID=""
            ((port++))
        done

        # All retries failed: remove lock directly
        rm -f "$LOCK_FILE"
        log "${RED}[ERROR] Server boot failed after ${MAX_RETRIES} attempts${NC}"
        log "   Check log: $LOG_FILE"
        tail -n 10 "$LOG_FILE" >&2
        return 1

    else

        # ── Follower process: wait then re-check server ─────────────
        local lock_pid
        lock_pid=$(cat "$LOCK_FILE" 2>/dev/null)
        log "  [WAIT] Another process (PID: $lock_pid)) is booting server. Waiting..."

        for (( t=1; t<=LOCK_TIMEOUT; t++ )); do
            sleep 1
            log -n "."

            # Lock owner died -> release lock and retry
            if [ -n "$lock_pid" ] && ! kill -0 "$lock_pid" 2>/dev/null; then
                log "\n  [WARN] Boot process exit detected -> retrying."
                rm -f "$LOCK_FILE"
                start_server
                return $?
            fi

            # Check if server is up
            local port
            if port=$(check_server_alive); then
                log "\n  ${GREEN}[OK] Server up confirmed (PORT: $port)${NC}"
                echo "$port"
                return 0
            fi
        done

        log "\n${RED}[ERROR] Server boot wait timeout (${LOCK_TIMEOUT}s)${NC}"
        return 1
    fi
}


# ════════════════════════════════════════════════════════════
#  --once dedicated server boot -> stdout: port / exit 0=OK 1=fail
#
#  Fully separated from start_server:
#   - PID-based dedicated directory -> no lock/info file conflicts
#   - Does not affect check_server_alive (start_server side)
#   - No race condition lock needed (disposable, no competition)
# ════════════════════════════════════════════════════════════
start_once_server() {
    # Set PID-based dedicated directory
    ONCE_TMP_DIR="${DIGDAG_TMP_DIR}/once.$$"
    local once_jvm_tmp="${ONCE_TMP_DIR}/jvm-tmp"
    local once_task_log="${ONCE_TMP_DIR}/task-logs"
    local once_info="${ONCE_TMP_DIR}/server.info"
    local once_log="${ONCE_TMP_DIR}/server.log.$$"

    mkdir -p "$ONCE_TMP_DIR" "$once_task_log" "$once_jvm_tmp"
    chmod 700 "$ONCE_TMP_DIR" "$once_task_log" "$once_jvm_tmp"

    # --once also uses dedicated tmpdir for DIGDAG_BIN
    local once_bin=(java -Djava.io.tmpdir="$once_jvm_tmp" -jar "$DIGDAG_JAR")

    local port=$BASE_PORT
    for (( i=1; i<=MAX_RETRIES; i++ )); do
        if port_in_use $port; then
            ((port++)); continue
        fi

        log "  [OK] Port $port -> booting disposable server..."

        setsid "${once_bin[@]}" server \
            --bind 0.0.0.0 \
            --port $port \
            --memory \
            --task-log "$once_task_log" \
            > "$once_log" 2>&1 &

        BOOTING_PID=$!
        disown $BOOTING_PID

        log -n "  [WAIT] Booting "
        for (( j=1; j<=BOOT_TIMEOUT; j++ )); do
            sleep 1; log -n "."
            ! kill -0 "$BOOTING_PID" 2>/dev/null && { log " Process exit detected"; break; }
            if port_in_use $port; then
                log " Done! (${j}s)"
                BOOT_SUCCESS=true; break
            fi
        done

        if $BOOT_SUCCESS; then
            cat > "$once_info" <<EOF
PORT=$port
PID=$BOOTING_PID
URL=http://$HOST_NAME:$port
STARTED=$(date '+%Y-%m-%d %H:%M:%S')
EOF
            chmod 600 "$once_info"
            echo "$port"
            return 0
        fi

        kill -9 "$BOOTING_PID" 2>/dev/null
        BOOTING_PID=""
        ((port++))
    done

    log "${RED}[ERROR] Disposable server boot failed${NC}"
    log "  Check log: $once_log"
    tail -n 10 "$once_log" >&2
    return 1
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: run_workflow
# ════════════════════════════════════════════════════════════
cmd_run_workflow() {
    local project_dir="$WORK_DIR"
    local project_name=""
    local workflow_name=""
    local params_file=""       # -P / --params-file (optional)
    local once=false           # --once: disposable (new server -> push -> start -> shutdown)
    local log_file=""          # --log <file>: background monitoring log file (--once only)
    local log_file=""          # --log <file>: background monitoring log file (--once only)

    # Security 4: Strict option parsing - reject unknown options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --once) once=true; shift ;;
            --log|-L) log_file="$2"; shift 2 ;;
            --project|-d)
                project_dir="$2"; shift 2 ;;
            --params-file|-P)
                params_file="$2"; shift 2 ;;
            -*)
                log "${RED}[ERROR] Unknown option: $1${NC}"
                exit 1 ;;
            *)
                if [ -z "$project_name" ]; then
                    project_name="$1"
                elif [ -z "$workflow_name" ]; then
                    workflow_name="$1"
                fi
                shift ;;
        esac
    done

    # ── Validate required arguments ───────────────────────────────────────
    local has_error=false

    if [ -z "$project_name" ] || [ -z "$workflow_name" ]; then
        has_error=true
    fi

    # Check params-file exists if specified
    if [ -n "$params_file" ] && [ ! -f "$params_file" ]; then
        log "${RED}[ERROR] --params-file not found: $params_file${NC}"
        has_error=true
    fi

    if $has_error; then
        log ""
        log "${RED}Usage: digdag run_workflow [options] <project_name> <workflow_name>${NC}"
        log ""
        log "  [Required]"
        log "  project_name             : Project name to register in Digdag"
        log "  workflow_name            : Workflow name to execute (.dig filename)"
        log ""
        log "  [Optional]"
        log "  --project, -d <dir>      : Project directory (default: current directory)"
        log "  --params-file, -P <file> : External parameter file path"
        log "  --once                   : Disposable server (new server -> push -> start -> bg wait -> server stop)
  --log, -L <file>         : --once only. Background monitoring log file path"
        log ""
        log "  Example) digdag run_workflow my_project etl_workflow"
        log "  Example) digdag run_workflow --once -P params.yml my_project etl_workflow"
        log "  Example) digdag run_workflow --project /path/to/proj -P params.yml my_project etl_workflow"
        log ""
        exit 1
    fi

    # Server mode label
    local mode_str
    if $once; then
        mode_str="Disposable server (--once: new server -> auto shutdown after completion)"
    else
        mode_str="Reuse existing server or start new one"
    fi

    print_divider
    log "${BOLD}  [START] run_workflow${NC}"
    log "  Project dir      : ${CYAN}$project_dir${NC}"
    log "  Project name     : ${CYAN}$project_name${NC}"
    log "  Workflow name    : ${CYAN}$workflow_name${NC}"
    [ -n "$params_file" ] && log "  Params file      : ${CYAN}$params_file${NC}"
    log "  Server mode      : ${CYAN}${mode_str}${NC}"
    [ -n "$log_file" ] && log "  Monitor log      : ${CYAN}${log_file}${NC}"
    print_divider

    # ── STEP 1. Check or boot server ─────────────────────────
    log "\n${YELLOW}[STEP 1]${NC} Checking Digdag server..."

    local port
    local server_booted=false  # Whether server was booted this run (used to decide --once shutdown)

    if ! $once && port=$(check_server_alive); then
        log "${GREEN}[OK] Reusing existing server (PORT: $port)${NC}"
    else
        if $once; then
            log "  [INFO] --once: booting dedicated disposable server."
            if ! port=$(start_once_server); then
                log "${RED}[ERROR] Disposable server boot failed. Aborting run_workflow.${NC}"
                exit 1
            fi
        else
            log "  No server found -> auto-booting."
            if ! port=$(start_server); then
                log "${RED}[ERROR] Server boot failed. Aborting run_workflow.${NC}"
                exit 1
            fi
        fi
        server_booted=true
        log "${GREEN}[OK] Server ready (PORT: $port)${NC}"
    fi

    local endpoint="http://$HOST_NAME:$port"

    # ── STEP 2. Push ─────────────────────────────────────────
    log "\n${YELLOW}[STEP 2]${NC} Pushing project..."
    log "  $ digdag push $project_name -e $endpoint --project $project_dir"

    "${DIGDAG_BIN[@]}" push "$project_name" \
        -e "$endpoint" \
        --project "$project_dir"

    if [ $? -ne 0 ]; then
        log "${RED}[ERROR] Push failed. Aborting run_workflow.${NC}"
        $once && $server_booted && _kill_server_by_port "$port"
        exit 1
    fi
    log "${GREEN}[OK] Push done${NC}"

    # ── STEP 3. Start ────────────────────────────────────────
    log "\n${YELLOW}[STEP 3]${NC} Starting workflow..."

    local start_cmd
    start_cmd=(
        "${DIGDAG_BIN[@]}" start "$project_name" "$workflow_name"
        -e "$endpoint"
        --session now
    )
    [ -n "$params_file" ] && start_cmd+=(--params-file "$params_file")

    log "  $ ${start_cmd[*]}"
    local start_output
    start_output=$("${start_cmd[@]}" 2>&1)
    local start_rc=$?
    echo "$start_output" >&2

    if [ $start_rc -ne 0 ]; then
        log "${RED}[ERROR] Start failed.${NC}"
        $once && $server_booted && _kill_server_by_port "$port"
        exit 1
    fi
    log "${GREEN}[OK] Start done${NC}"

    # ── STEP 4 (--once only). Extract attempt id ────────────────
    if $once && $server_booted; then
        local attempt_id
        attempt_id=$(echo "$start_output" | awk -F': ' '/attempt id:|^ *id:/ {gsub(/^ +/,"",$2); print $2}' | grep -E '^[0-9]+$' | head -1)

        if [ -n "$attempt_id" ]; then
            log "\n${YELLOW}[STEP 4]${NC} attempt id: ${BOLD}$attempt_id${NC}"
        else
            log "\n${YELLOW}[STEP 4]${NC} ${YELLOW}[WARN] attempt id extraction failed. Using fallback polling.${NC}"
        fi

        # ── STEP 5. Switch to background -> return prompt immediately ────
        log "\n${YELLOW}[STEP 5]${NC} Switching to background."
        log "  Server will auto-shutdown after workflow completes."
        if [ -n "$log_file" ]; then
            log "  Workflow log  : ${CYAN}$log_file${NC}  (written after completion)"
            log "  Status log    : ${CYAN}${log_file}.status${NC}  (real-time)"
        else
            log "  ${YELLOW}(Use --log <file> to specify a log file)${NC}"
        fi

        # Save monitor script to /tmp (safe even if ONCE_TMP_DIR not yet created)
        local _v_ep="$endpoint"
        local _v_port="$port"
        local _v_aid="$attempt_id"
        local _v_lf="$log_file"
        local _v_od="$ONCE_TMP_DIR"
        local _v_host="$HOST_NAME"
        local _v_jar="$DIGDAG_JAR"
        local _v_jvm="${DIGDAG_TMP_DIR}/jvm-tmp"
        local _script="/tmp/digdag_monitor_${USER_NAME}_$$.sh"

        _q() { printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\''/g")"; }
        {
          echo "#!/bin/bash"
          echo "_ep=$(_q "$_v_ep")"
          echo "_port=$(_q "$_v_port")"
          echo "_aid=$(_q "$_v_aid")"
          echo "_lf=$(_q "$_v_lf")"
          echo "_od=$(_q "$_v_od")"
          echo "_host=$(_q "$_v_host")"
          echo "_jar=$(_q "$_v_jar")"
          echo "_jvm=$(_q "$_v_jvm")"
        } > "$_script"
        cat >> "$_script" << 'MONITOR_LOGIC'
_sf=''
# Auto-create ONCE_TMP_DIR/workflow.log if --log not specified
[ -z "$_lf" ] && _lf="${_od}/workflow.log"
_sf="${_lf}.status"
_s() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "$_sf"; }
_s "Started (aid=${_aid:-N/A} port=${_port})"
_s "Workflow log -> ${_lf}"
_s "Status log   -> ${_sf}"
if [ -n "$_aid" ]; then
    # digdag log -f: stream logs until attempt completes -> auto-exit
    # Handles both --log specified and unspecified cases uniformly
    _s "Starting digdag log -f..."
    java -Djava.io.tmpdir="$_jvm" -jar "$_jar" log "$_aid" -e "http://${_host}:${_port}" -f > "$_lf" 2>&1
    _s "digdag log -f done (exit=$?)"
else
    # fallback: poll server alive + running status when no attempt id
    _s "[WARN] No attempt id. Using polling fallback."
    sleep 5
    while true; do
        sleep 10
        (echo > /dev/tcp/${_host}/${_port}) >/dev/null 2>&1 || { _s "[WARN] Server not responding. Exiting."; break; }
        _running=$(java -Djava.io.tmpdir="$_jvm" -jar "$_jar" attempts -e "http://${_host}:${_port}" 2>/dev/null | grep 'status: *running')
        [ -n "$_running" ] && { _s "polling... running"; continue; }
        break
    done
fi
# Server stop
_s "Stopping server (PORT=${_port})..."
_jn=$(basename "$_jar")
while IFS= read -r _p || [ -n "$_p" ]; do
    [ -z "$_p" ] && continue
    _ph=$(awk 'NR>1 && $4=="0A" {print $2}' "/proc/${_p}/net/tcp" 2>/dev/null | awk -F: '{print $2}' | head -1)
    _pp=$(printf '%d' "0x${_ph}" 2>/dev/null)
    if [ "$_pp" = "$_port" ]; then
        kill -- "-${_p}" 2>/dev/null
        [ -n "$_od" ] && [ -d "$_od" ] && rm -rf "$_od"
        _s "Server stopped (PID=${_p})"
        break
    fi
done < <(ps -u "$(id -un)" -f 2>/dev/null | awk -v j="$_jn" '$0~j&&/server/&&!/run/&&!/push/&&!/start/&&!/retry/&&!/kill/&&!/check/{print $2}')
rm -f "$0"
_s "Monitor done"
MONITOR_LOGIC
        chmod 700 "$_script"

        # Run fully detached via nohup + setsid -> prompt returns immediately
        nohup setsid bash "$_script" </dev/null >/dev/null 2>/dev/null &
        disown $!
    fi

    log ""
    print_divider
    log "${GREEN}${BOLD}[DONE] run_workflow complete!${NC}"
    log "  Project   : $project_name"
    log "  Workflow  : $workflow_name"
    if $once && $server_booted; then
        log "  Mode      : ${YELLOW}Disposable (waiting for completion in background)${NC}"
        log "  Server PORT: $port"
        if [ -n "$log_file" ]; then
            log "  Log file  : ${CYAN}$log_file${NC}  (written after workflow completes)"
            log "  Status    : ${CYAN}${log_file}.status${NC}  (real-time)"
        else
            log "  Log file  : ${CYAN}${ONCE_TMP_DIR}/workflow.log${NC}  (auto-created)"
            log "  Status    : ${CYAN}${ONCE_TMP_DIR}/workflow.log.status${NC}"
        fi
    else
        log "  URL       : $endpoint"
    fi
    print_divider
    log ""
}

# ════════════════════════════════════════════════════════════
#  Internal helper: kill server process group by port
# ════════════════════════════════════════════════════════════
_kill_server_by_port() {
    local target_port="$1"
    local all_pids
    all_pids=$(find_my_digdag_server_pid)
    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local p
        p=$(find_port_by_pid "$pid")
        if [ "$p" = "$target_port" ]; then
            kill -- "-$pid" 2>/dev/null
            # --once server: remove entire ONCE_TMP_DIR
            # start_server: remove LOCK_FILE / INFO_FILE only
            if [ -n "$ONCE_TMP_DIR" ] && [ -d "$ONCE_TMP_DIR" ]; then
                rm -rf "$ONCE_TMP_DIR"
            else
                rm -f "$LOCK_FILE" "$INFO_FILE"
            fi
            log "  ${GREEN}[OK] Server stopped (PID=$pid)${NC}"
            return 0
        fi
    done <<< "$all_pids"
    log "  ${YELLOW}[WARN] No server found to stop.${NC}"
    return 1
}

# ════════════════════════════════════════════════════════════
#  Common helper: fetch attempts
#
#  Args: $1=project $2=workflow $3=endpoint $4=status_filter("running"|"")
#  Sets the following vars after call:
#    _attempts_raw  : Raw output of digdag attempts
#    _blocks        : Filtered blocks matching conditions (blank-line separated)
#    _running_ids   : Attempt IDs with status=running (for kill use)
# ════════════════════════════════════════════════════════════
fetch_attempts() {
    local proj="$1" wf="$2" ep="$3" status_filter="$4"

    local cmd=("${DIGDAG_BIN[@]}" attempts -e "$ep")
    [ -n "$proj" ] && cmd+=(--project "$proj")

    _attempts_raw=$("${cmd[@]}" 2>/dev/null) || return 1

    # Split blocks by blank lines then filter (handles 2-space indent)
    # NOTE: pass awk vars via env to avoid special char issues
    _blocks=$(sf="$status_filter" wf="$wf" awk 'BEGIN{RS=""; ORS="\n\n"} (ENVIRON["sf"]=="" || $0 ~ "status: *"ENVIRON["sf"]) && (ENVIRON["wf"]=="" || $0 ~ "workflow: *"ENVIRON["wf"]) {print}' <<< "$_attempts_raw")

    # Extract running attempt IDs (for kill) - stable block-based extraction
    _running_ids=$(awk 'BEGIN{RS=""; ORS="\n"} /status: *running/ {match($0, /attempt id: *([0-9]+)/, a); if(a[1]!="") print a[1]}' <<< "$_blocks" | grep -v "^$")

    [ -z "$_blocks" ] && return 2
    return 0
}

# ════════════════════════════════════════════════════════════
#  Common helper: print attempts table
#
#  Columns: project | workflow | session id | attempt id
#             created at | finished at | status
#  Note: digdag attempts output has 2-space indent per field
# ════════════════════════════════════════════════════════════
print_attempts_table() {
    # Extract fields from blocks into arrays
    local projects=() workflows=() session_ids=() attempt_ids=()
    local created_ats=() finished_ats=() statuses=()

    # Insert block separators then iterate
    # Strip leading spaces via gsub then extract $2
    local block=""
    while IFS= read -r line || [ -n "$line" ]; do
        if [ "$line" = "---BLOCK---" ]; then
            [ -z "$block" ] && continue
            attempt_ids+=( "$(echo "$block" | awk -F': ' '/attempt id:/  {gsub(/^ +/,"",$2); print $2}')" )
            session_ids+=( "$(echo "$block" | awk -F': ' '/session id:/  {gsub(/^ +/,"",$2); print $2}')" )
            projects+=(    "$(echo "$block" | awk -F': ' '/project:/     {gsub(/^ +/,"",$2); print $2}')" )
            workflows+=(   "$(echo "$block" | awk -F': ' '/workflow:/    {gsub(/^ +/,"",$2); print $2}')" )
            created_ats+=( "$(echo "$block" | awk -F': ' '/created at:/  {gsub(/^ +/,"",$2); print $2}')" )
            finished_ats+=("$(echo "$block" | awk -F': ' '/finished at:/ {gsub(/^ +/,"",$2); print $2}')" )
            statuses+=(    "$(echo "$block" | awk -F': ' '/status:/      {gsub(/^ +/,"",$2); print $2}')" )
            block=""
        else
            block+="$line"$'\n'
        fi
    done <<< "$(awk 'BEGIN{RS=""; ORS="\n---BLOCK---\n"} {print}' <<< "$_blocks")"

    local count=${#attempt_ids[@]}
    if [ "$count" -eq 0 ]; then return; fi

    # Calculate max column widths (including header)
    local w_proj=7 w_wf=8 w_sid=10 w_aid=10 w_cat=19 w_fat=19 w_st=8
    for (( i=0; i<count; i++ )); do
        [ ${#projects[$i]}     -gt $w_proj ] && w_proj=${#projects[$i]}
        [ ${#workflows[$i]}    -gt $w_wf   ] && w_wf=${#workflows[$i]}
        [ ${#session_ids[$i]}  -gt $w_sid  ] && w_sid=${#session_ids[$i]}
        [ ${#attempt_ids[$i]}  -gt $w_aid  ] && w_aid=${#attempt_ids[$i]}
        [ ${#created_ats[$i]}  -gt $w_cat  ] && w_cat=${#created_ats[$i]}
        [ ${#finished_ats[$i]} -gt $w_fat  ] && w_fat=${#finished_ats[$i]}
        [ ${#statuses[$i]}     -gt $w_st   ] && w_st=${#statuses[$i]}
    done

    # Build separator line
    local sep
    sep=$(printf '+-%s-+-%s-+-%s-+-%s-+-%s-+-%s-+-%s-+
' \
        "$(printf '%*s' $w_proj | tr ' ' '-')" \
        "$(printf '%*s' $w_wf   | tr ' ' '-')" \
        "$(printf '%*s' $w_sid  | tr ' ' '-')" \
        "$(printf '%*s' $w_aid  | tr ' ' '-')" \
        "$(printf '%*s' $w_cat  | tr ' ' '-')" \
        "$(printf '%*s' $w_fat  | tr ' ' '-')" \
        "$(printf '%*s' $w_st   | tr ' ' '-')")

    # Print header
    log "$sep"
    log "$(printf "| %-${w_proj}s | %-${w_wf}s | %-${w_sid}s | %-${w_aid}s | %-${w_cat}s | %-${w_fat}s | %-${w_st}s |" \
        "project" "workflow" "session id" "attempt id" "created at" "finished at" "status")"
    log "$sep"

    # Print data rows (color by status)
    for (( i=0; i<count; i++ )); do
        local color="$NC"
        case "${statuses[$i]}" in
            *running*) color="$GREEN" ;;
            *error*|*failed*) color="$RED" ;;
            *success*) color="$CYAN" ;;
        esac
        log "$(printf "| %-${w_proj}s | %-${w_wf}s | %-${w_sid}s | %-${w_aid}s | %-${w_cat}s | %-${w_fat}s | ${color}%-${w_st}s${NC} |" \
            "${projects[$i]}" "${workflows[$i]}" "${session_ids[$i]}" "${attempt_ids[$i]}" \
            "${created_ats[$i]}" "${finished_ats[$i]}" "${statuses[$i]}")"
    done
    log "$sep"
    log "  total: ${BOLD}${count}${NC} attempt(s)"
}

# ════════════════════════════════════════════════════════════
#  Common helper: kill attempts
#  Args: $1=IDs to kill (newline-separated), $2=endpoint
# ════════════════════════════════════════════════════════════
do_kill() {
    local ids="$1" ep="$2"
    local fail_count=0
    while IFS= read -r id || [ -n "$id" ]; do
        [ -z "$id" ] && continue
        log -n "  attempt $id killing... "
        if "${DIGDAG_BIN[@]}" kill "$id" -e "$ep" >/dev/null 2>&1; then
            log "${GREEN}[OK]${NC}"
        else
            log "${RED}[FAIL]${NC}"
            ((fail_count++))
        fi
    done <<< "$ids"
    return $fail_count
}

# ════════════════════════════════════════════════════════════
#  Common helper: parse -p / -w options
#  Usage: parse_pw_opts "$@" -> sets project_name / workflow_name
# ════════════════════════════════════════════════════════════
parse_pw_opts() {
    project_name=""
    workflow_name=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -p|--project)  project_name="$2"; shift 2 ;;
            -w|--workflow) workflow_name="$2"; shift 2 ;;
            -*)
                log "${RED}[ERROR] Unknown option: $1${NC}"
                exit 1 ;;
            *)
                log "${RED}[ERROR] Unknown argument: $1${NC}"
                exit 1 ;;
        esac
    done
}

# ════════════════════════════════════════════════════════════
#  Common helper: select server -> stdout: port
#  Single server: auto-select
#  Multiple servers: numbered prompt (single selection)
#  Exit code: 0=OK 1=no server 2=canceled
# ════════════════════════════════════════════════════════════
select_server_port() {
    local all_pids
    all_pids=$(find_my_digdag_server_pid)

    if [ -z "$all_pids" ]; then
        log "${RED}[ERROR] No running Digdag server found.${NC}"
        return 1
    fi

    # Collect valid servers only
    local srv_pids=() srv_ports=() srv_runnings=()
    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        # Brief running project/workflow display (comma-separated)
        local _attempts_raw _blocks _running_ids
        local running_str="(none)"
        fetch_attempts "" "" "http://$HOST_NAME:$port" "running" 2>/dev/null
        if [ $? -eq 0 ] && [ -n "$_blocks" ]; then
            running_str=$(awk 'BEGIN{RS=""; ORS=","} /status: *running/ {proj=""; wf=""; n=split($0,a,"\n"); for(i=1;i<=n;i++){if(a[i]~/project:/) {split(a[i],b,": "); gsub(/^ +/,"",b[2]); proj=b[2]} if(a[i]~/workflow:/) {split(a[i],b,": "); gsub(/^ +/,"",b[2]); wf=b[2]}} if(proj!="") print proj"/"wf}' <<< "$_blocks" | sed 's/,$//')
        fi

        srv_pids+=("$pid")
        srv_ports+=("$port")
        srv_runnings+=("$running_str")
    done <<< "$all_pids"

    local count=${#srv_pids[@]}
    if [ "$count" -eq 0 ]; then
        log "${RED}[ERROR] No valid server found.${NC}"
        return 1
    fi

    # Single server: auto-select
    if [ "$count" -eq 1 ]; then
        log "  [OK] Auto-selected server (PID=${srv_pids[0]}, PORT=${srv_ports[0]})"
        echo "${srv_ports[0]}"
        return 0
    fi

    # Multiple servers: show table then select
    log ""
    log "  ${BOLD}Running server list (please select)${NC}"

    local w_no=3 w_pid=5 w_port=5 w_run=20
    local i
    for (( i=0; i<count; i++ )); do
        [ ${#srv_pids[$i]}     -gt $w_pid  ] && w_pid=${#srv_pids[$i]}
        [ ${#srv_ports[$i]}    -gt $w_port ] && w_port=${#srv_ports[$i]}
        [ ${#srv_runnings[$i]} -gt $w_run  ] && w_run=${#srv_runnings[$i]}
    done

    local sep
    sep=$(printf '  +-%s-+-%s-+-%s-+-%s-+' \
        "$(printf '%*s' $w_no   | tr ' ' '-')" \
        "$(printf '%*s' $w_pid  | tr ' ' '-')" \
        "$(printf '%*s' $w_port | tr ' ' '-')" \
        "$(printf '%*s' $w_run  | tr ' ' '-')")

    log "$sep"
    log "  $(printf "| %-${w_no}s | %-${w_pid}s | %-${w_port}s | %-${w_run}s |" "no." "PID" "PORT" "RUNNING")"
    log "$sep"
    for (( i=0; i<count; i++ )); do
        log "  $(printf "| %-${w_no}s | ${BOLD}%-${w_pid}s${NC} | ${CYAN}%-${w_port}s${NC} | %-${w_run}s |" \
            "$((i+1))" "${srv_pids[$i]}" "${srv_ports[$i]}" "${srv_runnings[$i]}")"
    done
    log "$sep"
    log ""

    echo -n "  Select server number (1-${count} / q=canceled): " >&2
    read -r ans
    case "$ans" in
        q|"")
            log "\n[INFO] Canceled."
            return 2 ;;
        *[!0-9]*)
            log "${RED}[ERROR] Please enter a number.${NC}"
            return 2 ;;
    esac
    if (( ans < 1 || ans > count )); then
        log "${RED}[ERROR] Number out of range. (1-${count})${NC}"
        return 2
    fi

    local idx=$(( ans - 1 ))
    log "  [OK] Server selected (PID=${srv_pids[$idx]}, PORT=${srv_ports[$idx]})"
    echo "${srv_ports[$idx]}"
    return 0
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: list_job
#
#  Usage:
#    digdag list_job [-p <project>] [-w <workflow>] [--all]
#
#  Default: show running status only
#  --all: show all statuses (success / error / running etc.)
# ════════════════════════════════════════════════════════════
cmd_list_job() {
    local show_all=false
    local project_name="" workflow_name=""

    # Extract --all first, delegate rest to parse_pw_opts
    local remaining=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) show_all=true; shift ;;
            *)     remaining+=("$1"); shift ;;
        esac
    done
    parse_pw_opts "${remaining[@]}"

    # ── Select server (single: auto / multiple: prompt) ──────────────
    local port
    port=$(select_server_port)
    local rc_srv=$?
    [ $rc_srv -eq 1 ] && exit 1
    [ $rc_srv -eq 2 ] && exit 0
    local endpoint="http://$HOST_NAME:$port"

    # Determine status filter
    local status_filter="running"
    $show_all && status_filter=""

    local condition_str=""
    [ -n "$project_name" ]  && condition_str+=" project=$project_name"
    [ -n "$workflow_name" ] && condition_str+=" workflow=$workflow_name"
    [ -z "$condition_str" ] && condition_str=" (all)"

    print_divider
    log "${BOLD}  [LIST] list_job${NC}"
    log "  Filter: ${CYAN}${condition_str}${NC}"
    log "  Status: ${CYAN}$( $show_all && echo 'all' || echo 'running' )${NC}"
    print_divider

    log "
[STEP 1] Fetching attempts..."

    local _attempts_raw _blocks _running_ids
    fetch_attempts "$project_name" "$workflow_name" "$endpoint" "$status_filter"
    local rc=$?

    if [ $rc -eq 1 ]; then
        log "${RED}[ERROR] Attempt fetch failed.${NC}"
        exit 1
    elif [ $rc -eq 2 ]; then
        log "${YELLOW}[INFO] No attempts match the given conditions.${NC}"
        log "  Filter:${condition_str}"
        exit 0
    fi

    log "
[RESULT] Attempt list:"
    log ""
    print_attempts_table
    log ""
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: kill_job
#
#  Usage:
#    digdag kill_job [--all] [-p <project>] [-w <workflow>]
#
#  Filter combinations:
#    -p -w both  : attempts matching project + workflow
#    -p only     : all attempts in the project
#    -w only     : all attempts with the workflow name
#    no options  : all running attempts on server
#
#  Without --all: show list then kill by selected ID
#  With --all: kill all matching attempts at once
# ════════════════════════════════════════════════════════════
cmd_kill_job() {
    local kill_all=false
    local project_name="" workflow_name=""

    # Extract --all first, delegate rest to parse_pw_opts
    local remaining=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) kill_all=true; shift ;;
            *)     remaining+=("$1"); shift ;;
        esac
    done
    parse_pw_opts "${remaining[@]}"

    # ── Select server (single: auto / multiple: prompt) ──────────────
    local port
    port=$(select_server_port)
    local rc_srv=$?
    [ $rc_srv -eq 1 ] && exit 1
    [ $rc_srv -eq 2 ] && exit 0
    local endpoint="http://$HOST_NAME:$port"

    local condition_str=""
    [ -n "$project_name" ]  && condition_str+=" project=$project_name"
    [ -n "$workflow_name" ] && condition_str+=" workflow=$workflow_name"
    [ -z "$condition_str" ] && condition_str=" (all)"

    print_divider
    log "${BOLD}  [KILL] kill_job start${NC}"
    log "  Filter: ${CYAN}${condition_str}${NC}"
    log "  Mode: ${CYAN}$( $kill_all && echo 'Kill all (--all)' || echo 'Selective kill' )${NC}"
    print_divider

    log "
[STEP 1] Fetching running attempts..."

    local _attempts_raw _blocks _running_ids
    fetch_attempts "$project_name" "$workflow_name" "$endpoint" "running"
    local rc=$?

    if [ $rc -eq 1 ]; then
        log "${RED}[ERROR] Attempt fetch failed.${NC}"
        exit 1
    elif [ $rc -eq 2 ]; then
        log "${YELLOW}[INFO] No running attempts match the given conditions.${NC}"
        log "  Filter:${condition_str}"
        exit 0
    fi

    log "
[STEP 2] Running attempt list:"
    log ""
    print_attempts_table
    log ""

    # ── Execute kill ─────────────────────────────────────────
    if $kill_all; then
        log "[STEP 3] Killing all..."
        do_kill "$_running_ids" "$endpoint"
        local result=$?
        log ""
        [ $result -eq 0 ]             && log "${GREEN}[DONE] All killed${NC}"             || log "${YELLOW}[WARN] Some failed: ${result}${NC}"

    else
        log "[STEP 3] Enter attempt ID(s) to kill."
        log "  (space-separated / 'all' = kill all / 'q' = cancel)"
        log ""
        echo -n "  Input> " >&2
        read -r user_input

        case "$user_input" in
            q|"")
                log "
[INFO] Canceled."
                exit 0 ;;
            all)
                user_input="$_running_ids" ;;
        esac

        local validated_ids=""
        for id in $user_input; do
            if ! echo "$_running_ids" | grep -qw "$id"; then
                log "  ${YELLOW}[SKIP] $id: Not in running state or does not exist${NC}"
                continue
            fi
            validated_ids+="$id"$'
'
        done

        if [ -z "$validated_ids" ]; then
            log "${YELLOW}[INFO] No valid IDs to kill.${NC}"
            exit 0
        fi

        do_kill "$validated_ids" "$endpoint"
        local result=$?
        log ""
        [ $result -eq 0 ]             && log "${GREEN}[DONE] Kill done${NC}"             || log "${YELLOW}[WARN] Some failed: ${result}${NC}"
    fi
    log ""
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: kill_server
#
#  Usage:
#    digdag kill_server
#
#  Behavior:
#    - Find and stop my digdag server processes
#    - Confirm then kill
#    - Clean up lock / info files
# ════════════════════════════════════════════════════════════
cmd_kill_server() {
    local kill_all=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) kill_all=true; shift ;;
            -*) log "${RED}[ERROR] Unknown option: $1${NC}"; exit 1 ;;
            *)  log "${RED}[ERROR] Unknown argument: $1${NC}";  exit 1 ;;
        esac
    done

    print_divider
    log "${BOLD}  [KILL_SERVER] Stop Digdag server${NC}"
    print_divider

    # ── Collect all running server PIDs ────────────────────────
    local all_pids_raw
    all_pids_raw=$(find_my_digdag_server_pid)

    if [ -z "$all_pids_raw" ]; then
        log "${YELLOW}[INFO] No running Digdag server found.${NC}"
        log ""
        exit 0
    fi

    # Collect valid servers (includes port check)
    local srv_pids=() srv_ports=() srv_urls=() srv_running_cells=()
    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        local url="http://$HOST_NAME:$port"
        local _attempts_raw _blocks _running_ids
        local cell="(none)"
        fetch_attempts "" "" "$url" "running"
        if [ $? -eq 0 ] && [ -n "$_blocks" ]; then
            local block="" proj_list=""
            while IFS= read -r line || [ -n "$line" ]; do
                if [ "$line" = "---BLOCK---" ]; then
                    [ -z "$block" ] && continue
                    local proj wf
                    proj=$(echo "$block" | awk -F': ' '/project:/  {gsub(/^ +/,"",$2); print $2}')
                    wf=$(  echo "$block" | awk -F': ' '/workflow:/  {gsub(/^ +/,"",$2); print $2}')
                    [ -n "$proj" ] && proj_list+="${proj}/${wf}"$'\n'
                    block=""
                else
                    block+="$line"$'\n'
                fi
            done <<< "$(awk 'BEGIN{RS=""; ORS="\n---BLOCK---\n"} {print}' <<< "$_blocks")"
            [ -n "$proj_list" ] && cell=$(echo "$proj_list" | sort -u | grep -v '^$')
        fi

        srv_pids+=("$pid")
        srv_ports+=("$port")
        srv_urls+=("$url")
        srv_running_cells+=("$cell")
    done <<< "$all_pids_raw"

    local server_count=${#srv_pids[@]}
    # pids / ports kept for backward compat
    local pids=("${srv_pids[@]}")
    local ports=("${srv_ports[@]}")

    if [ "$server_count" -eq 0 ]; then
        log "${YELLOW}[INFO] No valid server found.${NC}"
        log ""
        exit 0
    fi

    # ── Calculate column widths then print shared table ──────────────────
    local w_pid=5 w_port=5 w_url=25 w_running=20
    local i
    for (( i=0; i<server_count; i++ )); do
        [ ${#srv_pids[$i]}  -gt $w_pid     ] && w_pid=${#srv_pids[$i]}
        [ ${#srv_ports[$i]} -gt $w_port    ] && w_port=${#srv_ports[$i]}
        [ ${#srv_urls[$i]}  -gt $w_url     ] && w_url=${#srv_urls[$i]}
        while IFS= read -r rline || [ -n "$rline" ]; do
            [ ${#rline} -gt $w_running ] && w_running=${#rline}
        done <<< "${srv_running_cells[$i]}"
    done
    _print_server_table "$server_count"

    # --all option: stop all immediately
    if $kill_all; then
        log "[INFO] --all option -> killing all servers."
        local fail=0
        for (( i=0; i<server_count; i++ )); do
            _do_kill_server "${pids[$i]}" "${ports[$i]}" || ((fail++))
        done
        log ""
        [ $fail -eq 0 ]             && log "${GREEN}[DONE] All servers stopped${NC}"             || log "${YELLOW}[WARN] Some failed: ${fail}${NC}"
        log ""
        exit 0
    fi

    # Selection prompt (multi-select)
    log "  Enter server number(s) to stop."
    log "  (space-separated / 'all' = kill all / 'q' = cancel)"
    echo -n "  Input> " >&2
    read -r user_input

    case "$user_input" in
        q|"") log "
[INFO] Canceled."; exit 0 ;;
        all)
            local fail=0
            for (( i=0; i<server_count; i++ )); do
                _do_kill_server "${pids[$i]}" "${ports[$i]}" || ((fail++))
            done
            log ""
            [ $fail -eq 0 ]                 && log "${GREEN}[DONE] All servers stopped${NC}"                 || log "${YELLOW}[WARN] Some failed: ${fail}${NC}" ;;
        *)
            local fail=0
            for no in $user_input; do
                # Validate input
                if ! [[ "$no" =~ ^[0-9]+$ ]] || (( no < 1 || no > server_count )); then
                    log "  ${YELLOW}[SKIP] $no: Invalid number${NC}"
                    continue
                fi
                local idx=$(( no - 1 ))
                _do_kill_server "${pids[$idx]}" "${ports[$idx]}" || ((fail++))
            done
            log ""
            [ $fail -eq 0 ]                 && log "${GREEN}[DONE] Selected servers stopped${NC}"                 || log "${YELLOW}[WARN] Some failed: ${fail}${NC}" ;;
    esac
    log ""
}

# ── Common kill execution helper ──────────────────────────────────────
# Args: $1=pid $2=port
_do_kill_server() {
    local target_pid="$1" target_port="$2"
    log -n "  PID $target_pid (PORT $target_port) stopping... "
    if kill -- "-$target_pid" 2>/dev/null; then
        # --once server: clean up /tmp/digdag_$USER/once.* directory
        # start_server: clean up LOCK_FILE / INFO_FILE
        local _found_once=false
        for _od in "${DIGDAG_TMP_DIR}"/once.*/; do
            [ -d "$_od" ] || continue
            local _od_port
            _od_port=$(grep '^PORT=' "${_od}server.info" 2>/dev/null | cut -d= -f2)
            if [ "$_od_port" = "$target_port" ]; then
                rm -rf "$_od"
                _found_once=true
                break
            fi
        done
        $_found_once || rm -f "$LOCK_FILE" "$INFO_FILE"
        log "${GREEN}[OK]${NC}"
        return 0
    else
        log "${RED}[FAIL]${NC}"
        return 1
    fi
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: list_server
#
#  Usage:
#    digdag list_server
#
#  Behavior:
#    - Shows currently running server info (PORT / PID / URL / STARTED)
#    - Shows running project list per server (based on running attempts)
# ════════════════════════════════════════════════════════════
cmd_list_server() {
    print_divider
    log "${BOLD}  [LIST_SERVER] Server status${NC}"
    print_divider

    # ── Collect all running server PIDs ────────────────────────
    local all_pids
    all_pids=$(find_my_digdag_server_pid)

    if [ -z "$all_pids" ]; then
        log "${YELLOW}[INFO] No running Digdag server found.${NC}"
        log ""
        exit 0
    fi

    # ── Collect per-server info ─────────────────────────────────────
    local srv_pids=() srv_ports=() srv_urls=() srv_running_cells=()

    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        local url="http://$HOST_NAME:$port"

        # Fetch running attempts then build project/workflow list
        local endpoint="$url"
        local _attempts_raw _blocks _running_ids
        local cell="(none)"
        fetch_attempts "" "" "$endpoint" "running"
        if [ $? -eq 0 ] && [ -n "$_blocks" ]; then
            local block="" proj_list=""
            while IFS= read -r line || [ -n "$line" ]; do
                if [ "$line" = "---BLOCK---" ]; then
                    [ -z "$block" ] && continue
                    local proj wf
                    proj=$(echo "$block" | awk -F': ' '/project:/  {gsub(/^ +/,"",$2); print $2}')
                    wf=$(  echo "$block" | awk -F': ' '/workflow:/  {gsub(/^ +/,"",$2); print $2}')
                    [ -n "$proj" ] && proj_list+="${proj}/${wf}"$'\n'
                    block=""
                else
                    block+="$line"$'\n'
                fi
            done <<< "$(awk 'BEGIN{RS=""; ORS="\n---BLOCK---\n"} {print}' <<< "$_blocks")"
            [ -n "$proj_list" ] && cell=$(echo "$proj_list" | sort -u | grep -v '^$')

            # If single running: append attempt_id to URL
            local running_count
            running_count=$(echo "$_running_ids" | grep -c '[0-9]')
            if [ "$running_count" -eq 1 ]; then
                local single_aid
                single_aid=$(echo "$_running_ids" | grep -m1 '[0-9]')
                url="http://$HOST_NAME:$port/attempts/$single_aid"
            fi
        fi

        srv_pids+=("$pid")
        srv_ports+=("$port")
        srv_urls+=("$url")
        srv_running_cells+=("$cell")

    done <<< "$all_pids"

    local server_count=${#srv_pids[@]}
    if [ "$server_count" -eq 0 ]; then
        log "${YELLOW}[INFO] No valid server found.${NC}"
        log ""
        exit 0
    fi

    # ── Calculate column widths then print shared table ──────────────────
    local w_pid=5 w_port=5 w_url=25 w_running=20
    local i
    for (( i=0; i<server_count; i++ )); do
        [ ${#srv_pids[$i]}  -gt $w_pid     ] && w_pid=${#srv_pids[$i]}
        [ ${#srv_ports[$i]} -gt $w_port    ] && w_port=${#srv_ports[$i]}
        [ ${#srv_urls[$i]}  -gt $w_url     ] && w_url=${#srv_urls[$i]}
        while IFS= read -r rline || [ -n "$rline" ]; do
            [ ${#rline} -gt $w_running ] && w_running=${#rline}
        done <<< "${srv_running_cells[$i]}"
    done

    _print_server_table "$server_count"

    log "  total: ${BOLD}${server_count}${NC} server(s)"
    log ""
}


# ════════════════════════════════════════════════════════════
#  Internal helper: print server table (shared by list_server / kill_server)
#  Args: $1 = server_count
#  Uses arrays: srv_pids / srv_ports / srv_urls / srv_running_cells
# ════════════════════════════════════════════════════════════
_print_server_table() {
    local server_count="$1"
    local w_no=3
    local sep
    sep=$(printf '+-%s-+-%s-+-%s-+-%s-+-%s-+' \
        "$(printf '%*s' $w_no      | tr ' ' '-')" \
        "$(printf '%*s' $w_pid     | tr ' ' '-')" \
        "$(printf '%*s' $w_port    | tr ' ' '-')" \
        "$(printf '%*s' $w_url     | tr ' ' '-')" \
        "$(printf '%*s' $w_running | tr ' ' '-')")

    log ""
    log "$sep"
    log "$(printf "| %-${w_no}s | %-${w_pid}s | %-${w_port}s | %-${w_url}s | %-${w_running}s |" \
        "no." "PID" "PORT" "URL" "RUNNING (project/workflow)")"
    log "$sep"

    local i
    for (( i=0; i<server_count; i++ )); do
        local first_line=true
        local cell_data="${srv_running_cells[$i]}"
        [ -z "$cell_data" ] && cell_data="(none)"
        while IFS= read -r rline || [ -n "$rline" ]; do
            [ -z "$rline" ] && continue
            if [ "$first_line" = true ]; then
                log "$(printf "| %-${w_no}s | ${BOLD}%-${w_pid}s${NC} | ${CYAN}%-${w_port}s${NC} | %-${w_url}s | ${GREEN}%-${w_running}s${NC} |" \
                    "$((i+1))" "${srv_pids[$i]}" "${srv_ports[$i]}" "${srv_urls[$i]}" "$rline")"
                first_line=false
            else
                log "$(printf "| %-${w_no}s | %-${w_pid}s | %-${w_port}s | %-${w_url}s | ${GREEN}%-${w_running}s${NC} |" \
                    "" "" "" "" "$rline")"
            fi
        done <<< "$cell_data"
        log "$sep"
    done
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: browse
# ════════════════════════════════════════════════════════════
cmd_browse() {
    print_divider
    log "${BOLD}  [BROWSE] Open Digdag UI${NC}"
    print_divider

    # Select server (single: auto / multiple: by number)
    local port
    port=$(select_server_port)
    local rc=$?
    [ $rc -eq 1 ] && exit 1
    [ $rc -eq 2 ] && exit 0

    local url="http://$HOST_NAME:$port"
    log "\n  URL: ${BOLD}${CYAN}$url${NC}"

    # Try xdg-open first (Linux default browser), fallback to firefox
    if command -v xdg-open >/dev/null 2>&1; then
        log "  [WEB] Opening with default browser..."
        xdg-open "$url" >/dev/null 2>&1 &
    elif command -v firefox >/dev/null 2>&1; then
        log "  [WEB] Opening with Firefox..."
        firefox "$url" >/dev/null 2>&1 &
    else
        log "${RED}[ERROR] No browser command found.${NC}"
        log "  Open manually: ${CYAN}$url${NC}"
        exit 1
    fi
    log "  ${GREEN}[OK] Browser launched${NC}"
    log ""
}

# ════════════════════════════════════════════════════════════
#  Custom subcommand: start_server
#
#  Usage:
#    digdag start_server
#
#  Behavior:
#    - Only one server per user
#    - Reuses existing server or boots new one
#    - Multiple servers only allowed via run_workflow --once (disposable mode).
# ════════════════════════════════════════════════════════════
cmd_start_server() {
    print_divider
    log "${BOLD}  [START] start_server${NC}"
    log "  User   : ${CYAN}$USER_NAME${NC}"
    print_divider

    local port
    if port=$(check_server_alive); then
        log "\n${GREEN}[OK] Reusing already running server.${NC}"
    else
        log "\n[INFO] No server found -> auto-booting."
        if ! port=$(start_server); then
            log "${RED}[ERROR] Server boot failed.${NC}"
            exit 1
        fi
        log "${GREEN}[OK] Server boot complete${NC}"
    fi

    log ""
    print_divider
    log "${GREEN}${BOLD}[DONE] Server ready!${NC}"
    log "  PORT : ${BOLD}$port${NC}"
    log "  URL  : ${BOLD}http://$HOST_NAME:$port${NC}"
    print_divider
    log ""
}

# ════════════════════════════════════════════════════════════
#  Main: subcommand dispatch
# ════════════════════════════════════════════════════════════
SUBCOMMAND="$1"

case "$SUBCOMMAND" in
    start_server)
        cmd_start_server
        ;;
    list_job)
        shift
        cmd_list_job "$@"
        ;;
    kill_job)
        shift
        cmd_kill_job "$@"
        ;;
    run_workflow)
        shift
        cmd_run_workflow "$@"
        ;;
    list_server)
        cmd_list_server
        ;;
    kill_server)
        shift
        cmd_kill_server "$@"
        ;;
    browse)
        cmd_browse
        ;;
    "")
        "${DIGDAG_BIN[@]}"
        ;;
    *)
        "${DIGDAG_BIN[@]}" "$@"
        ;;
esac
