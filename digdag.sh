#!/bin/bash
# ============================================================
#  digdag.sh  (wrapper)
#  기존 digdag 명령어는 그대로 동작하고,
#  커스텀 서브커맨드 "run_workflow", "browse" 를 추가합니다.
#
#  사용법:
#    digdag run workflow.dig          # 기존 명령 그대로
#    digdag server                    # 기존 명령 그대로
#    digdag run_workflow <project_name> <workflow_name>
#    digdag run_workflow --project <dir> [-P <file>] <project_name> <workflow_name>
#    digdag browse                    # 기본 브라우저로 UI 열기
#
#  run_workflow 동작 순서:
#    1. 내 Digdag 서버 확인 (http://<hostname>:<port>)
#    2. 없으면 서버 자동 기동 (Race Condition 방지 lock 포함)
#    3. digdag push <project_name> -e http://<hostname>:<port>
#    4. digdag start <project_name> <workflow_name> --session now
#
#  Race Condition 방지:
#    - 하나의 계정은 하나의 서버만 사용
#    - 최초 프로세스만 서버 기동, 후발 프로세스는 대기 후 재사용
#    - lock 파일 생명주기 = digdag server 프로세스 생명주기
#      (kill -0 폴링 감시 프로세스가 server 종료 시 lock 자동 삭제)
# ============================================================

# ── 설정 ────────────────────────────────────────────────────
BASE_PORT=65432
MAX_RETRIES=10
BOOT_TIMEOUT=15
LOCK_TIMEOUT=60            # 후발 프로세스 최대 대기 시간 (초)
USER_NAME=$(id -un)
HOST_NAME=$(hostname)
WORK_DIR="$(pwd)"
# 각 서버(컴퓨트팜)의 로컬 /tmp 사용
#  - 홈디렉토리 용량 절약 (NFS 공유 홈 1GiB 제한 대응)
#  - 서버별 독립 공간 확보 (LSF 멀티 컴퓨트팜 환경)
#  - 재부팋 시 자연 소멸 (서버도 같이 죽으므로 문제 없음)
DIGDAG_TMP_DIR="/tmp/digdag_${USER_NAME}"
DIGDAG_JVM_TMP="${DIGDAG_TMP_DIR}/jvm-tmp"  # JVM 임시 디렉토리 (digdag 내부 tempdir 생성 위치 고정)
LOG_FILE="${DIGDAG_TMP_DIR}/server.log"
TASK_LOG_DIR="${DIGDAG_TMP_DIR}/task-logs"
INFO_FILE="${DIGDAG_TMP_DIR}/server.info"
LOCK_FILE="${DIGDAG_TMP_DIR}/server.lock"
DIGDAG_JAR="/user/aaa/usr/local/bin/digdag-0.10.5.1.jar"  # 버전 변경 시 이 경로만 수정
# ────────────────────────────────────────────────────────────

# ── 색상 정의 ────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'
# ────────────────────────────────────────────────────────────

# ── 공통 헬퍼 ────────────────────────────────────────────────
log() { echo -e "$@" >&2; }
print_divider() { log "${CYAN}----------------------------------------------------${NC}"; }

# 포트 사용 여부 (순수 Bash /dev/tcp)
port_in_use() {
    (echo > /dev/tcp/$HOST_NAME/$1) >/dev/null 2>&1
}

# 내 계정의 "digdag server" 프로세스만 탐색
# jar 파일명 기준으로 탐색하여 정확도 향상 (run/push/start 등 제외)
find_my_digdag_server_pid() {
    local jar_name
    jar_name=$(basename "$DIGDAG_JAR")
    ps -u "$USER_NAME" -f 2>/dev/null | awk -v jar="$jar_name" '$0 ~ jar && /server/ && !/run/ && !/push/ && !/start/ && !/retry/ && !/kill/ && !/check/ { print $2 }' | head -1
}

# PID -> 포트 추출
find_port_by_pid() {
    ss -tlnp 2>/dev/null \
        | grep "pid=$1," \
        | awk '{print $4}' \
        | awk -F':' '{print $NF}' \
        | head -1
}
# ── 보안 1: DIGDAG_JAR 유효성 확인 및 실행 명령 구성 ────────
# jar 방식으로 직접 지정하므로 which 탐색 불필요
# java -jar <jar> 형태로 실행하여 버전을 명확히 고정

if [ ! -f "$DIGDAG_JAR" ]; then
    log "${RED}[ERROR] DIGDAG_JAR 파일을 찾을 수 없습니다: $DIGDAG_JAR${NC}"
    exit 1
fi

# java 존재 여부 확인
if ! command -v java >/dev/null 2>&1; then
    log "${RED}[ERROR] java 명령어를 찾을 수 없습니다. JDK/JRE 설치를 확인하세요.${NC}"
    exit 1
fi

# DIGDAG_BIN: 이후 코드에서 digdag 실행 시 이 변수를 사용
# 사용법: "${DIGDAG_BIN[@]}" server / push / start ...
DIGDAG_BIN=(java -Djava.io.tmpdir="$DIGDAG_JVM_TMP" -jar "$DIGDAG_JAR")
# ────────────────────────────────────────────────────────────

# ── 보안 2: 중단 시 고아 프로세스 정리 (INT TERM만) ──────────
BOOTING_PID=""
BOOT_SUCCESS=false

cleanup_on_exit() {
    if [ -n "$BOOTING_PID" ] && ! $BOOT_SUCCESS; then
        log "\n${YELLOW}[WARN] 실행이 중단되었습니다. 기동 중이던 서버를 정리합니다. (PID: $BOOTING_PID)${NC}"
        kill -9 "$BOOTING_PID" 2>/dev/null
        rm -f "$LOCK_FILE"
    fi
}
trap cleanup_on_exit INT TERM
# ────────────────────────────────────────────────────────────

# ── 작업 디렉토리 초기화 (스크립트 진입 시 항상 수행) ────────────
# run/push 등 서버 비사용 커맨드에서도 DIGDAG_JVM_TMP 가 필요
mkdir -p "$DIGDAG_TMP_DIR" "$TASK_LOG_DIR" "$DIGDAG_JVM_TMP"
chmod 700 "$DIGDAG_TMP_DIR" "$TASK_LOG_DIR" "$DIGDAG_JVM_TMP"
# ────────────────────────────────────────────────────────────

# ── 서버 alive 체크 -> stdout: 포트 / 종료코드 0=OK 1=없음 ───
check_server_alive() {
    local pid port

    pid=$(find_my_digdag_server_pid)
    [ -z "$pid" ] && return 1

    port=$(find_port_by_pid "$pid")
    [ -z "$port" ] && return 1

    port_in_use "$port" || return 1

    echo "$port"
    return 0
}

# ── 서버 기동 -> stdout: 포트 / 종료코드 0=OK 1=실패 ─────────
#
#  Race Condition 방지 설계:
#   - noclobber(set -C) 로 lock 파일을 atomic 하게 생성
#     -> 동시 실행 시 최초 프로세스만 lock 획득 성공
#   - 후발 프로세스는 lock 해제될 때까지 대기 후 서버 재확인
#   - kill -0 폴링 감시 프로세스가 server PID 종료를 감지하면 lock 자동 삭제
#     -> lock 파일 생명주기 = digdag server 프로세스 생명주기
#
start_server() {
    # server.log를 PID 기반 파일명으로 저장 (server.log.<PID>)
    # 로테이션 없이 자연스럽게 누적, /tmp 특성상 재부팅 시 자동 소멸
    LOG_FILE="${DIGDAG_TMP_DIR}/server.log.$$"

    # ── Lock 획득 시도 (atomic: noclobber) ──────────────────
    if (set -C; echo $$ > "$LOCK_FILE") 2>/dev/null; then

        # ── 최초 프로세스: lock 획득 성공 -> 서버 기동 ─────
        log "  [LOCK] Lock 획득 -> 서버 기동을 시작합니다. (PID: $$)"

        local port=$BASE_PORT

        for (( i=1; i<=MAX_RETRIES; i++ )); do
            log "  ${YELLOW}[시도 $i/$MAX_RETRIES]${NC} 포트 ${BOLD}$port${NC} 확인 중..."

            if port_in_use $port; then
                log "  [ERROR] 포트 $port 사용 중 -> 다음 포트"
                ((port++)); continue
            fi

            log "  [OK] 포트 $port 사용 가능 -> 서버 기동 중..."

            # setsid: 새 세션으로 분리 → 부모 종료 시 SIGHUP 전달 안됨
            # disown: bash job table 에서도 제거 → 완전히 독립
            setsid "${DIGDAG_BIN[@]}" server \
                --bind 0.0.0.0 \
                --port $port \
                --memory \
                --task-log "$TASK_LOG_DIR" \
                > "$LOG_FILE" 2>&1 &

            BOOTING_PID=$!
            disown $BOOTING_PID

            # polling: 실제 포트가 열릴 때까지 대기
            log -n "  [WAIT] 기동 대기 중 "
            for (( j=1; j<=BOOT_TIMEOUT; j++ )); do
                sleep 1; log -n "."
                ! kill -0 "$BOOTING_PID" 2>/dev/null && { log " 프로세스 종료 감지"; break; }
                if port_in_use $port; then
                    log " 완료! (${j}초)"
                    BOOT_SUCCESS=true; break
                fi
            done

            if $BOOT_SUCCESS; then
                # 보안 3: server.info 파일 권한 보호 (본인만 읽기/쓰기)
                cat > "$INFO_FILE" <<EOF
PORT=$port
PID=$BOOTING_PID
URL=http://$HOST_NAME:$port
STARTED=$(date '+%Y-%m-%d %H:%M:%S')
EOF
                chmod 600 "$INFO_FILE"

                # ── 감시 프로세스 ────────────────────────────
                # kill -0 폴링으로 server PID 종료를 감지하면 lock 자동 삭제
                # lock 파일 생명주기 = digdag server 프로세스 생명주기
                (
                    exec >/dev/null 2>&1
                    local watch_port="$port"
                    local watch_lock="$LOCK_FILE"
                    local watch_info="$INFO_FILE"
                    local jar_name
                    jar_name=$(basename "$DIGDAG_JAR")
                    # PID 재사용 문제 방지: find_my_digdag_server_pid 로 실제 서버 존재 확인
                    # port 가 닫히면 서버가 죽은 것으로 판단 (가장 확실한 기준)
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

            log "  [WARN] 기동 실패 -> 다음 포트로"
            kill -9 "$BOOTING_PID" 2>/dev/null
            BOOTING_PID=""
            ((port++))
        done

        # 모든 시도 실패 시 lock 직접 삭제
        rm -f "$LOCK_FILE"
        log "${RED}[ERROR] 서버 기동 실패 (${MAX_RETRIES}회 시도)${NC}"
        log "   로그 확인: $LOG_FILE"
        tail -n 10 "$LOG_FILE" >&2
        return 1

    else

        # ── 후발 프로세스: 대기 후 서버 재확인 ─────────────
        local lock_pid
        lock_pid=$(cat "$LOCK_FILE" 2>/dev/null)
        log "  [WAIT] 다른 프로세스(PID: $lock_pid)가 서버 기동 중입니다. 대기합니다..."

        for (( t=1; t<=LOCK_TIMEOUT; t++ )); do
            sleep 1
            log -n "."

            # lock 건 프로세스가 죽었으면 -> lock 해제 후 재시도
            if [ -n "$lock_pid" ] && ! kill -0 "$lock_pid" 2>/dev/null; then
                log "\n  [WARN] 기동 프로세스 종료 감지 -> 재시도합니다."
                rm -f "$LOCK_FILE"
                start_server
                return $?
            fi

            # 서버가 정상적으로 떴는지 확인
            local port
            if port=$(check_server_alive); then
                log "\n  ${GREEN}[OK] 서버 기동 완료 확인 (PORT: $port)${NC}"
                echo "$port"
                return 0
            fi
        done

        log "\n${RED}[ERROR] 서버 기동 대기 시간 초과 (${LOCK_TIMEOUT}초)${NC}"
        return 1
    fi
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: run_workflow
# ════════════════════════════════════════════════════════════
cmd_run_workflow() {
    local project_dir="$WORK_DIR"
    local project_name=""
    local workflow_name=""
    local params_file=""       # -P / --params-file (선택)

    # 보안 4: 엄격한 파라미터 파싱 - 알 수 없는 옵션 차단
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --project|-d)
                project_dir="$2"; shift 2 ;;
            --params-file|-P)
                params_file="$2"; shift 2 ;;
            -*)
                log "${RED}[ERROR] 알 수 없는 옵션입니다: $1${NC}"
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

    # ── 필수 인자 검증 ───────────────────────────────────────
    local has_error=false

    if [ -z "$project_name" ] || [ -z "$workflow_name" ]; then
        has_error=true
    fi


    # params-file 지정 시 파일 존재 여부 확인
    if [ -n "$params_file" ] && [ ! -f "$params_file" ]; then
        log "${RED}[ERROR] --params-file 파일을 찾을 수 없습니다: $params_file${NC}"
        has_error=true
    fi

    if $has_error; then
        log ""
        log "${RED}사용법: digdag run_workflow [옵션] <project_name> <workflow_name>${NC}"
        log ""
        log "  [필수]"
        log "  project_name          : Digdag에 등록할 프로젝트 이름"
        log "  workflow_name         : 실행할 워크플로우 이름 (.dig 파일명)"
        log ""
        log "  [선택]"
        log "  --project, -d <dir>   : 프로젝트 디렉토리 (기본값: 현재 디렉토리)"
        log "  --params-file, -P <file> : 외부 파라미터 파일 경로"
        log ""
        log "  예시) digdag run_workflow --project /path/to/proj -P params.yml my_project etl_workflow"
        log ""
        exit 1
    fi

    print_divider
    log "${BOLD}  [START] run_workflow 시작${NC}"
    log "  프로젝트 디렉토리: ${CYAN}$project_dir${NC}"
    log "  프로젝트 이름    : ${CYAN}$project_name${NC}"
    log "  워크플로우 이름  : ${CYAN}$workflow_name${NC}"
    [ -n "$params_file" ] && log "  파라미터 파일    : ${CYAN}$params_file${NC}"
    print_divider

    # ── STEP 1. 서버 확인 또는 기동 ─────────────────────────
    log "\n${YELLOW}[STEP 1]${NC} Digdag 서버 확인 중..."

    local port
    if port=$(check_server_alive); then
        log "${GREEN}[OK] 기존 서버 사용 (PORT: $port)${NC}"
    else
        log "  서버 없음 -> 자동 기동합니다."
        if ! port=$(start_server); then
            log "${RED}[ERROR] 서버 기동 실패. run_workflow 를 중단합니다.${NC}"
            exit 1
        fi
        log "${GREEN}[OK] 서버 기동 완료 (PORT: $port)${NC}"
    fi

    local endpoint="http://$HOST_NAME:$port"

    # ── STEP 2. Push ─────────────────────────────────────────
    log "\n${YELLOW}[STEP 2]${NC} 프로젝트 Push 중..."
    log "  $ digdag push $project_name -e $endpoint --project $project_dir"

    "${DIGDAG_BIN[@]}" push "$project_name" \
        -e "$endpoint" \
        --project "$project_dir"

    if [ $? -ne 0 ]; then
        log "${RED}[ERROR] Push 실패. run_workflow 를 중단합니다.${NC}"
        exit 1
    fi
    log "${GREEN}[OK] Push 완료${NC}"

    # ── STEP 3. Start ────────────────────────────────────────
    log "\n${YELLOW}[STEP 3]${NC} 워크플로우 Start 중..."

    # start 명령 구성 (params-file 은 선택적으로 추가)
    local start_cmd
    start_cmd=(
        "${DIGDAG_BIN[@]}" start "$project_name" "$workflow_name"
        -e "$endpoint"
        --session now
    )
    [ -n "$params_file" ] && start_cmd+=(--params-file "$params_file")

    log "  $ ${start_cmd[*]}"
    "${start_cmd[@]}"

    if [ $? -ne 0 ]; then
        log "${RED}[ERROR] Start 실패.${NC}"
        exit 1
    fi

    log ""
    print_divider
    log "${GREEN}${BOLD}[DONE] run_workflow 완료!${NC}"
    log "  프로젝트  : $project_name"
    log "  워크플로우: $workflow_name"
    log "  URL       : $endpoint"
    print_divider
    log ""
}


# ════════════════════════════════════════════════════════════
#  공통 헬퍼: attempts 조회
#
#  인자: $1=project, $2=workflow, $3=endpoint, $4=status_filter("running"|"")
#  호출 후 아래 변수가 설정됨:
#    _attempts_raw  : digdag attempts 원본 출력
#    _blocks        : 조건에 맞는 블록 목록 (빈줄 구분)
#    _running_ids   : status=running 인 attempt id 목록 (kill 용)
# ════════════════════════════════════════════════════════════
fetch_attempts() {
    local proj="$1" wf="$2" ep="$3" status_filter="$4"

    local cmd=("${DIGDAG_BIN[@]}" attempts -e "$ep")
    [ -n "$proj" ] && cmd+=(--project "$proj")

    _attempts_raw=$("${cmd[@]}" 2>/dev/null) || return 1

    # 빈줄 기준 블록 분리 후 조건 필터링 (들여쓰기 2공백 대응)
    # NOTE: env 방식으로 awk 변수 전달 (특수문자 이슈 방지)
    _blocks=$(sf="$status_filter" wf="$wf" awk 'BEGIN{RS=""; ORS="\n\n"} (ENVIRON["sf"]=="" || $0 ~ "status: *"ENVIRON["sf"]) && (ENVIRON["wf"]=="" || $0 ~ "workflow: *"ENVIRON["wf"]) {print}' <<< "$_attempts_raw")

    # running attempt id 추출 (kill 용) - 블록 단위로 안정적으로 추출
    _running_ids=$(awk 'BEGIN{RS=""; ORS="\n"} /status: *running/ {match($0, /attempt id: *([0-9]+)/, a); if(a[1]!="") print a[1]}' <<< "$_blocks" | grep -v "^$")

    [ -z "$_blocks" ] && return 2
    return 0
}

# ════════════════════════════════════════════════════════════
#  공통 헬퍼: attempts 테이블 출력
#
#  표시 컬럼: project | workflow | session id | attempt id
#             created at | finished at | status
#  주의: digdag attempts 출력은 각 필드가 2공백 들여쓰기됨
# ════════════════════════════════════════════════════════════
print_attempts_table() {
    # 블록에서 필드 추출하여 배열에 저장
    local projects=() workflows=() session_ids=() attempt_ids=()
    local created_ats=() finished_ats=() statuses=()

    # 블록 구분자 삽입 후 순회
    # gsub로 앞 공백 제거 후 $2 추출
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

    # 각 컬럼 최대 너비 계산 (헤더 포함)
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

    # 구분선 생성
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

    # 헤더 출력
    log "$sep"
    log "$(printf "| %-${w_proj}s | %-${w_wf}s | %-${w_sid}s | %-${w_aid}s | %-${w_cat}s | %-${w_fat}s | %-${w_st}s |" \
        "project" "workflow" "session id" "attempt id" "created at" "finished at" "status")"
    log "$sep"

    # 데이터 행 출력 (status 별 색상)
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
#  공통 헬퍼: attempt kill 실행
#  인자: $1=kill 할 ID 목록(줄바꿈 구분), $2=endpoint
# ════════════════════════════════════════════════════════════
do_kill() {
    local ids="$1" ep="$2"
    local fail_count=0
    while IFS= read -r id || [ -n "$id" ]; do
        [ -z "$id" ] && continue
        log -n "  attempt $id kill 중... "
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
#  공통: -p / -w 옵션 파싱 헬퍼
#  사용법: parse_pw_opts "$@"  → project_name / workflow_name 설정
# ════════════════════════════════════════════════════════════
parse_pw_opts() {
    project_name=""
    workflow_name=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -p|--project)  project_name="$2"; shift 2 ;;
            -w|--workflow) workflow_name="$2"; shift 2 ;;
            -*)
                log "${RED}[ERROR] 알 수 없는 옵션입니다: $1${NC}"
                exit 1 ;;
            *)
                log "${RED}[ERROR] 알 수 없는 인자입니다: $1${NC}"
                exit 1 ;;
        esac
    done
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: list_job
#
#  사용법:
#    digdag list_job [-p <project>] [-w <workflow>] [--all]
#
#  기본: running 상태만 표시
#  --all: 모든 status 표시 (success / error / running 등)
# ════════════════════════════════════════════════════════════
cmd_list_job() {
    local show_all=false
    local project_name="" workflow_name=""

    # --all 먼저 추출 후 나머지는 parse_pw_opts 에 위임
    local remaining=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) show_all=true; shift ;;
            *)     remaining+=("$1"); shift ;;
        esac
    done
    parse_pw_opts "${remaining[@]}"

    # ── 서버 확인 ────────────────────────────────────────────
    local port
    if ! port=$(check_server_alive); then
        log "${RED}[ERROR] 실행 중인 Digdag 서버가 없습니다.${NC}"
        exit 1
    fi
    local endpoint="http://$HOST_NAME:$port"

    # 상태 필터 결정
    local status_filter="running"
    $show_all && status_filter=""

    local condition_str=""
    [ -n "$project_name" ]  && condition_str+=" project=$project_name"
    [ -n "$workflow_name" ] && condition_str+=" workflow=$workflow_name"
    [ -z "$condition_str" ] && condition_str=" (전체)"

    print_divider
    log "${BOLD}  [LIST] list_job${NC}"
    log "  조건  : ${CYAN}${condition_str}${NC}"
    log "  상태  : ${CYAN}$( $show_all && echo '전체' || echo 'running' )${NC}"
    print_divider

    log "
[STEP 1] attempt 조회 중..."

    local _attempts_raw _blocks _running_ids
    fetch_attempts "$project_name" "$workflow_name" "$endpoint" "$status_filter"
    local rc=$?

    if [ $rc -eq 1 ]; then
        log "${RED}[ERROR] attempt 조회 실패.${NC}"
        exit 1
    elif [ $rc -eq 2 ]; then
        log "${YELLOW}[INFO] 조건에 맞는 attempt 가 없습니다.${NC}"
        log "  조건:${condition_str}"
        exit 0
    fi

    log "
[RESULT] attempt 목록:"
    log ""
    print_attempts_table
    log ""
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: kill_job
#
#  사용법:
#    digdag kill_job [--all] [-p <project>] [-w <workflow>]
#
#  조건 조합:
#    -p -w 둘 다   : project + workflow 일치하는 attempts
#    -p 만          : 해당 project 의 모든 attempts
#    -w 만          : 해당 workflow 이름의 모든 attempts
#    옵션 없음      : 서버의 모든 running attempts
#
#  --all 없으면: 목록 출력 후 ID 선택하여 개별 kill
#  --all 있으면: 조건에 맞는 모든 attempt 를 한번에 kill
# ════════════════════════════════════════════════════════════
cmd_kill_job() {
    local kill_all=false
    local project_name="" workflow_name=""

    # --all 먼저 추출 후 나머지는 parse_pw_opts 에 위임
    local remaining=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) kill_all=true; shift ;;
            *)     remaining+=("$1"); shift ;;
        esac
    done
    parse_pw_opts "${remaining[@]}"

    # ── 서버 확인 ────────────────────────────────────────────
    local port
    if ! port=$(check_server_alive); then
        log "${RED}[ERROR] 실행 중인 Digdag 서버가 없습니다.${NC}"
        exit 1
    fi
    local endpoint="http://$HOST_NAME:$port"

    local condition_str=""
    [ -n "$project_name" ]  && condition_str+=" project=$project_name"
    [ -n "$workflow_name" ] && condition_str+=" workflow=$workflow_name"
    [ -z "$condition_str" ] && condition_str=" (전체)"

    print_divider
    log "${BOLD}  [KILL] kill_job 시작${NC}"
    log "  조건: ${CYAN}${condition_str}${NC}"
    log "  모드: ${CYAN}$( $kill_all && echo '전체 kill (--all)' || echo '선택 kill' )${NC}"
    print_divider

    log "
[STEP 1] running 상태 attempt 조회 중..."

    local _attempts_raw _blocks _running_ids
    fetch_attempts "$project_name" "$workflow_name" "$endpoint" "running"
    local rc=$?

    if [ $rc -eq 1 ]; then
        log "${RED}[ERROR] attempt 조회 실패.${NC}"
        exit 1
    elif [ $rc -eq 2 ]; then
        log "${YELLOW}[INFO] 조건에 맞는 실행 중인 attempt 가 없습니다.${NC}"
        log "  조건:${condition_str}"
        exit 0
    fi

    log "
[STEP 2] 실행 중인 attempt 목록:"
    log ""
    print_attempts_table
    log ""

    # ── kill 실행 ────────────────────────────────────────────
    if $kill_all; then
        log "[STEP 3] 전체 kill 실행 중..."
        do_kill "$_running_ids" "$endpoint"
        local result=$?
        log ""
        [ $result -eq 0 ]             && log "${GREEN}[DONE] 전체 kill 완료${NC}"             || log "${YELLOW}[WARN] 일부 실패: ${result}건${NC}"

    else
        log "[STEP 3] kill 할 attempt ID 를 입력하세요."
        log "  (여러 개는 공백으로 구분 / 'all' = 전체 / 'q' = 취소)"
        log ""
        echo -n "  입력> " >&2
        read -r user_input

        case "$user_input" in
            q|"")
                log "
[INFO] 취소되었습니다."
                exit 0 ;;
            all)
                user_input="$_running_ids" ;;
        esac

        local validated_ids=""
        for id in $user_input; do
            if ! echo "$_running_ids" | grep -qw "$id"; then
                log "  ${YELLOW}[SKIP] $id: running 상태가 아니거나 존재하지 않는 ID${NC}"
                continue
            fi
            validated_ids+="$id"$'
'
        done

        if [ -z "$validated_ids" ]; then
            log "${YELLOW}[INFO] kill 할 유효한 ID 가 없습니다.${NC}"
            exit 0
        fi

        do_kill "$validated_ids" "$endpoint"
        local result=$?
        log ""
        [ $result -eq 0 ]             && log "${GREEN}[DONE] kill 완료${NC}"             || log "${YELLOW}[WARN] 일부 실패: ${result}건${NC}"
    fi
    log ""
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: kill_server
#
#  사용법:
#    digdag kill_server
#
#  동작:
#    - 내 계정의 digdag server 프로세스를 찾아 종료
#    - 확인 프롬프트 후 kill
#    - lock / info 파일 정리
# ════════════════════════════════════════════════════════════
cmd_kill_server() {
    print_divider
    log "${BOLD}  [KILL_SERVER] 내 Digdag 서버 종료${NC}"
    print_divider

    # ── 서버 프로세스 확인 ───────────────────────────────────
    local pid
    pid=$(find_my_digdag_server_pid)

    if [ -z "$pid" ]; then
        log "${YELLOW}[INFO] 실행 중인 Digdag 서버가 없습니다.${NC}"
        exit 0
    fi

    # server.info 에서 포트/URL 정보 표시
    local port url
    port=$(grep '^PORT=' "$INFO_FILE" 2>/dev/null | cut -d= -f2)
    url=$(grep '^URL='  "$INFO_FILE" 2>/dev/null | cut -d= -f2)

    log ""
    log "  PID  : ${BOLD}$pid${NC}"
    [ -n "$port" ] && log "  PORT : $port"
    [ -n "$url"  ] && log "  URL  : $url"
    log ""

    # ── 확인 프롬프트 ────────────────────────────────────────
    echo -n "  이 서버를 종료하시겠습니까? [y/N] " >&2
    read -r answer
    case "$answer" in
        y|Y) ;;
        *)
            log "
[INFO] 취소되었습니다."
            exit 0 ;;
    esac

    # ── kill ─────────────────────────────────────────────────
    log "
  PID $pid 종료 중..."
    if kill "$pid" 2>/dev/null; then
        # 포트가 닫힐 때까지 대기 (최대 10초)
        local i
        for (( i=1; i<=10; i++ )); do
            sleep 1
            ! port_in_use "$port" && break
        done
        # lock/info 파일 정리 (감시 프로세스가 못 지운 경우 대비)
        rm -f "$LOCK_FILE" "$INFO_FILE"
        log "${GREEN}[DONE] 서버가 종료되었습니다.${NC}"
    else
        log "${RED}[ERROR] kill 실패. 이미 종료되었거나 권한이 없습니다.${NC}"
        exit 1
    fi
    log ""
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: browse
# ════════════════════════════════════════════════════════════
cmd_browse() {
    log ""
    log "${YELLOW}[browse]${NC} Digdag 서버 상태 확인 중..."

    local port
    if ! port=$(check_server_alive); then
        log "${RED}[ERROR] 실행 중인 Digdag 서버가 없습니다.${NC}"
        log "   먼저 다음 명령어로 서버를 시작하세요:"
        log "   ${CYAN}digdag run_workflow <project_name> <workflow_name>${NC}"
        log ""
        exit 1
    fi

    local url="http://$HOST_NAME:$port"
    log "${GREEN}[OK] 서버 확인 완료 (PORT: $port)${NC}"

    # xdg-open 우선 (Linux 기본 브라우저), 없으면 firefox 폴백
    if command -v xdg-open >/dev/null 2>&1; then
        log "  [WEB] 기본 브라우저로 접속합니다: ${CYAN}$url${NC}"
        xdg-open "$url" >/dev/null 2>&1 &
    elif command -v firefox >/dev/null 2>&1; then
        log "  [WEB] Firefox 로 접속합니다: ${CYAN}$url${NC}"
        firefox "$url" >/dev/null 2>&1 &
    else
        log "${RED}[ERROR] 브라우저 실행 명령어를 찾을 수 없습니다.${NC}"
        log "   직접 접속하세요: ${CYAN}$url${NC}"
        exit 1
    fi
    log ""
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: start_server
#  서버만 띄우는 명령어 (push/start 없이 서버 기동만)
# ════════════════════════════════════════════════════════════
cmd_start_server() {
    print_divider
    log "${BOLD}  [START] start_server 시작${NC}"
    log "  사용자: ${CYAN}$USER_NAME${NC}"
    print_divider

    log "\n${YELLOW}[STEP 1]${NC} Digdag 서버 확인 중..."

    local port
    if port=$(check_server_alive); then
        log "${GREEN}[OK] 이미 실행 중인 서버가 있습니다.${NC}"
    else
        log "  서버 없음 -> 자동 기동합니다."
        if ! port=$(start_server); then
            log "${RED}[ERROR] 서버 기동 실패.${NC}"
            exit 1
        fi
        log "${GREEN}[OK] 서버 기동 완료${NC}"
    fi

    log ""
    print_divider
    log "${GREEN}${BOLD}[DONE] 서버 준비 완료!${NC}"
    log "  PORT : ${BOLD}$port${NC}"
    log "  URL  : ${BOLD}http://$HOST_NAME:$port${NC}"
    print_divider
    log ""
}

# ════════════════════════════════════════════════════════════
#  메인: 서브커맨드 분기
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
    kill_server)
        cmd_kill_server
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
