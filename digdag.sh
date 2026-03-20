#!/bin/bash
# ============================================================
#  digdag.sh  (wrapper)
#
#  기존 digdag 명령어는 그대로 동작하며, 아래 커스텀 커맨드를 추가합니다.
#  LSF 다중사용자 HPC 환경에서 사용자별 전용 Digdag 서버를 관리합니다.
#
# ────────────────────────────────────────────────────────────
#  커스텀 커맨드 목록
# ────────────────────────────────────────────────────────────
#
#  start_server
#    유저당 1대만 허용. 기동 중인 서버가 있으면 재사용, 없으면 신규 기동
#    다중 서버는 run_workflow --once (1회용) 로만 허용
#    사용법: digdag start_server
#
#  kill_server
#    내 Digdag 서버를 종료
#    확인 프롬프트 후 kill -- -$pid 로 서버 + 하위 자식 프로세스 그룹 전체 종료
#    사용법: digdag kill_server
#
#  list_server
#    기동 중인 서버 전체를 가로 테이블로 표시
#    각 서버별 실행 중인 project/workflow 목록 포함
#    사용법: digdag list_server
#
#  run_workflow <project> <workflow> [옵션]
#    서버 기동(재사용 또는 신규) → push → start
#    --once 옵션 시: 새 서버 기동 → push → start
#                   → 워크플로우 완료 대기 → 서버 자동 종료 (1회용)
#    --once 는 프로젝트 1개당 서버 1대를 사용하여 완전한 격리 실행에 활용
#    옵션:
#    --once                   : 1회용 서버 모드
#    --log, -L <file>         : --once 전용. 백그라운드 모니터링 로그 파일
#      --project, -d <dir>      : 프로젝트 디렉토리 (기본: 현재 디렉토리)
#      --params-file, -P <file> : 외부 파라미터 파일
#    사용법: digdag run_workflow my_project etl_workflow
#            digdag run_workflow --once my_project etl_workflow
#            digdag run_workflow --project /path/to -P p.yml my_project etl_workflow
#
#  list_job [옵션]
#    서버가 1대면 자동 선택, 다수면 번호 선택 후 attempts 테이블 출력
#    옵션:
#      --all        : running 외 전체 status 표시 (기본: running만)
#      -p <project> : 프로젝트 필터
#      -w <workflow>: 워크플로우 필터
#    사용법: digdag list_job
#            digdag list_job --all -p my_project
#
#  kill_job [옵션]
#    서버 선택 → running attempts 목록 출력 → ID 입력 또는 전체 kill
#    옵션:
#      --all        : 조건에 맞는 모든 attempt 즉시 kill
#      -p <project> : 프로젝트 필터
#      -w <workflow>: 워크플로우 필터
#    사용법: digdag kill_job
#            digdag kill_job --all -p my_project
#
#  browse
#    브라우저(xdg-open / firefox)로 Digdag UI 열기
#    사용법: digdag browse
#
# ────────────────────────────────────────────────────────────
#  서버 관리 설계
# ────────────────────────────────────────────────────────────
#  - 한 계정에서 여러 서버 동시 운영 가능 (포트 자동 할당)
#  - setsid + disown 으로 서버를 부모 프로세스와 완전 분리
#    (스크립트 종료 / 터미널 닫힘 / LSF job 종료 후에도 서버 유지)
#  - kill -- -$pid 로 서버 + 하위 자식 프로세스 그룹 전체 종료
#  - 감시 프로세스가 포트 기반으로 서버 종료 감지 → lock 자동 삭제
#  - Race Condition 방지: noclobber lock 으로 동시 기동 시 최초 1개만 기동
#  - 모든 파일은 /tmp/digdag_$USER/ 에 저장 (NFS 홈 용량 절약)
#
#  파일 구조:
#    /tmp/digdag_<user>/
#      ├── server.log.<PID>   : 서버 로그
#      ├── server.info        : PORT / PID / URL / STARTED
#      ├── server.lock        : Race Condition 방지 lock
#      ├── task-logs/         : 태스크 실행 로그
#      └── jvm-tmp/           : JVM 임시 디렉토리
# ============================================================

# ── 설정 ────────────────────────────────────────────────────
BASE_PORT=65432
MAX_RETRIES=50
BOOT_TIMEOUT=15
LOCK_TIMEOUT=60            # 후발 프로세스 최대 대기 시간 (초)
USER_NAME=$(id -un)
HOST_NAME=$(hostname)
WORK_DIR="$(pwd)"
# 각 서버(컴퓨트팜)의 로컬 /tmp 사용
#  - 홈디렉토리 용량 절약 (NFS 공유 홈 1GiB 제한 대응)
#  - 서버별 독립 공간 확보 (LSF 멀티 컴퓨트팜 환경)
#  - 재부팋 시 자연 소멸 (서버도 같이 죽으므로 문제 없음)
DIGDAG_TMP_DIR="/tmp/digdag_${USER_NAME}"          # start_server 전용
DIGDAG_JVM_TMP="${DIGDAG_TMP_DIR}/jvm-tmp"          # JVM 임시 디렉토리
LOG_FILE="${DIGDAG_TMP_DIR}/server.log"
TASK_LOG_DIR="${DIGDAG_TMP_DIR}/task-logs"
INFO_FILE="${DIGDAG_TMP_DIR}/server.info"
LOCK_FILE="${DIGDAG_TMP_DIR}/server.lock"
# --once 전용 디렉토리: PID 기반으로 유일성 보장, start_server 와 완전 분리
# (실행 시점에 $$ 로 확정되므로 여기서는 빈 값)
ONCE_TMP_DIR=""
DIGDAG_JAR="/user/qarepo/usr/local/digdag-0.10.5.1.jar"  # 버전 변경 시 이 경로만 수정
# ────────────────────────────────────────────────────────────

# ── 색상 정의 ────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;34m'   # 파란색 (흰/검 배경 모두 가시성 양호)
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
    ps -u "$USER_NAME" -f 2>/dev/null | awk -v jar="$jar_name" '$0 ~ jar && /server/ && !/run/ && !/push/ && !/start/ && !/retry/ && !/kill/ && !/check/ { print $2 }'
}

# PID -> 포트 추출
# ss -tlnp 는 newgrp 환경에서 users 컬럼이 누락될 수 있으므로
# /proc/<pid>/net/tcp 를 직접 읽어 포트를 추출 (그룹 무관하게 동작)
find_port_by_pid() {
    local pid="$1"
    # /proc/<pid>/net/tcp: 로컬 주소 컬럼(2번)이 hex로 인코딩됨
    # LISTEN 상태(0A) 인 소켓의 포트만 추출
    local port_hex port_dec
    port_hex=$(cat "/proc/$pid/net/tcp" 2>/dev/null \
        | awk 'NR>1 && $4=="0A" {print $2}' \
        | awk -F: '{print $2}' \
        | head -1)
    [ -z "$port_hex" ] && return 1
    # hex → decimal 변환
    port_dec=$(printf '%d' "0x${port_hex}" 2>/dev/null)
    [ -n "$port_dec" ] && echo "$port_dec"
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
# start_server 전용: server.info 의 PID 와 대조하여 --once 서버 제외
check_server_alive() {
    local pid port

    # server.info 가 있으면 그 PID 를 우선 사용 (start_server 전용)
    if [ -f "$INFO_FILE" ]; then
        pid=$(grep '^PID=' "$INFO_FILE" 2>/dev/null | cut -d= -f2)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            port=$(grep '^PORT=' "$INFO_FILE" | cut -d= -f2)
            [ -n "$port" ] && port_in_use "$port" && { echo "$port"; return 0; }
        fi
        # info 파일이 있지만 프로세스가 없으면 → 정리 후 없음으로 처리
        rm -f "$INFO_FILE" "$LOCK_FILE"
        return 1
    fi

    # server.info 없으면 → start_server 서버 없음
    return 1
}

# ── 서버 기동 -> stdout: 포트 / 종료코드 0=OK 1=실패 ─────────
#
#  항상 새 서버를 기동한다 (기존 서버 유무 무관)
#  Race Condition 방지 설계:
#   - noclobber(set -C) 로 lock 파일을 atomic 하게 생성
#     -> 동시 실행 시 최초 프로세스만 lock 획득, 후발은 대기 후 재사용
#   - lock 파일 생명주기 = digdag server 프로세스 생명주기
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
#  --once 전용 서버 기동 -> stdout: 포트 / 종료코드 0=OK 1=실패
#
#  start_server 와 완전히 분리:
#   - PID 기반 전용 디렉토리 사용 → lock/info 파일 충돌 없음
#   - check_server_alive(start_server 서버) 에 영향 없음
#   - Race Condition lock 없음 (1회성이므로 경쟁 불필요)
# ════════════════════════════════════════════════════════════
start_once_server() {
    # PID 기반 전용 디렉토리 확정
    ONCE_TMP_DIR="${DIGDAG_TMP_DIR}/once.$$"
    local once_jvm_tmp="${ONCE_TMP_DIR}/jvm-tmp"
    local once_task_log="${ONCE_TMP_DIR}/task-logs"
    local once_info="${ONCE_TMP_DIR}/server.info"
    local once_log="${ONCE_TMP_DIR}/server.log.$$"

    mkdir -p "$ONCE_TMP_DIR" "$once_task_log" "$once_jvm_tmp"
    chmod 700 "$ONCE_TMP_DIR" "$once_task_log" "$once_jvm_tmp"

    # --once 는 DIGDAG_BIN 의 tmpdir 도 전용 디렉토리로 분리
    local once_bin=(java -Djava.io.tmpdir="$once_jvm_tmp" -jar "$DIGDAG_JAR")

    local port=$BASE_PORT
    for (( i=1; i<=MAX_RETRIES; i++ )); do
        if port_in_use $port; then
            ((port++)); continue
        fi

        log "  [OK] 포트 $port -> 1회용 서버 기동 중..."

        setsid "${once_bin[@]}" server \
            --bind 0.0.0.0 \
            --port $port \
            --memory \
            --task-log "$once_task_log" \
            > "$once_log" 2>&1 &

        BOOTING_PID=$!
        disown $BOOTING_PID

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

    log "${RED}[ERROR] 1회용 서버 기동 실패${NC}"
    log "  로그 확인: $once_log"
    tail -n 10 "$once_log" >&2
    return 1
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: run_workflow
# ════════════════════════════════════════════════════════════
cmd_run_workflow() {
    local project_dir="$WORK_DIR"
    local project_name=""
    local workflow_name=""
    local params_file=""       # -P / --params-file (선택)
    local once=false           # --once: 1회용 (새 서버 기동 → push → start → 서버 종료)
    local log_file=""          # --log <file>: 백그라운드 모니터링 로그 파일 (--once 전용)
    local log_file=""          # --log <file>: 백그라운드 모니터링 로그 파일 (--once 전용)

    # 보안 4: 엄격한 파라미터 파싱 - 알 수 없는 옵션 차단
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --once) once=true; shift ;;
            --log|-L) log_file="$2"; shift 2 ;;
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
        log "  project_name             : Digdag에 등록할 프로젝트 이름"
        log "  workflow_name            : 실행할 워크플로우 이름 (.dig 파일명)"
        log ""
        log "  [선택]"
        log "  --project, -d <dir>      : 프로젝트 디렉토리 (기본값: 현재 디렉토리)"
        log "  --params-file, -P <file> : 외부 파라미터 파일 경로"
        log "  --once                   : 1회용 서버 (새 서버 기동 → push → start → 백그라운드 대기 → 서버 종료)
  --log, -L <file>         : --once 전용. 백그라운드 모니터링 로그 파일 경로"
        log ""
        log "  예시) digdag run_workflow my_project etl_workflow"
        log "  예시) digdag run_workflow --once -P params.yml my_project etl_workflow"
        log "  예시) digdag run_workflow --project /path/to/proj -P params.yml my_project etl_workflow"
        log ""
        exit 1
    fi

    # 서버 모드 문자열
    local mode_str
    if $once; then
        mode_str="1회용 서버 (--once: 새 서버 기동 → 완료 후 종료)"
    else
        mode_str="기존 서버 재사용 또는 신규 기동"
    fi

    print_divider
    log "${BOLD}  [START] run_workflow 시작${NC}"
    log "  프로젝트 디렉토리: ${CYAN}$project_dir${NC}"
    log "  프로젝트 이름    : ${CYAN}$project_name${NC}"
    log "  워크플로우 이름  : ${CYAN}$workflow_name${NC}"
    [ -n "$params_file" ] && log "  파라미터 파일    : ${CYAN}$params_file${NC}"
    log "  서버 모드        : ${CYAN}${mode_str}${NC}"
    [ -n "$log_file" ] && log "  모니터링 로그    : ${CYAN}${log_file}${NC}"
    print_divider

    # ── STEP 1. 서버 확인 또는 기동 ─────────────────────────
    log "\n${YELLOW}[STEP 1]${NC} Digdag 서버 확인 중..."

    local port
    local server_booted=false  # 이번 실행에서 새로 기동했는지 여부 (--once 종료 판단용)

    if ! $once && port=$(check_server_alive); then
        log "${GREEN}[OK] 기존 서버 재사용 (PORT: $port)${NC}"
    else
        if $once; then
            log "  [INFO] --once: 1회용 전용 서버를 기동합니다."
            if ! port=$(start_once_server); then
                log "${RED}[ERROR] 1회용 서버 기동 실패. run_workflow 를 중단합니다.${NC}"
                exit 1
            fi
        else
            log "  서버 없음 -> 자동 기동합니다."
            if ! port=$(start_server); then
                log "${RED}[ERROR] 서버 기동 실패. run_workflow 를 중단합니다.${NC}"
                exit 1
            fi
        fi
        server_booted=true
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
        $once && $server_booted && _kill_server_by_port "$port"
        exit 1
    fi
    log "${GREEN}[OK] Push 완료${NC}"

    # ── STEP 3. Start ────────────────────────────────────────
    log "\n${YELLOW}[STEP 3]${NC} 워크플로우 Start 중..."

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
        log "${RED}[ERROR] Start 실패.${NC}"
        $once && $server_booted && _kill_server_by_port "$port"
        exit 1
    fi
    log "${GREEN}[OK] Start 완료${NC}"

    # ── STEP 4 (--once 전용). attempt id 추출 ────────────────
    if $once && $server_booted; then
        local attempt_id
        attempt_id=$(echo "$start_output" | awk -F': ' '/attempt id:|^ *id:/ {gsub(/^ +/,"",$2); print $2}' | grep -E '^[0-9]+$' | head -1)

        if [ -n "$attempt_id" ]; then
            log "\n${YELLOW}[STEP 4]${NC} attempt id: ${BOLD}$attempt_id${NC}"
        else
            log "\n${YELLOW}[STEP 4]${NC} ${YELLOW}[WARN] attempt id 추출 실패. fallback polling 사용${NC}"
        fi

        # ── STEP 5. 백그라운드 전환 → 즉시 프롬프트 반환 ────
        log "\n${YELLOW}[STEP 5]${NC} 백그라운드로 전환합니다."
        log "  워크플로우 완료 후 서버가 자동 종료됩니다."
        if [ -n "$log_file" ]; then
            log "  워크플로우 로그: ${CYAN}$log_file${NC}  (완료 후 기록)"
            log "  진행 상태     : ${CYAN}${log_file}.status${NC}  (실시간)"
        else
            log "  ${YELLOW}(--log <file> 옵션으로 로그 파일 지정 가능)${NC}"
        fi

        # 모니터 스크립트를 /tmp 에 저장 (ONCE_TMP_DIR 미생성 시에도 안전)
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
# --log 미지정 시 ONCE_TMP_DIR/workflow.log 를 자동 생성
[ -z "$_lf" ] && _lf="${_od}/workflow.log"
_sf="${_lf}.status"
_s() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "$_sf"; }
_s "시작 (aid=${_aid:-N/A} port=${_port})"
_s "워크플로우 로그 → ${_lf}"
_s "상태 로그     → ${_sf}"
if [ -n "$_aid" ]; then
    # digdag log -f: attempt 완료까지 로그 스트리밍 → 완료 시 자동 종료
    # --log 지정이든 미지정이든 동일하게 처리
    _s "digdag log -f 시작..."
    java -Djava.io.tmpdir="$_jvm" -jar "$_jar" log "$_aid" -e "http://${_host}:${_port}" -f > "$_lf" 2>&1
    _s "digdag log -f 완료 (exit=$?)"
else
    # fallback: attempt id 없을 때 서버 alive + running 여부 polling
    _s "[WARN] attempt id 없음. polling fallback 사용"
    sleep 5
    while true; do
        sleep 10
        (echo > /dev/tcp/${_host}/${_port}) >/dev/null 2>&1 || { _s "[WARN] 서버 응답 없음. 종료합니다."; break; }
        _running=$(java -Djava.io.tmpdir="$_jvm" -jar "$_jar" attempts -e "http://${_host}:${_port}" 2>/dev/null | grep 'status: *running')
        [ -n "$_running" ] && { _s "polling... running"; continue; }
        break
    done
fi
# 서버 종료
_s "서버 종료 중 (PORT=${_port})..."
_jn=$(basename "$_jar")
while IFS= read -r _p || [ -n "$_p" ]; do
    [ -z "$_p" ] && continue
    _ph=$(awk 'NR>1 && $4=="0A" {print $2}' "/proc/${_p}/net/tcp" 2>/dev/null | awk -F: '{print $2}' | head -1)
    _pp=$(printf '%d' "0x${_ph}" 2>/dev/null)
    if [ "$_pp" = "$_port" ]; then
        kill -- "-${_p}" 2>/dev/null
        [ -n "$_od" ] && [ -d "$_od" ] && rm -rf "$_od"
        _s "서버 종료 완료 (PID=${_p})"
        break
    fi
done < <(ps -u "$(id -un)" -f 2>/dev/null | awk -v j="$_jn" '$0~j&&/server/&&!/run/&&!/push/&&!/start/&&!/retry/&&!/kill/&&!/check/{print $2}')
rm -f "$0"
_s "모니터링 종료"
MONITOR_LOGIC
        chmod 700 "$_script"

        # nohup + setsid 로 완전 분리 실행 → 즉시 프롬프트 반환
        nohup setsid bash "$_script" </dev/null >/dev/null 2>/dev/null &
        disown $!
    fi

    log ""
    print_divider
    log "${GREEN}${BOLD}[DONE] run_workflow 완료!${NC}"
    log "  프로젝트  : $project_name"
    log "  워크플로우: $workflow_name"
    if $once && $server_booted; then
        log "  모드      : ${YELLOW}1회용 (백그라운드에서 완료 대기 중)${NC}"
        log "  서버 PORT : $port"
        if [ -n "$log_file" ]; then
            log "  로그 파일 : ${CYAN}$log_file${NC}  (워크플로우 완료 후 기록)"
            log "  상태 파일 : ${CYAN}${log_file}.status${NC}  (폴링 진행 상황)"
        else
            log "  로그 파일 : ${CYAN}${ONCE_TMP_DIR}/workflow.log${NC}  (자동 생성)"
            log "  상태 파일 : ${CYAN}${ONCE_TMP_DIR}/workflow.log.status${NC}"
        fi
    else
        log "  URL       : $endpoint"
    fi
    print_divider
    log ""
}

# ════════════════════════════════════════════════════════════
#  내부 헬퍼: 포트 기준으로 서버 프로세스 그룹 종료
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
            # --once 서버: ONCE_TMP_DIR 전체 삭제
            # start_server 서버: LOCK_FILE / INFO_FILE 만 삭제
            if [ -n "$ONCE_TMP_DIR" ] && [ -d "$ONCE_TMP_DIR" ]; then
                rm -rf "$ONCE_TMP_DIR"
            else
                rm -f "$LOCK_FILE" "$INFO_FILE"
            fi
            log "  ${GREEN}[OK] 서버 종료 완료 (PID=$pid)${NC}"
            return 0
        fi
    done <<< "$all_pids"
    log "  ${YELLOW}[WARN] 종료할 서버를 찾을 수 없습니다.${NC}"
    return 1
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
#  공통 헬퍼: 서버 선택 → stdout: 포트
#  서버 1대: 자동 선택
#  서버 다수: 번호 선택 프롬프트 (단일 선택)
#  종료코드: 0=OK 1=서버없음 2=취소
# ════════════════════════════════════════════════════════════
select_server_port() {
    local all_pids
    all_pids=$(find_my_digdag_server_pid)

    if [ -z "$all_pids" ]; then
        log "${RED}[ERROR] 실행 중인 Digdag 서버가 없습니다.${NC}"
        return 1
    fi

    # 유효한 서버만 수집
    local srv_pids=() srv_ports=() srv_runnings=()
    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        # running project/workflow 간략 표시 (콤마 구분)
        local _attempts_raw _blocks _running_ids
        local running_str="(없음)"
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
        log "${RED}[ERROR] 유효한 서버가 없습니다.${NC}"
        return 1
    fi

    # 서버 1대: 자동 선택
    if [ "$count" -eq 1 ]; then
        log "  [OK] 서버 자동 선택 (PID=${srv_pids[0]}, PORT=${srv_ports[0]})"
        echo "${srv_ports[0]}"
        return 0
    fi

    # 서버 다수: 테이블 출력 후 선택
    log ""
    log "  ${BOLD}기동 중인 서버 목록 (서버를 선택하세요)${NC}"

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

    echo -n "  서버 번호를 선택하세요 (1-${count} / q=취소): " >&2
    read -r ans
    case "$ans" in
        q|"")
            log "\n[INFO] 취소되었습니다."
            return 2 ;;
        *[!0-9]*)
            log "${RED}[ERROR] 숫자를 입력하세요.${NC}"
            return 2 ;;
    esac
    if (( ans < 1 || ans > count )); then
        log "${RED}[ERROR] 범위를 벗어났습니다. (1-${count})${NC}"
        return 2
    fi

    local idx=$(( ans - 1 ))
    log "  [OK] 서버 선택 (PID=${srv_pids[$idx]}, PORT=${srv_ports[$idx]})"
    echo "${srv_ports[$idx]}"
    return 0
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

    # ── 서버 선택 (1대: 자동 / 다수: 프롬프트) ──────────────
    local port
    port=$(select_server_port)
    local rc_srv=$?
    [ $rc_srv -eq 1 ] && exit 1
    [ $rc_srv -eq 2 ] && exit 0
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

    # ── 서버 선택 (1대: 자동 / 다수: 프롬프트) ──────────────
    local port
    port=$(select_server_port)
    local rc_srv=$?
    [ $rc_srv -eq 1 ] && exit 1
    [ $rc_srv -eq 2 ] && exit 0
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
    local kill_all=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) kill_all=true; shift ;;
            -*) log "${RED}[ERROR] 알 수 없는 옵션: $1${NC}"; exit 1 ;;
            *)  log "${RED}[ERROR] 알 수 없는 인자: $1${NC}";  exit 1 ;;
        esac
    done

    print_divider
    log "${BOLD}  [KILL_SERVER] Digdag 서버 종료${NC}"
    print_divider

    # ── 기동 중인 서버 PID 전체 수집 ────────────────────────
    local all_pids_raw
    all_pids_raw=$(find_my_digdag_server_pid)

    if [ -z "$all_pids_raw" ]; then
        log "${YELLOW}[INFO] 실행 중인 Digdag 서버가 없습니다.${NC}"
        log ""
        exit 0
    fi

    # 유효한 서버만 배열에 수집 (포트 확인 포함)
    local srv_pids=() srv_ports=() srv_urls=() srv_running_cells=()
    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        local url="http://$HOST_NAME:$port"
        local _attempts_raw _blocks _running_ids
        local cell="(없음)"
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
    # pids / ports 는 기존 코드 호환용
    local pids=("${srv_pids[@]}")
    local ports=("${srv_ports[@]}")

    if [ "$server_count" -eq 0 ]; then
        log "${YELLOW}[INFO] 유효한 서버가 없습니다.${NC}"
        log ""
        exit 0
    fi

    # ── 컬럼 너비 계산 후 공용 테이블 출력 ──────────────────
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

    # --all 옵션: 전체 즉시 종료
    if $kill_all; then
        log "[INFO] --all 옵션 -> 전체 서버 종료합니다."
        local fail=0
        for (( i=0; i<server_count; i++ )); do
            _do_kill_server "${pids[$i]}" "${ports[$i]}" || ((fail++))
        done
        log ""
        [ $fail -eq 0 ]             && log "${GREEN}[DONE] 전체 서버 종료 완료${NC}"             || log "${YELLOW}[WARN] 일부 실패: ${fail}건${NC}"
        log ""
        exit 0
    fi

    # 선택 프롬프트 (다중 선택)
    log "  종료할 서버 번호를 입력하세요."
    log "  (여러 개는 공백으로 구분 / 'all' = 전체 / 'q' = 취소)"
    echo -n "  입력> " >&2
    read -r user_input

    case "$user_input" in
        q|"") log "
[INFO] 취소되었습니다."; exit 0 ;;
        all)
            local fail=0
            for (( i=0; i<server_count; i++ )); do
                _do_kill_server "${pids[$i]}" "${ports[$i]}" || ((fail++))
            done
            log ""
            [ $fail -eq 0 ]                 && log "${GREEN}[DONE] 전체 서버 종료 완료${NC}"                 || log "${YELLOW}[WARN] 일부 실패: ${fail}건${NC}" ;;
        *)
            local fail=0
            for no in $user_input; do
                # 입력값 검증
                if ! [[ "$no" =~ ^[0-9]+$ ]] || (( no < 1 || no > server_count )); then
                    log "  ${YELLOW}[SKIP] $no: 유효하지 않은 번호${NC}"
                    continue
                fi
                local idx=$(( no - 1 ))
                _do_kill_server "${pids[$idx]}" "${ports[$idx]}" || ((fail++))
            done
            log ""
            [ $fail -eq 0 ]                 && log "${GREEN}[DONE] 선택 서버 종료 완료${NC}"                 || log "${YELLOW}[WARN] 일부 실패: ${fail}건${NC}" ;;
    esac
    log ""
}

# ── kill 실행 공통 함수 ──────────────────────────────────────
# 인자: $1=pid, $2=port
_do_kill_server() {
    local target_pid="$1" target_port="$2"
    log -n "  PID $target_pid (PORT $target_port) 종료 중... "
    if kill -- "-$target_pid" 2>/dev/null; then
        # --once 서버: /tmp/digdag_$USER/once.* 디렉토리 정리
        # start_server 서버: LOCK_FILE / INFO_FILE 정리
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
#  커스텀 서브커맨드: list_server
#
#  사용법:
#    digdag list_server
#
#  동작:
#    - 현재 기동 중인 서버 정보 표시 (PORT / PID / URL / STARTED)
#    - 서버에서 실행 중인 프로젝트 목록 (running attempts 기반)
# ════════════════════════════════════════════════════════════
cmd_list_server() {
    print_divider
    log "${BOLD}  [LIST_SERVER] 서버 현황${NC}"
    print_divider

    # ── 기동 중인 서버 PID 전체 수집 ────────────────────────
    local all_pids
    all_pids=$(find_my_digdag_server_pid)

    if [ -z "$all_pids" ]; then
        log "${YELLOW}[INFO] 기동 중인 Digdag 서버가 없습니다.${NC}"
        log ""
        exit 0
    fi

    # ── 서버별 정보 수집 ─────────────────────────────────────
    local srv_pids=() srv_ports=() srv_urls=() srv_running_cells=()

    while IFS= read -r pid || [ -n "$pid" ]; do
        [ -z "$pid" ] && continue
        local port
        port=$(find_port_by_pid "$pid")
        [ -z "$port" ] && continue
        port_in_use "$port" || continue

        local url="http://$HOST_NAME:$port"

        # fetch_attempts 로 running attempts 조회 후 project/workflow 목록 생성
        local endpoint="$url"
        local _attempts_raw _blocks _running_ids
        local cell="(없음)"
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

            # running 이 1개면 URL 에 attempt_id 포함
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
        log "${YELLOW}[INFO] 유효한 서버가 없습니다.${NC}"
        log ""
        exit 0
    fi

    # ── 컬럼 너비 계산 후 공용 테이블 출력 ──────────────────
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
#  내부 헬퍼: 서버 테이블 출력 (list_server / kill_server 공용)
#  인자: $1 = server_count
#  사용 배열: srv_pids / srv_ports / srv_urls / srv_running_cells
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
        [ -z "$cell_data" ] && cell_data="(없음)"
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
#  커스텀 서브커맨드: browse
# ════════════════════════════════════════════════════════════
cmd_browse() {
    print_divider
    log "${BOLD}  [BROWSE] Digdag UI 열기${NC}"
    print_divider

    # 서버 선택 (1대: 자동 / 다수: 번호 선택)
    local port
    port=$(select_server_port)
    local rc=$?
    [ $rc -eq 1 ] && exit 1
    [ $rc -eq 2 ] && exit 0

    local url="http://$HOST_NAME:$port"
    log "\n  URL: ${BOLD}${CYAN}$url${NC}"

    # xdg-open 우선 (Linux 기본 브라우저), 없으면 firefox 폴백
    if command -v xdg-open >/dev/null 2>&1; then
        log "  [WEB] 기본 브라우저로 접속합니다..."
        xdg-open "$url" >/dev/null 2>&1 &
    elif command -v firefox >/dev/null 2>&1; then
        log "  [WEB] Firefox 로 접속합니다..."
        firefox "$url" >/dev/null 2>&1 &
    else
        log "${RED}[ERROR] 브라우저 실행 명령어를 찾을 수 없습니다.${NC}"
        log "  직접 접속하세요: ${CYAN}$url${NC}"
        exit 1
    fi
    log "  ${GREEN}[OK] 브라우저 실행 완료${NC}"
    log ""
}

# ════════════════════════════════════════════════════════════
#  커스텀 서브커맨드: start_server
#
#  사용법:
#    digdag start_server
#
#  동작:
#    - 유저당 1대만 허용
#    - 기동 중인 서버가 있으면 재사용, 없으면 신규 기동
#    - 다중 서버는 run_workflow --once (1회용) 로만 허용
# ════════════════════════════════════════════════════════════
cmd_start_server() {
    print_divider
    log "${BOLD}  [START] start_server 시작${NC}"
    log "  사용자: ${CYAN}$USER_NAME${NC}"
    print_divider

    local port
    if port=$(check_server_alive); then
        log "\n${GREEN}[OK] 이미 실행 중인 서버를 재사용합니다.${NC}"
    else
        log "\n[INFO] 서버 없음 -> 자동 기동합니다."
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
