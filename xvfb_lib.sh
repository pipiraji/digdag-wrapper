#!/bin/bash
# ============================================================
# xvfb_lib.sh  —  Xvfb 공통 라이브러리
# 직접 실행하지 말고, 다른 스크립트에서 source로 불러서 사용
#
# 사용법:
#   source /path/to/xvfb_lib.sh
#   xvfb_start          # Xvfb 시작
#   xvfb_stop           # Xvfb 종료
#   xvfb_run <명령어>   # Xvfb 안에서 명령 실행 후 자동 종료
# ============================================================

# --- 기본값 (호출 스크립트에서 덮어쓸 수 있음) --------------
XVFB_DISPLAY=""           # 비워두면 자동 탐색, 직접 지정도 가능 (예: ":99")
XVFB_SCREEN="${XVFB_SCREEN:-1920x1080x24}"
XVFB_DISPLAY_RANGE_START="${XVFB_DISPLAY_RANGE_START:-99}"   # 탐색 시작 번호
XVFB_DISPLAY_RANGE_END="${XVFB_DISPLAY_RANGE_END:-199}"      # 탐색 끝 번호
XVFB_PID=""

# --- 함수 정의 ----------------------------------------------

# 디스플레이 번호가 사용 중인지 확인
# 비어있으면 return 0 (사용 가능), 사용 중이면 return 1
_xvfb_display_in_use() {
    local num=$1

    # 1) lock 파일 존재 여부
    if [ -f "/tmp/.X${num}-lock" ]; then
        return 1
    fi

    # 2) X 소켓 존재 여부 (/tmp/.X11-unix/X<num>)
    if [ -S "/tmp/.X11-unix/X${num}" ]; then
        return 1
    fi

    return 0
}

# 사용 가능한 디스플레이 번호를 자동으로 찾아 XVFB_DISPLAY에 설정
_xvfb_find_free_display() {
    local start="${XVFB_DISPLAY_RANGE_START}"
    local end="${XVFB_DISPLAY_RANGE_END}"

    echo "[xvfb] 사용 가능한 DISPLAY 번호 탐색 중... (범위: :${start} ~ :${end})"

    for num in $(seq "${start}" "${end}"); do
        if _xvfb_display_in_use "${num}"; then
            XVFB_DISPLAY=":${num}"
            echo "[xvfb] 사용 가능한 DISPLAY 발견: ${XVFB_DISPLAY}"
            return 0
        fi
    done

    echo "[xvfb] ERROR: :${start} ~ :${end} 범위 내 사용 가능한 DISPLAY 없음"
    return 1
}

xvfb_start() {
    # XVFB_DISPLAY가 비어있으면 자동 탐색
    if [ -z "${XVFB_DISPLAY}" ]; then
        _xvfb_find_free_display || return 1
    else
        # 직접 지정된 경우에도 사용 중인지 검사
        local num="${XVFB_DISPLAY#:}"
        if ! _xvfb_display_in_use "${num}"; then
            echo "[xvfb] WARN: 지정된 ${XVFB_DISPLAY} 이미 사용 중 → 자동 탐색으로 전환"
            XVFB_DISPLAY=""
            _xvfb_find_free_display || return 1
        fi
    fi

    echo "[xvfb] 시작 중... (DISPLAY=${XVFB_DISPLAY})"

    Xvfb "${XVFB_DISPLAY}" -screen 0 "${XVFB_SCREEN}" -ac &
    XVFB_PID=$!
    sleep 1

    if ! kill -0 "${XVFB_PID}" 2>/dev/null; then
        echo "[xvfb] ERROR: 시작 실패"
        return 1
    fi

    export DISPLAY="${XVFB_DISPLAY}"
    echo "[xvfb] 시작 완료 (PID=${XVFB_PID}, DISPLAY=${DISPLAY})"
}

xvfb_stop() {
    if [ -n "${XVFB_PID}" ] && kill -0 "${XVFB_PID}" 2>/dev/null; then
        echo "[xvfb] 종료 중... (PID=${XVFB_PID})"
        kill "${XVFB_PID}"
        wait "${XVFB_PID}" 2>/dev/null
        XVFB_PID=""
        echo "[xvfb] 종료 완료"
    fi
}

# 훅 함수 — run 스크립트에서 필요 시 재정의
# xvfb_run 종료 시 xvfb_stop 후 자동 호출됨
xvfb_on_exit() {
    : # 기본값: 아무것도 안 함 (run 스크립트에서 override 가능)
}

# 내부 정리 함수 (xvfb_stop + 훅 호출)
_xvfb_cleanup() {
    xvfb_stop
    xvfb_on_exit
}

# xvfb_run <명령어 ...>
# — Xvfb 시작 → 명령 실행 → 종료 시 _xvfb_cleanup 자동 호출
# 단일 바이너리, 스크립트 모두 안전하게 처리
xvfb_run() {
    xvfb_start || return 1

    _XVFB_CHILD_PID=""

    # 시그널 감지 시: 자식 프로세스도 함께 종료 후 정리
    trap '_xvfb_signal_handler SIGINT'  SIGINT
    trap '_xvfb_signal_handler SIGTERM' SIGTERM
    trap '_xvfb_signal_handler SIGHUP'  SIGHUP

    echo "[xvfb] 실행: $*"

    # 백그라운드로 실행해서 자식 PID 확보
    "$@" &
    _XVFB_CHILD_PID=$!

    # 자식이 끝날 때까지 대기 (시그널 오면 trap으로 점프)
    wait "${_XVFB_CHILD_PID}"
    local exit_code=$?

    _xvfb_cleanup
    trap - SIGINT SIGTERM SIGHUP
    return "${exit_code}"
}

# 시그널 핸들러 — 자식 프로세스 그룹까지 종료
_xvfb_signal_handler() {
    local sig="${1:-SIGTERM}"
    echo "[xvfb] ${sig} 감지 → 자식 프로세스 종료 중..."

    if [ -n "${_XVFB_CHILD_PID}" ]; then
        # 자식 프로세스 그룹 전체에 SIGTERM 전달 (스크립트 내부 프로세스 포함)
        kill -- "-${_XVFB_CHILD_PID}" 2>/dev/null  # 프로세스 그룹
        kill "${_XVFB_CHILD_PID}" 2>/dev/null       # 자식 자체
        wait "${_XVFB_CHILD_PID}" 2>/dev/null
    fi

    _xvfb_cleanup

    # 시그널별 exit 코드 구분 (128 + 시그널 번호)
    case "${sig}" in
        SIGINT)  exit 130 ;;   # 128 + 2
        SIGHUP)  exit 129 ;;   # 128 + 1
        SIGTERM) exit 143 ;;   # 128 + 15
        *)       exit 1   ;;
    esac
}
