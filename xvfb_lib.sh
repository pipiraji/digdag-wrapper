#!/bin/bash
# ============================================================
# xvfb_lib.sh  —  Xvfb 공통 라이브러리
#
# 역할 분담:
#   xvfb-run  → 보안, 디스플레이 탐색, Xvfb 라이프사이클
#   이 파일   → 훅(xvfb_on_exit), lib 구조, 시그널 관리
#
# 사용법:
#   source /path/to/xvfb_lib.sh
#   xvfb_run <명령어>
# ============================================================

# --- xvfb-run 존재 여부 확인 --------------------------------
if ! command -v xvfb-run &>/dev/null; then
    echo "[xvfb] ERROR: xvfb-run 이 설치되어 있지 않습니다."
    echo "[xvfb]        sudo apt install xvfb  또는  yum install xorg-x11-server-Xvfb"
    exit 1
fi

# --- 기본값 (run 스크립트에서 덮어쓸 수 있음) ---------------
# xvfb-run 에 전달할 추가 옵션
# -a : 사용 중인 디스플레이 번호가 있으면 자동으로 다음 번호 탐색
XVFB_RUN_OPTS="${XVFB_RUN_OPTS:--a}"

# --- 내부 변수 ----------------------------------------------
_XVFB_CHILD_PID=""

# ============================================================
# 훅 함수 — run 스크립트에서 필요 시 재정의
# 명령 종료 후 자동 호출됨
# ============================================================
xvfb_on_exit() {
    : # 기본값: 아무것도 안 함
}

# --- 내부 정리 함수 -----------------------------------------
_xvfb_cleanup() {
    xvfb_on_exit
}

# --- 시그널 핸들러 ------------------------------------------
_xvfb_signal_handler() {
    local sig="${1:-SIGTERM}"
    echo "[xvfb] ${sig} 감지 → 자식 프로세스 종료 중..."

    if [ -n "${_XVFB_CHILD_PID}" ]; then
        # setsid로 실행했으므로 PID == PGID → 그룹 전체 종료
        kill -TERM "-${_XVFB_CHILD_PID}" 2>/dev/null
        kill -TERM "${_XVFB_CHILD_PID}"  2>/dev/null
        wait "${_XVFB_CHILD_PID}" 2>/dev/null
    fi

    _xvfb_cleanup

    case "${sig}" in
        SIGINT)  exit 130 ;;   # 128 + 2
        SIGHUP)  exit 129 ;;   # 128 + 1
        SIGTERM) exit 143 ;;   # 128 + 15
        *)       exit 1   ;;
    esac
}

# ============================================================
# xvfb_run <명령어 ...>
#
# xvfb-run 이 담당:
#   - 빈 디스플레이 번호 자동 탐색 (-displayfd)
#   - xauth 인증 자동 처리
#   - -nolisten tcp (TCP 차단)
#   - Xvfb 라이프사이클 (시작/종료)
#
# 이 함수가 담당:
#   - 시그널 처리 (SIGTERM / SIGHUP)
#   - 자식 프로세스 그룹 종료 (setsid)
#   - xvfb_on_exit 훅 호출
# ============================================================
xvfb_run() {
    echo "[xvfb] 실행: $*"

    # setsid: 자식을 새 세션으로 분리 → PID == PGID 보장 → 그룹 kill 가능
    # xvfb-run: 보안/디스플레이 처리 위임
    setsid xvfb-run ${XVFB_RUN_OPTS} "$@" &
    _XVFB_CHILD_PID=$!

    trap '_xvfb_signal_handler SIGINT'  SIGINT
    trap '_xvfb_signal_handler SIGTERM' SIGTERM
    trap '_xvfb_signal_handler SIGHUP'  SIGHUP

    # 자식 종료까지 대기 (시그널 오면 trap으로 점프)
    wait "${_XVFB_CHILD_PID}"
    local exit_code=$?

    trap - SIGINT SIGTERM SIGHUP
    _xvfb_cleanup
    return "${exit_code}"
}
