#!/bin/bash
# ============================================================
# llm_lib.sh  —  LSF 환경용 로컬 LLM (Ollama) 공통 라이브러리
#
# 역할:
#   - 빈 포트 동적 탐색 ($RANDOM, 외부 명령 없음)
#   - Ollama 서버 백그라운드 실행 및 준비 상태(Health Check) 대기
#   - 사용자 명령어 실행 (opencode 등)
#   - 시그널 처리 및 LLM 프로세스 안전 종료
#
# 사용법:
#   source /path/to/llm_lib.sh
#   llm_run opencode run pdk-analyzer --input data.json
# ============================================================

# --- 내부 변수 ----------------------------------------------
_LLM_CHILD_PID=""
_LLM_PORT=""
_LLM_HOST=""

# --- 의존성 확인 (source 시점) ------------------------------
# xvfb_lib.sh 패턴 동일: 없으면 즉시 실패
if ! command -v ollama &>/dev/null; then
    echo "[llm] ERROR: ollama is not installed or not in PATH." >&2
    return 1 2>/dev/null || exit 1
fi

# --- 1. 빈 포트 탐색 ----------------------------------------
#
# $RANDOM: Bash 내장 (0~32767) → 외부 명령 호출 없음
# 실제 범위: 10000~42767 (32,768개) → LSF 환경에서 충분
# /dev/tcp connect 실패 = 해당 포트가 LISTEN 중이 아님 = 빈 포트
#
# digdag.sh port_in_use()와 동일한 방식,
# shuf 대비 ~2배 빠름 (벤치마크: shuf 38ms vs RANDOM 19ms)
# -----------------------------------------------------------
_get_free_port() {
    local port i
    for (( i=0; i<200; i++ )); do
        port=$(( 10000 + RANDOM % 32768 ))
        if ! (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null; then
            echo "$port"
            return 0
        fi
    done
    echo "[llm] ERROR: No free port found in range 10000-42767." >&2
    return 1
}

# --- 2. 멱등성 보장 Cleanup ---------------------------------
#
# _LLM_CHILD_PID 가 빈 문자열이면 즉시 return → 이중 호출 안전
# setsid로 실행했으므로 PID == PGID
# → kill -TERM -PID 로 ollama가 fork한 러너 프로세스까지 일괄 종료
# -----------------------------------------------------------
_llm_cleanup() {
    [ -z "${_LLM_CHILD_PID}" ] && return 0

    echo "[llm] Shutting down process group (PGID: ${_LLM_CHILD_PID})..." >&2
    kill -TERM "-${_LLM_CHILD_PID}" 2>/dev/null   # 그룹 전체
    kill -TERM  "${_LLM_CHILD_PID}" 2>/dev/null   # fallback: 개별
    wait "${_LLM_CHILD_PID}" 2>/dev/null

    _LLM_CHILD_PID=""
    echo "[llm] LLM server terminated." >&2
}

# --- 3. 시그널 핸들러 ----------------------------------------
#
# SIGTERM → _llm_on_exit → _llm_cleanup → exit 143
# exit 143 이 EXIT trap 재발동 → _llm_cleanup 재진입
# → _LLM_CHILD_PID="" 이므로 즉시 return 0 (멱등성)
# -----------------------------------------------------------
_llm_on_exit() {
    local sig="${1:-SIGTERM}"
    echo "[llm] ${sig} detected → cleaning up..." >&2
    _llm_cleanup
    case "${sig}" in
        SIGINT)  exit 130 ;;   # 128 + 2
        SIGHUP)  exit 129 ;;   # 128 + 1
        SIGTERM) exit 143 ;;   # 128 + 15
        *)       exit 1   ;;
    esac
}

# --- 4. 메인 래퍼 함수 --------------------------------------
llm_run() {
    if [ $# -eq 0 ]; then
        echo "[llm] ERROR: No command provided to llm_run." >&2
        return 1
    fi

    # 1. 빈 포트 탐색
    _LLM_PORT=$(_get_free_port) || return 1
    _LLM_HOST="127.0.0.1:${_LLM_PORT}"
    export OPENCODE_PROVIDER_URL="http://${_LLM_HOST}/v1"
    echo "[llm] Allocated port: ${_LLM_PORT}" >&2

    # 2. Ollama 서버 실행
    #
    # setsid  : 새 세션/프로세스 그룹 → PID == PGID → 그룹 kill 가능
    # env ... : OLLAMA_HOST를 서버 프로세스에만 한정 주입
    #           (export하면 이미 실행 중인 ollama 클라이언트에도 영향)
    echo "[llm] Starting Ollama server on ${_LLM_HOST}..." >&2
    setsid env OLLAMA_HOST="${_LLM_HOST}" ollama serve >/dev/null 2>&1 &
    _LLM_CHILD_PID=$!

    # 3. 트랩 설정
    # EXIT trap: health check 실패(exit 1) 포함 모든 종료 경로에서 cleanup 보장
    trap '_llm_cleanup'         EXIT
    trap '_llm_on_exit SIGTERM' SIGTERM
    trap '_llm_on_exit SIGINT'  SIGINT
    trap '_llm_on_exit SIGHUP'  SIGHUP

    # 4. Health Check
    #
    # 두 조건 동시 확인:
    #   a) kill -0: 내 자식 PID 생존 여부
    #               → 프로세스 소멸 즉시 감지, 타인 Ollama 통과 방지
    #   b) /dev/tcp: 포트 LISTEN 여부 (curl보다 빠름, 외부 명령 없음)
    #               → connect 성공 = ollama가 포트를 열었음을 확인
    echo "[llm] Waiting for LLM to be ready..." >&2
    local retries=30 is_ready=0
    while (( retries-- > 0 )); do
        if ! kill -0 "${_LLM_CHILD_PID}" 2>/dev/null; then
            echo "[llm] ERROR: Ollama process (PID: ${_LLM_CHILD_PID}) died unexpectedly." >&2
            exit 1   # EXIT trap → _llm_cleanup 자동 호출
        fi
        if (echo > /dev/tcp/127.0.0.1/${_LLM_PORT}) 2>/dev/null; then
            is_ready=1; break
        fi
        sleep 1
    done

    if (( is_ready == 0 )); then
        echo "[llm] ERROR: LLM server did not respond within timeout." >&2
        exit 1
    fi

    echo "[llm] LLM is fully operational." >&2
    echo "[llm] --------------------------------------------------" >&2

    # 5. 사용자 명령어 실행
    # 보안: $@ 를 로그에 출력하지 않음 (토큰/패스워드 노출 방지)
    echo "[llm] Executing designated task..." >&2
    "$@"
    local exit_code=$?

    echo "[llm] --------------------------------------------------" >&2
    echo "[llm] Task finished with exit code ${exit_code}." >&2

    return "${exit_code}"   # EXIT trap → _llm_cleanup 자동 호출
}
