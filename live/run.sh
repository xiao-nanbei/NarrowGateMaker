#!/usr/bin/env bash
# NarrowGate Maker Engine — 后台启动/停止/状态管理
#
# Usage:
#   ./live/run.sh start    # 后台启动
#   ./live/run.sh stop     # 优雅停止
#   ./live/run.sh restart  # 重启
#   ./live/run.sh status   # 查看状态
#   ./live/run.sh logs     # tail 日志
#   ./live/run.sh reload   # 热重载配置 (SIGHUP)
#   ./live/run.sh profile  # 显示将被持久化的 runtime profile
#   ./live/run.sh dry-run  # 限时本地校验；零网络、零订单

set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$DIR/logs/maker.pid"
LOG_FILE="$DIR/logs/maker.log"
MAIN_PY="$DIR/live/main.py"
CONFIG_FILE="${NARROWGATE_LIVE_CONFIG:-$DIR/live/config.yaml}"
DRY_RUN_CONFIG_FILE="${NARROWGATE_LIVE_CONFIG:-$DIR/live/formal_dry_run_public.yaml}"
DRY_RUN_TIMEOUT_S="${NARROWGATE_DRY_RUN_TIMEOUT_S:-30}"
PROFILE_STATE_FILE="$DIR/logs/maker.profile"
PREFLIGHT_STATE_FILE="$DIR/logs/maker.preflight.json"
if [[ -x "$DIR/.venv-active/bin/python3" ]]; then
    PYTHON_BIN="$DIR/.venv-active/bin/python3"
elif [[ -x "$DIR/.venv/bin/python3" ]]; then
    PYTHON_BIN="$DIR/.venv/bin/python3"
else
    PYTHON_BIN="$(command -v python3)"
fi

# Check if a PID is alive AND is our python process (not a reused PID)
_is_our_process() {
    local pid=$1
    kill -0 "$pid" 2>/dev/null || return 1
    # Accept both the absolute path used by this script and the historical
    # relative ``live/main.py`` form.  Missing the latter can leave an old
    # process alive and create duplicate bid/ask orders after restart.
    local command
    command=$(ps -p "$pid" -o command= 2>/dev/null || true)
    [[ "$command" == *"$MAIN_PY"* || "$command" =~ (^|[[:space:]])live/main\.py([[:space:]]|$) ]]
}

# Find all PIDs running our main.py (excluding this script)
_find_maker_pids() {
    ps ax -o pid=,command= 2>/dev/null \
        | awk -v main="$MAIN_PY" '
            /[Pp]ython/ && (index($0, main) > 0 || $0 ~ /(^|[[:space:]])live\/main\.py([[:space:]]|$)/) {
                print $1
            }
        ' || true
}

_profile_path() {
    local requested="${NARROWGATE_LIVE_PROFILE:-native}"
    if [[ -n "${NARROWGATE_LIVE_PROFILE_FILE:-}" ]]; then
        printf '%s\n' "$NARROWGATE_LIVE_PROFILE_FILE"
    elif [[ "$requested" == */* ]]; then
        printf '%s\n' "$requested"
    else
        printf '%s\n' "$DIR/live/profiles/${requested}.env"
    fi
}

_load_runtime_environment() {
    # API credentials and private host settings are deliberately separate from
    # the checked-in, non-secret native/python compute profiles.
    local requested_profile="${NARROWGATE_LIVE_PROFILE:-}"
    local requested_profile_file="${NARROWGATE_LIVE_PROFILE_FILE:-}"

    # BUY E3 activation authority is invocation-only.  Snapshot both value and
    # presence before sourcing mutable host files so an activation command
    # cannot be overridden and rollback's ``env -u`` cannot be reintroduced.
    local f05_buy_e3_owner_override_set=0
    local f05_buy_e3_owner_override_value=""
    local f05_buy_e3_release_path_set=0
    local f05_buy_e3_release_path_value=""
    local f05_buy_e3_release_file_sha256_set=0
    local f05_buy_e3_release_file_sha256_value=""
    local f05_buy_e3_release_canonical_sha256_set=0
    local f05_buy_e3_release_canonical_sha256_value=""
    if [[ "${NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY+set}" == "set" ]]; then
        f05_buy_e3_owner_override_set=1
        f05_buy_e3_owner_override_value="$NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY"
    fi
    if [[ "${NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH+set}" == "set" ]]; then
        f05_buy_e3_release_path_set=1
        f05_buy_e3_release_path_value="$NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH"
    fi
    if [[ "${NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256+set}" == "set" ]]; then
        f05_buy_e3_release_file_sha256_set=1
        f05_buy_e3_release_file_sha256_value="$NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256"
    fi
    if [[ "${NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256+set}" == "set" ]]; then
        f05_buy_e3_release_canonical_sha256_set=1
        f05_buy_e3_release_canonical_sha256_value="$NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256"
    fi

    if [[ -f "$DIR/live/.env" ]]; then
        set -a
        source "$DIR/live/.env"
        set +a
    fi
    [[ -n "$requested_profile" ]] && export NARROWGATE_LIVE_PROFILE="$requested_profile"
    [[ -n "$requested_profile_file" ]] && export NARROWGATE_LIVE_PROFILE_FILE="$requested_profile_file"

    local profile_path
    profile_path="$(_profile_path)"
    if [[ ! -f "$profile_path" ]]; then
        echo "Runtime profile not found: $profile_path" >&2
        return 1
    fi
    set -a
    source "$profile_path"
    set +a
    export NARROWGATE_LIVE_PROFILE_FILE="$profile_path"

    if [[ "$f05_buy_e3_owner_override_set" == "1" ]]; then
        export NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY="$f05_buy_e3_owner_override_value"
    else
        unset NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY
    fi
    if [[ "$f05_buy_e3_release_path_set" == "1" ]]; then
        export NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH="$f05_buy_e3_release_path_value"
    else
        unset NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_PATH
    fi
    if [[ "$f05_buy_e3_release_file_sha256_set" == "1" ]]; then
        export NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256="$f05_buy_e3_release_file_sha256_value"
    else
        unset NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_FILE_SHA256
    fi
    if [[ "$f05_buy_e3_release_canonical_sha256_set" == "1" ]]; then
        export NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256="$f05_buy_e3_release_canonical_sha256_value"
    else
        unset NARROWGATE_F05_BUY_E3_ACTIVE_RELEASE_CANONICAL_SHA256
    fi
}

_run_deploy_preflight() {
    local preflight_tmp="$PREFLIGHT_STATE_FILE.tmp.$$"
    "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else "NarrowGate requires Python >=3.11")'
    if ! "$PYTHON_BIN" "$DIR/scripts/preflight_live_deploy.py" \
        --config "$CONFIG_FILE" \
        --repo-root "$DIR" > "$preflight_tmp"; then
        rm -f "$preflight_tmp"
        echo "Live preflight failed; maker was not started." >&2
        return 1
    fi
    mv "$preflight_tmp" "$PREFLIGHT_STATE_FILE"
}

profile() {
    _load_runtime_environment
    echo "Profile: ${NARROWGATE_LIVE_PROFILE_NAME:-unmanaged}"
    echo "File: ${NARROWGATE_LIVE_PROFILE_FILE:-unknown}"
    echo "NARROWGATE_CPP_QUOTE_CORE=${NARROWGATE_CPP_QUOTE_CORE:-0}"
    echo "NARROWGATE_CPP_SIGNAL_FEATURES=${NARROWGATE_CPP_SIGNAL_FEATURES:-0}"
    echo "NARROWGATE_CPP_GLOBAL_FLOW=${NARROWGATE_CPP_GLOBAL_FLOW:-0}"
    echo "NARROWGATE_CPP_LIVE_ROUTING=${NARROWGATE_CPP_LIVE_ROUTING:-0}"
    echo "NARROWGATE_CPP_STRICT=${NARROWGATE_CPP_STRICT:-0}"
}

dry_run() {
    # Deliberately do not source live/.env or a runtime profile. The formal
    # dry-run is local-only and exits before credentials or network code matter.
    "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else "NarrowGate requires Python >=3.11")'
    "$PYTHON_BIN" "$MAIN_PY" \
        --dry-run \
        --dry-run-timeout-s "$DRY_RUN_TIMEOUT_S" \
        --config "$DRY_RUN_CONFIG_FILE"
}

# Kill a single PID with escalation: TERM → INT → KILL
_kill_pid() {
    local pid=$1 label=${2:-""}
    [[ -n "$label" ]] && label=" ($label)"

    # Stage 1: SIGTERM (graceful)
    if kill -0 "$pid" 2>/dev/null; then
        echo "  SIGTERM → $pid$label"
        kill -TERM "$pid" 2>/dev/null || true
        local i=0
        while kill -0 "$pid" 2>/dev/null && [[ $i -lt 5 ]]; do
            sleep 1
            printf "."
            ((++i))
        done
        echo ""
    fi

    # Stage 2: SIGINT (in case SIGTERM handler is stuck in sleep)
    if kill -0 "$pid" 2>/dev/null; then
        echo "  SIGINT → $pid"
        kill -INT "$pid" 2>/dev/null || true
        local i=0
        while kill -0 "$pid" 2>/dev/null && [[ $i -lt 5 ]]; do
            sleep 1
            printf "."
            ((++i))
        done
        echo ""
    fi

    # Stage 3: SIGKILL (force)
    if kill -0 "$pid" 2>/dev/null; then
        echo "  SIGKILL → $pid"
        kill -9 "$pid" 2>/dev/null || true
        sleep 0.5
    fi
}

start() {
    # Clean stale PID file
    if [[ -f "$PID_FILE" ]]; then
        local old_pid
        old_pid=$(cat "$PID_FILE")
        if _is_our_process "$old_pid"; then
            echo "Already running (PID $old_pid)"
            return 1
        else
            rm -f "$PID_FILE"
        fi
    fi

    # The PID file may be missing after a manual launch or older deployment.
    # Never start a second maker process merely because the file is absent.
    local existing_pids
    existing_pids=$(_find_maker_pids)
    if [[ -n "$existing_pids" ]]; then
        local existing_pid
        existing_pid=$(printf '%s\n' "$existing_pids" | head -1)
        echo "$existing_pid" > "$PID_FILE"
        echo "Already running (PID(s): $(printf '%s' "$existing_pids" | tr '\n' ' '))"
        return 1
    fi

    mkdir -p "$DIR/logs"

    _load_runtime_environment
    _run_deploy_preflight
    {
        echo "profile=${NARROWGATE_LIVE_PROFILE_NAME:-unmanaged}"
        echo "profile_file=${NARROWGATE_LIVE_PROFILE_FILE:-unknown}"
        echo "cpp_quote_core=${NARROWGATE_CPP_QUOTE_CORE:-0}"
        echo "cpp_signal_features=${NARROWGATE_CPP_SIGNAL_FEATURES:-0}"
        echo "cpp_global_flow=${NARROWGATE_CPP_GLOBAL_FLOW:-0}"
        echo "cpp_live_routing=${NARROWGATE_CPP_LIVE_ROUTING:-0}"
        echo "cpp_strict=${NARROWGATE_CPP_STRICT:-0}"
        echo "preflight_identity=$PREFLIGHT_STATE_FILE"
    } > "$PROFILE_STATE_FILE"

    # stdout/stderr → /dev/null; logging is handled by RotatingFileHandler
    nohup "$PYTHON_BIN" "$MAIN_PY" --config "$CONFIG_FILE" > /dev/null 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"

    # Verify it actually started (give it 2s)
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        echo "Started (PID $pid)"
        echo "Python: $PYTHON_BIN"
        echo "Config: $CONFIG_FILE"
        echo "Profile: ${NARROWGATE_LIVE_PROFILE_NAME:-unmanaged}"
        echo "Preflight: $PREFLIGHT_STATE_FILE"
        echo "Log: $LOG_FILE"
    else
        rm -f "$PID_FILE"
        echo "Failed to start — check $LOG_FILE"
        return 1
    fi
}

stop() {
    local stopped=false

    # Kill by PID file
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if _is_our_process "$pid"; then
            echo "Stopping PID $pid ..."
            _kill_pid "$pid" "main"
            stopped=true
        fi
        rm -f "$PID_FILE"
    fi

    # Also kill any orphan maker processes (missed by PID file)
    local orphans
    orphans=$(_find_maker_pids)
    if [[ -n "$orphans" ]]; then
        echo "Cleaning orphan processes..."
        while IFS= read -r opid; do
            [[ -z "$opid" ]] && continue
            _kill_pid "$opid" "orphan"
        done <<< "$orphans"
        stopped=true
    fi

    if $stopped; then
        echo "Stopped"
    else
        echo "Not running"
    fi
}

restart() {
    # Validate the candidate runtime before stopping the healthy process.
    mkdir -p "$DIR/logs"
    _load_runtime_environment
    _run_deploy_preflight
    stop 2>/dev/null || true
    sleep 1
    start
}

status() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if _is_our_process "$pid"; then
            echo "Running (PID $pid)"
            ps -p "$pid" -o pid,etime,rss,command | tail -1
            [[ -f "$PROFILE_STATE_FILE" ]] && cat "$PROFILE_STATE_FILE"
            return 0
        else
            rm -f "$PID_FILE"
        fi
    fi

    # Check for orphans not tracked by PID file
    local orphans
    orphans=$(_find_maker_pids)
    if [[ -n "$orphans" ]]; then
        echo "Running (orphan, no PID file): $orphans"
        return 0
    fi

    echo "Not running"
    return 1
}

reload() {
    if [[ -f "$PID_FILE" ]] && _is_our_process "$(cat "$PID_FILE")"; then
        kill -HUP "$(cat "$PID_FILE")"
        echo "Reload signal sent"
    else
        echo "Not running"
        return 1
    fi
}

logs() {
    tail -f "$LOG_FILE"
}

case "${1:-help}" in
    start)   start   ;;
    stop)    stop    ;;
    restart) restart ;;
    status)  status  ;;
    dry-run) dry_run ;;
    profile) profile ;;
    reload)  reload  ;;
    logs)    logs    ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|dry-run|profile|reload|logs}"
        exit 1
        ;;
esac
