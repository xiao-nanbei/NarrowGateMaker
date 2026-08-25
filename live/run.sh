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

# Scrub interpreter/loader startup hooks before the first external helper.
# The deployment wrapper also starts this script from a clean environment;
# this second boundary protects direct operator invocation.
unset BASH_ENV ENV LD_PRELOAD LD_LIBRARY_PATH LD_AUDIT LD_PROFILE
unset LD_DEBUG LD_DEBUG_OUTPUT LD_ORIGIN_PATH
DIR="$(builtin cd "$(/usr/bin/dirname "$0")/.." && /bin/pwd -P)"
RUN_SH="$DIR/live/run.sh"
PID_FILE="$DIR/logs/maker.pid"
CHILD_PID_FILE="$DIR/logs/maker.child.pid"
LOG_FILE="$DIR/logs/maker.log"
SUPERVISOR_LOG_FILE="$DIR/logs/maker.supervisor.log"
SUPERVISOR_STATE_FILE="$DIR/logs/maker.supervisor.state"
STOP_STATE_FILE="$DIR/logs/maker.stop.state"
RUNTIME_HEALTH_FILE="$DIR/logs/runtime_health.json"
MAIN_PY="$DIR/live/main.py"
CONFIG_FILE="${NARROWGATE_LIVE_CONFIG:-$DIR/live/config.yaml}"
DRY_RUN_CONFIG_FILE="${NARROWGATE_LIVE_CONFIG:-$DIR/live/formal_dry_run_public.yaml}"
DRY_RUN_TIMEOUT_S="${NARROWGATE_DRY_RUN_TIMEOUT_S:-30}"
PROFILE_STATE_FILE="$DIR/logs/maker.profile"
PREFLIGHT_STATE_FILE="$DIR/logs/maker.preflight.json"
SUPERVISOR_MAX_RESTARTS="${NARROWGATE_SUPERVISOR_MAX_RESTARTS:-0}"
SUPERVISOR_BACKOFF_S="${NARROWGATE_SUPERVISOR_BACKOFF_S:-5}"
EXECUTION_STATE_UNCERTAIN_EXIT_CODE=78
# Deploy proves a cache-free, content-bound site-packages tree before this
# launcher is allowed to run it.  Keep that closure immutable: CPython must
# neither consult the user site nor create new bytecode after the proof.
export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1
STARTUP_STATIC_GATE_REQUIRED=0
# Every real live start in this release is deployment-bound.  This remains
# true even if an attacker or operator deletes `.venv-active`; selector absence
# must fail closed rather than select a developer/system interpreter.
LIVE_START_REQUIRES_STATIC_RUNTIME=1
if [[ "$LIVE_START_REQUIRES_STATIC_RUNTIME" == "1" ]]; then
    case "${1:-}" in
        start|restart|__supervise) STARTUP_STATIC_GATE_REQUIRED=1 ;;
    esac
fi
if [[ -e "$DIR/.venv-active" || -L "$DIR/.venv-active" ]]; then
    # A deployed runtime selector is authority-bound.  Do not silently fall
    # back to the developer venv if the selector is broken or tampered with.
    STARTUP_STATIC_GATE_REQUIRED=1
    PYTHON_BIN="$DIR/.venv-active/bin/python3"
elif [[ -x "$DIR/.venv/bin/python3" ]]; then
    PYTHON_BIN="$DIR/.venv/bin/python3"
else
    PYTHON_BIN="$(command -v python3)"
fi
LAUNCH_HOME="${HOME:-}"
readonly DIR RUN_SH MAIN_PY CONFIG_FILE PYTHON_BIN STARTUP_STATIC_GATE_REQUIRED
readonly LIVE_START_REQUIRES_STATIC_RUNTIME LAUNCH_HOME

_runtime_file_sha256() {
    /usr/bin/sha256sum "$1" | /usr/bin/awk '{print $1}'
}

_runtime_file_nlink() {
    /usr/bin/stat -c '%h' "$1"
}

_runtime_file_mode() {
    /usr/bin/stat -c '%a' "$1"
}

_verify_startup_static_runtime() {
    [[ "$STARTUP_STATIC_GATE_REQUIRED" == "1" ]] || return 0

    local name value
    for name in \
        NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH \
        NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256 \
        NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256 \
        NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH \
        NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256 \
        NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH \
        NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256; do
        if [[ "${!name+x}" != "x" || -z "${!name}" ]]; then
            echo "Missing deployment-bound startup authority: $name" >&2
            return 1
        fi
    done

    local authority="$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH"
    local verifier="$NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH"
    local trusted_python="$NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH"
    local expected
    for expected in \
        "$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256" \
        "$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256" \
        "$NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256" \
        "$NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256"; do
        [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || {
            echo "Invalid deployment-bound startup SHA256" >&2
            return 1
        }
    done
    [[ "$authority" == /* && "$verifier" == /* && "$trusted_python" == /* ]] || {
        echo "Startup authority paths must be absolute" >&2
        return 1
    }
    [[ ! -L "$trusted_python" && -f "$trusted_python" && -x "$trusted_python" ]] || {
        echo "Trusted startup Python inode is unsafe" >&2
        return 1
    }
    [[ ! -L "$verifier" && -f "$verifier" ]] || {
        echo "Startup verifier inode is unsafe" >&2
        return 1
    }
    [[ ! -L "$authority" && -f "$authority" ]] || {
        echo "Startup authority inode is unsafe" >&2
        return 1
    }
    [[ "$(_runtime_file_nlink "$trusted_python")" == "1" \
        && "$(_runtime_file_nlink "$verifier")" == "1" \
        && "$(_runtime_file_nlink "$authority")" == "1" ]] || {
        echo "Startup authority files must have one hard link" >&2
        return 1
    }
    local authority_mode
    authority_mode="$(_runtime_file_mode "$authority")"
    [[ "$authority_mode" == "400" || "$authority_mode" == "600" ]] || {
        echo "Startup authority mode is unsafe: $authority_mode" >&2
        return 1
    }
    [[ "$(_runtime_file_sha256 "$trusted_python")" \
        == "$NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256" \
        && "$(_runtime_file_sha256 "$verifier")" \
        == "$NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256" \
        && "$(_runtime_file_sha256 "$authority")" \
        == "$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256" ]] || {
        echo "Startup authority file hash drifted" >&2
        return 1
    }

    # Clear loader and Python injection surfaces before the trusted verifier is
    # loaded.  The verifier then rechecks its own bytes, both Git trees, the
    # selector, release receipt, target interpreter, and every installed file.
    unset LD_PRELOAD LD_LIBRARY_PATH LD_AUDIT LD_PROFILE LD_DEBUG
    unset LD_DEBUG_OUTPUT LD_ORIGIN_PATH DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH
    /usr/bin/env -i \
        HOME="$LAUNCH_HOME" \
        PATH=/usr/bin:/bin \
        PYTHONDONTWRITEBYTECODE=1 \
        PYTHONNOUSERSITE=1 \
        "$trusted_python" -I -S "$verifier" \
        --authority "$authority" \
        --expected-file-sha256 \
        "$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256" \
        --expected-canonical-sha256 \
        "$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256"
}

readonly -f _runtime_file_sha256 _runtime_file_nlink _runtime_file_mode
readonly -f _verify_startup_static_runtime

# Check if a PID is alive AND is our python process (not a reused PID)
_is_our_process() {
    local pid=$1
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    # Accept both the absolute path used by this script and the historical
    # relative ``live/main.py`` form.  Missing the latter can leave an old
    # process alive and create duplicate bid/ask orders after restart.
    local command
    command=$(ps -p "$pid" -o command= 2>/dev/null || true)
    [[ "$command" == *"$MAIN_PY"* \
        || "$command" =~ (^|[[:space:]])live/main\.py([[:space:]]|$) \
        || ( "$command" == *"$RUN_SH"* && "$command" == *"__supervise"* ) ]]
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

_find_supervisor_pids() {
    ps ax -o pid=,command= 2>/dev/null \
        | awk -v runner="$RUN_SH" '
            index($0, runner) > 0 && $0 ~ /(^|[[:space:]])__supervise([[:space:]]|$)/ {
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
    local f05_boolean_owner_override_set=0
    local f05_boolean_owner_override_value=""
    local f05_buy_e3_release_path_set=0
    local f05_buy_e3_release_path_value=""
    local f05_buy_e3_release_file_sha256_set=0
    local f05_buy_e3_release_file_sha256_value=""
    local f05_buy_e3_release_canonical_sha256_set=0
    local f05_buy_e3_release_canonical_sha256_value=""
    local live_safety_successor_path_set=0
    local live_safety_successor_path_value=""
    local live_safety_successor_file_sha256_set=0
    local live_safety_successor_file_sha256_value=""
    local live_safety_successor_canonical_sha256_set=0
    local live_safety_successor_canonical_sha256_value=""
    local startup_exchange_reconciliation_path_set=0
    local startup_exchange_reconciliation_path_value=""
    local startup_exchange_reconciliation_file_sha256_set=0
    local startup_exchange_reconciliation_file_sha256_value=""
    local startup_exchange_reconciliation_canonical_sha256_set=0
    local startup_exchange_reconciliation_canonical_sha256_value=""
    local startup_exchange_reconciliation_account_key_sha256_set=0
    local startup_exchange_reconciliation_account_key_sha256_value=""
    local startup_static_runtime_authority_path_set=0
    local startup_static_runtime_authority_path_value=""
    local startup_static_runtime_authority_file_sha256_set=0
    local startup_static_runtime_authority_file_sha256_value=""
    local startup_static_runtime_authority_canonical_sha256_set=0
    local startup_static_runtime_authority_canonical_sha256_value=""
    local startup_static_runtime_verifier_path_set=0
    local startup_static_runtime_verifier_path_value=""
    local startup_static_runtime_verifier_sha256_set=0
    local startup_static_runtime_verifier_sha256_value=""
    local startup_static_trusted_python_path_set=0
    local startup_static_trusted_python_path_value=""
    local startup_static_trusted_python_sha256_set=0
    local startup_static_trusted_python_sha256_value=""
    if [[ "${NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY+set}" == "set" ]]; then
        f05_buy_e3_owner_override_set=1
        f05_buy_e3_owner_override_value="$NARROWGATE_ALLOW_F05_BUY_E3_OWNER_DEPLOY"
    fi
    if [[ "${NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY+set}" == "set" ]]; then
        f05_boolean_owner_override_set=1
        f05_boolean_owner_override_value="$NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY"
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
    if [[ "${NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH+set}" == "set" ]]; then
        live_safety_successor_path_set=1
        live_safety_successor_path_value="$NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH"
    fi
    if [[ "${NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256+set}" == "set" ]]; then
        live_safety_successor_file_sha256_set=1
        live_safety_successor_file_sha256_value="$NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256"
    fi
    if [[ "${NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256+set}" == "set" ]]; then
        live_safety_successor_canonical_sha256_set=1
        live_safety_successor_canonical_sha256_value="$NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH+set}" == "set" ]]; then
        startup_exchange_reconciliation_path_set=1
        startup_exchange_reconciliation_path_value="$NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH"
    fi
    if [[ "${NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256+set}" == "set" ]]; then
        startup_exchange_reconciliation_file_sha256_set=1
        startup_exchange_reconciliation_file_sha256_value="$NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256+set}" == "set" ]]; then
        startup_exchange_reconciliation_canonical_sha256_set=1
        startup_exchange_reconciliation_canonical_sha256_value="$NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256+set}" == "set" ]]; then
        startup_exchange_reconciliation_account_key_sha256_set=1
        startup_exchange_reconciliation_account_key_sha256_value="$NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH+set}" == "set" ]]; then
        startup_static_runtime_authority_path_set=1
        startup_static_runtime_authority_path_value="$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256+set}" == "set" ]]; then
        startup_static_runtime_authority_file_sha256_set=1
        startup_static_runtime_authority_file_sha256_value="$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256+set}" == "set" ]]; then
        startup_static_runtime_authority_canonical_sha256_set=1
        startup_static_runtime_authority_canonical_sha256_value="$NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH+set}" == "set" ]]; then
        startup_static_runtime_verifier_path_set=1
        startup_static_runtime_verifier_path_value="$NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256+set}" == "set" ]]; then
        startup_static_runtime_verifier_sha256_set=1
        startup_static_runtime_verifier_sha256_value="$NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH+set}" == "set" ]]; then
        startup_static_trusted_python_path_set=1
        startup_static_trusted_python_path_value="$NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH"
    fi
    if [[ "${NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256+set}" == "set" ]]; then
        startup_static_trusted_python_sha256_set=1
        startup_static_trusted_python_sha256_value="$NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256"
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
    if [[ "$f05_boolean_owner_override_set" == "1" ]]; then
        export NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY="$f05_boolean_owner_override_value"
    else
        unset NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_OWNER_DEPLOY
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
    if [[ "$live_safety_successor_path_set" == "1" ]]; then
        export NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH="$live_safety_successor_path_value"
    else
        unset NARROWGATE_LIVE_SAFETY_SUCCESSOR_PATH
    fi
    if [[ "$live_safety_successor_file_sha256_set" == "1" ]]; then
        export NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256="$live_safety_successor_file_sha256_value"
    else
        unset NARROWGATE_LIVE_SAFETY_SUCCESSOR_FILE_SHA256
    fi
    if [[ "$live_safety_successor_canonical_sha256_set" == "1" ]]; then
        export NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256="$live_safety_successor_canonical_sha256_value"
    else
        unset NARROWGATE_LIVE_SAFETY_SUCCESSOR_CANONICAL_SHA256
    fi
    if [[ "$startup_exchange_reconciliation_path_set" == "1" ]]; then
        export NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH="$startup_exchange_reconciliation_path_value"
    else
        unset NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH
    fi
    if [[ "$startup_exchange_reconciliation_file_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256="$startup_exchange_reconciliation_file_sha256_value"
    else
        unset NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256
    fi
    if [[ "$startup_exchange_reconciliation_canonical_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256="$startup_exchange_reconciliation_canonical_sha256_value"
    else
        unset NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256
    fi
    if [[ "$startup_exchange_reconciliation_account_key_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256="$startup_exchange_reconciliation_account_key_sha256_value"
    else
        unset NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256
    fi
    if [[ "$startup_static_runtime_authority_path_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH="$startup_static_runtime_authority_path_value"
    else
        unset NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_PATH
    fi
    if [[ "$startup_static_runtime_authority_file_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256="$startup_static_runtime_authority_file_sha256_value"
    else
        unset NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_FILE_SHA256
    fi
    if [[ "$startup_static_runtime_authority_canonical_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256="$startup_static_runtime_authority_canonical_sha256_value"
    else
        unset NARROWGATE_STARTUP_STATIC_RUNTIME_AUTHORITY_CANONICAL_SHA256
    fi
    if [[ "$startup_static_runtime_verifier_path_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH="$startup_static_runtime_verifier_path_value"
    else
        unset NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_PATH
    fi
    if [[ "$startup_static_runtime_verifier_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256="$startup_static_runtime_verifier_sha256_value"
    else
        unset NARROWGATE_STARTUP_STATIC_RUNTIME_VERIFIER_SHA256
    fi
    if [[ "$startup_static_trusted_python_path_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH="$startup_static_trusted_python_path_value"
    else
        unset NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_PATH
    fi
    if [[ "$startup_static_trusted_python_sha256_set" == "1" ]]; then
        export NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256="$startup_static_trusted_python_sha256_value"
    else
        unset NARROWGATE_STARTUP_STATIC_TRUSTED_PYTHON_SHA256
    fi
    # Mutable host environment files cannot undo the locked-runtime proof.
    unset PYTHONPATH PYTHONHOME PYTHONSTARTUP PYTHONUSERBASE PYTHONINSPECT
    unset PYTHONWARNINGS PYTHONBREAKPOINT PYTHONMALLOC PYTHONPLATLIBDIR
    unset PYTHONSAFEPATH LD_PRELOAD LD_LIBRARY_PATH LD_AUDIT LD_PROFILE
    unset LD_DEBUG LD_DEBUG_OUTPUT LD_ORIGIN_PATH BASH_ENV ENV
    unset DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH
    export PYTHONDONTWRITEBYTECODE=1
    export PYTHONNOUSERSITE=1
}

_run_deploy_preflight() {
    local preflight_tmp="$PREFLIGHT_STATE_FILE.tmp.$$"
    _verify_startup_static_runtime
    "$PYTHON_BIN" -I -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else "NarrowGate requires Python >=3.11")'
    _verify_startup_static_runtime
    if ! "$PYTHON_BIN" -I "$DIR/scripts/preflight_live_deploy.py" \
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
    _verify_startup_static_runtime
    "$PYTHON_BIN" -I -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else "NarrowGate requires Python >=3.11")'
    _verify_startup_static_runtime
    "$PYTHON_BIN" -I "$MAIN_PY" \
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

    local escalation=0
    # Stage 2: SIGINT (in case SIGTERM handler is stuck in sleep)
    if kill -0 "$pid" 2>/dev/null; then
        escalation=1
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
        escalation=2
        echo "  SIGKILL → $pid"
        kill -9 "$pid" 2>/dev/null || true
        sleep 0.5
    fi
    if kill -0 "$pid" 2>/dev/null; then
        return 3
    fi
    return "$escalation"
}

_validate_supervisor_policy() {
    if [[ ! "$SUPERVISOR_MAX_RESTARTS" =~ ^[0-9]+$ ]]; then
        echo "NARROWGATE_SUPERVISOR_MAX_RESTARTS must be a non-negative integer" >&2
        return 1
    fi
    if [[ "$SUPERVISOR_MAX_RESTARTS" != "0" ]]; then
        echo "automatic live restarts are disabled; NARROWGATE_SUPERVISOR_MAX_RESTARTS must be 0" >&2
        return 1
    fi
    if [[ ! "$SUPERVISOR_BACKOFF_S" =~ ^[1-9][0-9]*$ ]]; then
        echo "NARROWGATE_SUPERVISOR_BACKOFF_S must be a positive integer" >&2
        return 1
    fi
}

_record_supervisor_state() {
    local state=$1 exit_code=${2:-} restart_count=${3:-0}
    local temporary="$SUPERVISOR_STATE_FILE.tmp.$$"
    umask 077
    {
        echo "state=$state"
        echo "supervisor_pid=$$"
        echo "child_pid=${_supervisor_child_pid:-}"
        echo "last_exit_code=$exit_code"
        echo "restart_count=$restart_count"
        echo "max_restarts=$SUPERVISOR_MAX_RESTARTS"
        echo "backoff_s=$SUPERVISOR_BACKOFF_S"
        echo "updated_at_epoch=$(date +%s)"
    } > "$temporary"
    mv "$temporary" "$SUPERVISOR_STATE_FILE"
}

_supervisor_child_pid=""
_supervisor_stop_requested=0
_supervisor_signal_generation=0

_forward_supervisor_stop() {
    _supervisor_stop_requested=1
    _supervisor_signal_generation=$((_supervisor_signal_generation + 1))
    if [[ -n "$_supervisor_child_pid" ]] && kill -0 "$_supervisor_child_pid" 2>/dev/null; then
        kill -TERM "$_supervisor_child_pid" 2>/dev/null || true
    fi
}

_wait_for_supervisor_child() {
    # Bash interrupts ``wait`` when a trapped signal is delivered and reports
    # 128+signal even though the child is still running.  Keep waiting for the
    # same child whenever the trap generation changed during the wait.  A
    # second wait returning 127 means the preceding wait already reaped the
    # child, so preserve that real status instead of inventing a new one.
    local child_pid=$1
    local signal_generation_before
    local child_exit=127
    local last_reaped_exit=127
    while true; do
        signal_generation_before=$_supervisor_signal_generation
        wait "$child_pid"
        child_exit=$?
        if [[ $child_exit -ne 127 ]]; then
            last_reaped_exit=$child_exit
        fi
        if [[ $_supervisor_signal_generation -ne $signal_generation_before ]]; then
            continue
        fi
        if [[ $child_exit -eq 127 && $last_reaped_exit -ne 127 ]]; then
            return "$last_reaped_exit"
        fi
        return "$child_exit"
    done
}

_cleanup_supervisor() {
    if [[ -f "$CHILD_PID_FILE" ]] \
        && [[ "$(cat "$CHILD_PID_FILE" 2>/dev/null || true)" == "$_supervisor_child_pid" ]]; then
        rm -f "$CHILD_PID_FILE"
    fi
    if [[ -f "$PID_FILE" ]] \
        && [[ "$(cat "$PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
        rm -f "$PID_FILE"
    fi
}

supervise() {
    _validate_supervisor_policy
    mkdir -p "$DIR/logs"
    trap _forward_supervisor_stop TERM INT
    trap _cleanup_supervisor EXIT

    local restart_count=0
    while true; do
        if [[ $restart_count -gt 0 ]]; then
            if ! _run_deploy_preflight; then
                _record_supervisor_state "preflight_failed" 125 "$restart_count"
                return 125
            fi
        fi

        _supervisor_child_pid=""
        _verify_startup_static_runtime
        "$PYTHON_BIN" -I "$MAIN_PY" --config "$CONFIG_FILE" &
        _supervisor_child_pid=$!
        # Close the start/stop race where TERM arrived after the trap was
        # installed but before the freshly spawned PID was published.
        if [[ $_supervisor_stop_requested -eq 1 ]] \
            && kill -0 "$_supervisor_child_pid" 2>/dev/null; then
            kill -TERM "$_supervisor_child_pid" 2>/dev/null || true
        fi
        echo "$_supervisor_child_pid" > "$CHILD_PID_FILE"
        _record_supervisor_state "running" "" "$restart_count"

        local child_exit
        set +e
        _wait_for_supervisor_child "$_supervisor_child_pid"
        child_exit=$?
        set -e
        rm -f "$CHILD_PID_FILE"

        if [[ $_supervisor_stop_requested -eq 1 ]]; then
            if [[ $child_exit -eq 0 ]]; then
                _record_supervisor_state "stopped_by_operator" 0 "$restart_count"
                return 0
            fi
            if [[ $child_exit -eq $EXECUTION_STATE_UNCERTAIN_EXIT_CODE ]]; then
                _record_supervisor_state \
                    "reconciliation_required_no_restart" \
                    "$child_exit" \
                    "$restart_count"
                return "$child_exit"
            fi
            _record_supervisor_state "fatal_exit_no_restart" "$child_exit" "$restart_count"
            return "$child_exit"
        fi
        if [[ $child_exit -eq 0 ]]; then
            _record_supervisor_state "clean_exit_no_restart" 0 "$restart_count"
            return 0
        fi
        if [[ $child_exit -eq $EXECUTION_STATE_UNCERTAIN_EXIT_CODE ]]; then
            _record_supervisor_state \
                "reconciliation_required_no_restart" \
                "$child_exit" \
                "$restart_count"
            return "$child_exit"
        fi
        # Unknown nonzero exits are not proven transient.  Restarting a fresh
        # process could seed over an unresolved fill/campaign state, so the
        # supervisor exposes the failure and waits for operator reconciliation.
        _record_supervisor_state "fatal_exit_no_restart" "$child_exit" "$restart_count"
        return "$child_exit"
    done
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
    local existing_supervisors
    existing_supervisors=$(_find_supervisor_pids)
    if [[ -n "$existing_supervisors" ]]; then
        echo "Already running (supervisor PID(s): $(printf '%s' "$existing_supervisors" | tr '\n' ' '))"
        return 1
    fi
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
    _validate_supervisor_policy
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

    rm -f "$CHILD_PID_FILE" "$SUPERVISOR_STATE_FILE" "$RUNTIME_HEALTH_FILE"
    # The supervisor makes silent exits and stale PIDs visible.  It never
    # restarts an unclassified non-zero exit without operator reconciliation.
    local -a clean_environment=()
    local environment_entry
    while IFS= read -r -d '' environment_entry; do
        case "$environment_entry" in
            BASH_ENV=*|ENV=*|BASH_FUNC_*=*) continue ;;
        esac
        clean_environment+=("$environment_entry")
    done < <(/usr/bin/env -0)
    /usr/bin/nohup /usr/bin/env -i "${clean_environment[@]}" \
        /bin/bash --noprofile --norc "$RUN_SH" __supervise \
        > "$SUPERVISOR_LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"

    # Verify it actually started (give it 2s)
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        echo "Started supervisor (PID $pid)"
        echo "Python: $PYTHON_BIN"
        echo "Config: $CONFIG_FILE"
        echo "Profile: ${NARROWGATE_LIVE_PROFILE_NAME:-unmanaged}"
        echo "Preflight: $PREFLIGHT_STATE_FILE"
        echo "Log: $LOG_FILE"
        echo "Supervisor: max_restarts=$SUPERVISOR_MAX_RESTARTS backoff=${SUPERVISOR_BACKOFF_S}s"
    else
        rm -f "$PID_FILE"
        echo "Failed to start — check $LOG_FILE"
        return 1
    fi
}

stop() {
    local stopped=false
    local unsafe=false
    local supervisor_pid=""
    local child_pid=""
    local child_exit=""
    local supervisor_state=""
    local kill_escalation=0
    local orphan_count=0
    rm -f "$STOP_STATE_FILE"

    # Kill by PID file
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if _is_our_process "$pid"; then
            supervisor_pid="$pid"
            if [[ -s "$CHILD_PID_FILE" ]]; then
                child_pid=$(cat "$CHILD_PID_FILE")
            fi
            echo "Stopping PID $pid ..."
            if _kill_pid "$pid" "supervisor"; then
                kill_escalation=0
            else
                kill_escalation=$?
                unsafe=true
            fi
            stopped=true
            if [[ -f "$SUPERVISOR_STATE_FILE" ]]; then
                supervisor_state=$(awk -F= '$1=="state" {print $2}' "$SUPERVISOR_STATE_FILE")
                child_exit=$(awk -F= '$1=="last_exit_code" {print $2}' "$SUPERVISOR_STATE_FILE")
            fi
            if [[ "$supervisor_state" != "stopped_by_operator" || "$child_exit" != "0" ]]; then
                unsafe=true
            fi
        fi
        rm -f "$PID_FILE"
    fi
    rm -f "$CHILD_PID_FILE"

    # Also kill any orphan maker processes (missed by PID file)
    local orphans
    orphans=$(_find_maker_pids)
    if [[ -n "$orphans" ]]; then
        echo "Cleaning orphan processes..."
        while IFS= read -r opid; do
            [[ -z "$opid" ]] && continue
            ((++orphan_count))
            if _kill_pid "$opid" "orphan"; then
                :
            else
                kill_escalation=$?
            fi
            unsafe=true
        done <<< "$orphans"
        stopped=true
    fi

    local orphan_supervisors
    orphan_supervisors=$(_find_supervisor_pids)
    if [[ -n "$orphan_supervisors" ]]; then
        echo "Cleaning orphan supervisors..."
        while IFS= read -r opid; do
            [[ -z "$opid" ]] && continue
            ((++orphan_count))
            if _kill_pid "$opid" "orphan-supervisor"; then
                :
            else
                kill_escalation=$?
            fi
            unsafe=true
        done <<< "$orphan_supervisors"
        stopped=true
    fi

    # A disappeared supervisor can still have left an execution-state-uncertain
    # child exit.  Absence of a PID is not authority to erase that state.
    if ! $stopped && [[ -f "$SUPERVISOR_STATE_FILE" ]]; then
        supervisor_state=$(awk -F= '$1=="state" {print $2}' "$SUPERVISOR_STATE_FILE")
        child_exit=$(awk -F= '$1=="last_exit_code" {print $2}' "$SUPERVISOR_STATE_FILE")
        if [[ "$supervisor_state" != "stopped_by_operator" \
            && "$supervisor_state" != "clean_exit_no_restart" ]]; then
            unsafe=true
        elif [[ -n "$child_exit" && "$child_exit" != "0" ]]; then
            unsafe=true
        fi
    fi

    local temporary="$STOP_STATE_FILE.tmp.$$"
    umask 077
    {
        echo "schema=narrowgate_live_stop_state.v1"
        echo "supervisor_pid=$supervisor_pid"
        echo "child_pid=$child_pid"
        echo "supervisor_state=$supervisor_state"
        echo "child_exit_code=$child_exit"
        echo "kill_escalation=$kill_escalation"
        echo "orphan_count=$orphan_count"
        echo "clean=$([[ "$unsafe" == false ]] && echo 1 || echo 0)"
        echo "updated_at_epoch=$(date +%s)"
    } > "$temporary"
    mv "$temporary" "$STOP_STATE_FILE"

    if $unsafe; then
        echo "STOP_RESULT clean=0 child_exit=${child_exit:-unknown} "\
"kill_escalation=$kill_escalation orphan_count=$orphan_count" >&2
        return "$EXECUTION_STATE_UNCERTAIN_EXIT_CODE"
    elif $stopped; then
        echo "STOP_RESULT clean=1 child_exit=0 kill_escalation=0 orphan_count=0"
    else
        echo "Not running"
    fi
}

restart() {
    # Validate the candidate runtime before stopping the healthy process.
    mkdir -p "$DIR/logs"
    _load_runtime_environment
    _run_deploy_preflight
    stop
    sleep 1
    start
}

status() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if _is_our_process "$pid"; then
            echo "Running (supervisor PID $pid)"
            ps -p "$pid" -o pid,etime,rss,command | tail -1
            if [[ -f "$CHILD_PID_FILE" ]]; then
                local child_pid
                child_pid=$(cat "$CHILD_PID_FILE")
                if [[ "$child_pid" =~ ^[0-9]+$ ]] && kill -0 "$child_pid" 2>/dev/null; then
                    echo "Child PID: $child_pid"
                fi
            fi
            [[ -f "$PROFILE_STATE_FILE" ]] && cat "$PROFILE_STATE_FILE"
            [[ -f "$SUPERVISOR_STATE_FILE" ]] && cat "$SUPERVISOR_STATE_FILE"
            [[ -f "$RUNTIME_HEALTH_FILE" ]] && cat "$RUNTIME_HEALTH_FILE"
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
    [[ -f "$SUPERVISOR_STATE_FILE" ]] && cat "$SUPERVISOR_STATE_FILE"
    [[ -f "$RUNTIME_HEALTH_FILE" ]] && cat "$RUNTIME_HEALTH_FILE"
    return 1
}

reload() {
    if [[ -f "$CHILD_PID_FILE" ]]; then
        local child_pid
        child_pid=$(cat "$CHILD_PID_FILE")
        if [[ "$child_pid" =~ ^[0-9]+$ ]] && kill -0 "$child_pid" 2>/dev/null; then
            kill -HUP "$child_pid"
            echo "Reload signal sent"
            return 0
        fi
    fi
    if [[ -f "$PID_FILE" ]] && _is_our_process "$(cat "$PID_FILE")"; then
        echo "Supervisor is running without a live child; reload was not sent"
        return 1
    else
        echo "Not running"
        return 1
    fi
}

logs() {
    tail -f "$LOG_FILE"
}

case "${1:-help}" in
    __supervise) supervise ;;
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
