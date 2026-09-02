#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
N="${N:-10000}"
SIGNAL_N="${SIGNAL_N:-1000}"
TRADES_PER_SECOND="${TRADES_PER_SECOND:-4}"
TASKSET_CPU="${TASKSET_CPU:-}"
RUN_PERF="${RUN_PERF:-0}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-1}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"

cd "$ROOT"

if [[ ! -x "$PYTHON" ]]; then
  echo "Python not found or not executable: $PYTHON" >&2
  exit 2
fi

run_cmd() {
  if [[ -n "$TASKSET_CPU" ]]; then
    taskset -c "$TASKSET_CPU" "$@"
  else
    "$@"
  fi
}

bench_live_path() {
  local label="$1"
  shift
  echo
  echo "== $label =="
  run_cmd "$PYTHON" bench/bench_live_path.py \
    --n "$N" \
    --signal-n "$SIGNAL_N" \
    --trades-per-second "$TRADES_PER_SECOND" \
    "$@"
}

echo "root=$ROOT"
echo "python=$PYTHON"
"$PYTHON" - <<'PY'
import platform
import sys
print("version", sys.version.split()[0])
print("machine", platform.machine(), platform.platform())
try:
    import narrowgate_cpp
    print("narrowgate_cpp", narrowgate_cpp.__file__)
    print("live_build_profile", narrowgate_cpp.NATIVE_LIVE_BUILD_PROFILE)
    print("live_build_options", narrowgate_cpp.NATIVE_LIVE_BUILD_COMPILE_OPTIONS)
    print("live_build_production", narrowgate_cpp.NATIVE_LIVE_BUILD_IS_PRODUCTION)
except Exception as exc:
    raise SystemExit(f"narrowgate_cpp unavailable: {exc!r}")
PY
echo "N=$N SIGNAL_N=$SIGNAL_N TRADES_PER_SECOND=$TRADES_PER_SECOND TASKSET_CPU=${TASKSET_CPU:-none}"
echo "threads OMP=$OMP_NUM_THREADS OPENBLAS=$OPENBLAS_NUM_THREADS MKL=$MKL_NUM_THREADS NUMEXPR=$NUMEXPR_NUM_THREADS MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX"

bench_live_path "python baseline" --engine python

NARROWGATE_CPP_STRICT=1 \
bench_live_path "cpp quote core" --engine cpp --strict-cpp

NARROWGATE_CPP_SIGNAL_FEATURES=1 \
NARROWGATE_CPP_STRICT=1 \
bench_live_path "cpp signal features only" --engine python

NARROWGATE_CPP_QUOTE_CORE=1 \
NARROWGATE_CPP_SIGNAL_FEATURES=1 \
NARROWGATE_CPP_STRICT=1 \
bench_live_path "candidate: quote core + signal features" --engine cpp --strict-cpp

echo
echo "== live routing compact ABI =="
run_cmd "$PYTHON" bench/bench_live_routing_bridge.py --n "${ROUTING_N:-100000}"

if [[ "$RUN_PERF" == "1" ]]; then
  echo
  echo "== perf stat candidate =="
  PERF_BIN="${PERF_BIN:-perf}"
  PERF_PREFIX=()
  if [[ "${PERF_SUDO:-1}" == "1" ]]; then
    PERF_PREFIX=(sudo)
  fi
  "${PERF_PREFIX[@]}" "$PERF_BIN" stat \
    -e cycles,instructions,branches,branch-misses,L1-icache-load-misses,iTLB-load-misses,cache-misses,context-switches,cpu-migrations,page-faults \
    -- env \
    OMP_NUM_THREADS="$OMP_NUM_THREADS" \
    OPENBLAS_NUM_THREADS="$OPENBLAS_NUM_THREADS" \
    MKL_NUM_THREADS="$MKL_NUM_THREADS" \
    NUMEXPR_NUM_THREADS="$NUMEXPR_NUM_THREADS" \
    MALLOC_ARENA_MAX="$MALLOC_ARENA_MAX" \
    PYTHONHASHSEED="$PYTHONHASHSEED" \
    NARROWGATE_CPP_QUOTE_CORE=1 \
    NARROWGATE_CPP_SIGNAL_FEATURES=1 \
    NARROWGATE_CPP_STRICT=1 \
    "$PYTHON" bench/bench_live_path.py \
      --n "${PERF_N:-3000}" \
      --signal-n "${PERF_SIGNAL_N:-300}" \
      --trades-per-second "$TRADES_PER_SECOND" \
      --engine cpp \
      --strict-cpp || true
fi
