#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
N="${N:-100000}"
SIGNAL_N="${SIGNAL_N:-10000}"
ROUTING_N="${ROUTING_N:-200000}"
TRADES_PER_SECOND="${TRADES_PER_SECOND:-4}"
TASKSET_CPU="${TASKSET_CPU:-}"

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
except Exception as exc:
    raise SystemExit(f"narrowgate_cpp unavailable: {exc!r}")
PY
echo "N=$N SIGNAL_N=$SIGNAL_N ROUTING_N=$ROUTING_N TRADES_PER_SECOND=$TRADES_PER_SECOND TASKSET_CPU=${TASKSET_CPU:-none}"
echo "threads OMP=$OMP_NUM_THREADS OPENBLAS=$OPENBLAS_NUM_THREADS MKL=$MKL_NUM_THREADS NUMEXPR=$NUMEXPR_NUM_THREADS MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX"

echo
echo "== candidate soak: quote core + signal features =="
NARROWGATE_CPP_QUOTE_CORE=1 \
NARROWGATE_CPP_SIGNAL_FEATURES=1 \
NARROWGATE_CPP_STRICT=1 \
run_cmd "$PYTHON" bench/bench_live_path.py \
  --n "$N" \
  --signal-n "$SIGNAL_N" \
  --trades-per-second "$TRADES_PER_SECOND" \
  --engine cpp \
  --strict-cpp

echo
echo "== live routing compact ABI soak =="
run_cmd "$PYTHON" bench/bench_live_routing_bridge.py --api compact --n "$ROUTING_N"
