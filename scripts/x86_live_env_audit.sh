#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"

echo "== host =="
hostname || true
date -Is || true
uname -a || true

echo
echo "== os =="
if [[ -f /etc/os-release ]]; then
  sed -n '1,16p' /etc/os-release
fi

echo
echo "== cpu =="
lscpu | sed -n '1,90p'

echo
echo "== compiler/runtime =="
command -v gcc >/dev/null 2>&1 && gcc --version | head -1 || echo "gcc: missing"
command -v g++ >/dev/null 2>&1 && g++ --version | head -1 || echo "g++: missing"
command -v cmake >/dev/null 2>&1 && cmake --version | head -1 || echo "cmake: missing"
if [[ -x "$PYTHON" ]]; then
  "$PYTHON" - <<'PY'
import platform
import sys
print("python", sys.version.split()[0])
print("platform", platform.platform(), platform.machine())
try:
    import narrowgate_cpp
    print("narrowgate_cpp", narrowgate_cpp.__file__)
except Exception as exc:
    print("narrowgate_cpp: missing", repr(exc))
PY
else
  echo "python: $PYTHON missing"
fi

echo
echo "== kernel knobs =="
cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null | sed 's/^/clocksource = /' || true
cat /sys/devices/system/clocksource/clocksource0/available_clocksource 2>/dev/null | sed 's/^/available_clocksource = /' || true
for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [[ -f "$f" ]] && echo "$f = $(cat "$f")"
done
sysctl \
  kernel.perf_event_paranoid \
  kernel.nmi_watchdog \
  kernel.sched_rt_runtime_us \
  net.core.rmem_max \
  net.core.wmem_max \
  net.ipv4.tcp_congestion_control 2>/dev/null || true

echo
echo "== affinity/interrupts =="
taskset -pc $$ 2>/dev/null || true
grep -E '(CPU|eth|ena|nvme)' /proc/interrupts 2>/dev/null | sed -n '1,80p' || true

echo
echo "== python numerical backend =="
if [[ -x "$PYTHON" ]]; then
  "$PYTHON" - <<'PY'
try:
    import numpy as np
    np.show_config()
except Exception as exc:
    print(repr(exc))
PY
fi
