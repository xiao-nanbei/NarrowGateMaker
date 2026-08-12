#!/usr/bin/env bash
set -euo pipefail

ROOT="${NARROWGATE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CONFIG="${NARROWGATE_CONFIG:-${ROOT}/live/config.yaml}"
DURATION_S="${1:-3600}"
CAPTURE_ROOT="${ROOT}/logs/receive_time_capture"
LOCK_FILE="${CAPTURE_ROOT}/capture.lock"
STAMP="${NARROWGATE_CAPTURE_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
if [[ ! "${STAMP}" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
  echo "invalid NARROWGATE_CAPTURE_ID: ${STAMP}" >&2
  exit 64
fi
MARKER_DIR="${CAPTURE_ROOT}/${STAMP}"
TOGGLE="${ROOT}/scripts/configure_receive_time_capture.py"
MANAGER="${ROOT}/scripts/bounded_receive_time_capture.py"
PYTHON="${NARROWGATE_CAPTURE_PYTHON:-${ROOT}/.venv-active/bin/python3}"
SOURCE_PROVIDER="${NARROWGATE_CAPTURE_SOURCE_PROVIDER:?missing capture source provider}"
SOURCE_REGION="${NARROWGATE_CAPTURE_SOURCE_REGION:?missing capture source region}"
SOURCE_CITY="${NARROWGATE_CAPTURE_SOURCE_CITY:?missing capture source city}"
SOURCE_PUBLIC_IPV4="${NARROWGATE_CAPTURE_SOURCE_PUBLIC_IPV4:?missing capture source public IPv4}"
SOURCE_SSH_TARGET="${NARROWGATE_CAPTURE_SOURCE_SSH_TARGET:?missing capture source SSH target}"
START_SENTINEL="${MARKER_DIR}/capture.started"
ENABLED=0

if [[ ! -x "${PYTHON}" ]]; then
  echo "capture Python is unavailable: ${PYTHON}" >&2
  exit 69
fi
"${PYTHON}" -c 'import sys; assert sys.version_info >= (3, 10), sys.version'

mkdir -p "${CAPTURE_ROOT}"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "another bounded receive-time capture is active" >&2
  exit 75
fi
mkdir -p "${MARKER_DIR}"
touch "${START_SENTINEL}"

signal_live_reload() {
  local found=0
  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    kill -HUP "${pid}"
    found=1
  done < <(pgrep -f "${ROOT}/live/main.py --config ${CONFIG}" || true)
  if [[ "${found}" -eq 0 ]]; then
    echo "live process not found for ${CONFIG}" >&2
    return 1
  fi
}

disable_capture() {
  if [[ "${ENABLED}" -eq 1 ]]; then
    "${PYTHON}" "${TOGGLE}" \
      --config "${CONFIG}" \
      --disable \
      --queue-size 20000 \
      --no-backup \
      --marker "${MARKER_DIR}/disable.json"
    signal_live_reload || true
    ENABLED=0
  fi
}

trap disable_capture EXIT INT TERM

"${PYTHON}" "${TOGGLE}" \
  --config "${CONFIG}" \
  --enable \
  --queue-size 20000 \
  --marker "${MARKER_DIR}/enable.json"
ENABLED=1
signal_live_reload

sleep "${DURATION_S}"
disable_capture
sleep 10

"${PYTHON}" "${MANAGER}" finalize \
  --root "${ROOT}" \
  --config "${CONFIG}" \
  --marker-dir "${MARKER_DIR}" \
  --sentinel "${START_SENTINEL}" \
  --duration-s "${DURATION_S}" \
  --source-provider "${SOURCE_PROVIDER}" \
  --source-region "${SOURCE_REGION}" \
  --source-city "${SOURCE_CITY}" \
  --source-public-ipv4 "${SOURCE_PUBLIC_IPV4}" \
  --source-ssh-target "${SOURCE_SSH_TARGET}"

trap - EXIT INT TERM
