"""Policy-neutral primitives for fail-closed live process handoffs.

This module deliberately knows nothing about a strategy, model, exchange, or
release name.  Policy deployers provide their own phase ordering and evidence
semantics; this module owns the process boundary shared by every live deploy.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Final, Literal


class LiveDeployContractError(RuntimeError):
    """Raised when generic deployment evidence cannot fail closed."""


EvidenceKind = Literal[
    "none",
    "process_ref_optional_epoch",
    "process_ref",
    "process_family",
    "process_identity",
    "runtime_identity",
]

BASE_RESULT_FIELDS: Final = frozenset(
    {
        "label",
        "command_sha256",
        "returncode",
        "stdout_sha256",
        "stderr_sha256",
    }
)
PROCESS_FAMILY_IDENTITY_FIELDS: Final = frozenset(
    {
        "supervisor_pid",
        "supervisor_start_ticks",
        "child_pid",
        "child_start_ticks",
        "child_ppid",
    }
)
PROCESS_FAMILY_RESULT_FIELDS: Final = PROCESS_FAMILY_IDENTITY_FIELDS | frozenset(
    {"process_family_identity_sha256"}
)
RESULT_FIELDS: Final = BASE_RESULT_FIELDS | frozenset(
    {
        "observed_pid",
        "observed_start_ticks",
        "process_identity_sha256",
        *PROCESS_FAMILY_RESULT_FIELDS,
        "runtime_identity_file_sha256",
        "startup_attestation_sha256",
    }
)


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    candidate = str(value)
    if len(candidate) != 64 or any(char not in "0123456789abcdef" for char in candidate):
        raise LiveDeployContractError(f"{label} is not a lowercase SHA256")
    return candidate


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LiveDeployContractError(f"{label} is not a positive integer")
    return value


def command_result(
    row: Mapping[str, Any], completed: subprocess.CompletedProcess[str] | None
) -> dict[str, Any]:
    """Bind a command outcome without retaining raw stdout, stderr, or secrets."""

    base = {
        "label": row["label"],
        "command_sha256": row["command_sha256"],
    }
    if completed is None:
        return {
            **base,
            "returncode": None,
            "stdout_sha256": None,
            "stderr_sha256": None,
        }
    return {
        **base,
        "returncode": int(completed.returncode),
        "stdout_sha256": hashlib.sha256((completed.stdout or "").encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256((completed.stderr or "").encode()).hexdigest(),
    }


def validate_command_result(
    result: Mapping[str, Any], *, evidence_kind: EvidenceKind = "none"
) -> None:
    """Validate one result using an explicit plan-selected evidence type."""

    if set(result) - RESULT_FIELDS:
        raise LiveDeployContractError("command result embeds forbidden fields")
    label = result.get("label")
    if not isinstance(label, str) or not label:
        raise LiveDeployContractError("command result label is malformed")
    _require_sha256(result.get("command_sha256"), "command result hash")
    returncode = result.get("returncode")
    if returncode is None:
        if set(result) != BASE_RESULT_FIELDS or any(
            result.get(field) is not None for field in ("stdout_sha256", "stderr_sha256")
        ):
            raise LiveDeployContractError("runner failure carries fabricated evidence")
        return
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise LiveDeployContractError("command return code is malformed")
    _require_sha256(result.get("stdout_sha256"), "command stdout hash")
    _require_sha256(result.get("stderr_sha256"), "command stderr hash")

    evidence_fields = set(result) - BASE_RESULT_FIELDS
    if returncode != 0 and evidence_fields:
        raise LiveDeployContractError("failed command carries fabricated evidence")
    if not evidence_fields:
        return
    if evidence_kind == "process_ref_optional_epoch":
        if evidence_fields not in (
            {"observed_pid"},
            {"observed_pid", "observed_start_ticks"},
        ):
            raise LiveDeployContractError("process reference evidence is malformed")
        _positive_int(result["observed_pid"], "observed PID")
        if "observed_start_ticks" in result:
            _positive_int(result["observed_start_ticks"], "observed start ticks")
        return
    if evidence_kind == "process_ref":
        if evidence_fields != {"observed_pid", "observed_start_ticks"}:
            raise LiveDeployContractError("process epoch evidence is malformed")
        _positive_int(result["observed_pid"], "observed PID")
        _positive_int(result["observed_start_ticks"], "observed start ticks")
        return
    if evidence_kind == "process_family":
        if evidence_fields != PROCESS_FAMILY_RESULT_FIELDS:
            raise LiveDeployContractError("process family evidence is malformed")
        family = {
            field: _positive_int(result[field], field)
            for field in PROCESS_FAMILY_IDENTITY_FIELDS
        }
        if family["supervisor_pid"] == family["child_pid"]:
            raise LiveDeployContractError("process family reuses one PID")
        if family["child_ppid"] != family["supervisor_pid"]:
            raise LiveDeployContractError("process family PPID is malformed")
        observed = _require_sha256(
            result["process_family_identity_sha256"], "process family identity"
        )
        if observed != canonical_sha256(family):
            raise LiveDeployContractError("process family canonical identity drifted")
        return
    if evidence_kind == "process_identity":
        if evidence_fields != {"observed_pid", "process_identity_sha256"}:
            raise LiveDeployContractError("process identity evidence is malformed")
        _positive_int(result["observed_pid"], "observed PID")
        _require_sha256(result["process_identity_sha256"], "process identity")
        return
    if evidence_kind == "runtime_identity":
        if evidence_fields != {
            "runtime_identity_file_sha256",
            "startup_attestation_sha256",
        }:
            raise LiveDeployContractError("runtime identity evidence is malformed")
        _require_sha256(result["runtime_identity_file_sha256"], "runtime identity file")
        _require_sha256(result["startup_attestation_sha256"], "startup attestation")
        return
    raise LiveDeployContractError("non-evidence command carries process evidence")


def parse_process_family_probe(stdout: str) -> dict[str, int | str]:
    """Parse an observed supervisor/child epoch and actual child PPID."""

    lines = stdout.splitlines()
    tokens = lines[0].split() if len(lines) == 1 else []
    if len(tokens) != 5 or any(not token.isascii() or not token.isdecimal() for token in tokens):
        raise LiveDeployContractError("process family probe is malformed")
    values = [int(token) for token in tokens]
    if min(values) <= 0:
        raise LiveDeployContractError("process family probe is malformed")
    supervisor_pid, supervisor_ticks, child_pid, child_ticks, child_ppid = values
    family: dict[str, int | str] = {
        "supervisor_pid": supervisor_pid,
        "supervisor_start_ticks": supervisor_ticks,
        "child_pid": child_pid,
        "child_start_ticks": child_ticks,
        "child_ppid": child_ppid,
    }
    validate_command_result(
        {
            "label": "process-family-probe",
            "command_sha256": "0" * 64,
            "returncode": 0,
            "stdout_sha256": "0" * 64,
            "stderr_sha256": "0" * 64,
            **family,
            "process_family_identity_sha256": canonical_sha256(family),
        },
        evidence_kind="process_family",
    )
    family["process_family_identity_sha256"] = canonical_sha256(family)
    return family


def _absolute_posix_path(value: str, label: str) -> str:
    raw = str(value)
    path = PurePosixPath(raw)
    if (
        not path.is_absolute()
        or "\x00" in raw
        or "\n" in raw
        or "\r" in raw
        or any(part in {"", ".", ".."} for part in raw.split("/")[1:])
    ):
        raise LiveDeployContractError(f"{label} must be a canonical absolute POSIX path")
    return raw


def render_repo_cwd_shell(*, repository_root: str, action_shell: str) -> str:
    """Render a command that cannot start outside its physical checkout."""

    root = _absolute_posix_path(repository_root, "repository root")
    if not action_shell or "\x00" in action_shell:
        raise LiveDeployContractError("live action shell is malformed")
    quoted = shlex.quote(root)
    return (
        f"cd -- {quoted} && "
        f"test \"$(/bin/pwd -P)\" = {quoted} && "
        f"test \"$(/usr/bin/readlink -f -- .)\" = {quoted} && "
        f"{action_shell}"
    )


def render_process_family_probe_shell(
    *,
    repository_root: str,
    supervisor_pid_file: str,
    child_pid_file: str,
    proc_root: str = "/proc",
) -> str:
    """Render a stable PID/start-tick/PPID/cwd probe for one live family."""

    root = _absolute_posix_path(repository_root, "repository root")
    supervisor_file = _absolute_posix_path(supervisor_pid_file, "supervisor PID file")
    child_file = _absolute_posix_path(child_pid_file, "child PID file")
    proc = _absolute_posix_path(proc_root, "proc root")
    if supervisor_file == child_file:
        raise LiveDeployContractError("supervisor and child PID files must differ")
    script = f"""set -eu
repo={shlex.quote(root)}
supervisor_file={shlex.quote(supervisor_file)}
child_file={shlex.quote(child_file)}
proc_root={shlex.quote(proc)}
uid=$(/usr/bin/id -u)
read_ref() {{
  file=$1
  test ! -L \"$file\" && test -f \"$file\"
  test \"$(/usr/bin/stat -c '%h' -- \"$file\")\" = 1
  test \"$(/usr/bin/stat -c '%u' -- \"$file\")\" = \"$uid\"
  mode=$(/usr/bin/stat -c '%a' -- \"$file\")
  case \"$mode\" in ''|*[!0-7]*) return 1 ;; esac
  test $((8#$mode & 022)) -eq 0
  value=$(/bin/cat -- \"$file\")
  case \"$value\" in ''|*[!0-9]*) return 1 ;; esac
  test \"$value\" -gt 1
  /usr/bin/printf '%s' \"$value\"
}}
read_stat() {{
  pid=$1
  raw=$(/bin/cat -- \"$proc_root/$pid/stat\")
  rest=${{raw##*) }}
  test \"$rest\" != \"$raw\"
  set -- $rest
  test \"$#\" -ge 20
  ppid=$2
  ticks=${{20}}
  case \"$ppid:$ticks\" in *[!0-9:]*) return 1 ;; esac
  test \"$ppid\" -gt 0 && test \"$ticks\" -gt 0
  /usr/bin/printf '%s %s' \"$ppid\" \"$ticks\"
}}
test \"$(/usr/bin/readlink -f -- \"$repo\")\" = \"$repo\"
sp1=$(read_ref \"$supervisor_file\")
cp1=$(read_ref \"$child_file\")
test \"$sp1\" != \"$cp1\"
set -- $(read_stat \"$sp1\"); sppid1=$1; ss1=$2
set -- $(read_stat \"$cp1\"); cppid1=$1; cs1=$2
test \"$cppid1\" = \"$sp1\"
test \"$(/usr/bin/readlink -f -- \"$proc_root/$sp1/cwd\")\" = \"$repo\"
test \"$(/usr/bin/readlink -f -- \"$proc_root/$cp1/cwd\")\" = \"$repo\"
sp2=$(read_ref \"$supervisor_file\")
cp2=$(read_ref \"$child_file\")
set -- $(read_stat \"$sp2\"); sppid2=$1; ss2=$2
set -- $(read_stat \"$cp2\"); cppid2=$1; cs2=$2
test \"$sp1:$ss1:$cp1:$cs1:$cppid1\" = \"$sp2:$ss2:$cp2:$cs2:$cppid2\"
test \"$cppid2\" = \"$sp2\"
/usr/bin/printf '%s %s %s %s %s\\n' \"$sp2\" \"$ss2\" \"$cp2\" \"$cs2\" \"$cppid2\""""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


def render_process_epoch_probe_shell(
    *,
    pid_file: str,
    allow_quiescent: bool = False,
    proc_root: str = "/proc",
) -> str:
    """Read one stable PID epoch, or accept a proven fully quiescent host."""

    file = _absolute_posix_path(pid_file, "PID file")
    proc = _absolute_posix_path(proc_root, "proc root")
    if not isinstance(allow_quiescent, bool):
        raise LiveDeployContractError("allow_quiescent must be boolean")
    fallback = render_quiescence_probe_shell() if allow_quiescent else "exit 1"
    script = f"""set -u
pid_file={shlex.quote(file)}
proc_root={shlex.quote(proc)}
read_ref() {{
  test ! -L "$pid_file" && test -f "$pid_file" || return 1
  test "$(/usr/bin/stat -c '%h' -- "$pid_file")" = 1 || return 1
  value=$(/bin/cat -- "$pid_file") || return 1
  case "$value" in ''|*[!0-9]*) return 1 ;; esac
  test "$value" -gt 1 || return 1
  /usr/bin/printf '%s' "$value"
}}
read_epoch() {{
  pid=$1
  raw=$(/bin/cat -- "$proc_root/$pid/stat") || return 1
  rest=${{raw##*) }}
  test "$rest" != "$raw" || return 1
  set -- $rest
  test "$#" -ge 20 || return 1
  ticks=${{20}}
  case "$ticks" in ''|*[!0-9]*) return 1 ;; esac
  test "$ticks" -gt 0 || return 1
  /usr/bin/printf '%s' "$ticks"
}}
probe() {{
  p1=$(read_ref) || return 1
  e1=$(read_epoch "$p1") || return 1
  p2=$(read_ref) || return 1
  e2=$(read_epoch "$p2") || return 1
  test "$p1:$e1" = "$p2:$e2" || return 1
  /usr/bin/printf '%s %s\n' "$p2" "$e2"
}}
if probe; then
  exit 0
fi
{fallback}"""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


def render_bounded_readiness_shell(
    *,
    family_probe_shell: str,
    readiness_probe_shell: str,
    timeout_s: int = 120,
    poll_s: int = 1,
    predicate_timeout_s: int = 10,
    fatal_returncodes: Sequence[int] = (),
) -> str:
    """Wait for one immutable family and a bounded application predicate."""

    for value, label in (
        (timeout_s, "readiness timeout"),
        (poll_s, "readiness poll"),
        (predicate_timeout_s, "predicate timeout"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise LiveDeployContractError(f"{label} must be a positive integer")
    if predicate_timeout_s >= timeout_s:
        raise LiveDeployContractError("predicate timeout must be shorter than readiness timeout")
    fatal = tuple(sorted(set(fatal_returncodes)))
    if any(isinstance(code, bool) or code <= 1 or code >= 124 for code in fatal):
        raise LiveDeployContractError("fatal readiness return codes must be between 2 and 123")
    if not family_probe_shell or not readiness_probe_shell:
        raise LiveDeployContractError("readiness probes must not be empty")
    fatal_case = "|".join(str(code) for code in fatal)
    fatal_branch = f"{fatal_case}) exit 77 ;;" if fatal_case else ""
    family_command = f"/bin/bash --noprofile --norc -c {shlex.quote(family_probe_shell)}"
    predicate_command = (
        f"/usr/bin/timeout --signal=TERM --kill-after=2s {predicate_timeout_s}s "
        f"/bin/bash --noprofile --norc -c {shlex.quote(readiness_probe_shell)}"
    )
    script = f"""set -u
deadline=$((SECONDS + {timeout_s}))
while :; do
  family=$({family_command} 2>/dev/null) && break
  test \"$SECONDS\" -lt \"$deadline\" || exit 74
  /bin/sleep {poll_s}
done
while :; do
  current=$({family_command} 2>/dev/null) || exit 75
  test \"$current\" = \"$family\" || exit 75
  {predicate_command}
  rc=$?
  current=$({family_command} 2>/dev/null) || exit 75
  test \"$current\" = \"$family\" || exit 75
  case \"$rc\" in
    0) break ;;
    124|137) exit 78 ;;
    {fatal_branch}
  esac
  test \"$SECONDS\" -lt \"$deadline\" || exit 76
  /bin/sleep {poll_s}
done
/usr/bin/printf '%s\\n' \"$family\""""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


def render_quiescence_probe_shell() -> str:
    """Prove that neither the maker nor a supervisor capable of respawning it exists."""

    return (
        "test -z \"$(/usr/bin/pgrep -f -- '[l]ive/main.py' || true)\" && "
        "test -z \"$(/usr/bin/pgrep -f -- '[l]ive/run.sh __supervise' || true)\""
    )


def render_containment_shell(
    *,
    stop_shell: str,
    timeout_s: int = 30,
    poll_s: int = 1,
) -> str:
    """Attempt stop even with partial PID state, then require actual quiescence."""

    for value, label in ((timeout_s, "containment timeout"), (poll_s, "poll interval")):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise LiveDeployContractError(f"{label} must be a positive integer")
    if not stop_shell or "\x00" in stop_shell:
        raise LiveDeployContractError("stop shell is malformed")
    quiescent = render_quiescence_probe_shell()
    script = f"""set -u
/usr/bin/timeout --signal=TERM --kill-after=3s {timeout_s}s /bin/bash --noprofile --norc -c {shlex.quote(stop_shell)} >/dev/null 2>&1 || true
deadline=$((SECONDS + {timeout_s}))
until {quiescent}; do
  test "$SECONDS" -lt "$deadline" || exit 82
  /bin/sleep {poll_s}
done
{quiescent}"""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


def render_fenced_action_shell(
    *,
    lock_path: str,
    action_shell: str,
    failure_containment_shell: str | None = None,
    lock_wait_s: int = 20,
    action_timeout_s: int = 90,
    containment_timeout_s: int = 25,
) -> str:
    """Serialize one live mutation without leaking the lock into descendants."""

    lock = _absolute_posix_path(lock_path, "deployment lock")
    for value, label in (
        (lock_wait_s, "lock wait"),
        (action_timeout_s, "action timeout"),
        (containment_timeout_s, "containment timeout"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise LiveDeployContractError(f"{label} must be a positive integer")
    if not action_shell or "\x00" in action_shell:
        raise LiveDeployContractError("fenced action shell is malformed")
    cleanup = failure_containment_shell or ":"
    cleanup_required = "1" if failure_containment_shell else "0"
    parent = str(PurePosixPath(lock).parent)
    script = f"""set -u
parent={shlex.quote(parent)}
lock={shlex.quote(lock)}
test ! -L "$parent"
test -d "$parent" || /usr/bin/install -d -m 700 -- "$parent"
test ! -L "$lock"
exec 9>"$lock"
/bin/chmod 600 -- "$lock"
/usr/bin/flock -w {lock_wait_s} 9 || exit 79
cleanup_required={cleanup_required}
on_exit() {{
  rc=$?
  trap - EXIT HUP INT TERM
  if test "$cleanup_required" = 1; then
    /usr/bin/timeout --signal=TERM --kill-after=3s {containment_timeout_s}s /bin/bash --noprofile --norc -c {shlex.quote(cleanup)} 9>&- || exit 83
  fi
  exit "$rc"
}}
trap on_exit EXIT
trap 'exit 84' HUP INT TERM
/usr/bin/timeout --signal=TERM --kill-after=5s {action_timeout_s}s /bin/bash --noprofile --norc -c {shlex.quote(action_shell)} 9>&-
rc=$?
test "$rc" -ne 0 || cleanup_required=0
exit "$rc"
"""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


__all__ = [
    "BASE_RESULT_FIELDS",
    "EvidenceKind",
    "LiveDeployContractError",
    "PROCESS_FAMILY_IDENTITY_FIELDS",
    "PROCESS_FAMILY_RESULT_FIELDS",
    "RESULT_FIELDS",
    "canonical_sha256",
    "command_result",
    "parse_process_family_probe",
    "render_bounded_readiness_shell",
    "render_containment_shell",
    "render_fenced_action_shell",
    "render_process_epoch_probe_shell",
    "render_process_family_probe_shell",
    "render_quiescence_probe_shell",
    "render_repo_cwd_shell",
    "validate_command_result",
]
