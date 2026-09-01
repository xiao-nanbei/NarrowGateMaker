"""Policy-neutral primitives for fail-closed live process handoffs.

This module deliberately knows nothing about a strategy, model, exchange, or
release name.  Policy deployers provide their own phase ordering and evidence
semantics; this module owns the process boundary shared by every live deploy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
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

PUBLIC_SOURCE_RELEASE_SCHEMA: Final = "narrowgate_public_source_release.v1"
_GIT_OBJECT_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_SSH_TARGET_RE: Final = re.compile(r"^[A-Za-z0-9_.@:\[\]-]+$")
_TAG_NAME_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
_RELEASE_ID_RE: Final = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$")
_SERVICE_USER_RE: Final = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_SSH_USER_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
_PROXY_HOST_RE: Final = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$"
)
PREPARED_ACTIVATION_PHASES: Final = (
    "verify",
    "stop_quiescence",
    "fresh_reconcile",
    "start",
    "bounded_health",
    "activation_receipt",
    "publish_current",
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


def _run_git(repository_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", *arguments),
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=120.0,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise LiveDeployContractError(
            f"local Git command failed: git {' '.join(arguments)}: {detail}"
        )
    return completed.stdout.strip()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_tag_name(value: str) -> str:
    tag = str(value).strip()
    if (
        _TAG_NAME_RE.fullmatch(tag) is None
        or "//" in tag
        or ".." in tag
        or "@{" in tag
        or tag.endswith(("/", ".", ".lock"))
        or any(part.startswith(".") for part in tag.split("/"))
    ):
        raise LiveDeployContractError("annotated release tag name is malformed")
    return tag


def inspect_clean_public_source(
    repository_root: str | Path,
    *,
    annotated_tag: str | None = None,
) -> dict[str, str | None]:
    """Bind one physical, clean checkout before any public source transfer."""

    root = Path(repository_root).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise LiveDeployContractError("repository root is not a directory")
    top_level = Path(_run_git(root, "rev-parse", "--show-toplevel")).resolve(strict=True)
    if top_level != root:
        raise LiveDeployContractError("repository root is not the Git top level")
    commit = _run_git(root, "rev-parse", "HEAD")
    tree = _run_git(root, "rev-parse", "HEAD^{tree}")
    if _GIT_OBJECT_RE.fullmatch(commit) is None or _GIT_OBJECT_RE.fullmatch(tree) is None:
        raise LiveDeployContractError("source commit/tree identity is malformed")
    if _run_git(root, "cat-file", "-t", commit) != "commit":
        raise LiveDeployContractError("source HEAD is not a commit")
    status = _run_git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise LiveDeployContractError("public source checkout is not clean")
    identity: dict[str, str | None] = {
        "repository_root": str(root),
        "execution_commit": commit,
        "execution_tree": tree,
        "annotated_tag": None,
        "annotated_tag_object": None,
    }
    if annotated_tag is not None:
        tag = _validate_tag_name(annotated_tag)
        tag_ref = f"refs/tags/{tag}"
        _run_git(root, "check-ref-format", tag_ref)
        tag_object = _run_git(root, "rev-parse", "--verify", f"{tag_ref}^{{tag}}")
        peeled_commit = _run_git(root, "rev-parse", "--verify", f"{tag_ref}^{{commit}}")
        if _GIT_OBJECT_RE.fullmatch(tag_object) is None:
            raise LiveDeployContractError("annotated release tag object is malformed")
        if peeled_commit != commit:
            raise LiveDeployContractError("annotated release tag does not peel to HEAD")
        identity.update(
            {
                "annotated_tag": tag,
                "annotated_tag_object": tag_object,
            }
        )
    return identity


def create_public_source_bundle(
    *,
    repository_root: str | Path,
    output_path: str | Path,
    annotated_tag: str | None = None,
) -> dict[str, Any]:
    """Create and verify a bundle from one clean HEAD without private extras."""

    identity_before = inspect_clean_public_source(
        repository_root,
        annotated_tag=annotated_tag,
    )
    root = Path(str(identity_before["repository_root"]))
    output = Path(output_path).expanduser().absolute()
    if output.exists() or output.is_symlink():
        raise LiveDeployContractError("source bundle output must be create-only")
    if not output.parent.is_dir() or output.parent.is_symlink():
        raise LiveDeployContractError("source bundle parent is unavailable or unsafe")
    bundle_refs = ["HEAD"]
    if identity_before["annotated_tag"] is not None:
        bundle_refs.append(f"refs/tags/{identity_before['annotated_tag']}")
    _run_git(root, "bundle", "create", str(output), *bundle_refs)
    output.chmod(0o600)
    _run_git(root, "bundle", "verify", str(output))
    identity_after = inspect_clean_public_source(root, annotated_tag=annotated_tag)
    if identity_after != identity_before:
        output.unlink(missing_ok=True)
        raise LiveDeployContractError("source checkout changed while bundling")
    bundle_size = output.stat().st_size
    if bundle_size <= 0:
        output.unlink(missing_ok=True)
        raise LiveDeployContractError("source bundle is empty")
    return {
        **identity_before,
        "bundle_path": str(output),
        "bundle_sha256": _sha256_file(output),
        "bundle_size_bytes": bundle_size,
    }


def _validate_ssh_target(value: str) -> str:
    target = str(value).strip()
    if (
        not target
        or target.startswith("-")
        or _SSH_TARGET_RE.fullmatch(target) is None
        or target.count("@") > 1
    ):
        raise LiveDeployContractError("SSH target is malformed")
    return target


def _validate_public_release_dir(value: str) -> str:
    release = _absolute_posix_path(value, "public release directory")
    path = PurePosixPath(release)
    if path == PurePosixPath("/") or path.parent == PurePosixPath("/"):
        raise LiveDeployContractError("public release directory is too broad")
    if path.name.startswith("."):
        raise LiveDeployContractError("public release directory name cannot be hidden")
    return release


def render_public_source_publish_shell(
    *,
    release_dir: str,
    execution_commit: str,
    execution_tree: str,
    bundle_sha256: str,
    annotated_tag: str | None = None,
    annotated_tag_object: str | None = None,
) -> str:
    """Render one locked remote source publication fed by a bundle on stdin."""

    release = _validate_public_release_dir(release_dir)
    commit = str(execution_commit)
    tree = str(execution_tree)
    bundle_hash = _require_sha256(bundle_sha256, "public source bundle")
    if _GIT_OBJECT_RE.fullmatch(commit) is None or _GIT_OBJECT_RE.fullmatch(tree) is None:
        raise LiveDeployContractError("source commit/tree identity is malformed")
    if (annotated_tag is None) != (annotated_tag_object is None):
        raise LiveDeployContractError("annotated release tag identity is incomplete")
    tag = _validate_tag_name(annotated_tag) if annotated_tag is not None else ""
    tag_object = str(annotated_tag_object) if annotated_tag_object is not None else ""
    if tag_object and _GIT_OBJECT_RE.fullmatch(tag_object) is None:
        raise LiveDeployContractError("annotated release tag object is malformed")
    release_path = PurePosixPath(release)
    parent = str(release_path.parent)
    name = release_path.name
    staging = str(release_path.parent / f".{name}.staging-{commit}")
    bundle = str(release_path.parent / f".{name}.{bundle_hash}.bundle")
    lock = str(release_path.parent / ".narrowgate-source-deploy.lock")
    script = f"""set -eu
umask 077
export PATH=/usr/bin:/bin
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
release={shlex.quote(release)}
parent={shlex.quote(parent)}
staging={shlex.quote(staging)}
bundle={shlex.quote(bundle)}
lock={shlex.quote(lock)}
commit={shlex.quote(commit)}
tree={shlex.quote(tree)}
bundle_sha256={shlex.quote(bundle_hash)}
annotated_tag={shlex.quote(tag)}
annotated_tag_object={shlex.quote(tag_object)}
uid=$(/usr/bin/id -u)
test ! -L "$parent" && test -d "$parent"
test "$(/usr/bin/readlink -f -- "$parent")" = "$parent"
test "$(/usr/bin/stat -c '%u' -- "$parent")" = "$uid"
mode=$(/usr/bin/stat -c '%a' -- "$parent")
case "$mode" in ''|*[!0-7]*) exit 90 ;; esac
test $((8#$mode & 022)) -eq 0
test ! -L "$lock"
exec 9>"$lock"
/bin/chmod 600 -- "$lock"
/usr/bin/flock -w 30 9
validate_checkout() {{
  checkout=$1
  test ! -L "$checkout" && test -d "$checkout"
  test "$(/usr/bin/readlink -f -- "$checkout")" = "$checkout"
  test "$(/usr/bin/git -C "$checkout" rev-parse --show-toplevel)" = "$checkout"
  test "$(/usr/bin/git -C "$checkout" rev-parse HEAD)" = "$commit"
  test "$(/usr/bin/git -C "$checkout" rev-parse 'HEAD^{{tree}}')" = "$tree"
  test -z "$(/usr/bin/git -C "$checkout" status --porcelain=v1 --untracked-files=all)"
  if test -n "$annotated_tag"; then
    tag_ref="refs/tags/$annotated_tag"
    test "$(/usr/bin/git -C "$checkout" cat-file -t "$tag_ref")" = tag
    test "$(/usr/bin/git -C "$checkout" rev-parse "$tag_ref")" = "$annotated_tag_object"
    test "$(/usr/bin/git -C "$checkout" rev-parse "$tag_ref^{{commit}}")" = "$commit"
  fi
}}
if test -e "$release" || test -L "$release"; then
  /bin/cat >/dev/null
  validate_checkout "$release"
  /usr/bin/printf 'already-present %s %s\\n' "$commit" "$tree"
  exit 0
fi
if test -e "$bundle" || test -L "$bundle"; then
  test ! -L "$bundle" && test -f "$bundle"
  test "$(/usr/bin/stat -c '%u' -- "$bundle")" = "$uid"
  test "$(/usr/bin/stat -c '%h' -- "$bundle")" = 1
  test "$(/usr/bin/stat -c '%a' -- "$bundle")" = 600
  test "$(/usr/bin/sha256sum -- "$bundle" | /usr/bin/awk '{{print $1}}')" = "$bundle_sha256"
  /bin/cat >/dev/null
else
  upload=$(/usr/bin/mktemp "$parent/.{name}.bundle-upload.XXXXXX")
  cleanup_upload() {{ /bin/rm -f -- "$upload"; }}
  trap cleanup_upload EXIT HUP INT TERM
  /bin/cat >"$upload"
  /bin/chmod 600 -- "$upload"
  test "$(/usr/bin/sha256sum -- "$upload" | /usr/bin/awk '{{print $1}}')" = "$bundle_sha256"
  /bin/mv -T -- "$upload" "$bundle"
  trap - EXIT HUP INT TERM
fi
if test -e "$staging" || test -L "$staging"; then
  validate_checkout "$staging"
else
  /usr/bin/git clone --no-checkout -- "$bundle" "$staging"
  /usr/bin/git -C "$staging" checkout --detach --force "$commit"
  /usr/bin/git -C "$staging" remote remove origin
  validate_checkout "$staging"
fi
/bin/mv -T -- "$staging" "$release"
validate_checkout "$release"
/bin/rm -f -- "$bundle"
/usr/bin/printf 'published %s %s\\n' "$commit" "$tree"
"""
    return f"/bin/bash --noprofile --norc -c {shlex.quote(script)}"


def deploy_public_source_release(
    *,
    repository_root: str | Path,
    target: str,
    release_dir: str,
    annotated_tag: str | None = None,
    dry_run: bool = False,
    connect_timeout_s: int = 20,
    command_timeout_s: int = 600,
) -> dict[str, Any]:
    """Publish only an exact public Git checkout; never copy private inputs."""

    ssh_target = _validate_ssh_target(target)
    release = _validate_public_release_dir(release_dir)
    for value, label in (
        (connect_timeout_s, "SSH connect timeout"),
        (command_timeout_s, "source publication timeout"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise LiveDeployContractError(f"{label} must be a positive integer")
    if connect_timeout_s >= command_timeout_s:
        raise LiveDeployContractError("SSH connect timeout must be shorter than command timeout")

    with tempfile.TemporaryDirectory(prefix="narrowgate-source-release-") as temporary:
        bundle_path = Path(temporary) / "source.bundle"
        identity = create_public_source_bundle(
            repository_root=repository_root,
            output_path=bundle_path,
            annotated_tag=annotated_tag,
        )
        base = {
            "schema_version": PUBLIC_SOURCE_RELEASE_SCHEMA,
            "target": ssh_target,
            "release_dir": release,
            "execution_commit": identity["execution_commit"],
            "execution_tree": identity["execution_tree"],
            "annotated_tag": identity["annotated_tag"],
            "annotated_tag_object": identity["annotated_tag_object"],
            "bundle_sha256": identity["bundle_sha256"],
            "bundle_size_bytes": identity["bundle_size_bytes"],
            "private_materials_transferred": False,
            "process_restarted": False,
        }
        if dry_run:
            return {**base, "mode": "dry-run", "status": "planned"}
        remote_shell = render_public_source_publish_shell(
            release_dir=release,
            execution_commit=str(identity["execution_commit"]),
            execution_tree=str(identity["execution_tree"]),
            bundle_sha256=str(identity["bundle_sha256"]),
            annotated_tag=(
                str(identity["annotated_tag"])
                if identity["annotated_tag"] is not None
                else None
            ),
            annotated_tag_object=(
                str(identity["annotated_tag_object"])
                if identity["annotated_tag_object"] is not None
                else None
            ),
        )
        with bundle_path.open("rb") as bundle_stream:
            completed = subprocess.run(
                (
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    f"ConnectTimeout={connect_timeout_s}",
                    ssh_target,
                    remote_shell,
                ),
                stdin=bundle_stream,
                check=False,
                capture_output=True,
                timeout=float(command_timeout_s),
            )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).decode(
                "utf-8", errors="replace"
            ).strip()[-2000:]
            raise LiveDeployContractError(
                f"remote public source publication failed (rc={completed.returncode}): {detail}"
            )
        output = completed.stdout.decode("ascii", errors="strict").strip().splitlines()
        if len(output) != 1:
            raise LiveDeployContractError("remote source publication result is malformed")
        fields = output[0].split()
        if fields not in (
            ["published", identity["execution_commit"], identity["execution_tree"]],
            ["already-present", identity["execution_commit"], identity["execution_tree"]],
        ):
            raise LiveDeployContractError("remote source publication identity drifted")
        return {
            **base,
            "mode": "deploy",
            "status": fields[0],
        }


def _activation_value(value: str, *, label: str, pattern: re.Pattern[str]) -> str:
    normalized = str(value).strip()
    if pattern.fullmatch(normalized) is None:
        raise LiveDeployContractError(f"{label} is malformed")
    return normalized


def _socks5_proxy(value: str | None) -> str | None:
    if value is None:
        return None
    host, separator, port_text = str(value).strip().rpartition(":")
    if separator != ":" or _PROXY_HOST_RE.fullmatch(host) is None:
        raise LiveDeployContractError("SOCKS5 proxy must be HOST:PORT")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise LiveDeployContractError("SOCKS5 proxy port is malformed") from exc
    if not 1 <= port <= 65535 or str(port) != port_text:
        raise LiveDeployContractError("SOCKS5 proxy port is malformed")
    return f"{host}:{port}"


def _proxy_ssh_target(value: str) -> str:
    user, separator, host = value.rpartition("@")
    if not separator:
        host = value
    elif _SSH_USER_RE.fullmatch(user) is None:
        raise LiveDeployContractError("proxied SSH target user is malformed")
    if _PROXY_HOST_RE.fullmatch(host) is None or any(char in host for char in "[]%*?"):
        raise LiveDeployContractError("proxied SSH target host is malformed")
    return value


def _activation_path(value: str, label: str) -> str:
    path = _absolute_posix_path(value, label)
    if "%" in path:
        raise LiveDeployContractError(f"{label} cannot contain percent expansion")
    return path


def render_prepared_release_activation_shell(
    *,
    release_id: str,
    release_dir: str,
    previous_release_dir: str,
    private_environment_file: str,
    active_config_path: str,
    deployment_envelope_path: str,
    deployment_envelope_sha256: str,
    trusted_python_path: str,
    stopped_reconciliation_path: str,
    activation_receipt_path: str,
    current_pointer_path: str,
    service_user: str = "ec2-user",
    health_timeout_s: int = 180,
) -> str:
    """Render one fixed transaction over an already prepared release."""

    q = shlex.quote
    rid = _activation_value(release_id, label="release ID", pattern=_RELEASE_ID_RE)
    user = _activation_value(service_user, label="service user", pattern=_SERVICE_USER_RE)
    release = _validate_public_release_dir(release_dir)
    previous = _validate_public_release_dir(previous_release_dir)
    path = _activation_path
    env_file = path(private_environment_file, "private environment file")
    config = path(active_config_path, "active config")
    envelope = path(deployment_envelope_path, "deployment envelope")
    trusted = path(trusted_python_path, "trusted Python")
    reconciliation = path(stopped_reconciliation_path, "stopped reconciliation")
    activation = path(activation_receipt_path, "activation receipt")
    current = path(current_pointer_path, "current pointer")
    envelope_sha = _require_sha256(deployment_envelope_sha256, "deployment envelope root")
    if "%" in release or "%" in previous:
        raise LiveDeployContractError("activation release paths cannot contain percent expansion")
    if release == previous or len({reconciliation, activation, current}) != 3:
        raise LiveDeployContractError("activation release/output paths overlap")
    if not isinstance(health_timeout_s, int) or isinstance(health_timeout_s, bool):
        raise LiveDeployContractError("health timeout must be an integer")
    if not 10 <= health_timeout_s <= 900:
        raise LiveDeployContractError("health timeout must be between 10 and 900 seconds")
    json_get = (
        "import json,sys;v=json.load(open(sys.argv[1]));"
        "[v:=v[k] for k in sys.argv[2].split('.')];print(v)"
    )
    health_check = (
        "import json,math,sys;h=json.load(open(sys.argv[1]));"
        "r=json.load(open(sys.argv[2]));p=int(sys.argv[3]);start=int(sys.argv[4]);"
        "generation=int(h.get('recordedAtNs',0));assert generation>start;"
        "assert h.get('pid')==p;"
        "assert h.get('quoteLoopRunning') is True;"
        "assert h.get('ownershipConflictLatched') is False;"
        "assert h.get('fatalRuntimeLatched') is False;"
        "assert h.get('reconciliationRequired') is False;"
        "assert h.get('reconciliationPending') is False;"
        "assert h.get('fatalReason')=='';"
        "age=float(h.get('lastTickAge'));assert math.isfinite(age) and 0<=age<=1.0;"
        "assert r.get('pid')==p and r.get('dry_run') is False and r.get('testnet') is False;"
        "print(generation)"
    )
    process_check = (
        "import os,sys;pid=int(sys.argv[1]);expected=os.fsencode(sys.argv[2]);"
        "args=open(f'/proc/{pid}/cmdline','rb').read().split(b'\\0');"
        "assert expected in args"
    )
    fsync_parent = (
        "import os,sys;p=os.path.dirname(sys.argv[1]);"
        "fd=os.open(p,os.O_RDONLY|getattr(os,'O_DIRECTORY',0));"
        "os.fsync(fd);os.close(fd)"
    )
    pointer_check = (
        "import runpy,sys;from pathlib import Path;"
        "m=runpy.run_path(sys.argv[1]);"
        "v=m['load_current_pointer'](Path(sys.argv[2]),"
        "deployment_envelope_path=Path(sys.argv[3]),"
        "activation_receipt_path=Path(sys.argv[4]));p=v['pointer'];"
        "assert p['release_id']==sys.argv[5];"
        "assert p['activation_receipt_sha256']==sys.argv[6]"
    )
    script = f"""set -euo pipefail
umask 077
export PATH=/usr/bin:/bin
rid={q(rid)} release={q(release)} previous={q(previous)} user={q(user)}
env_file={q(env_file)} config={q(config)} envelope={q(envelope)}
envelope_sha={q(envelope_sha)} trusted={q(trusted)}
reconciliation={q(reconciliation)} activation={q(activation)} current={q(current)}
pointer_stage="$(dirname "$current")/.current-$rid-$$.pending"
pointer_stage_owned=0
start_marker="" start_marker_owned=0
lock="$(dirname "$current")/.narrowgate-live-activation.lock"
cleanup_required=0
quiescent() {{
  test -z "$(pgrep -f -- '[l]ive/main.py' || true)" \
    && test -z "$(pgrep -f -- '[l]ive/run.sh __supervise' || true)"
}}
cleanup() {{
  rc=$?
  trap - EXIT HUP INT TERM
  if test "$pointer_stage_owned" = 1; then rm -f -- "$pointer_stage"; fi
  if test "$start_marker_owned" = 1; then rm -f -- "$start_marker"; fi
  if test "$cleanup_required" = 1; then
    cleanup_probe_deadline=$((SECONDS + 5))
    while true; do
      unit_cwd="$(systemctl show narrowgate.service -p WorkingDirectory --value 2>/dev/null || true)"
      if test "$unit_cwd" = "$release"; then
        sudo systemctl stop narrowgate.service >/dev/null 2>&1 || true
        cleanup_deadline=$((SECONDS + 30))
        while ! quiescent; do
          if test "$SECONDS" -ge "$cleanup_deadline"; then rc=83; break; fi
          sleep 1
        done
        break
      fi
      test -z "$unit_cwd" || break
      test "$SECONDS" -lt "$cleanup_probe_deadline" || break
      sleep 0.1
    done
  fi
  exit "$rc"
}}
canonical_input() {{
  test -e "$1" && test ! -L "$1"
  test "$(readlink -f -- "$1")" = "$1"
  test "$(readlink -f -- "$(dirname -- "$1")")" = "$(dirname -- "$1")"
}}
canonical_output() {{
  test ! -L "$1" && test ! -d "$1"
  test "$(readlink -f -- "$(dirname -- "$1")")" = "$(dirname -- "$1")"
}}
private_parent() {{
  parent="$(dirname -- "$1")"
  canonical_input "$parent"
  test "$(/usr/bin/stat -c %u "$parent")" = "$(id -u)"
  test "$(/usr/bin/stat -c %a "$parent")" = 700
}}
process_matches() {{
  case "$1" in ''|*[!0-9]*) return 1 ;; esac
  test "$1" -gt 0
  test "$(readlink -f -- "/proc/$1/cwd")" = "$2"
  "$trusted" -c {q(process_check)} "$1" "$2/live/main.py"
}}
candidate_unit_matches() {{
  test "$(systemctl show narrowgate.service -p ActiveState --value)" = active
  test "$(systemctl show narrowgate.service -p SubState --value)" = running
  test "$(systemctl show narrowgate.service -p Transient --value)" = yes
  test "$(systemctl show narrowgate.service -p WorkingDirectory --value)" = "$release"
  test "$(systemctl show narrowgate.service -p MainPID --value)" = "$1"
  test "$(systemctl show narrowgate.service -p NRestarts --value)" = 0
  process_matches "$1" "$release"
}}
trap cleanup EXIT
trap 'exit 84' HUP INT TERM
canonical_input "$release" && canonical_input "$previous"
canonical_input "$env_file" && canonical_input "$config"
canonical_input "$envelope" && canonical_input "$trusted"
test -d "$release" && test -d "$previous" && test -f "$env_file"
test -f "$config" && test -f "$envelope" && test -x "$trusted"
canonical_output "$reconciliation" && canonical_output "$activation"
canonical_output "$current" && canonical_output "$pointer_stage" && canonical_output "$lock"
test ! -e "$reconciliation" && test ! -e "$activation"
test ! -e "$pointer_stage"
pointer_stage_owned=1
private_parent "$lock"
trap '' HUP INT TERM
if test ! -e "$lock"; then
  (set -o noclobber; : >"$lock") 2>/dev/null || true
fi
canonical_input "$lock"
test -f "$lock"
test "$(/usr/bin/stat -c %u "$lock")" = "$(id -u)"
test "$(/usr/bin/stat -c %a "$lock")" = 600
lock_identity="$(/usr/bin/stat -c %d:%i "$lock")"
exec 9>>"$lock"
trap 'exit 84' HUP INT TERM
flock -w 30 9
canonical_input "$lock"
test "$(/usr/bin/stat -c %d:%i "$lock")" = "$lock_identity"
test "$(readlink -f -- "/proc/$$/fd/9")" = "$lock"
test "$(/usr/bin/stat -Lc %d:%i "/proc/$$/fd/9")" = "$lock_identity"
test "$($trusted -c {q(json_get)} "$envelope" canonical_sha256)" = "$envelope_sha"
common=(
  --property="User=$user" --property="WorkingDirectory=$release"
  --property="EnvironmentFile=$env_file" --property=UMask=0077
  --setenv="NARROWGATE_LIVE_CONFIG=$config"
  --setenv="NARROWGATE_DEPLOYMENT_ENVELOPE_PATH=$envelope"
  --setenv="NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256=$envelope_sha"
  --setenv="NARROWGATE_STARTUP_TRUSTED_PYTHON_PATH=$trusted"
)
sudo systemd-run --quiet --wait --collect --service-type=oneshot \
  --unit="narrowgate-verify-$rid" --property=NoNewPrivileges=true \
  --property=TimeoutStartSec=180 "${{common[@]}}" \
  "$release/live/run.sh" candidate-verify
test "$(systemctl show narrowgate.service -p ActiveState --value)" = active
test "$(systemctl show narrowgate.service -p SubState --value)" = running
test "$(systemctl show narrowgate.service -p Transient --value)" = yes
test "$(systemctl show narrowgate.service -p WorkingDirectory --value)" = "$previous"
old_pid="$(systemctl show narrowgate.service -p MainPID --value)"
process_matches "$old_pid" "$previous"
sudo systemctl stop narrowgate.service
quiescent
install -d -m 0700 "$release/logs"
if test -f "$previous/logs/fill_cooldown_state.json"; then
  install -m 0600 "$previous/logs/fill_cooldown_state.json" \
    "$release/logs/fill_cooldown_state.json"
fi
sudo systemd-run --quiet --wait --collect --service-type=oneshot \
  --unit="narrowgate-reconcile-$rid" --property=NoNewPrivileges=true \
  --property=TimeoutStartSec=180 "${{common[@]}}" \
  "$release/live/run.sh" reconcile-stopped "$reconciliation"
reconciliation_sha="$($trusted -c {q(json_get)} \
  "$reconciliation" canonical_exchange_reconciliation_sha256)"
[[ "$reconciliation_sha" =~ ^[0-9a-f]{{64}}$ ]]
deadline=$((SECONDS + 30))
while test "$(systemctl show narrowgate.service -p LoadState --value 2>/dev/null || true)" \
    != not-found; do
  test "$SECONDS" -lt "$deadline"
  sleep 1
done
canonical_input "$release" && canonical_input "$env_file"
canonical_input "$config" && canonical_input "$envelope" && canonical_input "$trusted"
test "$($trusted -c {q(json_get)} "$envelope" canonical_sha256)" = "$envelope_sha"
start_marker="$release/logs/.activation-start-$rid"
test ! -e "$start_marker"
canonical_output "$start_marker"
private_parent "$start_marker"
trap '' HUP INT TERM
set -o noclobber
: >"$start_marker"
set +o noclobber
start_marker_owned=1
trap 'exit 84' HUP INT TERM
start_ns="$(date +%s%N)"
cleanup_required=1
sudo systemd-run --quiet --collect --service-type=simple --unit=narrowgate \
  --property=Restart=no --property=KillSignal=SIGTERM \
  --property=TimeoutStartSec=120 --property=TimeoutStopSec=120 \
  "${{common[@]}}" \
  --setenv="NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH=$reconciliation" \
  --setenv="NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256=$reconciliation_sha" \
  "$release/live/run.sh" service
health="$release/logs/runtime_health.json"
runtime="$release/logs/runtime_identity.json"
observations=0 last_generation=0 candidate_pid=""
deadline=$((SECONDS + {health_timeout_s}))
while test "$SECONDS" -lt "$deadline"; do
  state="$(systemctl is-active narrowgate.service 2>/dev/null || true)"
  pid="$(systemctl show narrowgate.service -p MainPID --value 2>/dev/null || true)"
  if test -z "$candidate_pid" && test "$state" = active; then candidate_pid="$pid"; fi
  if test -n "$candidate_pid" && test "$pid" = "$candidate_pid" \
      && candidate_unit_matches "$candidate_pid" \
      && test -s "$health" && test -s "$runtime" && test "$health" -nt "$start_marker"; then
    generation="$($trusted -c {q(health_check)} \
      "$health" "$runtime" "$candidate_pid" "$start_ns" 2>/dev/null || true)"
    case "$generation" in ''|*[!0-9]*) generation=0 ;; esac
    if test "$generation" -gt "$last_generation"; then
      observations=$((observations + 1))
      last_generation="$generation"
    fi
    if test "$observations" -ge 2; then break; fi
  fi
  test "$state" != failed && test "$state" != inactive
  sleep 1
done
test "$observations" -ge 2
candidate_unit_matches "$candidate_pid"
"$trusted" -I -B "$release/live/deployment_runtime.py" build-activation-receipt \
  --release-id "$rid" --deployment-envelope "$envelope" \
  --deployment-envelope-sha256 "$envelope_sha" \
  --stopped-reconciliation "$reconciliation" \
  --stopped-reconciliation-sha256 "$reconciliation_sha" \
  --runtime-identity "$runtime" --output "$activation"
activation_sha="$($trusted -c {q(json_get)} "$activation" canonical_sha256)"
fresh=0 deadline=$((SECONDS + {health_timeout_s}))
while test "$SECONDS" -lt "$deadline"; do
  candidate_unit_matches "$candidate_pid"
  generation="$($trusted -c {q(health_check)} \
    "$health" "$runtime" "$candidate_pid" "$start_ns" 2>/dev/null || true)"
  case "$generation" in ''|*[!0-9]*) generation=0 ;; esac
  if test "$generation" -gt "$last_generation"; then fresh=1; break; fi
  sleep 1
done
test "$fresh" = 1
trap '' HUP INT TERM
candidate_unit_matches "$candidate_pid"
"$trusted" -I -B "$release/live/deployment_runtime.py" publish-current-pointer \
  --release-id "$rid" --deployment-envelope "$envelope" \
  --deployment-envelope-sha256 "$envelope_sha" \
  --activation-receipt "$activation" --activation-receipt-sha256 "$activation_sha" \
  --stopped-reconciliation "$reconciliation" --runtime-identity "$runtime" \
  --output "$pointer_stage"
"$trusted" -c {q(pointer_check)} "$release/live/deployment_runtime.py" \
  "$pointer_stage" "$envelope" "$activation" "$rid" "$activation_sha"
candidate_unit_matches "$candidate_pid"
post_generation="$($trusted -c {q(health_check)} \
  "$health" "$runtime" "$candidate_pid" "$start_ns")"
test "$post_generation" -ge "$generation"
canonical_output "$current"
mv -f -- "$pointer_stage" "$current"
"$trusted" -c {q(fsync_parent)} "$current"
cleanup_required=0
trap 'exit 84' HUP INT TERM
printf 'activated %s %s %s %s\n' \
  "$rid" "$envelope_sha" "$reconciliation_sha" "$activation_sha"
"""
    return f"/bin/bash --noprofile --norc -c {q(script)}"


def activate_prepared_release(
    *,
    target: str,
    execute: bool = False,
    connect_timeout_s: int = 20,
    command_timeout_s: int = 900,
    socks5_proxy: str | None = None,
    **activation: Any,
) -> dict[str, Any]:
    """Plan or execute one fixed prepared-release activation transaction."""

    target = _validate_ssh_target(target)
    remote_shell = render_prepared_release_activation_shell(**activation)
    release_id = _activation_value(
        str(activation["release_id"]), label="release ID", pattern=_RELEASE_ID_RE
    )
    result = {
        "target": target,
        "release_id": release_id,
        "phases": list(PREPARED_ACTIVATION_PHASES),
    }
    proxy = _socks5_proxy(socks5_proxy)
    if proxy is not None:
        target = _proxy_ssh_target(target)
    if not execute:
        return {**result, "mode": "dry-run", "status": "planned"}
    if connect_timeout_s <= 0 or connect_timeout_s >= command_timeout_s:
        raise LiveDeployContractError("activation SSH timeouts are invalid")
    ssh_options = ["-o", "BatchMode=yes", "-o", f"ConnectTimeout={connect_timeout_s}"]
    if proxy is not None:
        ssh_options.extend(("-o", f"ProxyCommand=nc -x {proxy} -X 5 %h %p"))
    completed = subprocess.run(
        (
            "ssh",
            *ssh_options,
            target,
            remote_shell,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=float(command_timeout_s),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise LiveDeployContractError(
            f"prepared release activation failed (rc={completed.returncode}): {detail}"
        )
    lines = [line.split() for line in completed.stdout.splitlines() if line.strip()]
    fields = lines[-1] if lines else []
    if len(fields) != 5 or fields[:2] != ["activated", release_id]:
        raise LiveDeployContractError("prepared release activation result is malformed")
    envelope_root = _require_sha256(fields[2], "envelope root")
    expected_envelope_root = _require_sha256(
        str(activation["deployment_envelope_sha256"]), "deployment envelope root"
    )
    if envelope_root != expected_envelope_root:
        raise LiveDeployContractError("prepared release activation envelope root drifted")
    return {
        **result,
        "mode": "execute",
        "status": "activated",
        "deployment_envelope_sha256": envelope_root,
        "stopped_reconciliation_sha256": _require_sha256(fields[3], "reconciliation root"),
        "activation_receipt_sha256": _require_sha256(fields[4], "activation root"),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    source = subparsers.add_parser(
        "source-release",
        help="publish one exact clean public checkout without private material",
    )
    source.add_argument("--repo-root", type=Path, default=Path.cwd())
    source.add_argument("--target", required=True)
    source.add_argument("--release-dir", required=True)
    source.add_argument(
        "--annotated-tag",
        help="one explicit annotated public release tag that must peel to HEAD",
    )
    source.add_argument("--dry-run", action="store_true")
    source.add_argument("--connect-timeout-s", type=int, default=20)
    source.add_argument("--command-timeout-s", type=int, default=600)
    activation = subparsers.add_parser("activate-prepared-release")
    for name in (
        "target",
        "release-id",
        "release-dir",
        "previous-release-dir",
        "private-environment-file",
        "active-config",
        "deployment-envelope",
        "deployment-envelope-sha256",
        "trusted-python",
        "stopped-reconciliation",
        "activation-receipt",
        "current-pointer",
    ):
        activation.add_argument(f"--{name}", required=True)
    activation.add_argument("--service-user", default="ec2-user")
    activation.add_argument("--health-timeout-s", type=int, default=180)
    activation.add_argument("--connect-timeout-s", type=int, default=20)
    activation.add_argument("--command-timeout-s", type=int, default=900)
    activation.add_argument("--socks5-proxy")
    activation.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "source-release":
            result = deploy_public_source_release(
                repository_root=args.repo_root,
                target=args.target,
                release_dir=args.release_dir,
                annotated_tag=args.annotated_tag,
                dry_run=args.dry_run,
                connect_timeout_s=args.connect_timeout_s,
                command_timeout_s=args.command_timeout_s,
            )
        elif args.command == "activate-prepared-release":
            result = activate_prepared_release(
                target=args.target,
                execute=args.execute,
                connect_timeout_s=args.connect_timeout_s,
                command_timeout_s=args.command_timeout_s,
                socks5_proxy=args.socks5_proxy,
                release_id=args.release_id,
                release_dir=args.release_dir,
                previous_release_dir=args.previous_release_dir,
                private_environment_file=args.private_environment_file,
                active_config_path=args.active_config,
                deployment_envelope_path=args.deployment_envelope,
                deployment_envelope_sha256=args.deployment_envelope_sha256,
                trusted_python_path=args.trusted_python,
                stopped_reconciliation_path=args.stopped_reconciliation,
                activation_receipt_path=args.activation_receipt,
                current_pointer_path=args.current_pointer,
                service_user=args.service_user,
                health_timeout_s=args.health_timeout_s,
            )
        else:  # pragma: no cover - argparse owns the command set.
            raise LiveDeployContractError(f"unsupported command: {args.command}")
    except (OSError, subprocess.SubprocessError, LiveDeployContractError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


__all__ = [
    "BASE_RESULT_FIELDS",
    "EvidenceKind",
    "LiveDeployContractError",
    "PREPARED_ACTIVATION_PHASES",
    "PROCESS_FAMILY_IDENTITY_FIELDS",
    "PROCESS_FAMILY_RESULT_FIELDS",
    "PUBLIC_SOURCE_RELEASE_SCHEMA",
    "RESULT_FIELDS",
    "activate_prepared_release",
    "canonical_sha256",
    "command_result",
    "create_public_source_bundle",
    "deploy_public_source_release",
    "inspect_clean_public_source",
    "main",
    "parse_process_family_probe",
    "render_bounded_readiness_shell",
    "render_containment_shell",
    "render_fenced_action_shell",
    "render_prepared_release_activation_shell",
    "render_process_epoch_probe_shell",
    "render_process_family_probe_shell",
    "render_public_source_publish_shell",
    "render_quiescence_probe_shell",
    "render_repo_cwd_shell",
    "validate_command_result",
]


if __name__ == "__main__":
    raise SystemExit(main())
