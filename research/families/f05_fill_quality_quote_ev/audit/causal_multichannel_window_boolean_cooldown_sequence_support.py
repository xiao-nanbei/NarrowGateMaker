#!/usr/bin/env python3
"""Bind the frozen native sequence audit to cooldown-v2 target windows.

This is an outcome-blind denominator audit.  It maps each frozen target day to
its target and continuation sequence eligibility.  D-1 is admitted through the
target-start snapshot check carried by the upstream audit; D and D+1 must both
be strict eligible.  Current cache bytes are validated again by the label-panel
prebuild, so this artifact is a preflight mapping rather than execution proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from data_paths import data_root
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_d1_support as denominator,
)

IDENTITY = denominator.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.native_sequence_support_mapping.v1"
UPSTREAM_SCHEMA = "native_exchange_book_sequence_audit.v2"

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_spec_20260810.json"
)
DEFAULT_AUDIT_ROOT = DATA_ROOT / (
    "reports/"
    "native_strict_universe_20260724"
)
DEFAULT_AUDIT_JSON = DEFAULT_AUDIT_ROOT / "sequence_audit_v2.json"
DEFAULT_AUDIT_CSV = DEFAULT_AUDIT_ROOT / "sequence_audit_v2.csv"
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "strict_native_sequence_support_mapping_v1.json"
)

EXPECTED_SPEC_SHA256 = denominator.EXPECTED_V2_SPEC_SHA256
EXPECTED_AUDIT_JSON_SHA256 = (
    "f1784983ae75feeced4a6c3dac9f00f7e4e623352e320b35ebc2609fa3b16765"
)
EXPECTED_AUDIT_CSV_SHA256 = (
    "042db7f53fb0de37e42deb0fc25125a45762f9556451147dff4a40f908b07d51"
)


class SequenceSupportError(RuntimeError):
    """Raised when the upstream audit or frozen denominator drifts."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SequenceSupportError(f"cannot load {label}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise SequenceSupportError(f"{label} root must be an object")
    return payload


def _require_bound(path: Path, expected_sha256: str, *, label: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise SequenceSupportError(f"{label} is missing: {resolved}")
    observed = file_sha256(resolved)
    if observed != expected_sha256:
        raise SequenceSupportError(
            f"{label} hash drifted: {observed} != {expected_sha256}"
        )
    return resolved


def _upstream_identity(audit: Mapping[str, Any]) -> str:
    payload = dict(audit)
    payload.pop("identity_sha256", None)
    return canonical_sha256(payload)


def _is_strict_eligible(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("eligible") is True
        and row.get("target_initialized_at_start") is True
        and row.get("target_initialization_source_at_start") == "snapshot"
        and int(row.get("target_accepted_updates", 0)) > 0
        and int(row.get("target_sequence_gaps", -1)) == 0
        and int(row.get("target_invalid_sequence_messages", -1)) == 0
        and int(row.get("target_message_time_reversals", -1)) == 0
    )


def build_mapping(
    *,
    spec: Mapping[str, Any],
    upstream_audit: Mapping[str, Any],
    spec_path: Path,
    spec_sha256: str,
    audit_json_path: Path,
    audit_json_sha256: str,
    audit_csv_path: Path,
    audit_csv_sha256: str,
) -> dict[str, Any]:
    """Map frozen target windows to the pre-existing sequence audit."""

    if spec.get("identity") != IDENTITY:
        raise SequenceSupportError("cooldown-v2 spec identity drifted")
    ordered = spec.get("ordered_utc_days")
    if not isinstance(ordered, Mapping):
        raise SequenceSupportError("cooldown-v2 spec lacks ordered UTC days")
    prefix = tuple(str(day) for day in ordered.get("prefix40", ()))
    added = tuple(str(day) for day in ordered.get("added10", ()))
    if prefix != denominator.PREFIX40 or added != denominator.ADDED10:
        raise SequenceSupportError("frozen 40+10 denominator drifted")

    strict_source = spec.get("source_separation", {}).get("strict_native_2026")
    if not isinstance(strict_source, Mapping):
        raise SequenceSupportError("strict-native source contract is missing")
    frozen_reduced = frozenset(
        str(day) for day in strict_source.get("reduced_support_days", ())
    )
    frozen50 = (*prefix, *added)
    if len(frozen_reduced) != 9 or not frozen_reduced.issubset(frozen50):
        raise SequenceSupportError("frozen reduced-support denominator drifted")

    if upstream_audit.get("schema_version") != UPSTREAM_SCHEMA:
        raise SequenceSupportError("upstream sequence-audit schema drifted")
    if upstream_audit.get("identity_sha256") != _upstream_identity(upstream_audit):
        raise SequenceSupportError("upstream sequence-audit identity drifted")
    if upstream_audit.get("audit_csv_sha256") != audit_csv_sha256:
        raise SequenceSupportError("upstream CSV binding drifted")
    day_audits = upstream_audit.get("day_audits")
    if not isinstance(day_audits, Mapping):
        raise SequenceSupportError("upstream day audits are missing")

    rows: list[dict[str, Any]] = []
    formal_failures: list[dict[str, Any]] = []
    for panel, days in (("prefix40", prefix), ("added10", added)):
        for target_day in days:
            continuation_day = (
                date.fromisoformat(target_day) + timedelta(days=1)
            ).isoformat()
            target = day_audits.get(target_day)
            continuation = day_audits.get(continuation_day)
            frozen_formal = target_day not in frozen_reduced
            if frozen_formal and (
                not isinstance(target, Mapping)
                or not isinstance(continuation, Mapping)
            ):
                raise SequenceSupportError(
                    f"upstream audit lacks target/D+1 rows for {target_day}"
                )
            target_eligible = (
                _is_strict_eligible(target) if isinstance(target, Mapping) else None
            )
            continuation_eligible = (
                _is_strict_eligible(continuation)
                if isinstance(continuation, Mapping)
                else None
            )
            strict_window_eligible = bool(
                target_eligible is True and continuation_eligible is True
            )
            row = {
                "panel": panel,
                "target_day": target_day,
                "continuation_day": continuation_day,
                "frozen_formal_day": frozen_formal,
                "target_sequence_eligible": target_eligible,
                "continuation_sequence_eligible": continuation_eligible,
                "strict_D_and_D_plus_1_sequence_eligible": strict_window_eligible,
                "target_exclusion_reasons": list(
                    target.get("exclusion_reasons", ())
                    if isinstance(target, Mapping)
                    else ("upstream_target_day_not_audited",)
                ),
                "continuation_exclusion_reasons": list(
                    continuation.get("exclusion_reasons", ())
                    if isinstance(continuation, Mapping)
                    else ("upstream_continuation_day_not_audited",)
                ),
            }
            rows.append(row)
            if frozen_formal and not strict_window_eligible:
                formal_failures.append(row)

    if formal_failures:
        raise SequenceSupportError(
            "frozen formal denominator contains strict sequence failures: "
            + ", ".join(row["target_day"] for row in formal_failures)
        )

    formal_rows = [row for row in rows if row["frozen_formal_day"]]
    reduced_rows = [row for row in rows if not row["frozen_formal_day"]]
    sequence_reduced = [
        row
        for row in reduced_rows
        if row["target_sequence_eligible"] is False
        or row["continuation_sequence_eligible"] is False
    ]
    sequence_unconfirmed = [
        row
        for row in reduced_rows
        if row["target_sequence_eligible"] is None
        or row["continuation_sequence_eligible"] is None
    ]
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "spec": {"path": str(spec_path), "sha256": spec_sha256},
        "upstream_sequence_audit": {
            "json_path": str(audit_json_path),
            "json_sha256": audit_json_sha256,
            "csv_path": str(audit_csv_path),
            "csv_sha256": audit_csv_sha256,
            "upstream_identity_sha256": upstream_audit["identity_sha256"],
        },
        "strictness_contract": {
            "D_minus_1": (
                "target-start initialized and snapshot-seeded; earlier warmup gaps "
                "may recover before the target boundary"
            ),
            "D_and_D_plus_1": (
                "zero sequence gaps, invalid sequence messages, and time reversals"
            ),
            "execution_revalidation": (
                "required against current immutable native-hour cache bytes"
            ),
        },
        "counts": {
            "requested_days": len(rows),
            "frozen_formal_days": len(formal_rows),
            "frozen_reduced_days": len(reduced_rows),
            "formal_sequence_supported_days": sum(
                row["strict_D_and_D_plus_1_sequence_eligible"]
                for row in formal_rows
            ),
            "reduced_days_with_sequence_failure": len(sequence_reduced),
            "reduced_days_sequence_unconfirmed": len(sequence_unconfirmed),
        },
        "sequence_reduced_days": [row["target_day"] for row in sequence_reduced],
        "sequence_unconfirmed_days": [
            row["target_day"] for row in sequence_unconfirmed
        ],
        "days": rows,
        "permissions": {
            "economic_outcomes_read": False,
            "labels_generated": False,
            "orders_simulated": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    report["canonical_report_sha256"] = canonical_sha256(report)
    return report


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}"
    )
    try:
        with temporary.open("x", encoding="ascii") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def run(
    *,
    spec_path: Path = DEFAULT_SPEC,
    audit_json_path: Path = DEFAULT_AUDIT_JSON,
    audit_csv_path: Path = DEFAULT_AUDIT_CSV,
    output_path: Path = DEFAULT_OUTPUT,
    expected_spec_sha256: str = EXPECTED_SPEC_SHA256,
    expected_audit_json_sha256: str = EXPECTED_AUDIT_JSON_SHA256,
    expected_audit_csv_sha256: str = EXPECTED_AUDIT_CSV_SHA256,
) -> dict[str, Any]:
    spec = _require_bound(spec_path, expected_spec_sha256, label="v2 spec")
    audit_json = _require_bound(
        audit_json_path,
        expected_audit_json_sha256,
        label="native sequence audit JSON",
    )
    audit_csv = _require_bound(
        audit_csv_path,
        expected_audit_csv_sha256,
        label="native sequence audit CSV",
    )
    report = build_mapping(
        spec=_load_json(spec, label="v2 spec"),
        upstream_audit=_load_json(audit_json, label="native sequence audit"),
        spec_path=spec,
        spec_sha256=expected_spec_sha256,
        audit_json_path=audit_json,
        audit_json_sha256=expected_audit_json_sha256,
        audit_csv_path=audit_csv,
        audit_csv_sha256=expected_audit_csv_sha256,
    )
    _atomic_json(output_path, report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--audit-json", type=Path, default=DEFAULT_AUDIT_JSON)
    parser.add_argument("--audit-csv", type=Path, default=DEFAULT_AUDIT_CSV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    report = run(
        spec_path=args.spec,
        audit_json_path=args.audit_json,
        audit_csv_path=args.audit_csv,
        output_path=args.output,
    )
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
