#!/usr/bin/env python3
"""Build and validate the versioned normalized 100 ms L2 registry.

The registry is an immutable hardlink view over existing normalized BBO/L2
artifacts. It never rewrites source files. A complete source union is validated
before a staging directory is created, then the finished dataset is published
with one atomic rename.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DATASET_VERSION = "normalized_l2_100ms_v2"
CONTRACT_VERSION = 1
DAILY_QUALITY_FILENAME = "daily_quality.csv"
MANIFEST_FILENAME = "manifest.json"
DATA_KINDS = ("bbo", "l2")

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.download_cryptohft_orderbook import _normalized_day_summary  # noqa: E402
from models.audit.native_normalized_book_manifest import (  # noqa: E402
    normalized_summary_is_strict,
)


class NormalizedL2RegistryError(RuntimeError):
    """Base error for registry contract failures."""


class IncompleteSourceUnionError(NormalizedL2RegistryError):
    """Raised before writes when one or more good days lack a valid source."""


class FormalEligibilityError(NormalizedL2RegistryError):
    """Raised when formal replay requests a non-formal or altered day."""


@dataclass(frozen=True)
class SourceRootSpec:
    """One normalized source root, ordered from highest to lowest priority."""

    root: Path
    reconstruction_mode: str
    formal_capable: bool = False
    label: str = ""

    def normalized(self) -> SourceRootSpec:
        root = Path(self.root).expanduser().resolve()
        mode = self.reconstruction_mode.strip()
        if not mode:
            raise ValueError(f"source root has empty reconstruction mode: {root}")
        return SourceRootSpec(
            root=root,
            reconstruction_mode=mode,
            formal_capable=bool(self.formal_capable),
            label=self.label.strip() or root.name,
        )


@dataclass(frozen=True)
class CadencePolicy:
    """Structural gates for inclusion in the descriptive 100 ms registry.

    Coverage is intentionally not a registry-wide admission gate.  A rebuilt
    day may contain a source gap and remain useful in the 128-day descriptive
    panel.  Formal eligibility is inherited only from the separately frozen
    strict-day manifest, whose audit already required at least 99% coverage.
    """

    levels: int = 20
    freshness_s: float = 5.0
    min_coverage: float = 0.0
    min_valid_spread_ratio: float = 0.999
    max_p99_gap_s: float = 0.5


@dataclass(frozen=True)
class RegistryBuildResult:
    output_root: Path
    quality: pd.DataFrame
    manifest: dict[str, Any]
    dry_run: bool


@dataclass(frozen=True)
class _PairInspection:
    valid: bool
    reason: str
    bbo_rows: int
    l2_rows: int
    bbo_first_timestamp_ms: int
    bbo_last_timestamp_ms: int
    l2_first_timestamp_ms: int
    l2_last_timestamp_ms: int
    bbo_coverage: float
    l2_coverage: float
    bbo_p99_gap_s: float
    l2_p99_gap_s: float
    l2_valid_spread_ratio: float


@dataclass(frozen=True)
class _SelectedDay:
    day: str
    source: SourceRootSpec
    bbo_path: Path
    l2_path: Path
    inspection: _PairInspection


def default_dataset_root(data_root: Path) -> Path:
    return Path(data_root).expanduser().resolve() / DATASET_VERSION


def default_source_roots(data_root: Path) -> list[SourceRootSpec]:
    """Return the current priority order, with mixed legacy data last."""

    root = Path(data_root).expanduser().resolve()
    return [
        SourceRootSpec(
            root / "replay_l2_strict62_100ms_v1",
            "snapshot_24h_warmup",
            formal_capable=True,
        ),
        SourceRootSpec(
            root / "replay_l2_strict62_100ms_midnight_warmup_v1",
            "snapshot_24h_warmup",
            formal_capable=True,
        ),
        SourceRootSpec(
            root / "replay_l2_strict66_20260704_12_100ms_v1",
            "snapshot_24h_warmup",
        ),
        SourceRootSpec(
            root / "replay_l2_strict66_additional_100ms_v1",
            "snapshot_24h_warmup",
        ),
        SourceRootSpec(
            root / "replay_l2_retained100ms_v1",
            "delta_converged_120s",
        ),
        SourceRootSpec(
            root,
            "legacy_mixed_verified_100ms",
            label="legacy_mixed_bbo_l2",
        ),
    ]


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_identity(path: Path, *, logical_path: Path | None = None) -> dict[str, Any]:
    path = Path(path)
    resolved = path.resolve(strict=True)
    stat = resolved.stat()
    return {
        "path": str((logical_path or path).expanduser().resolve()),
        "resolved_path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
    }


def _parse_days(path: Path, *, label: str) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame.columns:
        raise NormalizedL2RegistryError(f"{label} must contain day: {path}")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        bad = frame.loc[parsed.isna(), "day"].astype(str).head(3).tolist()
        raise NormalizedL2RegistryError(f"{label} has invalid days {bad}: {path}")
    days = parsed.dt.strftime("%Y-%m-%d").tolist()
    if len(days) != len(set(days)):
        raise NormalizedL2RegistryError(f"{label} has duplicate days: {path}")
    if not days:
        raise NormalizedL2RegistryError(f"{label} is empty: {path}")
    return sorted(days)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    if value is None or bool(pd.isna(value)):
        return False
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n", "", "nan"}:
        return False
    raise NormalizedL2RegistryError(f"invalid boolean value: {value!r}")


def _indexed_csv(path: Path, *, label: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if "day" not in frame.columns:
        raise NormalizedL2RegistryError(f"{label} must contain day: {path}")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise NormalizedL2RegistryError(f"{label} contains invalid UTC days: {path}")
    frame = frame.copy()
    frame["day"] = parsed.dt.strftime("%Y-%m-%d")
    if frame["day"].duplicated().any():
        raise NormalizedL2RegistryError(f"{label} has duplicate days: {path}")
    return frame.set_index("day", drop=False)


def _expected_path(root: Path, *, kind: str, symbol: str, day: str) -> Path:
    return root / kind / f"{symbol}-{kind}-{day}.parquet"


def _inspect_normalized_root(
    *,
    root: Path,
    symbol: str,
    day: str,
    policy: CadencePolicy | None = None,
) -> _PairInspection:
    """Delegate cadence/schema evaluation to the existing native audit."""

    policy = policy or CadencePolicy()
    day_start = pd.Timestamp(day, tz="UTC").to_pydatetime()
    try:
        summary = _normalized_day_summary(
            root,
            symbol,
            day_start,
            int(float(policy.freshness_s) * 1_000),
            levels=int(policy.levels),
        )
        valid, reasons = normalized_summary_is_strict(
            summary,
            min_coverage=policy.min_coverage,
            min_valid_spread_ratio=policy.min_valid_spread_ratio,
            max_p99_gap_s=policy.max_p99_gap_s,
        )
    except Exception as exc:
        return _PairInspection(
            valid=False,
            reason=f"normalized_summary_error:{type(exc).__name__}:{exc}",
            bbo_rows=0,
            l2_rows=0,
            bbo_first_timestamp_ms=0,
            bbo_last_timestamp_ms=0,
            l2_first_timestamp_ms=0,
            l2_last_timestamp_ms=0,
            bbo_coverage=0.0,
            l2_coverage=0.0,
            bbo_p99_gap_s=float("inf"),
            l2_p99_gap_s=float("inf"),
            l2_valid_spread_ratio=0.0,
        )
    return _PairInspection(
        valid=bool(valid),
        reason="|".join(reasons),
        bbo_rows=int(summary.get("bbo_rows") or 0),
        l2_rows=int(summary.get("l2_rows") or 0),
        bbo_first_timestamp_ms=int(summary.get("bbo_first_ts") or 0),
        bbo_last_timestamp_ms=int(summary.get("bbo_last_ts") or 0),
        l2_first_timestamp_ms=int(summary.get("l2_first_ts") or 0),
        l2_last_timestamp_ms=int(summary.get("l2_last_ts") or 0),
        bbo_coverage=float(summary.get("bbo_coverage") or 0.0),
        l2_coverage=float(summary.get("l2_coverage") or 0.0),
        bbo_p99_gap_s=float(summary.get("bbo_p99_gap_s") or 0.0),
        l2_p99_gap_s=float(summary.get("l2_p99_gap_s") or 0.0),
        l2_valid_spread_ratio=float(
            summary.get("l2_valid_spread_ratio") or 0.0
        ),
    )


def _select_day_source(
    *,
    day: str,
    symbol: str,
    sources: Sequence[SourceRootSpec],
    cadence_policy: CadencePolicy,
) -> tuple[_SelectedDay | None, list[str]]:
    attempts: list[str] = []
    for source in sources:
        bbo_path = _expected_path(source.root, kind="bbo", symbol=symbol, day=day)
        l2_path = _expected_path(source.root, kind="l2", symbol=symbol, day=day)
        if not bbo_path.is_file() or not l2_path.is_file():
            missing = [
                kind
                for kind, path in (("bbo", bbo_path), ("l2", l2_path))
                if not path.is_file()
            ]
            attempts.append(f"{source.label}:missing_{'+'.join(missing)}")
            continue
        inspection = _inspect_normalized_root(
            root=source.root,
            symbol=symbol,
            day=day,
            policy=cadence_policy,
        )
        if not inspection.valid:
            attempts.append(f"{source.label}:invalid:{inspection.reason}")
            continue
        return (
            _SelectedDay(
                day=day,
                source=source,
                bbo_path=bbo_path,
                l2_path=l2_path,
                inspection=inspection,
            ),
            attempts,
        )
    return None, attempts


def _quality_row(
    *,
    selected: _SelectedDay,
    source_availability: pd.DataFrame,
    sequence_audit: pd.DataFrame,
    strict_days: set[str],
    bbo_identity: dict[str, Any],
    l2_identity: dict[str, Any],
) -> dict[str, Any]:
    day = selected.day
    availability = (
        source_availability.loc[day]
        if day in source_availability.index
        else None
    )
    sequence = sequence_audit.loc[day] if day in sequence_audit.index else None
    target_valid = bool(
        availability is not None
        and _parse_bool(availability.get("target_complete", False))
    )
    warmup_valid = bool(
        availability is not None
        and _parse_bool(availability.get("warmup_complete", False))
    )
    sequence_valid = bool(
        sequence is not None and _parse_bool(sequence.get("eligible", False))
    )
    strict_listed = day in strict_days
    formal_eligible = bool(
        target_valid
        and warmup_valid
        and sequence_valid
        and strict_listed
        and selected.source.formal_capable
    )
    exclusions: list[str] = []
    if not target_valid:
        exclusions.append("target_source_incomplete")
    if not warmup_valid:
        exclusions.append("warmup_invalid")
    if not sequence_valid:
        exclusions.append("sequence_invalid")
    if not strict_listed:
        exclusions.append("not_in_normalized_strict_days")
    if not selected.source.formal_capable:
        exclusions.append("selected_source_not_formal_capable")

    inspection = selected.inspection
    return {
        "day": day,
        "rebuilt": True,
        "sequence_valid": sequence_valid,
        "warmup_valid": warmup_valid,
        "target_source_valid": target_valid,
        "strict_listed": strict_listed,
        "formal_eligible": formal_eligible,
        "formal_exclusion_reason": "|".join(exclusions),
        "source_root": str(selected.source.root),
        "source_label": selected.source.label,
        "reconstruction_mode": selected.source.reconstruction_mode,
        "source_formal_capable": selected.source.formal_capable,
        "cadence_schema_valid": inspection.valid,
        "bbo_rows": inspection.bbo_rows,
        "l2_rows": inspection.l2_rows,
        "bbo_first_timestamp_ms": inspection.bbo_first_timestamp_ms,
        "bbo_last_timestamp_ms": inspection.bbo_last_timestamp_ms,
        "l2_first_timestamp_ms": inspection.l2_first_timestamp_ms,
        "l2_last_timestamp_ms": inspection.l2_last_timestamp_ms,
        "bbo_coverage": inspection.bbo_coverage,
        "l2_coverage": inspection.l2_coverage,
        "coverage_99_valid": bool(
            inspection.bbo_coverage >= 0.99
            and inspection.l2_coverage >= 0.99
        ),
        "bbo_p99_gap_s": inspection.bbo_p99_gap_s,
        "l2_p99_gap_s": inspection.l2_p99_gap_s,
        "l2_valid_spread_ratio": inspection.l2_valid_spread_ratio,
        "bbo_source_path": str(selected.bbo_path.resolve()),
        "bbo_sha256": bbo_identity["sha256"],
        "bbo_size_bytes": bbo_identity["size_bytes"],
        "l2_source_path": str(selected.l2_path.resolve()),
        "l2_sha256": l2_identity["sha256"],
        "l2_size_bytes": l2_identity["size_bytes"],
    }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def assemble_registry(
    *,
    output_root: Path,
    good_days_path: Path,
    source_availability_path: Path,
    sequence_audit_path: Path,
    normalized_strict_days_path: Path,
    source_roots: Sequence[SourceRootSpec],
    symbol: str = "BTCUSDC",
    cadence_policy: CadencePolicy | None = None,
    dry_run: bool = False,
) -> RegistryBuildResult:
    """Assemble a complete immutable hardlink registry for all good days."""

    cadence_policy = cadence_policy or CadencePolicy()
    output_root = Path(output_root).expanduser().resolve()
    inputs = {
        "good_days": Path(good_days_path).expanduser().resolve(),
        "source_availability": Path(source_availability_path).expanduser().resolve(),
        "sequence_audit": Path(sequence_audit_path).expanduser().resolve(),
        "normalized_strict_days": Path(normalized_strict_days_path).expanduser().resolve(),
    }
    sources = [source.normalized() for source in source_roots]
    if not sources:
        raise NormalizedL2RegistryError("at least one source root is required")
    if len({str(source.root) for source in sources}) != len(sources):
        raise NormalizedL2RegistryError("source roots must be unique")
    if output_root in {source.root for source in sources}:
        raise NormalizedL2RegistryError("output root cannot also be a source root")
    if output_root.exists():
        raise NormalizedL2RegistryError(
            f"immutable output root already exists: {output_root}"
        )

    good_days = _parse_days(inputs["good_days"], label="good-day manifest")
    strict_days = set(
        _parse_days(inputs["normalized_strict_days"], label="normalized strict days")
    )
    unknown_strict = strict_days.difference(good_days)
    if unknown_strict:
        raise NormalizedL2RegistryError(
            f"strict manifest contains non-good days: {sorted(unknown_strict)[:5]}"
        )
    source_availability = _indexed_csv(
        inputs["source_availability"], label="source availability"
    )
    sequence_audit = _indexed_csv(inputs["sequence_audit"], label="sequence audit")

    selected_days: list[_SelectedDay] = []
    unresolved: dict[str, list[str]] = {}
    for day in good_days:
        selected, attempts = _select_day_source(
            day=day,
            symbol=symbol,
            sources=sources,
            cadence_policy=cadence_policy,
        )
        if selected is None:
            unresolved[day] = attempts
        else:
            selected_days.append(selected)
    if unresolved:
        details = "; ".join(
            f"{day}=[{', '.join(attempts)}]"
            for day, attempts in list(unresolved.items())[:8]
        )
        raise IncompleteSourceUnionError(
            f"source union does not cover {len(unresolved)}/{len(good_days)} "
            f"good days before writes: {details}"
        )

    identities: dict[tuple[str, str], dict[str, Any]] = {}
    quality_rows: list[dict[str, Any]] = []
    file_records: list[dict[str, Any]] = []
    for selected in selected_days:
        for kind, path in (("bbo", selected.bbo_path), ("l2", selected.l2_path)):
            identity = _file_identity(path)
            identities[(selected.day, kind)] = identity
            file_records.append(
                {
                    "day": selected.day,
                    "kind": kind,
                    "destination_relative_path": (
                        f"{kind}/{symbol}-{kind}-{selected.day}.parquet"
                    ),
                    "source_root": str(selected.source.root),
                    "source_relative_path": str(
                        path.relative_to(selected.source.root)
                    ),
                    "source_label": selected.source.label,
                    "reconstruction_mode": selected.source.reconstruction_mode,
                    "source_identity": identity,
                }
            )
        quality_rows.append(
            _quality_row(
                selected=selected,
                source_availability=source_availability,
                sequence_audit=sequence_audit,
                strict_days=strict_days,
                bbo_identity=identities[(selected.day, "bbo")],
                l2_identity=identities[(selected.day, "l2")],
            )
        )

    quality = pd.DataFrame(quality_rows).sort_values("day").reset_index(drop=True)
    input_identities = {
        name: _file_identity(path) for name, path in inputs.items()
    }
    module_identity = _file_identity(Path(__file__))
    legacy_mixed_sources: list[dict[str, Any]] = []
    for source in sources:
        if source.reconstruction_mode != "legacy_mixed_verified_100ms":
            continue
        included = [
            {
                "day": record["day"],
                "kind": record["kind"],
                "relative_path": record["source_relative_path"],
                "resolved_path": record["source_identity"]["resolved_path"],
                "size_bytes": record["source_identity"]["size_bytes"],
                "sha256": record["source_identity"]["sha256"],
            }
            for record in file_records
            if record["source_root"] == str(source.root)
        ]
        identity_payload = {
            "root": str(source.root),
            "bbo_directory": str(source.root / "bbo"),
            "l2_directory": str(source.root / "l2"),
            "included_files": included,
        }
        legacy_mixed_sources.append(
            {
                **identity_payload,
                "included_file_count": len(included),
                "identity_sha256": _canonical_sha256(identity_payload),
            }
        )
    manifest: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "dataset_version": DATASET_VERSION,
        "symbol": symbol,
        "output_root": str(output_root),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dry_run": bool(dry_run),
        "builder": module_identity,
        "inputs": input_identities,
        "cadence_policy": asdict(cadence_policy),
        "source_roots": [
            {
                "priority": priority,
                "root": str(source.root),
                "label": source.label,
                "reconstruction_mode": source.reconstruction_mode,
                "formal_capable": source.formal_capable,
            }
            for priority, source in enumerate(sources)
        ],
        "day_count": int(len(quality)),
        "formal_day_count": int(quality["formal_eligible"].sum()),
        "source_counts": {
            str(key): int(value)
            for key, value in quality["source_label"].value_counts().items()
        },
        "legacy_mixed_sources": legacy_mixed_sources,
        "files": file_records,
    }
    if dry_run:
        return RegistryBuildResult(output_root, quality, manifest, True)

    output_root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(
        tempfile.mkdtemp(
            prefix=f".{output_root.name}.staging-",
            dir=output_root.parent,
        )
    )
    try:
        for kind in DATA_KINDS:
            (stage / kind).mkdir()
        for selected in selected_days:
            for kind, source_path in (
                ("bbo", selected.bbo_path),
                ("l2", selected.l2_path),
            ):
                destination = (
                    stage / kind / f"{symbol}-{kind}-{selected.day}.parquet"
                )
                os.link(source_path.resolve(strict=True), destination)
                if not os.path.samefile(source_path.resolve(strict=True), destination):
                    raise NormalizedL2RegistryError(
                        f"hardlink identity mismatch: {source_path} -> {destination}"
                    )

        quality_path = stage / DAILY_QUALITY_FILENAME
        quality.to_csv(quality_path, index=False)
        quality_identity = _file_identity(
            quality_path,
            logical_path=output_root / DAILY_QUALITY_FILENAME,
        )
        manifest["daily_quality"] = quality_identity
        manifest["dry_run"] = False
        _atomic_json(stage / MANIFEST_FILENAME, manifest)
        os.replace(stage, output_root)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise

    return RegistryBuildResult(output_root, quality, manifest, False)


def _load_contract(dataset_root: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    root = Path(dataset_root).expanduser().resolve()
    manifest_path = root / MANIFEST_FILENAME
    quality_path = root / DAILY_QUALITY_FILENAME
    if not manifest_path.is_file() or not quality_path.is_file():
        raise FormalEligibilityError(f"registry contract is incomplete: {root}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("dataset_version") != DATASET_VERSION:
        raise FormalEligibilityError(
            f"expected {DATASET_VERSION}, got {manifest.get('dataset_version')}"
        )
    expected_quality_hash = manifest.get("daily_quality", {}).get("sha256")
    actual_quality_hash = sha256_file(quality_path)
    if not expected_quality_hash or actual_quality_hash != expected_quality_hash:
        raise FormalEligibilityError("daily_quality.csv identity mismatch")
    quality = pd.read_csv(quality_path, dtype={"day": str})
    required = {
        "day",
        "formal_eligible",
        "bbo_sha256",
        "bbo_size_bytes",
        "l2_sha256",
        "l2_size_bytes",
    }
    missing = required.difference(quality.columns)
    if missing:
        raise FormalEligibilityError(
            f"daily quality contract missing columns: {sorted(missing)}"
        )
    if quality["day"].duplicated().any():
        raise FormalEligibilityError("daily quality contract has duplicate days")
    return manifest, quality.set_index("day", drop=False)


def require_formal_days(
    dataset_root: Path,
    days: Iterable[str],
    *,
    verify_hashes: bool = False,
) -> pd.DataFrame:
    """Fail fast unless every requested day has a frozen formal identity."""

    root = Path(dataset_root).expanduser().resolve()
    manifest, quality = _load_contract(root)
    symbol = str(manifest.get("symbol", "BTCUSDC"))
    requested = sorted(set(str(day) for day in days))
    if not requested:
        raise FormalEligibilityError("formal day request is empty")
    accepted: list[pd.Series] = []
    for day in requested:
        try:
            normalized_day = pd.Timestamp(day, tz="UTC").strftime("%Y-%m-%d")
        except Exception as exc:
            raise FormalEligibilityError(f"invalid requested UTC day: {day}") from exc
        if normalized_day != day:
            raise FormalEligibilityError(
                f"requested day must use YYYY-MM-DD: {day}"
            )
        if day not in quality.index:
            raise FormalEligibilityError(f"day is absent from registry: {day}")
        row = quality.loc[day]
        if not _parse_bool(row["formal_eligible"]):
            reason = row.get("formal_exclusion_reason", "")
            raise FormalEligibilityError(
                f"day is not formal eligible: {day} ({reason})"
            )
        for kind in DATA_KINDS:
            path = _expected_path(root, kind=kind, symbol=symbol, day=day)
            if not path.is_file():
                raise FormalEligibilityError(
                    f"formal {kind} artifact is missing for {day}: {path}"
                )
            expected_size = int(row[f"{kind}_size_bytes"])
            if path.stat().st_size != expected_size:
                raise FormalEligibilityError(
                    f"formal {kind} size mismatch for {day}: {path}"
                )
            if verify_hashes:
                expected_hash = str(row[f"{kind}_sha256"])
                if sha256_file(path) != expected_hash:
                    raise FormalEligibilityError(
                        f"formal {kind} SHA256 mismatch for {day}: {path}"
                    )
        accepted.append(row)
    return pd.DataFrame(accepted).reset_index(drop=True)


def require_formal_day(
    dataset_root: Path,
    day: str,
    *,
    verify_hashes: bool = False,
) -> pd.Series:
    return require_formal_days(
        dataset_root,
        [day],
        verify_hashes=verify_hashes,
    ).iloc[0]


def _parse_source_spec(value: str) -> SourceRootSpec:
    parts = value.split("|")
    if len(parts) not in {2, 3, 4}:
        raise argparse.ArgumentTypeError(
            "source must be ROOT|MODE[|FORMAL_CAPABLE[|LABEL]]"
        )
    formal = _parse_bool(parts[2]) if len(parts) >= 3 else False
    label = parts[3] if len(parts) == 4 else ""
    return SourceRootSpec(Path(parts[0]), parts[1], formal, label)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    assemble = subparsers.add_parser("assemble")
    assemble.add_argument("--output-root", type=Path, required=True)
    assemble.add_argument("--good-days", type=Path, required=True)
    assemble.add_argument("--source-availability", type=Path, required=True)
    assemble.add_argument("--sequence-audit", type=Path, required=True)
    assemble.add_argument("--normalized-strict-days", type=Path, required=True)
    assemble.add_argument(
        "--source",
        type=_parse_source_spec,
        action="append",
        required=True,
        help="Priority-ordered ROOT|MODE[|FORMAL_CAPABLE[|LABEL]]",
    )
    assemble.add_argument("--symbol", default="BTCUSDC")
    assemble.add_argument("--dry-run", action="store_true")

    validate = subparsers.add_parser("validate-formal")
    validate.add_argument("--dataset-root", type=Path, required=True)
    validate.add_argument("--day", action="append", required=True)
    validate.add_argument("--verify-hashes", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "assemble":
        result = assemble_registry(
            output_root=args.output_root,
            good_days_path=args.good_days,
            source_availability_path=args.source_availability,
            sequence_audit_path=args.sequence_audit,
            normalized_strict_days_path=args.normalized_strict_days,
            source_roots=args.source,
            symbol=args.symbol,
            dry_run=args.dry_run,
        )
        print(
            json.dumps(
                {
                    "dataset_version": DATASET_VERSION,
                    "output_root": str(result.output_root),
                    "dry_run": result.dry_run,
                    "day_count": int(len(result.quality)),
                    "formal_day_count": int(
                        result.quality["formal_eligible"].sum()
                    ),
                    "source_counts": result.manifest["source_counts"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    accepted = require_formal_days(
        args.dataset_root,
        args.day,
        verify_hashes=args.verify_hashes,
    )
    print(accepted[["day", "source_label", "reconstruction_mode"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
