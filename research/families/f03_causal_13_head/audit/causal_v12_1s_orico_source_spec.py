#!/usr/bin/env python3
"""Build an exact, authority-validated ORICO source spec for one F03 1s day."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)

SCHEMA_VERSION = "causal_v12_1s_orico_source_spec_builder.v1"
PROVIDER_NORMALIZED_PROFILE = "provider_normalized_v1"
NATIVE_NORMALIZED_PROFILE = "native_normalized_v1"
NATIVE_HISTORICAL_MINIMAL141_PROFILE = "native_historical_minimal141_individual_reference_v1"
PER_DAY_JSON_QUALITY_AUTHORITY = "per_day_json_v1"
REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY = "registry_manifest_daily_csv_v1"


@dataclass(frozen=True, slots=True)
class OricoSourceProfile:
    profile_id: str
    local_trade_tempo_dir: str
    local_manifest_path: str
    execution_l2_dir: str
    execution_l2_quality_dir: str
    metrics_dir: str
    reference_bar_dir: str
    execution_l2_clock_identity: str
    execution_l2_quality_authority: str = PER_DAY_JSON_QUALITY_AUTHORITY
    execution_l2_manifest_path: str | None = None
    execution_l2_quality_path: str | None = None
    local_source_identity: str = "binance_futures_individual_trades_1s_tempo.v1"
    reference_source_identity: str = "binance_futures_reference_trades_1s.v1"


PROFILES = {
    PROVIDER_NORMALIZED_PROFILE: OricoSourceProfile(
        profile_id=PROVIDER_NORMALIZED_PROFILE,
        local_trade_tempo_dir=("trade_features_causal_v5_expanded_20250801_20260725/BTCUSDC"),
        local_manifest_path=("trade_features_causal_v5_expanded_20250801_20260725/manifest.json"),
        execution_l2_dir="normalized_tardis_l2_100ms_v1/l2",
        execution_l2_quality_dir="normalized_tardis_l2_100ms_v1/quality",
        metrics_dir="raw_metrics",
        reference_bar_dir="bars_1s",
        execution_l2_clock_identity="tardis_provider_local_visibility_ms",
    ),
    NATIVE_NORMALIZED_PROFILE: OricoSourceProfile(
        profile_id=NATIVE_NORMALIZED_PROFILE,
        local_trade_tempo_dir="trade_features_causal_v6_postfit_20260726_31/BTCUSDC",
        local_manifest_path="trade_features_causal_v6_postfit_20260726_31/manifest.json",
        execution_l2_dir=("normalized_l2_postfit_oos_20260725_31_context_registry_v1/l2"),
        execution_l2_quality_dir="native_l2_daily_quality_v1/quality",
        metrics_dir="raw_metrics",
        reference_bar_dir="reference_bars_1s_trades_v1",
        execution_l2_clock_identity="cryptohft_transaction_time_100ms_grid",
    ),
    NATIVE_HISTORICAL_MINIMAL141_PROFILE: OricoSourceProfile(
        profile_id=NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        local_trade_tempo_dir=("trade_features_causal_v5_expanded_20250801_20260725/BTCUSDC"),
        local_manifest_path=("trade_features_causal_v5_expanded_20250801_20260725/manifest.json"),
        execution_l2_dir="normalized_l2_100ms_v2_minimal141_20260727/l2",
        execution_l2_quality_dir="normalized_l2_100ms_v2_minimal141_20260727",
        metrics_dir="raw_metrics",
        reference_bar_dir="reference_bars_1s_trades_v1",
        execution_l2_clock_identity="cryptohft_transaction_time_100ms_grid",
        execution_l2_quality_authority=REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY,
        execution_l2_manifest_path=("normalized_l2_100ms_v2_minimal141_20260727/manifest.json"),
        execution_l2_quality_path=("normalized_l2_100ms_v2_minimal141_20260727/daily_quality.csv"),
        reference_source_identity="binance_futures_reference_individual_trades_1s.v1",
    ),
}


@dataclass(frozen=True, slots=True)
class BuiltSourceSpec:
    bundle: daily.DailySourceBundle
    probe: dict[str, Any]
    profile_id: str
    market_data_root: Path


def _canonical_day(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise base.FeatureContractError("target day must be canonical YYYY-MM-DD") from exc
    canonical = parsed.strftime("%Y-%m-%d")
    if canonical != value:
        raise base.FeatureContractError("target day must be canonical YYYY-MM-DD")
    return canonical


def _required_days(target_day: str) -> tuple[str, str]:
    target = datetime.strptime(target_day, "%Y-%m-%d").replace(tzinfo=UTC)
    return ((target - timedelta(days=1)).strftime("%Y-%m-%d"), target_day)


def _inside_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _require_exact_files(paths: tuple[Path, ...], *, root: Path) -> None:
    escaped = [str(path) for path in paths if not _inside_root(path, root)]
    if escaped:
        raise base.FeatureContractError(
            "resolved authority paths escape market-data root: " + ", ".join(escaped)
        )
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise base.FeatureContractError(
            "missing exact ORICO authority paths; fallback discovery is forbidden: "
            + ", ".join(missing)
        )


def _resolve_l2_quality_paths(
    *, root: Path, profile: OricoSourceProfile, days: tuple[str, str]
) -> tuple[Path, ...]:
    if profile.execution_l2_quality_authority == PER_DAY_JSON_QUALITY_AUTHORITY:
        return tuple(
            (root / profile.execution_l2_quality_dir / f"BTCUSDC-{item}.json").resolve()
            for item in days
        )
    if profile.execution_l2_quality_authority == REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY:
        if profile.execution_l2_manifest_path is None or profile.execution_l2_quality_path is None:
            raise base.FeatureContractError(
                "registry L2 authority requires explicit manifest and daily-quality paths"
            )
        return (
            (root / profile.execution_l2_manifest_path).resolve(),
            (root / profile.execution_l2_quality_path).resolve(),
        )
    raise base.FeatureContractError(
        f"unsupported execution L2 quality authority: {profile.execution_l2_quality_authority}"
    )


def resolve_orico_daily_source_bundle(
    *,
    target_day: str,
    market_data_root: Path,
    profile_id: str = PROVIDER_NORMALIZED_PROFILE,
) -> daily.DailySourceBundle:
    """Resolve deterministic D-1/D paths without searching for substitutes."""

    day = _canonical_day(target_day)
    profile = PROFILES.get(profile_id)
    if profile is None:
        raise base.FeatureContractError(f"unknown ORICO source profile: {profile_id}")
    root = market_data_root.expanduser().resolve()
    if not root.is_dir():
        raise base.FeatureContractError(f"market-data root is not a directory: {root}")
    warmup_day, target_day = _required_days(day)
    days = (warmup_day, target_day)

    local_paths = tuple(
        (root / profile.local_trade_tempo_dir / f"BTCUSDC-trade-tempo-{item}.parquet").resolve()
        for item in days
    )
    local_manifest = (root / profile.local_manifest_path).resolve()
    l2_paths = tuple(
        (root / profile.execution_l2_dir / f"BTCUSDC-l2-{item}.parquet").resolve() for item in days
    )
    quality_paths = _resolve_l2_quality_paths(root=root, profile=profile, days=days)
    metric_paths = tuple(
        (root / profile.metrics_dir / f"BTCUSDC-metrics-{item}.csv").resolve() for item in days
    )
    reference_paths = tuple(
        (root / profile.reference_bar_dir / f"BTCUSDT-1s-{item}.parquet").resolve() for item in days
    )
    reference_meta_paths = tuple(
        (root / profile.reference_bar_dir / f"BTCUSDT-1s-{item}.parquet.meta.json").resolve()
        for item in days
    )
    all_paths = (
        *local_paths,
        local_manifest,
        *l2_paths,
        *quality_paths,
        *metric_paths,
        *reference_paths,
        *reference_meta_paths,
    )
    _require_exact_files(all_paths, root=root)

    return daily.DailySourceBundle(
        utc_day=target_day,
        local_trade_tempo_paths=local_paths,
        local_source_manifest_paths=(local_manifest,),
        execution_l2_paths=l2_paths,
        execution_l2_quality_paths=quality_paths,
        metric_paths=metric_paths,
        reference_bar_paths=reference_paths,
        reference_bar_manifest_paths=reference_meta_paths,
        local_source_identity=profile.local_source_identity,
        execution_l2_clock_identity=profile.execution_l2_clock_identity,
        reference_source_identity=profile.reference_source_identity,
    )


def _strict_csv_bool(value: str, *, field: str, day: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise base.FeatureContractError(
        f"native L2 daily quality {day} has non-canonical {field}: {value!r}"
    )


def _unique_rows_by_day(rows: list[dict[str, str]], *, source: str) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    for row in rows:
        day = row.get("day", "")
        _canonical_day(day)
        if day in indexed:
            raise base.FeatureContractError(f"duplicate {source} day: {day}")
        indexed[day] = row
    return indexed


def _validate_registry_manifest_csv_l2_authority(
    bundle: daily.DailySourceBundle,
    profile: OricoSourceProfile,
) -> dict[str, Any]:
    """Validate the minimal141 registry without translating it to per-day JSON."""

    if len(bundle.execution_l2_quality_paths) != 2:
        raise base.FeatureContractError(
            "registry L2 authority requires exactly manifest.json and daily_quality.csv"
        )
    manifest_path, quality_path = bundle.execution_l2_quality_paths
    if manifest_path.name != "manifest.json" or quality_path.name != "daily_quality.csv":
        raise base.FeatureContractError(
            "registry L2 authority paths must be exact manifest.json and daily_quality.csv"
        )
    if bundle.execution_l2_clock_identity != profile.execution_l2_clock_identity:
        raise base.FeatureContractError("registry L2 clock identity drift")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("dataset_version") != "normalized_l2_100ms_v2":
        raise base.FeatureContractError("unsupported native L2 registry dataset version")
    if manifest.get("contract_version") != 1:
        raise base.FeatureContractError("unsupported native L2 registry contract version")
    cadence = manifest.get("cadence_policy")
    if not isinstance(cadence, dict) or cadence.get("levels") != 20:
        raise base.FeatureContractError("native L2 registry does not bind 20 depth levels")

    quality_identity = manifest.get("daily_quality")
    if not isinstance(quality_identity, dict):
        raise base.FeatureContractError("native L2 registry lacks daily-quality identity")
    if quality_identity.get("sha256") != daily.sha256_file(quality_path):
        raise base.FeatureContractError("native L2 daily-quality SHA256 mismatch")
    if quality_identity.get("size_bytes") != quality_path.stat().st_size:
        raise base.FeatureContractError("native L2 daily-quality size mismatch")

    file_rows = manifest.get("files")
    if not isinstance(file_rows, list):
        raise base.FeatureContractError("native L2 registry files is not a list")
    l2_manifest_rows = [
        row for row in file_rows if isinstance(row, dict) and row.get("kind") == "l2"
    ]
    manifest_by_day = _unique_rows_by_day(l2_manifest_rows, source="native L2 manifest")
    if manifest.get("day_count") not in (None, len(manifest_by_day)):
        raise base.FeatureContractError("native L2 registry day-count mismatch")
    with quality_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {
            "day",
            "rebuilt",
            "sequence_valid",
            "warmup_valid",
            "target_source_valid",
            "formal_eligible",
            "formal_exclusion_reason",
            "source_label",
            "reconstruction_mode",
            "source_formal_capable",
            "cadence_schema_valid",
            "l2_rows",
            "l2_source_path",
            "l2_sha256",
            "l2_size_bytes",
        }
        if reader.fieldnames is None or not required_columns.issubset(reader.fieldnames):
            raise base.FeatureContractError("native L2 daily-quality schema is incomplete")
        quality_by_day = _unique_rows_by_day(list(reader), source="native L2 daily quality")
    if set(quality_by_day) != set(manifest_by_day):
        raise base.FeatureContractError("native L2 registry/quality day universe mismatch")

    warmup_day, target_day = _required_days(bundle.utc_day)
    # Match only the frozen filename; content-based discovery is forbidden.
    l2_by_day = {
        item: next(
            (
                path
                for path in bundle.execution_l2_paths
                if path.name == f"BTCUSDC-l2-{item}.parquet"
            ),
            None,
        )
        for item in (warmup_day, target_day)
    }
    bound_days: list[dict[str, Any]] = []
    errors: list[str] = []
    for day, role in ((warmup_day, "warmup"), (target_day, "target")):
        day_errors: list[str] = []
        path = l2_by_day[day]
        manifest_row = manifest_by_day.get(day)
        quality_row = quality_by_day.get(day)
        if path is None:
            day_errors.append("exact L2 path is absent")
        if manifest_row is None:
            day_errors.append("native L2 manifest entry is absent")
        if quality_row is None:
            day_errors.append("native L2 daily-quality row is absent")
        if path is not None and manifest_row is not None and quality_row is not None:
            actual_sha = daily.sha256_file(path)
            actual_size = path.stat().st_size
            actual_rows = pq.ParquetFile(path).metadata.num_rows
            expected_relative = f"l2/BTCUSDC-l2-{day}.parquet"
            if manifest_row.get("destination_relative_path") != expected_relative:
                day_errors.append("native L2 manifest destination path mismatch")
            if manifest_row.get("source_label") not in (None, "registry_20260727"):
                day_errors.append("native L2 manifest source label mismatch")
            if manifest_row.get("reconstruction_mode") not in (
                None,
                "registry_snapshot_20260727",
            ):
                day_errors.append("native L2 manifest reconstruction mode mismatch")
            source_identity = manifest_row.get("source_identity")
            if not isinstance(source_identity, dict):
                day_errors.append("native L2 manifest lacks source identity")
            else:
                if source_identity.get("sha256") != actual_sha:
                    day_errors.append("native L2 manifest SHA256 mismatch")
                if source_identity.get("size_bytes") != actual_size:
                    day_errors.append("native L2 manifest size mismatch")
            if quality_row.get("l2_sha256") != actual_sha:
                day_errors.append("native L2 daily-quality SHA256 mismatch")
            if int(quality_row.get("l2_size_bytes", "-1")) != actual_size:
                day_errors.append("native L2 daily-quality size mismatch")
            if int(quality_row.get("l2_rows", "-1")) != actual_rows:
                day_errors.append("native L2 daily-quality row-count mismatch")
            if Path(quality_row.get("l2_source_path", "")).name != path.name:
                day_errors.append("native L2 daily-quality filename mismatch")
            for field in ("rebuilt", "source_formal_capable", "cadence_schema_valid"):
                if not _strict_csv_bool(quality_row.get(field, ""), field=field, day=day):
                    day_errors.append(f"native L2 daily-quality {field} is false")
            if quality_row.get("source_label") != "registry_20260727":
                day_errors.append("native L2 source label mismatch")
            if quality_row.get("reconstruction_mode") != "registry_snapshot_20260727":
                day_errors.append("native L2 reconstruction mode mismatch")
            role_field = "warmup_valid" if role == "warmup" else "formal_eligible"
            if not _strict_csv_bool(quality_row.get(role_field, ""), field=role_field, day=day):
                day_errors.append(f"native L2 {role_field} is false")
            if role == "target":
                for field in ("target_source_valid", "sequence_valid"):
                    if not _strict_csv_bool(quality_row.get(field, ""), field=field, day=day):
                        day_errors.append(f"native L2 target {field} is false")
            if role == "target" and quality_row.get("formal_exclusion_reason", ""):
                day_errors.append("native L2 target has a formal exclusion reason")
            names = set(pq.ParquetFile(path).schema_arrow.names)
            expected_levels = {
                f"{side}_{kind}_{level}"
                for side in ("bid", "ask")
                for kind in ("px", "qty")
                for level in range(1, 21)
            }
            if not expected_levels.issubset(names):
                day_errors.append("native L2 Parquet lacks the 20 declared depth levels")
        errors.extend(f"{day}: {message}" for message in day_errors)
        bound_days.append(
            {
                "day": day,
                "role": role,
                "l2_path": None if path is None else str(path),
                "valid": not day_errors,
                "errors": day_errors,
            }
        )
    return {
        "authority_mode": REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY,
        "expected_clock_source": profile.execution_l2_clock_identity,
        "manifest_path": str(manifest_path),
        "manifest_sha256": daily.sha256_file(manifest_path),
        "daily_quality_path": str(quality_path),
        "daily_quality_sha256": daily.sha256_file(quality_path),
        "bound_days": bound_days,
        "errors": errors,
        "valid": not errors,
    }


def _probe_source_bundle(
    bundle: daily.DailySourceBundle, profile: OricoSourceProfile
) -> dict[str, Any]:
    probe = daily.probe_source_bundle(bundle)
    if profile.execution_l2_quality_authority == PER_DAY_JSON_QUALITY_AUTHORITY:
        probe.update(
            {
                "schema_version": "causal_v12_1s_profile_bound_source_probe.v3",
                "profile_id": profile.profile_id,
                "source_permissions": execution_identity.SOURCE_PERMISSION_CONTRACT,
                "fallback_discovery_used": False,
                "substitute_warmup_used": False,
                "aggregate_reference_bars_used": False,
            }
        )
        return probe

    l2_authority = _validate_registry_manifest_csv_l2_authority(bundle, profile)
    coverage = probe["path_day_coverage"]
    coverage["execution_l2_quality"] = {
        "group": "execution_l2_quality",
        "required_days": list(_required_days(bundle.utc_day)),
        "authority_paths": [str(path) for path in bundle.execution_l2_quality_paths],
        "authority_mode": REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY,
        "valid": l2_authority["valid"],
    }
    for row in probe["files"]:
        if row["group"] == "execution_l2_quality":
            row["schema_supported"] = l2_authority["valid"]
            row.pop("schema_error", None)
    probe["execution_l2_quality_authority"] = l2_authority
    failure_reasons: list[str] = []
    for group, result in coverage.items():
        if not result["valid"]:
            failure_reasons.append(f"{group}: D-1/target path coverage is incomplete")
    for name, result in (
        ("local_manifest", probe["local_source_authority"]),
        ("execution_l2_quality", l2_authority),
        ("metrics", probe["metrics_authority"]),
        ("reference_manifest", probe["reference_btcusdt_authority"]),
        ("bar_clock", probe["bar_clock_authority"]),
    ):
        if not result["valid"]:
            failure_reasons.append(f"{name}: authority binding failed")
    if not all(bool(row.get("schema_supported")) for row in probe["files"]):
        failure_reasons.append("one or more physical file schemas are unsupported")
    probe.update(
        {
            "schema_version": "causal_v12_1s_profile_bound_source_probe.v3",
            "profile_id": profile.profile_id,
            "source_permissions": {
                **execution_identity.SOURCE_PERMISSION_CONTRACT,
                "feature_prediction_training_authority": False,
                "feature_parity_authority": True,
            },
            "physical_materialization_eligible": not failure_reasons,
            "failure_reasons": failure_reasons,
            "fallback_discovery_used": False,
            "substitute_warmup_used": False,
            "aggregate_reference_bars_used": False,
            "economic_outcomes_read": False,
        }
    )
    return probe


def build_orico_daily_source_spec(
    *,
    target_day: str,
    market_data_root: Path,
    profile_id: str = PROVIDER_NORMALIZED_PROFILE,
) -> BuiltSourceSpec:
    bundle = resolve_orico_daily_source_bundle(
        target_day=target_day,
        market_data_root=market_data_root,
        profile_id=profile_id,
    )
    profile = PROFILES[profile_id]
    probe = _probe_source_bundle(bundle, profile)
    if not probe.get("physical_materialization_eligible"):
        reasons = probe.get("failure_reasons", ["unknown authority failure"])
        raise base.FeatureContractError(
            "resolved ORICO bundle failed physical authority: "
            + "; ".join(str(reason) for reason in reasons)
        )
    return BuiltSourceSpec(
        bundle=bundle,
        probe=probe,
        profile_id=profile_id,
        market_data_root=market_data_root.expanduser().resolve(),
    )


def source_spec_payload(bundle: daily.DailySourceBundle) -> dict[str, Any]:
    return {
        "utc_day": bundle.utc_day,
        "local_trade_tempo_paths": [str(path) for path in bundle.local_trade_tempo_paths],
        "local_source_manifest_paths": [str(path) for path in bundle.local_source_manifest_paths],
        "execution_l2_paths": [str(path) for path in bundle.execution_l2_paths],
        "execution_l2_quality_paths": [str(path) for path in bundle.execution_l2_quality_paths],
        "metric_paths": [str(path) for path in bundle.metric_paths],
        "reference_bar_paths": [str(path) for path in bundle.reference_bar_paths],
        "reference_bar_manifest_paths": [str(path) for path in bundle.reference_bar_manifest_paths],
        "local_source_identity": bundle.local_source_identity,
        "execution_l2_clock_identity": bundle.execution_l2_clock_identity,
        "metric_source_identity": bundle.metric_source_identity,
        "reference_source_identity": bundle.reference_source_identity,
    }


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_write_json(path: Path, payload: Any) -> bool:
    """Publish JSON atomically; reuse only an exactly identical payload."""

    output = path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if _canonical_sha256(existing) != _canonical_sha256(payload):
            raise FileExistsError(f"refusing to replace a different artifact: {output}")
        return True
    temporary = output.with_name(f".{output.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-day", required=True)
    parser.add_argument("--market-data-root", type=Path, required=True)
    parser.add_argument(
        "--profile",
        choices=tuple(sorted(PROFILES)),
        default=PROVIDER_NORMALIZED_PROFILE,
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe-output", type=Path)
    args = parser.parse_args(argv)

    built = build_orico_daily_source_spec(
        target_day=args.target_day,
        market_data_root=args.market_data_root,
        profile_id=args.profile,
    )
    spec_payload = source_spec_payload(built.bundle)
    probe_output = (
        args.probe_output
        if args.probe_output is not None
        else args.output.with_name(f"{args.output.stem}.probe.json")
    )
    probe_reused = _atomic_write_json(probe_output, built.probe)
    spec_reused = _atomic_write_json(args.output, spec_payload)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "profile_id": built.profile_id,
        "utc_day": built.bundle.utc_day,
        "market_data_root": str(built.market_data_root),
        "source_spec_path": str(args.output.expanduser().resolve()),
        "source_spec_sha256": daily.sha256_file(args.output.expanduser().resolve()),
        "source_spec_reused": spec_reused,
        "probe_path": str(probe_output.expanduser().resolve()),
        "probe_sha256": daily.sha256_file(probe_output.expanduser().resolve()),
        "probe_reused": probe_reused,
        "bundle_identity_sha256": built.bundle.identity_sha256(),
        "physical_materialization_eligible": True,
        "fallback_discovery_used": False,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
