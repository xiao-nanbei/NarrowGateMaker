#!/usr/bin/env python3
"""Validate source-aware dataset bindings for NarrowGate experiments."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "narrowgate_experiment_dataset_binding.v1"
CANONICAL_FULL_PATH_BASELINE = (
    "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_50d_spec_20260810.json"
)
FULL_PATH_CLASSES = {
    "daily_fresh_start_full_path_action",
    "strict_native_queue_action",
}
EXPERIMENT_CLASSES = {
    "prediction",
    "daily_fresh_start_full_path_action",
    "strict_native_queue_action",
    "diagnostic",
}
TRAINING_WINDOW_MODES = {
    "expanding_all_eligible_pre_cutoff",
    "nested_chronological_selected",
    "auxiliary_source_then_current_source_calibration",
    "not_applicable",
}
SOURCE_POOLING_MODES = {
    "single_authority",
    "source_stratified",
    "auxiliary_then_current_source_calibration",
}
PANEL_ORDER = (
    "development",
    "embargo_1",
    "validation",
    "embargo_2",
    "sealed_holdout",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_path(value: object, *, root: Path) -> Path:
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else (root / path).resolve()


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return value


def _days(value: object, *, name: str, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    days = [str(day) for day in value]
    if not allow_empty and not days:
        raise ValueError(f"{name} must not be empty")
    if days != sorted(days):
        raise ValueError(f"{name} must be chronological")
    if len(days) != len(set(days)):
        raise ValueError(f"{name} contains duplicate days")
    for day in days:
        if len(day) != 10 or day[4] != "-" or day[7] != "-":
            raise ValueError(f"{name} contains a non-ISO UTC day: {day}")
    return days


def canonical_full_path_days(*, root: Path | None = None) -> tuple[str, list[str]]:
    project_root = (root or _project_root()).resolve()
    spec_path = project_root / CANONICAL_FULL_PATH_BASELINE
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    prefix = _days(
        payload["immutable_prefix"]["ordered_utc_days"],
        name="canonical immutable prefix",
    )
    added = _days(
        payload["added_panel"]["ordered_utc_days"],
        name="canonical added panel",
    )
    combined = sorted(prefix + added)
    if len(combined) != 50 or len(set(combined)) != 50:
        raise ValueError("canonical full-path baseline is not a unique 50-day panel")
    return str(payload["identity"]), combined


def _validate_universe(payload: Mapping[str, Any], *, root: Path) -> list[str]:
    universe = _mapping(payload.get("universe_manifest"), name="universe_manifest")
    path = _resolve_path(universe.get("path", ""), root=root)
    expected_hash = str(universe.get("sha256", ""))
    if not path.is_file():
        raise ValueError(f"universe manifest is missing: {path}")
    if not expected_hash or sha256_file(path) != expected_hash:
        raise ValueError("universe manifest hash does not match")

    requirements = payload.get("required_capabilities")
    if not isinstance(requirements, list) or not requirements:
        raise ValueError("required_capabilities must be a non-empty list")
    if any(not str(item).strip() for item in requirements):
        raise ValueError("required_capabilities contains an empty capability")
    return _days(payload.get("eligible_days"), name="eligible_days")


def _validate_panels(payload: Mapping[str, Any], *, eligible_days: list[str]) -> list[str]:
    evidence = _mapping(payload.get("evidence"), name="evidence")
    panels = _mapping(evidence.get("panels"), name="evidence.panels")
    eligible = set(eligible_days)
    seen: set[str] = set()
    development: list[str] = []
    previous_last = ""

    for panel_name in PANEL_ORDER:
        panel = _mapping(panels.get(panel_name), name=f"panel {panel_name}")
        panel_days = _days(
            panel.get("days"),
            name=f"panel {panel_name} days",
            allow_empty=panel_name != "development",
        )
        if panel_name == "development":
            development = panel_days
        outside = sorted(set(panel_days) - eligible)
        if outside:
            raise ValueError(f"panel {panel_name} contains ineligible days: {outside}")
        overlap = sorted(set(panel_days) & seen)
        if overlap:
            raise ValueError(f"evidence panels overlap on {overlap}")
        if panel_days and previous_last and panel_days[0] <= previous_last:
            raise ValueError(f"panel {panel_name} is not after the preceding panel")
        if panel_days:
            previous_last = panel_days[-1]
        seen.update(panel_days)

        status = str(panel.get("status", ""))
        if panel_days and status not in {"open", "locked", "sealed", "consumed"}:
            raise ValueError(f"panel {panel_name} has an invalid non-empty status")
        if not panel_days and status not in {"not_allocated", "blocked"}:
            raise ValueError(f"empty panel {panel_name} must be not_allocated or blocked")
        if panel_name == "sealed_holdout" and panel_days and status != "sealed":
            raise ValueError("a non-empty sealed_holdout must have status=sealed")
    return development


def _validate_training_and_oof(
    payload: Mapping[str, Any], *, development_days: list[str]
) -> None:
    training = _mapping(payload.get("training_window"), name="training_window")
    mode = str(training.get("mode", ""))
    if mode not in TRAINING_WINDOW_MODES:
        raise ValueError(f"unsupported training-window mode: {mode}")
    cutoff_basis = str(training.get("cutoff_basis", ""))
    if cutoff_basis not in {
        "evidence_panel_boundary",
        "source_or_semantics_epoch_boundary",
        "nested_chronological_selection",
        "not_applicable",
    }:
        raise ValueError("training_window.cutoff_basis is not evidence-based")

    authorities = training.get("source_authorities")
    if not isinstance(authorities, list) or not authorities:
        raise ValueError("training_window.source_authorities must be non-empty")
    pooling = str(training.get("source_pooling", ""))
    if pooling not in SOURCE_POOLING_MODES:
        raise ValueError(f"unsupported source-pooling mode: {pooling}")
    if len(set(map(str, authorities))) > 1 and pooling == "single_authority":
        raise ValueError("multiple source authorities cannot be pooled as one authority")

    oof = _mapping(payload.get("oof"), name="oof")
    enabled = bool(oof.get("enabled", False))
    folds = oof.get("folds")
    if not enabled:
        if folds not in ([], None):
            raise ValueError("disabled OOF must not contain folds")
        return
    if str(oof.get("scope", "")) != "development_only":
        raise ValueError("OOF scope must be development_only")
    if not isinstance(folds, list) or not folds:
        raise ValueError("enabled OOF requires at least one fold")

    development = set(development_days)
    seen_test: set[str] = set()
    for index, fold in enumerate(folds):
        fold_obj = _mapping(fold, name=f"OOF fold {index}")
        train = _days(fold_obj.get("train_days"), name=f"OOF fold {index} train")
        test = _days(fold_obj.get("test_days"), name=f"OOF fold {index} test")
        if set(train) - development or set(test) - development:
            raise ValueError("OOF folds may use Development days only")
        if set(train) & set(test):
            raise ValueError(f"OOF fold {index} train/test overlap")
        if train[-1] >= test[0]:
            raise ValueError(f"OOF fold {index} is not chronological")
        if set(test) & seen_test:
            raise ValueError("OOF test days repeat across folds")
        seen_test.update(test)
    if int(oof.get("test_day_count", -1)) != len(seen_test):
        raise ValueError("oof.test_day_count does not match the emitted fold dates")


def _validate_full_path_denominator(payload: Mapping[str, Any], *, root: Path) -> None:
    experiment_class = str(payload.get("experiment_class", ""))
    if experiment_class not in FULL_PATH_CLASSES:
        return

    execution = _mapping(payload.get("execution_denominator"), name="execution_denominator")
    identity, canonical_days = canonical_full_path_days(root=root)
    actual_days = _days(execution.get("days"), name="execution denominator days")
    reduced = bool(execution.get("reduced_support", False))

    if not reduced:
        if str(execution.get("identity", "")) != identity:
            raise ValueError("full-path execution denominator identity is not canonical")
        if actual_days != canonical_days:
            raise ValueError("new full-path studies must use the canonical 50-day panel")
        if bool(execution.get("claims_current_50_day_baseline", False)) is not True:
            raise ValueError("the complete canonical panel must identify itself as 50-day")
    else:
        outside = sorted(set(actual_days) - set(canonical_days))
        if outside:
            raise ValueError(f"reduced-support denominator contains non-canonical days: {outside}")
        missing = sorted(set(canonical_days) - set(actual_days))
        exclusions = execution.get("excluded_days")
        if not isinstance(exclusions, list):
            raise ValueError("reduced-support denominator requires excluded_days")
        excluded_map = {
            str(row.get("day")): list(row.get("reasons") or ())
            for row in exclusions
            if isinstance(row, Mapping)
        }
        if sorted(excluded_map) != missing:
            raise ValueError("reduced-support exclusions do not match missing canonical days")
        if any(not excluded_map[day] for day in missing):
            raise ValueError("every reduced-support exclusion needs a capability reason")
        if bool(execution.get("claims_current_50_day_baseline", False)):
            raise ValueError("a reduced-support identity cannot claim the 50-day baseline")

    if not bool(execution.get("report_prefix40_added10_pooled50", False)):
        raise ValueError("full-path reports must retain prefix40/added10/pooled50 views")


def validate_dataset_binding(
    payload: Mapping[str, Any], *, project_root: Path | None = None
) -> dict[str, Any]:
    root = (project_root or _project_root()).resolve()
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported dataset binding schema: {payload.get('schema_version')}")
    if not str(payload.get("experiment_id", "")).strip():
        raise ValueError("experiment_id is required")
    experiment_class = str(payload.get("experiment_class", ""))
    if experiment_class not in EXPERIMENT_CLASSES:
        raise ValueError(f"unsupported experiment class: {experiment_class}")

    eligible_days = _validate_universe(payload, root=root)
    development_days = _validate_panels(payload, eligible_days=eligible_days)
    _validate_training_and_oof(payload, development_days=development_days)
    _validate_full_path_denominator(payload, root=root)
    return {
        "experiment_id": str(payload["experiment_id"]),
        "experiment_class": experiment_class,
        "eligible_day_count": len(eligible_days),
        "development_day_count": len(development_days),
        "oof_test_day_count": int(payload["oof"].get("test_day_count", 0)),
        "valid": True,
    }


def _cmd_validate(args: argparse.Namespace) -> None:
    path = Path(args.path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    result = validate_dataset_binding(payload)
    print(json.dumps(result, indent=2, sort_keys=True))


def _cmd_show_baseline(_: argparse.Namespace) -> None:
    identity, days = canonical_full_path_days()
    print(json.dumps({"identity": identity, "days": days}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    validate_parser.set_defaults(func=_cmd_validate)
    baseline_parser = subparsers.add_parser("show-canonical-full-path-baseline")
    baseline_parser.set_defaults(func=_cmd_show_baseline)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
