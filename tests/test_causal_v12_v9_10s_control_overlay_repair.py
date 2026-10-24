from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_overlay_repair as repair,
)

DAY = "2026-04-17"


def _write_feature_pair(
    tmp_path: Path,
    *,
    include_first: bool = True,
    include_last: bool = True,
) -> tuple[Path, Path]:
    labels = repair.canonical_feature_labels(DAY)
    prior_labels = labels[:1] if include_first else np.empty(0, dtype=np.int64)
    target_labels = labels[1:] if include_last else labels[1:-1]
    prior = pd.DataFrame(
        {"feature": np.arange(len(prior_labels), dtype=np.float64)},
        index=pd.to_datetime(prior_labels, unit="ms", utc=True),
    )
    target = pd.DataFrame(
        {"feature": np.arange(len(target_labels), dtype=np.float64)},
        index=pd.to_datetime(target_labels, unit="ms", utc=True),
    )
    prior_path = tmp_path / "prior.parquet"
    target_path = tmp_path / "target.parquet"
    prior.to_parquet(prior_path)
    target.to_parquet(target_path)
    return prior_path, target_path


def _ml_data(*, ready: np.ndarray | None = None) -> tuple[object, ...]:
    timestamps = repair.canonical_visibility_grid(DAY) if ready is None else ready
    values: list[object] = [timestamps]
    values.extend(
        np.full(len(timestamps), 0.5, dtype=np.float64) for _ in range(repair.MAIN_ARRAY_COUNT - 1)
    )
    values.append({"feature": np.ones(len(timestamps), dtype=np.float64)})
    return tuple(values)


def test_legal_midnight_row_comes_from_d_minus_one_final_bucket(tmp_path: Path) -> None:
    prior, target = _write_feature_pair(tmp_path)
    selected = repair._select_feature_rows(prior, target, day=DAY)
    expected_first_label = repair.canonical_visibility_grid(DAY)[0] - repair.CADENCE_MS
    assert int(selected.index[0].timestamp() * 1000) == expected_first_label
    assert len(selected) == repair.ROWS_PER_DAY


@pytest.mark.parametrize("missing", ["first", "last"])
def test_missing_boundary_feature_bucket_fails_closed(tmp_path: Path, missing: str) -> None:
    prior, target = _write_feature_pair(
        tmp_path,
        include_first=missing != "first",
        include_last=missing != "last",
    )
    with pytest.raises(repair.ControlOverlayRepairError, match="first/last causal"):
        repair._select_feature_rows(prior, target, day=DAY)


def test_future_visibility_row_is_rejected() -> None:
    ready = np.append(
        repair.canonical_visibility_grid(DAY),
        repair.canonical_visibility_grid(DAY)[-1] + repair.CADENCE_MS,
    )
    with pytest.raises(repair.ControlOverlayRepairError, match="noncanonical visibility"):
        repair._validate_ml_data(_ml_data(ready=ready), day=DAY)


def test_noncanonical_prior_visibility_row_is_rejected() -> None:
    canonical = repair.canonical_visibility_grid(DAY)
    ready = np.concatenate(([canonical[0] - repair.CADENCE_MS], canonical[1:]))
    with pytest.raises(repair.ControlOverlayRepairError, match="noncanonical visibility"):
        repair._validate_ml_data(_ml_data(ready=ready), day=DAY)


def test_model_context_window_drift_is_rejected(tmp_path: Path) -> None:
    window = tmp_path / "window.pkl"
    window.write_bytes(b"window")
    receipt = {"path": str(window), "sha256": repair._sha256_file(window)}
    row = {
        "window_cache": receipt,
        "model_overlay": {
            "identity": {"market_context_identity_sha256": "a" * 64},
            "market_context_output_parity": {
                "exact_trades_and_rolling_arrays": True,
                "window_sha256": "b" * 64,
                "market_context_identity_sha256": "a" * 64,
            },
        },
    }
    with pytest.raises(repair.ControlOverlayRepairError, match="context drift"):
        repair._window_overlay_parity(row, day=DAY)


def test_component_publication_is_atomic_and_resume_safe(tmp_path: Path) -> None:
    identity = {
        "schema_version": repair.SCHEMA_VERSION,
        "identity": repair.IDENTITY,
        "utc_day": DAY,
        "test": True,
    }
    first = repair._publish_component(
        tmp_path,
        identity=identity,
        mode="regenerate_full_day",
        ml_data=_ml_data(),
    )
    second = repair._publish_component(
        tmp_path,
        identity=identity,
        mode="regenerate_full_day",
        ml_data=_ml_data(),
    )
    assert first == second
    assert (Path(first["directory"]) / repair.COMPONENT_SUCCESS).is_file()
    assert not list(Path(first["directory"]).parent.glob("*.partial"))


def test_feature_semantics_drift_is_rejected() -> None:
    base = {
        "generator_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "feature_semantics_version": 6,
        "feature_dag_id": "live_10s_signal_cutoff.v1",
        "feature_dag_sha256": "c" * 64,
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_ready_offset_ms": 10_000,
        "market_stage": "minimal",
    }
    drift = dict(base)
    drift["feature_dag_sha256"] = "d" * 64
    with pytest.raises(repair.ControlOverlayRepairError, match="semantics differ"):
        repair._validate_feature_semantics(base, drift)


def test_explicit_panel_sha_is_required_before_schedule_load(tmp_path: Path) -> None:
    panel = tmp_path / "panel-manifest.json"
    panel.write_text(json.dumps({"identity": repair.IDENTITY}), encoding="utf-8")
    with pytest.raises(repair.ControlOverlayRepairError, match="explicit control panel SHA256"):
        repair.load_admitted_control_schedule(
            panel,
            panel_sha256="0" * 64,
            panel_identity_sha256="1" * 64,
            day=DAY,
        )
