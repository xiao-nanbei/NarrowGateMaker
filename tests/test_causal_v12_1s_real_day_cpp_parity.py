from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path

import pytest

cpp = pytest.importorskip("narrowgate_cpp")

from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_full_schema as full,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_real_day_cpp_parity as parity,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_schema as schema,
)

BASE_TS_MS = 1_780_000_000_000
LOCAL_REAL_PROBE = Path(tempfile.gettempdir()) / "f03_1s_real_source_probe_20260805_v3"
STALE_REAL_PROBES = (
    Path(tempfile.gettempdir()) / "f03_1s_real_source_probe_20260805_v2",
    Path(tempfile.gettempdir()) / "f03_1s_real_source_probe_20260805_v3",
)


def _require_current_local_probe_identity(probe: Path) -> None:
    manifest_path = probe / "manifest.json"
    if not manifest_path.is_file():
        pytest.skip("local two-row admitted real-source probe is unavailable")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bound_code_identity = manifest.get("cache_identity_payload", {}).get("code")
    current_code_identity = parity._current_python_code_identity()
    if bound_code_identity != current_code_identity:
        pytest.skip(
            "stale local diagnostic: probe code identity does not match current code identity"
        )


def _bars(count: int) -> tuple[base.OneSecondBar, ...]:
    rows = []
    for index in range(count):
        start = BASE_TS_MS + index * 1_000
        close = 60_000.0 + index * 0.2
        rows.append(
            base.OneSecondBar(
                start_ts_ms=start,
                finalized_ts_ms=start + 1_000,
                open=close - 0.05,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=1.0 + index * 0.001,
                buy_volume=0.55 + index * 0.0006,
                sell_volume=0.45 + index * 0.0004,
                trade_count=10 + index % 4,
                buy_count=6,
                sell_count=4,
                buy_quote_qty=30_000.0 + index,
                sell_quote_qty=29_500.0 + index,
                max_same_side_run=2 + index % 3,
                buy_price_high=close + 0.1,
                buy_price_low=close - 0.1,
                sell_price_high=close + 0.1,
                sell_price_low=close - 0.1,
            )
        )
    return tuple(rows)


def _row_fixture() -> tuple[
    dict[str, object], full.FullFeatureRow, dict[str, object], parity.CutoffSourceView
]:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    view = parity.CutoffSourceView(
        local_bars=bars[:400],
        execution_l2=(),
        metrics=(),
        reference_bars=(),
        local_bar_lag_state="observed_completed_trade_1s",
        local_synthetic_seconds_24h=0,
        reference_bar_lag_state="source_unavailable",
        reference_synthetic_seconds_1h=0,
    )
    py_row = full.generate_full_feature_row(
        view.local_bars,
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff,
    )
    cpp_row = dict(
        parity.compute_cpp_row(
            cpp,
            view,
            cutoff_exclusive_ms=cutoff,
            decision_ts_ms=cutoff,
        )
    )
    panel_row: dict[str, object] = {
        "cutoff_exclusive_ms": cutoff,
        "decision_ts_ms": cutoff,
        "feature_ready_ts_ms": py_row.feature_ready_ts_ms,
        "unsupported_feature_count": sum(value.value is None for value in py_row.values.values()),
        "feature_row_fingerprint_sha256": py_row.fingerprint_sha256,
        "local_bar_lag_state": view.local_bar_lag_state,
        "local_synthetic_seconds_24h": 0,
        "reference_bar_lag_state": view.reference_bar_lag_state,
        "reference_synthetic_seconds_1h": 0,
        **{name: py_row.values[name].value for name in schema.TRAINABLE_FEATURE_ORDER},
    }
    return panel_row, py_row, cpp_row, view


def test_field_parity_compares_all_six_cpp_outputs() -> None:
    panel_row, py_row, cpp_row, view = _row_fixture()
    stats = {name: parity.FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}

    parity._compare_one_row(
        panel_row,
        py_row,
        cpp_row,
        view,
        stats,
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    )

    assert sum(item.supported_rows + item.unsupported_rows for item in stats.values()) == 173
    assert all(item.supported_rows + item.unsupported_rows == 1 for item in stats.values())


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("valid", "validity mismatch"),
        ("source_latest_ts_ms", "source_latest_ts_ms mismatch"),
        ("feature_ready_ts_ms_by_feature", "feature_ready_ts_ms mismatch"),
        ("observation_count", "observation_count mismatch"),
        ("lag_state", "lag_state mismatch"),
    ],
)
def test_each_cpp_metadata_channel_fails_closed(field: str, message: str) -> None:
    panel_row, py_row, original, view = _row_fixture()
    cpp_row = copy.deepcopy(original)
    index = schema.TRAINABLE_FEATURE_ORDER.index("close")
    if field == "valid":
        cpp_row[field][index] = not cpp_row[field][index]
    elif field == "lag_state":
        cpp_row[field][index] = "tampered"
    else:
        cpp_row[field][index] += 1
    stats = {name: parity.FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}

    with pytest.raises(parity.RealDayParityError, match=message):
        parity._compare_one_row(
            panel_row,
            py_row,
            cpp_row,
            view,
            stats,
            rtol=parity.DEFAULT_RTOL,
            atol=parity.DEFAULT_ATOL,
        )


def test_cpp_value_channel_fails_closed() -> None:
    panel_row, py_row, original, view = _row_fixture()
    cpp_row = copy.deepcopy(original)
    index = schema.TRAINABLE_FEATURE_ORDER.index("close")
    cpp_row["values"][index] += 1.0
    stats = {name: parity.FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}

    with pytest.raises(parity.RealDayParityError, match="value mismatch"):
        parity._compare_one_row(
            panel_row,
            py_row,
            cpp_row,
            view,
            stats,
            rtol=parity.DEFAULT_RTOL,
            atol=parity.DEFAULT_ATOL,
        )


def test_signed_quote_reduction_uses_input_scale_bound_only_for_roundoff() -> None:
    panel_row, py_row, original, view = _row_fixture()
    index = schema.TRAINABLE_FEATURE_ORDER.index("taker_signed_quote_sum_60s")
    expected = float(py_row.values["taker_signed_quote_sum_60s"].value)
    bound = parity._signed_quote_reduction_error_bound(
        "taker_signed_quote_sum_60s",
        view,
    )
    assert bound > 0.0
    assert parity._feature_abs_tolerance(
        "taker_signed_quote_sum_60s",
        0.0,
        view,
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    ) == bound
    allowed = bound

    within_panel = copy.deepcopy(panel_row)
    within_panel["taker_signed_quote_sum_60s"] = expected + 0.5 * allowed
    within = copy.deepcopy(original)
    within["values"][index] = expected + 0.5 * allowed
    stats = {name: parity.FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}
    parity._compare_one_row(
        within_panel,
        py_row,
        within,
        view,
        stats,
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    )

    outside_panel = copy.deepcopy(panel_row)
    outside_panel["taker_signed_quote_sum_60s"] = expected + 2.0 * allowed
    outside = copy.deepcopy(original)
    outside["values"][index] = expected + 2.0 * allowed
    with pytest.raises(parity.RealDayParityError, match="value mismatch"):
        parity._compare_one_row(
            outside_panel,
            py_row,
            outside,
            view,
            stats,
            rtol=parity.DEFAULT_RTOL,
            atol=parity.DEFAULT_ATOL,
        )


def test_signed_quote_reduction_bound_does_not_apply_to_other_features() -> None:
    _, py_row, _, view = _row_fixture()
    expected = float(py_row.values["close"].value)
    assert parity._feature_abs_tolerance(
        "close",
        expected,
        view,
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    ) == pytest.approx(parity.DEFAULT_ATOL + parity.DEFAULT_RTOL * abs(expected))


def test_canonical_cutoff_stream_hash_matches_frozen_json_list() -> None:
    values = (1_000, 2_000, 5_000)
    hasher = parity._CanonicalIntListHasher()
    for value in values:
        hasher.update(value)
    assert hasher.hexdigest() == parity._canonical_sha256(list(values))


def test_python_oracle_sample_is_boundary_heavy_and_deterministic() -> None:
    indices = parity.python_oracle_sample_indices(parity.FULL_DAY_ROWS)
    assert len(indices) == parity.PYTHON_ORACLE_FULL_DAY_SAMPLE_ROWS == 1_262
    assert indices[: parity.PYTHON_ORACLE_EDGE_SECONDS] == tuple(
        range(parity.PYTHON_ORACLE_EDGE_SECONDS)
    )
    assert indices[-parity.PYTHON_ORACLE_EDGE_SECONDS :] == tuple(
        range(
            parity.FULL_DAY_ROWS - parity.PYTHON_ORACLE_EDGE_SECONDS,
            parity.FULL_DAY_ROWS,
        )
    )
    assert indices == tuple(sorted(set(indices)))


def test_full_day_panel_cpp_comparison_covers_all_173_fields() -> None:
    panel_row, _, cpp_row, view = _row_fixture()
    stats = {name: parity.FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}
    exact = parity._compare_panel_cpp_full_day_row(
        panel_row,
        cpp_row,
        {
            "local_bar_lag_state": view.local_bar_lag_state,
            "local_synthetic_seconds_24h": view.local_synthetic_seconds_24h,
            "reference_bar_lag_state": view.reference_bar_lag_state,
            "reference_synthetic_seconds_1h": view.reference_synthetic_seconds_1h,
        },
        stats,
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    )
    assert isinstance(exact, bool)
    assert all(item.supported_rows + item.unsupported_rows == 1 for item in stats.values())


def test_full_day_stream_requires_every_canonical_second() -> None:
    parity._validate_stream_cutoff(
        cutoff_mode="all_authoritative_target_day_decision_timestamps",
        day_start=1_000_000,
        row_index=7,
        cutoff=1_007_000,
    )
    with pytest.raises(parity.RealDayParityError, match="exact canonical target-day grid"):
        parity._validate_stream_cutoff(
            cutoff_mode="all_authoritative_target_day_decision_timestamps",
            day_start=1_000_000,
            row_index=7,
            cutoff=1_008_000,
        )


def test_physical_source_sha_mismatch_fails_before_feature_compute(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    path.write_bytes(b"not-the-bound-content")
    with pytest.raises(parity.RealDayParityError, match="SHA256 mismatch"):
        parity._verify_file_identity(
            path,
            {"size_bytes": path.stat().st_size, "sha256": "0" * 64},
            label="fixture source",
        )


def test_local_two_row_real_source_probe_when_code_identity_matches() -> None:
    _require_current_local_probe_identity(LOCAL_REAL_PROBE)
    report = parity.audit_real_day_cpp_parity(
        panel_manifest_path=LOCAL_REAL_PROBE / "manifest.json",
        source_bundle_identity_path=LOCAL_REAL_PROBE / "source_probe.json",
        batch_rows=1,
    )
    assert report["status"] == (
        "passed_complete_day_cpp_and_stratified_python_173_field_parity"
    )
    assert report["cutoffs"]["rows"] == 2
    assert report["cutoffs"]["streaming_output_rows_retained"] == 0
    assert report["parity"]["cpp_python_tolerance_parity_rows"] == 2
    assert report["parity"]["full_day_panel_cpp_tolerance_parity_rows"] == 2
    assert report["parity"]["validity_mismatches"] == 0
    assert report["permissions"] == {
        "labels_read": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "training_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }


@pytest.mark.parametrize("stale_probe", STALE_REAL_PROBES, ids=("v2", "v3"))
def test_old_two_row_probes_remain_stale_and_fail_closed(stale_probe: Path) -> None:
    if not (stale_probe / "manifest.json").is_file():
        pytest.skip(f"stale local two-row probe is unavailable: {stale_probe}")
    with pytest.raises(
        parity.RealDayParityError,
        match="unsupported admitted panel schema version|Python code identity differs",
    ):
        parity.audit_real_day_cpp_parity(
            panel_manifest_path=stale_probe / "manifest.json",
            source_bundle_identity_path=stale_probe / "source_probe.json",
            batch_rows=1,
        )
