import hashlib
import json
from argparse import Namespace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.system_engineering.audit.rest_latency_calibration import (
    calibrate,
    load_runtime_timing_samples,
    runtime_compute_overrides,
)


@pytest.fixture
def runtime_samples():
    return {
        "schema": "narrowgate_private_empirical_timing_samples.v1",
        "gateway": {
            "columns": [
                "outcome", "execution_status", "dispatch_to_http_response_ms",
                "dispatch_to_private_ack_ms", "private_exchange_event_minus_dispatch_ms",
                "decision_to_dispatch_ms",
            ],
            "operations": {
                "order.place": [
                    ["successes", "authoritative_success", 9.0, 4.0, 2.0, None],
                    ["successes", "authoritative_success", 5.0, 8.0, 1.0, 500.0],
                    ["successes", "authoritative_success", 1000.0, 900.0, 3.0, 800.0],
                ],
                "order.cancel": [["successes", "authoritative_success", 7.0, 3.0, 1.0, 9.0]],
            },
            "coverage": {
                "order.place": {"unique_completed_attempts": 3, "matched_private_ack": 3},
                "order.cancel": {"unique_completed_attempts": 1, "matched_private_ack": 1},
            },
        },
        "compute": {
            "columns": ["offset_ms", "signal_path", "signal_compute_ms"],
            "rows": [[0.0, "cached", 0.01], [100.0, "new_bucket", 1.0]],
        },
        "snapshots": {"population": "decision-snapshot-weighted"},
        "bulk_cancel_matched_risk_case": {
            "source": "synthetic matched non-shutdown risk case",
            "target_count": 1, "sample_count": 1,
            "exchange_event_proxy_ms": 4.25,
            "private_visibility_ms": 7.75,
            "http_return_ms": 6.5,
        },
        "bulk_cancel_http_observations": {
            "samples_ms": [6.5, 0.1],
            "contexts": ["risk_case", "unmatched_shutdown"],
        },
    }


def test_runtime_compute_adapter_preserves_rows_and_does_not_charge_fifo_wait():
    compute = {
        "columns": ["sync_check_ms", "signal_compute_ms", "compute_quotes_ms", "requote_total_ms"],
        "by_signal_path": {
            "cached_no_new_bucket": [[1.0, 2.0, 4.0, 20.0]],
            "new_bucket": [[1.0, 10.0, 8.0, 100.0]],
            "catch_up": [[1.0, 50.0, 10.0, 120.0]],
        },
    }
    params = runtime_compute_overrides(
        {"compute": compute}, initial_bucket_end_ms=10_000, clock="source_time_assumption",
    )
    samples = params["_runtime_compute_samples_by_path"]
    np.testing.assert_array_equal(samples["cached_no_new_bucket"], [[3.0, 7.0, 13.0]])
    np.testing.assert_array_equal(samples["new_bucket"], [[11.0, 19.0, 81.0]])
    assert params["runtime_compute_initial_bucket_end_ms"] == 10_000
    assert "not exact admission gaps" in params["_runtime_compute_sample_semantics"]
    compute["by_signal_path"]["catch_up"][0][-1] = 1.0
    with pytest.raises(ValueError, match="exceed total"):
        runtime_compute_overrides(
            {"compute": compute}, initial_bucket_end_ms=10_000, clock="source_time_assumption",
        )


@pytest.mark.parametrize("bad", [None, -1.0, float("nan"), float("inf"), True])
def test_runtime_compute_adapter_never_replaces_missing_or_bad_stages_with_zero(bad):
    compute = {
        "columns": ["sync_check_ms", "signal_compute_ms", "compute_quotes_ms", "requote_total_ms"],
        "by_signal_path": {
            path: [[0.0, bad, 1.0, 10.0]]
            for path in ("cached_no_new_bucket", "new_bucket", "catch_up")
        },
    }
    with pytest.raises(ValueError, match="observed finite nonnegative"):
        runtime_compute_overrides(
            {"compute": compute}, initial_bucket_end_ms=0, clock="source_time_assumption",
        )


@pytest.mark.parametrize(("assumption", "effective"), [
    ("dispatch", [0.0, 0.0, 0.0]),
    ("exchange_event_proxy", [2.0, 1.0, 3.0]),
    ("observable_upper_bound", [4.0, 5.0, 900.0]),
])
def test_runtime_timing_samples_preserve_pairs_tails_and_compute_metadata(
    tmp_path, runtime_samples, monkeypatch, assumption, effective,
):
    source = tmp_path / "samples.json"
    raw = json.dumps(runtime_samples).encode()
    source.write_bytes(raw)
    reads = []
    read_bytes = Path.read_bytes

    def read_once(path):
        reads.append(path)
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", read_once)
    result = load_runtime_timing_samples(source, effective_time_assumption=assumption)
    params, calibration = result["params"], result["calibration"]
    assert reads == [source]
    assert calibration["source"]["sha256"] == hashlib.sha256(raw).hexdigest()
    assert params["rest_gateway_timing_mode"] == "sampled_async_fifo"
    assert params["replay_event_clock"] == "merged"
    assert params["replay_main_loop_sleep_ms"] == 100
    assert params["cross_side_order_lanes_enabled"] is False
    assert params["latency_baseline_clip_quantile"] == 1.0
    assert params["latency_jitter_ms"] == 0.0
    new = params["_serial_rest_return_samples_by_operation"]["new"]
    np.testing.assert_array_equal(new[:, 0], effective)
    np.testing.assert_array_equal(new[:, 1:], [[4, 9], [8, 5], [900, 1000]])
    assert new.flags.c_contiguous
    assert assumption in params["_serial_rest_return_sample_semantics"]
    assert calibration["compute"]["by_signal_path"]["new_bucket"] == [[100, "new_bucket", 1]]
    assert calibration["compute"]["consumed_by_replay"] is False
    assert not any(key.startswith("_decision_to_gateway") for key in params)
    assert not any("visibility" in key for key in params)
    assert not any(key.startswith("_pre_snapshot_compute") for key in params)
    assert "_bulk_cancel_timing_samples_ms" not in params
    assert calibration["bulk_cancel_model"]["consumed_by_replay"] is False


@pytest.mark.parametrize(("assumption", "effective"), [
    ("dispatch", 0.0), ("exchange_event_proxy", 4.25), ("observable_upper_bound", 6.5),
])
def test_runtime_bulk_model_requires_opt_in_and_preserves_single_matched_case(
    tmp_path, runtime_samples, assumption, effective,
):
    path = tmp_path / "samples.json"
    path.write_text(json.dumps(runtime_samples))
    result = load_runtime_timing_samples(
        path, effective_time_assumption=assumption, bulk_cancel_model="matched_risk_case"
    )
    np.testing.assert_array_equal(
        result["params"]["_bulk_cancel_timing_samples_ms"], [[effective, 7.75, 6.5]]
    )
    metadata = result["calibration"]["bulk_cancel_model"]
    assert metadata["observed_sample_count"] == metadata["observed_target_count"] == 1
    assert metadata["consumed_by_replay"] is True
    assert metadata["shared_phases_for_all_targets"] == "modeling_assumption"
    semantics = result["params"]["_bulk_cancel_timing_sample_semantics"]
    assert "n=1" in semantics and "not a stable distribution" in semantics
    assert "all targets" in semantics and "shutdown HTTP samples are not used" in semantics
    assert semantics in result["calibration"]["limitations"]


@pytest.mark.parametrize(("field", "value"), [
    ("source", ""), ("target_count", 2), ("target_count", True),
    ("sample_count", 2), ("sample_count", 1.0), ("private_visibility_ms", None),
    ("http_return_ms", -1.0), ("http_return_ms", float("nan")),
    ("exchange_event_proxy_ms", 6.501),
])
def test_runtime_bulk_model_rejects_incomplete_or_unmatched_case(
    tmp_path, runtime_samples, field, value,
):
    runtime_samples["bulk_cancel_matched_risk_case"][field] = value
    path = tmp_path / "samples.json"
    path.write_text(json.dumps(runtime_samples))
    with pytest.raises(ValueError, match="risk case|bulk cancel"):
        load_runtime_timing_samples(
            path, effective_time_assumption="exchange_event_proxy",
            bulk_cancel_model="matched_risk_case",
        )


def test_runtime_bulk_model_never_substitutes_unmatched_shutdown_http(tmp_path, runtime_samples):
    del runtime_samples["bulk_cancel_matched_risk_case"]
    path = tmp_path / "samples.json"
    path.write_text(json.dumps(runtime_samples))
    with pytest.raises(ValueError, match="single-target risk case"):
        load_runtime_timing_samples(
            path, effective_time_assumption="dispatch", bulk_cancel_model="matched_risk_case"
        )
    result = load_runtime_timing_samples(path, effective_time_assumption="dispatch")
    assert "_bulk_cancel_timing_samples_ms" not in result["params"]
    with pytest.raises(ValueError, match="bulk_cancel_model"):
        load_runtime_timing_samples(
            path, effective_time_assumption="dispatch", bulk_cancel_model="shutdown_http"
        )


def test_http_authority_uses_complete_result_statuses_not_success_flag(tmp_path, runtime_samples):
    source = tmp_path / "samples.json"
    source.write_text(json.dumps(runtime_samples))
    result = load_runtime_timing_samples(source, effective_time_assumption="exchange_event_proxy")
    assert result["params"]["_serial_rest_http_result_status_by_operation"] == {}
    runtime_samples["http_result_status_counts"] = {
        "source": "same-request validated synthetic RESULT",
        "order.place": {"NEW": 3}, "order.cancel": {"CANCELED": 1},
    }
    source.write_text(json.dumps(runtime_samples))
    result = load_runtime_timing_samples(source, effective_time_assumption="exchange_event_proxy")
    assert result["params"]["_serial_rest_http_result_status_by_operation"] == {
        "new": "NEW", "cancel": "CANCELED",
    }
    # Observed private timestamps remain unmodified; the runtime computes the
    # first local authoritative state separately from this reporting clock.
    np.testing.assert_array_equal(
        result["params"]["_serial_rest_return_samples_by_operation"]["new"][:, 1],
        [4.0, 8.0, 900.0],
    )
    runtime_samples["http_result_status_counts"]["order.place"] = {"FILLED": 3}
    source.write_text(json.dumps(runtime_samples))
    with pytest.raises(ValueError, match="must cover every paired request"):
        load_runtime_timing_samples(source, effective_time_assumption="exchange_event_proxy")


@pytest.mark.parametrize("mutation", [
    "schema", "duplicate_column", "missing_column", "short_row", "empty_operation",
    "missing_private", "negative_http", "nan_http", "infinite_private", "boolean_http",
    "unknown", "reject", "unmatched", "excluded", "coverage_unknown", "coverage_mismatch",
])
def test_runtime_timing_samples_reject_invalid_or_unmodeled_rows(
    tmp_path, runtime_samples, mutation,
):
    gateway = runtime_samples["gateway"]
    row = gateway["operations"]["order.place"][0]
    if mutation == "schema":
        runtime_samples["schema"] = "unknown"
    elif mutation == "duplicate_column":
        gateway["columns"][0] = gateway["columns"][1]
    elif mutation == "missing_column":
        gateway["columns"][2] = "not_http"
    elif mutation == "short_row":
        row.pop()
    elif mutation == "empty_operation":
        gateway["operations"]["order.cancel"] = []
    elif mutation in {"negative_http", "nan_http", "boolean_http"}:
        row[2] = {"negative_http": -1, "nan_http": float("nan"), "boolean_http": True}[mutation]
    elif mutation in {"missing_private", "infinite_private"}:
        row[3] = None if mutation == "missing_private" else float("inf")
    elif mutation == "unknown":
        row[1] = "unknown"
    elif mutation == "reject":
        row[0] = "rejects"
    else:
        field, value = {
            "unmatched": ("unmatched_private_ack", 1),
            "excluded": ("duplicate_completion_rows_excluded", 1),
            "coverage_unknown": ("execution_status:unknown", 1),
            "coverage_mismatch": ("unique_completed_attempts", 4),
        }[mutation]
        gateway["coverage"]["order.place"][field] = value
    source = tmp_path / "samples.json"
    source.write_text(json.dumps(runtime_samples))
    with pytest.raises(ValueError):
        load_runtime_timing_samples(source, effective_time_assumption="dispatch")


@pytest.mark.parametrize("proxy", [None, -0.001, 4.001, float("nan")])
def test_runtime_timing_proxy_never_clamps_or_discards(tmp_path, runtime_samples, proxy):
    runtime_samples["gateway"]["operations"]["order.place"][0][4] = proxy
    source = tmp_path / "samples.json"
    source.write_text(json.dumps(runtime_samples))
    with pytest.raises(ValueError, match="exchange_event_proxy|exceeds observed"):
        load_runtime_timing_samples(source, effective_time_assumption="exchange_event_proxy")
    # The explicitly selected dispatch bound does not claim the proxy was observed.
    loaded = load_runtime_timing_samples(source, effective_time_assumption="dispatch")
    assert loaded["calibration"]["sample_counts"]["new"] == 3


def test_runtime_timing_effective_assumption_is_mandatory(tmp_path):
    source = tmp_path / "absent.json"
    with pytest.raises(TypeError, match="effective_time_assumption"):
        load_runtime_timing_samples(source)
    with pytest.raises(ValueError, match="effective_time_assumption"):
        load_runtime_timing_samples(source, effective_time_assumption="rtt_half")


def test_rest_latency_profile_freezes_complete_days(tmp_path):
    source = tmp_path / "telemetry.csv"
    pd.DataFrame({
        "timestamp": [
            pd.Timestamp("2026-07-10T01:00:00Z").timestamp(),
            pd.Timestamp("2026-07-11T01:00:00Z").timestamp(),
            pd.Timestamp("2026-07-12T01:00:00Z").timestamp(),
        ],
        "rest_new_count": [2, 1, 1],
        "rest_new_sum_us": [20_000, 30_000, 90_000],
        "rest_new_max_us": [12_000, 30_000, 90_000],
        "rest_cancel_count": [1, 2, 1],
        "rest_cancel_sum_us": [15_000, 40_000, 80_000],
        "rest_cancel_max_us": [15_000, 25_000, 80_000],
    }).to_csv(source, index=False)
    args = Namespace(
        telemetry=source,
        replay_telemetry=tmp_path / "replay.csv.gz",
        output=tmp_path / "profile.json",
        start_day="2026-07-10",
        end_day="2026-07-11",
        recent_hours=3.0,
        profile_id="test",
        region="test",
        instance="test",
        os_label="test",
        cpu_label="test",
        memory_label="test",
        config_sha256="abc",
    )
    report = calibrate(args)
    assert report["fit_interval"]["rows"] == 2
    assert report["fit_distributions"]["avg"]["new"]["count"] == 2
    assert report["fit_distributions"]["avg"]["new"]["p50_ms"] == 20.0
    assert pd.read_csv(args.replay_telemetry).shape[0] == 2
