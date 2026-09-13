from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_ml_ab_replay as replay,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as overlays,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema
from research.families.f03_causal_13_head.audit import causal_v12_1s_training as training


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _write_test_overlay(tmp_path: Path, *, rows: int = 4) -> Path:
    output = tmp_path / "overlay"
    output.mkdir(parents=True)
    day_start = 1_753_056_000_000  # 2025-07-21T00:00:00Z
    arrays: list[pa.Array] = [
        pa.array(day_start + np.arange(rows, dtype=np.int64) * 1_000),
        pa.array(day_start + np.arange(rows, dtype=np.int64) * 1_000),
        pa.array(day_start + np.arange(rows, dtype=np.int64) * 1_000 - 1),
        pa.array([f"{index:064x}" for index in range(rows)]),
    ]
    for head in training.HEAD_SPECS:
        value = 0.5 if training.HEAD_SPECS[head][3] else (0.1 if head.startswith("vol_") else 0.0)
        arrays.append(pa.array(np.full(rows, value, dtype=np.float64)))
    table = pa.Table.from_arrays(arrays, schema=overlays.prediction_overlay_arrow_schema())
    overlay_path = output / overlays.OVERLAY_FILENAME
    pq.write_table(table, overlay_path, compression="zstd")
    bundle_heads = [
        {
            "head": head,
            "model_sha256": hashlib.sha256(f"model:{head}".encode()).hexdigest(),
            "metadata_sha256": hashlib.sha256(f"meta:{head}".encode()).hexdigest(),
        }
        for head in training.HEAD_SPECS
    ]
    identity_payload = {
        "research_bundle": {
            "bundle_sha256": "a" * 64,
            "heads": bundle_heads,
        }
    }
    manifest = {
        "schema_version": overlays.ARTIFACT_SCHEMA_VERSION,
        "identity": schema.IDENTITY,
        "utc_day": "2025-07-21",
        "cache_identity_payload": identity_payload,
        "cache_identity_sha256": _canonical_sha256(identity_payload),
        "feature_bucket_ms": 1_000,
        "overlay_schema": overlays.prediction_overlay_schema_payload(),
        "overlay": {
            "path": overlays.OVERLAY_FILENAME,
            "sha256": _sha256_file(overlay_path),
            "rows": rows,
            "compression": "zstd",
        },
        "head_count": 13,
        "test_only": True,
        "atomic_admission": True,
        "labels_read": False,
        "economic_outcomes_read": False,
        "training_performed": False,
        "prediction_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    manifest_path = output / overlays.MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (output / overlays.SUCCESS_FILENAME).write_text(
        _sha256_file(manifest_path) + "\n", encoding="ascii"
    )
    return output


def _binding(tmp_path: Path) -> dict:
    identity_path = tmp_path / "identity.json"
    config_path = tmp_path / "config.yaml"
    config_path.write_text("ml: true\n", encoding="utf-8")
    identity_path.write_text("{}", encoding="utf-8")
    identity = {
        "schema_version": replay.EXPECTED_BASELINE_SCHEMA,
        "baseline_id": replay.EXPECTED_BASELINE_ID,
        "config": {
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
    }
    return {
        "pointer": {
            "baseline_id": replay.EXPECTED_BASELINE_ID,
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "live_config_sha256": _sha256_file(config_path),
        },
        "identity": identity,
        "identity_path": identity_path,
        "identity_sha256": _sha256_file(identity_path),
        "config_path": config_path,
    }


def test_loads_complete_13_head_overlay_and_projects_existing_quote_abi(tmp_path: Path) -> None:
    schedule = replay.load_admitted_one_second_overlay(
        _write_test_overlay(tmp_path), allow_test_only=True
    )
    assert tuple(schedule.predictions) == tuple(training.HEAD_SPECS)
    assert len(schedule.ml_data) == 6
    assert schedule.ml_data[0] is schedule.decision_ts_ms
    assert schedule.ml_data[1] is schedule.predictions["dir_10s"]
    assert schedule.ml_data[4] is schedule.predictions["tox_bid_10s"]
    assert np.all(schedule.feature_ready_ts_ms <= schedule.decision_ts_ms)


def test_formal_loader_rejects_test_only_overlay(tmp_path: Path) -> None:
    with pytest.raises(replay.OneSecondReplayABIError, match="test-only"):
        replay.load_admitted_one_second_overlay(_write_test_overlay(tmp_path))


def test_rejects_future_ready_timestamp_and_missing_head_hash(tmp_path: Path) -> None:
    output = _write_test_overlay(tmp_path)
    table = pq.read_table(output / overlays.OVERLAY_FILENAME)
    columns = [table.column(index) for index in range(table.num_columns)]
    columns[2] = pa.chunked_array([np.asarray(table["decision_ts_ms"]) + 1], type=pa.int64())
    pq.write_table(
        pa.Table.from_arrays(columns, schema=table.schema),
        output / overlays.OVERLAY_FILENAME,
        compression="zstd",
    )
    manifest_path = output / overlays.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["overlay"]["sha256"] = _sha256_file(output / overlays.OVERLAY_FILENAME)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (output / overlays.SUCCESS_FILENAME).write_text(
        _sha256_file(manifest_path) + "\n", encoding="ascii"
    )
    with pytest.raises(replay.OneSecondReplayABIError, match="feature_ready"):
        replay.load_admitted_one_second_overlay(output, allow_test_only=True)

    manifest["cache_identity_payload"]["research_bundle"]["heads"][0].pop("model_sha256")
    manifest["cache_identity_sha256"] = _canonical_sha256(manifest["cache_identity_payload"])
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (output / overlays.SUCCESS_FILENAME).write_text(
        _sha256_file(manifest_path) + "\n", encoding="ascii"
    )
    with pytest.raises(replay.OneSecondReplayABIError, match="model/meta hashes"):
        replay.load_admitted_one_second_overlay(output, allow_test_only=True)


def test_formal_bundle_rehashes_all_physical_head_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle_path = tmp_path / "bundle_meta.json"
    bundle_path.write_text("{}", encoding="utf-8")
    rows = [
        {
            "head": head,
            "model_sha256": hashlib.sha256(f"model:{head}".encode()).hexdigest(),
            "metadata_sha256": hashlib.sha256(f"meta:{head}".encode()).hexdigest(),
        }
        for head in training.HEAD_SPECS
    ]
    payload = {
        "research_bundle": {
            "bundle_path": str(bundle_path),
            "bundle_sha256": _sha256_file(bundle_path),
            "heads": rows,
        }
    }
    admitted = SimpleNamespace(
        bundle_sha256=_sha256_file(bundle_path),
        heads=tuple(
            SimpleNamespace(
                head=row["head"],
                model_sha256=row["model_sha256"],
                metadata_sha256=row["metadata_sha256"],
            )
            for row in rows
        ),
    )
    monkeypatch.setattr(overlays, "load_admitted_research_bundle", lambda _: admitted)
    replay._validate_physical_research_bundle(
        payload,
        expected_bundle_sha256=_sha256_file(bundle_path),
    )
    admitted.heads[0].metadata_sha256 = "f" * 64
    with pytest.raises(replay.OneSecondReplayABIError, match="physical hashes drifted"):
        replay._validate_physical_research_bundle(
            payload,
            expected_bundle_sha256=_sha256_file(bundle_path),
        )


def test_same_second_sample_and_hold_has_no_future_read() -> None:
    prediction = np.asarray([1_000, 2_000, 3_000], dtype=np.int64)
    events = np.asarray([999, 1_000, 1_999, 2_000, 2_999, 3_000], dtype=np.int64)
    assert replay.sample_and_hold_indices(prediction, events).tolist() == [-1, 0, 0, 1, 1, 2]


def test_v9_arms_only_change_model_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(replay, "load_operational_baseline_binding", lambda **_: _binding(tmp_path))
    base = {
        "ml_enabled": True,
        "vol_blend": 0.5,
        "skew_strength": 1.0,
        "asym_strength": 2.0,
        "ret_skew": 3.0,
        "gamma_dir_bonus": 4.0,
        "dynamic_fill_hazard_action_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": True,
        "buy_fill_selection_live_enabled": False,
        "buy_fill_selection_shadow_enabled": False,
        "gamma": 0.1,
    }
    arms = replay.bind_current_v9_ml_ab_arms(base)
    differences = {
        key
        for key in set(arms.ml_off) | set(arms.ml_on)
        if arms.ml_off.get(key) != arms.ml_on.get(key)
    }
    assert differences == {"ml_enabled", *replay.ML_PARAM_KEYS}
    assert arms.ml_on["dynamic_fill_hazard_shadow_enabled"] is True
    assert arms.ml_off["dynamic_fill_hazard_shadow_enabled"] is True


def test_v9_binding_rejects_q90_or_buy_selector_action(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    binding = _binding(tmp_path)
    binding["pointer"]["dynamic_fill_hazard_action_enabled"] = True
    monkeypatch.setattr(replay, "load_operational_baseline_binding", lambda **_: binding)
    with pytest.raises(replay.OneSecondReplayABIError, match="q90 action OFF"):
        replay.bind_current_v9_ml_ab_arms({})


def test_paired_runner_never_accepts_old_loader_window_and_only_injects_ml_on(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    schedule = replay.load_admitted_one_second_overlay(
        _write_test_overlay(tmp_path / "artifact"), allow_test_only=True
    )
    monkeypatch.setattr(replay, "load_operational_baseline_binding", lambda **_: _binding(tmp_path))
    window = SimpleNamespace(
        ml_data=None,
        book_source_authority="native_formal_lifecycle",
        trades="trades",
        var_ts_ms="var-ts",
        var_ssq="var-ssq",
        bbo_data="bbo",
        l2_data="l2",
        var_ti="ti",
        var_retsq="retsq",
    )
    calls: list[dict] = []

    def simulate(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {"terminal_mtm_pnl": 0.0}

    result = replay.run_paired_tick_replay(
        window=window,
        base_params={
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": True,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
        },
        schedule=schedule,
        simulate=simulate,
    )
    assert len(calls) == 2
    assert calls[0]["kwargs"]["ml_data"] is None
    assert len(calls[1]["kwargs"]["ml_data"]) == 6
    assert calls[1]["kwargs"]["ml_data"][0] is schedule.decision_ts_ms
    assert calls[1]["kwargs"]["ml_data"][1] is schedule.predictions["dir_10s"]
    assert result["identity"]["historical_10s_loader_called"] is False
    assert result["identity"]["full_path_ml_ab_run"] is True

    window.ml_data = (np.asarray([1]),)
    with pytest.raises(replay.OneSecondReplayABIError, match="model-free window"):
        replay.run_paired_tick_replay(
            window=window,
            base_params={},
            schedule=schedule,
            simulate=simulate,
        )


def test_replay_identity_is_pre_economic_and_hash_bound(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    schedule = replay.load_admitted_one_second_overlay(
        _write_test_overlay(tmp_path / "artifact"), allow_test_only=True
    )
    monkeypatch.setattr(replay, "load_operational_baseline_binding", lambda **_: _binding(tmp_path))
    arms = replay.bind_current_v9_ml_ab_arms(
        {
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        }
    )
    identity = replay.replay_identity_payload(schedule, arms, engine="cpp")
    assert identity["all_13_heads_validated"] is True
    assert identity["replay_projection_heads"] == list(replay.REPLAY_HEADS)
    assert identity["economic_outcomes_read"] is False
    assert identity["full_path_ml_ab_run"] is False
    assert identity["prediction_authority"] is False
    assert identity["action_authority"] is False
    assert identity["live_authority"] is False


def test_model_free_window_loader_cannot_be_overridden() -> None:
    with pytest.raises(replay.OneSecondReplayABIError, match="cannot override"):
        replay.load_model_free_tick_window(
            "2025-07-21",
            {},
            load_ml=True,
        )


def test_daily_runner_binds_overlay_day_and_uses_model_free_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    overlay_dir = _write_test_overlay(tmp_path / "artifact")
    monkeypatch.setattr(replay, "load_operational_baseline_binding", lambda **_: _binding(tmp_path))
    window = SimpleNamespace(
        ml_data=None,
        book_source_authority="native_formal_lifecycle",
        trades="trades",
        var_ts_ms="var-ts",
        var_ssq="var-ssq",
        bbo_data="bbo",
        l2_data="l2",
        var_ti="ti",
        var_retsq="retsq",
    )
    loaded: list[tuple[str, dict, dict]] = []

    def window_loader(day, params, **options):
        loaded.append((day, dict(params), dict(options)))
        return window

    calls: list[tuple] = []

    def simulate(*args, **kwargs):
        calls.append((args, kwargs))
        return {"terminal_mtm_pnl": 0.0}

    result = replay.run_daily_paired_tick_replay(
        day="2025-07-21",
        overlay_dir=overlay_dir,
        base_params={
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
        window_loader=window_loader,
        simulate=simulate,
        allow_test_only=True,
    )
    assert loaded == [
        (
            "2025-07-21",
            {
                "dynamic_fill_hazard_action_enabled": False,
                "buy_fill_selection_live_enabled": False,
            },
            {},
        )
    ]
    assert len(calls) == 2
    assert result["identity"]["utc_day"] == "2025-07-21"

    with pytest.raises(replay.OneSecondReplayABIError, match="differs from replay day"):
        replay.run_daily_paired_tick_replay(
            day="2025-07-22",
            overlay_dir=overlay_dir,
            base_params={},
            window_loader=window_loader,
            simulate=simulate,
            allow_test_only=True,
        )
