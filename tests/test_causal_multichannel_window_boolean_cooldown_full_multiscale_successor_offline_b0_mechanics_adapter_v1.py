from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import yaml

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_b0_mechanics_adapter_v1 as adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as builder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)


def _request(tmp_path: Path) -> builder.DayMaterializationRequest:
    files = {}
    for name in (
        "bbo.parquet",
        "l2.parquet",
        "features.parquet",
        "source.json",
        "book.json",
        "features.json",
        "config.yaml",
    ):
        path = tmp_path / name
        path.write_text("{}", encoding="ascii")
        files[name] = path
    return builder.DayMaterializationRequest(
        utc_day="2026-06-27",
        panel_role=builder.PANEL_ROLE,
        queue_identity=builder.QUEUE_IDENTITY,
        same_millisecond_ambiguity_policy="censor",
        bbo_path=files["bbo.parquet"],
        l2_path=files["l2.parquet"],
        features_path=files["features.parquet"],
        source_manifest_path=files["source.json"],
        book_view_manifest_path=files["book.json"],
        features_manifest_path=files["features.json"],
        private_config_path=files["config.yaml"],
        native_observation_root=tmp_path / "observations",
        source_receipts={"source_manifest_canonical_sha256": "1" * 64},
        input_binding_sha256="2" * 64,
    )


def _replay_inputs() -> adapter._ReplayInputs:
    return adapter._ReplayInputs(
        utc_day="2026-06-27",
        continuation_day="2026-06-28",
        trades=np.array([1]),
        var_ts_ms=np.array([1], dtype=np.int64),
        var_ssq=np.array([1.0]),
        var_ti=np.array([1.0]),
        var_retsq=np.array([0.0]),
        bbo_data=object(),
        l2_data=object(),
        ml_data=(np.array([1], dtype=np.int64),),
        params={"rng_seed": 42},
        market_window_identity_sha256="3" * 64,
        model_overlay_identity_sha256="4" * 64,
        latency_identity_sha256="5" * 64,
        queue_random_identity_sha256="6" * 64,
        replay_input_receipt_sha256="7" * 64,
    )


def test_factory_identity_matches_panel_builder() -> None:
    instance = adapter.build_canonical_b0_mechanics_adapter()
    assert instance.identity == builder.CANONICAL_ADAPTER_IDENTITY
    assert adapter.IDENTITY == builder.CANONICAL_ADAPTER_IDENTITY


def test_missing_fields_are_explicit_and_outcome_blind() -> None:
    error = adapter.BlockedMissingCanonicalFields(
        "2026-06-27",
        ("features_only:2026-06-28", "native_observation:2026-06-28"),
    )
    payload = error.as_dict()
    assert payload["status"] == "blocked_missing_canonical_fields"
    assert payload["missing_canonical_fields"] == [
        "features_only:2026-06-28",
        "native_observation:2026-06-28",
    ]
    assert payload["economic_outcomes_read"] is False
    assert payload["candidate_actions_generated"] is False


def test_run_day_executes_only_b0_mechanics_and_returns_no_economics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path)
    replay = _replay_inputs()
    calls = []
    emitter = SimpleNamespace(
        audit=lambda: SimpleNamespace(snapshots_emitted=7, economic_outcomes_read=False)
    )
    evaluator = object()
    monkeypatch.setattr(adapter, "_materialize_replay_inputs", lambda value: replay)

    def execute(value, *, emitter, evaluator):
        calls.append((value, emitter, evaluator))
        return {
            f"snapshot-{index}": {
                "campaign_id": index,
                "order_id": index,
                "exposure_fill_ordinal": index,
                "assignment_equity_usdc": float(index),
            }
            for index in range(1, 8)
        }

    monkeypatch.setattr(adapter, "_execute_outcome_blind_replay", execute)
    result = adapter.CanonicalB0MechanicsAdapter().run_day(
        request,
        emitter=emitter,
        evaluator=evaluator,
    )
    assert calls == [(replay, emitter, evaluator)]
    assert set(result) == builder._ADAPTER_RESULT_FIELDS
    assert result["current_owner_b0_executed"] is True
    assert result["candidate_actions_generated"] is False
    assert result["economic_outcomes_read"] is False
    assert result["labels_read"] is False
    assert not any("pnl" in key.lower() or "reward" in key.lower() for key in result)


def test_identity_hashes_bind_config_model_p3_feature_dag_and_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path)
    model = tmp_path / "model"
    model.mkdir()
    p3 = model / "fill_prob_params.json"
    p3.write_text("{}", encoding="ascii")
    monkeypatch.setattr(adapter, "_configuration", lambda value: (value.private_config_path, model))
    monkeypatch.setattr(
        adapter,
        "_load_json",
        lambda *args, **kwargs: {"feature_dag_sha256": "6" * 64},
    )
    monkeypatch.setattr(adapter, "_model_bundle_identity", lambda value: "7" * 64)
    hashes = adapter.CanonicalB0MechanicsAdapter().identity_hashes(request)
    assert set(hashes) == {
        "config_sha256",
        "code_sha256",
        "model_sha256",
        "p3_sha256",
        "feature_dag_sha256",
        "execution_abi_sha256",
        "baseline_identity_sha256",
    }
    assert hashes["model_sha256"] == "7" * 64
    assert hashes["feature_dag_sha256"] == "6" * 64
    assert hashes["baseline_identity_sha256"] == offline.ACTIVE_OWNER_POLICY_SHA256


def test_replay_discards_simulator_economic_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay = _replay_inputs()
    seen = {}

    def simulate(*args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return {
            "pnl": 999.0,
            "labels": [1],
            "_cooldown_duration_opportunity_trace": [
                {
                    "exposure_fill_ordinal": 1,
                    "campaign_id": 3,
                    "order_id": 0,
                    "assignment_equity_usdc": 11.5,
                    "side": "SELL",
                    "role_at_fill": "add",
                }
            ],
            "_cooldown_v2_snapshot_receipts": [
                {
                    "snapshot_id": "snapshot-1",
                    "exposure_fill_ordinal": 1,
                    "campaign_id": 3,
                    "side": "SELL",
                    "role_at_fill": "add",
                }
            ],
        }

    monkeypatch.setattr(adapter.bt, "_simulate_tick_with_engine", simulate)
    emitter, evaluator = object(), object()
    assert adapter._execute_outcome_blind_replay(
        replay,
        emitter=emitter,
        evaluator=evaluator,
    ) == {
        "snapshot-1": {
            "campaign_id": 3,
            "order_id": 0,
            "exposure_fill_ordinal": 1,
            "assignment_equity_usdc": 11.5,
        }
    }
    assert seen["args"][0] == "python"
    assert seen["kwargs"]["ml_data"] is replay.ml_data
    assert seen["args"][4]["cooldown_v2_snapshot_emitter"] is emitter
    assert seen["args"][4]["cooldown_duration_policy_evaluator"] is evaluator


def test_offline_config_projection_disables_only_live_journal_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = {
        "strategy": {"fill_cooldown_s": 85.0},
        "lifecycle_journal_v2": {
            "enabled": True,
            "storage_profile": "bounded_remote_spool",
            "root": "/remote-only/path",
        },
    }
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    projected_payload: dict[str, object] = {}
    owner_key = adapter.live_runtime_policy.F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV
    monkeypatch.delenv(owner_key, raising=False)

    def load_params(*, config_path: Path, **kwargs):
        assert os.environ.get(owner_key) == "1"
        projected_payload.update(yaml.safe_load(config_path.read_text(encoding="utf-8")))
        return {"rng_seed": 42}

    monkeypatch.setattr(adapter, "load_tick_base_params", load_params)
    monkeypatch.setattr(adapter.bt, "configure_symbol", lambda symbol: None)
    result = adapter._load_params(config)
    assert result.changed_paths == ("lifecycle_journal_v2.enabled",)
    assert projected_payload["strategy"] == raw["strategy"]
    assert projected_payload["lifecycle_journal_v2"] == {
        **raw["lifecycle_journal_v2"],
        "enabled": False,
    }
    assert result.raw_mapping_sha256 != result.projected_mapping_sha256
    assert owner_key not in os.environ
