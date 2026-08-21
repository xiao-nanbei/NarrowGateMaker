from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as base_backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sell_only_orchestrator_v1 as orchestrator,
)


class _NoResumeAdapter:
    identity = "test-adapter"
    artifact_sha256 = "a" * 64

    def completed_side_resume_summary(self, *, require_complete: bool):
        assert require_complete is False
        return None


def test_sell_only_executor_forbids_cross_execution_strategy_cache_reuse() -> None:
    executor = orchestrator.formal_executor_contract()
    qualification = orchestrator.cpp_current_qualification_contract()

    assert executor["formal_sides"] == ["SELL"]
    assert executor["cross_execution_strategy_cache_reuse_allowed"] is False
    assert executor["predecessor_cache_key_transform_allowed"] is False
    assert executor["same_execution_exact_key_resume_allowed"] is True
    assert "completed_side_resume" not in executor
    assert qualification["invariance_receipt_file"] == orchestrator.INVARIANCE_RECEIPT_NAME
    assert qualification["fresh_current_source_hash_equality_required"] is True
    assert "predecessor_receipt_file" not in qualification


def test_historical_sell_only_bundle_does_not_invent_dataset_binding(
    tmp_path: Path,
) -> None:
    bundle = orchestrator.base._new_formal_offline_bundle(
        execution_manifest_path=tmp_path / "execution.json",
        execution_manifest={},
        source_manifest_path=tmp_path / "source.json",
        source_manifest={},
        panel_manifest_path=tmp_path / "panel.json",
        panel_manifest={},
        panel_files={},
        repository_root=tmp_path,
    )

    assert bundle.dataset_binding_path is None
    assert bundle.dataset_binding == {}


def test_sell_only_target_source_coverage_has_only_two_legal_paths() -> None:
    coverage = backend._target_request_source_coverage(_NoResumeAdapter())

    assert coverage["formal_sides"] == ["SELL"]
    assert coverage["allowed_resolution_paths"] == [
        "exact_v27_cache_key",
        "fresh_v27_compute",
    ]
    assert coverage["cross_execution_source_count"] == 0
    assert coverage["economic_outcomes_read"] is False
    assert coverage["action_authorized"] is False
    assert coverage["live_authorized"] is False


def test_sell_only_target_source_coverage_rejects_completed_side_resume() -> None:
    adapter = _NoResumeAdapter()
    adapter.completed_side_resume_summary = lambda **_kwargs: {"side": "BUY"}

    with pytest.raises(
        base_backend.OfflineRepeatedPolicyBackendError,
        match="forbids completed-side",
    ):
        backend._target_request_source_coverage(adapter)


def test_adapter_factory_receives_no_predecessor_cache_transform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = tmp_path / "adapter.py"
    module_path.write_text("# fixed adapter fixture\n", encoding="ascii")
    captured: dict[str, object] = {}

    class Acceleration:
        def __init__(self, *, day_input_cache_root: Path) -> None:
            captured["mmap_root"] = day_input_cache_root

    class Topology:
        def payload(self):
            return dict(orchestrator.base.EXECUTOR_ONE_SHOT_TOPOLOGY)

    adapter = _NoResumeAdapter()

    def factory(**kwargs):
        captured.update(kwargs)
        return adapter

    module = SimpleNamespace(
        __file__=str(module_path),
        SequentialReplayAccelerationOptions=Acceleration,
        OneShotProcessTopology=Topology,
        EXECUTOR_ACCELERATION_IDENTITY=(
            orchestrator.base.EXECUTOR_ACCELERATION_IDENTITY
        ),
        DAY_INPUT_CACHE_IDENTITY=orchestrator.base.EXECUTOR_DAY_INPUT_CACHE_IDENTITY,
        DAY_INPUT_MATERIALIZATION_WORKERS=(
            orchestrator.base.EXECUTOR_DAY_INPUT_MATERIALIZATION_WORKERS
        ),
        build_canonical_replay_adapter=factory,
    )
    bundle = SimpleNamespace(
        execution_manifest={"executor": orchestrator.formal_executor_contract()},
        repository_root=tmp_path,
    )
    qualification_sha256 = "f" * 64
    monkeypatch.setattr(
        backend.orchestrator,
        "_validate_cpp_current_qualification_receipt",
        lambda _bundle: {"canonical_receipt_sha256": qualification_sha256},
    )
    monkeypatch.setattr(backend.importlib, "import_module", lambda _name: module)
    monkeypatch.setattr(
        backend.backend,
        "CANONICAL_REPLAY_ADAPTER_MODULE",
        type(adapter).__module__,
    )
    monkeypatch.setattr(backend.backend, "_validate_adapter_shape", lambda value: value)
    monkeypatch.setattr(
        backend.backend,
        "_file_sha256",
        lambda _path: adapter.artifact_sha256,
    )
    monkeypatch.setattr(
        backend,
        "resolve_portable_path",
        lambda _value, *, root: root / "mmap",
    )

    observed = backend._load_canonical_replay_adapter(bundle)

    assert observed is adapter
    assert captured["completed_side_resume"] is None
    assert captured["completed_side_resume_receipt_sha256"] is None
    assert captured["cpp_qualification_receipt_sha256"] == qualification_sha256


def test_formal_backend_executes_only_sell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    bindings = SimpleNamespace(
        execution_manifest_sha256="1" * 64,
        source_manifest_sha256="2" * 64,
        panel_manifest_sha256="3" * 64,
        fold_manifest_sha256="4" * 64,
        nested_fold_manifest_sha256="5" * 64,
        exact_owner_policy_sha256=orchestrator.offline.ACTIVE_OWNER_POLICY_SHA256,
        exact_owner_predicate_bundle_sha256=(
            orchestrator.offline.ACTIVE_PREDICATE_BUNDLE_SHA256
        ),
        exact_owner_private_config_sha256=(
            orchestrator.offline.ACTIVE_PRIVATE_CONFIG_SHA256
        ),
    )
    mechanics = SimpleNamespace(
        panel=object(),
        fold_manifest=object(),
        bindings=bindings,
        mechanics_receipt_sha256="6" * 64,
    )

    class Adapter(_NoResumeAdapter):
        def build_search_contract(self, _mechanics):
            return ("ladder", "continuous")

    adapter = Adapter()

    class Provider:
        def __init__(self, _mechanics, _adapter) -> None:
            self.receipts = []

    class Evaluator:
        def __init__(self, _mechanics, _adapter) -> None:
            self.receipts = []

    class Result:
        def report(self):
            return {"status": "synthetic-no-economic-fixture"}

    def fake_nested(*_args, **kwargs):
        captured["config"] = kwargs["config"]
        return Result()

    monkeypatch.setattr(backend.orchestrator, "load_formal_sell_only_bundle", lambda _path: object())
    monkeypatch.setattr(backend, "_load_canonical_replay_adapter", lambda _bundle: adapter)
    monkeypatch.setattr(
        backend.backend,
        "_preflight_bound_panel_schema",
        lambda _bundle, _adapter: {
            "status": base_backend.FORMAL_PANEL_SCHEMA_READY_STATUS,
            "missing_canonical_fields": [],
        },
    )
    monkeypatch.setattr(
        backend.backend,
        "load_outcome_blind_mechanics",
        lambda *_args, **_kwargs: mechanics,
    )
    monkeypatch.setattr(
        backend,
        "_adapter_preflight",
        lambda *_args: {
            "status": base_backend.MECHANICS_READY_STATUS,
            "missing_canonical_fields": [],
        },
    )
    monkeypatch.setattr(backend.backend, "CanonicalFoldScopedLabelProvider", Provider)
    monkeypatch.setattr(backend.backend, "CanonicalSequentialEvaluator", Evaluator)
    monkeypatch.setattr(backend.nested, "run_nested_chronological_oof", fake_nested)

    result = backend.run_canonical_offline_sell_economics(tmp_path / "manifest.json")

    assert captured["config"].sides == ("SELL",)
    assert result["formal_sides"] == ["SELL"]
    assert result["component_scope"] == "sell_only_learning_algorithm_oof"
    assert result["cross_execution_strategy_cache_reuse_used"] is False
    assert result["permissions"]["action_authorized"] is False
    assert result["permissions"]["live_authorized"] is False


def test_composition_contract_cannot_claim_combined_result() -> None:
    contract = orchestrator.formal_composition_contract()

    assert contract["buy_component"]["source_execution_identity"] == "formal_v24"
    assert contract["buy_component"]["required_complete_cache_units"] == 577
    assert contract["sell_component"]["formal_sides"] == ["SELL"]
    assert contract["cross_commit_composition_receipt_required"] is True
    assert contract["combined_result_authorized"] is False
