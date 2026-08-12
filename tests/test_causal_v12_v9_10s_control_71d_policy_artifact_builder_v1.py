from __future__ import annotations

import hashlib
import json
import pickle
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

pytest.importorskip("narrowgate_cpp")

from models.replay.restart_aware_continuous_ab import canonical_sha256
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as framework,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_3 as concrete,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_v9_10s_control_71d_policy_artifact_builder_v1 as subject,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1 as f05,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binding(path: Path) -> dict[str, object]:
    return {
        "path": str(path.resolve()),
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _minimal_raw_config() -> dict[str, object]:
    return {
        "strategy": {
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
        "ml": {"enabled": True},
        "lifecycle_journal_v2": {
            "enabled": True,
            "storage_profile": "bounded_remote_spool",
            "root": "/remote/journal",
        },
    }


def test_offline_projection_changes_only_remote_journal_writer() -> None:
    raw = _minimal_raw_config()
    projected, differences = subject._offline_projection_payload(raw)  # noqa: SLF001

    assert raw["lifecycle_journal_v2"]["enabled"] is True
    assert projected["lifecycle_journal_v2"]["enabled"] is False
    assert differences == [
        {
            "path": "lifecycle_journal_v2.enabled",
            "before": True,
            "after": False,
        }
    ]
    assert projected["strategy"] == raw["strategy"]
    assert projected["ml"] == raw["ml"]


def test_offline_projection_rejects_enabled_action() -> None:
    raw = _minimal_raw_config()
    raw["strategy"]["dynamic_fill_hazard_action_enabled"] = True
    with pytest.raises(subject.F03ControlArtifactBuildError, match="q90 action"):
        subject._offline_projection_payload(raw)  # noqa: SLF001


def test_offline_projection_receipt_is_deeply_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_path = tmp_path / "raw.yaml"
    raw_path.write_text(yaml.safe_dump(_minimal_raw_config()), encoding="utf-8")
    monkeypatch.setattr(
        subject.native_runner,
        "_load_formal_base_params",
        lambda _path: {
            "ml_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_live_enabled": False,
        },
    )
    offline, receipt, payload = subject._materialize_offline_execution_config(  # noqa: SLF001
        raw_config=_binding(raw_path),
        cache_root=tmp_path / "cache",
    )

    observed = subject.validate_offline_execution_config(
        raw_config=_binding(raw_path),
        offline_config=offline,
        receipt=receipt,
    )
    assert observed["projection_identity_sha256"] == payload[
        "projection_identity_sha256"
    ]
    Path(str(offline["path"])).write_text("tampered: true\n", encoding="utf-8")
    with pytest.raises(
        subject.F03ControlArtifactBuildError,
        match="(size|SHA256) drift",
    ):
        subject.validate_offline_execution_config(
            raw_config=_binding(raw_path),
            offline_config=offline,
            receipt=receipt,
        )


def test_market_window_rejects_ml_and_source_authority_drift(tmp_path: Path) -> None:
    valid = SimpleNamespace(
        ml_data=None,
        trades=[1],
        bbo_data=object(),
        l2_data=object(),
        book_source_authority="native_formal_lifecycle",
    )
    path = tmp_path / "window.pkl"
    path.write_bytes(pickle.dumps(valid))
    subject.validate_market_window(
        _binding(path),
        day=framework.EXPECTED_DAYS[0],
        source_profile="native",
    )

    polluted = SimpleNamespace(**vars(valid) | {"ml_data": (1,)})
    polluted_path = tmp_path / "polluted.pkl"
    polluted_path.write_bytes(pickle.dumps(polluted))
    with pytest.raises(subject.F03ControlArtifactBuildError, match="not model-free"):
        subject.validate_market_window(
            _binding(polluted_path),
            day=framework.EXPECTED_DAYS[0],
            source_profile="native",
        )
    with pytest.raises(subject.F03ControlArtifactBuildError, match="source authority"):
        subject.validate_market_window(
            _binding(path),
            day=framework.EXPECTED_DAYS[0],
            source_profile="provider_normalized",
        )


def test_asof_feature_projection_holds_only_past_ready_state() -> None:
    index = pd.to_datetime([1_000, 3_000], unit="ms", utc=True)
    features = pd.DataFrame({"signal": [1.0, 3.0]}, index=index)
    selected, audit = subject._asof_feature_projection_frame(  # noqa: SLF001
        features,
        canonical_labels_ms=np.asarray([2_000, 3_000, 4_000], dtype=np.int64),
    )

    assert selected["signal"].tolist() == [1.0, 3.0, 3.0]
    assert audit["exact_generation_count"] == 1
    assert audit["asof_hold_count"] == 2
    assert audit["max_asof_age_ms"] == 1_000
    assert audit["future_rows_used"] == 0
    assert audit["interpolation_used"] is False


def _source_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, day in enumerate(framework.EXPECTED_DAYS):
        rows.append(
            {
                "day": day,
                "book_identity": (
                    "native_available"
                    if index < 52
                    else "provider_normalized_sensitivity"
                ),
                "book_root": f"/book/{day}",
                "feature_identity": f"feature-{day}",
                "exact_queue_authority": index < 52,
                "exact_lifecycle_authority": index < 52,
                "continuous_economic_sensitivity_authority": True,
                "artifacts": [
                    {"role": "bbo", "path": f"/bbo/{day}"},
                    {"role": "l2", "path": f"/l2/{day}"},
                    {"role": "feature", "path": f"/feature/{day}"},
                ],
            }
        )
    return rows


def test_frozen_source_loader_requires_exact_71_day_order_and_strata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _source_rows()
    monkeypatch.setattr(
        subject.concrete,
        "load_historical_framework_plan",
        lambda *_args: {
            "plan_identity_sha256": "a" * 64,
            "continuous_plan": {"source_bindings": rows},
        },
    )
    _, observed = subject.load_frozen_sources(Path("unused"))
    assert tuple(observed) == framework.EXPECTED_DAYS
    assert sum(row["source_profile"] == "native" for row in observed.values()) == 52
    assert (
        sum(
            row["source_profile"] == "provider_normalized"
            for row in observed.values()
        )
        == 19
    )

    reversed_rows = list(reversed(rows))
    monkeypatch.setattr(
        subject.concrete,
        "load_historical_framework_plan",
        lambda *_args: {
            "continuous_plan": {"source_bindings": reversed_rows}
        },
    )
    with pytest.raises(subject.F03ControlArtifactBuildError, match="ordered 71 days"):
        subject.load_frozen_sources(Path("unused"))


def test_provider_authority_view_binds_frozen_source_without_mutating_it(
    tmp_path: Path,
) -> None:
    day = "2026-04-28"
    prior = "2026-04-27"
    source_root = tmp_path / "provider-source"
    artifacts: dict[str, dict[str, object]] = {}
    for source_day in (prior, day):
        for role in ("bbo", "l2"):
            path = source_root / role / f"BTCUSDC-{role}-{source_day}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(f"{source_day}-{role}".encode("ascii"))
            if source_day == day:
                artifacts[role] = {"role": role, **_binding(path)}
    source = {
        "book_root": str(source_root),
        "source_profile": "provider_normalized",
        "source_identity_sha256": "a" * 64,
        "artifacts_by_role": artifacts,
    }
    view_root, binding = subject._provider_authority_view(  # noqa: SLF001
        day=day,
        source=source,
        sources={day: source},
        cache_root=tmp_path / "cache",
    )

    quality = (view_root / "daily_quality.csv").read_text(encoding="utf-8")
    assert f"{day},provider_normalized_causal,false,true,false" in quality
    assert binding["strict_queue_authorized"] is False
    assert len(binding["linked_inputs"]) == 4
    assert not (source_root / "manifest.json").exists()
    assert not (source_root / "daily_quality.csv").exists()


def test_builder_emits_both_f03_policy_schemas_with_exact_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = tmp_path / "shared.bin"
    shared.write_bytes(b"shared")
    artifact = _binding(shared)
    sources = {
        day: {
            "source_profile": "native" if index < 52 else "provider_normalized",
            "source_identity_sha256": canonical_sha256({"source": day}),
        }
        for index, day in enumerate(framework.EXPECTED_DAYS)
    }
    days = {
        day: {
            **sources[day],
            "market_window": artifact,
            "control_overlay_binding": {
                "manifest": artifact,
                "data": artifact,
                "identity_sha256": canonical_sha256({"overlay": day}),
            },
        }
        for day in framework.EXPECTED_DAYS
    }
    operational = {
        "historical_v9_baseline_id": "historical-v9",
        "current_baseline_id": "current-v10",
        "raw_operational_config": artifact,
        "operational_config": artifact,
        "offline_config_receipt": artifact,
        "offline_config_structured_differences": [
            {
                "path": "lifecycle_journal_v2.enabled",
                "before": True,
                "after": False,
            }
        ],
        "baseline_identity": artifact,
        "historical_v9_identity": artifact,
        "pointer": artifact,
        "model": {
            "bundle_meta": artifact,
            "content_identity_sha256": "b" * 64,
        },
        "p3": artifact,
        "feature_dag": artifact,
        "semantic_feature_dag_sha256": "c" * 64,
    }
    monkeypatch.setattr(
        subject,
        "load_frozen_sources",
        lambda *_args, **_kwargs: (
            {"plan_identity_sha256": "d" * 64},
            sources,
        ),
    )
    monkeypatch.setattr(
        subject,
        "load_operational_projection",
        lambda *_args, **_kwargs: operational,
    )
    monkeypatch.setattr(subject, "_load_control_panel", lambda _path: ({}, {}))
    monkeypatch.setattr(subject, "_collect_complete_days", lambda **_kwargs: days)
    monkeypatch.setattr(subject, "build_initial_state", lambda **_kwargs: artifact)
    monkeypatch.setattr(subject.framework, "load_policy_artifacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        subject,
        "validate_admission",
        lambda root, **_kwargs: {
            "admission_root": str(root),
            "policy_identity_sha256": json.loads(
                (root / subject.V13_MANIFEST).read_text(encoding="utf-8")
            )["policy_identity_sha256"],
        },
    )
    paths = subject.BuilderPaths(
        latency_profile=shared,
        engine_state_schema=shared,
        cache_root=tmp_path / "cache",
    )
    root = tmp_path / "admission"
    subject.build_admission(admission_root=root, paths=paths)

    v12 = json.loads((root / subject.V12_MANIFEST).read_text(encoding="utf-8"))
    v13 = json.loads((root / subject.V13_MANIFEST).read_text(encoding="utf-8"))
    assert v12["schema_version"] == framework.POLICY_ARTIFACT_SCHEMA_VERSION
    assert v13["schema_version"] == concrete.POLICY_SCHEMA_VERSION
    assert tuple(v12["days"]) == framework.EXPECTED_DAYS
    assert tuple(v13["days"]) == framework.EXPECTED_DAYS
    assert v13["raw_operational_config"] == artifact
    assert v13["offline_execution_config_receipt"] == artifact


def test_f05_accepts_concrete_control_only_plan(tmp_path: Path) -> None:
    shared = tmp_path / "shared.bin"
    shared.write_bytes(b"shared")
    artifact = _binding(shared)
    manifest_payload = {
        "schema_version": concrete.POLICY_SCHEMA_VERSION,
        "arm": "control",
        "identity": "test-control",
        "cadence_ms": 10_000,
        "ml_enabled": True,
        "q90_action_enabled": False,
        "buy_fill_selection_enabled": False,
        "operational_config": artifact,
        "baseline_identity": artifact,
        "bundle_meta": artifact,
        "feature_dag": artifact,
        "initial_state": artifact,
        "policy_identity_sha256": "a" * 64,
        "model_bundle_identity_sha256": "b" * 64,
        "days": {
            day: {
                "source_profile": (
                    "native" if index < 52 else "provider_normalized"
                ),
                "source_identity_sha256": canonical_sha256({"source": day}),
                "market_window": artifact,
                "control_overlay_binding": {},
            }
            for index, day in enumerate(framework.EXPECTED_DAYS)
        },
    }
    manifest = tmp_path / "control-policy.json"
    _write_json(manifest, manifest_payload)
    observed = concrete._load_policy_manifest(  # noqa: SLF001
        manifest,
        expected_arm="control",
    )
    plan_without_identity = {
        "schema_version": concrete.PLAN_SCHEMA_VERSION,
        "identity": "test-control-only-plan",
        "operation_tape_sha256": "c" * 64,
        "policy_artifacts": {"control": observed},
        "blockers": ["candidate_authoritative_market_window_and_overlay_unbound"],
        "execution_eligible": False,
    }
    plan = plan_without_identity | {
        "plan_identity_sha256": canonical_sha256(plan_without_identity)
    }
    plan_path = tmp_path / concrete.PLAN_FILENAME
    _write_json(plan_path, plan)
    (tmp_path / concrete.PLAN_SUCCESS).write_text(_sha256(plan_path) + "\n", encoding="ascii")

    binding, blockers = f05._inspect_f03_control_plan(plan_path)  # noqa: SLF001
    assert blockers == []
    assert binding is not None
    assert binding["control_policy_bound"] is True
