from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_post_oof as finalizer,
)


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )


def _policy(side: str, predicate: str) -> dict[str, Any]:
    action = "FIXED_79S"
    return {
        "identity": (
            "causal_multichannel_window_boolean_cooldown_duration_v2."
            "nested_chronological_boolean_oof.v1"
        ),
        "side": side,
        "ordered_first_match_rules": [
            {
                "action": action,
                "clauses": [{"literals": [{"predicate": predicate, "negated": side == "SELL"}]}],
            }
        ],
        "default_action": finalizer.CONTROL_ACTION,
        "permissions": {"action_authorized": False, "live_authorized": False},
    }


def _gate(*, panel: str, passed: bool, continuous: bool = False) -> dict[str, Any]:
    if continuous:
        return {
            "evaluated_after_outer_oof": True,
            "evidence_route": finalizer.EVIDENCE_ROUTE,
            "panel_scope": panel,
            "passed_for_owner_repeated_policy_successor": False,
            "decision": "diagnostic_only",
            "reasons": ["continuous_comparator_cannot_nominate_a_policy_successor"],
            "action_support": {
                "action_opportunities": 10,
                "action_campaigns": 4,
                "action_days": 3,
                "action_rate": 0.5,
            },
            "action_authorized": False,
            "live_authorized": False,
            "strict_queue_authorized": False,
        }
    return {
        "evaluated_after_outer_oof": True,
        "evidence_route": finalizer.EVIDENCE_ROUTE,
        "panel_scope": panel,
        "passed_for_owner_repeated_policy_successor": passed,
        "decision": "owner_replay_candidate_supported" if passed else "abstain",
        "reasons": [] if passed else ["identified_oof_lcb_not_above_economic_epsilon"],
        "action_support": {
            "action_opportunities": 8,
            "action_campaigns": 4,
            "action_days": 3,
            "action_rate": 0.4,
        },
        "action_authorized": False,
        "live_authorized": False,
        "strict_queue_authorized": False,
    }


@dataclass(frozen=True)
class _Fixture:
    oof_dir: Path
    config: Path
    config_sha: str
    spec: Path
    spec_sha: str
    execution: Path
    execution_sha: str

    def kwargs(self) -> dict[str, Any]:
        return {
            "config_path": self.config,
            "expected_config_sha256": self.config_sha,
            "spec_path": self.spec,
            "expected_spec_sha256": self.spec_sha,
            "execution_amendment_path": self.execution,
            "expected_execution_amendment_sha256": self.execution_sha,
        }


def _fixture(
    root: Path,
    *,
    passing_sides: set[str],
    passing_block: str = "M0",
    continuous_claims_support: bool = False,
) -> _Fixture:
    days = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
    panels = {
        "prefix40_modeled_label_development": {
            "days": 4,
            "eligible_feature_blocks": ["R0", "M0"],
            "may_grant_owner_exploratory_support": True,
            "may_grant_strict_research_support": False,
        },
        "prefix33_raw_m2_common_support": {
            "days": 3,
            "excluded_days": [days[1]],
            "eligible_feature_blocks": ["R0", "M0", "M1", "M2"],
            "may_grant_owner_exploratory_support": True,
            "may_grant_strict_research_support": False,
        },
        "added10": {"available_modeled_labels": False, "status": "not_run_not_imputed"},
        "pooled50": {"available_modeled_labels": False, "status": "not_run_not_imputed"},
    }
    spec = {
        "schema_version": finalizer.SPEC_SCHEMA,
        "identity": finalizer.IDENTITY,
        "status": "pre_economic_oof_frozen",
        "strict_native_boundary": {
            "strict_label_execution_eligible": False,
            "exact_queue_policy_eligible": False,
            "strict_queue_authority_inherited": False,
        },
        "development_days": days,
        "analysis_panels": panels,
        "duration_vocabulary": {
            "BUY": [finalizer.CONTROL_ACTION, "FIXED_79S"],
            "SELL": [finalizer.CONTROL_ACTION, "FIXED_79S"],
        },
        "promotion_contract": {
            "only_supported_side_may_advance": True,
            "support_label": "owner_risk_accepted_promotion",
            "strict_research_supported_promotion_available": False,
            "repeated_policy_required_after_oof": True,
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    config = {
        "schema_version": finalizer.CONFIG_SCHEMA,
        "identity": finalizer.IDENTITY,
        "config_status": "frozen_before_owner_oof_economic_read",
        "analysis_panels": [
            "prefix40_modeled_label_development",
            "prefix33_raw_m2_common_support",
        ],
        "feature_blocks": ["R0", "M0", "M1", "M2"],
        "post_oof_gate": {
            "only_prefix_panels_may_grant_owner_support": True,
            "added10_or_pooled50_may_grant_support": False,
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    spec_path = root / "spec.json"
    config_path = root / "config.json"
    _write_json(spec_path, spec)
    _write_json(config_path, config)
    spec_sha = _file_sha(spec_path)
    config_sha = _file_sha(config_path)

    artifacts = {
        "frozen_config": {
            "path": str(config_path.resolve()),
            "sha256": config_sha,
            "identity": finalizer.IDENTITY,
        },
        "frozen_owner_spec": {
            "path": str(spec_path.resolve()),
            "sha256": spec_sha,
            "identity": finalizer.IDENTITY,
        },
        "modeled_label_manifest": {
            "path": str((root / "labels.json").resolve()),
            "sha256": "1" * 64,
            "identity": "multiscale_ema_boolean_cooldown_duration_policy_v1",
        },
        "feature_panel_manifest": {
            "path": str((root / "features.json").resolve()),
            "sha256": "2" * 64,
            "identity": (
                "causal_multichannel_window_boolean_cooldown_duration_v2."
                "owner_modeled_queue_feature_panel.v1"
            ),
        },
    }
    execution = {
        "schema_version": finalizer.EXECUTION_AMENDMENT_SCHEMA,
        "identity": finalizer.IDENTITY,
        "status": "frozen_before_owner_oof_economic_read",
        "artifact_bindings": artifacts,
        "code_bindings": [{"path": "modeled_oof.py", "sha256": "3" * 64}],
        "library_versions": {"python": "test"},
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    execution["execution_identity_sha256"] = _canonical_sha(execution)
    execution_path = root / "execution.json"
    _write_json(execution_path, execution)
    execution_sha = _file_sha(execution_path)

    bindings = {
        "frozen_config": {"path": str(config_path.resolve()), "sha256": config_sha},
        "frozen_owner_spec": {"path": str(spec_path.resolve()), "sha256": spec_sha},
        "execution_amendment": {
            "path": str(execution_path.resolve()),
            "sha256": execution_sha,
            "execution_identity_sha256": execution["execution_identity_sha256"],
            "artifact_bindings": artifacts,
        },
        "code": {"modeled_oof.py": "3" * 64},
        "library_versions": {"python": "test"},
    }
    bindings["binding_sha256"] = _canonical_sha(bindings)

    report_results: dict[str, Any] = {}
    selected_candidates: dict[str, Any] = {}
    panel_denominators: dict[str, Any] = {}
    for panel in ("prefix40_modeled_label_development", "prefix33_raw_m2_common_support"):
        panel_days = (
            days if panel.startswith("prefix40") else [day for day in days if day != days[1]]
        )
        blocks = panels[panel]["eligible_feature_blocks"]
        panel_denominators[panel] = {
            "days": panel_days,
            "day_count": len(panel_days),
            "eligible_feature_blocks": blocks,
            "all_blocks_use_common_scope_denominator": True,
            "sides": {},
        }
        report_results[panel] = {}
        selected_candidates[panel] = {}
        for side in finalizer.SIDES:
            report_results[panel][side] = {}
            selected_candidates[panel][side] = {}
            for block in blocks:
                policy = _policy(side, f"{block.lower()}::{side.lower()}")
                policy_sha = _canonical_sha(policy)
                passed = (
                    side in passing_sides
                    and panel.startswith("prefix40")
                    and block == passing_block
                )
                boolean_gate = _gate(panel=panel, passed=passed)
                continuous_gate = _gate(panel=panel, passed=False, continuous=True)
                if continuous_claims_support and panel.startswith("prefix40") and block == "M0":
                    continuous_gate["passed_for_owner_repeated_policy_successor"] = True
                    continuous_gate["decision"] = "owner_replay_candidate_supported"
                    continuous_gate["reasons"] = []
                report_results[panel][side][block] = {
                    "boolean": {
                        "side": side,
                        "feature_block": block,
                        "panel_scope": panel,
                        "method": "bounded_sparse_boolean_dnf",
                        "deployment_gate": boolean_gate,
                        "folds": [{"fold_id": "outer1", "selected_candidate_id": policy_sha}],
                    },
                    "continuous": {
                        "side": side,
                        "feature_block": block,
                        "panel_scope": panel,
                        "method": "continuous_multioutput_decision_tree",
                        "deployment_gate": continuous_gate,
                        "folds": [{"fold_id": "outer1"}],
                        "diagnostic_point_uplift_usdc": 999.0,
                    },
                }
                selected_candidates[panel][side][block] = {
                    "boolean": [policy],
                    "continuous": [{"diagnostic_model": "cannot_nominate"}],
                }

    report = {
        "schema_version": finalizer.REPORT_SCHEMA,
        "identity": finalizer.IDENTITY,
        "evidence_route": finalizer.EVIDENCE_ROUTE,
        "queue_authority": finalizer.QUEUE_AUTHORITY,
        "strict_queue_policy_eligible": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "config_sha256": config_sha,
        "binding_sha256": bindings["binding_sha256"],
        "results": report_results,
        "panel_denominators": panel_denominators,
        "not_run_panels": {
            "added10": {
                "modeled_labels_imputed": False,
                "economic_oof_run": False,
                "may_grant_support": False,
            },
            "pooled50": {
                "modeled_labels_imputed": False,
                "economic_oof_run": False,
                "may_grant_support": False,
            },
        },
        "permissions": {
            "research_authority": "owner_route_exploratory_only",
            "action_authorized": False,
            "live_authorized": False,
        },
    }

    oof_dir = root / "oof"
    oof_dir.mkdir()
    files = {
        "frozen_config.json": config,
        "frozen_owner_spec.json": spec,
        "bindings.json": bindings,
        "report.json": report,
        "selected_candidates.json": selected_candidates,
    }
    for name, payload in files.items():
        _write_json(oof_dir / name, payload)
    (oof_dir / "purge_audits.json").write_text("{}\n", encoding="ascii")
    (oof_dir / "outer_oof.parquet").write_bytes(b"ECONOMIC_TABLE_MUST_NOT_BE_READ")
    inventory = [
        {
            "relative_path": path.name,
            "bytes": path.stat().st_size,
            "sha256": _file_sha(path),
        }
        for path in sorted(oof_dir.iterdir())
    ]
    manifest = {
        "schema_version": finalizer.OOF_MANIFEST_SCHEMA,
        "identity": finalizer.IDENTITY,
        "evidence_route": finalizer.EVIDENCE_ROUTE,
        "queue_authority": finalizer.QUEUE_AUTHORITY,
        "config_sha256": config_sha,
        "owner_spec_sha256": spec_sha,
        "binding_sha256": bindings["binding_sha256"],
        "files": inventory,
        "permissions": {
            "strict_queue_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest_path = oof_dir / finalizer.MANIFEST_NAME
    _write_json(manifest_path, manifest)
    (oof_dir / finalizer.SUCCESS_NAME).write_text(_file_sha(manifest_path) + "\n", encoding="ascii")
    return _Fixture(
        oof_dir=oof_dir,
        config=config_path,
        config_sha=config_sha,
        spec=spec_path,
        spec_sha=spec_sha,
        execution=execution_path,
        execution_sha=execution_sha,
    )


@pytest.mark.parametrize(
    ("passing_sides", "expected"),
    [
        ({"BUY"}, ["BUY"]),
        ({"SELL"}, ["SELL"]),
        ({"BUY", "SELL"}, ["BUY", "SELL"]),
        (set(), []),
    ],
)
def test_atomic_post_oof_side_decisions(
    tmp_path: Path,
    passing_sides: set[str],
    expected: list[str],
) -> None:
    fixture = _fixture(tmp_path, passing_sides=passing_sides)
    preflight = finalizer.preflight_post_oof(fixture.oof_dir, **fixture.kwargs())
    assert preflight["supported_sides"] == expected
    assert preflight["economic_tables_read"] == []

    output = tmp_path / "final"
    result = finalizer.finalize_post_oof(
        fixture.oof_dir,
        output,
        **fixture.kwargs(),
    )
    assert result["supported_sides"] == expected
    manifest_path = output / finalizer.MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    canonical = manifest.pop("canonical_manifest_sha256")
    assert canonical == _canonical_sha(manifest)
    assert (output / finalizer.SUCCESS_NAME).read_text(encoding="ascii").strip() == _file_sha(
        manifest_path
    )
    artifact_name = finalizer.POLICY_BUNDLE_NAME if expected else finalizer.CLOSURE_NAME
    artifact = json.loads((output / artifact_name).read_text(encoding="ascii"))
    artifact_canonical = artifact.pop("canonical_sha256")
    assert artifact_canonical == _canonical_sha(artifact)
    for side in finalizer.SIDES:
        side_payload = artifact["side_policies"][side]
        if side in expected:
            assert side_payload["mode"] == "owner_risk_accepted_boolean_candidate_bundle"
            assert side_payload["supported_cells"]
        else:
            assert side_payload["mode"] == "control_only"
            assert side_payload["fixed_action"] == finalizer.CONTROL_ACTION
            assert side_payload["reason"] == "no_M0_absolute_post_oof_gate_passed"
            assert not side_payload["feature_family_gate"]["paired_M1_minus_M0_evaluated"]
    if not expected:
        assert artifact["structural_eligibility"] == {
            "repeated_policy_implementation_eligible": False,
            "repeated_policy_economic_authorized": False,
            "restart_aware_execution_eligible": False,
            "transport_execution_eligible": False,
        }
        assert artifact["no_pass_scope"]["does_not_claim_entire_ema_architecture_failed"]


def test_non_m0_absolute_pass_cannot_bypass_frozen_hierarchy(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, passing_sides={"BUY"}, passing_block="M1")
    preflight = finalizer.preflight_post_oof(fixture.oof_dir, **fixture.kwargs())
    assert preflight["supported_sides"] == []
    assert preflight["decision_type"] == "limited_modeled_queue_one_shot_oof_no_pass"


def test_continuous_comparator_never_promotes_and_economic_table_is_not_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path, passing_sides=set())
    real_hash = finalizer._file_sha256

    def guarded_hash(path: Path) -> str:
        assert Path(path).suffix != ".parquet"
        return real_hash(path)

    monkeypatch.setattr(finalizer, "_file_sha256", guarded_hash)
    output = tmp_path / "final"
    result = finalizer.finalize_post_oof(
        fixture.oof_dir,
        output,
        **fixture.kwargs(),
    )
    assert result["decision_type"] == "limited_modeled_queue_one_shot_oof_no_pass"
    assert not (output / finalizer.POLICY_BUNDLE_NAME).exists()
    assert (output / finalizer.CLOSURE_NAME).is_file()


def test_continuous_authority_claim_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(
        tmp_path,
        passing_sides=set(),
        continuous_claims_support=True,
    )
    with pytest.raises(
        finalizer.PostOofFinalizationError,
        match="continuous comparator attempted",
    ):
        finalizer.preflight_post_oof(fixture.oof_dir, **fixture.kwargs())


@pytest.mark.parametrize("target", ["report", "config", "spec", "execution"])
def test_bound_hash_drift_fails_before_publication(tmp_path: Path, target: str) -> None:
    fixture = _fixture(tmp_path, passing_sides={"BUY"})
    path = {
        "report": fixture.oof_dir / "report.json",
        "config": fixture.config,
        "spec": fixture.spec,
        "execution": fixture.execution,
    }[target]
    path.write_bytes(path.read_bytes() + b" ")
    output = tmp_path / "final"
    with pytest.raises(finalizer.PostOofFinalizationError, match="SHA256 mismatch"):
        finalizer.finalize_post_oof(
            fixture.oof_dir,
            output,
            **fixture.kwargs(),
        )
    assert not output.exists()
