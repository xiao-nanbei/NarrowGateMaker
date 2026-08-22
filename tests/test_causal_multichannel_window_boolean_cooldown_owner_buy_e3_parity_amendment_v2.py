from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_formal_component_closeout_v1 as component_closeout,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as predicate_view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as repeated_backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_amendment_v2 as subject,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity_v1,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_refit_v1 as refit,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return path


def _document(payload: dict[str, Any], field: str) -> dict[str, Any]:
    output = dict(payload)
    output[field] = subject._document_sha256(output, field)
    return output


@dataclass
class EvidenceFixture:
    root: Path
    days: tuple[str, ...]
    component_manifest_path: Path
    component_manifest: dict[str, Any]
    owner_manifest_path: Path
    artifact: parity_v1.LoadedExactArtifact
    source_bundle: predicate_view.FrozenPredicateBundle
    mechanics: SimpleNamespace
    mechanics_receipt_path: Path

    def freeze_contract(
        self,
        path: Path,
        *,
        days: tuple[str, ...] | None = None,
        mechanics_receipt_path: Path | None = None,
    ) -> dict[str, Any]:
        return dict(
            subject.freeze_layer4_lockstep_contract(
                output_path=path,
                formal_buy_component_artifact_manifest_path=self.component_manifest_path,
                owner_execution_manifest_path=self.owner_manifest_path,
                artifact=self.artifact,
                mechanics_identity_receipt_path=(
                    mechanics_receipt_path or self.mechanics_receipt_path
                ),
                source_predicate_bundle=self.source_bundle,
                ordered_development_days=days or self.days,
            )
        )


@pytest.fixture
def evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EvidenceFixture:
    days = tuple(
        (date(2026, 7, 7) + timedelta(days=offset)).isoformat() for offset in range(30)
    )

    component_manifest = _document(
        {
            "schema_version": f"{component_closeout.IDENTITY}.component_artifact_manifest.v1",
            "identity": (
                f"{component_closeout.IDENTITY}:formal_v24_buy_component_artifacts"
            ),
            "formal_side": "BUY",
            "source_execution_manifest_sha256": _sha("v24-execution"),
            "component_result_canonical_sha256": _sha("component-result"),
            "nested_oof_artifact_manifest_canonical_sha256": _sha("nested-oof"),
            "bindings": {},
            "permissions": dict(component_closeout.EXPECTED_COMPONENT_PERMISSIONS),
        },
        "canonical_artifact_manifest_sha256",
    )
    component_path = _write_json(tmp_path / "component_artifact_manifest.json", component_manifest)
    monkeypatch.setattr(
        subject,
        "FORMAL_V24_BUY_COMPONENT_CANONICAL_SHA256",
        component_manifest["canonical_artifact_manifest_sha256"],
    )
    monkeypatch.setattr(
        subject,
        "FORMAL_V24_EXECUTION_MANIFEST_SHA256",
        component_manifest["source_execution_manifest_sha256"],
    )

    policy = _document(
        {
            "schema_version": "test.policy.v1",
            "identity": subject.IDENTITY,
            "status": "owner_refit_frozen_not_self_confirmed",
        },
        "canonical_sha256",
    )
    policy_path = _write_json(tmp_path / "artifact" / "policy.json", policy)
    selected_bundle = _document(
        {
            "schema_version": "test.selected_bundle.v1",
            "identity": subject.IDENTITY,
            "status": "selected_predicates_frozen",
        },
        "canonical_sha256",
    )
    selected_bundle_path = _write_json(
        tmp_path / "artifact" / "predicate_bundle.json", selected_bundle
    )
    artifact_manifest = _document(
        {
            "schema_version": refit.SCHEMA_VERSION,
            "identity": subject.IDENTITY,
            "status": "exact_buy_e3_artifact_frozen",
            "policy_file": "policy.json",
            "policy_file_sha256": subject._file_sha256(policy_path),
            "predicate_bundle_file": "predicate_bundle.json",
            "predicate_bundle_file_sha256": subject._file_sha256(selected_bundle_path),
            "training_days": list(days),
        },
        "artifact_sha256",
    )
    artifact_manifest_path = _write_json(
        tmp_path / "artifact" / "artifact_manifest.json", artifact_manifest
    )
    artifact = parity_v1.LoadedExactArtifact(
        manifest_path=artifact_manifest_path,
        policy_path=policy_path,
        predicate_bundle_path=selected_bundle_path,
        manifest_file_sha256=subject._file_sha256(artifact_manifest_path),
        policy_file_sha256=subject._file_sha256(policy_path),
        predicate_bundle_file_sha256=subject._file_sha256(selected_bundle_path),
        artifact_sha256=artifact_manifest["artifact_sha256"],
        manifest=artifact_manifest,
        policy_document=policy,
        predicate_bundle_document=selected_bundle,
        policy=None,  # type: ignore[arg-type]
        runtime=None,  # type: ignore[arg-type]
    )

    source_bundle_document = _document(
        {
            "schema_version": "test.source_bundle.v1",
            "identity": "test_source_predicate_bundle",
            "reference_days": ["2025-01-01"],
        },
        "canonical_sha256",
    )
    source_bundle_path = _write_json(
        tmp_path / "source" / "predicate_bundle.json", source_bundle_document
    )
    source_bundle = predicate_view.FrozenPredicateBundle(
        path=source_bundle_path,
        file_sha256=subject._file_sha256(source_bundle_path),
        canonical_sha256=source_bundle_document["canonical_sha256"],
        artifacts=MappingProxyType({}),
        artifact_file_sha256=MappingProxyType({}),
    )

    opportunity_ids = [f"opportunity-{index:02d}" for index in range(len(days))]
    sides = ["BUY" if index % 2 == 0 else "SELL" for index in range(len(days))]
    raw_frames = {
        "metadata": pd.DataFrame(
            {
                "opportunity_id": opportunity_ids,
                "utc_day": days,
                "side": sides,
            }
        ),
        "boolean_features": pd.DataFrame(
            {
                "opportunity_id": opportunity_ids,
                "utc_day": days,
                "side": sides,
                "primitive::ready": [index % 2 for index in range(len(days))],
            }
        ),
        "continuous_features": pd.DataFrame(
            {
                "opportunity_id": opportunity_ids,
                "utc_day": days,
                "side": sides,
                "continuous::value": [float(index) for index in range(len(days))],
            }
        ),
        "exact_owner_actions": pd.DataFrame(
            {
                "opportunity_id": opportunity_ids,
                "utc_day": days,
                "side": sides,
                "exact_owner_action": ["CONTROL_85N"] * len(days),
            }
        ),
        "replay_inputs": pd.DataFrame(
            {
                "opportunity_id": opportunity_ids,
                "utc_day": days,
                "side": sides,
                "day_input_sha256": [_sha(f"day-input:{day}") for day in days],
            }
        ),
    }
    panel_files: dict[str, dict[str, Any]] = {}
    mechanics_file_sha256: dict[str, str] = {}
    for role, frame in raw_frames.items():
        path = tmp_path / "panel" / f"{role}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        path.chmod(0o644)
        digest = subject._file_sha256(path)
        mechanics_file_sha256[role] = digest
        panel_files[role] = {
            "path": str(path),
            "sha256": digest,
            "size_bytes": path.stat().st_size,
        }

    fold_sha = _sha("fold-manifest")
    nested_fold_sha = _sha("nested-fold-manifest")
    owner_policy_sha = _sha("owner-policy")
    owner_config_sha = _sha("owner-config")
    source_execution = _document(
        {
            "schema_version": "test.source_execution_manifest.v1",
            "identity": "test_source_execution",
            "status": "pre_execution_bound",
            "fold_manifest_sha256": fold_sha,
            "nested_fold_manifest_sha256": nested_fold_sha,
            "permissions": {
                "research_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        },
        "canonical_execution_manifest_sha256",
    )
    source_execution_path = _write_json(
        tmp_path / "source" / "source_execution_manifest.json", source_execution
    )
    source_manifest = _document(
        {
            "schema_version": "test.source_manifest.v1",
            "identity": "test_source_manifest",
            "status": "source_frozen",
            "selected_days": list(days),
            "permissions": {
                "research_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        },
        "canonical_manifest_sha256",
    )
    source_manifest_path = _write_json(
        tmp_path / "source" / "source_manifest.json", source_manifest
    )
    panel_manifest = _document(
        {
            "schema_version": "test.panel_manifest.v1",
            "identity": "test_panel_manifest",
            "status": "mechanics_panel_frozen",
            "selected_days": list(days),
            "files": panel_files,
            "exact_current_owner_policy_sha256": owner_policy_sha,
            "exact_current_predicate_bundle_sha256": source_bundle.file_sha256,
            "exact_current_private_config_sha256": owner_config_sha,
            "economic_outcomes_present": False,
            "permissions": {
                "research_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        },
        "canonical_panel_manifest_sha256",
    )
    panel_manifest_path = _write_json(
        tmp_path / "source" / "panel_manifest.json", panel_manifest
    )

    def owner_file(path: Path) -> dict[str, Any]:
        return {
            "path": str(path),
            "sha256": subject._file_sha256(path),
            "size_bytes": path.stat().st_size,
        }

    owner_manifest = _document(
        {
            "schema_version": refit.EXECUTION_MANIFEST_SCHEMA,
            "identity": subject.IDENTITY,
            "status": "pre_refit_owner_execution_bound",
            "public_base_commit": "a" * 40,
            "annotated_tag": "f05-owner-buy-e3-test-attempt2",
            "fold_manifest_sha256": fold_sha,
            "nested_fold_manifest_sha256": nested_fold_sha,
            "bindings": {
                "source_execution_manifest": owner_file(source_execution_path),
                "source_manifest": owner_file(source_manifest_path),
                "panel_manifest": owner_file(panel_manifest_path),
                "outcome_blind_2025_predicate_bundle": owner_file(source_bundle_path),
            },
            "permissions": {
                "research_authorized": False,
                "action_authorized": False,
                "live_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        },
        "canonical_execution_manifest_sha256",
    )
    owner_path = _write_json(tmp_path / "owner_execution_manifest.json", owner_manifest)
    monkeypatch.setattr(
        subject,
        "ATTEMPT2_EXECUTION_MANIFEST_SHA256",
        owner_manifest["canonical_execution_manifest_sha256"],
    )
    monkeypatch.setattr(subject, "ATTEMPT2_EXECUTION_COMMIT", owner_manifest["public_base_commit"])
    monkeypatch.setattr(subject, "ATTEMPT2_EXECUTION_TAG", owner_manifest["annotated_tag"])

    indexed = {
        role: repeated_backend._index_panel_table(
            frame,
            role=role,
            selected_days=days,
        )
        for role, frame in raw_frames.items()
    }
    primitive_boolean = indexed["boolean_features"].loc[:, ["primitive::ready"]]
    expanded_boolean = primitive_boolean.astype("int8")
    metadata = indexed["metadata"]
    continuous = indexed["continuous_features"].loc[:, ["continuous::value"]]
    owner_actions = indexed["exact_owner_actions"]["exact_owner_action"].astype(str)
    replay_inputs = pd.DataFrame(
        {
            "opportunity_id": opportunity_ids,
            "utc_day": days,
            "side": sides,
            "day_input_sha256": [_sha(f"day-input:{day}") for day in days],
        },
        index=metadata.index,
    )
    predicate_view_receipt = {
        "identity": "test_preexpanded_predicate_view",
        "economic_outcomes_read": False,
    }
    formal_bindings = repeated_backend.FormalExecutionBindings(
        execution_manifest_sha256=owner_manifest[
            "canonical_execution_manifest_sha256"
        ],
        source_manifest_sha256=source_manifest["canonical_manifest_sha256"],
        panel_manifest_sha256=panel_manifest["canonical_panel_manifest_sha256"],
        fold_manifest_sha256=fold_sha,
        nested_fold_manifest_sha256=nested_fold_sha,
        exact_owner_policy_sha256=owner_policy_sha,
        exact_owner_predicate_bundle_sha256=source_bundle.file_sha256,
        exact_owner_private_config_sha256=owner_config_sha,
    )
    mechanics = SimpleNamespace(
        panel=SimpleNamespace(
            metadata=metadata,
            boolean_features=expanded_boolean,
            continuous_features=continuous,
            exact_owner_actions=owner_actions,
        ),
        selected_days=days,
        bindings=formal_bindings,
        file_sha256=mechanics_file_sha256,
        predicate_view_receipt=predicate_view_receipt,
        replay_inputs=replay_inputs,
    )
    mechanics_body = {
        "schema_version": (
            f"{repeated_backend.IDENTITY}.outcome_blind_mechanics_receipt.v1"
        ),
        "selected_days": list(days),
        "file_sha256": mechanics_file_sha256,
        "metadata_sha256": repeated_backend._frame_sha256(metadata),
        "boolean_features_sha256": repeated_backend._frame_sha256(expanded_boolean),
        "primitive_boolean_features_sha256": repeated_backend._frame_sha256(
            primitive_boolean
        ),
        "continuous_features_sha256": repeated_backend._frame_sha256(continuous),
        "exact_owner_actions_sha256": repeated_backend._frame_sha256(owner_actions),
        "replay_inputs_sha256": repeated_backend._frame_sha256(replay_inputs),
        "predicate_view_receipt": predicate_view_receipt,
        "bindings": formal_bindings.payload(),
        "economic_outcomes_present": False,
    }
    mechanics.mechanics_receipt_sha256 = subject._canonical_sha256(mechanics_body)
    mechanics_receipt_path = tmp_path / "mechanics_identity_receipt.json"
    subject.materialize_mechanics_identity_receipt(
        output_path=mechanics_receipt_path,
        owner_execution_manifest_path=owner_path,
        mechanics=mechanics,
    )
    return EvidenceFixture(
        root=tmp_path,
        days=days,
        component_manifest_path=component_path,
        component_manifest=component_manifest,
        owner_manifest_path=owner_path,
        artifact=artifact,
        source_bundle=source_bundle,
        mechanics=mechanics,
        mechanics_receipt_path=mechanics_receipt_path,
    )


def _fake_lockstep_day(**kwargs: Any) -> dict[str, Any]:
    day = kwargs["utc_day"]
    return {
        "summary_signature_sha256": _sha(f"summary:{day}"),
        "campaign_frame_sha256": _sha(f"campaign:{day}"),
        "fill_frame_sha256": _sha(f"fill:{day}"),
        "decision_frame_sha256": _sha(f"decision:{day}"),
        "decision_count": 1,
        "campaign_count": 1,
        "fill_count": 1,
        "mismatch_count": 0,
    }


def _install_fake_day_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parity_v1, "_run_lockstep_day", _fake_lockstep_day)
    monkeypatch.setattr(
        parity_v1.replay_adapter,
        "_resolve_execution_options",
        lambda _rows: SimpleNamespace(binding={}),
    )


def _run_layer4(
    evidence: EvidenceFixture,
    monkeypatch: pytest.MonkeyPatch,
    *,
    contract_path: Path,
    day_root: Path,
    output_path: Path,
    mechanics: SimpleNamespace | None = None,
) -> dict[str, Any]:
    _install_fake_day_runner(monkeypatch)
    return dict(
        subject.run_repeated_policy_lockstep_parity_v2(
            evidence.artifact,
            mechanics=mechanics or evidence.mechanics,
            source_predicate_bundle=evidence.source_bundle,
            contract_path=contract_path,
            day_receipt_dir=day_root,
            output_path=output_path,
        )
    )


def test_contract_derives_formal_learning_algorithm_sha(evidence: EvidenceFixture) -> None:
    contract_path = evidence.root / "contract.json"
    contract = evidence.freeze_contract(contract_path)
    formal = contract["formal_learning_algorithm"]

    assert contract["identity"] == refit.IDENTITY
    assert contract["schema_amendment"] == subject.SCHEMA_AMENDMENT
    assert contract["learning_algorithm_artifact_sha256"] == evidence.component_manifest[
        "canonical_artifact_manifest_sha256"
    ]
    assert contract["learning_algorithm_artifact_sha256"] != formal["manifest"][
        "file_sha256"
    ]
    assert contract["learning_algorithm_artifact_sha256"] != formal[
        "component_result_canonical_sha256"
    ]
    assert contract["learning_algorithm_artifact_sha256"] != formal[
        "nested_oof_artifact_manifest_canonical_sha256"
    ]
    assert os.stat(contract_path).st_mode & 0o777 == 0o600
    assert subject.validate_layer4_lockstep_contract(contract_path) == contract


def test_mechanics_identity_receipt_materializes_exact_canonical_body(
    evidence: EvidenceFixture,
) -> None:
    receipt = subject.validate_mechanics_identity_receipt(
        evidence.mechanics_receipt_path,
        mechanics=evidence.mechanics,
        expected_owner_execution_manifest_path=evidence.owner_manifest_path,
    )
    assert (evidence.mechanics_receipt_path.stat().st_mode & 0o777) == 0o600
    assert receipt["economic_outcomes_present"] is False
    assert receipt["mechanics_body"]["economic_outcomes_present"] is False
    assert receipt["mechanics_receipt_sha256"] == subject._canonical_sha256(
        receipt["mechanics_body"]
    )
    assert receipt["mechanics_receipt_sha256"] == (
        evidence.mechanics.mechanics_receipt_sha256
    )
    assert receipt["source_identity"]["fold_manifest_sha256"] == (
        evidence.mechanics.bindings.fold_manifest_sha256
    )


def test_contract_rejects_tampered_mechanics_receipt_file(
    evidence: EvidenceFixture,
) -> None:
    contract_path = evidence.root / "contract.json"
    evidence.freeze_contract(contract_path)
    receipt = json.loads(evidence.mechanics_receipt_path.read_text(encoding="ascii"))
    receipt["mechanics_body"]["metadata_sha256"] = _sha("tampered-metadata")
    receipt["mechanics_receipt_sha256"] = subject._canonical_sha256(
        receipt["mechanics_body"]
    )
    receipt["canonical_mechanics_identity_receipt_sha256"] = subject._document_sha256(
        receipt, "canonical_mechanics_identity_receipt_sha256"
    )
    _write_json(evidence.mechanics_receipt_path, receipt)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="binding drifted",
    ):
        subject.validate_layer4_lockstep_contract(contract_path)


def test_contract_rejects_missing_or_nonprivate_mechanics_receipt(
    evidence: EvidenceFixture,
) -> None:
    contract_path = evidence.root / "contract.json"
    evidence.freeze_contract(contract_path)
    evidence.mechanics_receipt_path.unlink()
    with pytest.raises(subject.OwnerBuyE3ParityAmendmentError, match="missing"):
        subject.validate_layer4_lockstep_contract(contract_path)

    receipt = subject.materialize_mechanics_identity_receipt(
        output_path=evidence.mechanics_receipt_path,
        owner_execution_manifest_path=evidence.owner_manifest_path,
        mechanics=evidence.mechanics,
    )
    assert receipt["status"] == "outcome_blind_mechanics_identity_materialized"
    evidence.mechanics_receipt_path.chmod(0o644)
    with pytest.raises(subject.OwnerBuyE3ParityAmendmentError, match="mode is not 0600"):
        subject.validate_layer4_lockstep_contract(contract_path)


def test_mechanics_receipt_rejects_cross_execution_substitution(
    evidence: EvidenceFixture,
) -> None:
    other_owner = json.loads(evidence.owner_manifest_path.read_text(encoding="ascii"))
    other_owner["public_base_commit"] = "b" * 40
    other_owner["canonical_execution_manifest_sha256"] = subject._document_sha256(
        other_owner, "canonical_execution_manifest_sha256"
    )
    other_path = _write_json(evidence.root / "other-owner.json", other_owner)
    receipt = json.loads(evidence.mechanics_receipt_path.read_text(encoding="ascii"))
    receipt["owner_execution_attempt"]["manifest"] = subject._file_binding(
        other_path, label="other owner"
    )
    receipt["owner_execution_attempt"]["canonical_execution_manifest_sha256"] = (
        other_owner["canonical_execution_manifest_sha256"]
    )
    receipt["owner_execution_attempt"]["execution_commit"] = "b" * 40
    receipt["canonical_mechanics_identity_receipt_sha256"] = subject._document_sha256(
        receipt, "canonical_mechanics_identity_receipt_sha256"
    )
    substituted = _write_json(evidence.root / "cross-execution-mechanics.json", receipt)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="owner attempt2 execution identity drifted",
    ):
        subject.validate_mechanics_identity_receipt(substituted)


def test_legacy_bare_mechanics_sha_contract_is_rejected(
    evidence: EvidenceFixture,
) -> None:
    contract_path = evidence.root / "contract.json"
    contract = evidence.freeze_contract(contract_path)
    mechanics_sha = contract.pop("mechanics_identity_receipt")[
        "mechanics_receipt_sha256"
    ]
    contract["mechanics_receipt_sha256"] = mechanics_sha
    contract["canonical_contract_sha256"] = subject._document_sha256(
        contract, "canonical_contract_sha256"
    )
    legacy = _write_json(evidence.root / "legacy-contract.json", contract)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="legacy bare mechanics SHA",
    ):
        subject.validate_layer4_lockstep_contract(legacy)


@pytest.mark.parametrize(
    "wrong_role",
    (
        "manifest_file",
        "component_result",
        "nested_oof_manifest",
        "v24_execution_manifest",
    ),
)
def test_contract_rejects_wrong_learning_algorithm_sha_roles(
    evidence: EvidenceFixture,
    wrong_role: str,
) -> None:
    original_path = evidence.root / "contract.json"
    contract = evidence.freeze_contract(original_path)
    formal = contract["formal_learning_algorithm"]
    replacements = {
        "manifest_file": formal["manifest"]["file_sha256"],
        "component_result": formal["component_result_canonical_sha256"],
        "nested_oof_manifest": formal[
            "nested_oof_artifact_manifest_canonical_sha256"
        ],
        "v24_execution_manifest": formal["formal_v24_execution_manifest_sha256"],
    }
    contract["learning_algorithm_artifact_sha256"] = replacements[wrong_role]
    contract["canonical_contract_sha256"] = subject._document_sha256(
        contract, "canonical_contract_sha256"
    )
    mutated = _write_json(evidence.root / f"wrong-{wrong_role}.json", contract)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="learning algorithm SHA role",
    ):
        subject.validate_layer4_lockstep_contract(mutated)


@pytest.mark.parametrize("day_variant", ("missing", "reordered"))
def test_contract_rejects_missing_or_reordered_development_days(
    evidence: EvidenceFixture,
    day_variant: str,
) -> None:
    days = evidence.days[:-1] if day_variant == "missing" else tuple(reversed(evidence.days))
    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="ordered Development day",
    ):
        evidence.freeze_contract(evidence.root / f"{day_variant}.json", days=days)


def test_v1_day_receipt_is_never_reused(
    evidence: EvidenceFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_path = evidence.root / "contract.json"
    evidence.freeze_contract(contract_path)
    day_root = evidence.root / "days"
    _write_json(
        day_root / f"{evidence.days[0]}.json",
        {
            "schema_version": parity_v1.LOCKSTEP_DAY_SCHEMA,
            "identity": refit.IDENTITY,
            "status": "day_lockstep_complete",
        },
    )

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="v1 Layer-4 day receipts are never reusable",
    ):
        _run_layer4(
            evidence,
            monkeypatch,
            contract_path=contract_path,
            day_root=day_root,
            output_path=evidence.root / "layer4.json",
        )


def test_resume_rejects_changed_contract(
    evidence: EvidenceFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_a = evidence.root / "contract-a.json"
    evidence.freeze_contract(contract_a)
    day_root = evidence.root / "days"
    _run_layer4(
        evidence,
        monkeypatch,
        contract_path=contract_a,
        day_root=day_root,
        output_path=evidence.root / "layer4-a.json",
    )

    mechanics_b_receipt = evidence.root / "mechanics-identity-b.json"
    mechanics_b_receipt.write_bytes(evidence.mechanics_receipt_path.read_bytes())
    mechanics_b_receipt.chmod(0o600)
    contract_b = evidence.root / "contract-b.json"
    evidence.freeze_contract(
        contract_b,
        mechanics_receipt_path=mechanics_b_receipt,
    )
    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="day receipt drifted",
    ):
        _run_layer4(
            evidence,
            monkeypatch,
            contract_path=contract_b,
            day_root=day_root,
            output_path=evidence.root / "layer4-b.json",
            mechanics=evidence.mechanics,
        )


def test_atomic_contract_write_never_overwrites(evidence: EvidenceFixture) -> None:
    contract_path = evidence.root / "contract.json"
    evidence.freeze_contract(contract_path)
    before = contract_path.read_bytes()

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="already exists",
    ):
        evidence.freeze_contract(contract_path)

    assert contract_path.read_bytes() == before
    assert not tuple(contract_path.parent.glob(f".{contract_path.name}.*"))


def test_all_v2_receipts_bind_false_evidence_boundaries(
    evidence: EvidenceFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_path = evidence.root / "contract.json"
    contract = evidence.freeze_contract(contract_path)
    day_root = evidence.root / "days"
    final = _run_layer4(
        evidence,
        monkeypatch,
        contract_path=contract_path,
        day_root=day_root,
        output_path=evidence.root / "layer4.json",
    )

    assert contract["evidence_boundary"] == subject._BOUNDARY
    assert final["evidence_boundary"] == subject._BOUNDARY
    assert final["learning_algorithm_artifact_sha256"] == contract[
        "learning_algorithm_artifact_sha256"
    ]
    assert final["mechanics_identity_receipt"] == contract[
        "mechanics_identity_receipt"
    ]
    for day in evidence.days:
        receipt = json.loads((day_root / f"{day}.json").read_text(encoding="ascii"))
        assert receipt["schema_version"] == subject.LOCKSTEP_DAY_SCHEMA_V2
        assert receipt["layer4_lockstep_contract_sha256"] == contract[
            "canonical_contract_sha256"
        ]
        assert receipt["learning_algorithm_artifact_sha256"] == contract[
            "learning_algorithm_artifact_sha256"
        ]
        assert receipt["mechanics_identity_receipt"] == contract[
            "mechanics_identity_receipt"
        ]
        assert receipt["evidence_boundary"] == subject._BOUNDARY


def test_contract_rejects_boundary_escalation(evidence: EvidenceFixture) -> None:
    contract_path = evidence.root / "contract.json"
    contract = evidence.freeze_contract(contract_path)
    contract["evidence_boundary"]["validation_read"] = True
    contract["canonical_contract_sha256"] = subject._document_sha256(
        contract, "canonical_contract_sha256"
    )
    mutated = _write_json(evidence.root / "boundary-drift.json", contract)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="contract identity drifted",
    ):
        subject.validate_layer4_lockstep_contract(mutated)


def test_final_receipt_rejects_reordered_or_missing_day_evidence(
    evidence: EvidenceFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_path = evidence.root / "contract.json"
    evidence.freeze_contract(contract_path)
    day_root = evidence.root / "days"
    final_path = evidence.root / "layer4.json"
    final = _run_layer4(
        evidence,
        monkeypatch,
        contract_path=contract_path,
        day_root=day_root,
        output_path=final_path,
    )
    final["evidence"]["day_receipts"] = list(
        reversed(final["evidence"]["day_receipts"])
    )
    final["evidence"]["day_receipts_sha256"] = subject._canonical_sha256(
        final["evidence"]["day_receipts"]
    )
    final["canonical_receipt_sha256"] = subject._document_sha256(
        final, "canonical_receipt_sha256"
    )
    reordered = _write_json(evidence.root / "layer4-reordered.json", final)

    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="day receipt order drifted",
    ):
        subject.validate_layer4_receipt_v2(
            reordered,
            contract_path=contract_path,
            day_receipt_dir=day_root,
        )

    (day_root / f"{evidence.days[-1]}.json").unlink()
    with pytest.raises(
        subject.OwnerBuyE3ParityAmendmentError,
        match="day receipt set is incomplete",
    ):
        subject.validate_layer4_receipt_v2(
            final_path,
            contract_path=contract_path,
            day_receipt_dir=day_root,
        )


@pytest.mark.parametrize(
    "bad_text",
    (
        '{"schema_version":"x","schema_version":"y"}\n',
        '{"schema_version":NaN}\n',
    ),
)
def test_strict_json_rejects_duplicate_keys_and_nan(
    evidence: EvidenceFixture,
    bad_text: str,
) -> None:
    path = evidence.root / "bad.json"
    path.write_text(bad_text, encoding="ascii")
    path.chmod(0o600)
    with pytest.raises(subject.OwnerBuyE3ParityAmendmentError):
        subject.validate_layer4_lockstep_contract(path)
