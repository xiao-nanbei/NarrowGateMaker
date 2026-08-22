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

    def freeze_contract(
        self,
        path: Path,
        *,
        days: tuple[str, ...] | None = None,
        mechanics_sha256: str | None = None,
    ) -> dict[str, Any]:
        return dict(
            subject.freeze_layer4_lockstep_contract(
                output_path=path,
                formal_buy_component_artifact_manifest_path=self.component_manifest_path,
                owner_execution_manifest_path=self.owner_manifest_path,
                artifact=self.artifact,
                mechanics_receipt_sha256=(
                    mechanics_sha256 or self.mechanics.mechanics_receipt_sha256
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

    owner_manifest = _document(
        {
            "schema_version": refit.EXECUTION_MANIFEST_SCHEMA,
            "identity": subject.IDENTITY,
            "status": "pre_refit_owner_execution_bound",
            "public_base_commit": "a" * 40,
            "annotated_tag": "f05-owner-buy-e3-test-attempt2",
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

    replay_inputs = pd.DataFrame(
        {
            "utc_day": days,
            "day_input_sha256": [_sha(f"day-input:{day}") for day in days],
        }
    )
    mechanics = SimpleNamespace(
        selected_days=days,
        mechanics_receipt_sha256=_sha("mechanics-a"),
        replay_inputs=replay_inputs,
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

    mechanics_b_sha = _sha("mechanics-b")
    contract_b = evidence.root / "contract-b.json"
    evidence.freeze_contract(contract_b, mechanics_sha256=mechanics_b_sha)
    mechanics_b = SimpleNamespace(
        selected_days=evidence.days,
        mechanics_receipt_sha256=mechanics_b_sha,
        replay_inputs=evidence.mechanics.replay_inputs,
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
            mechanics=mechanics_b,
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
    for day in evidence.days:
        receipt = json.loads((day_root / f"{day}.json").read_text(encoding="ascii"))
        assert receipt["schema_version"] == subject.LOCKSTEP_DAY_SCHEMA_V2
        assert receipt["layer4_lockstep_contract_sha256"] == contract[
            "canonical_contract_sha256"
        ]
        assert receipt["learning_algorithm_artifact_sha256"] == contract[
            "learning_algorithm_artifact_sha256"
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
