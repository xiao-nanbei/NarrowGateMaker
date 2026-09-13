from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path

import pytest

from narrowgate import replay_demo as demo
from narrowgate.cli import main as narrowgate_main

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = demo.FIXTURE_ROOT


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_public_replay_demo_is_exposed_through_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    return_code = narrowgate_main(
        [
            "replay-demo",
            "--output-dir",
            str(tmp_path / "cli"),
            "--verify-reference",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert return_code == 0
    assert payload["gate_status"] == "passed_demo_mechanics_only"
    assert payload["reference_verified"] is True
    assert Path(payload["summary"]).is_file()


def test_public_replay_demo_matches_reference_byte_for_byte(tmp_path: Path) -> None:
    first = demo.run_demo(output_dir=tmp_path / "first", verify_reference=True)
    demo.run_demo(output_dir=tmp_path / "second", verify_reference=True)

    for filename in ("summary.json", "trace.jsonl", "receipt.json"):
        first_bytes = (tmp_path / "first" / filename).read_bytes()
        assert first_bytes == (tmp_path / "second" / filename).read_bytes()
        assert first_bytes == (FIXTURE_ROOT / "reference" / filename).read_bytes()

    summary = first.summary
    receipt = first.receipt
    assert summary["identity"]["tape"]["sha256"] == demo.sha256_file(
        FIXTURE_ROOT / "synthetic_tape.jsonl"
    )
    code_identity = summary["identity"]["code"]
    assert code_identity["version"] == demo.ENGINE_VERSION
    assert code_identity["identity_kind"] == "package_distribution"
    assert code_identity["package_version"] == "0.1.2.dev0"
    assert code_identity["exact_bytes"] == "external_wheel_digest_or_git_commit_tree"
    assert "sources" not in code_identity
    assert "bundle_sha256" not in code_identity
    assert summary["identity"]["contract"]["version"] == demo.CONTRACT_VERSION
    assert summary["denominators"] == _read_json(FIXTURE_ROOT / "contract.json")[
        "expected_denominators"
    ]
    assert summary["campaign_terminal_value_usdc"] == "0.00200000"
    assert summary["gate"]["status"] == "passed_demo_mechanics_only"
    assert summary["gate"]["passed"] is True
    assert summary["gate"]["promotion_eligible"] is False

    assert receipt["identity"] == summary["identity"]
    assert receipt["denominators"] == summary["denominators"]
    assert receipt["campaign_terminal_value_usdc"] == "0.00200000"
    assert receipt["gate"]["status"] == "passed_demo_mechanics_only"
    assert receipt["artifacts"]["summary"]["sha256"] == demo.sha256_file(
        first.summary_path
    )
    assert receipt["artifacts"]["trace"]["sha256"] == demo.sha256_file(first.trace_path)
    assert summary["canonical_summary_sha256"] == demo.canonical_document_sha256(
        summary, "canonical_summary_sha256"
    )
    assert receipt["canonical_receipt_sha256"] == demo.canonical_document_sha256(
        receipt, "canonical_receipt_sha256"
    )


def test_public_replay_demo_has_no_network_or_external_order_surface(tmp_path: Path) -> None:
    imported_roots: set[str] = set()
    for relative_path in demo.CODE_SOURCE_PATHS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots.update(
            node.names[0].name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import) and node.names
        )
        imported_roots.update(
            node.module.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
    assert imported_roots.isdisjoint(
        {"aiohttp", "execution", "httpx", "live", "requests", "socket", "urllib", "websocket"}
    )

    result = demo.run_demo(output_dir=tmp_path / "offline")
    assert result.summary["permissions"] == {
        "external_order_submission": False,
        "live_runtime_import": False,
        "network_access": False,
        "private_evidence_read": False,
        "runtime_clock_read": False,
    }
    assert result.summary["classification"]["economic_authority"] == "none"
    assert result.summary["frozen_generated_at_utc"] == "2026-08-23T00:00:00Z"
    assert result.summary["gate"]["economic_evidence_eligible"] is False
    assert result.summary["gate"]["live_action_eligible"] is False


@pytest.mark.parametrize("input_authority_claims", [False, True])
def test_demo_authority_is_fixed_by_program_not_contract(tmp_path, input_authority_claims):
    fixture = tmp_path / "fixture"
    shutil.copytree(FIXTURE_ROOT, fixture)
    contract_path = fixture / "contract.json"
    contract = _read_json(contract_path)
    assert {"fixture", "permissions", "gate_contract"}.isdisjoint(contract)
    if input_authority_claims:
        contract.update({
            "fixture": {"classification": "live", "economic_authority": "granted"},
            "permissions": {key: True for key in demo.DEMO_PERMISSIONS},
            "gate_contract": {
                **dict.fromkeys(demo.DEMO_AUTHORITY, True),
                "status_on_pass": "live_economic_evidence",
            },
        })
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    result = demo.run_demo(output_dir=tmp_path / "output", contract_path=contract_path)
    reference = _read_json(FIXTURE_ROOT / "reference" / "summary.json")
    assert result.summary["classification"] == demo.DEMO_CLASSIFICATION
    assert result.summary["permissions"] == demo.DEMO_PERMISSIONS
    assert result.receipt["permissions"] == demo.DEMO_PERMISSIONS
    for document in (result.summary, result.receipt):
        assert document["gate"]["status"] == "passed_demo_mechanics_only"
        assert all(document["gate"][key] is False for key in demo.DEMO_AUTHORITY)
    assert {row["check"] for row in result.summary["gate"]["checks"]}.isdisjoint({
        "offline_permissions_locked", "synthetic_non_economic_boundary",
    })
    assert result.summary["terminal"] == reference["terminal"]
    assert result.summary["denominators"] == reference["denominators"]
    assert result.trace_path.read_bytes() == (
        FIXTURE_ROOT / "reference" / "trace.jsonl"
    ).read_bytes()


def test_demo_reuses_the_tape_admission_digest(tmp_path, monkeypatch):
    observed_paths = []
    original_sha256_file = demo.sha256_file

    def count_digest(path):
        observed_paths.append(Path(path).resolve())
        return original_sha256_file(path)

    monkeypatch.setattr(demo, "sha256_file", count_digest)
    result = demo.run_demo(output_dir=tmp_path / "output")
    tape_path = (FIXTURE_ROOT / "synthetic_tape.jsonl").resolve()
    assert observed_paths.count(tape_path) == 1
    assert result.summary["identity"]["tape"]["sha256"] == original_sha256_file(tape_path)


@pytest.mark.parametrize("version_field", ["schema_version", "engine_version"])
def test_demo_still_rejects_unsupported_contract_schema(tmp_path, version_field):
    fixture = tmp_path / "fixture"
    shutil.copytree(FIXTURE_ROOT, fixture)
    contract_path = fixture / "contract.json"
    contract = _read_json(contract_path)
    contract[version_field] = "unsupported"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(demo.DemoAdmissionError):
        demo.run_demo(output_dir=tmp_path / "output", contract_path=contract_path)
    assert not (tmp_path / "output").exists()


def test_public_replay_demo_rejects_tape_tampering_before_output(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture"
    shutil.copytree(FIXTURE_ROOT, fixture)
    tape = fixture / "synthetic_tape.jsonl"
    tape.write_bytes(tape.read_bytes() + b"\n")
    output = tmp_path / "tampered-output"

    with pytest.raises(demo.DemoAdmissionError, match="tape SHA256 mismatch"):
        demo.run_demo(output_dir=output, contract_path=fixture / "contract.json")
    assert not output.exists()


def test_reference_mismatch_fails_before_output_publication(tmp_path: Path) -> None:
    reference = tmp_path / "reference"
    shutil.copytree(FIXTURE_ROOT / "reference", reference)
    summary_path = reference / "summary.json"
    summary_path.write_bytes(summary_path.read_bytes() + b"\n")
    output = tmp_path / "mismatched-output"

    with pytest.raises(demo.DemoAdmissionError, match="reference byte mismatch"):
        demo.run_demo(
            output_dir=output,
            verify_reference=True,
            reference_dir=reference,
        )
    assert not output.exists()


def test_public_replay_demo_gate_fails_closed_on_denominator_drift(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture"
    shutil.copytree(FIXTURE_ROOT, fixture)
    contract_path = fixture / "contract.json"
    contract = _read_json(contract_path)
    contract["expected_denominators"]["orders"]["submitted"] = 4
    contract_path.write_text(
        json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    result = demo.run_demo(output_dir=tmp_path / "failed", contract_path=contract_path)
    assert result.summary["gate"]["status"] == "failed_closed"
    assert result.summary["gate"]["passed"] is False
    assert result.summary["gate"]["promotion_eligible"] is False
    assert "denominators_match_contract" in result.summary["gate"]["failures"]
    assert result.receipt["gate"]["status"] == "failed_closed"
