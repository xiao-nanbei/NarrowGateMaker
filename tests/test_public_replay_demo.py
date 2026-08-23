from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path

import pytest

from narrowgate.cli import main as narrowgate_main
from scripts import narrowgate_replay_demo as demo

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "examples" / "replay_demo"


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
    assert summary["identity"]["code"]["version"] == demo.ENGINE_VERSION
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
    contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = demo.run_demo(output_dir=tmp_path / "failed", contract_path=contract_path)
    assert result.summary["gate"]["status"] == "failed_closed"
    assert result.summary["gate"]["passed"] is False
    assert result.summary["gate"]["promotion_eligible"] is False
    assert "denominators_match_contract" in result.summary["gate"]["failures"]
    assert result.receipt["gate"]["status"] == "failed_closed"
