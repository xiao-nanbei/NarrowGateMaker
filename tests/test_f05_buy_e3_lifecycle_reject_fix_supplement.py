from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import f05_buy_e3_active_release as legacy_release
from scripts import f05_buy_e3_lifecycle_reject_fix_supplement as subject

V4_EXECUTION = {
    "execution_commit": "a" * 40,
    "execution_tree": "b" * 40,
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v4-20260824",
    "annotated_operational_tag_object": "c" * 40,
    "tag_peeled_commit": "a" * 40,
}
BINDING = {"git_blob_sha1": "d" * 40, "file_sha256": "e" * 64}
REGRESSION = {
    "execution_commit": "a" * 40,
    "targets": ["tests/test_example.py"],
    "collect_command": ["python", "-m", "pytest", "--collect-only", "-q"],
    "run_command": ["python", "-m", "pytest", "-q"],
    "collect_returncode": 0,
    "run_returncode": 0,
    "nodeids": ["tests/test_example.py::test_example"],
    "passed": 1,
    "failed": 0,
    "skipped": 0,
    "junit_xml_sha256": "1" * 64,
    "collect_stdout_sha256": "2" * 64,
    "collect_stderr_sha256": "3" * 64,
    "run_stdout_sha256": "4" * 64,
    "run_stderr_sha256": "5" * 64,
    "interpreter": {},
    "sanitized_python_environment": {},
    "safe_import_files": dict(subject.SAFE_IMPORT_MODULES),
    "safe_import_stdout_sha256": "7" * 64,
    "test_source_files": {},
    "collector_source": dict(BINDING),
    "canonical_regression_sha256": "6" * 64,
}


def _patch_git_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        legacy_release,
        "_operational_git_identity",
        lambda *_args, **_kwargs: dict(V4_EXECUTION),
    )
    monkeypatch.setattr(
        subject,
        "_changed_repository_files",
        lambda *_args, **_kwargs: {
            path: dict(BINDING)
            for path in sorted(subject.EXPECTED_CHANGED_REPOSITORY_FILES)
        },
    )
    monkeypatch.setattr(
        subject,
        "_unchanged_critical_files",
        lambda *_args, **_kwargs: {
            path: dict(BINDING)
            for path in subject.CRITICAL_UNCHANGED_REPOSITORY_FILES
        },
    )
    monkeypatch.setattr(
        subject,
        "_unchanged_decision_ast",
        lambda *_args, **_kwargs: {
            name: "f" * 64 for name in subject.BUY_E3_DECISION_AST_NODES
        },
    )
    monkeypatch.setattr(
        subject,
        "_collect_regression",
        lambda *_args, **_kwargs: dict(REGRESSION),
    )


def test_builder_freezes_exact_non_economic_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_git_evidence(monkeypatch)

    payload = subject.build_supplement(
        repository_root=tmp_path,
        annotated_operational_tag=V4_EXECUTION["annotated_operational_tag"],
        generated_utc="2026-08-24T00:00:00Z",
    )

    assert set(payload) == set(subject.TOP_LEVEL_FIELDS)
    assert set(payload["changed_runtime_files"]) == set(subject.RUNTIME_CHANGED_FILES)
    assert set(payload["changed_repository_files"]) == set(
        subject.EXPECTED_CHANGED_REPOSITORY_FILES
    )
    assert payload["permissions"] == subject.PERMISSIONS
    assert not any(payload["permissions"].values())
    assert payload["e3_unchanged"]["verified"] is True
    assert payload[subject.CANONICAL_FIELD] == legacy_release.document_sha256(
        payload,
        subject.CANONICAL_FIELD,
    )


def test_validator_recomputes_all_git_and_semantic_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_git_evidence(monkeypatch)
    payload = subject.build_supplement(
        repository_root=tmp_path,
        annotated_operational_tag=V4_EXECUTION["annotated_operational_tag"],
        generated_utc="2026-08-24T00:00:00Z",
    )
    receipt = tmp_path / "supplement.json"
    receipt.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    receipt.chmod(0o600)
    monkeypatch.setattr(
        subject,
        "_validate_regression",
        lambda value, **_kwargs: dict(value),
    )

    assert subject.validate_supplement(receipt, repository_root=tmp_path) == payload
    binding = subject.supplement_binding(receipt, repository_root=tmp_path)
    assert set(binding) == {
        "schema_version",
        "status",
        "file_sha256",
        "canonical_field",
        "canonical_sha256",
        "size_bytes",
        "mode",
    }
    assert binding["canonical_sha256"] == payload[subject.CANONICAL_FIELD]


def test_junit_failure_count_is_not_reported_as_passed() -> None:
    raw = (
        b'<testsuites><testsuite tests="2" failures="1" errors="0" skipped="0"/>'
        b"</testsuites>"
    )

    assert subject._junit_counts(raw) == (1, 1, 0)  # noqa: SLF001


def test_current_buy_e3_decision_ast_matches_direct_v3() -> None:
    root = Path(__file__).resolve().parents[1]
    strategy_path = "strategy/boolean_cooldown_buy_e3.py"
    parent = subject._semantic_ast_hashes(  # noqa: SLF001
        subject._git_bytes(  # noqa: SLF001
            root,
            "show",
            f"{subject.PARENT_EXECUTION['execution_commit']}:{strategy_path}",
        )
    )
    current = subject._semantic_ast_hashes(  # noqa: SLF001
        (root / strategy_path).read_bytes()
    )

    assert current == parent
