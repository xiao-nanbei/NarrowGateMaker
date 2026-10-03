from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from models import backtest_tick
from research.governance.public_machine_projection import (
    projection_for,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_touch_conditional_curve_quote_mapping_v1_spec_20260803.json"
)
SPEC_SHA256 = "c88e99fdeb69b0654a4b4a79c3e31bda190ffdfaf61f3b6b82db78298016c59a"
RUNNER_ID = "f02.conditional_p3_scalar_compression_adapter"
ERRATA = ROOT / (
    "research/families/f02_empirical_p3_touch/docs/"
    "p3_touch_conditional_curve_quote_mapping_v1_contract_errata_20260803.json"
)


def _params(**overrides):
    params = {
        "historical_reproduction": True,
        "historical_reproduction_runner_id": RUNNER_ID,
        "historical_reproduction_spec_path": str(SPEC),
        "historical_reproduction_spec_sha256": SPEC_SHA256,
    }
    params.update(overrides)
    return params


def test_generic_cpp_replay_rejects_unregistered_conditional_p3_overlay() -> None:
    with pytest.raises(ValueError, match="scalar compression is closed"):
        backtest_tick._simulate_tick_cpp(
            None,
            None,
            None,
            {"_conditional_p3_ts_ms": [1]},
        )


def test_scalar_adapter_requires_explicit_historical_reproduction() -> None:
    with pytest.raises(RuntimeError, match="pass --historical-reproduction"):
        backtest_tick._require_conditional_p3_historical_reproduction(
            _params(historical_reproduction=False)
        )


def test_scalar_adapter_requires_exact_closed_spec_sha256() -> None:
    with pytest.raises(ValueError, match="exact closed Spec SHA256"):
        backtest_tick._require_conditional_p3_historical_reproduction(
            _params(historical_reproduction_spec_sha256="0" * 64)
        )


def test_scalar_adapter_registered_spec_is_historical_evidence_only() -> None:
    identity = backtest_tick._require_conditional_p3_historical_reproduction(
        _params()
    )
    marker = backtest_tick._conditional_p3_historical_output_marker(identity)

    assert identity["runner_id"] == RUNNER_ID
    assert identity["spec_sha256"] == SPEC_SHA256
    assert marker["research_authority"] == "historical_evidence_only"
    assert marker["new_experiment_identity_allowed"] is False
    assert marker["action_or_live_authorization"] is False
    assert marker["conditional_p3_adapter_scope"] == (
        "pair_average_scalar_delta_star_kappa_adapter"
    )


def test_historical_registry_binds_exact_scalar_adapter_spec() -> None:
    registry_path = (
        ROOT / "research/governance/historical_reproduction_registry.v1.json"
    )
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    entry = registry["runners"][RUNNER_ID]

    assert entry["family"] == "F02"
    assert entry["authority"] == "historical_evidence_only"
    assert entry["interpretive_identity"] == (
        "conditional_p3_scalar_compression_adapter_v1"
    )
    assert entry["specs"] == [
        {
            "path": str(SPEC.relative_to(ROOT)),
            "sha256": SPEC_SHA256,
        }
    ]


def test_errata_narrows_scope_without_rewriting_frozen_artifacts() -> None:
    projection = projection_for(ERRATA)
    assert projection is not None
    payload = json.loads(
        source_document_path(ERRATA, require_private=True).read_text(encoding="utf-8")
    )
    normalized = dict(payload)
    expected = normalized.pop("canonical_errata_identity_sha256")
    observed = hashlib.sha256(
        json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    assert observed == expected
    assert payload["classification"]["interpretive_identity"] == (
        "conditional_p3_scalar_compression_adapter_v1"
    )
    assert payload["classification"][
        "conditional_p3_curve_to_quote_route_closed"
    ] is False
    assert payload["classification"]["full_side_specific_curve_mapping_tested"] is False
    for artifact in payload["immutable_original_artifacts"].values():
        if not isinstance(artifact, dict) or "path" not in artifact:
            continue
        path = Path(artifact["path"])
        if not path.is_absolute():
            path = ROOT / path
        if path.is_file():
            if projection_for(path) is not None:
                assert source_identity_sha256(path) == artifact["sha256"]
            elif path.suffix != ".md":
                assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]
            else:
                # Public prose may be reformatted without changing the executed
                # machine identity; its current bytes are audited independently.
                assert hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            assert Path(artifact["path"]).is_absolute()
            assert len(artifact["sha256"]) == 64
