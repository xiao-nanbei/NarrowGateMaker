from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from models.backtest_config import (
    load_operational_baseline_binding,
    resolve_backtest_config_path,
)
from research.governance.public_machine_projection import projection_for

ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_binding(root: Path) -> tuple[Path, Path]:
    config = root / "docs" / "private" / "live_config.current.local.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("ml:\n  enabled: true\n", encoding="utf-8")

    model = root / "models" / "v12"
    model.mkdir(parents=True)
    bundle_meta = model / "bundle_meta.json"
    training_summary = model / "training_summary.json"
    p3 = model / "fill_prob_params.json"
    bundle_meta.write_text("{}\n", encoding="utf-8")
    training_summary.write_text("{}\n", encoding="utf-8")
    p3.write_text("{}\n", encoding="utf-8")

    identity = (
        root
        / "research"
        / "families"
        / "f10_live_replay_attribution"
        / "docs"
        / "operational_baseline_identity.json"
    )
    identity.parent.mkdir(parents=True)
    identity.write_text(
        json.dumps(
            {
                "baseline_id": "baseline-v12",
                "config": {"sha256": _sha256(config)},
                "model": {
                    "directory": str(model.relative_to(root)),
                    "bundle_meta_sha256": _sha256(bundle_meta),
                    "training_summary_sha256": _sha256(training_summary),
                },
                "p3": {
                    "path": str(p3.relative_to(root)),
                    "sha256": _sha256(p3),
                },
                "permissions": {
                    "operational_baseline_active": True,
                    "baseline_promotion_authorized": True,
                    "backtest_default_control_authorized": True,
                },
            }
        ),
        encoding="utf-8",
    )

    pointer = identity.with_name("operational_baseline_current.json")
    pointer.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_operational_baseline_pointer.v1",
                "baseline_id": "baseline-v12",
                "identity_path": str(identity.relative_to(root)),
                "identity_sha256": _sha256(identity),
                "live_config_path": str(config.relative_to(root)),
                "live_config_sha256": _sha256(config),
                "backtest_control_arm": "v12_ml_on",
                "model_directory": str(model.relative_to(root)),
                "bundle_meta_sha256": _sha256(bundle_meta),
                "backtest_default_control_authorized": True,
            }
        ),
        encoding="utf-8",
    )
    return pointer, config


def test_current_baseline_pointer_resolves_hash_bound_config(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)

    binding = load_operational_baseline_binding(
        root=tmp_path,
        pointer_path=pointer,
    )

    assert binding is not None
    assert binding["config_exists"] is True
    assert binding["pointer"]["backtest_control_arm"] == "v12_ml_on"
    assert resolve_backtest_config_path(
        root=tmp_path,
        pointer_path=pointer,
    ) == config.resolve()


def test_current_baseline_config_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    config.write_text("ml:\n  enabled: false\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="config SHA256 mismatch"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_public_checkout_without_private_config_uses_template(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    config.unlink()
    public = tmp_path / "live" / "config.yaml"
    public.parent.mkdir(parents=True)
    public.write_text("ml:\n  enabled: false\n", encoding="utf-8")

    assert resolve_backtest_config_path(
        root=tmp_path,
        pointer_path=pointer,
    ) == public.resolve()


def test_remote_candidate_with_same_hash_is_current_baseline(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    remote = tmp_path / "live" / "config.yaml"
    remote.parent.mkdir(parents=True)
    remote.write_bytes(config.read_bytes())
    config.unlink()
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    payload["live_config_candidates"] = [
        "docs/private/live_config.current.local.yaml",
        "live/config.yaml",
    ]
    pointer.write_text(json.dumps(payload), encoding="utf-8")

    binding = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)

    assert binding is not None
    assert binding["config_exists"] is True
    assert binding["config_path"] == remote.resolve()


def test_runtime_code_hashes_distinguish_exact_baseline_from_overlay(
    tmp_path: Path,
) -> None:
    pointer, _ = _write_binding(tmp_path)
    runtime = tmp_path / "live" / "main.py"
    runtime.parent.mkdir(parents=True, exist_ok=True)
    runtime.write_text("DEPLOYED = True\n", encoding="utf-8")

    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    identity = tmp_path / pointer_payload["identity_path"]
    identity_payload = json.loads(identity.read_text(encoding="utf-8"))
    identity_payload["runtime_code"] = {
        "deployment_scope": "test",
        "live/main.py": _sha256(runtime),
    }
    identity.write_text(json.dumps(identity_payload), encoding="utf-8")
    pointer_payload["identity_sha256"] = _sha256(identity)
    pointer.write_text(json.dumps(pointer_payload), encoding="utf-8")

    exact = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert exact is not None
    assert exact["runtime_code_audit"]["matches"] is True

    runtime.write_text("DEPLOYED = False\n", encoding="utf-8")
    overlay = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert overlay is not None
    assert overlay["runtime_code_audit"]["matches"] is False
    assert list(overlay["runtime_code_audit"]["mismatched_paths"]) == [
        "live/main.py"
    ]


def test_repository_current_baseline_identity_is_resolver_complete() -> None:
    binding = load_operational_baseline_binding(root=ROOT)

    assert binding is not None
    identity = binding["identity"]
    pointer = binding["pointer"]
    pointer_projection = projection_for(binding["pointer_path"])
    assert pointer_projection is not None
    assert binding["pointer_sha256"] == pointer_projection.public_projection_sha256
    assert binding["pointer_public_projection_sha256"] == (
        pointer_projection.public_projection_sha256
    )
    assert binding["pointer_source_sha256"] == pointer_projection.source_private_sha256
    identity_projection = projection_for(binding["identity_path"])
    assert identity_projection is not None
    assert binding["identity_sha256"] == identity_projection.public_projection_sha256
    assert binding["identity_public_projection_sha256"] == (
        identity_projection.public_projection_sha256
    )
    assert binding["identity_source_sha256"] == identity_projection.source_private_sha256
    assert identity["baseline_id"] == pointer["baseline_id"]
    assert identity["permissions"]["baseline_promotion_authorized"] is True
    assert identity["permissions"]["backtest_default_control_authorized"] is True
    assert identity["model"]["training_summary_sha256"]
    assert identity["p3"]["sha256"]
    assert pointer["buy_fill_selection_shadow_enabled"] is False
    assert pointer["buy_fill_selection_live_enabled"] is False
    assert pointer["quote_snapshot_atomicity_contract"] == "v2"
    assert pointer["baseline_integrity_gate_passed"] is True
    replay_baseline = binding["replay_baseline"]
    assert replay_baseline is not None
    assert replay_baseline["control"]["identity"] == (
        "current_live_held_global_ber_control"
    )
    assert replay_baseline["replay_semantics"]["ber_clock_identity"] == (
        "live_held_completed_10s_feature_sampled_on_completed_1s_callback.v1"
    )
    assert replay_baseline["economics"]["terminal_mtm_pnl_usdc"] == pytest.approx(
        -165.56607903599894
    )
    assert replay_baseline["panel"]["days"] == 50
    assert replay_baseline["parity"]["prefix_daily_mismatch_count"] == 0
    assert identity["engineering_verification"]["status"] == (
        "pass_with_owner_accepted_evidence_limitations"
    )
    assert identity["research_gate_passed"] is False
    assert identity["permissions"]["owner_risk_accepted_live_authorized"] is True
    assert identity["f05_boolean_cooldown"]["restart_aware_71d_terminal_delta_usdc"] == (
        pytest.approx(16.877254176)
    )
    assert identity["f05_boolean_cooldown"]["research_hard_gates_passed"] is False
    # The immutable v11 identity records the deployed code. This checkout has
    # continued development, so it is deliberately classified as a runtime
    # overlay rather than an exact reproduction of deployed v11.
    assert binding["runtime_code_audit"]["declared"] is True
    assert binding["runtime_code_audit"]["matches"] is False
    assert binding["runtime_code_audit"]["mismatched_paths"]
