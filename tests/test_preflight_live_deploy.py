from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from scripts.preflight_live_deploy import validate_deploy_config
from strategy.model_contract import (
    ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
    REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_LABEL_SEMANTICS_VERSION,
    REQUIRED_LABEL_WINDOW_SEMANTICS,
    REQUIRED_MODEL_HEADS,
)

EXAMPLE_REMOTE_COLLECTION_ROOT = "/srv/example-live/formal_collection"
EXAMPLE_STORAGE_ROOT = "/srv/example-storage"
PUBLIC_DRY_RUN_BUNDLE = Path("examples/public_dry_run_model_bundle")


def _write_fixture(
    tmp_path: Path,
    *,
    override: float = 0.0,
    q90_action_enabled: bool = False,
) -> Path:
    model_dir = tmp_path / "models" / "bundle"
    model_dir.mkdir(parents=True)
    (model_dir / "fill_prob_params.json").write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_p3_touch_calibration.v2",
                "model_type": "empirical_survival",
                "delta_grid": [0.1, 14.0, 30.0],
                "probability_grid": [0.8, 0.2, 0.01],
                "metadata": {
                    "event_type": "touch",
                    "horizon_s": 10.0,
                    "distance_unit": "USDC_per_BTC",
                },
                "delta_star": 14.0,
                "kappa_eff": 0.067,
            }
        ),
        encoding="utf-8",
    )
    for head in REQUIRED_MODEL_HEADS:
        (model_dir / f"{head}.txt").write_text("placeholder", encoding="utf-8")
        metadata = {
            "feature_cols": ["close"],
            "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
            "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
            "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
            "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
            "label_semantics_version": REQUIRED_LABEL_SEMANTICS_VERSION,
            "label_window_semantics": REQUIRED_LABEL_WINDOW_SEMANTICS,
            "feature_manifest_sha256": "fixture-manifest",
        }
        if head.startswith("vol_"):
            metadata["label_semantics"] = ABSOLUTE_PRICE_VARIANCE_SEMANTICS
        (model_dir / f"{head}_meta.json").write_text(
            json.dumps(metadata),
            encoding="utf-8",
        )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "strategy": {
                    "p3_kappa_eff_override": override,
                    "quote_horizon_s": 1.0,
                    "use_bar_pricing": False,
                    "dynamic_fill_hazard_action_enabled": q90_action_enabled,
                },
                "ml": {"model_dir": "models/bundle"},
                "risk": {
                    "max_exec_book_visible_age_s": 5.0,
                    "max_exec_book_source_lag_s": 5.0,
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_preflight_uses_empirical_p3_artifact(tmp_path: Path) -> None:
    identity = validate_deploy_config(_write_fixture(tmp_path), tmp_path)

    assert identity["effective_source"] == "artifact"
    assert identity["effective_kappa"] == pytest.approx(0.067)
    assert identity["delta_star"] == pytest.approx(14.0)
    assert identity["p3_event_type"] == "touch"
    assert identity["p3_horizon_s"] == pytest.approx(10.0)
    assert identity["p3_distance_unit"] == "USDC_per_BTC"
    assert len(identity["p3_sha256"]) == 64
    assert identity["validated_model_heads"] == sorted(REQUIRED_MODEL_HEADS)
    assert identity["feature_dag_id"] == REQUIRED_FEATURE_DAG_ID
    assert identity["feature_dag_sha256"] == REQUIRED_FEATURE_DAG_SHA256
    assert identity["use_bar_pricing"] is False
    assert identity["max_exec_book_visible_age_s"] == pytest.approx(5.0)
    assert identity["max_exec_book_source_lag_s"] == pytest.approx(5.0)


def test_public_dry_run_bundle_is_hash_bound_and_preflight_valid() -> None:
    root = Path(__file__).resolve().parents[1]
    bundle = root / PUBLIC_DRY_RUN_BUNDLE
    manifest = json.loads(
        (bundle / "fixture_manifest.json").read_text(encoding="utf-8")
    )

    assert manifest["synthetic"] is True
    assert all(value is False for value in manifest["authority"].values())
    for entry in manifest["files"]:
        path = bundle / entry["path"]
        payload = path.read_bytes()
        assert len(payload) == entry["bytes"]
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]

    identity = validate_deploy_config(
        root / "examples/live_dry_run_config.yaml",
        root,
    )

    assert identity["ml_enabled"] is False
    assert identity["dynamic_fill_hazard_action_enabled"] is False
    assert identity["validated_model_heads"] == sorted(REQUIRED_MODEL_HEADS)


def test_preflight_requires_explicit_quote_snapshot_clock_limits(
    tmp_path: Path,
) -> None:
    config_path = _write_fixture(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    del config["risk"]["max_exec_book_source_lag_s"]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="risk.max_exec_book_source_lag_s must be explicit",
    ):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_invalid_bundle_while_ml_is_disabled(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    meta_path = tmp_path / "models" / "bundle" / "dir_10s_meta.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["feature_semantics_version"] = 4
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ValueError, match="feature_semantics_version=4"):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_pre_cutoff_feature_dag_identity(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    meta_path = tmp_path / "models" / "bundle" / "dir_10s_meta.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["feature_dag_sha256"] = "legacy-pre-cutoff-dag"
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ValueError, match="incompatible feature DAG identity"):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_unapproved_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY", raising=False)

    with pytest.raises(ValueError, match="NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY"):
        validate_deploy_config(_write_fixture(tmp_path, override=0.055), tmp_path)


def test_preflight_allows_explicit_override_trial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY", "1")

    identity = validate_deploy_config(
        _write_fixture(tmp_path, override=0.055),
        tmp_path,
    )

    assert identity["effective_source"] == "config_override"
    assert identity["effective_kappa"] == pytest.approx(0.055)


def test_preflight_rejects_unrepaired_q90_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(
        "NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY", raising=False
    )

    with pytest.raises(ValueError, match="POST_CANCEL_RECOVERY"):
        validate_deploy_config(
            _write_fixture(tmp_path, q90_action_enabled=True),
            tmp_path,
        )


def test_preflight_labels_explicit_unrepaired_q90_owner_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY", "1")

    identity = validate_deploy_config(
        _write_fixture(tmp_path, q90_action_enabled=True),
        tmp_path,
    )

    assert identity["dynamic_fill_hazard_action_enabled"] is True
    assert identity["q90_post_cancel_recovery_contract_supported"] is False
    assert identity["q90_action_deploy_authority"] == "owner_risk_accepted_override"
    assert identity["q90_action_runtime_authority"] == (
        "owner_risk_accepted_override"
    )
    assert identity["q90_owner_override_requested"] is True
    assert identity["q90_owner_override_effective"] is True


def _enable_remote_lifecycle_collection(
    config_path: Path,
    *,
    baseline_sha256: str,
) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["lifecycle_journal_v2"] = {
        "enabled": True,
        "storage_profile": "bounded_remote_spool",
        "required_mount": EXAMPLE_STORAGE_ROOT,
        "root": f"{EXAMPLE_REMOTE_COLLECTION_ROOT}/journal",
        "prospective_epoch_root": f"{EXAMPLE_REMOTE_COLLECTION_ROOT}/epochs",
        "remote_spool_allowlisted_roots": [
            EXAMPLE_REMOTE_COLLECTION_ROOT
        ],
        "remote_session_max_duration_s": 3600.0,
        "remote_session_max_bytes": 4 * 1024 * 1024 * 1024,
        "baseline_identity_path": "research/baseline.json",
        "baseline_identity_sha256": baseline_sha256,
    }
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def test_preflight_binds_enabled_remote_lifecycle_collection(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    baseline = tmp_path / "research" / "baseline.json"
    baseline.parent.mkdir(parents=True)
    baseline.write_text('{"baseline_id":"v9"}\n', encoding="utf-8")
    baseline_sha = hashlib.sha256(baseline.read_bytes()).hexdigest()
    _enable_remote_lifecycle_collection(
        config_path,
        baseline_sha256=baseline_sha,
    )

    identity = validate_deploy_config(config_path, tmp_path)

    lifecycle = identity["lifecycle_journal_v2"]
    assert lifecycle["enabled"] is True
    assert lifecycle["storage_profile"] == "bounded_remote_spool"
    assert lifecycle["formal_collection_valid_at_remote_write"] is False
    assert lifecycle["baseline_identity_sha256"] == baseline_sha


def test_preflight_rejects_lifecycle_baseline_hash_drift(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    baseline = tmp_path / "research" / "baseline.json"
    baseline.parent.mkdir(parents=True)
    baseline.write_text("{}\n", encoding="utf-8")
    _enable_remote_lifecycle_collection(
        config_path,
        baseline_sha256="0" * 64,
    )

    with pytest.raises(ValueError, match="baseline identity SHA256 mismatch"):
        validate_deploy_config(config_path, tmp_path)
