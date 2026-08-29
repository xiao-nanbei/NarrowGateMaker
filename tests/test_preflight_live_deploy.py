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
    absolute_price_variance_unit_contract,
    validate_model_bundle,
)

EXAMPLE_REMOTE_COLLECTION_ROOT = "/srv/example-live/formal_collection"
EXAMPLE_STORAGE_ROOT = "/srv/example-storage"
PUBLIC_DRY_RUN_BUNDLE = Path("examples/public_dry_run_model_bundle")


def _write_live_authorization(model_dir: Path) -> None:
    tree_hashes = {
        head: hashlib.sha256((model_dir / f"{head}.txt").read_bytes()).hexdigest()
        for head in REQUIRED_MODEL_HEADS
    }
    metadata_hashes = {
        head: hashlib.sha256(
            (model_dir / f"{head}_meta.json").read_bytes()
        ).hexdigest()
        for head in REQUIRED_MODEL_HEADS
    }
    authorization = {
        "schema_version": "narrowgate.private_deployment_authorization.v1",
        "training_experiment_id": "deploy-fixture-v1",
        "private_deployment_authorized": True,
        "active_runtime_inference_authorized": True,
        "baseline_promotion_authorized": False,
        "authority": {"live": True},
        "derived_bundle": {
            "model_tree_sha256": tree_hashes,
            "head_metadata_sha256": metadata_hashes,
            "p3_sha256": hashlib.sha256(
                (model_dir / "fill_prob_params.json").read_bytes()
            ).hexdigest(),
        },
    }
    (model_dir / "deployment_authorization.json").write_text(
        json.dumps(authorization),
        encoding="utf-8",
    )


def _write_fixture(
    tmp_path: Path,
    *,
    override: float = 0.0,
    q90_action_enabled: bool = False,
    ret_skew: float = 0.0,
    quote_horizon_s: float = 1.0,
    direct_ret_action_horizon_s: float | None = None,
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
            "symbol": "BTCUSDC",
            "feature_cols": ["close"],
            "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
            "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
            "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
            "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
            "label_semantics_version": REQUIRED_LABEL_SEMANTICS_VERSION,
            "label_window_semantics": REQUIRED_LABEL_WINDOW_SEMANTICS,
            "feature_manifest_sha256": "fixture-manifest",
            "training_experiment_id": "deploy-fixture-v1",
            "promotion_authority": "private_deployment_authorized",
            "volatility_unit_contract": absolute_price_variance_unit_contract(
                "BTCUSDC"
            ),
        }
        if head.startswith("vol_"):
            metadata["label_semantics"] = ABSOLUTE_PRICE_VARIANCE_SEMANTICS
        if head == "ret_10s" and direct_ret_action_horizon_s is not None:
            metadata["direct_quote_action"] = {
                "schema_version": "narrowgate.f03.direct_quote_action.v1",
                "compatible": True,
                "event_type": "decision_to_fixed_horizon_return",
                "horizon_s": direct_ret_action_horizon_s,
                "price_origin": "decision_mid",
                "return_unit": "fraction",
                "consumer": "quote_center_shift",
            }
        (model_dir / f"{head}_meta.json").write_text(
            json.dumps(metadata),
            encoding="utf-8",
        )
    _write_live_authorization(model_dir)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "symbol": "BTCUSDC",
                "strategy": {
                    "p3_kappa_eff_override": override,
                    "quote_horizon_s": quote_horizon_s,
                    "use_bar_pricing": False,
                    "dynamic_fill_hazard_action_enabled": q90_action_enabled,
                },
                "ml": {
                    "model_dir": "models/bundle",
                    "enabled": ret_skew > 0.0,
                    "ret_skew": ret_skew,
                },
                "risk": {
                    "max_exec_book_visible_age_s": 5.0,
                    "max_exec_book_source_lag_s": 5.0,
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


@pytest.mark.parametrize("quote_horizon_s", (1.0, 10.0))
def test_preflight_rejects_legacy_f03_ret_head_as_direct_quote_action(
    tmp_path: Path,
    quote_horizon_s: float,
) -> None:
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        validate_deploy_config(
            _write_fixture(
                tmp_path,
                ret_skew=1.0,
                quote_horizon_s=quote_horizon_s,
            ),
            tmp_path,
        )


def test_preflight_accepts_explicit_point_horizon_f03_action_contract(
    tmp_path: Path,
) -> None:
    identity = validate_deploy_config(
        _write_fixture(
            tmp_path,
            ret_skew=1.0,
            quote_horizon_s=10.0,
            direct_ret_action_horizon_s=10.0,
        ),
        tmp_path,
    )
    assert identity["validated_model_heads"] == sorted(REQUIRED_MODEL_HEADS)


def test_preflight_rejects_side_bbo_floor_with_inward_compression(
    tmp_path: Path,
) -> None:
    config_path = _write_fixture(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["strategy"].update(
        {
            "historical_p3_scalar_adapter_enabled": False,
            "p3_side_bbo_floor_enabled": True,
            "spread_cap_mode": "compress",
        }
    )
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ValueError, match="side-BBO floor cannot be combined"):
        validate_deploy_config(config_path, tmp_path)


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
    assert identity["model_promotion_authority"] == "private_deployment_authorized"
    assert identity["model_live_authorized"] is True
    assert identity["f05_buy_e3_artifacts"] == {"enabled": False}
    assert identity["startup_gates_not_validated"] == [
        "deployment_envelope",
        "locked_runtime",
        "stopped_exchange_reconciliation",
    ]


def test_preflight_accepts_private_config_and_bundle_outside_repository(
    tmp_path: Path,
) -> None:
    private_root = tmp_path / "private"
    private_root.mkdir()
    config_path = _write_fixture(private_root)
    model_dir = private_root / "models" / "bundle"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["ml"]["model_dir"] = str(model_dir.resolve())
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    repository = tmp_path / "checkout"
    repository.mkdir()

    identity = validate_deploy_config(config_path.resolve(), repository.resolve())

    assert identity["config_path"] == str(config_path.resolve())
    assert identity["model_dir"] == str(model_dir.resolve())
    assert identity["p3_path"] == str((model_dir / "fill_prob_params.json").resolve())


def test_preflight_enabled_buy_e3_missing_artifacts_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_fixture(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["strategy"].update(
        {
            "buy_e3_cooldown_policy_enabled": True,
            "buy_e3_cooldown_evidence_route": "private_deployment_buy_e3",
            "fill_cooldown": 85.0,
            "adaptive_add_cooldown_enabled": False,
            "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        }
    )
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    monkeypatch.setenv("NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY", "1")

    with pytest.raises(ValueError, match="requires strategy.buy_e3"):
        validate_deploy_config(config_path, tmp_path)


@pytest.mark.parametrize("field", ("eta_inventory", "a_spread"))
def test_preflight_accepts_explicit_quote_unit_coefficients(
    tmp_path: Path,
    field: str,
) -> None:
    config_path = _write_fixture(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["strategy"][field] = 0.046
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    identity = validate_deploy_config(config_path, tmp_path)

    assert identity["quote_unit_contract"][field] == pytest.approx(0.046)


@pytest.mark.parametrize(
    "field",
    (
        "inventory_reference_qty",
        "eta_inventory",
        "a_spread",
        "risk_per_order",
        "execution_intensity_slope",
        "risk_horizon_s",
    ),
)
def test_preflight_rejects_invalid_quote_unit_coefficients(
    tmp_path: Path,
    field: str,
) -> None:
    config_path = _write_fixture(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["strategy"][field] = float("nan")
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ValueError, match=rf"strategy\.{field}"):
        validate_deploy_config(config_path, tmp_path)


def test_public_dry_run_bundle_is_hash_bound_but_not_deploy_authorized() -> None:
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
    assert sorted(validate_model_bundle(bundle)) == sorted(REQUIRED_MODEL_HEADS)

    with pytest.raises(ValueError, match="public_dry_run_only"):
        validate_deploy_config(
            root / "examples/live_dry_run_config.yaml",
            root,
        )


def test_formal_public_dry_run_config_cannot_pass_deploy_preflight() -> None:
    root = Path(__file__).resolve().parents[1]

    with pytest.raises(ValueError):
        validate_deploy_config(root / "live/formal_dry_run_public.yaml", root)


def test_public_dry_run_config_is_rejected_without_text_markers(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "examples/live_dry_run_config.yaml").read_text(encoding="utf-8")
    config = yaml.safe_load(source)
    config["ml"]["model_dir"] = str((root / PUBLIC_DRY_RUN_BUNDLE).resolve())
    config_path = tmp_path / "renamed.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ValueError, match="public_dry_run_only"):
        validate_deploy_config(config_path, root)


def test_tracked_public_template_marker_fails_before_deploy_validation() -> None:
    root = Path(__file__).resolve().parents[1]

    with pytest.raises(ValueError, match="marked PUBLIC TEMPLATE"):
        validate_deploy_config(root / "live/config.yaml", root)


@pytest.mark.parametrize(
    ("promotion_authority", "message"),
    [
        (None, "lacks explicit live promotion_authority"),
        ("public_dry_run_only", "public_dry_run_only"),
        ("research_only", "research_only"),
    ],
)
def test_preflight_rejects_non_live_model_authority(
    tmp_path: Path,
    promotion_authority: str | None,
    message: str,
) -> None:
    config_path = _write_fixture(tmp_path)
    model_dir = tmp_path / "models" / "bundle"
    for head in REQUIRED_MODEL_HEADS:
        metadata_path = model_dir / f"{head}_meta.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if promotion_authority is None:
            metadata.pop("promotion_authority")
        else:
            metadata["promotion_authority"] = promotion_authority
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_missing_live_authorization(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    authorization_path = (
        tmp_path / "models" / "bundle" / "deployment_authorization.json"
    )
    authorization_path.unlink()

    with pytest.raises(ValueError, match="requires deployment_authorization.json"):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_explicit_authority_live_false(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    authorization_path = (
        tmp_path / "models" / "bundle" / "deployment_authorization.json"
    )
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    authorization["authority"]["live"] = False
    authorization_path.write_text(json.dumps(authorization), encoding="utf-8")

    with pytest.raises(ValueError, match=r"authority\.live=true"):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_rejects_bundle_manifest_live_false(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    manifest_path = tmp_path / "models" / "bundle" / "fixture_manifest.json"
    manifest_path.write_text(
        json.dumps({"synthetic": False, "authority": {"live": False}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"authority\.live=true"):
        validate_deploy_config(config_path, tmp_path)


@pytest.mark.parametrize(
    ("relative_path", "message"),
    [
        ("dir_10s.txt", "model hash mismatch"),
        ("dir_10s_meta.json", "metadata hash mismatch"),
        ("fill_prob_params.json", "P3 hash mismatch"),
    ],
)
def test_preflight_rejects_hash_bound_bundle_tamper(
    tmp_path: Path,
    relative_path: str,
    message: str,
) -> None:
    config_path = _write_fixture(tmp_path)
    artifact_path = tmp_path / "models" / "bundle" / relative_path
    artifact_path.write_bytes(artifact_path.read_bytes() + b"\n")

    with pytest.raises(ValueError, match=message):
        validate_deploy_config(config_path, tmp_path)


def test_deploy_targets_share_structural_preflight() -> None:
    root = Path(__file__).resolve().parents[1]
    makefile = (root / "Makefile").read_text(encoding="utf-8")

    assert "deploy-preflight:" in makefile
    assert "scripts/preflight_live_deploy.py --config" in makefile
    assert makefile.count("scripts/live_deploy_common.py source-release") == 2


def test_ci_pytest_failure_remains_a_job_failure_with_summary() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.safe_load(
        (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["python"]["steps"]
    pytest_step = next(step for step in steps if step.get("id") == "pytest")
    summary_step = next(
        step for step in steps if step.get("name") == "Publish failing pytest node IDs"
    )

    assert pytest_step.get("continue-on-error") is not True
    assert "failure()" in summary_step["if"]
    assert "steps.pytest.outcome == 'failure'" in summary_step["if"]
    assert "scripts/report_pytest_failures.py" in summary_step["run"]


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

    with pytest.raises(ValueError, match="independently hash-bound override identity"):
        validate_deploy_config(_write_fixture(tmp_path, override=0.055), tmp_path)


def test_preflight_env_flag_cannot_lend_artifact_identity_to_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY", "1")

    with pytest.raises(ValueError, match="independently hash-bound override identity"):
        validate_deploy_config(
            _write_fixture(tmp_path, override=0.055),
            tmp_path,
        )


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


def test_preflight_labels_explicit_unrepaired_q90_private_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY", "1")

    identity = validate_deploy_config(
        _write_fixture(tmp_path, q90_action_enabled=True),
        tmp_path,
    )

    assert identity["dynamic_fill_hazard_action_enabled"] is True
    assert identity["q90_post_cancel_recovery_contract_supported"] is False
    assert identity["q90_action_deploy_authority"] == "private_deployment_approved"
    assert identity["q90_action_runtime_authority"] == (
        "private_deployment_approved"
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
