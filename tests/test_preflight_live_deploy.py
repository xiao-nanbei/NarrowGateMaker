from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from live import runtime_policy
from scripts.preflight_live_deploy import validate_deploy_config
from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy
from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy
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
    assert identity["model_authorization_path"].endswith(
        "deployment_authorization.json"
    )
    assert identity["f05_buy_e3_artifacts"] == {"enabled": False}
    assert identity["startup_gates_not_validated"] == [
        "deployment_envelope",
        "policy_approvals",
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
    assert identity["model_authorization_path"] == str(
        (model_dir / "deployment_authorization.json").resolve()
    )
    assert identity["p3_path"] == str((model_dir / "fill_prob_params.json").resolve())


def test_preflight_enabled_buy_e3_missing_artifacts_fails_closed(
    tmp_path: Path,
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

    with pytest.raises(ValueError, match="requires strategy.buy_e3"):
        validate_deploy_config(config_path, tmp_path)


def test_preflight_derives_policy_leaf_hashes_from_files_not_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_fixture(tmp_path)
    boolean_policy = tmp_path / "boolean-policy.json"
    boolean_bundle = tmp_path / "boolean-bundle.json"
    buy_manifest = tmp_path / "buy-manifest.json"
    buy_policy = tmp_path / "buy-policy.json"
    buy_bundle = tmp_path / "buy-bundle.json"
    artifact_sha256 = "a" * 64
    boolean_policy.write_text("{}\n", encoding="utf-8")
    boolean_bundle.write_text("{}\n", encoding="utf-8")
    buy_manifest.write_text(
        json.dumps({"artifact_sha256": artifact_sha256}) + "\n",
        encoding="utf-8",
    )
    buy_policy.write_text("{}\n", encoding="utf-8")
    buy_bundle.write_text("{}\n", encoding="utf-8")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["strategy"].update(
        {
            "fill_cooldown": 85.0,
            "adaptive_add_cooldown_enabled": False,
            "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
            "boolean_cooldown_policy_enabled": True,
            "boolean_cooldown_evidence_route": "private_deployment_approval",
            "boolean_cooldown_policy_path": str(boolean_policy),
            "boolean_cooldown_policy_sha256": "0" * 64,
            "boolean_cooldown_predicate_bundle_path": str(boolean_bundle),
            "boolean_cooldown_predicate_bundle_sha256": "1" * 64,
            "boolean_cooldown_ema_warmup_s": 2048.0,
            "buy_e3_cooldown_policy_enabled": True,
            "buy_e3_cooldown_evidence_route": "private_deployment_buy_e3",
            "buy_e3_cooldown_artifact_manifest_path": str(buy_manifest),
            "buy_e3_cooldown_artifact_manifest_sha256": "2" * 64,
            "buy_e3_cooldown_artifact_sha256": "3" * 64,
            "buy_e3_cooldown_policy_path": str(buy_policy),
            "buy_e3_cooldown_policy_sha256": "4" * 64,
            "buy_e3_cooldown_predicate_bundle_path": str(buy_bundle),
            "buy_e3_cooldown_predicate_bundle_sha256": "5" * 64,
            "buy_e3_cooldown_ema_warmup_s": 2048.0,
        }
    )
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    observed: dict[str, dict[str, object]] = {}

    def fake_boolean_from_files(_cls, **kwargs):
        observed["boolean"] = kwargs
        return SimpleNamespace(
            evaluator=SimpleNamespace(
                policy_sha256=kwargs["policy_sha256"],
                predicate_bundle_sha256=kwargs["predicate_bundle_sha256"],
                predicate_columns=(),
            )
        )

    def fake_buy_from_files(_cls, **kwargs):
        observed["buy"] = kwargs
        return SimpleNamespace(
            artifact_sha256=artifact_sha256,
            evaluator=SimpleNamespace(
                policy_sha256=kwargs["policy_sha256"],
                predicate_bundle_sha256=kwargs["predicate_bundle_sha256"],
            ),
        )

    monkeypatch.setattr(
        LiveBooleanCooldownPolicy,
        "from_files",
        classmethod(fake_boolean_from_files),
    )
    monkeypatch.setattr(
        LiveBuyE3CooldownPolicy,
        "from_files",
        classmethod(fake_buy_from_files),
    )

    identity = validate_deploy_config(config_path, tmp_path)

    assert observed["boolean"]["policy_sha256"] == hashlib.sha256(
        boolean_policy.read_bytes()
    ).hexdigest()
    assert observed["boolean"]["predicate_bundle_sha256"] == hashlib.sha256(
        boolean_bundle.read_bytes()
    ).hexdigest()
    assert observed["buy"]["artifact_manifest_sha256"] == hashlib.sha256(
        buy_manifest.read_bytes()
    ).hexdigest()
    assert observed["buy"]["expected_artifact_sha256"] == artifact_sha256
    assert observed["buy"]["policy_sha256"] == hashlib.sha256(
        buy_policy.read_bytes()
    ).hexdigest()
    assert identity["f05_buy_e3_artifacts"]["artifact_sha256"] == artifact_sha256


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


def test_source_publish_targets_are_separate_from_deploy_preflight() -> None:
    root = Path(__file__).resolve().parents[1]
    makefile = (root / "Makefile").read_text(encoding="utf-8")

    assert "deploy-preflight:" in makefile
    assert "scripts/preflight_live_deploy.py --config" in makefile
    assert "publish-source:" in makefile
    assert "publish-source-dry:" in makefile
    assert "\ndeploy:" not in makefile
    assert "\ndeploy-dry:" not in makefile
    assert makefile.count("scripts/live_deploy_common.py source-release") == 2


def test_native_live_wheel_build_is_bounded_and_separate_from_deploy() -> None:
    root = Path(__file__).resolve().parents[1]
    makefile = (root / "Makefile").read_text(encoding="utf-8")

    assert "NATIVE_BUILD_PARALLEL_LEVEL ?= 1" in makefile
    assert "NATIVE_BUILD_MIN_AVAILABLE_MIB ?= 2048" in makefile
    assert (
        "NATIVE_BUILD_COMMIT ?= $(shell git rev-parse --verify HEAD 2>/dev/null)"
        in makefile
    )
    assert "NATIVE_WHEEL_DIR ?= dist/native/live/$(NATIVE_BUILD_COMMIT)" in makefile
    assert "native-live-build-preflight:" in makefile
    assert "native-live-wheel: native-live-build-preflight" in makefile
    assert "MemAvailable:" in makefile
    assert "narrowgate.service narrowgate-maker.service" in makefile
    assert "CMAKE_BUILD_PARALLEL_LEVEL=\"$(NATIVE_BUILD_PARALLEL_LEVEL)\"" in makefile
    assert "PIP_NO_INDEX=1" in makefile
    assert "PIP_DISABLE_PIP_VERSION_CHECK=1" in makefile
    assert "--no-build-isolation" in makefile
    assert "--check-build-dependencies" in makefile
    assert "NARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2" in makefile
    assert "NARROWGATE_BUILD_FLAVOR=live" in makefile
    assert 'getconf GNU_LIBC_VERSION' in makefile
    assert 'sys.version_info[:2] == (3, 12)' in makefile

    publish_source = makefile.split("publish-source:", 1)[1].split(
        "publish-source-dry:", 1
    )[0]
    publish_source_dry = makefile.split("publish-source-dry:", 1)[1].split(
        "# ── Cleanup", 1
    )[0]
    for recipe in (publish_source, publish_source_dry):
        assert "native-live-wheel" not in recipe
        assert "pip wheel" not in recipe


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


@pytest.mark.parametrize("obsolete_flag", [None, "1"])
def test_preflight_cannot_grant_policy_approval_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, obsolete_flag,
) -> None:
    def forbidden():
        raise AssertionError("default preflight must not evaluate deployment approval")

    monkeypatch.setattr(runtime_policy, "deployment_envelope_runtime_authority", forbidden)
    if obsolete_flag is None:
        monkeypatch.delenv("NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY", raising=False)
    else:
        monkeypatch.setenv("NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY", obsolete_flag)

    identity = validate_deploy_config(
        _write_fixture(tmp_path, q90_action_enabled=True),
        tmp_path,
    )

    assert identity["dynamic_fill_hazard_action_enabled"] is True
    assert identity["policy_admission"] == "not_evaluated_requires_deployment_envelope"
    assert "policy_approvals" in identity["startup_gates_not_validated"]
    assert "q90_action_runtime_authority" not in identity
    assert "q90_owner_override_effective" not in identity


def _verified_fixture_authority(config_path: Path, approvals: list[str]) -> dict:
    authorization_path = (
        config_path.parent / "models/bundle/deployment_authorization.json"
    ).resolve()
    return {
        "config_file_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "policy_approvals": approvals,
        "model_policy_member_paths": {"model_authorization": str(authorization_path)},
        "model_policy_member_sha256": {
            "model_authorization": hashlib.sha256(authorization_path.read_bytes()).hexdigest(),
        },
    }


@pytest.mark.parametrize("approvals", [[], ["f05_boolean_cooldown"], ["q90_action"]])
def test_preflight_opt_in_checks_verified_policy_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, approvals: list[str],
) -> None:
    config_path = _write_fixture(tmp_path, q90_action_enabled=True)
    authority = _verified_fixture_authority(config_path, approvals)
    monkeypatch.setattr(
        runtime_policy, "deployment_envelope_runtime_authority", lambda: authority,
    )
    monkeypatch.setenv("NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY", "1")

    if "q90_action" not in approvals:
        with pytest.raises(ValueError, match="does not approve enabled policy: q90_action"):
            validate_deploy_config(config_path, tmp_path, check_policy_approval=True)
        return

    identity = validate_deploy_config(config_path, tmp_path, check_policy_approval=True)
    assert identity["policy_admission"] == {
        "approved_policies": ["q90_action"],
        "authorization_source": "deployment_envelope",
    }
    assert identity["startup_gates_not_validated"] == [
        "locked_runtime", "stopped_exchange_reconciliation",
    ]


@pytest.mark.parametrize("drift", ["config", "model_authorization"])
def test_preflight_opt_in_binds_exact_config_and_artifact_locators(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str,
) -> None:
    config_path = _write_fixture(tmp_path, q90_action_enabled=True)
    authority = _verified_fixture_authority(config_path, ["q90_action"])
    if drift == "config":
        authority["config_file_sha256"] = "0" * 64
        expected_error = "deploy config differs from deployment envelope"
    else:
        authority["model_policy_member_paths"]["model_authorization"] = str(
            config_path.resolve()
        )
        authority["model_policy_member_sha256"]["model_authorization"] = authority[
            "config_file_sha256"
        ]
        expected_error = "policy_artifact_authority_config_path_drifted:model_authorization"
    monkeypatch.setattr(
        runtime_policy, "deployment_envelope_runtime_authority", lambda: authority,
    )
    with pytest.raises(ValueError, match=expected_error):
        validate_deploy_config(config_path, tmp_path, check_policy_approval=True)


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
