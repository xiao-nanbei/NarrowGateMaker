"""Validate a private deployment config and print its effective identities."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PUBLIC_TEMPLATE_MARKER = "PUBLIC TEMPLATE"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _buy_e3_artifact_sha256(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("F05 BUY E3 artifact manifest is unreadable") from exc
    if not isinstance(payload, dict):
        raise ValueError("F05 BUY E3 artifact manifest must be a mapping")
    observed = str(payload.get("artifact_sha256", "")).strip().lower()
    if len(observed) != 64 or any(
        char not in "0123456789abcdef" for char in observed
    ):
        raise ValueError("F05 BUY E3 artifact manifest is missing artifact_sha256")
    return observed


def _as_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a mapping")
    return value


def _identity_path(path: Path, repo_root: Path) -> str:
    """Use a stable relative label when possible, otherwise preserve absolute paths."""

    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def validate_deploy_config(
    config_path: Path,
    repo_root: Path,
    *,
    check_policy_approval: bool = False,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    config_bytes = config_path.read_bytes()
    config_sha256 = hashlib.sha256(config_bytes).hexdigest()
    config_text = config_bytes.decode("utf-8")
    if PUBLIC_TEMPLATE_MARKER in config_text:
        raise ValueError(
            f"{config_path} is marked {PUBLIC_TEMPLATE_MARKER}; "
            "select a private deploy config"
        )

    from execution.order_lifecycle_journal_storage_v2 import (
        BOUNDED_REMOTE_SPOOL,
        validate_lifecycle_journal_storage,
    )
    from research.families.f02_empirical_p3_touch.fill_probability import (
        FillProbabilityModel,
    )
    from strategy.model_contract import (
        PRIVATE_DEPLOYMENT_AUTHORITY,
        REQUIRED_FEATURE_DAG_ID,
        REQUIRED_FEATURE_DAG_SHA256,
        REQUIRED_MODEL_HEADS,
        f03_direct_quote_action_contract,
        resolve_model_authorization_manifest,
        validate_model_bundle,
    )

    config = _as_mapping(yaml.safe_load(config_text), "config")
    strategy = _as_mapping(config.get("strategy"), "strategy")
    ml = _as_mapping(config.get("ml"), "ml")
    risk = _as_mapping(config.get("risk"), "risk")
    q_ref = float(strategy.get("inventory_reference_qty", 1.0))
    if not math.isfinite(q_ref) or q_ref <= 0.0:
        raise ValueError(
            "strategy.inventory_reference_qty must be positive and finite"
        )
    for field_name in (
        "eta_inventory",
        "a_spread",
        "risk_per_order",
        "execution_intensity_slope",
        "risk_horizon_s",
    ):
        raw_value = strategy.get(field_name)
        if raw_value is None:
            continue
        value = float(raw_value)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(
                f"strategy.{field_name} must be positive and finite when set"
            )
    if bool(strategy.get("historical_p3_scalar_adapter_enabled", True)) and bool(
        strategy.get("p3_side_bbo_floor_enabled", False)
    ):
        raise ValueError(
            "historical P3 pair projection and P3 side-BBO floor are mutually exclusive"
        )
    spread_cap_mode = str(
        strategy.get("spread_cap_mode", "pause_exposure") or "pause_exposure"
    ).strip().lower()
    if bool(strategy.get("p3_side_bbo_floor_enabled", False)) and (
        spread_cap_mode == "compress"
    ):
        raise ValueError(
            "P3 side-BBO floor cannot be combined with spread_cap_mode=compress; "
            "later inward compression would violate the side-specific distance floor"
        )
    lifecycle_v2 = _as_mapping(
        config.get("lifecycle_journal_v2", {}),
        "lifecycle_journal_v2",
    )

    lifecycle_identity: dict[str, Any] = {"enabled": False}
    if bool(lifecycle_v2.get("enabled", False)):
        profile = str(lifecycle_v2.get("storage_profile", "")).strip()
        if profile != BOUNDED_REMOTE_SPOOL:
            raise ValueError(
                "enabled EC2 lifecycle_journal_v2 requires bounded_remote_spool"
            )
        required = (
            "root",
            "prospective_epoch_root",
            "required_mount",
            "remote_spool_allowlisted_roots",
            "baseline_identity_path",
            "baseline_identity_sha256",
            "remote_session_max_duration_s",
            "remote_session_max_bytes",
        )
        missing = [name for name in required if name not in lifecycle_v2]
        if missing:
            raise ValueError(
                "enabled lifecycle_journal_v2 lacks explicit field(s): "
                + ", ".join(missing)
            )
        storage = validate_lifecycle_journal_storage(
            profile=profile,
            journal_root=str(lifecycle_v2["root"]),
            prospective_epoch_root=str(lifecycle_v2["prospective_epoch_root"]),
            required_mount=str(lifecycle_v2["required_mount"]),
            remote_spool_allowlisted_roots=lifecycle_v2[
                "remote_spool_allowlisted_roots"
            ],
            # Deployment validates EC2 paths lexically. Runtime startup repeats
            # validation with enabled=True after the remote directories exist.
            enabled=False,
        )
        duration_s = float(lifecycle_v2["remote_session_max_duration_s"])
        max_bytes = int(lifecycle_v2["remote_session_max_bytes"])
        if not math.isfinite(duration_s) or not (60.0 <= duration_s <= 86_400.0):
            raise ValueError(
                "lifecycle_journal_v2.remote_session_max_duration_s must be in "
                "[60, 86400]"
            )
        if not (1024 * 1024 <= max_bytes <= 100 * 1024 * 1024 * 1024):
            raise ValueError(
                "lifecycle_journal_v2.remote_session_max_bytes must be in "
                "[1 MiB, 100 GiB]"
            )
        baseline_path = Path(str(lifecycle_v2["baseline_identity_path"]))
        if not baseline_path.is_absolute():
            baseline_path = repo_root / baseline_path
        baseline_path = baseline_path.resolve()
        if not baseline_path.is_file():
            raise ValueError(
                "lifecycle_journal_v2 baseline identity does not exist: "
                f"{baseline_path}"
            )
        expected_baseline_sha = str(
            lifecycle_v2["baseline_identity_sha256"]
        ).strip().lower()
        if expected_baseline_sha != _sha256(baseline_path):
            raise ValueError("lifecycle_journal_v2 baseline identity SHA256 mismatch")
        lifecycle_identity = {
            "enabled": True,
            "storage_profile": storage.profile,
            "journal_root": str(storage.journal_root),
            "prospective_epoch_root": str(storage.prospective_epoch_root),
            "remote_spool_allowlisted_root": str(storage.allowlisted_root),
            "remote_session_max_duration_s": duration_s,
            "remote_session_max_bytes": max_bytes,
            "baseline_identity_path": _identity_path(baseline_path, repo_root),
            "baseline_identity_sha256": expected_baseline_sha,
            "formal_collection_valid_at_remote_write": False,
        }

    clock_limits: dict[str, float] = {}
    for field in (
        "max_exec_book_visible_age_s",
        "max_exec_book_source_lag_s",
    ):
        if field not in risk:
            raise ValueError(f"risk.{field} must be explicit in deploy config")
        value = float(risk[field])
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(f"risk.{field} must be positive and finite")
        clock_limits[field] = value

    model_dir_value = str(ml.get("model_dir") or "").strip()
    if not model_dir_value:
        raise ValueError("ml.model_dir must identify an explicit deploy bundle")
    model_dir = Path(model_dir_value)
    if not model_dir.is_absolute():
        model_dir = repo_root / model_dir
    model_dir = model_dir.resolve()
    if not model_dir.is_dir():
        raise ValueError(f"ml.model_dir does not exist: {model_dir}")

    ml_enabled = bool(ml.get("enabled", False))
    # ML enablement is frozen with the config. Disabled inference needs P3,
    # not trees or a grant for an inference path that will never execute.
    model_metadata = (
        validate_model_bundle(
            model_dir,
            require_live_authorization=True,
            expected_symbol=str(config.get("symbol") or ""),
        )
        if ml_enabled else {}
    )
    model_authorization_path = (
        resolve_model_authorization_manifest(model_dir, model_metadata)
        if ml_enabled else None
    )

    if "quote_horizon_s" not in strategy:
        raise ValueError("strategy.quote_horizon_s must be explicit in deploy config")
    quote_horizon_s = float(strategy["quote_horizon_s"])
    if not math.isfinite(quote_horizon_s) or quote_horizon_s <= 0.0:
        raise ValueError("strategy.quote_horizon_s must be positive and finite")
    ret_skew = float(ml.get("ret_skew", 0.0) or 0.0)
    if ml_enabled and ret_skew > 0.0:
        ret_action = f03_direct_quote_action_contract(
            model_metadata.get("ret_10s", {})
        )
        producer_horizon_s = float(ret_action["horizon_s"])
        if (
            not bool(ret_action["compatible"])
            or not math.isclose(
                producer_horizon_s,
                quote_horizon_s,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(
                "F03 ret action horizon is not compatible with the quote "
                f"consumer: producer={producer_horizon_s!r}s "
                f"consumer={quote_horizon_s!r}s"
            )

    p3_path = model_dir / "fill_prob_params.json"
    if not p3_path.is_file():
        raise ValueError(f"deploy bundle is missing fill_prob_params.json: {p3_path}")
    p3_raw = p3_path.read_bytes()
    p3 = _as_mapping(json.loads(p3_raw), "P3 artifact")
    p3_model = FillProbabilityModel.from_bytes(
        p3_raw, artifact_path=p3_path, require_live_compatible=True,
    )
    p3_identity = p3_model.semantic_identity(require_artifact_hash=True)
    artifact_kappa = float(p3.get("kappa_eff", 0.0))
    delta_star = float(p3.get("delta_star", 0.0))
    if artifact_kappa <= 0.0 or delta_star <= 0.0:
        raise ValueError(
            "P3 artifact must contain positive kappa_eff and delta_star; "
            f"got kappa_eff={artifact_kappa}, delta_star={delta_star}"
        )

    override = float(strategy.get("p3_kappa_eff_override", 0.0) or 0.0)
    if not math.isfinite(override) or override < 0.0:
        raise ValueError(
            "strategy.p3_kappa_eff_override must be finite and nonnegative"
        )
    if override > 0.0:
        raise ValueError(
            "nonzero strategy.p3_kappa_eff_override cannot inherit the P3 "
            "artifact identity; live deployment requires an independently "
            "hash-bound override identity"
        )

    f05_artifact_identity: dict[str, Any] = {"enabled": False}
    if bool(strategy.get("boolean_cooldown_policy_enabled", False)):
        from strategy.boolean_cooldown_live import LiveBooleanCooldownPolicy

        if not math.isclose(
            float(strategy.get("fill_cooldown", 0.0)),
            85.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("F05 Boolean cooldown requires fill_cooldown=85")
        if bool(strategy.get("adaptive_add_cooldown_enabled", False)):
            raise ValueError("F05 Boolean cooldown requires adaptive add cooldown OFF")
        if str(
            strategy.get(
                "fill_cooldown_consecutive_reset_policy",
                "",
            )
        ).strip() != "opposite_fill_only":
            raise ValueError(
                "F05 Boolean cooldown requires opposite_fill_only reset"
            )

        def resolve_artifact(name: str) -> Path:
            raw = str(strategy.get(name, "")).strip()
            if not raw:
                raise ValueError(f"F05 Boolean cooldown requires strategy.{name}")
            candidate = Path(raw).expanduser()
            if not candidate.is_absolute():
                candidate = repo_root / candidate
            candidate = candidate.resolve()
            if not candidate.is_file():
                raise ValueError(f"F05 Boolean cooldown artifact missing: {name}")
            return candidate

        policy_path = resolve_artifact("boolean_cooldown_policy_path")
        bundle_path = resolve_artifact("boolean_cooldown_predicate_bundle_path")
        # YAML supplies locators only. The checked file bytes determine the
        # leaves that the deployment envelope will bind.
        policy_sha = _sha256(policy_path)
        bundle_sha = _sha256(bundle_path)
        runtime = LiveBooleanCooldownPolicy.from_files(
            policy_path=policy_path,
            policy_sha256=policy_sha,
            predicate_bundle_path=bundle_path,
            predicate_bundle_sha256=bundle_sha,
            warmup_s=float(strategy.get("boolean_cooldown_ema_warmup_s", 0.0)),
            max_feature_age_s=float(risk["max_exec_book_visible_age_s"]),
        )
        f05_artifact_identity = {
            "enabled": True,
            "policy_path": _identity_path(policy_path, repo_root),
            "policy_sha256": runtime.evaluator.policy_sha256,
            "predicate_bundle_path": _identity_path(bundle_path, repo_root),
            "predicate_bundle_sha256": (
                runtime.evaluator.predicate_bundle_sha256
            ),
            "selected_predicates": list(runtime.evaluator.predicate_columns),
            "ema_warmup_s": float(
                strategy.get("boolean_cooldown_ema_warmup_s", 0.0)
            ),
        }

    buy_e3_enabled = bool(strategy.get("buy_e3_cooldown_policy_enabled", False))
    buy_e3_artifact_identity: dict[str, Any] = {"enabled": False}
    if buy_e3_enabled:
        from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy

        if not math.isclose(
            float(strategy.get("fill_cooldown", 0.0)),
            85.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("F05 BUY E3 requires fill_cooldown=85")
        if bool(strategy.get("adaptive_add_cooldown_enabled", False)):
            raise ValueError("F05 BUY E3 requires adaptive add cooldown OFF")
        if str(
            strategy.get("fill_cooldown_consecutive_reset_policy", "")
        ).strip() != "opposite_fill_only":
            raise ValueError("F05 BUY E3 requires opposite_fill_only reset")

        def resolve_buy_e3_artifact(name: str) -> Path:
            raw = str(strategy.get(name, "")).strip()
            if not raw:
                raise ValueError(f"F05 BUY E3 requires strategy.{name}")
            candidate = Path(raw).expanduser()
            if not candidate.is_absolute():
                candidate = repo_root / candidate
            candidate = candidate.resolve()
            if not candidate.is_file():
                raise ValueError(f"F05 BUY E3 artifact missing: {name}")
            return candidate

        manifest_path = resolve_buy_e3_artifact(
            "buy_e3_cooldown_artifact_manifest_path"
        )
        buy_policy_path = resolve_buy_e3_artifact("buy_e3_cooldown_policy_path")
        buy_bundle_path = resolve_buy_e3_artifact(
            "buy_e3_cooldown_predicate_bundle_path"
        )
        manifest_file_sha256 = _sha256(manifest_path)
        artifact_sha256 = _buy_e3_artifact_sha256(manifest_path)
        policy_file_sha256 = _sha256(buy_policy_path)
        predicate_bundle_file_sha256 = _sha256(buy_bundle_path)
        runtime = LiveBuyE3CooldownPolicy.from_files(
            artifact_manifest_path=manifest_path,
            artifact_manifest_sha256=manifest_file_sha256,
            expected_artifact_sha256=artifact_sha256,
            policy_path=buy_policy_path,
            policy_sha256=policy_file_sha256,
            predicate_bundle_path=buy_bundle_path,
            predicate_bundle_sha256=predicate_bundle_file_sha256,
            warmup_s=float(strategy.get("buy_e3_cooldown_ema_warmup_s", 0.0)),
            max_feature_age_s=float(risk["max_exec_book_visible_age_s"]),
        )
        buy_e3_artifact_identity = {
            "enabled": True,
            "artifact_manifest_path": _identity_path(manifest_path, repo_root),
            "artifact_manifest_sha256": manifest_file_sha256,
            "artifact_sha256": runtime.artifact_sha256,
            "policy_path": _identity_path(buy_policy_path, repo_root),
            "policy_sha256": runtime.evaluator.policy_sha256,
            "predicate_bundle_path": _identity_path(buy_bundle_path, repo_root),
            "predicate_bundle_sha256": runtime.evaluator.predicate_bundle_sha256,
        }

    policy_admission: str | dict[str, Any] = (
        "not_evaluated_requires_deployment_envelope"
    )
    startup_gates_not_validated = [
        "deployment_envelope",
        "policy_approvals",
        "locked_runtime",
        "stopped_exchange_reconciliation",
    ]
    if check_policy_approval:
        from types import SimpleNamespace

        from live.runtime_policy import (
            admit_runtime_policies,
            deployment_envelope_runtime_authority,
        )
        from strategy.maker_engine import validate_live_artifact_authority

        deployment_authority = deployment_envelope_runtime_authority()
        if config_sha256 != deployment_authority["config_file_sha256"]:
            raise ValueError("deploy config differs from deployment envelope")
        validate_live_artifact_authority(
            SimpleNamespace(strategy=SimpleNamespace(**strategy), ml=SimpleNamespace(**ml)),
            artifact_authority=deployment_authority,
            model_authorization_path=model_authorization_path,
            p3_path=p3_path,
        )
        policy_admission = admit_runtime_policies(
            strategy,
            deployment_authority=deployment_authority,
        )
        startup_gates_not_validated = [
            "locked_runtime",
            "stopped_exchange_reconciliation",
        ]

    state_mode = str(
        strategy.get("state_conditioned_policy_mode", "disabled") or "disabled"
    ).strip().lower()
    state_policy_identity: dict[str, Any] = {"mode": state_mode}
    if state_mode != "disabled":
        from strategy.state_conditioned_quote_policy import StateConditionedQuotePolicy

        path_text = str(strategy.get("state_conditioned_policy_model_path", "")).strip()
        if not path_text:
            raise ValueError("state-conditioned policy requires an artifact path")
        state_path = Path(path_text).expanduser()
        if not state_path.is_absolute():
            state_path = repo_root / state_path
        expected_state_sha256 = (
            deployment_authority["model_policy_member_sha256"]["state_conditioned_quote_policy"]
            if check_policy_approval else None
        )
        state_policy = StateConditionedQuotePolicy.load(
            state_path, mode=state_mode, expected_sha256=expected_state_sha256,
        )
        state_policy_identity["policy_id"] = state_policy.artifact.policy_id
        state_policy_identity["path"] = _identity_path(state_path.resolve(), repo_root)

    return {
        "config_path": str(config_path),
        "config_sha256": config_sha256,
        "model_dir": _identity_path(model_dir, repo_root),
        "model_authorization_path": (
            _identity_path(model_authorization_path, repo_root)
            if model_authorization_path is not None else None
        ),
        "p3_path": _identity_path(p3_path, repo_root),
        "p3_sha256": p3_identity["artifact_sha256"],
        "p3_schema": str(p3.get("schema_version") or ""),
        "p3_event_type": str(p3_identity["event_type"]),
        "p3_horizon_s": float(p3_identity["horizon_s"]),
        "p3_distance_origin": str(p3_identity["distance_origin"]),
        "p3_distance_unit": str(p3_identity["distance_unit"]),
        "p3_side": str(p3_identity["side"]),
        "p3_queue_included": bool(p3_identity["queue_included"]),
        "p3_artifact_sha256": str(p3_identity["artifact_sha256"]),
        "delta_star": delta_star,
        "artifact_kappa_eff": artifact_kappa,
        "override": override,
        "effective_kappa": override if override > 0.0 else artifact_kappa,
        "effective_source": "config_override" if override > 0.0 else "artifact",
        "ml_enabled": ml_enabled,
        "state_conditioned_policy": state_policy_identity,
        "buy_fill_selection_shadow_enabled": bool(
            strategy.get("buy_fill_selection_shadow_enabled", False)
        ),
        "buy_fill_selection_live_enabled": bool(
            strategy.get("buy_fill_selection_live_enabled", False)
        ),
        "required_model_heads": list(REQUIRED_MODEL_HEADS) if ml_enabled else [],
        "validated_model_heads": sorted(model_metadata),
        "model_promotion_authority": PRIVATE_DEPLOYMENT_AUTHORITY if ml_enabled else None,
        "model_live_authorized": True if ml_enabled else None,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID if ml_enabled else None,
        "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256 if ml_enabled else None,
        "quote_horizon_s": quote_horizon_s,
        "quote_unit_contract": {
            "inventory_reference_qty": q_ref,
            "eta_inventory": strategy.get("eta_inventory"),
            "a_spread": strategy.get("a_spread"),
            "risk_per_order": strategy.get("risk_per_order"),
            "execution_intensity_slope": strategy.get(
                "execution_intensity_slope"
            ),
            "risk_horizon_s": strategy.get("risk_horizon_s"),
            "historical_p3_scalar_adapter_enabled": bool(
                strategy.get("historical_p3_scalar_adapter_enabled", True)
            ),
            "p3_side_bbo_floor_enabled": bool(
                strategy.get("p3_side_bbo_floor_enabled", False)
            ),
        },
        "use_bar_pricing": bool(strategy.get("use_bar_pricing", True)),
        **clock_limits,
        "dynamic_fill_hazard_action_enabled": bool(
            strategy.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "f05_boolean_cooldown_enabled": bool(
            strategy.get("boolean_cooldown_policy_enabled", False)
        ),
        "f05_boolean_cooldown_artifacts": f05_artifact_identity,
        "f05_buy_e3_enabled": buy_e3_enabled,
        "f05_buy_e3_artifacts": buy_e3_artifact_identity,
        "lifecycle_journal_v2": lifecycle_identity,
        "validation_scope": "config_model_p3_and_enabled_policy_artifacts",
        "policy_admission": policy_admission,
        "startup_gates_not_validated": startup_gates_not_validated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument(
        "--check-policy-approval",
        action="store_true",
        help="admit enabled policies against the invocation-bound deployment envelope",
    )
    args = parser.parse_args()

    try:
        identity = validate_deploy_config(
            args.config,
            args.repo_root.resolve(),
            check_policy_approval=bool(args.check_policy_approval),
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        parser.error(str(exc))

    print(json.dumps(identity, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
