"""Strict runtime contract for a configured 13-head LightGBM bundle."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH

REQUIRED_MODEL_HEADS = (
    "dir_10s", "dir_30s", "dir_60s",
    "vol_10s", "vol_30s", "vol_60s",
    "ret_10s", "ret_30s", "ret_60s",
    "tox_bid_5s", "tox_ask_5s",
    "tox_bid_10s", "tox_ask_10s",
)

ABSOLUTE_PRICE_VARIANCE_SEMANTICS = "fixed_forward_h_absolute_price_variance"
VARIANCE_UNIT_CONTRACT_SCHEMA = "narrowgate.absolute_price_variance_unit_contract.v1"
ABSOLUTE_PRICE_VARIANCE_UNITS = "(quote/base)^2_per_second"
ABSOLUTE_PRICE_VARIANCE_SAMPLE_PERIOD_S = 1.0
REQUIRED_FEATURE_SEMANTICS_VERSION = 6
REQUIRED_FEATURE_DAG_ID = TEN_SECOND_CAUSAL_GRAPH.graph_id
REQUIRED_FEATURE_DAG_SHA256 = TEN_SECOND_CAUSAL_GRAPH.sha256()
REQUIRED_LABEL_SEMANTICS_VERSION = 3
REQUIRED_LABEL_WINDOW_SEMANTICS = "left_closed_right_open_[t,t+h)"
REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS = (
    "preserve_datetime_physical_unit_ms_us_ns_before_epoch_conversion"
)
PRIVATE_DEPLOYMENT_AUTHORITY = "private_deployment_authorized"
DEPLOYMENT_AUTHORIZATION_SCHEMA = "narrowgate.private_deployment_authorization.v1"
LEGACY_OWNER_AUTHORIZED_LIVE_CANARY = "owner_authorized_live_canary"
LEGACY_LIVE_CANARY_AUTHORIZATION_SCHEMA = (
    "narrowgate.owner_authorized_live_canary.v1"
)
PUBLIC_SYNTHETIC_MANIFEST_SCHEMA = "narrowgate_public_dry_run_model_bundle.v1"
NON_LIVE_PROMOTION_AUTHORITIES = frozenset(
    {"public_dry_run_only", "research_only"}
)
F03_DIRECT_QUOTE_ACTION_SCHEMA = "narrowgate.f03.direct_quote_action.v1"
F03_DIRECT_QUOTE_ACTION_EVENT_TYPE = "decision_to_fixed_horizon_return"
F03_DIRECT_QUOTE_ACTION_PRICE_ORIGIN = "decision_mid"
F03_DIRECT_QUOTE_ACTION_RETURN_UNIT = "fraction"
F03_DIRECT_QUOTE_ACTION_CONSUMER = "quote_center_shift"

_MODEL_QUOTE_ASSET_SUFFIXES = (
    "FDUSD",
    "USDC",
    "USDT",
    "BUSD",
    "TUSD",
    "DAI",
    "USD",
)
_LEGACY_VARIANCE_UNIT_IDENTITIES = (
    {
        "legacy_identity": "causal_v12_live_canary_feature_manifest_5409a398",
        "symbol": "BTCUSDC",
        "feature_manifest_sha256": (
            "5409a398d845eaf9a990dbf4f390cfa3aeff2b7dd014fd02d70b303a2f8a557f"
        ),
        "training_experiment_id": "causal_v12_expanded_source_aware_semantics_v6",
        "promotion_authority": LEGACY_OWNER_AUTHORIZED_LIVE_CANARY,
        "canonical_promotion_authority": PRIVATE_DEPLOYMENT_AUTHORITY,
        "source_profile": "all",
        "feature_variant": "base",
    },
    {
        "legacy_identity": "public_dry_run_feature_manifest_ffc85a81",
        "symbol": "BTCUSDC",
        "feature_manifest_sha256": (
            "ffc85a81b177825f43455f346dab8c9926a699c8f7f8e34a2ce9188861eeadd2"
        ),
        "training_experiment_id": "public_dry_run_model_bundle_v1",
        "promotion_authority": "public_dry_run_only",
        "source_profile": "synthetic_fixture",
        "feature_variant": "public_dry_run_fixture",
    },
)


def absolute_price_variance_unit_contract(symbol: str) -> dict[str, Any]:
    """Derive the variance-rate unit contract from one canonical symbol."""
    normalized = str(symbol or "").strip().upper().replace("/", "").replace("-", "")
    for quote_asset in _MODEL_QUOTE_ASSET_SUFFIXES:
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            base_asset = normalized[: -len(quote_asset)]
            return {
                "schema_version": VARIANCE_UNIT_CONTRACT_SCHEMA,
                "symbol": normalized,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "variance_units": ABSOLUTE_PRICE_VARIANCE_UNITS,
                "sample_period_s": ABSOLUTE_PRICE_VARIANCE_SAMPLE_PERIOD_S,
            }
    raise ValueError(f"cannot derive base/quote assets from model symbol {symbol!r}")


def validate_variance_unit_contract(
    contract: Any,
    *,
    symbol: str,
) -> dict[str, Any]:
    expected = absolute_price_variance_unit_contract(symbol)
    if not isinstance(contract, dict) or contract != expected:
        raise ValueError(
            "volatility_unit_contract must exactly match the symbol-derived "
            "absolute-price variance-rate contract"
        )
    return dict(expected)


def canonicalize_model_variance_unit_contract(meta: dict[str, Any]) -> dict[str, Any]:
    """Validate future metadata or canonicalize an exact hash-bound legacy identity."""
    raw_contract = meta.get("volatility_unit_contract")
    if raw_contract is not None:
        canonical = dict(meta)
        canonical["volatility_unit_contract"] = validate_variance_unit_contract(
            raw_contract,
            symbol=str(meta.get("symbol") or ""),
        )
        return canonical

    identity_keys = (
        "feature_manifest_sha256",
        "training_experiment_id",
        "promotion_authority",
        "source_profile",
        "feature_variant",
    )
    for legacy in _LEGACY_VARIANCE_UNIT_IDENTITIES:
        if all(str(meta.get(key) or "") == legacy[key] for key in identity_keys):
            meta_symbol = str(meta.get("symbol") or legacy["symbol"])
            if meta_symbol != legacy["symbol"]:
                break
            canonical = dict(meta)
            canonical["volatility_unit_contract"] = (
                absolute_price_variance_unit_contract(legacy["symbol"])
            )
            canonical["volatility_unit_contract_origin"] = (
                f"legacy_canonicalized:{legacy['legacy_identity']}"
            )
            canonical_authority = legacy.get("canonical_promotion_authority")
            if canonical_authority:
                canonical["promotion_authority_origin"] = canonical.get(
                    "promotion_authority"
                )
                canonical["promotion_authority"] = canonical_authority
            return canonical
    raise ValueError(
        "metadata requires an explicit symbol-derived volatility_unit_contract; "
        "unregistered legacy metadata cannot be canonicalized"
    )


def f03_direct_quote_action_contract(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Return a fail-closed, explicitly declared F03 quote-action contract.

    Existing F03 ``ret_10s`` labels are fill-conditioned outcomes spanning
    10--20 seconds.  Their historical name is not permission to consume them
    as a point-horizon quote-center action.  A future action model must carry
    this separate contract; absence deliberately returns an incompatible
    identity so ML inference with ``ret_skew == 0`` remains a no-op.
    """

    raw = meta.get("direct_quote_action")
    if raw is None:
        return {"compatible": False, "horizon_s": 0.0}
    if not isinstance(raw, Mapping):
        raise ValueError("F03 direct_quote_action must be a mapping")
    horizon_s = float(raw.get("horizon_s", 0.0) or 0.0)
    expected = {
        "schema_version": F03_DIRECT_QUOTE_ACTION_SCHEMA,
        "compatible": True,
        "event_type": F03_DIRECT_QUOTE_ACTION_EVENT_TYPE,
        "price_origin": F03_DIRECT_QUOTE_ACTION_PRICE_ORIGIN,
        "return_unit": F03_DIRECT_QUOTE_ACTION_RETURN_UNIT,
        "consumer": F03_DIRECT_QUOTE_ACTION_CONSUMER,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise ValueError(
                f"F03 direct_quote_action {key}={raw.get(key)!r}; expected {value!r}"
            )
    if not math.isfinite(horizon_s) or horizon_s <= 0.0:
        raise ValueError("F03 direct_quote_action horizon_s must be finite and positive")
    return {**expected, "horizon_s": horizon_s}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_hash_bound_deployment_authorization(
    root: Path,
    metadata: dict[str, dict[str, Any]],
    *,
    authorization_file: str,
    authorization_schema: str,
    authorization_label: str,
    required_true_fields: tuple[str, ...],
) -> None:
    authorization_path = root / authorization_file
    if not authorization_path.is_file():
        raise ValueError(f"{authorization_label} requires {authorization_file}")
    try:
        authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid {authorization_label}: {exc}") from exc

    if authorization.get("schema_version") != authorization_schema:
        raise ValueError(f"{authorization_label} has incompatible schema")
    for field in required_true_fields:
        if authorization.get(field) is not True:
            raise ValueError(f"{authorization_label} requires {field}=true")
    if "authority" in authorization:
        authority = authorization.get("authority")
        if not isinstance(authority, dict) or authority.get("live") is not True:
            raise ValueError(f"{authorization_label} requires authority.live=true")
    if authorization.get("baseline_promotion_authorized") is not False:
        raise ValueError(
            f"{authorization_label} must keep baseline promotion unauthorized"
        )

    experiment_ids = {
        str(head_metadata.get("training_experiment_id") or "")
        for head_metadata in metadata.values()
    }
    if experiment_ids != {str(authorization.get("training_experiment_id") or "")}:
        raise ValueError(f"{authorization_label} training experiment identity mismatch")

    derived = authorization.get("derived_bundle")
    if not isinstance(derived, dict):
        raise ValueError(f"{authorization_label} lacks derived_bundle")
    expected_trees = derived.get("model_tree_sha256")
    expected_metadata = derived.get("head_metadata_sha256")
    if not isinstance(expected_trees, dict) or not isinstance(expected_metadata, dict):
        raise ValueError(f"{authorization_label} lacks per-head hashes")
    for name in REQUIRED_MODEL_HEADS:
        if _sha256(root / f"{name}.txt") != str(expected_trees.get(name) or ""):
            raise ValueError(f"{authorization_label} model hash mismatch for {name}")
        if _sha256(root / f"{name}_meta.json") != str(
            expected_metadata.get(name) or ""
        ):
            raise ValueError(f"{authorization_label} metadata hash mismatch for {name}")

    p3_path = root / "fill_prob_params.json"
    if p3_path.is_file() and _sha256(p3_path) != str(derived.get("p3_sha256") or ""):
        raise ValueError(f"{authorization_label} P3 hash mismatch")


def _validate_private_deployment_authorization(
    root: Path,
    metadata: dict[str, dict[str, Any]],
) -> None:
    authority_origins = {
        str(head_metadata.get("promotion_authority_origin") or "")
        for head_metadata in metadata.values()
    }
    if authority_origins == {LEGACY_OWNER_AUTHORIZED_LIVE_CANARY}:
        _validate_hash_bound_deployment_authorization(
            root,
            metadata,
            authorization_file="live_canary_authorization.json",
            authorization_schema=LEGACY_LIVE_CANARY_AUTHORIZATION_SCHEMA,
            authorization_label="legacy live canary authorization",
            required_true_fields=(
                "owner_authorized",
                "active_live_inference_authorized",
            ),
        )
        return
    if authority_origins != {""}:
        raise ValueError("private deployment mixes incompatible authorization origins")
    _validate_hash_bound_deployment_authorization(
        root,
        metadata,
        authorization_file="deployment_authorization.json",
        authorization_schema=DEPLOYMENT_AUTHORIZATION_SCHEMA,
        authorization_label="private deployment authorization",
        required_true_fields=(
            "private_deployment_authorized",
            "active_runtime_inference_authorized",
        ),
    )


def _validate_bundle_manifest_live_authority(root: Path) -> None:
    manifest_path = root / "fixture_manifest.json"
    if not manifest_path.is_file():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid bundle fixture manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ValueError("bundle fixture manifest must be a mapping")
    if manifest.get("synthetic") is True:
        raise ValueError("synthetic model bundle cannot enter remote deployment")
    authority = manifest.get("authority")
    if not isinstance(authority, dict) or authority.get("live") is not True:
        raise ValueError("bundle fixture manifest requires authority.live=true")


def _validate_public_synthetic_manifest(root: Path) -> None:
    manifest_path = root / "fixture_manifest.json"
    if not manifest_path.is_file():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid bundle fixture manifest: {exc}") from exc
    if not isinstance(manifest, dict) or manifest.get("synthetic") is not True:
        return
    if manifest.get("schema_version") != PUBLIC_SYNTHETIC_MANIFEST_SCHEMA:
        raise ValueError("public synthetic bundle has incompatible manifest schema")
    if manifest.get("authority") != {
        "action": False,
        "live": False,
        "research": False,
    }:
        raise ValueError("public synthetic bundle authority must remain all false")

    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise ValueError("public synthetic bundle manifest requires a files list")
    expected_names = {
        "fill_prob_params.json",
        *(f"{name}.txt" for name in REQUIRED_MODEL_HEADS),
        *(f"{name}_meta.json" for name in REQUIRED_MODEL_HEADS),
    }
    observed_names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("public synthetic bundle file entry must be a mapping")
        name = str(entry.get("path") or "")
        relative = Path(name)
        if not name or relative.is_absolute() or relative.name != name:
            raise ValueError("public synthetic bundle file path must be one root-level name")
        if name in observed_names:
            raise ValueError(f"public synthetic bundle repeats manifest file {name}")
        observed_names.add(name)
        artifact = root / name
        if artifact.is_symlink() or not artifact.is_file():
            raise ValueError(f"public synthetic bundle is missing manifest file {name}")
        expected_bytes = entry.get("bytes")
        if (
            not isinstance(expected_bytes, int)
            or isinstance(expected_bytes, bool)
            or expected_bytes < 0
        ):
            raise ValueError(f"public synthetic bundle has invalid byte count for {name}")
        if artifact.stat().st_size != expected_bytes:
            raise ValueError(f"public synthetic bundle byte count mismatch for {name}")
        if _sha256(artifact) != str(entry.get("sha256") or ""):
            raise ValueError(f"public synthetic bundle SHA256 mismatch for {name}")
    if observed_names != expected_names:
        raise ValueError("public synthetic bundle manifest file set is incomplete")


def validate_model_bundle(
    model_dir: Path,
    *,
    allow_research_only: bool = False,
    require_live_authorization: bool = False,
    expected_symbol: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Validate every runtime head before any model is admitted.

    ``Prediction.vol_*`` is consumed as absolute price variance in
    ``(quote/base)^2``.  A missing model, feature schema, or incompatible
    volatility label is therefore a startup error rather than a neutral
    fallback.
    """

    root = Path(model_dir).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"model bundle directory does not exist: {root}")

    metadata: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    expected_variance_contract = (
        None
        if expected_symbol is None
        else absolute_price_variance_unit_contract(expected_symbol)
    )
    for name in REQUIRED_MODEL_HEADS:
        model_path = root / f"{name}.txt"
        meta_path = root / f"{name}_meta.json"
        if not model_path.is_file():
            errors.append(f"missing model {model_path.name}")
            continue
        if not meta_path.is_file():
            errors.append(f"missing metadata {meta_path.name}")
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"invalid metadata {meta_path.name}: {exc}")
            continue
        feature_cols = meta.get("feature_cols")
        if not isinstance(feature_cols, list) or not feature_cols:
            errors.append(f"{meta_path.name} requires non-empty feature_cols")
            continue
        if int(meta.get("feature_semantics_version", 0) or 0) != (
            REQUIRED_FEATURE_SEMANTICS_VERSION
        ):
            errors.append(
                f"{meta_path.name} feature_semantics_version="
                f"{meta.get('feature_semantics_version')!r}; expected "
                f"{REQUIRED_FEATURE_SEMANTICS_VERSION}"
            )
            continue
        if str(meta.get("feature_dag_id") or "") != REQUIRED_FEATURE_DAG_ID:
            errors.append(
                f"{meta_path.name} feature_dag_id={meta.get('feature_dag_id')!r}; "
                f"expected {REQUIRED_FEATURE_DAG_ID!r}"
            )
            continue
        if str(meta.get("feature_dag_sha256") or "") != REQUIRED_FEATURE_DAG_SHA256:
            errors.append(
                f"{meta_path.name} has incompatible feature DAG identity"
            )
            continue
        if str(meta.get("calendar_timestamp_semantics") or "") != (
            REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS
        ):
            errors.append(
                f"{meta_path.name} has incompatible calendar timestamp semantics"
            )
            continue
        if int(meta.get("label_semantics_version", 0) or 0) != (
            REQUIRED_LABEL_SEMANTICS_VERSION
        ) or str(meta.get("label_window_semantics") or "") != (
            REQUIRED_LABEL_WINDOW_SEMANTICS
        ):
            errors.append(f"{meta_path.name} has incompatible label semantics")
            continue
        if not str(meta.get("feature_manifest_sha256") or ""):
            errors.append(f"{meta_path.name} requires feature_manifest_sha256")
            continue
        try:
            meta = canonicalize_model_variance_unit_contract(meta)
        except ValueError as exc:
            errors.append(f"{meta_path.name} {exc}")
            continue
        if (
            expected_variance_contract is not None
            and meta["volatility_unit_contract"] != expected_variance_contract
        ):
            errors.append(
                f"{meta_path.name} volatility unit contract does not match "
                f"runtime symbol {expected_variance_contract['symbol']}"
            )
            continue
        promotion_authority = str(meta.get("promotion_authority") or "")
        if require_live_authorization and not promotion_authority:
            errors.append(
                f"{meta_path.name} lacks explicit live promotion_authority"
            )
            continue
        if (
            require_live_authorization
            and promotion_authority != PRIVATE_DEPLOYMENT_AUTHORITY
        ):
            if promotion_authority in NON_LIVE_PROMOTION_AUTHORITIES:
                errors.append(
                    f"{meta_path.name} is {promotion_authority} and cannot enter "
                    "remote deployment"
                )
            else:
                errors.append(
                    f"{meta_path.name} promotion_authority="
                    f"{promotion_authority!r} is not live-authorized"
                )
            continue
        if promotion_authority == "research_only" and not allow_research_only:
            errors.append(f"{meta_path.name} is research_only and cannot enter live")
            continue
        if name.startswith("vol_"):
            semantics = str(meta.get("label_semantics") or "")
            if semantics != ABSOLUTE_PRICE_VARIANCE_SEMANTICS:
                errors.append(
                    f"{meta_path.name} label_semantics={semantics!r}; expected "
                    f"{ABSOLUTE_PRICE_VARIANCE_SEMANTICS!r}"
                )
                continue
        metadata[name] = meta

    try:
        _validate_public_synthetic_manifest(root)
    except ValueError as exc:
        errors.append(str(exc))

    if len(metadata) == len(REQUIRED_MODEL_HEADS):
        manifest_hashes = {
            str(meta["feature_manifest_sha256"]) for meta in metadata.values()
        }
        feature_schemas = {
            tuple(str(column) for column in meta["feature_cols"])
            for meta in metadata.values()
        }
        source_profiles = {
            str(meta.get("source_profile") or "all") for meta in metadata.values()
        }
        feature_variants = {
            str(meta.get("feature_variant") or "base") for meta in metadata.values()
        }
        experiment_ids = {
            str(meta.get("training_experiment_id") or "")
            for meta in metadata.values()
        }
        promotion_authorities = {
            str(meta.get("promotion_authority") or "")
            for meta in metadata.values()
        }
        variance_unit_contracts = {
            json.dumps(
                meta["volatility_unit_contract"],
                sort_keys=True,
                separators=(",", ":"),
            )
            for meta in metadata.values()
        }
        if len(manifest_hashes) != 1:
            errors.append("model heads do not share one feature manifest")
        if len(feature_schemas) != 1:
            errors.append("model heads do not share one feature schema")
        if len(source_profiles) != 1 or len(feature_variants) != 1:
            errors.append("model heads do not share one predictive-ablation identity")
        if len(experiment_ids) != 1:
            errors.append("model heads do not share one training experiment id")
        if len(promotion_authorities) != 1:
            errors.append("model heads do not share one promotion authority")
        if len(variance_unit_contracts) != 1:
            errors.append("model heads do not share one volatility unit contract")

        if not errors and promotion_authorities == {PRIVATE_DEPLOYMENT_AUTHORITY}:
            try:
                _validate_private_deployment_authorization(root, metadata)
                if require_live_authorization:
                    _validate_bundle_manifest_live_authority(root)
            except ValueError as exc:
                errors.append(str(exc))

    if errors:
        raise ValueError("invalid ML bundle: " + "; ".join(errors))
    return metadata
