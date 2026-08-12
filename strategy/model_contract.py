"""Strict runtime contract for the active 13-head LightGBM bundle."""

from __future__ import annotations

import hashlib
import json
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
REQUIRED_FEATURE_SEMANTICS_VERSION = 6
REQUIRED_FEATURE_DAG_ID = TEN_SECOND_CAUSAL_GRAPH.graph_id
REQUIRED_FEATURE_DAG_SHA256 = TEN_SECOND_CAUSAL_GRAPH.sha256()
REQUIRED_LABEL_SEMANTICS_VERSION = 3
REQUIRED_LABEL_WINDOW_SEMANTICS = "left_closed_right_open_[t,t+h)"
REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS = (
    "preserve_datetime_physical_unit_ms_us_ns_before_epoch_conversion"
)
OWNER_AUTHORIZED_LIVE_CANARY = "owner_authorized_live_canary"
LIVE_CANARY_AUTHORIZATION_SCHEMA = "narrowgate.owner_authorized_live_canary.v1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_owner_authorized_live_canary(
    root: Path,
    metadata: dict[str, dict[str, Any]],
) -> None:
    authorization_path = root / "live_canary_authorization.json"
    if not authorization_path.is_file():
        raise ValueError(
            "owner-authorized live canary requires live_canary_authorization.json"
        )
    try:
        authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid live canary authorization: {exc}") from exc

    if authorization.get("schema_version") != LIVE_CANARY_AUTHORIZATION_SCHEMA:
        raise ValueError("live canary authorization has incompatible schema")
    if authorization.get("owner_authorized") is not True:
        raise ValueError("live canary authorization requires owner_authorized=true")
    if authorization.get("active_live_inference_authorized") is not True:
        raise ValueError(
            "live canary authorization requires active_live_inference_authorized=true"
        )
    if authorization.get("baseline_promotion_authorized") is not False:
        raise ValueError(
            "live canary authorization must keep baseline promotion unauthorized"
        )

    experiment_ids = {
        str(head_metadata.get("training_experiment_id") or "")
        for head_metadata in metadata.values()
    }
    if experiment_ids != {str(authorization.get("training_experiment_id") or "")}:
        raise ValueError("live canary training experiment identity mismatch")

    derived = authorization.get("derived_bundle")
    if not isinstance(derived, dict):
        raise ValueError("live canary authorization lacks derived_bundle")
    expected_trees = derived.get("model_tree_sha256")
    expected_metadata = derived.get("head_metadata_sha256")
    if not isinstance(expected_trees, dict) or not isinstance(expected_metadata, dict):
        raise ValueError("live canary authorization lacks per-head hashes")
    for name in REQUIRED_MODEL_HEADS:
        if _sha256(root / f"{name}.txt") != str(expected_trees.get(name) or ""):
            raise ValueError(f"live canary model hash mismatch for {name}")
        if _sha256(root / f"{name}_meta.json") != str(
            expected_metadata.get(name) or ""
        ):
            raise ValueError(f"live canary metadata hash mismatch for {name}")

    p3_path = root / "fill_prob_params.json"
    if p3_path.is_file() and _sha256(p3_path) != str(derived.get("p3_sha256") or ""):
        raise ValueError("live canary P3 hash mismatch")


def validate_model_bundle(
    model_dir: Path,
    *,
    allow_research_only: bool = False,
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
        if (
            str(meta.get("promotion_authority") or "") == "research_only"
            and not allow_research_only
        ):
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

        if not errors and promotion_authorities == {OWNER_AUTHORIZED_LIVE_CANARY}:
            try:
                _validate_owner_authorized_live_canary(root, metadata)
            except ValueError as exc:
                errors.append(str(exc))

    if errors:
        raise ValueError("invalid ML bundle: " + "; ".join(errors))
    return metadata
