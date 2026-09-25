"""Runtime verification for immutable execution-v1 model bundles."""

import hashlib
import json
from pathlib import Path


def digest(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("regular model artifact required")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_public_bundle(root, *, expected_symbol="BTCUSDC", live=False):
    from data.observation import CONTRACT, FEATURE_CONTRACT, EXECUTION_FEATURE_NAMES
    from data.tardis_input import CONTRACT as INPUT
    from strategy.model_contract import (
        REQUIRED_MODEL_HEADS, ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
        validate_variance_unit_contract,
    )
    root = Path(root)
    manifest_path = root / "public_input_model.json"
    manifest_digest = digest(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    expected = dict(schema="narrowgate.semantic_model_bundle.v1", symbol=expected_symbol, input_contract_id=INPUT,
                    observation_contract_id=CONTRACT, feature_contract_id=FEATURE_CONTRACT,
                    reference_market=None)
    if any(manifest.get(k) != v for k, v in expected.items()):
        raise ValueError("public model input contract mismatch")
    if not manifest.get("label_contract_id") or not manifest.get("split_manifest_id"):
        raise ValueError("public model label/split binding required")
    if set(manifest.get("heads", {})) != set(REQUIRED_MODEL_HEADS):
        raise ValueError("complete 13-head public model required")
    metadata = {}
    schemas = set()
    for name in REQUIRED_MODEL_HEADS:
        spec = manifest["heads"][name]
        path = root / f"{name}_meta.json"
        if digest(path) != spec.get("metadata_sha256") or digest(root / f"{name}.txt") != spec.get("sha256"):
            raise ValueError("public model artifact identity mismatch")
        meta = json.loads(path.read_text())
        if (meta.get("feature_timestamp_semantics") != "feature_ready_index"
                or meta.get("feature_cutoff_semantics") != "feature_ready_index"
                or "feature_bucket_ms" in meta):
            raise ValueError("current model requires unambiguous feature_ready_index metadata; offline migration required")
        names = spec.get("feature_cols")
        if (not names or len(names) != len(set(names)) or not set(names) <= set(EXECUTION_FEATURE_NAMES)
                or meta.get("feature_cols") != names or spec.get("missing_policy") != "native_nan"):
            raise ValueError("public model feature schema mismatch")
        keys = ("input_contract_id", "observation_contract_id", "feature_contract_id",
                "label_contract_id", "split_manifest_id")
        if meta.get("name") != name or any(meta.get(k) != manifest[k] for k in keys):
            raise ValueError("mixed head contract")
        selection = dict(meta.get("train_only_selection") or {})
        selection.pop("spec_path", None)
        required = ("spec_sha256", "feature_manifest_sha256", "feature_dag_sha256",
                    "source_manifest_sha256", "train_source_identity_sha256", "fit_days",
                    "selection_days", "refit_days", "sample_weight_policy")
        if (any(not selection.get(k) for k in required)
                or not isinstance(selection.get("sample_weight_policy"), dict)
                or selection["sample_weight_policy"].get("half_life_days") not in ("inf", 240, 120, 60)):
            raise ValueError("incomplete training identity")
        identity = {**{k: manifest[k] for k in keys}, "selection": selection}
        if identity != manifest.get("training_identity") or selection.get("external_panel_read_during_fit") is not False:
            raise ValueError("mixed training identity")
        validate_variance_unit_contract(meta.get("volatility_unit_contract"), symbol=expected_symbol)
        if name.startswith("absolute_price_variance_rate_") and meta.get("label_semantics") != ABSOLUTE_PRICE_VARIANCE_SEMANTICS:
            raise ValueError("public model variance label mismatch")
        schemas.add(tuple(names))
        metadata[name] = {**meta, **spec}
    if len(schemas) != 1:
        raise ValueError("public model heads have different input schemas")
    if live:
        fixture_path = root / "fixture_manifest.json"
        if fixture_path.exists():
            digest(fixture_path)
            fixture = json.loads(fixture_path.read_text())
            if fixture.get("synthetic") is True:
                raise ValueError("synthetic model bundle cannot enter remote deployment")
            if not isinstance(fixture.get("authority"), dict) or fixture["authority"].get("live") is not True:
                raise ValueError("bundle fixture manifest requires authority.live=true")
        authorization_path = root / "live_input_authorization.json"
        digest(authorization_path)
        authorization = json.loads(authorization_path.read_text())
        if (authorization.get("schema") != "narrowgate.execution_v1_live_authorization.v1"
                or authorization.get("model_manifest_sha256") != manifest_digest
                or authorization.get("feature_contract_id") != FEATURE_CONTRACT
                or authorization.get("trade_source") != "binance_usdm_individual_trade"
                or authorization.get("owner_authorized") is not True
                or authorization.get("economic_promotion_claim") is not False):
            raise ValueError("explicit hash-bound live input authorization required")
        p3_path = root / "touch_probability.json"
        if p3_path.exists() and authorization.get("p3_sha256") != digest(p3_path):
            raise ValueError("P3 hash mismatch in live input authorization")
    return metadata
