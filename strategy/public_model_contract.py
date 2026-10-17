"""Runtime verification for immutable execution-v1 model bundles."""

import json
from pathlib import Path


class ValidatedModelMetadata(dict):
    """One call's validation result; not a persistent admission cache."""

    def __init__(self, metadata, *, root, models):
        super().__init__(metadata)
        self.root = Path(root).resolve()
        self.models = models


def training_selection_contract(selection):
    """Training support and policy; file provenance is not compatibility."""
    return {key: value for key, value in selection.items()
            if key not in {"spec_path", "spec_sha256", "feature_manifest_sha256",
                           "feature_dag_sha256", "source_manifest_sha256",
                           "train_source_identity_sha256"}}


def validate_public_bundle(root, *, expected_symbol="BTCUSDC", live=False):
    from data.observation import CONTRACT, FEATURE_CONTRACT, EXECUTION_FEATURE_NAMES
    from data.tardis_input import CONTRACT as INPUT
    from strategy.model_contract import (
        REQUIRED_MODEL_HEADS, ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
        validate_variance_unit_contract,
    )
    root = Path(root)
    manifest_path = root / "public_input_model.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError("public model manifest must be a regular model artifact")
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
    import lightgbm as lgb
    metadata, models = {}, {}
    schemas = set()
    for name in REQUIRED_MODEL_HEADS:
        spec = manifest["heads"][name]
        path = root / f"{name}_meta.json"
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
        selection = training_selection_contract(meta.get("train_only_selection") or {})
        # Research-plan admission belongs to the producer's
        # head_training_identity(), not inference. Preserve provenance and
        # semantic head-to-manifest consistency without imposing its search grid.
        identity = {**{k: manifest[k] for k in keys}, "selection": selection}
        declared_identity = dict(manifest.get("training_identity") or {})
        declared_identity["selection"] = training_selection_contract(declared_identity.get("selection") or {})
        if not selection or identity != declared_identity:
            raise ValueError("mixed training identity")
        validate_variance_unit_contract(meta.get("volatility_unit_contract"), symbol=expected_symbol)
        if name.startswith("absolute_price_variance_rate_") and meta.get("label_semantics") != ABSOLUTE_PRICE_VARIANCE_SEMANTICS:
            raise ValueError("public model variance label mismatch")
        schemas.add(tuple(names))
        metadata[name] = {**meta, **spec}
        try:
            model = lgb.Booster(model_file=str(root / f"{name}.txt"))
        except lgb.basic.LightGBMError as exc:
            raise ValueError("public model file cannot load") from exc
        if model.feature_name() != names or model.num_feature() != len(names):
            raise ValueError("public model file feature schema mismatch")
        models[name] = model
    if len(schemas) != 1:
        raise ValueError("public model heads have different input schemas")
    if live:
        fixture_path = root / "fixture_manifest.json"
        if fixture_path.exists():
            fixture = json.loads(fixture_path.read_text())
            if fixture.get("synthetic") is True:
                raise ValueError("synthetic model bundle cannot enter remote deployment")
    return ValidatedModelMetadata(
        metadata, root=root, models=models,
    )
