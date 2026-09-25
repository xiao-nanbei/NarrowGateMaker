"""Synthetic current-schema validator fixtures, not trained models."""
import hashlib
import json

from data.observation import CONTRACT, FEATURE_CONTRACT
from data.tardis_input import CONTRACT as INPUT
from strategy.model_contract import (
    REQUIRED_MODEL_HEADS, ABSOLUTE_PRICE_VARIANCE_SEMANTICS,
    absolute_price_variance_unit_contract,
)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_bundle(root, *, variance_semantics=ABSOLUTE_PRICE_VARIANCE_SEMANTICS):
    root.mkdir(parents=True, exist_ok=True)
    identity = dict(input_contract_id=INPUT, observation_contract_id=CONTRACT,
                    feature_contract_id=FEATURE_CONTRACT, label_contract_id='synthetic',
                    split_manifest_id='synthetic-no-research')
    selection = dict(spec_sha256='spec', feature_manifest_sha256='features',
                     feature_dag_sha256='dag', source_manifest_sha256='source',
                     train_source_identity_sha256='inputs', fit_days=['fit'],
                     selection_days=['select'], refit_days=['fit', 'select'],
                     sample_weight_policy={'half_life_days': 'inf'},
                     external_panel_read_during_fit=False)
    heads = {}
    for name in REQUIRED_MODEL_HEADS:
        model, meta = root / f'{name}.txt', root / f'{name}_meta.json'
        model.write_text('synthetic-validator-placeholder')
        meta.write_text(json.dumps({**identity, 'name': name, 'feature_cols': ['mid'],
            'feature_timestamp_semantics': 'feature_ready_index',
            'feature_cutoff_semantics': 'feature_ready_index',
            'symbol': 'BTCUSDC', 'train_only_selection': selection,
            'volatility_unit_contract': absolute_price_variance_unit_contract('BTCUSDC'),
            'label_semantics': variance_semantics}))
        heads[name] = dict(sha256=sha(model), metadata_sha256=sha(meta),
                           feature_cols=['mid'], missing_policy='native_nan')
    manifest = {**identity, 'schema': 'narrowgate.semantic_model_bundle.v1',
                'symbol': 'BTCUSDC', 'reference_market': None, 'heads': heads,
                'training_identity': {**identity, 'selection': selection}}
    (root / 'public_input_model.json').write_text(json.dumps(manifest))


def bind_changed_metadata(root, head):
    path = root / 'public_input_model.json'
    manifest = json.loads(path.read_text())
    manifest['heads'][head]['metadata_sha256'] = sha(root / f'{head}_meta.json')
    path.write_text(json.dumps(manifest))


def authorize_current_bundle(root):
    payload = {
        'schema': 'narrowgate.execution_v1_live_authorization.v1',
        'model_manifest_sha256': sha(root / 'public_input_model.json'),
        'feature_contract_id': FEATURE_CONTRACT,
        'trade_source': 'binance_usdm_individual_trade', 'owner_authorized': True,
        'economic_promotion_claim': False,
    }
    p3 = root / 'touch_probability.json'
    if p3.exists():
        payload['p3_sha256'] = sha(p3)
    (root / 'live_input_authorization.json').write_text(json.dumps(payload))
