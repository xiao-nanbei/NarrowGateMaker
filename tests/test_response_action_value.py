import copy
import pickle

import pytest

from strategy.response_action_features import ABLATION, BASE, CONTEXT, FEATURES, HISTORY
from strategy.response_action_value import ResponseActionValue


def artifact(intercept=0):
    return dict(scope='modeled_action_labels_not_account_pnl', final_used=False, models={
        path: dict(features=list(names), means=[0.] * len(names), scales=[1.] * len(names),
                   coefficients=[0.] * len(names), intercept=intercept, roles=['opener'])
        for path, names in [('TRADE_BOOK_RESPONSE_VALUE', FEATURES),
                            ('RESPONSE_HISTORY_ABLATION', ABLATION)]})


def observation():
    return dict(role='opener', features=dict.fromkeys(BASE + HISTORY, 1.),
                action_context=dict.fromkeys(CONTEXT, 1.),
                optional_update_admission=dict(decision_admissible=True, reasons=[]))


@pytest.mark.parametrize('path', ['TRADE_BOOK_RESPONSE_VALUE', 'RESPONSE_HISTORY_ABLATION'])
def test_intent_sign_tie_and_pickle(path):
    row = observation()
    for value in (-1., 0., 1.):
        model = ResponseActionValue.from_fitted(artifact(value), path)
        restored = pickle.loads(pickle.dumps(model))
        for baseline in (False, True):
            actual = model.choose(row, baseline=baseline)
            assert actual[0] == (baseline if value == 0 else value > 0)
            assert actual == restored.choose(row, baseline=baseline)


def test_admission_missing_and_role_never_force_an_action():
    model = ResponseActionValue.from_fitted(artifact(1), 'TRADE_BOOK_RESPONSE_VALUE')
    for change in ('admission', 'feature', 'role'):
        row = observation()
        if change == 'admission':
            row['optional_update_admission']['reasons'] = ['pending']
        elif change == 'feature':
            row['features']['spread'] = None
        else:
            row['role'] = 'reducing'
        assert model.choose(row, baseline=False)[:2] == (False, None)


def test_schema_rejects_reordered_features_and_invalid_scale():
    original = artifact()
    for field in ('features', 'scales'):
        item = copy.deepcopy(original)
        model = item['models']['TRADE_BOOK_RESPONSE_VALUE']
        if field == 'features':
            model[field].reverse()
        else:
            model[field][0] = 0
        with pytest.raises(ValueError):
            ResponseActionValue.from_fitted(item, 'TRADE_BOOK_RESPONSE_VALUE')
