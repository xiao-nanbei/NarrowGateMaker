"""Frozen action-value inference only; order routing and clocks stay external."""
from dataclasses import dataclass
import math

import numpy as np

from strategy.response_action_features import ABLATION, FEATURES, extract


@dataclass(frozen=True)
class ResponseActionValue:
    path_id: str
    names: tuple
    means: tuple
    scales: tuple
    coefficients: tuple
    intercept: float
    roles: tuple

    @classmethod
    def from_fitted(cls, artifact, path_id):
        expected = {'TRADE_BOOK_RESPONSE_VALUE': FEATURES,
                    'RESPONSE_HISTORY_ABLATION': ABLATION}
        if (path_id not in expected or artifact.get('final_used') is not False
                or artifact.get('scope') != 'modeled_action_labels_not_account_pnl'):
            raise ValueError('unsupported action model identity or scope')
        model = artifact['models'][path_id]
        names = tuple(model['features'])
        vectors = [tuple(model[n]) for n in ('means', 'scales', 'coefficients')]
        roles = tuple(model['roles'])
        if (names != expected[path_id] or any(len(v) != len(names) for v in vectors)
                or not roles or len(set(roles)) != len(roles)
                or not set(roles) <= {'opener', 'add', 'reducing'}
                or any(not math.isfinite(x) for v in vectors for x in v)
                or any(x <= 0 for x in vectors[1])
                or not math.isfinite(model['intercept'])):
            raise ValueError('invalid action model schema or coefficients')
        return cls(path_id, names, *vectors, float(model['intercept']), roles)

    def choose(self, observation, *, baseline):
        """Return intent, not a sent/effective order; never relax admission."""
        admission = observation.get('optional_update_admission', {})
        if (admission.get('decision_admissible') is not True
                or admission.get('reasons') != []):
            return baseline, None, 'not_admissible'
        if observation.get('role') not in self.roles:
            return baseline, None, 'unsupported_role'
        # Both arms require the same full visible-state support. The ablation
        # masks history only in its coefficients, not in sidecar availability.
        if not isinstance(observation.get('features'), dict):
            return baseline, None, 'unsupported_features'
        features = extract(observation)
        if features is None:
            return baseline, None, 'unsupported_features'
        values = np.array([features[n] for n in self.names], dtype=float)
        value = float(self.intercept + ((values - self.means) / self.scales)
                      @ self.coefficients)
        if not math.isfinite(value):
            return baseline, None, 'nonfinite_prediction'
        if value == 0:
            return baseline, value, 'tie_baseline'
        return value > 0, value, 'model_intent'
