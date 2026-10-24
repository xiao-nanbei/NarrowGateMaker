"""T-only date-blocked, purged Ridge fits for the optional-update label."""
from collections import Counter
from datetime import date

import numpy as np

from research.families.f03_causal_13_head.time_weighted_evaluation import SPLITS
from strategy.response_action_features import FEATURES, ABLATION

ALPHAS = (.1, 1., 10.)
DAY = 86_400_000_000_000


def day_ns(day):
    return (date.fromisoformat(day)-date(1970, 1, 1)).days*DAY


def weights(rows):
    counts = Counter(r['day'] for r in rows)
    return np.array([(SPLITS['T'].index(r['day'])+1)/100/counts[r['day']] for r in rows])


def fit(rows, names, alpha):
    x = np.array([[r['features'][n] for n in names] for r in rows], dtype=float)
    y = np.array([r['delta_usdc'] for r in rows], dtype=float)
    if not len(y) or not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise ValueError('finite supported training labels/features required')
    w = weights(rows)
    w /= w.sum()
    means = w @ x
    scales = np.sqrt(w @ ((x-means)**2))
    scales[scales == 0] = 1.
    z = (x-means)/scales
    intercept = float(w @ y)
    beta = np.linalg.solve(z.T @ (w[:, None]*z)+alpha*np.eye(len(names)), z.T @ (w*(y-intercept)))
    return dict(features=list(names), means=means.tolist(), scales=scales.tolist(),
                coefficients=beta.tolist(), intercept=intercept, alpha=alpha,
                roles=sorted({r['role'] for r in rows}))


def predict(model, row):
    if row['role'] not in model['roles']:
        return None
    values = np.array([row['features'][n] for n in model['features']], dtype=float)
    if not np.all(np.isfinite(values)):
        return None
    return float(model['intercept']+((values-model['means'])/model['scales']) @ model['coefficients'])


def train_pair(rows):
    """Exactly the same rows/folds/weights for main and strong ablation."""
    rows = list(rows)
    if not rows or any(r['day'] not in SPLITS['T'] for r in rows):
        raise ValueError('fitting accepts T dates only; diagnostics must be separated')
    identities = [(r['day'], r['slot']) for r in rows]
    if len(identities) != len(set(identities)):
        raise ValueError('duplicate label slot')
    for row in rows:
        start, end = row['decision_ns'], row['outcome_end_ns']
        if (not day_ns(row['day']) <= start < end < day_ns(row['day'])+DAY
                or end-start != 30_000_000_000 or not row['valid']):
            raise ValueError('invalid main label boundary/support')
    folds = []
    for block in range(5):
        days = SPLITS['T'][block*20:(block+1)*20]
        lower, upper = day_ns(days[0]), day_ns(days[-1])+DAY
        validation = [r for r in rows if r['day'] in days]
        training = [r for r in rows if r['day'] not in days and
                    (r['outcome_end_ns'] <= lower or r['decision_ns'] >= upper)]
        if not training or not validation:
            raise ValueError('incomplete date-block support; do not fit on diagnostic roots')
        folds.append((training, validation))
    output = dict(scope='modeled_action_labels_not_account_pnl', models={},
                  input_rows=len(rows), training_days=sorted({r['day'] for r in rows}),
                  folds=[dict(train=len(t), validation=len(v)) for t, v in folds],
                  alpha_candidates=list(ALPHAS), final_used=False)
    for name, names in (('TRADE_BOOK_RESPONSE_VALUE', FEATURES), ('RESPONSE_HISTORY_ABLATION', ABLATION)):
        scores = {}
        for alpha in ALPHAS:
            numerator = denominator = 0.
            for training, validation in folds:
                model = fit(training, names, alpha)
                predicted = [predict(model, row) for row in validation]
                if any(p is None for p in predicted):
                    raise ValueError('held-out role has no training support; report before fitting')
                w = weights(validation)
                errors = np.array(predicted)-np.array([r['delta_usdc'] for r in validation])
                numerator += float(w @ errors**2)
                denominator += float(w.sum())
            scores[alpha] = numerator/denominator
        chosen = min(ALPHAS, key=lambda a: (scores[a], -a))
        output['models'][name] = dict(fit(rows, names, chosen), cv_mse=scores)
    return output
