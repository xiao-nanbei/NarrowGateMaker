"""Development300 bundle on the unchanged urgency-off Final account engine.

No training, dispatch, reference market, control replay or live activation.
"""
from research.families.f03_causal_13_head.development300 import CANDIDATE
from research.families.f04_external_market_alpha import final_b0 as final

EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_F03_TRAIN300_FINAL107_20261014'
ARMS = (CANDIDATE,)


def identity(spec):
    return final.identity(spec, experiment=EXPERIMENT, arms=ARMS)


def parameters(original_parent):
    return final.parameters(original_parent, CANDIDATE, arms=ARMS)


def validate_cell(output, spec):
    return final.validate_cell(output, spec, experiment=EXPERIMENT, arms=ARMS)


def execute_cell(prepared, **kwargs):
    return final.execute_cell(prepared, experiment=EXPERIMENT, arms=ARMS, **kwargs)
