"""Independent Final identity for the valid-label-uniform 300-day bundle."""
from research.families.f04_external_market_alpha import final_b0 as final

CANDIDATE = 'F03_BTCUSDC_TRAIN300_13HEAD_EQUAL_LABEL_20261015'
EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_F03_EQUAL_LABEL_FINAL107_20261015'
ARMS = (CANDIDATE,)


def identity(spec):
    return final.identity(spec, experiment=EXPERIMENT, arms=ARMS)


def parameters(original_parent):
    return final.parameters(original_parent, CANDIDATE, arms=ARMS)


def validate_cell(directory, spec):
    return final.validate_cell(directory, spec, experiment=EXPERIMENT, arms=ARMS)


def execute_cell(prepared, **kwargs):
    return final.execute_cell(prepared, experiment=EXPERIMENT, arms=ARMS, **kwargs)
