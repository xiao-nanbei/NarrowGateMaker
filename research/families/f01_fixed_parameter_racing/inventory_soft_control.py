"""Fixed five-cell inventory study contract; never dispatches or changes live.

The input is each original promoted B0 account's complete effective parameters,
not a current live template or the pre-promotion urgency-on parent.
"""

from copy import deepcopy
from datetime import datetime

from strategy.inventory_soft_control import (  # noqa: F401
    COOLDOWN_KEY, MARKOUT_KEY, consecutive_multiplier, cooldown_blocks_action,
    resolve_modes as modes,
)

from .mechanism_ablation import select_full_parents

EXPERIMENT = "INVENTORY_SOFT_CONTROL_COST_FULL407_20261017"
BASELINE = "B0_DEFENSE_URGENCY_OFF_20261011"
MODES = {
    "B0": ("original", "original_all_roles"),
    "COOLDOWN_NO_CONSECUTIVE_MULTIPLIER": (
        "no_consecutive_multiplier", "original_all_roles"),
    "FILL_COOLDOWN_ADMISSION_OFF": ("admission_off", "original_all_roles"),
    "GENERIC_MARKOUT_SKIP_PURE_REDUCING": (
        "original", "exclude_pure_reducing"),
    "COOLDOWN_NO_MULTIPLIER_AND_REDUCING_MARKOUT_RELEASE": (
        "no_consecutive_multiplier", "exclude_pure_reducing"),
}
ARMS = tuple(MODES)
HOST_LIMITS = {"local": 4, "oci-tokyo": 3, "oci-tokyo-4": 3, "oci-tokyo-6": 3}
MIN_STAGING_FREE_BYTES = 32 * 1024**3
CONTINUOUS_PARENT = "Final107-continuous"
INDEPENDENT_CONTRACT = "INVENTORY_SOFT_CONTROL_407_INDEPENDENT"
CONTINUOUS_CONTRACT = "INVENTORY_SOFT_CONTROL_FINAL107_CONTINUOUS"


def arm_parameters(original_b0_params, arm):
    if arm not in MODES:
        raise ValueError("undeclared inventory soft-control arm")
    if COOLDOWN_KEY in original_b0_params or MARKOUT_KEY in original_b0_params:
        raise ValueError("expected unmodified original B0 effective parameters")
    expected = {
        "exit_urgency_strength": 0, "position_timeout": 0,
        "defense_guard_enabled": True, "defense_pause": True,
        "order_size": .001, "lot_size": .001, "tick_size": .1,
        "max_inventory": .026, "max_position_value": 3000,
        "max_daily_loss": 50, "emergency_close_dd": 150,
        "fill_cooldown": 85, "fill_cooldown_reducing": 0,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "markout_spread_scale": .2, "asym_strength": .1,
        "rng_seed": 42, "latency_seed": 59,
        "response_observe_only": True, "response_update_branch_enabled": True,
    }
    mismatches = [key for key, value in expected.items()
                  if original_b0_params.get(key) != value]
    if mismatches:
        raise ValueError("original B0 parameter mismatch: " + ", ".join(mismatches))
    if original_b0_params.get("_research_generic_markout_policy_enabled", True) is not True:
        raise ValueError("original B0 cannot disable generic markout")
    policy = original_b0_params.get("response_action_policy", {})
    path = "TRADE_BOOK_RESPONSE_VALUE"
    model = policy.get("artifact", {}).get("models", {}).get(path, {})
    cost = policy.get("compute", {})
    if (policy.get("path_id") != path or len(model.get("features", ())) != 47
            or model.get("alpha") != 10
            or cost.get("schema") != "response_compute.v1"
            or cost.get("event_ns") != 109169 or cost.get("call_ns") != 195875
            or cost.get("common_to_both_arms") is not True):
        raise ValueError("original response model/compute contract differs")
    result = deepcopy(original_b0_params)
    # R0's full effective dictionary stays byte-serializably identical.
    if arm != "B0":
        result[COOLDOWN_KEY], result[MARKOUT_KEY] = MODES[arm]
    return result


def validate_effective_params(actual, original_b0_params, arm):
    if actual != arm_parameters(original_b0_params, arm):
        raise ValueError("effective parameters differ outside the fixed cell patch")


def _utc_ns(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()) * 10**9


def identity(spec, original_calendar):
    """Bind original IDs/calendar, rejecting old three-cell completion identities."""
    parents = select_full_parents(original_calendar)
    intervals = {p["shard_id"]: (_utc_ns(p["account_start_utc"]),
                                 _utc_ns(p["account_end_utc_exclusive"] +
                                         ("T00:00:00Z" if len(p["account_end_utc_exclusive"]) == 10 else "")))
                 for p in parents}
    contract = spec.get("account_contract")
    if contract == CONTINUOUS_CONTRACT:
        intervals = {CONTINUOUS_PARENT: (_utc_ns("2026-05-28T00:00:00Z"),
                                         _utc_ns("2026-09-12T00:00:00Z"))}
    elif contract != INDEPENDENT_CONTRACT:
        raise ValueError("wrong inventory account contract")
    if (spec.get("experiment_id") != EXPERIMENT or spec.get("baseline_id") != BASELINE
            or spec.get("arm_id") not in ARMS
            or spec.get("parent_id") not in intervals
            or (spec.get("start_ns"), spec.get("end_ns")) != intervals.get(spec.get("parent_id"))
            or spec.get("seed_pair") != {"rng_seed": 42, "latency_seed": 59}):
        raise ValueError("inventory account identity differs")
    keys = ("experiment_id", "baseline_id", "account_contract", "parent_id", "arm_id",
            "seed_pair", "start_ns", "end_ns", "input_manifest_id", "release_identity")
    for key in ("input_manifest_id", "release_identity"):
        value = spec.get(key)
        if (not isinstance(value, str) or len(value) != 64
                or any(c not in "0123456789abcdef" for c in value)):
            raise ValueError("missing immutable input/release identity")
    return {key: deepcopy(spec[key]) for key in keys}


def independent_cells(original_calendar, baseline_identities, release_identity):
    """Enumerate exactly the four candidates on the original 204 identities.

    Enumeration is not admission or dispatch. There are no result-dependent
    filters, regenerated parent IDs, control reruns or extra mode combinations.
    """
    parents = select_full_parents(original_calendar)
    expected = {p['shard_id'] for p in parents}
    if set(baseline_identities) != expected:
        raise ValueError('baseline identity coverage must be exactly 204 original parents')
    cells = []
    for parent in parents:
        old = baseline_identities[parent['shard_id']]
        if (old.get('experiment_id') != 'B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407'
                or old.get('arm_id') != 'DEFENSE_ON__URGENCY_OFF'
                or old.get('parent_id') != parent['shard_id']):
            raise ValueError('foreign baseline identity')
        for arm in ARMS[1:]:
            cell = dict(experiment_id=EXPERIMENT, baseline_id=BASELINE,
                account_contract=INDEPENDENT_CONTRACT, parent_id=old['parent_id'],
                arm_id=arm, seed_pair=old['seed_pair'], start_ns=old['start_ns'],
                end_ns=old['end_ns'], input_manifest_id=old['input_manifest_id'],
                release_identity=release_identity)
            cells.append(identity(cell, original_calendar))
    return cells


def continuous_cells(original_calendar, baseline_identity, release_identity):
    """Four single-init paths; never concatenate independent account outputs."""
    if (baseline_identity.get('experiment_id') != 'B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407'
            or baseline_identity.get('arm_id') != 'DEFENSE_ON__URGENCY_OFF'
            or baseline_identity.get('account_contract') != 'FINAL107_CONTINUOUS'
            or baseline_identity.get('parent_id') != CONTINUOUS_PARENT
            or baseline_identity.get('start_ns') != _utc_ns('2026-05-28T00:00:00Z')
            or baseline_identity.get('end_ns') != _utc_ns('2026-09-12T00:00:00Z')):
        raise ValueError('original continuous baseline identity differs')
    return [identity(dict(experiment_id=EXPERIMENT, baseline_id=BASELINE,
        account_contract=CONTINUOUS_CONTRACT, parent_id=CONTINUOUS_PARENT,
        arm_id=arm, seed_pair=baseline_identity['seed_pair'],
        start_ns=baseline_identity['start_ns'], end_ns=baseline_identity['end_ns'],
        input_manifest_id=baseline_identity['input_manifest_id'],
        release_identity=release_identity), original_calendar) for arm in ARMS[1:]]


def validate_account_bindings(binding, identity_validator):
    """Check each declared identity and uniqueness, without a run-count quota.

    A staged subset need not contain the whole study. Coverage belongs to the
    completion report, not a numerical admission limit or a consumed balance.
    """
    keys = set()
    for entry in binding['cells'].values():
        ident = identity_validator(entry['identity'], binding['calendar'])
        if ident['arm_id'] == 'B0' or ident['release_identity'] != binding['release_identity']:
            raise ValueError('binding contains control rerun or foreign release')
        key = (ident['account_contract'], ident['parent_id'], ident['arm_id'])
        if key in keys:
            raise ValueError('duplicate original account/arm')
        keys.add(key)
