"""Finite study contract tests; no economic runs or scheduler writes."""

from copy import deepcopy
from datetime import date, timedelta

import pytest

from research.families.f01_fixed_parameter_racing.inventory_soft_control import (
    ARMS, BASELINE, CONTINUOUS_CONTRACT, CONTINUOUS_PARENT, COOLDOWN_KEY,
    EXPERIMENT, HOST_LIMITS, INDEPENDENT_CONTRACT, MARKOUT_KEY, MODES,
    arm_parameters, consecutive_multiplier, cooldown_blocks_action, identity,
    modes, validate_effective_params, independent_cells, continuous_cells,
    validate_account_bindings,
)
from strategy.fill_cooldown import (
    cooldown_duration_after_same_side_fill, update_same_side_fill_units,
)
from test_defense_urgency import params
from strategy.inventory_soft_control import (
    pure_reducing_role, generic_markout_enabled_for_role, rounded_release_quantity,
)
from strategy.risk_selection import PendingExposure


def baseline():
    return dict(params(), exit_urgency_strength=0, position_timeout=0,
                order_size=.001, lot_size=.001, tick_size=.1,
                max_inventory=.026, max_position_value=3000,
                max_daily_loss=50, emergency_close_dd=150,
                fill_cooldown=85, fill_cooldown_reducing=0,
                fill_cooldown_consecutive_reset_policy="opposite_fill_only",
                asym_strength=.1, markout_spread_scale=.2,
                rng_seed=42, latency_seed=59, response_observe_only=True,
                response_update_branch_enabled=True)


@pytest.mark.parametrize("arm", ARMS)
def test_complete_inheritance_and_only_declared_patch(arm):
    old = baseline()
    before = deepcopy(old)
    new = arm_parameters(old, arm)
    expected = deepcopy(old)
    if arm != "B0":
        expected[COOLDOWN_KEY], expected[MARKOUT_KEY] = MODES[arm]
    assert new == expected
    validate_effective_params(new, old, arm)
    new["response_action_policy"]["compute"]["event_ns"] = 1
    assert old == before
    with pytest.raises(ValueError):
        validate_effective_params(new, old, arm)


@pytest.mark.parametrize("patch", [
    {"exit_urgency_strength": .5}, {"fill_cooldown": 0},
    {"fill_cooldown_consecutive_reset_policy": "opposite_fill_or_expiry"},
    {"markout_spread_scale": 0}, {"max_inventory": .052},
    {"response_update_branch_enabled": False},
    {"_research_generic_markout_policy_enabled": False},
])
def test_wrong_baseline_rejected(patch):
    with pytest.raises(ValueError):
        arm_parameters(dict(baseline(), **patch), ARMS[1])


def test_no_sixth_combination_or_historical_arm():
    assert modes({}) == MODES["B0"]
    with pytest.raises(ValueError):
        modes({COOLDOWN_KEY: "admission_off", MARKOUT_KEY: "exclude_pure_reducing"})
    with pytest.raises(ValueError):
        arm_parameters(baseline(), "GENERIC_MARKOUT_POLICY_OFF")
    assert HOST_LIMITS == {"local": 4, "oci-tokyo": 3, "oci-tokyo-4": 3, "oci-tokyo-6": 3}


def test_continuous_design_uses_one_original_global_input_and_four_cells():
    old = dict(experiment_id='B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407',
        arm_id='DEFENSE_ON__URGENCY_OFF', account_contract='FINAL107_CONTINUOUS',
        parent_id=CONTINUOUS_PARENT, start_ns=1779926400000000000,
        end_ns=1789171200000000000, input_manifest_id='a'*64,
        seed_pair={'rng_seed':42,'latency_seed':59})
    cells = continuous_cells(calendar(), old, 'b'*64)
    assert len(cells) == 4
    assert [x['arm_id'] for x in cells] == list(ARMS[1:])
    assert {x['input_manifest_id'] for x in cells} == {'a'*64}
    for patch in ({'account_contract':'independent'}, {'parent_id':'stitched'},
                  {'arm_id':'B0'}, {'end_ns':old['end_ns']+1}):
        with pytest.raises(ValueError):
            continuous_cells(calendar(), dict(old, **patch), 'b'*64)


@pytest.mark.parametrize("units", [0, .3, .7, 1, 2, 3, 7])
def test_r1_only_removes_multiplier_not_counter_or_other_factors(units):
    adaptive_ms = 85000 * 1.75
    assert adaptive_ms * consecutive_multiplier(units, "original") == adaptive_ms * max(1, units)
    assert adaptive_ms * consecutive_multiplier(units, "no_consecutive_multiplier") == adaptive_ms
    assert consecutive_multiplier(units, "admission_off") == max(1, units)


def test_quantity_units_opposite_reset_and_zero_duration_rebase_unchanged():
    b, s, _ = update_same_side_fill_units(side="BUY", fill_qty=.0003,
        order_size=.001, lot_size=.001, buy_units=0, sell_units=2)
    assert b == pytest.approx(.3) and s == 0
    b, s, _ = update_same_side_fill_units(side="BUY", fill_qty=.0007,
        order_size=.001, lot_size=.001, buy_units=b, sell_units=s)
    assert b == pytest.approx(1) and s == 0
    assert cooldown_duration_after_same_side_fill(previous_fill_ts_ms=1000,
        previous_cooldown_ms=85000, current_fill_ts_ms=11000,
        new_cooldown_ms=0) == 75000
    b, s, _ = update_same_side_fill_units(side="SELL", fill_qty=.001,
        order_size=.001, lot_size=.001, buy_units=b, sell_units=s)
    assert b == 0 and s == 1


@pytest.mark.parametrize("latent", [True, False])
def test_r2_latent_state_not_an_admission_veto(latent):
    assert cooldown_blocks_action(latent, "admission_off") is False
    assert cooldown_blocks_action(latent, "original") is latent
    assert cooldown_blocks_action(latent, "no_consecutive_multiplier") is latent


def calendar():
    groups = {"Development": [], "Final": []}
    for i in range(204):
        start = date(2025, 8, 1) + timedelta(days=2*i)
        days = 1 if i == 203 else 2
        group, index = ("Development", i) if i < 150 else ("Final", i-150)
        groups[group].append(dict(shard_id=f"{group.lower()}-{index:03d}-{start}",
            original_parent_index=index, account_start_utc=f"{start}T00:00:00Z",
            account_end_utc_exclusive=str(start+timedelta(days=days)),
            duration_hours=24*days,
            calendar_days=[str(start+timedelta(days=j)) for j in range(days)]))
    return {"groups": groups}


def spec():
    return dict(experiment_id=EXPERIMENT, baseline_id=BASELINE,
        account_contract=INDEPENDENT_CONTRACT, parent_id="development-000-2025-08-01",
        arm_id=ARMS[1], start_ns=1754006400000000000, end_ns=1754179200000000000,
        seed_pair={"rng_seed": 42, "latency_seed": 59},
        input_manifest_id="a"*64, release_identity="b"*64)


def test_original_calendar_identity_and_continuous_separation():
    assert identity(spec(), calendar()) == spec()
    continuous = dict(spec(), account_contract=CONTINUOUS_CONTRACT,
        parent_id=CONTINUOUS_PARENT, start_ns=1779926400000000000,
        end_ns=1789171200000000000)
    assert identity(continuous, calendar()) == continuous
    with pytest.raises(ValueError):
        identity(dict(continuous, account_contract=INDEPENDENT_CONTRACT), calendar())


def test_declared_economic_design_never_adds_control_or_extra_dates():
    original = {}
    for i, parent in enumerate(calendar()['groups']['Development'] + calendar()['groups']['Final']):
        original[parent['shard_id']] = dict(spec(),
            experiment_id='B0_RESPONSE_20261007_DEFENSE_URGENCY_FULL407',
            parent_id=parent['shard_id'], arm_id='DEFENSE_ON__URGENCY_OFF',
            start_ns=1754006400000000000+i*2*86400*10**9,
            end_ns=1754006400000000000+min(i*2+2,407)*86400*10**9)
    cells = independent_cells(calendar(), original, 'c'*64)
    assert len(cells) == 816
    assert len({(c['parent_id'],c['arm_id']) for c in cells}) == 816
    assert {c['arm_id'] for c in cells} == set(ARMS[1:])
    assert sum(c['end_ns']-c['start_ns'] for c in cells) == 4*407*86400*10**9
    for malformed in ({}, dict(original, extra=next(iter(original.values())))):
        with pytest.raises(ValueError, match='exactly 204'):
            independent_cells(calendar(), malformed, 'c'*64)
    bad = deepcopy(original)
    bad[next(iter(bad))]['arm_id'] = 'B0'
    with pytest.raises(ValueError, match='foreign baseline'):
        independent_cells(calendar(), bad, 'c'*64)


@pytest.mark.parametrize('count', [0, 1, 17, 820, 1001])
def test_binding_admission_has_no_run_count_quota(count):
    # Identity validation owns scientific scope; this admission layer imposes
    # no separate numerical quota, including above the old hard-coded 820.
    binding = dict(calendar={}, release_identity='c'*64, cells={str(i): dict(
        identity=dict(account_contract='declared', parent_id=str(i),
                      arm_id='candidate', release_identity='c'*64)) for i in range(count)})
    before = deepcopy(binding)
    validate_account_bindings(binding, lambda value, calendar: value)
    validate_account_bindings(binding, lambda value, calendar: value)
    assert binding == before  # No consumed-run balance or counter.


@pytest.mark.parametrize('change', ['duplicate', 'control', 'release'])
def test_no_quota_still_checks_identity_and_duplicate_accounts(change):
    value = dict(spec(), arm_id=ARMS[1])
    binding = dict(calendar=calendar(), release_identity=value['release_identity'],
                   cells={'one': dict(identity=value)})
    if change == 'duplicate':
        binding['cells']['two'] = deepcopy(binding['cells']['one'])
    elif change == 'control':
        value['arm_id'] = 'B0'
    else:
        value['release_identity'] = 'f'*64
    with pytest.raises(ValueError):
        validate_account_bindings(binding, identity)


@pytest.mark.parametrize("patch", [
    {"experiment_id": "B0_DEFENSE_URGENCY_OFF_20261011_MECHANISM_FULL407"},
    {"arm_id": "GENERIC_MARKOUT_POLICY_OFF"}, {"release_identity": "unknown"},
    {"end_ns": 1754179200000000001}, {"seed_pair": {"rng_seed": 43, "latency_seed": 59}},
])
def test_foreign_identity_rejected(patch):
    with pytest.raises(ValueError):
        identity(dict(spec(), **patch), calendar())


@pytest.mark.parametrize("side,q,qty,role", [
    ("SELL", .003, .001, "reducing"), ("SELL", .003, .003, "reducing"),
    ("SELL", .003, .004, "mixed_cross_zero"),
    ("BUY", -.003, .001, "reducing"), ("BUY", -.003, .003, "reducing"),
    ("BUY", -.003, .004, "mixed_cross_zero"),
    ("BUY", 0, .001, "opener"), ("SELL", 0, .001, "opener"),
    ("BUY", .003, .001, "add"), ("SELL", -.003, .001, "add"),
])
def test_role_uses_entire_pre_generic_quantity(side, q, qty, role):
    actual = pure_reducing_role(side=side, inventory=q, quantity=qty,
                               pending_orders=(), snapshot_known=True)
    assert actual == role
    assert generic_markout_enabled_for_role("original_all_roles", actual)
    assert generic_markout_enabled_for_role("exclude_pure_reducing", actual) is (role != "reducing")


def test_pending_cancel_still_consumes_reducing_capacity_and_unknown_retains_policy():
    role = pure_reducing_role(side="SELL", inventory=.003, quantity=.002,
        pending_orders=(PendingExposure("cancel-request-not-confirmed", "SELL", .002),),
        snapshot_known=True)
    assert role == "ambiguous_pending"
    assert generic_markout_enabled_for_role("exclude_pure_reducing", role)
    for q in [None, float("nan")]:
        assert pure_reducing_role(side="SELL", inventory=q, quantity=.001,
                                 pending_orders=(), snapshot_known=True) == "unknown"
    assert pure_reducing_role(side="SELL", inventory=.003, quantity=.001,
                             pending_orders=(), snapshot_known=False) == "unknown"


@pytest.mark.parametrize("arm", ARMS)
@pytest.mark.parametrize("scenario_name", ["ordinary", "async"])
def test_actual_event_loop_checkpoint_preserves_own_mode(arm, scenario_name, tmp_path):
    from models.backtest_tick import simulate_tick
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from tests.test_tick_runtime_checkpoint import scenario, assert_same
    args, kwargs = scenario(scenario_name)
    p = dict(args[3], fill_cooldown=85, fill_cooldown_reducing=0,
             fill_cooldown_consecutive_reset_policy="opposite_fill_only")
    p[COOLDOWN_KEY], p[MARKOUT_KEY] = MODES[arm]
    args = (*args[:3], p)
    full = simulate_tick(*args, **kwargs)
    partial = simulate_tick(*args, **kwargs, checkpoint_at_ts_ms=1200)
    checkpoint = tmp_path / "latest.checkpoint"
    save_runtime_checkpoint(checkpoint, partial["_replay_checkpoint"])
    resumed = simulate_tick(*args, **kwargs,
        resume_checkpoint=load_trusted_runtime_checkpoint(checkpoint))
    assert_same(resumed, full)
    if arm == "B0":
        original = dict(p)
        original.pop(COOLDOWN_KEY)
        original.pop(MARKOUT_KEY)
        assert_same(full, simulate_tick(*args[:3], original, **kwargs))


@pytest.mark.parametrize("arm", ARMS[1:])
def test_full_native_tick_rejects_unsupported_new_modes(arm):
    from models.backtest_tick import _simulate_tick_cpp
    with pytest.raises(NotImplementedError, match="inventory soft-control"):
        _simulate_tick_cpp(None, None, None,
            {COOLDOWN_KEY: MODES[arm][0], MARKOUT_KEY: MODES[arm][1]})


@pytest.mark.parametrize("arm", ARMS)
@pytest.mark.parametrize("q,side,qty,pending,expected", [
    (.003, "SELL", .001, (), "reducing"),
    (-.003, "BUY", .001, (), "reducing"),
    (.003, "SELL", .004, (), "mixed_cross_zero"),
    (.003, "SELL", .002, (PendingExposure("pending", "SELL", .002),), "ambiguous_pending"),
])
def test_actual_common_policy_consumer(arm, q, side, qty, pending, expected):
    from dataclasses import fields
    from types import SimpleNamespace
    import models.backtest_tick as replay
    from narrowgate.replay.executor import ReplayExecutor, ReplayServices

    cooldown, scope = MODES[arm]
    owner = ReplayExecutor(services=ReplayServices(**{
        field.name: getattr(replay, field.name) for field in fields(ReplayServices)
    }), generic_markout_enabled=True, inventory_cooldown_mode=cooldown,
        inventory_markout_scope=scope)
    owner.state = SimpleNamespace(mo_ema_bid=-2., mo_ema_ask=-2., cur_tox_bid=.5,
            cur_tox_ask=.5, q=q, t=0, max_inv=.026, max_exec_book_age_ms=2000,
            markout_spread_scale=.2, mo_ref=1., thin_depth_threshold=0.,
            depth_liquidity_baseline=50., order_size=qty, LOT_SIZE=.001,
            eta=0., symmetric_size=False, bid_orders=[],
            ask_orders=[dict(id=x.order_id, side=x.side, remaining=x.remaining_qty_btc)
                        for x in pending])
    owner._ranked_guard_order_id = lambda order: order["id"]
    owner._fill_cooldown_active = lambda *args, **kw: True
    ctx = {"depth_age_s": 0.}
    result, spread = owner._replay_common_side_policy(side, ctx)
    released = scope == "exclude_pure_reducing" and expected == "reducing"
    assert ctx["policy_markout"] is not released
    assert spread == (1 if released else 1.3)
    if arm != "B0":
        assert ctx["pre_generic_role"] == expected
    else:
        assert "pre_generic_role" not in ctx
    assert result.allow_post is (cooldown == "admission_off")
    # Defense remains a veto and dominates generic spread in every cell.
    ctx.update(defense_guard=True, defense_pause=True, defense_spread_mult=1.5)
    guarded, spread = owner._replay_common_side_policy(side, ctx)
    assert not guarded.allow_post and spread == 1.5


@pytest.mark.parametrize("side,q", [("SELL", .003), ("BUY", -.003)])
def test_original_lot_restoration_is_rechecked_before_release(side, q):
    assert rounded_release_quantity(quantity=.001, size_mult=.25, lot=.001,
        side=side, inventory=q, pending_orders=()) == .001
    assert rounded_release_quantity(quantity=.004, size_mult=1, lot=.001,
        side=side, inventory=q, pending_orders=()) is None
    assert rounded_release_quantity(quantity=.001, size_mult=1, lot=.001,
        side=side, inventory=q,
        pending_orders=(PendingExposure("changed-pending", side, .003),)) is None
    for mult in (0, -1, 2, float("nan")):
        assert rounded_release_quantity(quantity=.001, size_mult=mult, lot=.001,
            side=side, inventory=q, pending_orders=()) is None
