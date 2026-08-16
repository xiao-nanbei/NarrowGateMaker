from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.backtest_tick import _validate_f05_cpp_cooldown_runtime
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_cpp_runtime_v22 as cpp_runtime_v22,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    build_lockstep_digest,
)
from strategy.boolean_cooldown_live import (
    ReceiveTimeMidEmaWindows,
    RuntimeCooldownPolicyEvaluator,
)

cpp = pytest.importorskip("narrowgate_cpp")
if not hasattr(cpp, "F05RepeatedBooleanCooldownRuntime"):
    pytest.skip(
        "narrowgate_cpp was not rebuilt with the F05 repeated cooldown ABI",
        allow_module_level=True,
    )

LONG_CROSS = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
SHORT_CROSS = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
CAMPAIGN_AGE = "predicate::m0::campaign_age_gt_control_duration"
PREDICATE_COLUMNS = (LONG_CROSS, SHORT_CROSS, CAMPAIGN_AGE)
POLICY_SHA256 = "1" * 64
PREDICATE_SHA256 = "2" * 64
QUALIFICATION_SHA256 = "3" * 64
BASE_MS = 1_700_000_000_000
BASE_NS = BASE_MS * 1_000_000


def _literal(index: int, *, negated: bool = False):
    value = cpp.F05BooleanLiteral()
    value.predicate_index = index
    value.negated = negated
    return value


def _clause(*literals):
    value = cpp.F05BooleanClause()
    value.literals = list(literals)
    return value


def _rule(action_id: str, duration_ms: int, *clauses):
    value = cpp.F05BooleanRule()
    value.action_id = action_id
    value.duration_ms = duration_ms
    value.clauses = list(clauses)
    return value


def _cpp_policy():
    policy = cpp.F05BooleanPolicy()
    policy.policy_sha256 = POLICY_SHA256
    policy.predicate_bundle_sha256 = PREDICATE_SHA256
    policy.predicate_columns = list(PREDICATE_COLUMNS)
    policy.default_action = "CONTROL_85N"
    policy.rules = [
        _rule(
            "FIXED_1748S",
            1_748_000,
            _clause(_literal(1), _literal(2)),
            _clause(_literal(1, negated=True), _literal(2)),
        ),
        _rule(
            "FIXED_166S",
            166_000,
            _clause(_literal(0), _literal(2, negated=True)),
        ),
        _rule(
            "FIXED_211S",
            211_000,
            _clause(_literal(0, negated=True), _literal(2, negated=True)),
        ),
    ]
    return policy


def _cpp_config(
    *,
    qualified: bool = True,
    warmup_s: float = 0.2,
    max_feature_age_s: float = 0.5,
    qualification_scope: str = "synthetic_mechanics_only",
):
    config = cpp.F05RepeatedBooleanCooldownConfig()
    config.parity_qualified = qualified
    config.parity_qualification_sha256 = QUALIFICATION_SHA256 if qualified else ""
    config.qualification_scope = qualification_scope
    config.warmup_s = warmup_s
    config.max_feature_age_s = max_feature_age_s
    config.policy = _cpp_policy()
    return config


def _full_replay_cpp_config():
    config = _cpp_config(
        warmup_s=0.2,
        max_feature_age_s=0.5,
        qualification_scope="synthetic_full_replay_smoke",
    )
    config.policy.rules = [
        _rule(
            "FIXED_1S",
            1_000,
            _clause(_literal(1), _literal(2)),
            _clause(_literal(1, negated=True), _literal(2)),
            _clause(_literal(0), _literal(2, negated=True)),
            _clause(_literal(0, negated=True), _literal(2, negated=True)),
        )
    ]
    return config


def _fixed_one_second_cpp_config():
    config = _cpp_config(
        warmup_s=0.2,
        max_feature_age_s=0.5,
        qualification_scope="synthetic_full_replay_smoke",
    )
    config.policy.predicate_columns = [CAMPAIGN_AGE]
    config.policy.rules = [
        _rule(
            "FIXED_1S",
            1_000,
            _clause(_literal(0)),
            _clause(_literal(0, negated=True)),
        )
    ]
    return config


def _python_evaluator() -> RuntimeCooldownPolicyEvaluator:
    return RuntimeCooldownPolicyEvaluator(
        rules=(
            (
                "FIXED_1748S",
                (
                    ((SHORT_CROSS, False), (CAMPAIGN_AGE, False)),
                    ((SHORT_CROSS, True), (CAMPAIGN_AGE, False)),
                ),
            ),
            (
                "FIXED_166S",
                (((LONG_CROSS, False), (CAMPAIGN_AGE, True)),),
            ),
            (
                "FIXED_211S",
                (((LONG_CROSS, True), (CAMPAIGN_AGE, True)),),
            ),
        ),
        policy_sha256=POLICY_SHA256,
        predicate_bundle_sha256=PREDICATE_SHA256,
    )


def _window(
    index: int,
    mid: float | None,
    *,
    source_gap: bool = False,
    source_stale: bool = False,
):
    width = 100_000_000
    observation = cpp.F05CooldownWindowObservation()
    observation.left_ts_ns = index * width
    observation.right_ts_ns = (index + 1) * width
    observation.feature_ready_ts_ns = observation.right_ts_ns + 50_000_000
    observation.market_generation = index + 1
    observation.depth_generation = index + 1
    observation.mid_usdc_per_btc = mid
    observation.source_gap = source_gap
    observation.source_stale = source_stale
    observation.channel_support_valid = not (source_gap or source_stale)
    return observation


def _full_replay_window_tape():
    observations = []
    for index in range(91):
        right_ts_ns = BASE_NS + index * 100_000_000
        observation = cpp.F05CooldownWindowObservation()
        observation.left_ts_ns = right_ts_ns - 100_000_000
        observation.right_ts_ns = right_ts_ns
        observation.feature_ready_ts_ns = right_ts_ns
        observation.market_generation = index + 1
        observation.depth_generation = index + 1
        observation.mid_usdc_per_btc = 100.0 if (index // 5) % 2 == 0 else 100.2
        observation.channel_support_valid = True
        observations.append(observation)
    return observations


def _full_replay_predicate_rows():
    rows = []
    for ordinal, offset in enumerate((1_000, 3_000, 6_000), start=1):
        row = cpp.F05CooldownPredicateRow()
        row.exposure_fill_ordinal = ordinal
        row.fill_ts_ms = BASE_MS + offset
        row.side = cpp.Side.Sell
        row.campaign_id = 1
        row.snapshot_id = f"synthetic-full-replay-{ordinal}"
        row.predicate_values = [
            cpp.F05TriState.TRUE,
            cpp.F05TriState.TRUE,
            cpp.F05TriState.FALSE,
        ]
        rows.append(row)
    return rows


def _full_replay_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + offset for offset in range(0, 9_000, 1_000)],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0, 103.4, 103.4, 110.0, 110.0, 110.0, 120.0, 120.0, 120.0],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, 0.0, 0.001, 0.0, 0.0, 0.001, 0.0, 0.0],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.zeros(9, dtype=np.uint8),
        }
    )


def _full_replay_bbo(trades: pd.DataFrame) -> bt.HistoricalBBOData:
    prices = trades["price"].to_numpy(dtype=np.float64, copy=True)
    return bt.HistoricalBBOData(
        ts_ms=trades["transact_time"].to_numpy(dtype=np.int64, copy=True),
        best_bid=prices - 0.1,
        best_ask=prices + 0.1,
        bid_qty=np.ones(prices.size, dtype=np.float64),
        ask_qty=np.ones(prices.size, dtype=np.float64),
        source="synthetic_native_bbo",
    )


def _full_replay_emitter() -> CooldownV2ReplayEmitter:
    observations = (
        CausalWindowObservation(
            left_ts_ns=right_ts_ns - BASE_WINDOW_WIDTH_NS,
            right_ts_ns=right_ts_ns,
            feature_ready_ts_ns=right_ts_ns,
            market_generation=index,
            depth_generation=index,
            values={"mid_usdc_per_btc": 100.0},
            source_gap=False,
            warmup_admitted=right_ts_ns > BASE_NS,
        )
        for index, right_ts_ns in enumerate(
            range(
                BASE_NS,
                BASE_NS + 9_100_000_000,
                BASE_WINDOW_WIDTH_NS,
            ),
            start=1,
        )
    )
    return CooldownV2ReplayEmitter(
        feature_block="R0",
        observations=observations,
        warmup_cutoff_ts_ns=BASE_NS,
        warmup_identity="synthetic-d-minus-1",
        identity_hashes={
            "config_sha256": "a" * 64,
            "code_sha256": "b" * 64,
            "model_sha256": "c" * 64,
            "p3_sha256": "d" * 64,
            "feature_dag_sha256": "e" * 64,
            "execution_abi_sha256": "f" * 64,
            "baseline_identity_sha256": "1" * 64,
        },
        source_cursor_prefixes={
            "market": "synthetic-market",
            "depth": "synthetic-depth",
            "trade": "synthetic-trade",
        },
        retain_snapshots=True,
    )


@dataclass(frozen=True)
class _FixedOneSecondDecision:
    action_id: str
    duration_ms: float
    fallback_reason: str
    matched_rule_index: int | None
    policy_sha256: str
    predicate_bundle_sha256: str
    snapshot_id: str
    support_valid: bool


class _FixedOneSecondEvaluator:
    policy_sha256 = POLICY_SHA256
    predicate_bundle_sha256 = PREDICATE_SHA256

    def __init__(self) -> None:
        self.evaluations = 0

    def evaluate(self, snapshot, *, baseline_duration_ms: float):
        del baseline_duration_ms
        self.evaluations += 1
        return _FixedOneSecondDecision(
            action_id="FIXED_1S",
            duration_ms=1_000.0,
            fallback_reason="",
            matched_rule_index=0,
            policy_sha256=self.policy_sha256,
            predicate_bundle_sha256=self.predicate_bundle_sha256,
            snapshot_id=str(snapshot.snapshot_id),
            support_valid=True,
        )

    def audit(self):
        return {"evaluations": self.evaluations}


def _full_replay_params(*, engine: str) -> dict[str, object]:
    params: dict[str, object] = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown": 85.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "replay_initial_state_mode": "fresh_start",
        "trace_cooldown_duration_opportunities_max": 100,
        "trace_fills_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "cooldown_v2_snapshot_emitter": _full_replay_emitter(),
        "cooldown_duration_policy_evaluator": _FixedOneSecondEvaluator(),
    }
    if engine == "cpp":
        params.update(
            {
                "cooldown_duration_policy_cpp_runtime": (
                    cpp.F05RepeatedBooleanCooldownRuntime(_full_replay_cpp_config())
                ),
                "cooldown_duration_policy_cpp_parity_qualified": True,
                "cooldown_duration_policy_cpp_event_loop_parity_qualified": True,
                "cooldown_duration_policy_cpp_parity_receipt_sha256": (QUALIFICATION_SHA256),
                "_cooldown_duration_policy_cpp_window_tape": (_full_replay_window_tape()),
                "_cooldown_duration_policy_cpp_predicate_rows": (_full_replay_predicate_rows()),
            }
        )
    return params


def _warm(runtime) -> None:
    for index, mid in enumerate((100.0, 101.0, 99.0)):
        runtime.update_window(_window(index, mid))


def _fill(
    *,
    side=None,
    role=None,
    fill_ts_ms: int = 350,
    decision_ts_ns: int = 350_000_000,
    campaign_id: int = 7,
    campaign_age_s: float = 10.0,
    inventory_before: float = 0.0,
    inventory_after: float = -0.001,
    consecutive_units: float = 1.0,
    predicate_values=(),
):
    value = cpp.F05CooldownFillInput()
    value.snapshot_id = f"snapshot-{campaign_id}-{fill_ts_ms}"
    value.side = cpp.Side.Sell if side is None else side
    value.role = cpp.F05CooldownFillRole.OPENER if role is None else role
    value.fill_ts_ms = fill_ts_ms
    value.decision_ts_ns = decision_ts_ns
    value.campaign_id = campaign_id
    value.campaign_age_s = campaign_age_s
    value.inventory_before_fill_btc = inventory_before
    value.inventory_after_fill_btc = inventory_after
    value.consecutive_units_after = consecutive_units
    value.baseline_duration_ms = round(85_000 * max(1.0, consecutive_units))
    value.policy_input_valid = True
    value.support_valid = True
    value.channel_support_valid = True
    value.predicate_values = list(predicate_values)
    return value


def _python_value(value) -> int:
    return {
        cpp.F05TriState.UNOBSERVED: -1,
        cpp.F05TriState.FALSE: 0,
        cpp.F05TriState.TRUE: 1,
    }[value]


@pytest.mark.parametrize(
    "states",
    tuple(
        itertools.product(
            (
                cpp.F05TriState.UNOBSERVED,
                cpp.F05TriState.FALSE,
                cpp.F05TriState.TRUE,
            ),
            repeat=3,
        )
    ),
)
def test_cpp_matches_python_ordered_three_valued_rules(states) -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(runtime)
    cpp_decision = runtime.apply_fill(_fill(predicate_values=states))
    py_decision = _python_evaluator().evaluate_predicates(
        side="SELL",
        predicate_values={
            name: _python_value(state)
            for name, state in zip(PREDICATE_COLUMNS, states, strict=True)
        },
        baseline_duration_ms=85_000,
        snapshot_id="snapshot-7-350",
    )

    assert cpp_decision.action_id == py_decision.action_id
    assert cpp_decision.duration_ms == py_decision.duration_ms
    assert (cpp_decision.fallback_reason or None) == py_decision.fallback_reason
    assert cpp_decision.matched_rule_index == py_decision.matched_rule_index
    assert cpp_decision.support_valid is py_decision.support_valid


def test_cpp_selected_mid_stream_matches_python_feature_state() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    windows = ReceiveTimeMidEmaWindows(
        warmup_s=0.2,
        max_feature_age_s=0.5,
    )
    observations = (
        (50_000_000, 100.0),
        (150_000_000, 101.0),
        (250_000_000, 99.0),
        (350_000_000, 99.0),
    )
    for generation, (receive_ts_ns, mid) in enumerate(observations, start=1):
        windows.observe_depth(
            receive_ts_ns=receive_ts_ns,
            bids=[(mid - 0.1, 1.0)],
            asks=[(mid + 0.1, 1.0)],
            market_generation=generation,
            depth_generation=generation,
        )
    for index, mid in enumerate((100.0, 101.0, 99.0)):
        runtime.update_window(_window(index, mid))

    predicates, fallback, feature_ready, feature_age = windows.predicate_values(
        decision_ts_ns=350_000_000,
        campaign_age_s=10.0,
        baseline_duration_ms=85_000,
    )
    assert fallback is None
    assert predicates is not None
    py_decision = _python_evaluator().evaluate_predicates(
        side="SELL",
        predicate_values=predicates,
        baseline_duration_ms=85_000,
        snapshot_id="snapshot-7-350",
    )
    cpp_decision = runtime.apply_fill(_fill())

    assert cpp_decision.action_id == py_decision.action_id
    assert cpp_decision.duration_ms == py_decision.duration_ms
    assert (cpp_decision.fallback_reason or None) == py_decision.fallback_reason
    assert cpp_decision.feature_ready_ts_ns == feature_ready
    assert cpp_decision.feature_age_ms == pytest.approx(feature_age)


def test_cpp_fail_closed_warmup_stale_unobserved_and_qualification() -> None:
    unqualified = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config(qualified=False))
    _warm(unqualified)
    decision = unqualified.apply_fill(
        _fill(
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            )
        )
    )
    assert decision.action_id == "CONTROL_85N"
    assert decision.coverage_reason_code == "cpp_parity_not_qualified"
    assert not decision.support_valid

    warming = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    warming.update_window(_window(0, 100.0))
    decision = warming.apply_fill(
        _fill(
            decision_ts_ns=150_000_000,
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    assert decision.coverage_reason_code == "ema_warmup_incomplete"

    stale = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(stale)
    decision = stale.apply_fill(
        _fill(
            fill_ts_ms=901,
            decision_ts_ns=901_000_000,
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    assert decision.coverage_reason_code == "feature_state_stale"

    unobserved = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(unobserved)
    unobserved.update_window(_window(3, None, source_gap=True))
    decision = unobserved.apply_fill(
        _fill(
            fill_ts_ms=450,
            decision_ts_ns=450_000_000,
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    assert decision.coverage_reason_code == "latest_completed_mid_window_unobserved"

    reset = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(reset)
    for index in range(3, 9):
        reset.update_window(_window(index, None, source_gap=True))
    assert reset.audit().feature_state_reset_count == 1
    reset.update_window(_window(9, 100.0))
    decision = reset.apply_fill(
        _fill(
            fill_ts_ms=1_050,
            decision_ts_ns=1_050_000_000,
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    assert decision.coverage_reason_code == "ema_warmup_incomplete"


def test_cpp_repeated_fill_lineage_role_and_expiry_contract() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(runtime)
    first = runtime.apply_fill(
        _fill(
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            )
        )
    )
    assert first.action_id == "FIXED_166S"
    assert first.lineage_revision == 1
    assert runtime.add_blocked(cpp.Side.Sell, 350)

    second = runtime.apply_fill(
        _fill(
            role=cpp.F05CooldownFillRole.ADD,
            fill_ts_ms=400,
            decision_ts_ns=400_000_000,
            inventory_before=-0.001,
            inventory_after=-0.002,
            consecutive_units=2.0,
            predicate_values=(
                cpp.F05TriState.FALSE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    assert second.action_id == "FIXED_211S"
    assert second.lineage_revision == 2
    assert second.deadline_ts_ms == 211_400

    reducing = runtime.apply_fill(
        _fill(
            side=cpp.Side.Buy,
            role=cpp.F05CooldownFillRole.REDUCING,
            fill_ts_ms=425,
            decision_ts_ns=425_000_000,
            inventory_before=-0.002,
            inventory_after=-0.001,
            consecutive_units=1.0,
        )
    )
    assert reducing.coverage_reason_code == "reducing_fill_baseline_bypass"
    assert not reducing.lineage_applied
    assert not runtime.lineage(cpp.Side.Sell).active

    buy = runtime.apply_fill(
        _fill(
            side=cpp.Side.Buy,
            role=cpp.F05CooldownFillRole.OPENER,
            fill_ts_ms=450,
            decision_ts_ns=450_000_000,
            campaign_id=8,
            inventory_before=0.0,
            inventory_after=0.001,
            consecutive_units=1.0,
        )
    )
    assert buy.action_id == "CONTROL_85N"
    assert buy.coverage_reason_code == "buy_control_by_contract"
    assert runtime.add_blocked(cpp.Side.Buy, 451)
    runtime.advance_time(buy.deadline_ts_ms)
    assert not runtime.add_blocked(cpp.Side.Buy, buy.deadline_ts_ms)


def test_cpp_reducing_fill_uses_exact_sign_and_preserves_same_side_deadline() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(runtime)
    opener = runtime.apply_fill(
        _fill(
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            )
        )
    )
    reducing = runtime.apply_fill(
        _fill(
            role=cpp.F05CooldownFillRole.REDUCING,
            fill_ts_ms=400,
            decision_ts_ns=400_000_000,
            inventory_before=5e-11,
            inventory_after=0.0,
            consecutive_units=1.0,
        )
    )

    assert reducing.coverage_reason_code == "reducing_fill_baseline_bypass"
    assert runtime.lineage(cpp.Side.Sell).active
    assert runtime.lineage(cpp.Side.Sell).deadline_ts_ms == opener.deadline_ts_ms


def test_cpp_partial_fill_replaces_same_lineage_deadline() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    _warm(runtime)
    first = runtime.apply_fill(
        _fill(
            fill_ts_ms=350,
            consecutive_units=1.5,
            inventory_before=0.0,
            inventory_after=-0.0015,
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )
    second = runtime.apply_fill(
        _fill(
            role=cpp.F05CooldownFillRole.ADD,
            fill_ts_ms=425,
            decision_ts_ns=425_000_000,
            inventory_before=-0.0015,
            inventory_after=-0.002,
            consecutive_units=2.0,
            predicate_values=(
                cpp.F05TriState.FALSE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            ),
        )
    )

    assert first.baseline_duration_ms == 127_500
    assert first.lineage_revision == 1
    assert second.baseline_duration_ms == 170_000
    assert second.lineage_revision == 2
    assert second.deadline_ts_ms == 211_425
    assert runtime.lineage(cpp.Side.Sell).deadline_ts_ms == second.deadline_ts_ms


def test_cpp_checkpoint_is_deterministic_bound_and_resumable() -> None:
    config = _cpp_config()
    left = cpp.F05RepeatedBooleanCooldownRuntime(config)
    _warm(left)
    left.apply_fill(
        _fill(
            predicate_values=(
                cpp.F05TriState.TRUE,
                cpp.F05TriState.TRUE,
                cpp.F05TriState.FALSE,
            )
        )
    )
    first = left.checkpoint()
    second = left.checkpoint()
    assert first.canonical_payload == second.canonical_payload
    assert first.checkpoint_sha256 == second.checkpoint_sha256
    assert (
        first.checkpoint_sha256
        == hashlib.sha256(first.canonical_payload.encode("utf-8")).hexdigest()
    )

    resumed = cpp.F05RepeatedBooleanCooldownRuntime(config)
    resumed.restore(first)
    assert resumed.checkpoint().checkpoint_sha256 == first.checkpoint_sha256
    next_fill = _fill(
        role=cpp.F05CooldownFillRole.ADD,
        fill_ts_ms=400,
        decision_ts_ns=400_000_000,
        inventory_before=-0.001,
        inventory_after=-0.002,
        consecutive_units=2.0,
        predicate_values=(
            cpp.F05TriState.FALSE,
            cpp.F05TriState.TRUE,
            cpp.F05TriState.FALSE,
        ),
    )
    left_decision = left.apply_fill(next_fill)
    resumed_decision = resumed.apply_fill(next_fill)
    assert resumed_decision.action_id == left_decision.action_id
    assert resumed_decision.deadline_ts_ms == left_decision.deadline_ts_ms
    assert resumed.checkpoint().checkpoint_sha256 == left.checkpoint().checkpoint_sha256

    first.canonical_payload += "tampered"
    with pytest.raises(ValueError, match="f05_checkpoint_hash_drifted"):
        resumed.restore(first)

    mismatched = _cpp_config()
    mismatched.policy.policy_sha256 = "4" * 64
    with pytest.raises(ValueError, match="f05_checkpoint_identity_drifted"):
        cpp.F05RepeatedBooleanCooldownRuntime(mismatched).restore(second)


def test_invalid_and_mismatched_runtime_hashes_fail_closed() -> None:
    invalid = _cpp_config()
    invalid.policy.policy_sha256 = "NOT-A-SHA"
    invalid_runtime = cpp.F05RepeatedBooleanCooldownRuntime(invalid)
    assert not invalid_runtime.parity_qualified
    assert invalid_runtime.binding_error == "policy_sha256_invalid"

    self_attested_formal = cpp.F05RepeatedBooleanCooldownRuntime(
        _cpp_config(qualification_scope="full_replay")
    )
    assert not self_attested_formal.parity_qualified
    assert self_attested_formal.binding_error == "cpp_qualification_scope_invalid"

    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    params = {
        "cooldown_duration_policy_evaluator": SimpleNamespace(
            policy_sha256="4" * 64,
            predicate_bundle_sha256=PREDICATE_SHA256,
        ),
        "cooldown_duration_policy_cpp_runtime": runtime,
        "cooldown_duration_policy_cpp_parity_qualified": True,
        "cooldown_duration_policy_cpp_parity_receipt_sha256": (QUALIFICATION_SHA256),
    }
    with pytest.raises(RuntimeError, match="policy and predicate identities"):
        _validate_f05_cpp_cooldown_runtime(params, require_full_replay=False)

    with pytest.raises(RuntimeError, match="without the Python-authoritative"):
        _validate_f05_cpp_cooldown_runtime(
            {"cooldown_duration_policy_cpp_runtime": runtime},
            require_full_replay=False,
        )


def test_python_cpp_dispatch_gate_requires_explicit_bound_qualification() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_cpp_config())
    evaluator = SimpleNamespace(
        policy_sha256=POLICY_SHA256,
        predicate_bundle_sha256=PREDICATE_SHA256,
    )
    params = {
        "cooldown_duration_policy_evaluator": evaluator,
        "cooldown_duration_policy_cpp_runtime": runtime,
        "cooldown_duration_policy_cpp_parity_receipt_sha256": (QUALIFICATION_SHA256),
    }
    with pytest.raises(NotImplementedError, match="explicit parity-qualified"):
        _validate_f05_cpp_cooldown_runtime(params, require_full_replay=False)

    params["cooldown_duration_policy_cpp_parity_qualified"] = True
    assert _validate_f05_cpp_cooldown_runtime(params, require_full_replay=False) is runtime
    with pytest.raises(NotImplementedError, match="synthetic mechanics parity"):
        _validate_f05_cpp_cooldown_runtime(params, require_full_replay=True)

    params["cooldown_duration_policy_cpp_parity_receipt_sha256"] = "4" * 64
    with pytest.raises(RuntimeError, match="receipt identity drifted"):
        _validate_f05_cpp_cooldown_runtime(params, require_full_replay=False)


def test_full_replay_selection_requires_separate_event_loop_qualification() -> None:
    runtime = cpp.F05RepeatedBooleanCooldownRuntime(_full_replay_cpp_config())
    params = {
        "cooldown_duration_policy_evaluator": SimpleNamespace(
            policy_sha256=POLICY_SHA256,
            predicate_bundle_sha256=PREDICATE_SHA256,
        ),
        "cooldown_duration_policy_cpp_runtime": runtime,
        "cooldown_duration_policy_cpp_parity_qualified": True,
        "cooldown_duration_policy_cpp_parity_receipt_sha256": (QUALIFICATION_SHA256),
    }
    with pytest.raises(
        NotImplementedError,
        match="remains rejected until event-loop parity",
    ):
        _validate_f05_cpp_cooldown_runtime(params, require_full_replay=True)

    params["cooldown_duration_policy_cpp_event_loop_parity_qualified"] = True
    assert _validate_f05_cpp_cooldown_runtime(params, require_full_replay=True) is runtime


def test_cpp_full_replay_hook_matches_python_repeated_policy_path() -> None:
    bt.configure_symbol("BTCUSDC")
    trades = _full_replay_trades()
    bbo = _full_replay_bbo(trades)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    python_result = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        _full_replay_params(engine="python"),
        bbo_data=bbo,
    )
    cpp_result = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        _full_replay_params(engine="cpp"),
        bbo_data=bbo,
    )

    python_decisions = python_result["_cooldown_duration_policy_decisions"]
    cpp_decisions = cpp_result["_cooldown_duration_policy_decisions"]
    assert len(cpp_decisions) == len(python_decisions) >= 2
    for cpp_row, python_row in zip(cpp_decisions, python_decisions, strict=True):
        for field in (
            "exposure_fill_ordinal",
            "fill_visible_ts_ms",
            "side",
            "role_at_fill",
            "campaign_id",
            "action_id",
            "duration_ms",
            "fallback_reason",
            "matched_rule_index",
            "policy_sha256",
            "predicate_bundle_sha256",
            "support_valid",
        ):
            assert cpp_row[field] == python_row[field], field

    assert cpp_result["fills_total"] == python_result["fills_total"]
    assert cpp_result["fills_ask"] == python_result["fills_ask"]
    assert cpp_result["n_requotes"] == python_result["n_requotes"]
    assert cpp_result["final_inventory"] == pytest.approx(
        python_result["final_inventory"],
        abs=1e-12,
    )
    assert len(cpp_result["_fill_trace"]) == len(python_result["_fill_trace"])
    for cpp_fill, python_fill in zip(
        cpp_result["_fill_trace"],
        python_result["_fill_trace"],
        strict=True,
    ):
        for field in (
            "side",
            "fill_ts",
            "quote_ts",
            "price",
            "fill_qty",
            "inventory_before_fill",
            "inventory_after_fill",
        ):
            assert cpp_fill[field] == pytest.approx(python_fill[field]), field

    assert {row["campaign_id"] for row in cpp_decisions} == {1}
    assert cpp_result["cash_before_terminal"] == pytest.approx(
        python_result["cash_before_terminal"],
        abs=1e-12,
    )
    assert cpp_result["terminal_mtm_pnl"] == pytest.approx(
        python_result["terminal_mtm_pnl"],
        abs=1e-12,
    )
    assert cpp_decisions[0]["role_at_fill"] == "opener"
    assert all(row["role_at_fill"] == "add" for row in cpp_decisions[1:])
    assert [row["lineage_revision"] for row in cpp_decisions] == list(
        range(1, len(cpp_decisions) + 1)
    )
    checkpoint = cpp_result["_f05_repeated_cooldown_checkpoint"]
    assert checkpoint is not None
    assert checkpoint.sell_lineage.revision == len(cpp_decisions)
    assert not checkpoint.sell_lineage.active
    assert checkpoint.audit.evaluation_count == len(cpp_decisions)
    assert (
        hashlib.sha256(checkpoint.canonical_payload.encode("utf-8")).hexdigest()
        == checkpoint.checkpoint_sha256
    )
    restored = cpp.F05RepeatedBooleanCooldownRuntime(_full_replay_cpp_config())
    restored.restore(checkpoint)
    assert restored.checkpoint().checkpoint_sha256 == checkpoint.checkpoint_sha256

    control_params = _full_replay_params(engine="python")
    control_params.pop("cooldown_duration_policy_evaluator")
    control = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        control_params,
        bbo_data=bbo,
    )
    assert cpp_result["fills_ask"] > control["fills_ask"]


def test_cpp_one_shot_target_override_matches_python_owner_continuation() -> None:
    bt.configure_symbol("BTCUSDC")
    trades = _full_replay_trades()
    bbo = _full_replay_bbo(trades)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    census = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        _full_replay_params(engine="python"),
        bbo_data=bbo,
    )
    target = census["_cooldown_duration_opportunity_trace"][0]

    def params(engine: str) -> dict[str, object]:
        values = _full_replay_params(engine=engine)
        values["use_bar_pricing"] = False
        if engine == "cpp":
            target_row = cpp.F05CooldownPredicateRow()
            target_row.exposure_fill_ordinal = int(target["exposure_fill_ordinal"])
            target_row.fill_ts_ms = int(target["fill_visible_ts_ms"])
            target_row.side = cpp.Side.Sell
            target_row.campaign_id = int(target["campaign_id"])
            target_row.snapshot_id = "synthetic-one-shot-target"
            target_row.predicate_values = [cpp.F05TriState.FALSE]
            values["cooldown_duration_policy_cpp_runtime"] = cpp.F05RepeatedBooleanCooldownRuntime(
                _fixed_one_second_cpp_config()
            )
            values["_cooldown_duration_policy_cpp_predicate_rows"] = [target_row]
        values.update(
            {
                "cooldown_duration_fork_enabled": True,
                "cooldown_duration_fork_action": "FIXED_DURATION_MS",
                "cooldown_duration_fork_target_ordinal": int(target["exposure_fill_ordinal"]),
                "cooldown_duration_fork_target_ts_ms": int(target["fill_visible_ts_ms"]),
                "cooldown_duration_fork_target_side": str(target["side"]),
                "cooldown_duration_fork_target_order_id": int(target["order_id"]),
                "cooldown_duration_fork_target_campaign_id": int(target["campaign_id"]),
                "cooldown_duration_fork_expected_baseline_ms": float(
                    target["baseline_duration_ms"]
                ),
                "cooldown_duration_fork_fixed_ms": 2_500.0,
                "cooldown_duration_fork_baseline_policy_enabled": True,
                "cooldown_duration_fork_expected_owner_action": "FIXED_1S",
                "cooldown_duration_fork_expected_owner_policy_sha256": (POLICY_SHA256),
            }
        )
        return values

    python_result = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params("python"),
        bbo_data=bbo,
    )
    cpp_result = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params("cpp"),
        bbo_data=bbo,
    )

    python_trace = python_result["_cooldown_duration_fork_trace"]
    cpp_trace = cpp_result["_cooldown_duration_fork_trace"]
    for field in (
        "schema_version",
        "action",
        "side",
        "campaign_id",
        "target_exposure_fill_ordinal",
        "target_order_id",
        "assignment_ts_ms",
        "baseline_duration_ms",
        "applied_duration_ms",
        "applied_deadline_ts_ms",
        "exact_owner_baseline_policy_enabled",
        "exact_owner_action",
        "exact_owner_policy_sha256",
        "exact_owner_baseline_duration_ms",
        "arm_washout_complete",
        "right_censored",
        "terminal_reason",
        "post_assignment_buy_fill_count",
        "post_assignment_sell_fill_count",
    ):
        assert cpp_trace[field] == python_trace[field], field

    assert len(cpp_result["_fill_trace"]) == len(python_result["_fill_trace"])
    for cpp_fill, python_fill in zip(
        cpp_result["_fill_trace"],
        python_result["_fill_trace"],
        strict=True,
    ):
        for field in (
            "side",
            "fill_ts",
            "quote_ts",
            "price",
            "fill_qty",
            "inventory_before_fill",
            "inventory_after_fill",
        ):
            assert cpp_fill[field] == pytest.approx(python_fill[field]), field

    for field in (
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "terminal_inventory_btc",
        "terminal_mid_usdc_per_btc",
        "inventory_time_btc_s",
        "mae_usdc",
        "max_abs_inventory_btc",
        "accounting_residual_usdc",
    ):
        assert cpp_trace[field] == pytest.approx(python_trace[field]), field
    assert cpp_result["cash_before_terminal"] == pytest.approx(
        python_result["cash_before_terminal"],
        abs=1e-12,
    )
    assert cpp_result["terminal_mtm_pnl"] == pytest.approx(
        python_result["terminal_mtm_pnl"],
        abs=1e-12,
    )
    assert build_lockstep_digest(cpp_result) == build_lockstep_digest(python_result)


def test_cpp_real_builder_accepts_sparse_support_row_with_runtime_predicates() -> None:
    bt.configure_symbol("BTCUSDC")
    trades = _full_replay_trades()
    bbo = _full_replay_bbo(trades)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    census = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        _full_replay_params(engine="python"),
        bbo_data=bbo,
    )
    target = census["_cooldown_duration_opportunity_trace"][1]
    opportunity = {
        **target,
        "opportunity_id": "synthetic-real-builder-second-fill",
        "feature::support_valid": True,
        "feature::channel_support_valid": True,
        "owner_fallback_reason": "",
    }
    row = cpp_runtime_v22.build_target_predicate_row(cpp, opportunity)
    cpp_runtime_v22.validate_target_predicate_row(
        cpp,
        row,
        opportunity,
        expected_predicate_count=1,
    )
    assert row.exposure_fill_ordinal == 2
    assert list(row.predicate_values) == []

    params = _full_replay_params(engine="cpp")
    params["cooldown_duration_policy_cpp_runtime"] = cpp.F05RepeatedBooleanCooldownRuntime(
        _fixed_one_second_cpp_config()
    )
    params["_cooldown_duration_policy_cpp_predicate_rows"] = [row]
    result = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        bbo_data=bbo,
    )
    assert len(result["_cooldown_duration_policy_decisions"]) >= 2
    assert (
        result["_cooldown_duration_policy_decisions"][1]["snapshot_id"]
        == (opportunity["opportunity_id"])
    )


def test_cpp_real_builder_rejects_identity_and_width_drift() -> None:
    opportunity = {
        "opportunity_id": "synthetic-builder",
        "exposure_fill_ordinal": 3,
        "fill_visible_ts_ms": BASE_MS + 1_000,
        "side": "SELL",
        "campaign_id": 1,
        "feature::support_valid": True,
        "feature::channel_support_valid": True,
        "owner_fallback_reason": "",
    }
    row = cpp_runtime_v22.build_target_predicate_row(cpp, opportunity)
    row.exposure_fill_ordinal = 2
    with pytest.raises(cpp_runtime_v22.CppOwnerRuntimeError, match="identity drifted"):
        cpp_runtime_v22.validate_target_predicate_row(
            cpp,
            row,
            opportunity,
            expected_predicate_count=len(PREDICATE_COLUMNS),
        )
    row.exposure_fill_ordinal = 3
    row.predicate_values = [cpp.F05TriState.TRUE]
    with pytest.raises(cpp_runtime_v22.CppOwnerRuntimeError, match="width drifted"):
        cpp_runtime_v22.validate_target_predicate_row(
            cpp,
            row,
            opportunity,
            expected_predicate_count=len(PREDICATE_COLUMNS),
        )


def test_cpp_full_replay_rejects_unordered_or_wrong_width_sparse_rows() -> None:
    bt.configure_symbol("BTCUSDC")
    trades = _full_replay_trades()
    bbo = _full_replay_bbo(trades)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    rows = _full_replay_predicate_rows()

    unordered = _full_replay_params(engine="cpp")
    unordered["_cooldown_duration_policy_cpp_predicate_rows"] = [rows[1], rows[0]]
    with pytest.raises(ValueError, match="predicate-row identity is incomplete"):
        bt._simulate_tick_with_engine(
            "cpp",
            trades,
            empty_i64,
            empty_f64,
            unordered,
            bbo_data=bbo,
        )

    wrong_width = _full_replay_params(engine="cpp")
    rows[1].predicate_values = [cpp.F05TriState.TRUE]
    wrong_width["_cooldown_duration_policy_cpp_predicate_rows"] = [rows[1]]
    with pytest.raises(ValueError, match="predicate-row identity is incomplete"):
        bt._simulate_tick_with_engine(
            "cpp",
            trades,
            empty_i64,
            empty_f64,
            wrong_width,
            bbo_data=bbo,
        )


def _run_cpp_full_replay_with_predicate_rows(
    rows,
    *,
    config=None,
):
    bt.configure_symbol("BTCUSDC")
    trades = _full_replay_trades()
    params = _full_replay_params(engine="cpp")
    if config is not None:
        params["cooldown_duration_policy_cpp_runtime"] = (
            cpp.F05RepeatedBooleanCooldownRuntime(config)
        )
    params["_cooldown_duration_policy_cpp_predicate_rows"] = rows
    return bt._simulate_tick_with_engine(
        "cpp",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
        bbo_data=_full_replay_bbo(trades),
    )


def test_cpp_empty_predicate_values_require_compiled_policy_columns() -> None:
    compiled_row = _full_replay_predicate_rows()[0]
    compiled_row.predicate_values = []
    result = _run_cpp_full_replay_with_predicate_rows([compiled_row])
    assert result["_cooldown_duration_policy_decisions"]

    noncompiled_config = _full_replay_cpp_config()
    noncompiled_config.policy.predicate_columns = [
        *PREDICATE_COLUMNS,
        "predicate::synthetic::python_only",
    ]
    noncompiled_row = _full_replay_predicate_rows()[0]
    noncompiled_row.predicate_values = []
    with pytest.raises(
        ValueError,
        match="predicate-row identity is incomplete",
    ):
        _run_cpp_full_replay_with_predicate_rows(
            [noncompiled_row],
            config=noncompiled_config,
        )


@pytest.mark.parametrize("ordinal_case", ["zero", "duplicate", "decreasing"])
def test_cpp_predicate_row_ordinals_must_be_strictly_increasing(
    ordinal_case: str,
) -> None:
    rows = _full_replay_predicate_rows()
    if ordinal_case == "zero":
        rows = [rows[0]]
        rows[0].exposure_fill_ordinal = 0
    elif ordinal_case == "duplicate":
        rows = rows[:2]
        rows[1].exposure_fill_ordinal = rows[0].exposure_fill_ordinal
    else:
        rows = [rows[1], rows[0]]

    with pytest.raises(
        ValueError,
        match="predicate-row identity is incomplete",
    ):
        _run_cpp_full_replay_with_predicate_rows(rows)


@pytest.mark.parametrize("predicate_width", [1, 2, 4])
def test_cpp_nonempty_predicate_width_must_equal_policy_width(
    predicate_width: int,
) -> None:
    row = _full_replay_predicate_rows()[0]
    row.predicate_values = [cpp.F05TriState.TRUE] * predicate_width

    with pytest.raises(
        ValueError,
        match="predicate-row identity is incomplete",
    ):
        _run_cpp_full_replay_with_predicate_rows([row])


def test_cpp_shared_observation_tape_is_immutable_and_reusable() -> None:
    count = 3
    tape = cpp.build_f05_cooldown_window_tape(
        np.arange(count, dtype=np.int64) * 100_000_000,
        np.arange(1, count + 1, dtype=np.int64) * 100_000_000,
        np.arange(1, count + 1, dtype=np.int64) * 100_000_000,
        np.arange(1, count + 1, dtype=np.int64),
        np.arange(1, count + 1, dtype=np.int64),
        np.full(count, 100.0, dtype=np.float64),
        np.zeros(count, dtype=np.uint8),
        np.zeros(count, dtype=np.uint8),
        np.ones(count, dtype=np.uint8),
        np.ones(count, dtype=np.uint8),
        "4" * 64,
    )

    assert tape.size == count
    assert tape.content_sha256 == "4" * 64
    params_a = cpp.TickReplayParams()
    params_b = cpp.TickReplayParams()
    params_a.f05_cooldown_window_tape_shared = tape
    params_b.f05_cooldown_window_tape_shared = tape
    assert params_a.f05_cooldown_window_tape_shared is tape
    assert params_b.f05_cooldown_window_tape_shared is tape
