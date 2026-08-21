import time
from types import SimpleNamespace

from live.config import Config
from strategy.maker_engine import MakerEngine, POLICY_REASON_FILL_COOLDOWN
from strategy.order_manager import Side
from strategy.signal import Prediction


def _engine_with_active_fill_cooldowns() -> MakerEngine:
    cfg = Config()
    cfg.strategy.max_inventory = 0.026
    cfg.strategy.fill_cooldown = 41.0
    cfg.strategy.fill_cooldown_reducing = 0.0
    cfg.strategy.fill_cooldown_reducing_vol_ref = 0.0
    cfg.strategy.markout_spread_scale = 0.0
    cfg.strategy.thin_depth_threshold = 1.0
    cfg.strategy.kappa_depth_baseline = 1.0
    cfg.risk.max_exec_book_age_s = 0.0

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ref = 50.0
    engine._last_quote_context = {}
    engine._model_dir = ""
    engine._last_prediction = None
    engine._fill_cooldown_until = {
        "BUY": time.time() + 60.0,
        "SELL": time.time() + 60.0,
    }
    engine._toxicity_probs = lambda pred: (0.0, 0.0)
    engine._current_l2_policy_metrics = lambda mid: {
        "depth_age_s": 0.0,
        "microprice_shift_bps": 0.0,
        "l2_quote_flip_rate": 0.0,
        "l2_book_refresh_ratio": 1.0,
        "l2_book_cancel_ratio": 0.0,
        "l2_near_depth_total": 10.0,
    }
    return engine


def test_fill_cooldown_blocks_only_exposure_increasing_side() -> None:
    engine = _engine_with_active_fill_cooldowns()
    pred = Prediction()

    # Long inventory: BUY adds exposure, SELL reduces inventory.
    long_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=0.005, pred=pred)
    long_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.005, pred=pred)
    assert not long_buy.allow_post
    assert long_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN
    assert long_sell.allow_post
    assert not (long_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN)

    # Short inventory: SELL adds exposure, BUY reduces inventory.
    short_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=-0.005, pred=pred)
    short_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=-0.005, pred=pred)
    assert short_buy.allow_post
    assert not (short_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN)
    assert not short_sell.allow_post
    assert short_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_fill_cooldown_blocks_both_sides_when_flat() -> None:
    engine = _engine_with_active_fill_cooldowns()
    pred = Prediction()

    flat_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=0.0, pred=pred)
    flat_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.0, pred=pred)
    assert not flat_buy.allow_post
    assert flat_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN
    assert not flat_sell.allow_post
    assert flat_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_reducing_fill_cooldown_can_pace_reducing_side_when_enabled() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing = 12.0
    pred = Prediction()

    # Long inventory: SELL reduces inventory, but the separate reducing-side
    # cooldown can still pace repeated same-side reducer fills.
    long_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.005, pred=pred)
    assert not long_sell.allow_post
    assert long_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN

    # Short inventory: BUY reduces inventory and is paced by the same knob.
    short_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=-0.005, pred=pred)
    assert not short_buy.allow_post
    assert short_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_reducing_fill_cooldown_vol_multiplier_clamps() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing_vol_ref = 10.0
    engine.cfg.strategy.fill_cooldown_reducing_vol_min_mult = 0.5
    engine.cfg.strategy.fill_cooldown_reducing_vol_max_mult = 2.0

    engine._last_prediction = Prediction(vol_10s=30.0)
    assert engine._reducing_cooldown_vol_mult() == 2.0

    engine._last_prediction = Prediction(vol_10s=2.0)
    assert engine._reducing_cooldown_vol_mult() == 0.5

    engine._last_prediction = Prediction(vol_10s=10.0)
    assert engine._reducing_cooldown_vol_mult() == 1.0


def test_reducing_cooldown_campaign_gate_requires_inventory_or_age_hit() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing_campaign_only = True
    engine.cfg.strategy.fill_cooldown_reducing_inv_ratio = 6.0
    engine.cfg.strategy.fill_cooldown_reducing_age_s = 20.0 * 60.0
    engine.cfg.strategy.order_size = 0.001
    engine.inventory = SimpleNamespace(
        campaign_snapshot=lambda: SimpleNamespace(active=True, age_s=300.0)
    )

    # 5x order_size and a young campaign: ordinary natural reducing should not
    # start a cooldown.
    assert not engine._reducing_cooldown_campaign_gate_active(0.005)

    # 6x order_size is explicitly a high-inventory state.
    assert engine._reducing_cooldown_campaign_gate_active(0.006)

    # Even modest inventory can be paced once the campaign has stayed open long
    # enough to become a campaign-risk problem.
    engine.inventory = SimpleNamespace(
        campaign_snapshot=lambda: SimpleNamespace(active=True, age_s=1300.0)
    )
    assert engine._reducing_cooldown_campaign_gate_active(0.003)


def test_boolean_cooldown_changes_only_sell_exposure_duration() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            assert kwargs["side"] == "SELL"
            assert kwargs["baseline_duration_ms"] == 170_000
            return SimpleNamespace(
                action_id="FIXED_211S",
                duration_ms=211_000,
            )

    engine._boolean_cooldown_policy = StubPolicy()
    selected, decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=170.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-1",
    )
    assert selected == 211.0
    assert decision.action_id == "FIXED_211S"

    buy, buy_decision = engine._select_boolean_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=170.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-2",
    )
    assert buy == 170.0
    assert buy_decision is None

    reducing, reducing_decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=False,
        baseline_duration_s=12.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-3",
    )
    assert reducing == 12.0
    assert reducing_decision is None


def test_boolean_cooldown_control_fallback_preserves_exact_baseline() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            return SimpleNamespace(
                action_id="CONTROL_85N",
                duration_ms=127_500,
            )

    engine._boolean_cooldown_policy = StubPolicy()
    selected, decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=127.5004,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-4",
    )
    assert selected == 127.5004
    assert decision.action_id == "CONTROL_85N"


def test_buy_e3_changes_only_buy_exposure_duration_and_uses_total_action() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            assert kwargs["side"] == "BUY"
            assert kwargs["baseline_duration_ms"] == 255_000
            return SimpleNamespace(action_id="FIXED_2048S", duration_ms=2_048_000)

    engine._buy_e3_cooldown_policy = StubPolicy()
    selected, decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-1",
    )
    assert selected == 2_048.0
    assert decision.action_id == "FIXED_2048S"

    reducing, reducing_decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=False,
        baseline_duration_s=12.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-2",
    )
    assert reducing == 12.0
    assert reducing_decision is None

    sell, sell_decision = engine._select_buy_e3_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-3",
    )
    assert sell == 255.0
    assert sell_decision is None


def test_buy_e3_control_fallback_preserves_exact_b0_duration() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            return SimpleNamespace(action_id="CONTROL_85N", duration_ms=255_000)

    engine._buy_e3_cooldown_policy = StubPolicy()
    selected, decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0004,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-control",
    )
    assert selected == 255.0004
    assert decision.action_id == "CONTROL_85N"
