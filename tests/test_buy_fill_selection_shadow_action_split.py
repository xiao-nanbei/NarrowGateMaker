from __future__ import annotations

from types import SimpleNamespace

import pytest

import strategy.maker_engine as maker_module
from strategy.fill_selection_model import FillSelectionScore
from strategy.maker_engine import MakerEngine, SidePolicyDecision
from strategy.order_manager import Side
from strategy.policy_guards import POLICY_REASON_BUY_FILL_SELECTION


class _AlwaysHitModel:
    def score(self, _features):
        return FillSelectionScore(
            score=0.9,
            missing_features=0,
            used_features=1,
            model_count=1,
        )


def _engine(*, shadow_enabled: bool, action_enabled: bool) -> tuple[MakerEngine, list]:
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        symbol="BTCUSDC",
        strategy=SimpleNamespace(
            buy_fill_selection_shadow_enabled=shadow_enabled,
            buy_fill_selection_live_enabled=action_enabled,
            buy_fill_selection_live_apply_reducing=False,
            buy_fill_selection_live_score_threshold=0.44,
            buy_fill_selection_live_max_missing_features=99,
            buy_fill_selection_live_spread_mult_cap=1.0,
            buy_fill_selection_live_model_path="unused.json",
        ),
    )
    engine._buy_fill_selection_eval_count = 0
    engine._buy_fill_selection_hit_count = 0
    engine._buy_fill_selection_action_count = 0
    engine._buy_fill_selection_last_hit_time = 0.0
    engine._buy_fill_selection_last_eval_time = 0.0
    engine._buy_fill_selection_last_score = 0.0
    engine._buy_fill_selection_last_missing = 0
    engine._buy_fill_selection_shadow_log_path = "shadow.csv"
    engine._fill_selection_live_features = lambda *_args, **_kwargs: {}
    rows = []
    engine._append_row = lambda _path, row: rows.append(row)
    return engine, rows


def _apply(engine: MakerEngine) -> SidePolicyDecision:
    decision = SidePolicyDecision(side="BUY", spread_mult=1.4)
    engine._apply_buy_fill_selection_live_arm(
        side=Side.BUY,
        mid=100.0,
        q=0.0,
        decision=decision,
        quote_ctx={},
        pred=None,
    )
    return decision


def test_shadow_scores_and_logs_without_changing_quote(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maker_module, "_get_buy_fill_selection_model", lambda _path: _AlwaysHitModel())
    engine, rows = _engine(shadow_enabled=True, action_enabled=False)

    decision = _apply(engine)

    assert engine._buy_fill_selection_eval_count == 1
    assert engine._buy_fill_selection_hit_count == 1
    assert engine._buy_fill_selection_action_count == 0
    assert decision.spread_mult == pytest.approx(1.4)
    assert decision.reason_mask == 0
    assert len(rows) == 1
    assert rows[0].enabled == 0
    assert rows[0].actionable_hit == 1
    assert rows[0].final_spread_mult == pytest.approx(1.4)


def test_action_permission_is_required_to_change_quote(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maker_module, "_get_buy_fill_selection_model", lambda _path: _AlwaysHitModel())
    engine, rows = _engine(shadow_enabled=True, action_enabled=True)

    decision = _apply(engine)

    assert engine._buy_fill_selection_action_count == 1
    assert decision.spread_mult == pytest.approx(1.0)
    assert decision.reason_mask & POLICY_REASON_BUY_FILL_SELECTION
    assert rows[0].enabled == 1


def test_disabled_shadow_and_action_skip_scorer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        maker_module,
        "_get_buy_fill_selection_model",
        lambda _path: pytest.fail("disabled scorer must not load"),
    )
    engine, rows = _engine(shadow_enabled=False, action_enabled=False)

    decision = _apply(engine)

    assert engine._buy_fill_selection_eval_count == 0
    assert engine._buy_fill_selection_action_count == 0
    assert decision.spread_mult == pytest.approx(1.4)
    assert rows == []
