from __future__ import annotations

import math

import pytest

from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    CAUSES,
    ActiveOrderCompetingRiskCIF,
    CIFMechanicsError,
    GridEdgeSequenceError,
    MissedGridEdgeError,
    TerminalStateError,
    jointly_normalized_hazards,
)


def _rates(**overrides: float) -> dict[str, float]:
    rates = dict.fromkeys(CAUSES, 0.0)
    rates.update(overrides)
    return rates


def _state() -> ActiveOrderCompetingRiskCIF:
    return ActiveOrderCompetingRiskCIF.start(
        spell_id="order-1:spell-0",
        phase="ACTIVE",
        remaining_qty=0.001,
        last_edge=10,
    )


def test_joint_rates_use_single_competing_risk_normalization() -> None:
    rates = _rates(
        favorable_fill=1.0,
        adverse_fill=2.0,
        cancel_ack=3.0,
        other_terminal=4.0,
    )
    hazards, no_event = jointly_normalized_hazards(rates)

    event_probability = 1.0 - math.exp(-1.0)
    assert no_event == pytest.approx(math.exp(-1.0))
    assert hazards["favorable_fill"] == pytest.approx(0.1 * event_probability)
    assert hazards["adverse_fill"] == pytest.approx(0.2 * event_probability)
    assert hazards["cancel_ack"] == pytest.approx(0.3 * event_probability)
    assert hazards["other_terminal"] == pytest.approx(0.4 * event_probability)
    assert math.fsum((no_event, *hazards.values())) == 1.0


def test_stepwise_survival_cif_and_mass_are_exact_and_monotone() -> None:
    rates = _rates(favorable_fill=1.0, adverse_fill=2.0, cancel_ack=1.0)
    state = _state()
    prior_survival = state.survival
    prior_cif = state.cif_by_cause

    for edge in range(11, 211):
        state, interval = state.advance(edge=edge, rates_per_s=rates)
        assert state.survival <= prior_survival
        assert all(
            state.cif_by_cause[cause] >= prior_cif[cause] for cause in CAUSES
        )
        assert interval.mass_after == 1.0
        assert math.fsum((state.survival, *state.cif_by_cause.values())) == 1.0
        prior_survival = state.survival
        prior_cif = state.cif_by_cause

    assert state.survival == pytest.approx(math.exp(-80.0))
    assert state.cif_by_cause["favorable_fill"] == pytest.approx(0.25)
    assert state.cif_by_cause["adverse_fill"] == pytest.approx(0.50)
    assert state.cif_by_cause["cancel_ack"] == pytest.approx(0.25)
    assert state.cif_by_cause["other_terminal"] == 0.0


def test_zero_rates_preserve_state_and_still_advance_one_edge() -> None:
    state = _state()
    next_state, interval = state.advance(edge=11, rates_per_s=_rates())

    assert next_state.last_edge == 11
    assert next_state.survival == 1.0
    assert next_state.cif_by_cause == dict.fromkeys(CAUSES, 0.0)
    assert interval.no_event_probability == 1.0
    assert interval.mass_after == 1.0


@pytest.mark.parametrize(
    ("rates", "message"),
    [
        (_rates(adverse_fill=-0.1), "non-negative"),
        (_rates(cancel_ack=math.nan), "finite"),
        (_rates(other_terminal=math.inf), "finite"),
        ({"favorable_fill": 1.0}, "cause mismatch"),
        ({**_rates(), "partial_fill": 1.0}, "cause mismatch"),
    ],
)
def test_rate_validation_fails_closed(
    rates: dict[str, float], message: str
) -> None:
    state = _state()
    with pytest.raises(CIFMechanicsError, match=message):
        state.advance(edge=11, rates_per_s=rates)
    assert state == _state()


def test_missed_duplicate_and_reverse_edges_fail_without_backfill() -> None:
    state = _state()
    rates = _rates(adverse_fill=1.0)

    with pytest.raises(MissedGridEdgeError, match="expected 11, got 12"):
        state.advance(edge=12, rates_per_s=rates)
    with pytest.raises(GridEdgeSequenceError, match="expected 11, got 10"):
        state.advance(edge=10, rates_per_s=rates)
    assert state.last_edge == 10
    assert state.survival == 1.0


def test_partial_fill_requires_explicit_new_risk_spell() -> None:
    state, _ = _state().advance(
        edge=11, rates_per_s=_rates(favorable_fill=1.0)
    )

    with pytest.raises(CIFMechanicsError, match="partial fill"):
        state.terminate(cause="favorable_fill", remaining_qty=0.0004)

    reset = state.reset_after_partial_fill(
        spell_id="order-1:spell-1",
        phase="PARTIALLY_FILLED",
        remaining_qty=0.0004,
        last_edge=11,
    )
    assert not reset.terminal
    assert reset.spell_id == "order-1:spell-1"
    assert reset.phase == "PARTIALLY_FILLED"
    assert reset.remaining_qty == pytest.approx(0.0004)
    assert reset.last_edge == 11
    assert reset.survival == 1.0
    assert reset.cif_by_cause == dict.fromkeys(CAUSES, 0.0)

    continued, _ = reset.advance(edge=12, rates_per_s=_rates(cancel_ack=2.0))
    assert continued.last_edge == 12


def test_partial_fill_reset_rejects_implicit_or_non_decreasing_quantity() -> None:
    state = _state()
    with pytest.raises(CIFMechanicsError, match="new spell_id"):
        state.reset_after_partial_fill(
            spell_id=state.spell_id,
            phase="PARTIALLY_FILLED",
            remaining_qty=0.0005,
            last_edge=10,
        )
    with pytest.raises(CIFMechanicsError, match="strictly between"):
        state.reset_after_partial_fill(
            spell_id="order-1:spell-1",
            phase="PARTIALLY_FILLED",
            remaining_qty=0.001,
            last_edge=10,
        )
    with pytest.raises(CIFMechanicsError, match="preserve"):
        state.reset_after_partial_fill(
            spell_id="order-1:spell-1",
            phase="PARTIALLY_FILLED",
            remaining_qty=0.0005,
            last_edge=11,
        )


def test_terminal_state_stops_evaluation_phase_changes_and_spell_resets() -> None:
    state, _ = _state().advance(edge=11, rates_per_s=_rates(cancel_ack=1.0))
    terminal = state.terminate(cause="cancel_ack", remaining_qty=0.001)

    assert terminal.terminal
    assert terminal.phase == "EXCHANGE_TERMINAL"
    assert terminal.terminal_cause == "cancel_ack"
    assert terminal.terminal_edge == 11
    with pytest.raises(TerminalStateError):
        terminal.advance(edge=12, rates_per_s=_rates())
    with pytest.raises(TerminalStateError):
        terminal.transition_phase("ACTIVE")
    with pytest.raises(TerminalStateError):
        terminal.reset_after_partial_fill(
            spell_id="order-1:spell-1",
            phase="PARTIALLY_FILLED",
            remaining_qty=0.0005,
            last_edge=11,
        )
    with pytest.raises(TerminalStateError):
        terminal.terminate(cause="other_terminal")


def test_phase_transitions_preserve_cif_state() -> None:
    state, _ = _state().advance(edge=11, rates_per_s=_rates(adverse_fill=2.0))
    pending = state.transition_phase("CANCEL_PENDING")
    restored = pending.transition_phase("ACTIVE")

    assert pending.phase == "CANCEL_PENDING"
    assert restored.phase == "ACTIVE"
    assert restored.survival == state.survival
    assert restored.cumulative_incidence == state.cumulative_incidence


def test_checkpoint_roundtrip_restores_all_mechanics_state() -> None:
    state, _ = _state().advance(
        edge=11,
        rates_per_s=_rates(
            favorable_fill=0.25,
            adverse_fill=0.5,
            cancel_ack=0.75,
            other_terminal=0.1,
        ),
    )
    state = state.transition_phase("CANCEL_PENDING")
    restored = ActiveOrderCompetingRiskCIF.restore(state.checkpoint())

    assert restored == state
    assert restored.checkpoint() == state.checkpoint()

    terminal = state.terminate(cause="other_terminal", remaining_qty=0.001)
    assert ActiveOrderCompetingRiskCIF.restore(terminal.checkpoint()) == terminal


def test_checkpoint_restore_rejects_tampered_mass_and_schema() -> None:
    payload = _state().checkpoint()
    payload["survival"] = 0.9
    with pytest.raises(CIFMechanicsError, match="conserve probability mass"):
        ActiveOrderCompetingRiskCIF.restore(payload)

    payload = _state().checkpoint()
    payload["unexpected"] = True
    with pytest.raises(CIFMechanicsError, match="schema mismatch"):
        ActiveOrderCompetingRiskCIF.restore(payload)
