from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from models.tick_data_types import HistoricalExchangeBookEvent
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_features as native,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CHANNELS_BY_BLOCK,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_features import (
    DAY_NS,
    DISPLAYED_CHANGE_SEMANTICS,
    EXACT_DISPLAYED_RATE_CHANNELS,
    EXCHANGE_WINDOW_READY_CLOCK,
    NativeFeatureError,
    NativeM2BookFeatureAccumulator,
    NativeM2BookWindowContract,
    NativeM2TradeMergeAccumulator,
    RawNativeM2BookFeatureStream,
    native_m2_book_feature_schema,
    native_m2_observation_channel_names,
    stream_native_m2_causal_observations,
)

DAY_START_NS = 1_700_006_400_000_000_000
WINDOW_NS = 100_000_000
TICK_SIZE = 0.1
MECHANICS_START_NS = DAY_START_NS - 10_000_000_000


@dataclass(frozen=True)
class _FakeNativeTape:
    events: tuple[HistoricalExchangeBookEvent, ...]
    day_start_ns: int = DAY_START_NS
    day_end_ns: int = DAY_START_NS + DAY_NS
    process_start_ns: int = DAY_START_NS - DAY_NS
    warmup_hours: int = 24
    strict_complete: bool = True
    missing_paths: tuple[Path, ...] = ()
    tick_size: float = TICK_SIZE

    def __iter__(self):
        return iter(self.events)


def _levels(
    *,
    bid_shift: float = 0.0,
    ask_shift: float = 0.0,
) -> tuple[tuple[str, int, float], ...]:
    rows: list[tuple[str, int, float]] = []
    for index in range(21):
        rows.append(("bid", 1000 - index, 2.0 + 0.1 * index + bid_shift))
        rows.append(("ask", 1002 + index, 2.0 + 0.1 * index + ask_shift))
    return tuple(rows)


def _event(
    offset_ns: int,
    *,
    event_type: str,
    levels: tuple[tuple[str, int, float], ...] = (),
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    previous_final_update_id: int | None = None,
    last_update_id: int | None = None,
    receive_delay_ns: int = 5_000_000,
    ordinal: int,
    base_ns: int = MECHANICS_START_NS,
) -> HistoricalExchangeBookEvent:
    exchange_ts_ns = int(base_ns) + int(offset_ns)
    if event_type == "source_gap":
        return HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="source_gap",
            exchange_ts_ns=exchange_ts_ns,
            exchange_ts_source="source_gap",
            source=f"hour-{ordinal}",
            source_ordinal=ordinal,
        )
    return HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type=event_type,
        exchange_ts_ns=exchange_ts_ns,
        exchange_ts_source="transaction",
        local_receive_ts_ns=(
            exchange_ts_ns + int(receive_delay_ns) if receive_delay_ns >= 0 else 0
        ),
        event_time_ns=exchange_ts_ns,
        transaction_time_ns=exchange_ts_ns,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        last_update_id=last_update_id,
        levels=levels,
        source=f"hour-{ordinal}",
        source_ordinal=ordinal,
    )


def _snapshot(
    *,
    offset_ns: int = -1_000_000_000,
    update_id: int = 100,
    ordinal: int = 1,
    base_ns: int = MECHANICS_START_NS,
):
    return _event(
        offset_ns,
        event_type="snapshot",
        levels=_levels(),
        last_update_id=update_id,
        ordinal=ordinal,
        base_ns=base_ns,
    )


def _contract(*, end_offset_ns: int, **kwargs) -> NativeM2BookWindowContract:
    return NativeM2BookWindowContract(
        window_start_ns=MECHANICS_START_NS,
        window_end_ns=MECHANICS_START_NS + end_offset_ns,
        policy_start_ns=DAY_START_NS,
        **kwargs,
    )


def test_stream_uses_d1_state_and_excludes_right_boundary_and_partial_window() -> None:
    events = (
        _snapshot(),
        _event(
            50_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.0), ("ask", 1002, 3.5)),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            100_000_000,
            event_type="delta",
            levels=(("bid", 1000, 0.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
    )
    audit = NativeM2BookFeatureAccumulator()
    stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(events),
        contract=_contract(end_offset_ns=250_000_000),
        audit=audit,
    )

    first = next(stream)
    assert first.support_valid is True
    assert first.phase == "D_MINUS_1_WARMUP"
    assert first.left_ts_ns == MECHANICS_START_NS
    assert first.right_ts_ns == MECHANICS_START_NS + WINDOW_NS
    assert first.source_event_count == 1
    assert first.values["best_bid_tick"] == 1000
    assert first.values["best_ask_tick"] == 1002
    assert first.values["best_bid_qty_btc"] == pytest.approx(1.0)
    assert first.values["best_ask_qty_btc"] == pytest.approx(3.5)
    assert first.values["spread_ticks"] == 2
    assert first.values["top20_bid_depth_btc"] is not None
    assert first.values["top20_ask_depth_btc"] is not None
    assert first.values["bid_exact_level_displayed_depletion_btc"] == pytest.approx(1.0)
    assert first.values["ask_exact_level_displayed_refill_btc"] == pytest.approx(1.5)
    assert all(change.semantics == DISPLAYED_CHANGE_SEMANTICS for change in first.level_changes)

    at_best = stream.lookup_target_price(side="BUY", order_price_tick=1000)
    assert at_best.status == "exact"
    assert at_best.displayed_quantity_btc == pytest.approx(1.0)
    assert at_best.displayed_quantity_is_queue_ahead is False

    second = next(stream)
    assert second.support_valid is True
    assert second.source_event_count == 1
    assert second.values["best_bid_tick"] == 999
    assert second.values["bid_exact_level_displayed_depletion_btc"] == pytest.approx(1.0)
    removed = stream.lookup_target_price(side="bid", order_price_usdc_per_btc=100.0)
    assert removed.status == "known_zero"
    assert removed.known is True
    assert removed.displayed_quantity_btc == pytest.approx(0.0)
    outside = stream.lookup_target_price(side="BUY", order_price_tick=900)
    assert outside.status == "unknown"
    assert outside.known is False

    with pytest.raises(StopIteration):
        next(stream)
    frozen = audit.freeze()
    assert frozen.window_count == 2
    assert frozen.observed_window_count == 2
    assert frozen.warmup_admitted is False
    assert frozen.warmup_admission_finalized is False
    assert frozen.warmup_window_count == 2
    assert frozen.partial_trailing_window_excluded is True
    assert frozen.partial_trailing_window_ns == 50_000_000
    assert frozen.exact_level_change_count == 3
    assert frozen.economic_outcomes_read is False


def test_historical_exchange_ready_clock_does_not_mix_receive_lag() -> None:
    events = (
        _event(
            -1_000_000_000,
            event_type="snapshot",
            levels=_levels(),
            last_update_id=100,
            receive_delay_ns=250_000_000,
            ordinal=1,
        ),
        _event(
            50_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.5),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            receive_delay_ns=250_000_000,
            ordinal=2,
        ),
    )
    stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(events),
        contract=_contract(
            end_offset_ns=100_000_000,
            feature_ready_clock=EXCHANGE_WINDOW_READY_CLOCK,
        ),
    )

    window = next(stream)
    assert window.feature_ready_ts_ns == window.right_ts_ns
    assert window.last_source_receive_ts_ns > window.right_ts_ns
    assert window.receive_clock_valid is True


def test_sequence_gap_is_unobserved_until_snapshot_recovery_window_finishes() -> None:
    events = (
        _snapshot(),
        _event(
            50_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.5),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            150_000_000,
            event_type="delta",
            levels=(("ask", 1002, 1.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=999,
            ordinal=3,
        ),
        _event(
            310_000_000,
            event_type="snapshot",
            levels=_levels(bid_shift=1.0),
            last_update_id=200,
            ordinal=4,
        ),
        _event(
            450_000_000,
            event_type="delta",
            levels=(("ask", 1002, 1.5),),
            first_update_id=201,
            final_update_id=201,
            previous_final_update_id=200,
            ordinal=5,
        ),
    )
    audit = NativeM2BookFeatureAccumulator()
    stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(events),
        contract=_contract(end_offset_ns=500_000_000),
        audit=audit,
    )

    first = next(stream)
    assert first.support_valid is True
    broken = next(stream)
    assert broken.support_state == "UNOBSERVED"
    assert "sequence_gap" in broken.unobserved_reasons
    assert all(value is None for value in broken.values.values())
    broken_lookup = stream.lookup_target_price(side="SELL", order_price_tick=1002)
    assert broken_lookup.status == "unknown"
    assert broken_lookup.reason.startswith("window_unobserved:")

    still_broken = next(stream)
    assert still_broken.support_valid is False
    assert "sequence_unavailable" in still_broken.unobserved_reasons
    recovery = next(stream)
    assert recovery.support_valid is False
    assert "snapshot_reset_in_window" in recovery.unobserved_reasons
    recovered = next(stream)
    assert recovered.support_valid is True
    assert recovered.values["best_bid_qty_btc"] == pytest.approx(3.0)
    assert recovered.values["ask_exact_level_displayed_depletion_btc"] == pytest.approx(0.5)

    frozen = audit.freeze()
    assert frozen.sequence_gap_count == 1
    assert frozen.unobserved_window_count == 3
    assert frozen.observed_window_count == 2
    assert frozen.snapshot_reset_window_count == 1


def test_source_gap_and_missing_receive_clock_fail_closed() -> None:
    gap_stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(
            (
                _snapshot(),
                _event(50_000_000, event_type="source_gap", ordinal=2),
            )
        ),
        contract=_contract(end_offset_ns=100_000_000),
    )
    gap = next(gap_stream)
    assert gap.support_valid is False
    assert "source_gap" in gap.unobserved_reasons
    assert gap.source_gap_count == 1

    missing_receive_stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(
            (
                _snapshot(),
                _event(
                    50_000_000,
                    event_type="delta",
                    levels=(("bid", 1000, 1.0),),
                    first_update_id=101,
                    final_update_id=101,
                    previous_final_update_id=100,
                    receive_delay_ns=-1,
                    ordinal=2,
                ),
            )
        ),
        contract=_contract(end_offset_ns=100_000_000),
    )
    missing = next(missing_receive_stream)
    assert missing.support_valid is False
    assert "missing_receive_timestamp" in missing.unobserved_reasons
    assert missing.receive_clock_valid is False


def test_feature_ready_uses_receive_clock_and_freshness_gate_is_explicit() -> None:
    delayed = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(
            (
                _snapshot(),
                _event(
                    50_000_000,
                    event_type="delta",
                    levels=(("bid", 1000, 1.0),),
                    first_update_id=101,
                    final_update_id=101,
                    previous_final_update_id=100,
                    receive_delay_ns=100_000_000,
                    ordinal=2,
                ),
            )
        ),
        contract=_contract(end_offset_ns=100_000_000),
    )
    delayed_window = next(delayed)
    assert delayed_window.support_valid is True
    assert delayed_window.feature_ready_ts_ns == MECHANICS_START_NS + 150_000_000
    assert delayed_window.last_source_exchange_ts_ns == MECHANICS_START_NS + 50_000_000
    assert delayed_window.last_source_receive_ts_ns == MECHANICS_START_NS + 150_000_000
    assert delayed_window.source_exchange_age_ns == 50_000_000
    assert delayed_window.source_receive_age_ns == 0

    stale = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape((_snapshot(),)),
        contract=_contract(
            end_offset_ns=100_000_000,
            max_source_silence_ns=50_000_000,
        ),
    )
    stale_window = next(stale)
    assert stale_window.support_valid is False
    assert stale_window.source_stale is True
    assert "source_stale" in stale_window.unobserved_reasons


def test_target_lookup_requires_tick_aligned_price_and_completed_window() -> None:
    stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape((_snapshot(),)),
        contract=_contract(end_offset_ns=100_000_000),
    )
    with pytest.raises(NativeFeatureError, match="completed window"):
        stream.lookup_target_price(side="BUY", order_price_tick=1000)
    next(stream)
    with pytest.raises(NativeFeatureError, match="exactly one"):
        stream.lookup_target_price(side="BUY")
    with pytest.raises(NativeFeatureError, match="exchange tick"):
        stream.lookup_target_price(side="BUY", order_price_usdc_per_btc=100.05)
    with pytest.raises(NativeFeatureError, match="unsupported"):
        stream.lookup_target_price(side="flat", order_price_tick=1000)


def test_complete_d1_windows_feed_ema_before_policy_and_merge_official_trades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mini_day_ns = 3 * WINDOW_NS
    process_start_ns = DAY_START_NS - mini_day_ns
    monkeypatch.setattr(native, "DAY_NS", mini_day_ns)
    events = (
        _snapshot(offset_ns=10_000_000, base_ns=process_start_ns),
        _event(
            150_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.5),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
            base_ns=process_start_ns,
        ),
        _event(
            350_000_000,
            event_type="delta",
            levels=(("ask", 1002, 1.5),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
            base_ns=process_start_ns,
        ),
    )
    tape = _FakeNativeTape(
        events,
        day_start_ns=DAY_START_NS,
        day_end_ns=DAY_START_NS + mini_day_ns,
        process_start_ns=process_start_ns,
    )
    book_audit = NativeM2BookFeatureAccumulator()
    book_rows = tuple(
        RawNativeM2BookFeatureStream(
            tape=tape,
            contract=NativeM2BookWindowContract(
                window_start_ns=process_start_ns,
                window_end_ns=DAY_START_NS + WINDOW_NS,
                policy_start_ns=DAY_START_NS,
            ),
            audit=book_audit,
        )
    )

    assert [row.phase for row in book_rows] == [
        "D_MINUS_1_WARMUP",
        "D_MINUS_1_WARMUP",
        "D_MINUS_1_WARMUP",
        "POLICY",
    ]
    assert book_rows[0].support_state == "UNOBSERVED"
    assert "snapshot_reset_in_window" in book_rows[0].unobserved_reasons
    assert book_rows[1].support_valid is True
    assert book_rows[2].support_valid is True
    assert book_rows[3].warmup_admitted is True
    assert book_audit.freeze().warmup_admitted is True

    trades = pd.DataFrame(
        {
            "transact_time": [
                (process_start_ns + 150_000_000) // 1_000_000,
                (process_start_ns + 200_000_000) // 1_000_000,
                (process_start_ns + 350_000_000) // 1_000_000,
            ],
            "qty": [0.2, 0.3, 0.4],
            "is_buyer_maker": [False, True, False],
            "price": [100.0, 100.2, 100.1],
        }
    )
    merge_audit = NativeM2TradeMergeAccumulator()
    observations = tuple(
        stream_native_m2_causal_observations(
            book_windows=book_rows,
            official_trades=trades,
            audit=merge_audit,
        )
    )
    expected_names = {spec.name for spec in CHANNELS_BY_BLOCK["M2"]} | set(
        EXACT_DISPLAYED_RATE_CHANNELS
    )
    assert tuple(expected_names) != ()
    assert set(native_m2_observation_channel_names()) == expected_names
    assert all(set(row.values) == expected_names for row in observations)
    assert all(not name.startswith("target_price_") for name in observations[1].values)
    assert observations[0].source_gap is True
    assert all(row.warmup_admitted is False for row in observations[:3])
    assert all(value is None for value in observations[0].values.values())
    assert observations[1].values["aggressive_buy_qty_btc_per_s"] == pytest.approx(2.0)
    assert observations[1].values["trade_count_per_s"] == pytest.approx(10.0)
    assert observations[1].values["sell_run_length"] == pytest.approx(0.0)
    assert observations[2].values["aggressive_sell_qty_btc_per_s"] == pytest.approx(3.0)
    assert observations[2].values["sell_run_length"] == pytest.approx(1.0)
    assert observations[2].values["last_aggressive_sell_age_s"] == pytest.approx(0.1)
    assert observations[3].source_gap is False
    assert observations[3].warmup_admitted is True
    assert observations[3].feature_ready_ts_ns == observations[3].right_ts_ns

    frozen_merge = merge_audit.freeze()
    assert frozen_merge.warmup_window_count == 3
    assert frozen_merge.policy_window_count == 1
    assert frozen_merge.official_trade_count == 3
    assert frozen_merge.aggressive_buy_trade_count == 2
    assert frozen_merge.aggressive_sell_trade_count == 1
    assert frozen_merge.right_boundary_exclusion_count == 1
    assert frozen_merge.economic_outcomes_read is False


def test_policy_windows_fail_closed_when_d1_receive_clock_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mini_day_ns = 3 * WINDOW_NS
    process_start_ns = DAY_START_NS - mini_day_ns
    monkeypatch.setattr(native, "DAY_NS", mini_day_ns)
    events = (
        _snapshot(offset_ns=10_000_000, base_ns=process_start_ns),
        _event(
            150_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.5),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
            base_ns=process_start_ns,
        ),
        _event(
            250_000_000,
            event_type="delta",
            levels=(("ask", 1002, 1.5),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            receive_delay_ns=-1,
            ordinal=3,
            base_ns=process_start_ns,
        ),
    )
    audit = NativeM2BookFeatureAccumulator()
    rows = tuple(
        RawNativeM2BookFeatureStream(
            tape=_FakeNativeTape(
                events,
                day_start_ns=DAY_START_NS,
                day_end_ns=DAY_START_NS + mini_day_ns,
                process_start_ns=process_start_ns,
            ),
            contract=NativeM2BookWindowContract(
                window_start_ns=process_start_ns,
                window_end_ns=DAY_START_NS + WINDOW_NS,
                policy_start_ns=DAY_START_NS,
            ),
            audit=audit,
        )
    )
    policy = rows[-1]
    assert policy.phase == "POLICY"
    assert policy.warmup_admitted is False
    assert policy.support_valid is False
    assert "D_minus_1_warmup_not_admitted" in policy.unobserved_reasons
    observations = tuple(
        stream_native_m2_causal_observations(
            book_windows=rows,
            official_trades=pd.DataFrame(
                columns=("transact_time", "qty", "is_buyer_maker")
            ),
        )
    )
    assert observations[-1].warmup_admitted is False
    frozen = audit.freeze()
    assert frozen.warmup_admission_finalized is True
    assert frozen.warmup_missing_receive_timestamp_count == 1


def test_official_trade_merge_rejects_implicit_boolean_values() -> None:
    stream = RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape((_snapshot(),)),
        contract=_contract(end_offset_ns=WINDOW_NS),
    )
    with pytest.raises(NativeFeatureError, match="explicit bool"):
        tuple(
            stream_native_m2_causal_observations(
                book_windows=stream,
                official_trades=pd.DataFrame(
                    {
                        "transact_time": [MECHANICS_START_NS // 1_000_000],
                        "qty": [0.1],
                        "is_buyer_maker": ["nan"],
                    }
                ),
            )
        )


def test_contract_rejects_non_d1_or_incomplete_native_tape() -> None:
    contract = _contract(end_offset_ns=100_000_000)
    with pytest.raises(NativeFeatureError, match="D-1"):
        RawNativeM2BookFeatureStream(
            tape=_FakeNativeTape((_snapshot(),), warmup_hours=0),
            contract=contract,
        )
    with pytest.raises(NativeFeatureError, match="strict-complete"):
        RawNativeM2BookFeatureStream(
            tape=_FakeNativeTape(
                (_snapshot(),),
                strict_complete=False,
                missing_paths=(Path("missing-hour"),),
            ),
            contract=contract,
        )


def test_schema_is_outcome_blind_and_does_not_claim_queue_or_cancel_attribution() -> None:
    schema = native_m2_book_feature_schema()
    assert schema["economic_outcomes_read"] is False
    assert schema["forward_fill_across_invalid_segment"] is False
    assert schema["target_price_lookup_is_queue_ahead"] is False
    assert schema["target_price_lookup_in_ema_observation"] is False
    assert schema["policy_start"] == "target_day_00:00:00Z"
    assert schema["warmup"] == "previous_natural_UTC_day_24h_no_D-2"
    assert (
        schema["official_trade_merge"]["event_at_right_boundary"]
        == "excluded_then_enters_next_window"
    )
    assert schema["official_trade_merge"]["receive_time_transport_authority"] is False
    assert set(schema["m2_observation_channel_names"]) == {
        spec.name for spec in CHANNELS_BY_BLOCK["M2"]
    } | set(EXACT_DISPLAYED_RATE_CHANNELS)
    assert "not cancel attribution" in schema["displayed_change_semantics"]
    assert schema["parser"].endswith("CryptoHFTExchangeBookTape")
    assert schema["scheduler"].endswith("HistoricalExchangeBookScheduler")
