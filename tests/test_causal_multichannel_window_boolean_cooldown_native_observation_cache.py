from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from models.tick_data_types import HistoricalExchangeBookEvent
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_features as native,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache as cache,
)

DAY_START_NS = 1_700_006_400_000_000_000
DAY_NS = native.DAY_NS
WINDOW_NS = 100_000_000
MECHANICS_START_NS = DAY_START_NS - 10_000_000_000
TICK_SIZE = 0.1


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


def _levels() -> tuple[tuple[str, int, float], ...]:
    rows: list[tuple[str, int, float]] = []
    for index in range(21):
        rows.append(("bid", 1000 - index, 2.0 + 0.1 * index))
        rows.append(("ask", 1002 + index, 2.0 + 0.1 * index))
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
    ordinal: int,
    receive_delay_ns: int = 5_000_000,
) -> HistoricalExchangeBookEvent:
    exchange_ts_ns = MECHANICS_START_NS + offset_ns
    if event_type == "source_gap":
        return HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="source_gap",
            exchange_ts_ns=exchange_ts_ns,
            exchange_ts_source="source_gap",
            source=f"fixture-{ordinal}",
            source_ordinal=ordinal,
        )
    return HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type=event_type,
        exchange_ts_ns=exchange_ts_ns,
        exchange_ts_source="transaction",
        local_receive_ts_ns=(exchange_ts_ns + receive_delay_ns if receive_delay_ns >= 0 else 0),
        event_time_ns=exchange_ts_ns,
        transaction_time_ns=exchange_ts_ns,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        last_update_id=last_update_id,
        levels=levels,
        source=f"fixture-{ordinal}",
        source_ordinal=ordinal,
    )


def _events() -> tuple[HistoricalExchangeBookEvent, ...]:
    return (
        _event(
            -1_000_000_000,
            event_type="snapshot",
            levels=_levels(),
            last_update_id=100,
            ordinal=1,
        ),
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
            100_000_000,
            event_type="delta",
            levels=(("ask", 1002, 2.5),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
        _event(
            180_000_000,
            event_type="delta",
            levels=(("bid", 999, 1.0),),
            first_update_id=103,
            final_update_id=103,
            previous_final_update_id=102,
            ordinal=4,
            receive_delay_ns=-1,
        ),
        _event(250_000_000, event_type="source_gap", ordinal=5),
        _event(
            320_000_000,
            event_type="snapshot",
            levels=_levels(),
            last_update_id=200,
            ordinal=6,
        ),
        _event(
            420_000_000,
            event_type="delta",
            levels=(("bid", 1000, 1.0),),
            first_update_id=201,
            final_update_id=201,
            previous_final_update_id=200,
            ordinal=7,
        ),
    )


def _contract() -> native.NativeM2BookWindowContract:
    return native.NativeM2BookWindowContract(
        window_start_ns=MECHANICS_START_NS,
        window_end_ns=MECHANICS_START_NS + 500_000_000,
        policy_start_ns=DAY_START_NS,
        feature_ready_clock=native.EXCHANGE_WINDOW_READY_CLOCK,
        max_source_silence_ns=60_000_000,
    )


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": [
                (MECHANICS_START_NS + 10_000_000) // 1_000_000,
                (MECHANICS_START_NS + 100_000_000) // 1_000_000,
                (MECHANICS_START_NS + 100_000_000) // 1_000_000,
                (MECHANICS_START_NS + 150_000_000) // 1_000_000,
                (MECHANICS_START_NS + 250_000_000) // 1_000_000,
                (MECHANICS_START_NS + 450_000_000) // 1_000_000,
                (MECHANICS_START_NS + 500_000_000) // 1_000_000,
            ],
            "qty": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
            "is_buyer_maker": [False, True, False, False, True, False, True],
            "price": [100.0, 100.1, 100.2, 100.2, 100.1, 100.0, 100.0],
        }
    )


def _stream(
    book_audit: native.NativeM2BookFeatureAccumulator,
) -> native.RawNativeM2BookFeatureStream:
    return native.RawNativeM2BookFeatureStream(
        tape=_FakeNativeTape(_events()),
        contract=_contract(),
        audit=book_audit,
    )


def _observation_payload(observation) -> dict[str, object]:
    return {
        "left_ts_ns": observation.left_ts_ns,
        "right_ts_ns": observation.right_ts_ns,
        "feature_ready_ts_ns": observation.feature_ready_ts_ns,
        "market_generation": observation.market_generation,
        "depth_generation": observation.depth_generation,
        "values": dict(observation.values),
        "source_gap": observation.source_gap,
        "source_stale": observation.source_stale,
        "warmup_admitted": observation.warmup_admitted,
    }


def _reference_rows():
    book_audit = native.NativeM2BookFeatureAccumulator()
    trade_audit = native.NativeM2TradeMergeAccumulator()
    rows = tuple(
        native.stream_native_m2_causal_observations(
            book_windows=_stream(book_audit),
            official_trades=_trades(),
            audit=trade_audit,
        )
    )
    return rows, book_audit, trade_audit


def _indexed_rows():
    book_audit = native.NativeM2BookFeatureAccumulator()
    trade_audit = native.NativeM2TradeMergeAccumulator()
    rows = tuple(
        cache.stream_indexed_native_m2_causal_observations(
            book_windows=_stream(book_audit),
            official_trades=_trades(),
            audit=trade_audit,
        )
    )
    return rows, book_audit, trade_audit


def test_indexed_trade_merge_is_exactly_equivalent_to_reference() -> None:
    reference, reference_book, reference_trade = _reference_rows()
    indexed, indexed_book, indexed_trade = _indexed_rows()

    assert [_observation_payload(row) for row in indexed] == [
        _observation_payload(row) for row in reference
    ]
    assert cache.observation_sha256(indexed) == cache.observation_sha256(reference)
    assert cache.audit_payload(indexed_book.freeze()) == cache.audit_payload(
        reference_book.freeze()
    )
    assert cache.audit_payload(indexed_trade.freeze()) == cache.audit_payload(
        reference_trade.freeze()
    )
    assert indexed_book.freeze().missing_receive_clock_window_count == 1
    assert indexed_book.freeze().unobserved_reason_counts["source_stale"] == 2
    assert indexed_trade.freeze().right_boundary_exclusion_count == 3
    assert indexed_trade.freeze().official_trade_count == 6


def test_cache_atomic_roundtrip_preserves_rows_hashes_and_audits(tmp_path: Path) -> None:
    expected, _, _ = _indexed_rows()
    rows, book_audit, trade_audit = _indexed_rows()
    manifest = cache.materialize_observation_cache(
        day="2023-11-14",
        observations=rows,
        output_root=tmp_path,
        source_binding={"fixture": "short_raw_native", "sha256": "a" * 64},
        book_audit=book_audit,
        trade_audit=trade_audit,
        formal_exchange_day=False,
        batch_size=2,
    )

    day_root = tmp_path / "2023-11-14"
    assert day_root.is_dir()
    assert not tuple(tmp_path.glob(".2023-11-14.staging-*"))
    assert manifest["observation_count"] == len(expected)
    assert manifest["source_stream_observation_sha256"] == manifest[
        "cache_readback_observation_sha256"
    ]
    assert manifest["book_feature_audit"] == cache.audit_payload(book_audit.freeze())
    assert manifest["trade_merge_audit"] == cache.audit_payload(trade_audit.freeze())

    validation = cache.validate_admitted_cache(tmp_path, "2023-11-14", deep=True)
    assert validation.observation_count == len(expected)
    restored = tuple(cache.iter_cached_observations(tmp_path, "2023-11-14", batch_size=2))
    assert [_observation_payload(row) for row in restored] == [
        _observation_payload(row) for row in expected
    ]
    assert cache.observation_sha256(restored)[0] == validation.observation_sha256

    handle = cache.open_admitted_observation_cache(tmp_path, "2023-11-14")
    assert cache.audit_payload(handle.book_audit) == cache.audit_payload(book_audit.freeze())
    assert cache.audit_payload(handle.trade_audit) == cache.audit_payload(trade_audit.freeze())
    interval = tuple(
        handle.observations_between(
            start_feature_ready_ts_ns=expected[1].feature_ready_ts_ns,
            end_feature_ready_ts_ns=expected[-1].feature_ready_ts_ns,
            batch_size=2,
        )
    )
    assert [_observation_payload(row) for row in interval] == [
        _observation_payload(row)
        for row in expected
        if expected[1].feature_ready_ts_ns
        <= row.feature_ready_ts_ns
        < expected[-1].feature_ready_ts_ns
    ]


def test_cache_rejects_short_stream_under_formal_d1_contract(tmp_path: Path) -> None:
    rows, book_audit, trade_audit = _indexed_rows()
    with pytest.raises(cache.NativeObservationCacheError, match="exact D-1"):
        cache.materialize_observation_cache(
            day="2023-11-14",
            observations=rows,
            output_root=tmp_path,
            source_binding={"fixture": "short"},
            book_audit=book_audit,
            trade_audit=trade_audit,
            formal_exchange_day=True,
        )
    assert not (tmp_path / "2023-11-14").exists()
    assert not tuple(tmp_path.glob(".2023-11-14.staging-*"))
    assert not (tmp_path / ".2023-11-14.lock").exists()


def test_cache_failure_is_not_admitted(tmp_path: Path) -> None:
    rows, book_audit, trade_audit = _indexed_rows()

    def broken_stream():
        yield rows[0]
        raise RuntimeError("fixture interruption")

    with pytest.raises(RuntimeError, match="fixture interruption"):
        cache.materialize_observation_cache(
            day="2023-11-14",
            observations=broken_stream(),
            output_root=tmp_path,
            source_binding={"fixture": "broken"},
            book_audit=book_audit,
            trade_audit=trade_audit,
            formal_exchange_day=False,
        )
    assert not (tmp_path / "2023-11-14").exists()
    assert not tuple(tmp_path.glob(".2023-11-14.staging-*"))


def test_cache_validation_fails_closed_on_parquet_corruption(tmp_path: Path) -> None:
    rows, book_audit, trade_audit = _indexed_rows()
    cache.materialize_observation_cache(
        day="2023-11-14",
        observations=rows,
        output_root=tmp_path,
        source_binding={"fixture": "corruption"},
        book_audit=book_audit,
        trade_audit=trade_audit,
        formal_exchange_day=False,
    )
    with (tmp_path / "2023-11-14" / cache.PARQUET_NAME).open("ab") as handle:
        handle.write(b"corruption")
    with pytest.raises(cache.NativeObservationCacheError, match="file hash"):
        cache.validate_admitted_cache(tmp_path, "2023-11-14", deep=False)


def test_contract_and_validate_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cache.main(["contract"]) == 0
    contract = json.loads(capsys.readouterr().out)
    assert contract["source_semantics"]["window"] == (
        "100ms [left,right), partial excluded"
    )
    assert contract["materialization"]["book"].startswith("sequential once")

    rows, book_audit, trade_audit = _indexed_rows()
    cache.materialize_observation_cache(
        day="2023-11-14",
        observations=rows,
        output_root=tmp_path,
        source_binding={"fixture": "cli"},
        book_audit=book_audit,
        trade_audit=trade_audit,
        formal_exchange_day=False,
    )
    assert (
        cache.main(
            [
                "validate",
                "--day",
                "2023-11-14",
                "--output-root",
                str(tmp_path),
            ]
        )
        == 0
    )
    validated = json.loads(capsys.readouterr().out)
    assert validated["valid"] is True
    assert validated["observation_count"] == 5


def test_preflight_cli_reports_frozen_exchange_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    day = "2023-11-14"
    day_start_ns = cache._day_start_ns(day)

    @dataclass(frozen=True)
    class _PreflightStream:
        contract: native.NativeM2BookWindowContract

    fake_stream = _PreflightStream(
        native.NativeM2BookWindowContract(
            window_start_ns=day_start_ns - cache.DAY_NS,
            window_end_ns=day_start_ns + cache.DAY_NS,
            policy_start_ns=day_start_ns,
            feature_ready_clock=native.EXCHANGE_WINDOW_READY_CLOCK,
        )
    )

    observed_kwargs: dict[str, object] = {}

    def fake_sources(**kwargs):
        observed_kwargs.update(kwargs)
        return fake_stream, pd.DataFrame(index=range(7)), {"fixture": "preflight"}

    monkeypatch.setattr(cache, "_real_day_source_binding", fake_sources)
    assert (
        cache.main(
            [
                "preflight",
                "--day",
                day,
                "--output-root",
                str(tmp_path),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["eligible"] is True
    assert payload["official_trade_row_count"] == 7
    assert observed_kwargs["load_trades"] is False
    assert payload["contract"]["feature_ready_clock"] == (
        native.EXCHANGE_WINDOW_READY_CLOCK
    )
    assert payload["economic_outcomes_read"] is False
    assert payload["live_policy_authorized"] is False
