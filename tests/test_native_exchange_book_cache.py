from __future__ import annotations

from pathlib import Path

import pytest

from data.build_active_order_queue_tape import LogicalMessage
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from models.native_exchange_book_cache import (
    ensure_native_book_hour_cache,
    iter_native_book_hour_cache,
    native_book_hour_identity,
    require_native_book_hour_cache,
)
from models.replay_cache_dag import (
    REPLAY_WINDOW_CACHE_GRAPH,
    ReplayCacheGraph,
    ReplayCacheNodeSpec,
)
from models.tick_data_types import HistoricalExchangeBookEvent


def _source(tmp_path: Path) -> Path:
    path = (
        tmp_path
        / "binance_futures"
        / "2026-01-02"
        / "03"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    path.parent.mkdir(parents=True)
    path.write_bytes(b"immutable raw source")
    return path


def _events(source: Path) -> tuple[HistoricalExchangeBookEvent, ...]:
    return (
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="snapshot",
            exchange_ts_ns=1_767_322_800_001_000_000,
            exchange_ts_source="transaction",
            local_receive_ts_ns=1_767_322_800_002_345_678,
            event_time_ns=1_767_322_800_000_000_000,
            transaction_time_ns=1_767_322_800_001_000_000,
            last_update_id=100,
            levels=(("bid", 900_000, 1.25), ("ask", 900_002, 2.5)),
            source=str(source),
            source_ordinal=1,
        ),
        HistoricalExchangeBookEvent(
            market_id="binance_futures:perpetual:BTCUSDC",
            event_type="delta",
            exchange_ts_ns=1_767_322_800_101_000_000,
            exchange_ts_source="event",
            local_receive_ts_ns=1_767_322_800_103_000_001,
            event_time_ns=1_767_322_800_101_000_000,
            transaction_time_ns=0,
            first_update_id=101,
            final_update_id=102,
            previous_final_update_id=100,
            levels=(("bid", 900_000, 0.0), ("ask", 900_003, 3.75)),
            source=str(source),
            source_ordinal=2,
        ),
    )


def _identity(source: Path) -> dict[str, object]:
    return native_book_hour_identity(
        source_path=source,
        symbol="BTCUSDC",
        exchange="binance_futures",
        market_id="binance_futures:perpetual:BTCUSDC",
        tick_size=0.1,
        parser_identity_sha256="a" * 64,
    )


def test_native_hour_cache_round_trip_is_event_exact(tmp_path: Path) -> None:
    source = _source(tmp_path)
    expected = _events(source)
    cache_root = tmp_path / "cache"

    first = ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        events_factory=lambda: iter(expected),
    )
    assert not first.cache_hit
    assert first.event_count == 2
    assert first.level_count == 4
    assert tuple(iter_native_book_hour_cache(first)) == expected

    def unexpected_reparse():
        raise AssertionError("cache hit must not reparse the source")

    second = ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        events_factory=unexpected_reparse,
    )
    assert second.cache_hit
    assert second.data_path == first.data_path
    assert tuple(iter_native_book_hour_cache(second)) == expected


def test_native_hour_cache_invalidates_when_source_identity_changes(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path)
    cache_root = tmp_path / "cache"
    first = ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        events_factory=lambda: iter(_events(source)),
    )

    source.write_bytes(b"immutable raw source changed")
    second = ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        events_factory=lambda: iter(_events(source)),
    )

    assert second.identity_sha256 != first.identity_sha256
    assert second.data_path != first.data_path
    assert not second.cache_hit


def test_native_hour_cache_can_be_required_without_source_reparse(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path)
    cache_root = tmp_path / "cache"
    written = ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        events_factory=lambda: iter(_events(source)),
    )

    required = require_native_book_hour_cache(
        cache_root=cache_root,
        identity=_identity(source),
        verify_sha256=True,
    )

    assert required.cache_hit
    assert required.data_path == written.data_path
    assert tuple(iter_native_book_hour_cache(required)) == _events(source)


def test_cryptohft_tape_reuses_hour_cache_across_iterations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = (
        tmp_path
        / "raw"
        / "binance_futures"
        / "2026-01-02"
        / "00"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source")
    calls: list[Path] = []

    def fake_parser(path: Path, tick_size: float):
        calls.append(path)
        assert tick_size == 0.1
        yield LogicalMessage(
            event_type="snapshot",
            exchange_ts_ms=1_767_312_000_100,
            receive_time_ms=1_767_312_000_102,
            receive_time_ns=1_767_312_000_102_000_321,
            event_time_ms=1_767_312_000_099,
            transaction_time_ms=1_767_312_000_100,
            first_update_id=None,
            final_update_id=None,
            previous_final_update_id=None,
            last_update_id=100,
            levels=[("bid", 900_000, 1.0), ("ask", 900_001, 2.0)],
        )

    monkeypatch.setattr(
        "data.build_active_order_queue_tape.iter_cryptohft_logical_messages",
        fake_parser,
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=tmp_path / "raw",
        day="2026-01-02",
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=0,
        strict_complete=False,
        cache_dir=tmp_path / "cache",
    )

    first = tuple(tape)
    second = tuple(tape)

    assert first == second
    assert len(first) == 24
    assert calls == [source]
    assert tape.cache_stats()["hour_misses_or_writes"] == 1
    assert tape.cache_stats()["hour_hits"] == 1


def test_cryptohft_tape_binds_explicit_d_plus_one_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw"
    source_days = ("2026-01-02", "2026-01-03")
    sources = []
    for source_day in source_days:
        source = (
            raw_root
            / "binance_futures"
            / source_day
            / "00"
            / "BTCUSDC_orderbook.parquet.zst"
        )
        source.parent.mkdir(parents=True)
        source.write_bytes(source_day.encode("ascii"))
        sources.append(source)

    def fake_parser(path: Path, tick_size: float):
        assert tick_size == 0.1
        day_offset_ms = 0 if path == sources[0] else 86_400_000
        yield LogicalMessage(
            event_type="snapshot",
            exchange_ts_ms=1_767_312_000_100 + day_offset_ms,
            receive_time_ms=1_767_312_000_102 + day_offset_ms,
            receive_time_ns=(
                1_767_312_000_102_000_321 + day_offset_ms * 1_000_000
            ),
            event_time_ms=1_767_312_000_099 + day_offset_ms,
            transaction_time_ms=1_767_312_000_100 + day_offset_ms,
            first_update_id=None,
            final_update_id=None,
            previous_final_update_id=None,
            last_update_id=100,
            levels=[("bid", 900_000, 1.0), ("ask", 900_001, 2.0)],
        )

    monkeypatch.setattr(
        "data.build_active_order_queue_tape.iter_cryptohft_logical_messages",
        fake_parser,
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day="2026-01-02",
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=0,
        continuation_hours=24,
        strict_complete=False,
        cache_enabled=False,
    )

    events = tuple(tape)
    identity = tape.identity(include_sha256=False)
    assert len(events) == 48
    assert tape.source_paths == tuple(sources)
    assert identity["continuation_hours"] == 24


def test_cryptohft_tape_prebuilds_then_requires_read_only_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw"
    day = "2026-01-02"
    for hour in range(24):
        source = (
            raw_root
            / "binance_futures"
            / day
            / f"{hour:02d}"
            / "BTCUSDC_orderbook.parquet.zst"
        )
        source.parent.mkdir(parents=True)
        source.write_bytes(f"source-{hour}".encode("ascii"))
    parser_calls: list[Path] = []

    def fake_parser(path: Path, tick_size: float):
        parser_calls.append(path)
        hour = int(path.parent.name)
        timestamp_ms = 1_767_312_000_100 + hour * 3_600_000
        yield LogicalMessage(
            event_type="snapshot",
            exchange_ts_ms=timestamp_ms,
            receive_time_ms=timestamp_ms + 2,
            receive_time_ns=(timestamp_ms + 2) * 1_000_000 + 321,
            event_time_ms=timestamp_ms - 1,
            transaction_time_ms=timestamp_ms,
            first_update_id=None,
            final_update_id=None,
            previous_final_update_id=None,
            last_update_id=100 + hour,
            levels=[("bid", 900_000, 1.0), ("ask", 900_001, 2.0)],
        )

    monkeypatch.setattr(
        "data.build_active_order_queue_tape.iter_cryptohft_logical_messages",
        fake_parser,
    )
    cache_root = tmp_path / "cache"
    builder = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day=day,
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=0,
        strict_complete=True,
        cache_dir=cache_root,
    )
    audit = builder.materialize_cache(verify_sha256=True)

    assert audit["expected_hour_count"] == 24
    assert audit["complete_hour_count"] == 24
    assert len(audit["hours"]) == 24
    assert len(parser_calls) == 24

    parser_calls.clear()
    reader = CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day=day,
        symbol="BTCUSDC",
        tick_size=0.1,
        warmup_hours=0,
        strict_complete=True,
        cache_dir=cache_root,
        cache_read_only=True,
    )
    assert len(tuple(reader)) == 24
    assert not parser_calls
    assert reader.cache_stats()["read_only"] is True


def test_replay_cache_dag_marks_action_paths_non_reusable() -> None:
    order = REPLAY_WINDOW_CACHE_GRAPH.validate()
    assert order.index("native_orderbook_hour_source") < order.index(
        "native_orderbook_logical_hour"
    )
    by_name = {
        node.name: node for node in REPLAY_WINDOW_CACHE_GRAPH.nodes
    }
    assert by_name["native_orderbook_logical_hour"].materialization == "persistent"
    assert by_name["strategy_order_lifecycle"].materialization == "forbidden"
    assert by_name["inventory_campaign_outcome"].materialization == "forbidden"


def test_strategy_dependent_node_cannot_be_persistent() -> None:
    with pytest.raises(ValueError, match="cannot be persistent"):
        ReplayCacheGraph(
            graph_id="invalid.v1",
            nodes=(
                ReplayCacheNodeSpec(
                    name="bad",
                    dependencies=(),
                    materialization="persistent",
                    artifact_unit="action_path",
                    source_clock="decision_time",
                    visibility_clock="decision_time",
                    cache_namespace="bad",
                    identity_fields=("action",),
                    strategy_dependent=True,
                ),
            ),
        )
