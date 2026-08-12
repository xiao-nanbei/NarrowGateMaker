from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import zstandard as zstd

from data.build_active_order_queue_tape import (
    build_active_order_queue_tape,
    iter_cryptohft_logical_messages,
)


@pytest.fixture(scope="module")
def sparse_queue_tape(tmp_path_factory):
    root = tmp_path_factory.mktemp("active-order-queue-tape")
    day = "2026-01-02"
    day_start_ms = int(
        datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000
    )
    raw_root = root / "raw"
    raw_path = (
        raw_root
        / "binance_futures"
        / day
        / "00"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    raw_path.parent.mkdir(parents=True)

    def row(
        offset_ms: int,
        *,
        event_type: str,
        side: str,
        price: float,
        quantity: float,
        first_update_id=None,
        final_update_id=None,
        prev_final_update_id=None,
        last_update_id=None,
    ):
        timestamp = day_start_ms + offset_ms
        return {
            "event_time": timestamp,
            "transaction_time": timestamp,
            "received_time": timestamp * 1_000_000,
            "event_type": event_type,
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "prev_final_update_id": prev_final_update_id,
            "last_update_id": last_update_id,
            "side": side,
            "price": price,
            "quantity": quantity,
        }

    raw_rows = [
        row(
            1_000,
            event_type="snapshot",
            side="bid",
            price=100.0,
            quantity=5.0,
            last_update_id=100,
        ),
        row(
            1_000,
            event_type="snapshot",
            side="bid",
            price=99.8,
            quantity=3.0,
            last_update_id=100,
        ),
        row(
            1_000,
            event_type="snapshot",
            side="ask",
            price=100.2,
            quantity=4.0,
            last_update_id=100,
        ),
        row(
            1_000,
            event_type="snapshot",
            side="ask",
            price=100.4,
            quantity=2.0,
            last_update_id=100,
        ),
        row(
            2_000,
            event_type="update",
            side="bid",
            price=100.0,
            quantity=4.0,
            first_update_id=101,
            final_update_id=101,
            prev_final_update_id=100,
        ),
        row(
            2_200,
            event_type="update",
            side="bid",
            price=100.0,
            quantity=2.0,
            first_update_id=102,
            final_update_id=102,
            prev_final_update_id=999,
        ),
        row(
            2_300,
            event_type="update",
            side="bid",
            price=100.0,
            quantity=1.0,
            first_update_id=103,
            final_update_id=103,
            prev_final_update_id=102,
        ),
        row(
            2_600,
            event_type="snapshot",
            side="bid",
            price=100.0,
            quantity=6.0,
            last_update_id=200,
        ),
        row(
            2_600,
            event_type="snapshot",
            side="bid",
            price=99.8,
            quantity=2.0,
            last_update_id=200,
        ),
        row(
            2_600,
            event_type="snapshot",
            side="ask",
            price=100.2,
            quantity=3.0,
            last_update_id=200,
        ),
        row(
            2_600,
            event_type="snapshot",
            side="ask",
            price=100.4,
            quantity=1.0,
            last_update_id=200,
        ),
    ]
    raw_parquet = raw_path.with_suffix("")
    pd.DataFrame(raw_rows).to_parquet(raw_parquet, index=False)
    compressor = zstd.ZstdCompressor(level=1)
    with raw_parquet.open("rb") as source, raw_path.open("wb") as target:
        compressor.copy_stream(source, target)
    raw_parquet.unlink()

    manifest = root / "watch_manifest.parquet"
    watches = pd.DataFrame(
        [
            {
                "day": day,
                "watch_id": "exact",
                "order_id": "order-exact",
                "side": "BUY",
                "price": 100.0,
                "activate_ts_ms": day_start_ms + 1_500,
                "stop_ts_ms": day_start_ms + 2_900,
            },
            {
                "day": day,
                "watch_id": "known-zero",
                "order_id": "order-known-zero",
                "side": "bid",
                "price": 99.9,
                "activate_ts_ms": day_start_ms + 1_500,
                "stop_ts_ms": day_start_ms + 2_100,
            },
            {
                "day": day,
                "watch_id": "outside",
                "order_id": "order-outside",
                "side": "BUY",
                "price": 99.7,
                "activate_ts_ms": day_start_ms + 1_500,
                "stop_ts_ms": day_start_ms + 2_100,
            },
            {
                "day": day,
                "watch_id": "ambiguous",
                "order_id": "order-ambiguous",
                "side": "BUY",
                "price": 100.0,
                "activate_ts_ms": day_start_ms + 2_000,
                "stop_ts_ms": day_start_ms + 2_100,
            },
            {
                "day": day,
                "watch_id": "gap",
                "order_id": "order-gap",
                "side": "BUY",
                "price": 100.0,
                "activate_ts_ms": day_start_ms + 2_400,
                "stop_ts_ms": day_start_ms + 2_900,
            },
        ]
    )
    watches.to_parquet(manifest, index=False)
    output_dir = root / "output"
    summary = build_active_order_queue_tape(
        watch_manifest=manifest,
        raw_root=raw_root,
        output_dir=output_dir,
        tick_size=0.1,
        warmup_hours=0,
        reuse_raw_only=True,
    )
    assert summary["schema_version"] == "active_order_queue_tape_v3"
    assert summary["watch_manifest"] == manifest.name
    return {
        "day_start_ms": day_start_ms,
        "raw_path": raw_path,
        "output_dir": output_dir,
        "summary": summary,
        "seeds": pd.read_parquet(output_dir / "seeds.parquet").set_index("watch_id"),
        "events": pd.read_parquet(output_dir / "level_events.parquet"),
    }


def test_logical_message_stream_is_batch_size_invariant(
    sparse_queue_tape,
):
    raw_path = sparse_queue_tape["raw_path"]

    tiny = list(
        iter_cryptohft_logical_messages(
            raw_path,
            0.1,
            batch_size=2,
        )
    )
    normal = list(
        iter_cryptohft_logical_messages(
            raw_path,
            0.1,
            batch_size=100_000,
        )
    )

    assert tiny == normal


def test_header_only_logical_messages_preserve_sequence_identity(
    sparse_queue_tape,
):
    raw_path = sparse_queue_tape["raw_path"]

    complete = list(iter_cryptohft_logical_messages(raw_path, 0.1))
    headers = list(
        iter_cryptohft_logical_messages(
            raw_path,
            0.1,
            include_levels=False,
        )
    )

    assert len(headers) == len(complete)
    for header, message in zip(headers, complete, strict=True):
        assert header.event_type == message.event_type
        assert header.exchange_ts_ms == message.exchange_ts_ms
        assert header.first_update_id == message.first_update_id
        assert header.final_update_id == message.final_update_id
        assert (
            header.previous_final_update_id
            == message.previous_final_update_id
        )
        assert header.last_update_id == message.last_update_id
        assert header.levels == []


def test_exact_seed_uses_integer_tick_and_prior_complete_state(sparse_queue_tape):
    seed = sparse_queue_tape["seeds"].loc["exact"]

    assert seed["price_tick"] == 1_000
    assert seed["seed_status"] == "exact"
    assert seed["seed_reason"] == "visible_quantity"
    assert seed["seed_qty"] == pytest.approx(5.0)
    assert seed["seed_asof_ts_ms"] == sparse_queue_tape["day_start_ms"] + 1_000
    assert seed["seed_update_id"] == 100
    assert seed["segment_id"] == 1
    assert seed["seed_best_bid_tick"] == 1_000
    assert seed["seed_best_ask_tick"] == 1_002


def test_snapshot_range_distinguishes_known_zero_from_outside(sparse_queue_tape):
    seeds = sparse_queue_tape["seeds"]

    assert seeds.loc["known-zero", "price_tick"] == 999
    assert seeds.loc["known-zero", "seed_status"] == "known_zero"
    assert (
        seeds.loc["known-zero", "seed_reason"]
        == "inside_snapshot_range_absent"
    )
    assert seeds.loc["known-zero", "seed_qty"] == pytest.approx(0.0)
    assert seeds.loc["outside", "price_tick"] == 997
    assert seeds.loc["outside", "seed_status"] == "unknown"
    assert seeds.loc["outside", "seed_reason"] == "outside_snapshot_range"
    assert pd.isna(seeds.loc["outside", "seed_qty"])
    assert (
        seeds.loc["outside", "seed_asof_ts_ms"]
        == sparse_queue_tape["day_start_ms"] + 1_000
    )
    assert seeds.loc["outside", "segment_id"] == 1


def test_sequence_gap_invalidates_until_next_native_snapshot(sparse_queue_tape):
    events = sparse_queue_tape["events"]
    exact_events = events.loc[events["watch_id"] == "exact"]
    gap_seed = sparse_queue_tape["seeds"].loc["gap"]
    gap_events = events.loc[events["watch_id"] == "gap"]

    invalidate = exact_events.loc[exact_events["event_code"] == "invalidate"].iloc[0]
    assert invalidate["exchange_ts_ms"] == sparse_queue_tape["day_start_ms"] + 2_200
    assert invalidate["state_status"] == "unknown"
    assert gap_seed["seed_status"] == "unknown"
    assert gap_seed["seed_reason"] == "sequence_unavailable"

    recovered = gap_events.loc[gap_events["event_code"] == "snapshot"].iloc[0]
    assert recovered["exchange_ts_ms"] == sparse_queue_tape["day_start_ms"] + 2_600
    assert recovered["state_status"] == "exact"
    assert recovered["qty_after"] == pytest.approx(6.0)
    assert recovered["segment_id"] == 2

    audit = json.loads(
        (sparse_queue_tape["output_dir"] / "sequence_audit.json").read_text(
            encoding="utf-8"
        )
    )
    assert audit["strict_native_snapshot"] is True
    assert audit["delta_bootstrap_allowed"] is False
    assert audit["sequence_stats"]["sequence_gaps"] == 1
    assert audit["sequence_stats"]["ignored_before_snapshot"] == 1


def test_same_millisecond_exact_update_is_ambiguous_and_not_in_seed(
    sparse_queue_tape,
):
    seed = sparse_queue_tape["seeds"].loc["ambiguous"]
    events = sparse_queue_tape["events"]
    update = events.loc[
        (events["watch_id"] == "ambiguous")
        & (events["event_code"] == "update")
    ].iloc[0]

    assert seed["seed_status"] == "exact"
    assert seed["seed_qty"] == pytest.approx(5.0)
    assert bool(seed["ambiguous"]) is True
    assert update["qty_after"] == pytest.approx(4.0)
    assert bool(update["ambiguous"]) is True
    assert (
        update["exchange_ts_ms"]
        == sparse_queue_tape["day_start_ms"] + 2_000
    )


def test_builder_writes_required_summary_and_sparse_outputs(sparse_queue_tape):
    output_dir: Path = sparse_queue_tape["output_dir"]
    summary = sparse_queue_tape["summary"]

    assert (output_dir / "seeds.parquet").is_file()
    assert (output_dir / "level_events.parquet").is_file()
    assert (output_dir / "summary.json").is_file()
    assert (output_dir / "sequence_audit.json").is_file()
    assert summary["seed_status_counts"] == {
        "exact": 2,
        "known_zero": 1,
        "unknown": 2,
    }
    assert summary["seed_reason_counts"] == {
        "inside_snapshot_range_absent": 1,
        "outside_snapshot_range": 1,
        "sequence_unavailable": 1,
        "visible_quantity": 2,
    }
    assert summary["strict_seed_coverage"] == pytest.approx(3 / 5)
    assert summary["warmup_hours"] == 0
    assert summary["ambiguous_seed_count"] == 1
    assert summary["ambiguous_event_count"] == 1


def test_prior_day_snapshot_warmup_seeds_current_day_order(tmp_path: Path):
    day = "2026-01-02"
    previous_day = "2026-01-01"
    day_start_ms = int(
        datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000
    )
    raw_root = tmp_path / "raw"

    def write_hour(
        day_value: str,
        hour: str,
        rows: list[dict[str, object]],
    ) -> None:
        compressed = (
            raw_root
            / "binance_futures"
            / day_value
            / hour
            / "BTCUSDC_orderbook.parquet.zst"
        )
        compressed.parent.mkdir(parents=True, exist_ok=True)
        parquet = compressed.with_suffix("")
        pd.DataFrame(rows).to_parquet(parquet, index=False)
        with parquet.open("rb") as source, compressed.open("wb") as target:
            zstd.ZstdCompressor(level=1).copy_stream(source, target)
        parquet.unlink()

    def raw_row(
        timestamp_ms: int,
        *,
        event_type: str,
        side: str,
        price: float,
        quantity: float,
        first_update_id=None,
        final_update_id=None,
        prev_final_update_id=None,
        last_update_id=None,
    ) -> dict[str, object]:
        return {
            "event_time": timestamp_ms,
            "transaction_time": timestamp_ms,
            "received_time": timestamp_ms * 1_000_000,
            "event_type": event_type,
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "prev_final_update_id": prev_final_update_id,
            "last_update_id": last_update_id,
            "side": side,
            "price": price,
            "quantity": quantity,
        }

    snapshot_ts = day_start_ms - 500
    write_hour(
        previous_day,
        "23",
        [
            raw_row(
                snapshot_ts,
                event_type="snapshot",
                side="bid",
                price=100.0,
                quantity=5.0,
                last_update_id=10,
            ),
            raw_row(
                snapshot_ts,
                event_type="snapshot",
                side="ask",
                price=100.2,
                quantity=4.0,
                last_update_id=10,
            ),
        ],
    )
    update_ts = day_start_ms + 250
    write_hour(
        day,
        "00",
        [
            raw_row(
                update_ts,
                event_type="update",
                side="bid",
                price=100.0,
                quantity=3.0,
                first_update_id=11,
                final_update_id=11,
                prev_final_update_id=10,
            )
        ],
    )

    manifest = tmp_path / "watch.parquet"
    pd.DataFrame(
        [
            {
                "day": day,
                "watch_id": "warmup-order",
                "order_id": "warmup-order",
                "side": "BUY",
                "price": 100.0,
                "activate_ts_ms": day_start_ms + 500,
                "stop_ts_ms": day_start_ms + 900,
            }
        ]
    ).to_parquet(manifest, index=False)
    output_dir = tmp_path / "output"

    summary = build_active_order_queue_tape(
        watch_manifest=manifest,
        raw_root=raw_root,
        output_dir=output_dir,
        tick_size=0.1,
        warmup_hours=1,
        reuse_raw_only=True,
    )
    seed = pd.read_parquet(output_dir / "seeds.parquet").iloc[0]
    sequence = json.loads(
        (output_dir / "sequence_audit.json").read_text(encoding="utf-8")
    )

    assert seed["seed_status"] == "exact"
    assert seed["seed_reason"] == "visible_quantity"
    assert seed["seed_qty"] == pytest.approx(3.0)
    assert seed["seed_asof_ts_ms"] == update_ts
    assert summary["strict_seed_coverage"] == pytest.approx(1.0)
    assert summary["missing_warmup_hours"] == []
    assert sequence["segments"][0]["snapshot_ts_ms"] == snapshot_ts
