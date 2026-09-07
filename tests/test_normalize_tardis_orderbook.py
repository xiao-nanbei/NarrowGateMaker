from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard

from data.normalize_tardis_orderbook import (
    _freshness_union_coverage,
    _requested_days,
    audit_book_ticker,
    compare_normalized_sources,
    reconstruct_l2,
)


def test_requested_days_combines_csv_and_explicit_days(tmp_path: Path) -> None:
    manifest = tmp_path / "days.csv"
    manifest.write_text(
        "day,identity\n2025-08-02,technical\n2025-08-01,technical\n",
        encoding="utf-8",
    )
    assert _requested_days(["2025-08-03", "2025-08-02"], manifest) == [
        "2025-08-01",
        "2025-08-02",
        "2025-08-03",
    ]


def _write_zstd_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    path.write_bytes(zstandard.ZstdCompressor().compress(buffer.getvalue().encode()))


def _snapshot_rows(
    exchange_us: int,
    local_us: int,
    *,
    bid_quantity: float = 1.0,
) -> list[list[object]]:
    rows: list[list[object]] = []
    for side, prices in (
        ("ask", (101.0, 102.0)),
        ("bid", (100.0, 99.0)),
    ):
        for offset, price in enumerate(prices):
            quantity = bid_quantity if side == "bid" and offset == 0 else 1.0
            rows.append(
                [
                    "binance-futures",
                    "BTCUSDC",
                    exchange_us,
                    local_us,
                    True,
                    side,
                    price,
                    quantity,
                ]
            )
    return rows


def test_reconstruction_uses_causal_right_boundary_and_atomic_snapshot(tmp_path) -> None:
    start = 1_767_225_600_000_000
    raw = tmp_path / "l2.csv.zst"
    rows = _snapshot_rows(start + 40_000, start + 50_000)
    rows.extend(
        [
            [
                "binance-futures",
                "BTCUSDC",
                start + 110_000,
                start + 120_000,
                False,
                "bid",
                100.0,
                2.0,
            ],
            [
                "binance-futures",
                "BTCUSDC",
                start + 210_000,
                start + 230_000,
                False,
                "ask",
                101.0,
                3.0,
            ],
        ]
    )
    _write_zstd_csv(
        raw,
        [
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "is_snapshot",
            "side",
            "price",
            "amount",
        ],
        rows,
    )

    bbo, l2, quality = reconstruct_l2(
        raw,
        output_root=tmp_path / "normalized",
        day="2026-01-01",
        levels=2,
        pilot_duration_s=1,
    )

    bbo_table = pq.read_table(bbo).to_pydict()
    l2_table = pq.read_table(l2).to_pydict()
    clock = pq.read_table(
        tmp_path / "normalized/clock/BTCUSDC-clock-2026-01-01.parquet"
    ).to_pydict()
    assert bbo_table["timestamp"] == [1_767_225_600_100, 1_767_225_600_200, 1_767_225_600_300]
    assert l2_table["bid_qty_1"] == [1.0, 2.0, 2.0]
    assert l2_table["ask_qty_1"] == [1.0, 1.0, 3.0]
    assert clock["exchange_cut_timestamp_us"] == [
        start + 40_000,
        start + 110_000,
        start + 210_000,
    ]
    assert quality["snapshot_seen_at_start"] is True
    assert quality["causal_violations"] == 0
    assert quality["clock_source"] == "tardis_provider_local"
    assert quality["policy_visible"] is False
    assert quality["exact_queue_policy_eligible"] is False


def test_exchange_clock_removes_provider_transport_without_retiming_original_output(tmp_path):
    start = 1_767_225_600_000_000
    raw = tmp_path / "l2.csv.zst"
    header = ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"]
    rows = _snapshot_rows(start + 40_000, start + 250_000)
    rows.append(["binance-futures", "BTCUSDC", start + 110_000, start + 420_000,
                 False, "bid", 100., 2.])
    _write_zstd_csv(raw, header, rows)
    provider_root, exchange_root = tmp_path / "provider", tmp_path / "exchange"
    _, provider_l2, _ = reconstruct_l2(raw, output_root=provider_root, day="2026-01-01",
                                      levels=2, pilot_duration_s=1)
    before = provider_l2.read_bytes()
    _, exchange_l2, quality = reconstruct_l2(raw, output_root=exchange_root, day="2026-01-01",
                                            levels=2, pilot_duration_s=1, timestamp_source="exchange")
    provider = pq.read_table(provider_l2).to_pydict()
    exchange = pq.read_table(exchange_l2).to_pydict()
    assert provider["timestamp"] == [start // 1000 + 300, start // 1000 + 500]
    assert exchange["timestamp"] == [start // 1000 + 100, start // 1000 + 200]
    assert provider["bid_qty_1"] == exchange["bid_qty_1"] == [1., 2.]
    assert provider_l2.read_bytes() == before
    clock = pq.read_table(exchange_root / "clock/BTCUSDC-clock-2026-01-01.parquet").to_pydict()
    assert clock["exchange_resample_age_us"] == [60_000, 90_000]
    assert clock["last_provider_local_timestamp_us"] == [start + 250_000, start + 420_000]
    assert "provider_visibility_delay_us" not in clock
    assert quality["clock_source"] == "tardis_exchange"
    assert quality["dataset_id"] == "normalized_tardis_l2_exchange_100ms_v1"
    assert quality["exact_queue_policy_eligible"] is False
    # Exchange-clock mode must not reorder a regressing source.
    rows.append(["binance-futures", "BTCUSDC", start + 100_000, start + 430_000,
                 False, "bid", 100., 3.])
    _write_zstd_csv(raw, header, rows)
    with pytest.raises(ValueError, match="cannot reorder"):
        reconstruct_l2(raw, output_root=tmp_path / "bad", day="2026-01-01",
                       levels=2, pilot_duration_s=1, timestamp_source="exchange")


@pytest.mark.parametrize("force", [False, True])
def test_normalize_day_cannot_overwrite_other_clock_product(tmp_path, monkeypatch, force):
    import data.normalize_tardis_orderbook as normalizer
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    monkeypatch.setattr(normalizer, "_download_rows", lambda *args: {
        name: {"path": str(tmp_path / f"{name}.csv")}
        for name in ("incremental_book_L2", "book_ticker")
    })
    quality = tmp_path / "out/quality/BTCUSDC-2026-01-01.json"
    quality.parent.mkdir(parents=True)
    original = json.dumps({"clock_source": "tardis_provider_local"})
    quality.write_text(original)
    with pytest.raises(ValueError, match="separate output root"):
        normalizer.normalize_day(manifest, day="2026-01-01", output_root=tmp_path / "out",
                                 timestamp_source="exchange", force=force)
    assert quality.read_text() == original


@pytest.mark.parametrize("timestamp_source", ["provider", "exchange"])
def test_book_ticker_audit_is_strictly_causal_asof(tmp_path, timestamp_source) -> None:
    start = 1_767_225_600_000_000
    bbo_path = tmp_path / "bbo.parquet"
    pq.write_table(
        pa.table(
            {
                "timestamp": [start // 1_000 + 100, start // 1_000 + 200],
                "best_bid": [100.0, 100.0],
                "best_bid_qty": [1.0, 2.0],
                "best_ask": [101.0, 101.0],
                "best_ask_qty": [1.0, 1.0],
            }
        ),
        bbo_path,
    )
    ticker = tmp_path / "ticker.csv.zst"
    _write_zstd_csv(
        ticker,
        [
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "ask_amount",
            "ask_price",
            "bid_price",
            "bid_amount",
        ],
        [
            ["binance-futures", "BTCUSDC", start + 10_000, start + (320_000 if timestamp_source == "exchange" else 20_000), 1, 101, 100, 1],
            ["binance-futures", "BTCUSDC", start + 110_000, start + (420_000 if timestamp_source == "exchange" else 120_000), 1, 101, 100, 2],
            # This future row must not affect the 200 ms boundary.
            ["binance-futures", "BTCUSDC", start + 210_000, start + (520_000 if timestamp_source == "exchange" else 220_000), 1, 102, 99, 9],
        ],
    )

    audit = audit_book_ticker(
        ticker,
        bbo_path,
        day="2026-01-01",
        pilot_duration_s=1,
        timestamp_source=timestamp_source,
    )
    assert audit["book_ticker_rows_compared"] == 2
    assert audit["book_ticker_price_exact_ratio"] == 1.0
    assert audit["book_ticker_quantity_exact_ratio"] == 1.0
    assert audit["comparison_clock"] == timestamp_source


def test_freshness_coverage_is_distinct_from_bucket_density() -> None:
    coverage, p99 = _freshness_union_coverage(
        [100, 400, 700], start_ms=0, end_ms=1_000, freshness_ms=500
    )
    assert coverage == 0.9
    assert p99 == 300.0


def test_dual_source_diagnostic_cannot_upgrade_queue_identity(tmp_path) -> None:
    schema = {
        "timestamp": [100, 200, 300],
        "bid_px_1": [100.0, 100.0, 100.0],
        "bid_qty_1": [1.0, 2.0, 3.0],
        "ask_px_1": [101.0, 101.0, 101.0],
        "ask_qty_1": [1.0, 2.0, 3.0],
    }
    tardis = tmp_path / "tardis.parquet"
    crypto = tmp_path / "crypto.parquet"
    clock = tmp_path / "clock.parquet"
    pq.write_table(pa.table(schema), tardis)
    pq.write_table(pa.table(schema), crypto)
    pq.write_table(
        pa.table({"exchange_cut_timestamp_us": [100_000, 200_000, 300_000]}),
        clock,
    )
    result = compare_normalized_sources(
        tardis,
        crypto,
        tardis_clock=clock,
        levels=1,
        stride=1,
    )
    nearest = result["clock_agnostic_nearest"]
    assert nearest["top20_price_exact_ratio"] == 1.0
    assert nearest["top20_quantity_exact_ratio"] == 1.0
    assert nearest["is_causality_proof"] is False
    causal = result["exchange_time_causal_asof"]
    assert causal["top20_price_exact_ratio"] == 1.0
    assert causal["future_crypto_rows_forbidden"] is True
    assert result["cannot_upgrade_native_sequence_or_exact_queue"] is True
