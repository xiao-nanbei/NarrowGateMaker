from __future__ import annotations

import csv
import io
import json
import pickle
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard
import data.normalize_tardis_orderbook as normalizer

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


def test_cli_defaults_to_exchange_clock():
    args = normalizer._parser().parse_args(["--manifest", "input.json", "--day", "2026-01-01"])
    assert args.timestamp_source == "exchange"


def test_daily_parquet_reader_casts_core_without_losing_native_fields(tmp_path):
    path = tmp_path / "incremental_book_L2.parquet"
    pq.write_table(pa.table({"timestamp": [1000], "local_timestamp": [1010],
                            "is_snapshot": [True], "price": ["100.1"], "amount": ["2.3"],
                            "last_update_id": pa.array([None], type=pa.int64())}), path)
    with normalizer._open_csv(path) as reader:
        batch = next(reader)
        assert batch.column("price").to_pylist() == [100.1]
        assert batch.column("amount").to_pylist() == [2.3]
        assert batch.column("last_update_id").to_pylist() == [None]


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
    assert quality["clock_source"] == "tardis_exchange"
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
                                      levels=2, pilot_duration_s=1, timestamp_source="provider")
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


L2_HEADER = ["exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"]


def _raw_day(tmp_path, day="2026-01-01", *, offset_us=50_000, snapshot=True):
    start = normalizer._day_start_us(day)
    rows = [["binance-futures", "BTCUSDC", start + offset_us - 1000,
             start + offset_us, snapshot, side, price, 1.]
            for side, prices in (("bid", range(100, 80, -1)), ("ask", range(101, 121)))
            for price in prices]
    raw = tmp_path / f"{day}.csv.zst"
    _write_zstd_csv(raw, L2_HEADER, rows)
    return raw, rows


def _manifest(tmp_path, raw, day="2026-01-01", *, ticker=False):
    rows = [{"day": day, "dataset": normalizer.INCREMENTAL_L2, "path": str(raw),
             "sha256": normalizer._sha256(raw), "size_bytes": raw.stat().st_size}]
    if ticker:
        rows.append({"day": day, "dataset": normalizer.BOOK_TICKER,
                     "path": str(tmp_path / "absent.csv"), "sha256": "absent", "size_bytes": 0})
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"complete": False, "downloads": rows}))
    return path


@pytest.mark.parametrize("ticker", [False, True])
def test_incomplete_batch_with_readable_l2_does_not_require_auxiliary(tmp_path, ticker):
    raw, _ = _raw_day(tmp_path)
    manifest = _manifest(tmp_path, raw, ticker=ticker)
    q = normalizer.normalize_day(manifest, day="2026-01-01", output_root=tmp_path / "out", pilot_duration_s=1)
    assert q["emitted_rows"] == 1
    assert q["book_ticker_audit"]["status"] == "unavailable"
    assert not q["cross_channel_contract_valid"]
    assert not q["provider_normalized_replay_candidate"]


def test_duplicate_source_and_missing_primary_are_explicit(tmp_path):
    raw, _ = _raw_day(tmp_path)
    manifest = _manifest(tmp_path, raw)
    p = json.loads(manifest.read_text())
    p["downloads"] *= 2
    manifest.write_text(json.dumps(p))
    with pytest.raises(ValueError, match="duplicate source"):
        normalizer._download_rows(manifest, "2026-01-01")
    p["downloads"] = []
    manifest.write_text(json.dumps(p))
    with pytest.raises(RuntimeError, match="missing Tardis"):
        normalizer._download_rows(manifest, "2026-01-01")


@pytest.mark.parametrize("clock", ["provider", "exchange"])
def test_carried_view_preserves_real_observation_age_and_causality(tmp_path, clock):
    raw, rows = _raw_day(tmp_path)
    start = normalizer._day_start_us("2026-01-01")
    rows.append(["binance-futures", "BTCUSDC", start + 549_000, start + 550_000,
                 False, "bid", 100., 2.])
    _write_zstd_csv(raw, L2_HEADER, rows)
    args = dict(day="2026-01-01", output_start_us=start, output_end_us=start + 1_000_000,
                timestamp_source=clock)
    _, _, sparse = reconstruct_l2(raw, output_root=tmp_path / "sparse", **args)
    _, l2, q = reconstruct_l2(raw, output_root=tmp_path / "carry", gap_policy="carry_forward", **args)
    c = pq.read_table(tmp_path / "carry/clock/BTCUSDC-clock-2026-01-01.parquet").to_pydict()
    assert c["observation_kind"][:6] == ["source_observed"] + ["carried_forward"] * 4 + ["source_observed"]
    assert len(set(c["last_observation_timestamp_us"][:5])) == 1
    assert c["observation_age_us"][1:5] == [c["observation_age_us"][0] + i * 100000 for i in range(1, 5)]
    assert c["observation_age_us"][5] == c["observation_age_us"][0]
    assert all(t * 1000 > o for t, o in zip(c["timestamp"], c["last_observation_timestamp_us"], strict=True))
    assert pq.read_table(l2)["bid_qty_1"].to_pylist() == [1.] * 5 + [2.] * 4
    assert c["update_coverage"][1] == "unknown"
    assert q["unobserved_intervals"] == sparse["unobserved_intervals"]
    assert q["freshness_union_coverage"] == sparse["freshness_union_coverage"]
    assert q["carried_forward_rows"] == 7
    assert not q["normalized_replay_candidate_before_cross_channel"]
    assert q["dataset_id"].endswith("_carried_view_v2")
    assert q["bucket_density"] == 1.
    assert not any("volume" in name or "trade_count" in name for name in pq.read_schema(l2).names)


def test_adjacent_days_inherit_old_observation_without_inventing_update(tmp_path):
    state = normalizer.L2Continuation()
    start = normalizer._day_start_us("2026-01-01")
    raw, _ = _raw_day(tmp_path, offset_us=normalizer.DAY_US - 150_000)
    reconstruct_l2(raw, output_root=tmp_path / "out", day="2026-01-01", continuation=state,
                   output_start_us=start + normalizer.DAY_US - 300_000, gap_policy="carry_forward")
    previous = state.last_exchange_us
    raw2, _ = _raw_day(tmp_path, "2026-01-02", offset_us=250_000, snapshot=False)
    _, _, q = reconstruct_l2(raw2, output_root=tmp_path / "out", day="2026-01-02", continuation=state,
                            pilot_duration_s=1, gap_policy="carry_forward")
    c = pq.read_table(tmp_path / "out/clock/BTCUSDC-clock-2026-01-02.parquet").to_pydict()
    assert q["continuation_inherited"] and not q["snapshot_seen_at_start"]
    assert c["last_observation_timestamp_us"][:3] == [previous] * 3
    assert c["observation_age_us"][:3] == [151_000, 251_000, 351_000]
    assert c["observation_kind"][:3] == ["carried_forward"] * 3
    assert state.last_exchange_us > previous


@pytest.mark.parametrize("change", [{"symbol": "BTCUSDT"}, {"timestamp_source": "provider"},
                                    {"next_day_start_us": 0}])
def test_incompatible_continuation_is_rejected_without_mutation(tmp_path, change):
    raw, _ = _raw_day(tmp_path)
    state = normalizer.L2Continuation(initialized=True,
                                     next_day_start_us=normalizer._day_start_us("2026-01-01"))
    state.__dict__.update(change)
    before = pickle.dumps(state)
    with pytest.raises(ValueError, match="adjacent days"):
        reconstruct_l2(raw, day="2026-01-01", output_root=tmp_path / "out", continuation=state)
    assert pickle.dumps(state) == before


def test_failure_after_reconstruction_does_not_publish_or_advance_state(tmp_path, monkeypatch):
    raw, _ = _raw_day(tmp_path)
    manifest = _manifest(tmp_path, raw)
    state = normalizer.L2Continuation()
    args = dict(day="2026-01-01", output_root=tmp_path / "out", continuation=state, pilot_duration_s=1)
    before = pickle.dumps(state)
    monkeypatch.setattr(normalizer, "_atomic_json", lambda *a: (_ for _ in ()).throw(OSError("disk failure")))
    with pytest.raises(OSError, match="disk failure"):
        normalizer.normalize_day(manifest, **args)
    assert pickle.dumps(state) == before
    assert not list((tmp_path / "out").rglob("*.parquet"))


def test_publication_failure_rolls_back_existing_files(tmp_path, monkeypatch):
    old = tmp_path / "old"
    old.write_bytes(b"old")
    new = tmp_path / "new"
    new.write_bytes(b"new")
    with pytest.raises(FileNotFoundError):
        normalizer._publish_files([(new, old), (tmp_path / "absent", tmp_path / "second")])
    assert old.read_bytes() == b"old"
    assert not (tmp_path / "second").exists()


@pytest.mark.parametrize("clock", ["provider", "exchange"])
def test_read_failure_after_updates_preserves_previous_success_state(tmp_path, clock):
    raw, _ = _raw_day(tmp_path, offset_us=normalizer.DAY_US - 150000)
    state = normalizer.L2Continuation()
    reconstruct_l2(raw, output_root=tmp_path / "out", day="2026-01-01", continuation=state,
                   timestamp_source=clock)
    before = pickle.dumps(state)
    raw2, rows = _raw_day(tmp_path, "2026-01-02", snapshot=False)
    start = normalizer._day_start_us("2026-01-02")
    rows.append(["binance-futures", "BTCUSDC", start + 1000, start + 2000,
                 False, "bid", 100., 42.])
    _write_zstd_csv(raw2, L2_HEADER, rows)
    with pytest.raises(ValueError, match="cannot reorder"):
        reconstruct_l2(raw2, output_root=tmp_path / "out", day="2026-01-02", continuation=state,
                       timestamp_source=clock, pilot_duration_s=1)
    assert pickle.dumps(state) == before
    assert not list((tmp_path / "out").rglob("*2026-01-02.parquet"))


def test_window_keeps_preroll_observation_and_empty_primary_fails(tmp_path):
    raw, _ = _raw_day(tmp_path)
    start = normalizer._day_start_us("2026-01-01")
    _, _, q = reconstruct_l2(raw, output_root=tmp_path / "out", day="2026-01-01",
                            output_start_us=start+500000, output_end_us=start+1000000,
                            gap_policy="carry_forward")
    assert q["emitted_rows"] == q["possible_rows"] == 5
    assert q["unobserved_intervals"][0]["last_observation_us"] == start+49000
    raw, _ = _raw_day(tmp_path, snapshot=False)
    with pytest.raises(ValueError, match="no snapshot"):
        reconstruct_l2(raw, output_root=tmp_path / "bad", day="2026-01-01", pilot_duration_s=1)


def test_failed_replacement_preserves_all_previous_output_hashes(tmp_path, monkeypatch):
    raw, _ = _raw_day(tmp_path)
    manifest = _manifest(tmp_path, raw)
    args = dict(day="2026-01-01", output_root=tmp_path / "out", pilot_duration_s=1)
    normalizer.normalize_day(manifest, **args)
    before = {p: normalizer._sha256(p) for p in (tmp_path / "out").rglob("*") if p.is_file()}
    monkeypatch.setattr(normalizer, "_atomic_json", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        normalizer.normalize_day(manifest, force=True, **args)
    assert before == {p: normalizer._sha256(p) for p in (tmp_path / "out").rglob("*") if p.is_file()}


def test_cache_separates_continuous_and_carried_modes_and_schema(tmp_path):
    raw, _ = _raw_day(tmp_path)
    manifest = _manifest(tmp_path, raw)
    args = dict(day="2026-01-01", output_root=tmp_path / "out", pilot_duration_s=1)
    normalizer.normalize_day(manifest, **args)
    assert normalizer.normalize_day(manifest, **args)["resume_status"] == "validated_existing"
    with pytest.raises(ValueError, match="continuation changed"):
        normalizer.normalize_day(manifest, continuation=normalizer.L2Continuation(), **args)
    with pytest.raises(ValueError, match="gap policy changed"):
        normalizer.normalize_day(manifest, gap_policy="carry_forward", **args)
    p = tmp_path / "out/quality/BTCUSDC-2026-01-01.json"
    q = json.loads(p.read_text()); q.pop("observation_schema")
    p.write_text(json.dumps(q))
    assert normalizer.normalize_day(manifest, **args)["resume_status"] == "rebuilt"


@pytest.mark.parametrize("free_bytes", [0, 1024**4])
def test_continuous_cli_stops_at_failed_day(tmp_path, monkeypatch, capsys, free_bytes):
    manifest = tmp_path / "manifest.json"; manifest.write_text("{}")
    calls = []
    def fail(payload):
        calls.append(payload["day"])
        return {"day": payload["day"], "ok": False, "error": "unreadable primary"}
    monkeypatch.setattr(normalizer, "_normalize_day_safe", fail)
    # This synthetic CLI test must not depend on the runner's actual disk capacity.
    monkeypatch.setattr(normalizer.os, "statvfs", lambda _path: SimpleNamespace(
        f_bavail=free_bytes, f_frsize=1,
    ))
    args = ["--manifest", str(manifest), "--output-root", str(tmp_path / "out"),
            "--day", "2026-01-01", "--day", "2026-01-02", "--continuous"]
    if not free_bytes:
        with pytest.raises(SystemExit, match="storage safety gate failed"):
            normalizer.main(args)
        assert calls == []
        return
    assert normalizer.main(args) == 1
    out = json.loads(capsys.readouterr().out)
    assert calls == ["2026-01-01"]
    assert out["not_run_days"] == ["2026-01-02"]


def _fusion_input(tmp_path, name, records, *, native=False):
    """Synthetic original source rows: offset-us/snapshot/side/price/qty/ids."""
    rows = []
    base = normalizer._day_start_us("2026-01-01")
    for offset, snapshot, side, price, qty, final, previous in records:
        timestamp = base + offset
        rows.append({"exchange": "binance-futures", "symbol": "BTCUSDC",
                     "timestamp": timestamp, "local_timestamp": timestamp + 100,
                     "is_snapshot": snapshot, "side": side, "price": str(price), "amount": str(qty),
                     "event_time": timestamp // 1000 if native else None,
                     "transaction_time": timestamp // 1000 if native else None,
                     "first_update_id": final, "final_update_id": final,
                     "prev_final_update_id": previous,
                     "last_update_id": final if snapshot else None})
    path = tmp_path / f"{name}.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=2)
    return path


def _fusion_snapshot(offset, *, bid=100, qty=1, final=None):
    return [(offset, True, side, price, qty, final, None)
            for side, price in (("bid", bid), ("bid", bid-1), ("ask", 102), ("ask", 103))]


def _fusion_run(sources, **kwargs):
    batches, stats = normalizer.iter_fused_book_batches(sources, "2026-01-01", minimum_levels=2, **kwargs)
    return pa.Table.from_batches(list(batches)), stats


def test_fusion_independent_snapshot_switch_and_tardis_tie(tmp_path):
    a = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000) + [(300_000, False, "bid", 100, 3, None, None)])
    b = _fusion_input(tmp_path, "b", _fusion_snapshot(100_000, bid=101, qty=2, final=100)
                      + [(200_000, False, "bid", 101, 4, 101, 100)], native=True)
    table, stats = _fusion_run({"tardis": a, "cryptohft": b}, batch_rows=2)
    state = {}
    last_message = None
    states = {}
    for row in table.to_pylist():
        key = (row["timestamp"], row["local_timestamp"], row["is_snapshot"])
        if key != last_message and row["is_snapshot"]:
            state.clear()
        last_message = key
        level = (row["side"], row["price"])
        if float(row["amount"]) == 0:
            state.pop(level, None)
        else:
            state[level] = row["amount"]
        states[row["timestamp"]] = dict(state)
    base = normalizer._day_start_us("2026-01-01")
    assert ("bid", "101.00000000") not in states[base + 100_000]
    assert states[base + 200_000][("bid", "101.00000000")] == "4.00000000"
    assert ("bid", "101.00000000") not in states[base + 300_000]
    assert states[base + 300_000][("bid", "100.00000000")] == "3.00000000"
    assert stats["source_switches"] == 2
    assert stats["equal_clock_conflicting_states"] == 1
    assert stats["future_fill_violations"] == 0


def test_observed_union_does_not_materialize_source_coverage_deletions(tmp_path):
    a_rows = _fusion_snapshot(100_000) + [(100_000, True, "bid", 90, 5, None, None)]
    a_rows += [(300_000, False, "bid", 100, 1, None, None)]
    b_rows = _fusion_snapshot(200_000, final=100) + [(400_000, False, "bid", 100, 1, 101, 100)]
    a = _fusion_input(tmp_path, "deep", a_rows)
    b = _fusion_input(tmp_path, "shallow", b_rows, native=True)
    actual, stats = _fusion_run({"tardis": a, "cryptohft": b}, observed_union=True,
                                output_end_us=normalizer._day_start_us("2026-01-01") + 500_000,
                                normalized_root=tmp_path / "normalized", batch_rows=2)
    assert len(actual) == len(a_rows) + len(b_rows)
    assert set(actual["fusion_reason"].to_pylist()) == {"source_observation"}
    assert all(float(v) > 0 for v in actual["amount"].to_pylist())
    deep = [r for r in actual.to_pylist() if float(r["price"]) == 90]
    assert len(deep) == 1 and deep[0]["source_id"] == "tardis"
    assert stats["raw_state_differences_materialized"] is False
    assert stats["source_switches"] >= 2
    assert stats["future_fill_violations"] == 0
    assert all(r["source_observed_timestamp_us"] <= r["timestamp"] for r in actual.to_pylist())


def test_observed_union_preserves_original_fields_and_both_same_clock_sources(tmp_path):
    a = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000, qty="1.2300"))
    b = _fusion_input(tmp_path, "b", _fusion_snapshot(100_000, qty="2.3400", final=100), native=True)
    original = pq.read_table(b)
    original = original.set_column(original.schema.get_field_index("timestamp"), "timestamp",
                                   pa.array([v - 17_000 for v in original["timestamp"].to_pylist()]))
    pq.write_table(original, b)
    actual, _ = _fusion_run({"tardis": a, "cryptohft": b}, observed_union=True, batch_rows=1)
    assert len(actual) == 8
    for source, path in (("tardis", a), ("cryptohft", b)):
        rows = [r for r in actual.to_pylist() if r["source_id"] == source]
        expected = pq.read_table(path).to_pylist()
        for row, old in zip(rows, expected, strict=True):
            assert row["source_timestamp_us"] == old["timestamp"]
            for field in old:
                if field != "timestamp":
                    assert row[field] == old[field]
            assert row["source_native_sequence"] == (source == "cryptohft")


def test_observed_union_never_treats_legacy_view_as_original_source(tmp_path):
    source = _fusion_input(tmp_path, "legacy", _fusion_snapshot(100_000))
    raw = pq.read_table(source).replace_schema_metadata({b"narrowgate.book_fusion": b"reconstructed_fusion.v1"})
    pq.write_table(raw, source)
    with pytest.raises(ValueError, match="reconstructed view"):
        _fusion_run({"canonical": source}, observed_union=True)


def _write_observed_union(tmp_path, name, table):
    path = tmp_path / f"{name}.parquet"
    pq.write_table(table.replace_schema_metadata({b"narrowgate.book_fusion": b"observed_union.v1"}),
                   path, row_group_size=2)
    return path


@pytest.mark.parametrize(("field", "value", "error"), [
    ("source_id", None, "source_id must be present and non-null"),
    ("source_id", "unregistered", "undeclared source_id"),
    ("source_native_sequence", None, "source_native_sequence must be present and non-null"),
    ("source_native_sequence", True, "source sequence identity differs"),
])
def test_observed_union_rejects_invalid_identity_before_source_filter(tmp_path, field, value, error):
    source = _fusion_input(tmp_path, "original", _fusion_snapshot(100_000))
    table, _ = _fusion_run({"tardis": source}, observed_union=True)
    values = table[field].to_pylist()
    values[0] = value
    table = table.set_column(table.schema.get_field_index(field), field,
                             pa.array(values, type=table.schema.field(field).type))
    union = _write_observed_union(tmp_path, "invalid_union", table)
    with pytest.raises(ValueError, match=error):
        _fusion_run({"canonical": union}, observed_union=True)


def test_observed_union_rejects_same_source_refresh_instead_of_moving_early_rows(tmp_path):
    source = _fusion_input(tmp_path, "old", _fusion_snapshot(100_000)
                           + [(1_000_000, False, "bid", 100, 2, None, None)])
    table, _ = _fusion_run({"tardis": source}, observed_union=True)
    union = _write_observed_union(tmp_path, "old_union", table)
    incoming = _fusion_input(tmp_path, "incoming", _fusion_snapshot(200_000))
    with pytest.raises(ValueError, match="multiple files for one source.*identity merge"):
        _fusion_run({"canonical": union, "tardis": incoming}, observed_union=True)
    with pytest.raises(ValueError, match="multiple files for one source.*identity merge"):
        _fusion_run([{"source_id": "tardis", "paths": [source, incoming],
                      "native_sequence": False}], observed_union=True)


def test_observed_union_reread_preserves_native_canonical_slot_identity(tmp_path):
    native = _fusion_input(tmp_path, "native", _fusion_snapshot(100_000, final=100), native=True)
    table, _ = _fusion_run({"canonical": native}, observed_union=True)
    assert set(table["source_id"].to_pylist()) == {"canonical"}
    assert all(table["source_native_sequence"].to_pylist())
    union = _write_observed_union(tmp_path, "native_union", table)
    reread, stats = _fusion_run({"canonical": union}, observed_union=True, batch_rows=1)
    assert reread.equals(table)
    assert stats["accepted_messages"] == 1


def test_observed_union_lookahead_splits_sources_and_uses_original_exchange_prefix(tmp_path):
    base = normalizer._day_start_us("2026-01-01")
    end = base + normalizer.DAY_US
    current = _fusion_input(tmp_path, "current", _fusion_snapshot(100_000))
    a = _fusion_input(tmp_path, "tail_tardis", _fusion_snapshot(normalizer.DAY_US - 100_000))
    b = _fusion_input(tmp_path, "tail_crypto", _fusion_snapshot(normalizer.DAY_US - 100_000, final=100), native=True)
    tail, _ = _fusion_run({"tardis": a, "cryptohft": b}, observed_union=True)
    # The next union has a later publication floor, but the preserved source
    # observations are before this UTC day's end. Do not select row groups by
    # their raised timestamp, and do not discard the other provider's rows.
    raised = tail.set_column(tail.schema.get_field_index("timestamp"), "timestamp",
                              pa.array([end + 100_000] * len(tail)))
    future = []
    for row in tail.to_pylist():
        row.update(timestamp=end + 200_000, source_timestamp_us=end + 200_000,
                   source_observed_timestamp_us=end + 200_000, local_timestamp=end + 200_100)
        if row["source_native_sequence"]:
            row.update(event_time=(end + 200_000) // 1000,
                       transaction_time=(end + 200_000) // 1000)
        future.append(row)
    next_union = _write_observed_union(tmp_path, "next_union", pa.concat_tables([
        raised, pa.Table.from_pylist(future, schema=tail.schema)]))
    actual, stats = _fusion_run({"tardis": current}, observed_union=True,
                                next_sources={"canonical": next_union}, batch_rows=1)
    assert len(actual) == 4 + len(tail)
    observed_tail = actual.filter(pa.compute.equal(actual["source_observed_timestamp_us"], end - 100_000))
    assert len(observed_tail) == 8
    assert set(observed_tail["source_id"].to_pylist()) == {"cryptohft", "tardis"}
    assert set(observed_tail["timestamp"].to_pylist()) == {end - 100_000}
    assert max(actual["source_observed_timestamp_us"].to_pylist()) < end
    assert stats["future_fill_violations"] == 0
    with pytest.raises(ValueError, match="multiple files for one source.*identity merge"):
        _fusion_run({"tardis": current}, observed_union=True,
                    next_sources={"canonical": next_union, "tardis": a})


def test_fusion_bounded_native_push_preserves_switches_clocks_and_checkpoint(tmp_path, monkeypatch):
    # Source switches expand into multi-level absolute deltas. Splitting inside
    # snapshots/messages must not publish partial books or change causality.
    a = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000)
                      + [(offset, False, "bid", 100, 2 + index, None, None)
                         for index, offset in enumerate(range(300_000, 800_000, 200_000))])
    b = _fusion_input(tmp_path, "b", _fusion_snapshot(200_000, qty=7, final=100)
                      + [(offset, False, "bid", 100, 8 + index, 101 + index, 100 + index)
                         for index, offset in enumerate(range(400_000, 900_000, 200_000))], native=True)
    sources = {"tardis": a, "cryptohft": b}
    monkeypatch.setattr(normalizer, "FUSION_PUSH_ROWS", 100_000)
    large, expected = _fusion_run(sources, batch_rows=100, normalized_root=tmp_path / "large")
    monkeypatch.setattr(normalizer, "FUSION_PUSH_ROWS", 1)
    small, actual = _fusion_run(sources, batch_rows=100, normalized_root=tmp_path / "small")
    assert small.equals(large)
    assert actual["source_switches"] >= 5
    assert {key: value for key, value in actual.items() if key not in {"normalized", "write_pipeline"}} == {
        key: value for key, value in expected.items() if key not in {"normalized", "write_pipeline"}}
    for kind in ("bbo", "l2", "clock"):
        assert pq.read_table(actual["normalized"][kind]["path"]).equals(
            pq.read_table(expected["normalized"][kind]["path"]))


def test_fusion_old_native_canonical_uses_actual_event_clock_not_transaction_alias(tmp_path):
    source = _fusion_input(tmp_path, "old_native", _fusion_snapshot(100_000, final=100), native=True)
    table = pq.read_table(source)
    table = table.set_column(table.schema.get_field_index("timestamp"), "timestamp",
                             pa.array([value - 50_000 for value in table["timestamp"].to_pylist()]))
    pq.write_table(table, source)
    result, stats = _fusion_run({"canonical": source})
    expected = normalizer._day_start_us("2026-01-01") + 100_000
    assert set(result["timestamp"].to_pylist()) == {expected}
    assert set(result["source_observed_timestamp_us"].to_pylist()) == {expected}
    assert stats["future_fill_violations"] == 0


def test_fusion_native_interleaved_snapshot_and_reversed_E_no_rewind(tmp_path):
    # The real May24 shape: snapshot/u100 and stale update/u100 fragments
    # interleave while the snapshot's E is later than the update's E.
    snapshot = _fusion_snapshot(200_000, final=100)
    rows = [snapshot[0], (100_000, False, "bid", 100, 99, 100, 90),
            snapshot[1], snapshot[2], (100_000, False, "ask", 102, 99, 100, 90), snapshot[3],
            (150_000, False, "bid", 100, 2, 101, 100),
            (300_000, False, "bid", 100, 3, 102, 101)]
    source = _fusion_input(tmp_path, "native", rows, native=True)
    small, stats = _fusion_run({"cryptohft": source}, batch_rows=1)
    large, _ = _fusion_run({"cryptohft": source}, batch_rows=100)
    assert small.equals(large)
    first = small.filter(pa.compute.equal(small["timestamp"], normalizer._day_start_us("2026-01-01") + 200_000))
    assert set(first["side"].to_pylist()) == {"bid", "ask"}
    assert "99.00000000" not in first["amount"].to_pylist()
    assert stats["duplicate_or_stale_messages"] == 1
    assert stats["source_stats"]["cryptohft"]["presentation_delayed_rows"] == 3


def test_fusion_gap_older_source_is_not_rewound_or_refreshed(tmp_path):
    a = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000))
    b = _fusion_input(tmp_path, "b", _fusion_snapshot(200_000, qty=2, final=100)
                      + [(300_000, False, "bid", 100, 3, 105, 104)], native=True)
    table, stats = _fusion_run({"tardis": a, "cryptohft": b})
    assert stats["sequence_gaps"] == 1
    assert stats["older_fallback_suppressed"] == 1
    assert max(table["timestamp"].to_pylist()) == normalizer._day_start_us("2026-01-01") + 200_000


def test_fusion_crossday_checkpoint_and_deferred_rows(tmp_path):
    end = normalizer.DAY_US
    source = _fusion_input(tmp_path, "a", _fusion_snapshot(end - 200_000, final=100)
                           + [(end + 100_000, False, "bid", 100, 4, 101, 100)], native=True)
    first, stats = _fusion_run({"cryptohft": source}, batch_rows=2)
    assert stats["deferred_rows_by_source"]["cryptohft"] == 1
    batches, following = normalizer.iter_fused_book_batches({}, "2026-01-02", minimum_levels=2,
                                                          previous_state=stats["continuation"])
    second = pa.Table.from_batches(list(batches))
    base = normalizer._day_start_us("2026-01-02")
    opening = second.filter(pa.compute.equal(second["timestamp"], base))
    assert len(opening) == 4
    assert set(opening["fusion_reason"].to_pylist()) == {"carried_opening_snapshot"}
    assert max(opening["source_observed_timestamp_us"].to_pylist()) == base - 200_000
    assert set(opening["event_time"].to_pylist()) == {(base - 200_000) // 1000}
    assert set(opening["transaction_time"].to_pylist()) == {(base - 200_000) // 1000}
    assert set(opening["final_update_id"].to_pylist()) == {100}
    assert second[-1:]["amount"].to_pylist() == ["4.00000000"]
    assert following["sequence_gaps"] == 0
    assert max(first["timestamp"].to_pylist()) < base


def test_fusion_new_native_source_repeated_lookahead_resumes_global_frontier(tmp_path):
    end = normalizer.DAY_US
    tardis = _fusion_input(tmp_path, "tardis", _fusion_snapshot(end - 300_000)
                           + [(end - 50_000, False, "bid", 100, 2, None, None)])
    native = _fusion_input(tmp_path, "next_native",
        [(end - 100_000, False, "bid", 100, 9, 101, 100)]
        + _fusion_snapshot(end + 100_000, qty=3, final=200)
        + [(end + 200_000, False, "bid", 100, 4, 201, 200)], native=True)
    _, first = _fusion_run({"tardis": tardis}, next_sources={"cryptohft": native})
    state = first["continuation"]
    base = normalizer._day_start_us("2026-01-02")
    assert state["kernel"]["presentation_us"] == base - 50_000
    assert state["kernel"]["sources"][0]["presentation_us"] == -1
    assert first["pre_snapshot_messages"] == 1
    batches, stats = normalizer.iter_fused_book_batches(
        {"cryptohft": native}, "2026-01-02", minimum_levels=2, previous_state=state,
        normalized_root=tmp_path / "normalized", output_end_us=base + 400_000, batch_rows=1)
    rows = pa.Table.from_batches(list(batches))
    opening = rows.filter(pa.compute.equal(rows["timestamp"], base))
    assert set(opening["source_observed_timestamp_us"].to_pylist()) == {base - 50_000}
    assert "9.00000000" not in rows["amount"].to_pylist()
    assert stats["pre_snapshot_messages"] == 1
    assert stats["sequence_gaps"] == 0
    clock = pq.read_table(tmp_path / "normalized/clock/BTCUSDC-clock-2026-01-02.parquet").to_pydict()
    assert clock["last_observation_timestamp_us"] == [base - 50_000, base - 50_000,
                                                       base + 100_000, base + 200_000]
    assert clock["observation_age_us"] == [50_000, 150_000, 100_000, 100_000]


def test_fusion_existing_native_repeat_uses_sequence_not_global_clock_rewind(tmp_path):
    end = normalizer.DAY_US
    native_day = _fusion_input(tmp_path, "native_day", _fusion_snapshot(end - 300_000, final=100), native=True)
    next_native = _fusion_input(tmp_path, "next_native",
        [(end - 100_000, False, "bid", 100, 2, 101, 100),
         (end + 100_000, False, "bid", 100, 3, 102, 101)], native=True)
    tardis = _fusion_input(tmp_path, "tardis", _fusion_snapshot(end - 50_000, qty=4))
    _, first = _fusion_run({"cryptohft": native_day, "tardis": tardis},
                           next_sources={"cryptohft": next_native})
    base = normalizer._day_start_us("2026-01-02")
    assert first["continuation"]["kernel"]["sources"][0]["presentation_us"] == base - 100_000
    batches, stats = normalizer.iter_fused_book_batches(
        {"cryptohft": next_native}, "2026-01-02", minimum_levels=2,
        previous_state=first["continuation"], output_end_us=base + 300_000)
    rows = pa.Table.from_batches(list(batches))
    assert stats["duplicate_or_stale_messages"] == 1
    assert stats["sequence_gaps"] == 0
    assert rows[-1:]["source_observed_timestamp_us"].to_pylist() == [base + 100_000]
    changed_bid = rows.filter(pa.compute.and_(pa.compute.equal(rows["price"], "100.00000000"),
                                              pa.compute.equal(rows["timestamp"], base + 100_000)))
    assert changed_bid["amount"].to_pylist() == ["3.00000000"]


def test_fusion_deferred_prefix_is_floored_without_mutating_checkpoint_or_real_E(tmp_path):
    end = normalizer.DAY_US
    tardis = _fusion_input(tmp_path, "tardis", _fusion_snapshot(end - 50_000))
    _, first = _fusion_run({"tardis": tardis})
    state = first["continuation"]
    base = normalizer._day_start_us("2026-01-02")
    # Recovery/deferred input can itself precede another source's checkpoint.
    # Keep its real E and unknown pre-snapshot state, never refresh the book.
    state["deferred_rows_by_source"]["cryptohft"] = [
        [base - 100_000, base - 100_000, base - 90_000, 0, 1, 101, 101, 100, -1,
         (base - 100_000) // 1000, 0, 10_000_000_000, 100_000_000, 0]]
    frozen = json.dumps(state, sort_keys=True)
    batches, stats = normalizer.iter_fused_book_batches(
        {}, "2026-01-02", minimum_levels=2, previous_state=state,
        normalized_root=tmp_path / "normalized", output_end_us=base + 200_000)
    rows = pa.Table.from_batches(list(batches))
    assert json.dumps(state, sort_keys=True) == frozen
    assert stats["pre_snapshot_messages"] == 1
    assert set(rows["source_observed_timestamp_us"].to_pylist()) == {base - 50_000}
    clock = pq.read_table(tmp_path / "normalized/clock/BTCUSDC-clock-2026-01-02.parquet").to_pydict()
    assert clock["observation_age_us"] == [50_000, 150_000]


def test_fusion_next_capture_day_prefix_and_strict_sampling(tmp_path):
    base = normalizer._day_start_us("2026-01-01")
    source = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000, final=100))
    # This path represents an adjacent capture partition, but its real E is
    # still in the target day. Future-E rows must not enter that target state.
    next_source = _fusion_input(tmp_path, "next", [(200_000, False, "bid", 100, 5, 101, 100),
                              (normalizer.DAY_US + 10_000, False, "bid", 100, 9, 102, 101)], native=True)
    # Native identity on the opening source is also required.
    source = _fusion_input(tmp_path, "a_native", _fusion_snapshot(100_000, final=100), native=True)
    table, stats = _fusion_run({"cryptohft": source}, next_sources={"cryptohft": next_source},
                              normalized_root=tmp_path / "out", output_end_us=base + 600_000)
    assert max(table["timestamp"].to_pylist()) == base + 200_000
    assert "9.00000000" not in table["amount"].to_pylist()
    bbo = pq.read_table(tmp_path / "out/bbo/BTCUSDC-bbo-2026-01-01.parquet").to_pydict()
    clock = pq.read_table(tmp_path / "out/clock/BTCUSDC-clock-2026-01-01.parquet").to_pydict()
    assert bbo["timestamp"] == [(base + n) // 1000 for n in (200_000, 300_000, 400_000, 500_000)]
    assert bbo["best_bid_qty"] == [1., 5., 5., 5.]
    assert clock["observation_age_us"] == [100_000, 100_000, 200_000, 300_000]
    assert stats["continuation"] is None


def test_fusion_noop_observation_and_invalid_decimal_fail_closed(tmp_path):
    source = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000)
                           + [(200_000, False, "bid", 100, 1, None, None)])
    table, stats = _fusion_run({"tardis": source})
    assert table[-1:]["fusion_reason"].to_pylist() == ["observation_refresh"]
    assert stats["observation_refresh_messages"] == 1
    invalid = _fusion_input(tmp_path, "bad", _fusion_snapshot(100_000, qty="0.000000001"))
    iterator, status = normalizer.iter_fused_book_batches({"tardis": invalid}, "2026-01-01", minimum_levels=2)
    with pytest.raises(pa.ArrowInvalid):
        list(iterator)
    assert status["status"] == "FAILED"


@pytest.mark.parametrize("column", ["exchange", "symbol"])
def test_fusion_rejects_null_market_identity_instead_of_hardcoding_it(tmp_path, column):
    source = _fusion_input(tmp_path, "a", _fusion_snapshot(100_000))
    table = pq.read_table(source)
    table = table.set_column(table.schema.get_field_index(column), column,
                             pa.array([None] + table[column].to_pylist()[1:], type=pa.string()))
    pq.write_table(table, source)
    iterator, status = normalizer.iter_fused_book_batches({"tardis": source}, "2026-01-01", minimum_levels=2)
    with pytest.raises(ValueError, match=f"{column} mismatch"):
        list(iterator)
    assert status["status"] == "FAILED"
