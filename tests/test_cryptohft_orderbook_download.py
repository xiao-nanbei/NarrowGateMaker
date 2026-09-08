import json
import sys
from datetime import datetime, timezone

import pandas as pd
import pytest

import data.download_cryptohft_orderbook as cryptohft_orderbook
from data.download_cryptohft_orderbook import (
    DEFAULT_WARMUP_HOURS,
    BadDayRepair,
    DailyOutputWriter,
    OrderBookSequenceState,
    OrderBookState,
    _contiguous_day_ranges,
    _daily_write_start,
    _default_target_roots,
    _load_bad_day_repairs,
    _load_per_day_sequence_audits,
    _load_retained_days,
    _raw_paths_for_repair,
    _retained_process_ranges,
    _select_bad_day_repairs,
    _select_ts_ms,
    _sequence_audit_status,
)


def test_default_warmup_can_reach_a_prior_utc_day_snapshot():
    assert DEFAULT_WARMUP_HOURS >= 24


def test_tardis_daily_export_preserves_all_raw_columns_and_reuses(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    day = "2026-09-05"
    for hour in range(24):
        path = tmp_path / "raw" / "binance_futures" / day / f"{hour:02d}" / "BTCUSDC_orderbook.parquet.zst"
        path.parent.mkdir(parents=True)
        pd.DataFrame([dict(received_time=1788566400000111652, event_time=1788566399875,
                           transaction_time=1788566399872, symbol="BTCUSDC", event_type="update",
                           first_update_id=10, final_update_id=11, prev_final_update_id=9,
                           side="ask", price="79648.3000", quantity="0.476000", order_count=None)]).to_parquet(path)
    args = (tmp_path / "raw", tmp_path / "daily", "binance_futures", "BTCUSDC", day)
    result = cryptohft_orderbook.export_tardis_day(*args)
    assert result["rows"] == 24 and result["round_trip_all_original_columns"]
    output = tmp_path / "daily/binance_futures/BTCUSDC/incremental_book_L2/2026-09-05.parquet"
    rows = pq.read_table(output).to_pylist()
    assert rows[0]["timestamp"] == 1788566399872000
    assert rows[0]["local_timestamp"] == 1788566400000111
    assert rows[0]["received_time"] == 1788566400000111652
    assert rows[0]["amount"] == "0.476000"
    assert [r["source_hour"] for r in rows] == list(range(24))
    assert all(r["first_update_id"] == 10 for r in rows)
    assert cryptohft_orderbook.export_tardis_day(*args)["status"] == "reused_verified"
    assert path.exists() and not result["originals_deleted"]
    from data.build_active_order_queue_tape import iter_cryptohft_logical_messages
    before = list(iter_cryptohft_logical_messages(path, 0.1))
    monkeypatch.setenv("NARROWGATE_DAILY_ORDERBOOK_ROOT", str(tmp_path / "daily"))
    original_unlink = type(path).unlink
    def interrupted_unlink(item, *args, **kwargs):
        if item.name == "BTCUSDC_orderbook.parquet.zst" and item.parent.name == "12":
            raise OSError("simulated retirement interruption")
        return original_unlink(item, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(type(path), "unlink", interrupted_unlink)
        with pytest.raises(OSError, match="simulated retirement interruption"):
            cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert output.exists() and path.exists()
    retired = cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert retired["originals_deleted"] and not path.exists()
    assert cryptohft_orderbook.raw_hour_available(path)
    assert list(iter_cryptohft_logical_messages(path, 0.1)) == before
    from models.exchange_book_replay import CryptoHFTExchangeBookTape
    tape = CryptoHFTExchangeBookTape(raw_root=tmp_path / "raw", day=day,
        symbol="BTCUSDC", tick_size=0.1, warmup_hours=0, cache_enabled=False)
    assert not tape.missing_paths and len(list(tape)) == 24
    assert len(tape.identity()["files"]) == 24
    cached = CryptoHFTExchangeBookTape(raw_root=tmp_path / "raw", day=day,
        symbol="BTCUSDC", tick_size=0.1, warmup_hours=0, cache_dir=tmp_path / "cache")
    assert list(cached) == list(tape)
    assert list(cached) == list(tape)
    assert cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)["originals_deleted"]
    # A damaged daily container must not cause retirement of a redownloaded hour.
    path.parent.mkdir(parents=True)
    path.write_bytes(b"unique source")
    with pytest.raises(ValueError, match="Existing conversion differs"):
        cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert path.read_bytes() == b"unique source"
    with output.open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ValueError, match="Existing conversion differs"):
        cryptohft_orderbook.export_tardis_day(*args, delete_hourly=True)
    assert path.read_bytes() == b"unique source"


def test_tardis_export_incomplete_day_does_not_publish_or_delete(tmp_path):
    result = cryptohft_orderbook.export_tardis_day(tmp_path, tmp_path / "out", "binance_futures", "BTCUSDC", "2026-09-05")
    assert result["status"] == "missing_hours" and len(result["hours"]) == 24
    assert not list((tmp_path / "out").rglob("*.parquet"))


@pytest.mark.parametrize("mode", ["original", "preceding_update_id"])
def test_normalized_buckets_use_sequence_anchored_snapshot_clock(tmp_path, mode):
    hour = int(pd.Timestamp("2026-08-22T14:00:00Z").timestamp() * 1000)
    rows = []
    for kind, ts, identifier, previous in [
        ("snapshot", hour - 2000, 10, None),
        ("update", hour - 200, 11, 10),
        ("snapshot", hour, 11, None),
        ("update", hour - 100, 12, 11),
        ("update", hour + 100, 13, 12),
    ]:
        for side, price in [("bid", 100.), ("ask", 101.)]:
            rows.append(dict(event_type=kind, event_time=ts,
                             transaction_time=0 if kind == "snapshot" else ts,
                             received_time=ts + 5, first_update_id=identifier,
                             final_update_id=identifier, prev_final_update_id=previous,
                             last_update_id=identifier if kind == "snapshot" else None,
                             side=side, price=price, quantity=2.))
    path = tmp_path / "raw.parquet"
    pd.DataFrame(rows).to_parquet(path)
    book = OrderBookState()
    state = OrderBookSequenceState(book, recorder_snapshot_clock=mode)
    writer = DailyOutputWriter([tmp_path / "processed"], "BTCUSDC", 1)
    _, end = cryptohft_orderbook._replay_orderbook_file(
        path, book, writer, 1, 100, hour - 3000, None, None, state, "transaction", 0,
    )
    cryptohft_orderbook._emit_snapshot(book, end, writer, 1, hour - 3000, state, 0)
    writer.close()
    assert state.stats.snapshot_sequence_anchors == (mode != "original")
    assert state.stats.message_time_reversals == (mode == "original")
    if mode != "original":
        bbo = pd.read_parquet(tmp_path / "processed/bbo/BTCUSDC-bbo-2026-08-22.parquet")
        assert bbo.timestamp.is_monotonic_increasing
        assert hour not in bbo.timestamp.tolist()
        assert bbo.timestamp.tolist() == [hour - 2000, hour - 200, hour - 100, hour + 100]


def test_default_normalized_output_is_versioned_staging() -> None:
    roots = _default_target_roots()

    assert len(roots) == 1
    assert roots[0].name == "replay_l2_retained100ms_staging"


def test_bad_day_repair_csv_is_validated_and_boolean_is_parsed(tmp_path):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "yes",
                "missing_raw_hours": "19,20,21",
            },
            {
                "symbol": "BTCUSDT",
                "date": "2025-08-14",
                "cause": "raw_has_no_snapshots",
                "suggested_fix": "not_fixable_by_redownload",
                "redownload_can_fix": "0",
                "missing_raw_hours": "",
            },
        ]
    ).to_csv(manifest, index=False)

    repairs = _load_bad_day_repairs(manifest)

    assert repairs == [
        BadDayRepair(
            symbol="BTCUSDC",
            date="2026-05-26",
            cause="missing_raw_hours",
            suggested_fix="retry_download",
            redownload_can_fix=True,
            missing_raw_hours=("19", "20", "21"),
        ),
        BadDayRepair(
            symbol="BTCUSDT",
            date="2025-08-14",
            cause="raw_has_no_snapshots",
            suggested_fix="not_fixable_by_redownload",
            redownload_can_fix=False,
            missing_raw_hours=(),
        ),
    ]

    missing_column = tmp_path / "bad_days_missing_column.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(missing_column, index=False)
    with pytest.raises(ValueError, match="suggested_fix"):
        _load_bad_day_repairs(missing_column)

    invalid_boolean = tmp_path / "bad_days_invalid_boolean.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "maybe",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(invalid_boolean, index=False)
    with pytest.raises(ValueError, match="redownload_can_fix"):
        _load_bad_day_repairs(invalid_boolean)

    empty_symbol = tmp_path / "bad_days_empty_symbol.csv"
    pd.DataFrame(
        [
            {
                "symbol": "",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(empty_symbol, index=False)
    with pytest.raises(ValueError, match="empty symbol"):
        _load_bad_day_repairs(empty_symbol)

    unknown_fix = tmp_path / "bad_days_unknown_fix.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "unknown_action",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(unknown_fix, index=False)
    with pytest.raises(ValueError, match="invalid suggested_fix"):
        _load_bad_day_repairs(unknown_fix)


def test_bad_day_repair_selection_skips_nonfixable_by_default():
    repairs = [
        BadDayRepair(
            symbol="BTCUSDC",
            date="2026-05-26",
            cause="missing_raw_hours",
            suggested_fix="retry_download",
            redownload_can_fix=True,
            missing_raw_hours=("19",),
        ),
        BadDayRepair(
            symbol="BTCUSDT",
            date="2026-05-07",
            cause="normalized_gap_with_snapshots",
            suggested_fix="force_rebuild_from_raw",
            redownload_can_fix=True,
            missing_raw_hours=(),
        ),
        BadDayRepair(
            symbol="BTCUSDC",
            date="2025-08-14",
            cause="raw_has_no_snapshots",
            suggested_fix="not_fixable_by_redownload",
            redownload_can_fix=False,
            missing_raw_hours=(),
        ),
    ]

    assert _select_bad_day_repairs(repairs) == repairs[:2]
    assert _select_bad_day_repairs(
        repairs,
        symbols={"BTCUSDC"},
        causes={"missing_raw_hours"},
    ) == repairs[:1]
    assert _select_bad_day_repairs(
        repairs,
        symbols={"BTCUSDC"},
        include_nonfixable=True,
    ) == [repairs[0], repairs[2]]
    assert _select_bad_day_repairs(
        repairs,
        include_nonfixable=True,
        limit=1,
    ) == repairs[:1]


def test_bad_day_repair_raw_paths_include_exchange_and_respect_scope(
    tmp_path,
):
    repair = BadDayRepair(
        symbol="BTCUSDC",
        date="2026-05-26",
        cause="missing_raw_hours",
        suggested_fix="retry_download",
        redownload_can_fix=True,
        missing_raw_hours=("03", "19"),
    )

    selected = _raw_paths_for_repair(
        tmp_path,
        "binance_futures",
        repair,
    )
    assert selected == [
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "03"
        / "BTCUSDC_orderbook.parquet.zst",
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "19"
        / "BTCUSDC_orderbook.parquet.zst",
    ]

    entire_day = _raw_paths_for_repair(
        tmp_path,
        "binance_futures",
        repair,
        refresh_entire_day=True,
    )
    assert len(entire_day) == 24
    assert entire_day[0] == (
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "00"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    assert entire_day[-1] == (
        tmp_path
        / "binance_futures"
        / "2026-05-26"
        / "23"
        / "BTCUSDC_orderbook.parquet.zst"
    )


def test_classified_refresh_action_is_honored_without_cli_override():
    repair = BadDayRepair(
        symbol="BTCUSDC",
        date="2026-05-26",
        cause="raw_decode_errors",
        suggested_fix="refresh_raw_and_rebuild",
        redownload_can_fix=True,
        missing_raw_hours=(),
    )

    scope = cryptohft_orderbook._effective_repair_refresh_scope(
        repair,
        "none",
    )

    assert scope == "day"
    assert cryptohft_orderbook._repair_action(repair, scope) == (
        "refresh_raw_and_rebuild"
    )


def test_repair_raw_refresh_validates_before_atomic_replace(
    tmp_path,
    monkeypatch,
):
    raw_root = tmp_path / "raw"
    raw_path = (
        raw_root
        / "binance_futures"
        / "2026-05-26"
        / "19"
        / "BTCUSDC_orderbook.parquet.zst"
    )
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"old-cache")

    class SuccessfulClient:
        def download_file(self, relative_path, out_path):
            assert relative_path == raw_path.relative_to(raw_root)
            out_path.write_bytes(b"validated-refresh")
            return "downloaded"

    monkeypatch.setattr(
        cryptohft_orderbook,
        "_read_raw_parquet_zst_summary",
        lambda path: ([1], {}),
    )
    assert (
        cryptohft_orderbook._refresh_repair_raw_files(
            SuccessfulClient(),
            raw_root,
            [raw_path],
        )
        == 1
    )
    assert raw_path.read_bytes() == b"validated-refresh"

    raw_path.write_bytes(b"preserve-on-failure")

    class FailingClient:
        def download_file(self, relative_path, out_path):
            del relative_path
            out_path.write_bytes(b"partial-refresh")
            raise RuntimeError("network failed")

    with pytest.raises(RuntimeError, match="network failed"):
        cryptohft_orderbook._refresh_repair_raw_files(
            FailingClient(),
            raw_root,
            [raw_path],
        )
    assert raw_path.read_bytes() == b"preserve-on-failure"
    assert not raw_path.with_suffix(raw_path.suffix + ".refresh").exists()


def test_bad_day_repair_main_dry_run_has_no_side_effects(
    tmp_path,
    monkeypatch,
    capsys,
):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "1",
                "missing_raw_hours": "19,20",
            }
        ]
    ).to_csv(manifest, index=False)
    raw_root = tmp_path / "raw"
    target_root = tmp_path / "target"

    def unexpected_call(*args, **kwargs):
        del args, kwargs
        raise AssertionError("dry-run attempted a mutating repair operation")

    monkeypatch.delenv("CRYPTOHFTDATA_API_KEY", raising=False)
    monkeypatch.delenv("CRYPTOHFTDATA_JWT", raising=False)
    monkeypatch.setattr(
        cryptohft_orderbook,
        "CryptoHFTClient",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_prefetch_raw_hours",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_refresh_repair_raw_files",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_process_symbol",
        unexpected_call,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_audit_days",
        unexpected_call,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_cryptohft_orderbook.py",
            "--repair-audit-csv",
            str(manifest),
            "--symbols",
            "BTCUSDC",
            "--start",
            "2026-05-26",
            "--end",
            "2026-05-26",
            "--repair-refresh-raw",
            "listed",
            "--raw-root",
            str(raw_root),
            "--target-root",
            str(target_root),
            "--dry-run",
        ],
    )

    cryptohft_orderbook.main()

    output = capsys.readouterr().out
    assert "dry-run" in output
    assert "no raw or normalized files were changed" in output
    assert not raw_root.exists()
    assert not target_root.exists()


@pytest.mark.parametrize("eligible", [True, False])
def test_retry_download_forwards_credentials_and_enforces_post_audit(
    tmp_path,
    monkeypatch,
    capsys,
    eligible,
):
    manifest = tmp_path / "bad_days.csv"
    pd.DataFrame(
        [
            {
                "symbol": "BTCUSDC",
                "date": "2026-05-26",
                "cause": "missing_raw_hours",
                "suggested_fix": "retry_download",
                "redownload_can_fix": "true",
                "missing_raw_hours": "19",
            }
        ]
    ).to_csv(manifest, index=False)
    raw_root = tmp_path / "raw"
    target_root = tmp_path / "target"
    captured = {}
    fake_client = object()

    def build_client(*, api_key, jwt, transport):
        captured["client"] = {
            "api_key": api_key,
            "jwt": jwt,
            "transport": transport,
        }
        return fake_client

    def prefetch_raw_hours(**kwargs):
        captured["prefetch"] = kwargs
        return {"downloaded": 1, "exists": 0, "404": 0}

    def process_symbol(**kwargs):
        captured["process"] = kwargs
        return (
            1,
            0,
            100,
            {
                "day_sequence_audits": {
                    "2026-05-26": {
                        "target_initialized_at_start": True,
                    }
                }
            },
        )

    def audit_days(*args, **kwargs):
        captured["audit"] = {"args": args, "kwargs": kwargs}
        return pd.DataFrame([{"eligible": eligible}])

    def unexpected_refresh(*args, **kwargs):
        del args, kwargs
        raise AssertionError("retry_download unexpectedly forced raw refresh")

    monkeypatch.delenv("CRYPTOHFTDATA_API_KEY", raising=False)
    monkeypatch.delenv("CRYPTOHFTDATA_JWT", raising=False)
    monkeypatch.setattr(
        cryptohft_orderbook,
        "CryptoHFTClient",
        build_client,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_prefetch_raw_hours",
        prefetch_raw_hours,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_refresh_repair_raw_files",
        unexpected_refresh,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_process_symbol",
        process_symbol,
    )
    monkeypatch.setattr(
        cryptohft_orderbook,
        "_audit_days",
        audit_days,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_cryptohft_orderbook.py",
            "--repair-audit-csv",
            str(manifest),
            "--symbols",
            "BTCUSDC",
            "--start",
            "2026-05-26",
            "--end",
            "2026-05-26",
            "--raw-root",
            str(raw_root),
            "--target-root",
            str(target_root),
            "--api-key",
            "api-key-sentinel",
            "--jwt",
            "jwt-sentinel",
            "--transport",
            "rest",
        ],
    )

    if eligible:
        cryptohft_orderbook.main()
    else:
        with pytest.raises(SystemExit, match="post-audit failed"):
            cryptohft_orderbook.main()

    assert captured["client"] == {
        "api_key": "api-key-sentinel",
        "jwt": "jwt-sentinel",
        "transport": "rest",
    }
    assert captured["prefetch"]["api_key"] == "api-key-sentinel"
    assert captured["prefetch"]["jwt"] == "jwt-sentinel"
    assert captured["prefetch"]["transport"] == "rest"
    assert captured["prefetch"]["raw_root"] == raw_root
    assert captured["prefetch"]["symbols"] == ["BTCUSDC"]
    assert captured["process"]["client"] is fake_client
    assert captured["process"]["download_missing"] is True
    output = capsys.readouterr().out
    assert "api-key-sentinel" not in output
    assert "jwt-sentinel" not in output


def test_orderbook_top_levels_do_not_repeat_updated_or_readded_prices():
    book = OrderBookState()
    book.apply("bid", 100.0, 1.0)
    book.apply("bid", 100.0, 2.0)
    book.apply("bid", 99.0, 3.0)
    book.apply("bid", 100.0, 0.0)
    book.apply("bid", 100.0, 4.0)

    bids, _ = book.top_levels(5)

    assert bids == [(100.0, 4.0), (99.0, 3.0)]


def test_daily_normalization_start_does_not_truncate_existing_day():
    requested_hour = datetime(2026, 7, 15, 18, 37, tzinfo=timezone.utc)

    assert _daily_write_start(requested_hour) == datetime(
        2026, 7, 15, 0, 0, tzinfo=timezone.utc
    )


def test_sequence_state_requires_snapshot_and_invalidates_gap():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book)

    def begin(
        *,
        event_type="update",
        receive=1_000,
        event=900,
        transaction=890,
        first=None,
        final=None,
        previous=None,
        last=None,
    ):
        return sequence.begin_message(
            event_type=event_type,
            receive_time_ms=receive,
            event_time_ms=event,
            transaction_time_ms=transaction,
            first_update_id=first,
            final_update_id=final,
            previous_final_update_id=previous,
            last_update_id=last,
        )

    assert not begin(first=1, final=2, previous=0)
    assert begin(event_type="snapshot", receive=1_100, event=1_000, last=100)
    book.apply("bid", 99.0, 2.0)
    # Rows from one native snapshot may carry different recorder receive
    # timestamps. They remain one logical snapshot and must all apply.
    assert begin(event_type="snapshot", receive=1_200, event=1_000, last=100)
    assert book.top_levels(1)[0] == [(99.0, 2.0)]

    # The first delta spans the REST snapshot ID; pu may precede it.
    assert begin(receive=1_300, event=1_250, first=100, final=105, previous=98)
    assert begin(receive=1_400, event=1_350, first=106, final=109, previous=105)

    # A subsequent pu mismatch invalidates the full book until a new snapshot.
    assert not begin(receive=1_500, event=1_450, first=110, final=112, previous=107)
    assert book.top_levels(1) == ([], [])
    assert not begin(receive=1_600, event=1_550, first=113, final=114, previous=112)
    assert begin(event_type="snapshot", receive=1_700, event=1_650, last=120)
    assert begin(receive=1_800, event=1_750, first=125, final=130, previous=120)

    assert sequence.stats.duplicate_snapshots == 0
    assert sequence.stats.sequence_gaps == 1
    assert sequence.stats.ignored_before_snapshot == 2
    assert sequence.stats.message_intervals == 7
    assert sequence.stats.message_interval_le_100ms == 7
    assert sequence.stats.message_time_reversals == 0


def test_delta_bootstrap_is_explicit_and_waits_for_convergence():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book, allow_delta_bootstrap=True)

    assert sequence.begin_message(
        event_type="update",
        receive_time_ms=1_010,
        event_time_ms=1_000,
        transaction_time_ms=1_000,
        first_update_id=101,
        final_update_id=105,
        previous_final_update_id=100,
        last_update_id=None,
    )
    book.apply("bid", 100.0, 1.0)

    assert sequence.initialization_source == "delta"
    assert sequence.stats.delta_bootstrap_messages == 1
    assert not sequence.output_ready(60_999, 60_000)
    assert sequence.output_ready(61_000, 60_000)

    assert sequence.begin_message(
        event_type="update",
        receive_time_ms=61_010,
        event_time_ms=61_000,
        transaction_time_ms=61_000,
        first_update_id=106,
        final_update_id=110,
        previous_final_update_id=105,
        last_update_id=None,
    )
    assert not sequence.begin_message(
        event_type="update",
        receive_time_ms=61_110,
        event_time_ms=61_100,
        transaction_time_ms=61_100,
        first_update_id=111,
        final_update_id=115,
        previous_final_update_id=109,
        last_update_id=None,
    )
    assert not sequence.output_ready(120_000, 60_000)


def test_native_snapshot_does_not_require_delta_burn_in():
    book = OrderBookState()
    sequence = OrderBookSequenceState(book, allow_delta_bootstrap=True)

    assert sequence.begin_message(
        event_type="snapshot",
        receive_time_ms=1_010,
        event_time_ms=1_000,
        transaction_time_ms=1_000,
        first_update_id=None,
        final_update_id=None,
        previous_final_update_id=None,
        last_update_id=100,
    )

    assert sequence.initialization_source == "snapshot"
    assert sequence.output_ready(1_000, 60_000)


def test_timestamp_source_can_match_live_transaction_clock():
    frame = pd.DataFrame(
        {
            "event_time": [1_000, 2_000],
            "transaction_time": [990, None],
            "received_time": [1_100_000_000, 2_100_000_000],
        }
    )

    assert _select_ts_ms(frame, "transaction").tolist() == [990, 2_000]
    assert _select_ts_ms(frame, "event").tolist() == [1_000, 2_000]


def test_retained_manifest_is_strict_and_deduplicated(tmp_path):
    manifest = tmp_path / "retained.csv"
    manifest.write_text(
        "day\n2026-01-03\n2026-01-01\n2026-01-03\n",
        encoding="utf-8",
    )

    assert _load_retained_days(manifest) == ["2026-01-01", "2026-01-03"]


def test_retained_days_are_grouped_without_bridging_bad_days():
    ranges = _contiguous_day_ranges(
        ["2026-01-01", "2026-01-03", "2026-01-04"]
    )

    assert [
        (start.isoformat(), end.isoformat())
        for start, end in ranges
    ] == [
        (
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T23:00:00+00:00",
        ),
        (
            "2026-01-03T00:00:00+00:00",
            "2026-01-04T23:00:00+00:00",
        ),
    ]


def test_independent_retained_days_receive_separate_warmup_ranges():
    ranges = _retained_process_ranges(
        ["2026-01-03", "2026-01-04"],
        independent_days=True,
        sequence_bootstrap="snapshot",
    )

    assert [
        (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        for start, end in ranges
    ] == [
        ("2026-01-03", "2026-01-03"),
        ("2026-01-04", "2026-01-04"),
    ]


def test_contiguous_retained_ranges_can_be_bounded_for_parallel_balance():
    ranges = _retained_process_ranges(
        [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
            "2026-01-04",
            "2026-01-05",
        ],
        independent_days=False,
        sequence_bootstrap="snapshot",
        max_days=2,
    )

    assert [
        (start.isoformat(), end.isoformat())
        for start, end in ranges
    ] == [
        (
            "2026-01-01T00:00:00+00:00",
            "2026-01-02T23:00:00+00:00",
        ),
        (
            "2026-01-03T00:00:00+00:00",
            "2026-01-04T23:00:00+00:00",
        ),
        (
            "2026-01-05T00:00:00+00:00",
            "2026-01-05T23:00:00+00:00",
        ),
    ]


def test_target_scoped_sequence_audit_ignores_recovered_warmup_gap():
    passed, gaps, delta_bootstrap = _sequence_audit_status(
        {
            "sequence_gaps": 2,
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "snapshot",
            "target_accepted_updates": 100,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
            "target_delta_bootstrap_messages": 0,
        }
    )

    assert passed
    assert gaps == 0
    assert delta_bootstrap == 0


def test_target_scoped_sequence_audit_requires_snapshot_seed_at_midnight():
    passed, _, _ = _sequence_audit_status(
        {
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "delta",
            "target_accepted_updates": 100,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
        }
    )

    assert not passed


def test_daily_writer_does_not_emit_non_retained_days(tmp_path):
    writer = DailyOutputWriter(
        [tmp_path],
        "BTCUSDC",
        1,
        allowed_days={"2026-01-03"},
    )
    levels = [(100.0, 1.0)]
    writer.append(
        int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000),
        levels,
        [(100.1, 1.0)],
    )
    writer.append(
        int(datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000),
        levels,
        [(100.1, 1.0)],
    )
    writer.close()

    assert not (tmp_path / "bbo" / "BTCUSDC-bbo-2026-01-02.parquet").exists()
    assert (tmp_path / "bbo" / "BTCUSDC-bbo-2026-01-03.parquet").exists()


def test_sequence_audit_manifest_requires_one_range_per_day(tmp_path):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-03T23:00:00+00:00",
                        "sequence_audit": {
                            "accepted_updates": 10,
                            "sequence_gaps": 0,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_day = _load_per_day_sequence_audits(path)
    assert by_day == {
        "2026-01-03": {
            "accepted_updates": 10,
            "sequence_gaps": 0,
        }
    }

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["range_audits"][0]["range_end_utc"] = (
        "2026-01-04T23:00:00+00:00"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="one sequence audit range"):
        _load_per_day_sequence_audits(path)


def test_sequence_audit_manifest_accepts_target_scoped_days_in_one_range(
    tmp_path,
):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-04T23:00:00+00:00",
                        "sequence_audit": {
                            "day_sequence_audits": {
                                "2026-01-03": {
                                    "target_initialized_at_start": True,
                                    "target_initialization_source_at_start": (
                                        "snapshot"
                                    ),
                                    "target_accepted_updates": 10,
                                    "target_sequence_gaps": 0,
                                },
                                "2026-01-04": {
                                    "target_initialized_at_start": True,
                                    "target_initialization_source_at_start": (
                                        "snapshot"
                                    ),
                                    "target_accepted_updates": 11,
                                    "target_sequence_gaps": 0,
                                },
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_day = _load_per_day_sequence_audits(path)

    assert sorted(by_day) == ["2026-01-03", "2026-01-04"]
    assert by_day["2026-01-04"]["target_accepted_updates"] == 11


def test_sequence_audit_manifest_keeps_same_day_for_multiple_symbols(
    tmp_path,
):
    path = tmp_path / "sequence.json"
    path.write_text(
        json.dumps(
            {
                "range_audits": [
                    {
                        "symbol": symbol,
                        "range_start_utc": "2026-01-03T00:00:00+00:00",
                        "range_end_utc": "2026-01-03T23:00:00+00:00",
                        "sequence_audit": {
                            "accepted_updates": accepted_updates,
                            "sequence_gaps": 0,
                        },
                    }
                    for symbol, accepted_updates in (
                        ("BTCUSDC", 10),
                        ("BTCUSDT", 11),
                    )
                ]
            }
        ),
        encoding="utf-8",
    )

    _, by_symbol_day = _load_per_day_sequence_audits(path)

    assert sorted(by_symbol_day) == [
        "BTCUSDC:2026-01-03",
        "BTCUSDT:2026-01-03",
    ]
    assert by_symbol_day["BTCUSDT:2026-01-03"]["accepted_updates"] == 11
