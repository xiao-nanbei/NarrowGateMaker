from __future__ import annotations

import json
import numpy as np
import pandas as pd
import pytest

from features import feature_engineer as engineer


@pytest.fixture(autouse=True)
def isolated_reference_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("MM_REFERENCE_BARS_DIR", str(tmp_path / "reference-bars"))
    monkeypatch.delenv("MM_REFERENCE_BAR_PROOF", raising=False)


def _l2_frame(timestamps, bids, depths):
    frame = pd.DataFrame({"timestamp": timestamps})
    for side in ("bid", "ask"):
        for level in range(1, 11):
            frame[f"{side}_px_{level}"] = [bid + (level if side == "ask" else 1 - level) for bid in bids]
            frame[f"{side}_qty_{level}"] = depths
    return frame


def test_l2_flow_inherits_adjacent_day_tail_and_missing_seed_is_unknown(tmp_path, monkeypatch):
    start = int(pd.Timestamp("2026-01-02", tz="UTC").timestamp() * 1000)
    prior = tmp_path / "BTCUSDC-l2-2026-01-01.parquet"
    target = tmp_path / "BTCUSDC-l2-2026-01-02.parquet"
    _l2_frame([start - 100], [100.0], [1.0]).to_parquet(prior, index=False)
    _l2_frame([start, start + 1000], [101.0, 101.0], [2.0, 2.0]).to_parquet(target, index=False)
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol", lambda symbol: (tmp_path, tmp_path))
    monkeypatch.setattr(engineer, "filter_frame_for_orderbook_quality", lambda frame, *a, **k: frame)
    seed = engineer._previous_l2_state("2026-01-02", "BTCUSDC")
    assert seed["timestamp_ms"] == start - 100
    result = engineer.load_l2_summary_1s("2026-01-02", "BTCUSDC")
    assert result.iloc[0]["l2_quote_flip_rate"] == 1.0
    assert result.iloc[0]["l2_book_refresh_ratio"] == 1.0
    assert result.iloc[0]["l2_book_cancel_ratio"] == 0.0
    prior.unlink()
    result = engineer.load_l2_summary_1s("2026-01-02", "BTCUSDC")
    assert result.iloc[0][engineer.EXECUTION_L2_FLOW_COLS].isna().all()
    assert result.iloc[1][engineer.EXECUTION_L2_FLOW_COLS].eq(0).all()


def test_l2_prior_state_cannot_use_future_or_nonadjacent_file(tmp_path, monkeypatch):
    start = int(pd.Timestamp("2026-01-02", tz="UTC").timestamp() * 1000)
    path = tmp_path / "BTCUSDC-l2-2026-01-01.parquet"
    _l2_frame([start - 100, start + 100], [100.0, 999.0], [1.0, 2.0]).to_parquet(path, index=False)
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol", lambda symbol: (tmp_path, tmp_path))
    seed = engineer._previous_l2_state("2026-01-02", "BTCUSDC")
    assert seed["best_bid"] == 100.0
    assert engineer._previous_l2_state("2026-01-03", "BTCUSDC") is None


def test_l2_missing_state_and_initial_flow_do_not_become_zero(monkeypatch):
    index = pd.date_range("2026-01-01", periods=40, freq="1s", tz="UTC")
    l2 = pd.DataFrame(1.0, index=index[[0, 30]], columns=engineer.EXECUTION_L2_FEATURE_COLS)
    l2.loc[index[0], engineer.EXECUTION_L2_FLOW_COLS] = np.nan
    monkeypatch.setattr(engineer, "load_l2_summary_1s", lambda *a: l2)
    monkeypatch.setattr(engineer, "_load_exec_l2_max_age_s", lambda *a: 5.0)
    base = pd.DataFrame({"close": [1.0] * 4}, index=index[::10])
    result = engineer.add_execution_l2_features(base, index, "2026-01-01", "BTCUSDC", require_l2=True)
    assert result.iloc[0][engineer.EXECUTION_L2_FLOW_COLS].isna().all()
    assert result.iloc[1][engineer.EXECUTION_L2_FEATURE_COLS].isna().all()
    assert result.iloc[3][engineer.EXECUTION_L2_FEATURE_COLS].eq(1).all()


def test_l2_uses_real_observation_age_not_output_cadence(tmp_path, monkeypatch):
    start = int(pd.Timestamp("2026-01-02", tz="UTC").timestamp() * 1000)
    path = tmp_path / "BTCUSDC-l2-2026-01-02.parquet"
    _l2_frame([start, start + 1000, start + 6000, start + 19000],
              [100.0, 100.0, 100.0, 101.0], [1.0] * 4).to_parquet(path, index=False)
    monkeypatch.setattr(engineer, "_l2_observation_clock", lambda *args: (
        np.array([start, start, start, start + 19000]) * 1000,
        np.array([True, False, False, True]),
    ))
    summary = engineer._load_l2_summary_1s(path, unknown_initial_flow=True)
    assert summary.iloc[1][engineer.EXECUTION_L2_FLOW_COLS].isna().all()
    monkeypatch.setattr(engineer, "load_l2_summary_1s", lambda *args: summary)
    monkeypatch.setattr(engineer, "_load_exec_l2_max_age_s", lambda *args: 5.0)
    seconds = pd.date_range("2026-01-02", periods=20, freq="1s", tz="UTC")
    base = pd.DataFrame({"close": [100.0, 101.0]}, index=seconds[::10])
    result = engineer.add_execution_l2_features(base, seconds, "2026-01-02", "BTCUSDC", require_l2=True)
    assert result.iloc[0][engineer.EXECUTION_L2_STATE_COLS].isna().all()
    assert result.iloc[1][engineer.EXECUTION_L2_STATE_COLS].notna().all()


def test_l2_clock_loader_passes_requested_symbol(monkeypatch, tmp_path):
    from models import backtest_tick
    observed = []

    def load(path, timestamps, kind, *, symbol=None):
        observed.append((path, kind, symbol))
        return timestamps * 1000, np.ones(len(timestamps), dtype=bool)

    monkeypatch.setattr(backtest_tick, "_load_book_observations", load)
    path = tmp_path / "BTCUSDT-l2-2026-01-02.parquet"
    engineer._l2_observation_clock(path, np.array([1000]), "BTCUSDT")
    assert observed == [(path, "l2", "BTCUSDT")]


def test_l2_rejects_source_clock_regression_across_day(tmp_path, monkeypatch):
    start = int(pd.Timestamp("2026-01-02", tz="UTC").timestamp() * 1000)
    path = tmp_path / "BTCUSDC-l2-2026-01-02.parquet"
    _l2_frame([start], [100.0], [1.0]).to_parquet(path, index=False)
    monkeypatch.setattr(engineer, "_l2_observation_clock", lambda *args: (
        np.array([(start - 2000) * 1000]), np.array([True]),
    ))
    previous = {"timestamp_ms": start - 100, "best_bid": 100.0, "best_ask": 101.0,
                "total_depth": 20.0, "last_observation_timestamp_us": (start - 1000) * 1000}
    assert engineer._load_l2_summary_1s(path, previous_state=previous) is None


def test_process_day_features_only_never_calls_label_builder(monkeypatch) -> None:
    index = pd.date_range("2026-01-01", periods=180, freq="1s", tz="UTC")
    bars = pd.DataFrame(
        {
            "open": np.linspace(100.0, 101.0, len(index)),
            "high": np.linspace(100.1, 101.1, len(index)),
            "low": np.linspace(99.9, 100.9, len(index)),
            "close": np.linspace(100.0, 101.0, len(index)),
            "volume": np.ones(len(index)),
            "buy_volume": np.full(len(index), 0.5),
            "sell_volume": np.full(len(index), 0.5),
            "trade_count": np.ones(len(index)),
            "buy_count": np.ones(len(index)),
            "sell_count": np.ones(len(index)),
        },
        index=index,
    )
    monkeypatch.setattr(
        engineer,
        "add_taker_tempo_features",
        lambda frame, *args, **kwargs: frame,
    )
    monkeypatch.setattr(
        engineer,
        "add_execution_l2_features",
        lambda frame, *args, **kwargs: frame,
    )
    monkeypatch.setattr(engineer, "load_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        engineer,
        "add_cross_market_features",
        lambda frame, *args, **kwargs: frame,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("future labels must not be computed")

    monkeypatch.setattr(engineer, "add_labels", forbidden)
    result = engineer.process_day(
        bars,
        "2026-01-01",
        "BTCUSDC",
        market_stage="single",
        include_labels=False,
    )

    assert not any(column.startswith("label_") for column in result.columns)
    assert "sample_weight" in result.columns


def test_features_only_cli_is_explicit() -> None:
    source = engineer.Path(engineer.__file__).read_text(encoding="utf-8")
    assert 'split = {"inference": sorted(daily_tags)}' in source
    assert '"labels_materialized": bool(labels_materialized)' in source


@pytest.mark.parametrize("labels_materialized", [False, True])
def test_inference_manifest_does_not_load_label_only_artifacts(tmp_path, monkeypatch, labels_materialized):
    feature = tmp_path / "features_2025-08-01.parquet"
    pd.DataFrame({"signal": [1.0]}).to_parquet(feature)
    monkeypatch.setattr(engineer, "BARS_DIR", tmp_path / "bars")
    monkeypatch.setattr(engineer, "TRADE_FEATURE_DIR", tmp_path / "tempo")
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol",
                        lambda symbol: (tmp_path / "book/bbo", tmp_path / "book/l2"))

    def unavailable(*args, **kwargs):
        raise RuntimeError("label-only P3 is not installed on inference host")

    monkeypatch.setattr(engineer, "_load_label_quote_params", unavailable)
    kwargs = dict(symbol="BTCUSDC", feature_paths=[("2025-08-01", feature)], warmup_days=7,
                  market_stage="minimal", reference_symbol="BTCUSDT", config_path=None,
                  split={"inference": ["2025-08-01"]}, sample_weight_reference_date="2026-07-23",
                  sample_weight_lambda=.1, require_execution_l2=False, require_taker_tempo=False,
                  labels_materialized=labels_materialized)
    if labels_materialized:
        with pytest.raises(RuntimeError, match="label-only P3"):
            engineer.write_causal_feature_manifest(tmp_path, **kwargs)
        return
    payload = json.loads(engineer.write_causal_feature_manifest(tmp_path, **kwargs).read_text())
    assert payload["label_quote_calibration"] is None
    assert payload["label_quote_policy"] is None
    assert payload["labels_materialized"] is False
    assert payload["daily_file_count"] == 1
    assert payload["daily_files"][0]["sha256"] == engineer._sha256_file(feature)


@pytest.mark.parametrize("value, expected", [
    (True, True), (False, False), (np.bool_(True), True),
    (1, True), (0, False), (1.0, True), (0.0, False),
    ("True", True), ("False", False), (" TRUE ", True), ("false", False),
    ("1", True), ("0", False), ("1.0", True), ("0.0", False),
    ("UNKNOWN", None), (None, None), (np.nan, None), (pd.NA, None),
    ("unavailable", None), ("yes", None), (2, None), (float("inf"), None),
])
def test_quality_boolean_flags_require_explicit_truth(value, expected):
    actual = engineer._quality_boolean_series(pd.Series([value], dtype=object)).iloc[0]
    if expected is None:
        assert pd.isna(actual)
    else:
        assert bool(actual) is expected


@pytest.mark.parametrize("require_execution_l2", [False, True])
def test_manifest_quality_unknown_does_not_pass_required_schema(tmp_path, monkeypatch, require_execution_l2):
    days = ["2025-08-01", "2025-08-02", "2025-08-03"]
    flags = ["cadence_schema_valid", "coverage_99_valid", "formal_eligible",
             "provider_sensitivity_replay_eligible", "exact_queue_policy_eligible"]
    book = tmp_path / "book"
    book.mkdir()
    (book / "manifest.json").write_text("{}")
    pd.DataFrame({"day": days, **{name: ["True", "False", "UNKNOWN"] for name in flags}}).to_csv(
        book / "daily_quality.csv", index=False)
    paths = []
    for day in days:
        feature = tmp_path / f"features_{day}.parquet"
        pd.DataFrame({"signal": [1.0]}).to_parquet(feature)
        paths.append((day, feature))
    monkeypatch.setattr(engineer, "BARS_DIR", tmp_path / "bars")
    monkeypatch.setattr(engineer, "TRADE_FEATURE_DIR", tmp_path / "tempo")
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol", lambda symbol: (book / "bbo", book / "l2"))
    kwargs = dict(symbol="BTCUSDC", feature_paths=paths, warmup_days=7,
                  market_stage="minimal", reference_symbol="BTCUSDT", config_path=None,
                  split={"inference": days}, sample_weight_reference_date="2026-07-23",
                  sample_weight_lambda=.1, require_execution_l2=require_execution_l2,
                  require_taker_tempo=False, labels_materialized=False)
    if require_execution_l2:
        with pytest.raises(RuntimeError, match="cadence/schema failed or unknown.*2025-08-02, 2025-08-03"):
            engineer.write_causal_feature_manifest(tmp_path, **kwargs)
        return
    payload = json.loads(engineer.write_causal_feature_manifest(tmp_path, **kwargs).read_text())
    quality = payload["execution_l2_source"]
    assert quality["quality_boolean_counts"] == {
        name: {"true": 1, "false": 1, "unknown": 1} for name in flags}
    assert all(quality[f"{name}_days"] == 1 for name in flags)


def test_manifest_unknown_economic_flags_do_not_add_a_schema_gate(tmp_path, monkeypatch):
    day = "2025-08-01"
    book = tmp_path / "book"
    book.mkdir()
    (book / "manifest.json").write_text("{}")
    (book / "daily_quality.csv").write_text(
        "day,cadence_schema_valid,coverage_99_valid,formal_eligible\n"
        f"{day},True,UNKNOWN,False\n")
    feature = tmp_path / f"features_{day}.parquet"
    pd.DataFrame({"signal": [1.0]}).to_parquet(feature)
    monkeypatch.setattr(engineer, "BARS_DIR", tmp_path / "bars")
    monkeypatch.setattr(engineer, "TRADE_FEATURE_DIR", tmp_path / "tempo")
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol", lambda symbol: (book / "bbo", book / "l2"))
    path = engineer.write_causal_feature_manifest(
        tmp_path, symbol="BTCUSDC", feature_paths=[(day, feature)], warmup_days=7,
        market_stage="minimal", reference_symbol="BTCUSDT", config_path=None,
        split={"inference": [day]}, sample_weight_reference_date="2026-07-23",
        sample_weight_lambda=.1, require_execution_l2=True, require_taker_tempo=False,
        labels_materialized=False)
    quality = json.loads(path.read_text())["execution_l2_source"]
    assert quality["cadence_schema_valid_days"] == 1
    assert quality["coverage_99_valid_days"] == quality["formal_eligible_days"] == 0
    assert quality["quality_boolean_counts"]["coverage_99_valid"]["unknown"] == 1
    assert quality["quality_boolean_counts"]["provider_sensitivity_replay_eligible"]["unknown"] == 1


def test_warmup_input_still_emits_complete_target_day_grid() -> None:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-01-01 23:59:58", tz="UTC"),
            pd.Timestamp("2026-01-02 23:59:48", tz="UTC"),
        ]
    )
    bars = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [100.0, 101.0],
            "low": [100.0, 101.0],
            "close": [100.0, 101.0],
            "vwap": [100.0, 101.0],
            "volume": [1.0, 1.0],
            "buy_volume": [0.5, 0.5],
            "sell_volume": [0.5, 0.5],
            "trade_count": [1, 1],
            "buy_count": [1, 1],
            "sell_count": [1, 1],
        },
        index=index,
    )

    dense = engineer.densify_bars_1s(
        bars,
        ensure_through_day_tag="2026-01-02",
    )
    target = engineer.resample_to_10s(dense).loc["2026-01-02"]

    assert len(target) == 8_640
    assert target.index[-1] == pd.Timestamp("2026-01-02 23:59:50", tz="UTC")
    assert target.iloc[-1]["close"] == 101.0
    assert target.iloc[-1]["volume"] == 0.0


def test_sparse_leading_prices_are_not_filled_from_future_observation() -> None:
    index = pd.DatetimeIndex([pd.Timestamp("2026-01-01 00:00:21", tz="UTC")])
    bars = pd.DataFrame({"close": [100.0], "volume": [2.0], "trade_count": [3]}, index=index)
    dense = engineer.densify_bars_1s(bars, calendar_tag="2026-01-01")
    assert dense.loc[:"2026-01-01 00:00:20", "close"].isna().all()
    assert dense.loc["2026-01-01 00:00:22", "close"] == 100.0
    assert dense.loc["2026-01-01 00:00:22", "volume"] == 0.0
    ten_seconds = engineer.resample_to_10s(dense)
    assert ten_seconds.iloc[:2][["open", "high", "low", "close"]].isna().all().all()
    assert ten_seconds["trade_count"].sum() == 3


def test_inference_calendar_retains_historical_excluded_tempo_day(tmp_path, monkeypatch) -> None:
    import data_quality

    day = "2026-01-02"
    root = tmp_path / "BTCUSDC"
    root.mkdir()
    index = pd.date_range(day, periods=1, tz="UTC")
    frame = pd.DataFrame({name: [1.0] for name in engineer.TAKER_TEMPO_FEATURE_MAP}, index=index)
    frame.to_parquet(root / f"BTCUSDC-trade-tempo-{day}.parquet")
    monkeypatch.setattr(engineer, "TRADE_FEATURE_DIR", tmp_path)
    monkeypatch.setattr(data_quality, "excluded_orderbook_days", lambda symbol: frozenset([day]))
    monkeypatch.setattr(engineer, "EXPLICIT_FEATURE_CALENDAR_DAYS", ())
    assert engineer._load_taker_tempo_features("BTCUSDC", day) is None
    monkeypatch.setattr(engineer, "EXPLICIT_FEATURE_CALENDAR_DAYS", (day,))
    assert len(engineer._load_taker_tempo_features("BTCUSDC", day)) == 1


def test_inference_manifest_binds_individual_bars_and_actual_warmup(tmp_path, monkeypatch) -> None:
    bar_root = tmp_path / "bars"
    bar_root.mkdir()
    for day in ("2026-01-01", "2026-01-02"):
        bar = bar_root / f"BTCUSDC-1s-{day}.parquet"
        pd.DataFrame({"close": [100.0]}).to_parquet(bar)
        bar.with_suffix(".parquet.meta.json").write_text(json.dumps({
            "output_sha256": engineer._sha256_file(bar), "source_sha256": "source-" + day,
            "trade_count_unit": "individual_execution", "causal_visible_at": "t+1s",
        }))
    feature = tmp_path / "features_2026-01-02.parquet"
    pd.DataFrame({"signal": [1.0]}).to_parquet(feature)
    monkeypatch.setattr(engineer, "BARS_DIR", bar_root)
    monkeypatch.setattr(engineer, "TRADE_FEATURE_DIR", tmp_path / "tempo")
    monkeypatch.setattr(engineer, "_book_dirs_for_symbol", lambda symbol: (tmp_path / "book/bbo", tmp_path / "book/l2"))
    path = engineer.write_causal_feature_manifest(
        tmp_path, symbol="BTCUSDC", feature_paths=[("2026-01-02", feature)], warmup_days=7,
        market_stage="minimal", reference_symbol="BTCUSDT", config_path=None,
        split={"inference": ["2026-01-02"]}, sample_weight_reference_date="2026-01-02",
        sample_weight_lambda=.1, require_execution_l2=False, require_taker_tempo=False, labels_materialized=False,
    )
    manifest = json.loads(path.read_text())
    assert manifest["daily_files"][0]["bar_input_days"] == ["2026-01-01", "2026-01-02"]
    assert manifest["daily_files"][0]["warmup_days_observed"] == 1
    assert {row["trade_count_unit"] for row in manifest["bar_source"]["daily_files"]} == {"individual_execution"}
    assert manifest["split_mode"] == "inference_only"


def test_explicit_reference_bar_view_is_independent_of_execution_bar_root(tmp_path, monkeypatch) -> None:
    day = "2026-01-02"
    pd.DataFrame({"close": [99.0]}).to_parquet(tmp_path / f"BTCUSDT-1s-{day}.parquet")
    monkeypatch.setenv("MM_REFERENCE_BARS_DIR", str(tmp_path))
    monkeypatch.setattr(engineer, "market_bars_dir", lambda *args: tmp_path / "other")
    result = engineer._load_market_bars_for_tag("BTCUSDT", engineer.PERP_MARKET, day)
    assert result["close"].tolist() == [99.0]


def test_reference_manifest_uses_bound_calendar_proof_not_absent_sidecar(tmp_path, monkeypatch):
    day = "2026-01-02"
    path = tmp_path / f"BTCUSDT-1s-{day}.parquet"
    pd.DataFrame({"close": [100.], "trade_count": [3]}).to_parquet(path)
    digest = engineer._sha256_file(path)
    meta = {"complete": True, "utc_day": day, "symbol": "BTCUSDT", "output_sha256": digest,
            "trade_count_unit": "individual_execution", "source_data_type": "trades", "source_rows": 3,
            "source_sha256": "source", "bar_interval": "[t,t+1s)", "causal_visible_at": "t+1s"}
    record = {"day": day, "symbol": "BTCUSDT", "status": "VERIFIED", "readback_equal": True,
              "counts_and_last_event_verified": True, "output_sha256": digest,
              "trade_count_sum": 3, "builder_metadata": meta}
    proof = tmp_path / "proof.json"
    proof.write_text(json.dumps({"schema": "individual_reference_bars_calendar.v1", "status": "COMPLETED",
                                 "records": {day: record}}))
    monkeypatch.setenv("MM_REFERENCE_BARS_DIR", str(tmp_path))
    bound = engineer._reference_bar_manifest([day, "2026-01-03"], "BTCUSDT", proof)
    assert bound["daily_files"][0]["trade_count_unit"] == "individual_execution"
    assert bound["daily_files"][0]["sha256"] == digest
    assert bound["calendar_proof"]["sha256"] == engineer._sha256_file(proof)
    assert bound["missing_days"] == ["2026-01-03"]
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="evidence"):
        engineer._reference_bar_manifest([day], "BTCUSDT", proof)


def test_reference_manifest_missing_semantics_stays_unknown(tmp_path, monkeypatch):
    pd.DataFrame({"close": [100.]}).to_parquet(tmp_path / "BTCUSDT-1s-2026-01-02.parquet")
    monkeypatch.setenv("MM_REFERENCE_BARS_DIR", str(tmp_path))
    result = engineer._reference_bar_manifest(["2026-01-02"], "BTCUSDT")
    assert result["daily_files"][0]["trade_count_unit"] == "UNKNOWN"


def test_feature_spawn_worker_binds_parent_resolved_roots_even_for_existing_day(tmp_path, monkeypatch):
    from argparse import Namespace
    roots = {name: tmp_path / name for name in ("data", "bars", "metrics", "tempo")}
    target = tmp_path / "features_2026-01-02.parquet"
    target.write_bytes(b"existing synthetic file")
    for name in ("DATA_DIR", "BARS_DIR", "METRICS_DIR", "TRADE_FEATURE_DIR"):
        monkeypatch.setattr(engineer, name, tmp_path / "wrong-spawn-default")
    engineer._materialize_feature_day(
        tmp_path / "BTCUSDC-1s-2026-01-02.parquet", paths_by_day={}, out_dir=tmp_path,
        symbol="BTCUSDC", options=Namespace(verbose=False, force=False), config_path=None,
        market_stage="minimal", reference_symbol="BTCUSDT", sample_weight_reference_date="2026-01-02",
        calendar_days=("2026-01-02",), source_roots=roots,
    )
    assert engineer.METRICS_DIR == roots["metrics"]
    assert engineer.BARS_DIR == roots["bars"] and engineer.TRADE_FEATURE_DIR == roots["tempo"]


def test_metrics_warmup_reads_prior_days_but_not_future(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(engineer, "METRICS_DIR", tmp_path)
    for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
        pd.DataFrame({"sum_open_interest": [100.0]}, index=pd.date_range(day, periods=1, tz="UTC")).to_parquet(
            tmp_path / f"BTCUSDC-metrics-{day}.parquet",
        )
    result = engineer.load_metrics("2026-01-02", "BTCUSDC", start_day="2026-01-01")
    assert result.index.strftime("%Y-%m-%d").tolist() == ["2026-01-01", "2026-01-02"]
