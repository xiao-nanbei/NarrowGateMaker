import argparse
import hashlib
import json

import pandas as pd

from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    _resolve_input_roots,
    calibrate,
    survival_curve,
    window_reaches,
)


def test_exact_tick_window_reaches_are_side_correct(tmp_path):
    day = "2026-01-01"
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    bbo = pd.DataFrame({
        "timestamp": [start, start + 10_000],
        "best_bid": [100.0, 100.0],
        "best_ask": [101.0, 101.0],
    })
    trades = pd.DataFrame({
        "price": [98.0, 104.0],
        "transact_time": [start + 1_000, start + 2_000],
        "is_buyer_maker": [True, False],
    })
    bbo_path = tmp_path / "bbo.parquet"
    trade_path = tmp_path / "trades.csv"
    bbo.to_parquet(bbo_path, index=False)
    trades.to_csv(trade_path, index=False)
    result = window_reaches(
        day=day,
        bbo_path=bbo_path,
        trade_path=trade_path,
        horizon_s=10.0,
        max_bbo_age_ms=20_000,
    )
    assert result["BUY"][0] == 2.0
    assert result["SELL"][0] == 3.0
    curve = survival_curve(result["BUY"], pd.Series([0.1, 2.0, 2.1]).to_numpy())
    # Four windows have a causally fresh book, but only the first is touched.
    # No-trade/no-touch windows remain in the opportunity denominator.
    assert curve.tolist() == [0.25, 0.25, 0.0]


def test_input_roots_default_to_normalized_v2_layout(tmp_path):
    args = argparse.Namespace(
        data_root=tmp_path,
        bbo_root=None,
        trade_root=None,
    )
    assert _resolve_input_roots(args) == (
        (tmp_path / "normalized_l2_100ms_v2" / "bbo").resolve(),
        (tmp_path / "raw").resolve(),
    )


def test_calibration_records_explicit_split_input_roots_and_hashes(tmp_path):
    data_root = tmp_path / "marketdata"
    bbo_root = tmp_path / "normalized_l2_100ms_v2" / "bbo"
    trade_root = data_root / "raw"
    bbo_root.mkdir(parents=True)
    trade_root.mkdir(parents=True)
    split_days = {
        "train": ["2026-01-01"],
        "validation": ["2026-01-02"],
        "test": ["2026-01-03"],
    }
    for day in sum(split_days.values(), []):
        start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
        pd.DataFrame({
            "timestamp": [start],
            "best_bid": [100.0],
            "best_ask": [101.0],
        }).to_parquet(bbo_root / f"BTCUSDC-bbo-{day}.parquet", index=False)
        pd.DataFrame({
            "price": [99.0, 102.0],
            "transact_time": [start + 1_000, start + 2_000],
            "is_buyer_maker": [True, False],
        }).to_csv(trade_root / f"BTCUSDC-aggTrades-{day}.csv", index=False)

    model_meta = tmp_path / "model_meta.json"
    model_meta.write_text(
        json.dumps({"feature_panel_split": split_days}),
        encoding="utf-8",
    )
    output = tmp_path / "fill_prob_params.json"
    report_json = tmp_path / "report.json"
    report = calibrate(argparse.Namespace(
        symbol="BTCUSDC",
        data_root=data_root,
        bbo_root=bbo_root,
        trade_root=trade_root,
        model_meta=model_meta,
        horizon_s=10.0,
        max_bbo_age_ms=5_000,
        distance_min=0.1,
        distance_max=0.3,
        distance_step=0.1,
        output=output,
        report_json=report_json,
        daily_csv=tmp_path / "daily.csv",
    ))

    manifest = report["input_manifest"]
    assert manifest["roots"] == {
        "bbo": str(bbo_root.resolve()),
        "trade": str(trade_root.resolve()),
    }
    assert manifest["file_counts"] == {"bbo": 3, "trade": 3, "total": 6}
    assert all(len(value) == 64 for value in manifest["hashes"].values())
    assert report["metadata"]["input_manifest"] == manifest
    assert (
        report["metadata"]["input_identity_sha256"]
        == manifest["hashes"]["combined_input_identity_sha256"]
    )

    persisted_report = json.loads(report_json.read_text(encoding="utf-8"))
    persisted_model = json.loads(output.read_text(encoding="utf-8"))
    assert persisted_report["input_manifest"] == manifest
    assert persisted_report["artifact_sha256"] == hashlib.sha256(
        output.read_bytes()
    ).hexdigest()
    assert persisted_report["event_type"] == "touch"
    assert persisted_report["horizon_s"] == 10.0
    assert persisted_report["distance_unit"] == "USDC_per_BTC"
    assert persisted_model["metadata"]["input_manifest"] == manifest
    assert persisted_model["metadata"]["event_type"] == "touch"
    assert persisted_model["metadata"]["horizon_s"] == 10.0
    assert persisted_model["metadata"]["distance_unit"] == "USDC_per_BTC"

    expected_file_hashes = {
        str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((*bbo_root.glob("*"), *trade_root.glob("*")))
    }
    assert {row["path"]: row["sha256"] for row in report["inputs"]} == expected_file_hashes
