from argparse import Namespace

import pandas as pd

from research.system_engineering.audit.rest_latency_calibration import calibrate


def test_rest_latency_profile_freezes_complete_days(tmp_path):
    source = tmp_path / "telemetry.csv"
    pd.DataFrame({
        "timestamp": [
            pd.Timestamp("2026-07-10T01:00:00Z").timestamp(),
            pd.Timestamp("2026-07-11T01:00:00Z").timestamp(),
            pd.Timestamp("2026-07-12T01:00:00Z").timestamp(),
        ],
        "rest_new_count": [2, 1, 1],
        "rest_new_sum_us": [20_000, 30_000, 90_000],
        "rest_new_max_us": [12_000, 30_000, 90_000],
        "rest_cancel_count": [1, 2, 1],
        "rest_cancel_sum_us": [15_000, 40_000, 80_000],
        "rest_cancel_max_us": [15_000, 25_000, 80_000],
    }).to_csv(source, index=False)
    args = Namespace(
        telemetry=source,
        replay_telemetry=tmp_path / "replay.csv.gz",
        output=tmp_path / "profile.json",
        start_day="2026-07-10",
        end_day="2026-07-11",
        recent_hours=3.0,
        profile_id="test",
        region="test",
        instance="test",
        os_label="test",
        cpu_label="test",
        memory_label="test",
        config_sha256="abc",
    )
    report = calibrate(args)
    assert report["fit_interval"]["rows"] == 2
    assert report["fit_distributions"]["avg"]["new"]["count"] == 2
    assert report["fit_distributions"]["avg"]["new"]["p50_ms"] == 20.0
    assert pd.read_csv(args.replay_telemetry).shape[0] == 2
