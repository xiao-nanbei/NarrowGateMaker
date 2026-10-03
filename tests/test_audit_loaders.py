from __future__ import annotations

import gzip

from models.audit.support import parse_ts, read_csv_rows, read_csv_table


def test_parse_ts_treats_naive_dates_as_utc():
    assert parse_ts("2026-07-14") == 1783987200.0
    assert parse_ts("2026-07-14T00:00:00") == 1783987200.0


def test_csv_loaders_accept_gzip(tmp_path):
    path = tmp_path / "orders.csv.gz"
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as f:
        f.write("timestamp,event_type\n")
        f.write("1784073600,placed\n")
        f.write("1784073601,filled\n")

    table = read_csv_table(path)
    assert [row["event_type"] for row in table] == ["placed", "filled"]

    rows = read_csv_rows(
        path,
        start_ts=1784073600.5,
        end_ts=1784073601.5,
    )
    assert len(rows) == 1
    assert rows[0]["event_type"] == "filled"
    assert rows[0]["_ts"] == 1784073601.0
