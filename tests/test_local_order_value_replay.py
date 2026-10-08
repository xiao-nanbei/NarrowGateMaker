import json

import pandas as pd
import pyarrow as pa
import pytest

from research.families.f07_active_order_continuation.audit import local_order_value_replay as replay


def test_formal_quality_manifest_authorizes_target_and_warmup_only(
    tmp_path,
) -> None:
    manifest = tmp_path / "strict_days.csv"
    pd.DataFrame({"day": ["2026-05-07"]}).to_csv(manifest, index=False)

    allowed, identity = replay._formal_quality_context_identity(
        manifest,
        replay._sha256(manifest),
        target_days=["2026-05-07"],
        warmup_days=1,
    )

    assert allowed == ("2026-05-06", "2026-05-07")
    assert identity["target_days"] == ["2026-05-07"]
    assert identity["authority"] == (
        "frozen_native_strict_manifest_with_gap_censoring"
    )


def test_formal_quality_manifest_rejects_unlisted_day(tmp_path) -> None:
    manifest = tmp_path / "strict_days.csv"
    pd.DataFrame({"day": ["2026-05-07"]}).to_csv(manifest, index=False)

    with pytest.raises(ValueError, match="absent from the frozen formal"):
        replay._formal_quality_context_identity(
            manifest,
            replay._sha256(manifest),
            target_days=["2026-05-08"],
            warmup_days=1,
        )


def test_align_table_schema_reorders_same_fields_before_cast() -> None:
    schema = pa.schema([("left", pa.int64()), ("right", pa.float64())])
    table = pa.table(
        {
            "right": pa.array([2], type=pa.int32()),
            "left": pa.array([1], type=pa.int32()),
        }
    )

    aligned = replay._align_table_schema(
        table,
        schema,
        context="test",
    )

    assert aligned.schema == schema
    assert aligned.to_pydict() == {"left": [1], "right": [2.0]}


def test_lifecycle_worker_persists_partition_and_returns_compact_result(
    monkeypatch,
    tmp_path,
) -> None:
    item = {
        "day": "2026-01-02",
        "rows": [],
        "lifecycle_rows": [
            {
                "day": "2026-01-02",
                "order_id": 1,
                "event_type": "submit",
            }
        ],
        "quote_rows": [],
        "queue_missing_rows": [
            {
                "day": "2026-01-02",
                "order_id": 1,
                "reason": "missing",
            }
        ],
        "daily": {
            "day": "2026-01-02",
            "lifecycle_rows": 1,
            "runtime_s": 1.5,
        },
    }
    monkeypatch.setattr(replay, "_run_day", lambda _task: item)

    result = replay._run_lifecycle_day_to_partition(
        ("2026-01-02", "BTCUSDC", {}, str(tmp_path))
    )

    assert result["lifecycle_rows"] == []
    assert result["rows"] == []
    assert result["daily"]["lifecycle_rows"] == 1
    lifecycle = pd.read_parquet(tmp_path / "2026-01-02.lifecycle.parquet")
    assert lifecycle.to_dict("records") == item["lifecycle_rows"]
    queue_missing = pd.read_parquet(
        tmp_path / "2026-01-02.queue_missing.parquet"
    )
    assert queue_missing.to_dict("records") == item["queue_missing_rows"]
    assert json.loads(
        (tmp_path / "2026-01-02.daily.json").read_text(encoding="utf-8")
    ) == item["daily"]
    assert not list(tmp_path.glob(".*.tmp.*"))


def test_lifecycle_audit_worker_persists_and_reuses_partition(
    monkeypatch,
    tmp_path,
) -> None:
    lifecycle_path = tmp_path / "2026-01-02.lifecycle.parquet"
    pd.DataFrame([{"day": "2026-01-02", "order_id": 1}]).to_parquet(
        lifecycle_path,
        index=False,
    )
    intervals = pd.DataFrame(
        [{"day": "2026-01-02", "order_id": 1, "interval_ms": 1.0}]
    )
    monkeypatch.setattr(
        replay,
        "audit_lifecycle_events",
        lambda _frame, require_native_book: (
            intervals,
            {
                "rows": 1,
                "orders": 1,
                "risk_interval_rows": 1,
                "require_native_book": require_native_book,
            },
        ),
    )
    task = (
        "2026-01-02",
        str(lifecycle_path),
        str(tmp_path),
        True,
    )

    first = replay._audit_lifecycle_partition_to_disk(task)
    second = replay._audit_lifecycle_partition_to_disk(task)

    assert first["reused"] is False
    assert second["reused"] is True
    assert pd.read_parquet(first["risk_path"]).to_dict("records") == (
        intervals.to_dict("records")
    )
    assert second["audit"]["require_native_book"] is True
    assert second["audit"]["lifecycle_sha256"] == replay._sha256(
        lifecycle_path
    )
    assert not list(tmp_path.glob(".*.tmp.*"))
