"""Synthetic oracle regression: no private data."""
import pytest

from research.families.f08_side_taker_lifecycle.compare_scan_outputs import compare
from research.families.f08_side_taker_lifecycle.scan_perf import ScanPerf


@pytest.mark.parametrize("candidate", [{"a": [2, 1]}, {"a": [1]}, {"a": [1, 2.0]}, {"b": [1, 2]}])
def test_oracle_rejects_order_missing_and_types(candidate):
    with pytest.raises(AssertionError):
        compare({"a": [1, 2]}, candidate)


def test_exact_and_opt_in_timer():
    compare({"a": [1, None, 0.0]}, {"a": [1, None, 0.0]})
    perf = ScanPerf()
    with perf.measure("test"):
        pass
    assert perf.counters["test"] >= 0


def test_parquet_compression_not_values_or_row_order(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from research.families.f08_side_taker_lifecycle.compare_scan_outputs import compare_parquet
    table = pa.table({"identity": ["a", "b", "c"], "value": [None, 0.0, -1.0]})
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(table, a, compression="zstd")
    pq.write_table(table, b, compression="snappy")
    assert compare_parquet(a, b) == 3
    pq.write_table(table.take([2, 1, 0]), b)
    with pytest.raises(AssertionError):
        compare_parquet(a, b)
