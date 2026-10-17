"""The CLI data inventory remains independent of the retired web interface."""

import pytest

from data.quality.local_inventory import observe_inventory


def test_exact_files_are_metadata_only_and_version_bound(tmp_path):
    path = tmp_path / "daily.parquet"
    path.write_bytes(b"not a parquet payload")
    inventory = {
        "node": "local",
        "audit_version": "v1",
        "files_by_day": {"2026-01-01": [{"path": str(path), "size_bytes": path.stat().st_size}]},
    }
    result = observe_inventory(inventory, "2026-01-01", "BTCUSDC", "v1")
    assert result["status"] == "present_unverified"
    assert result["audit_size_matched"] is True
    assert result["file_counts"] == {"expected": 1, "present": 1, "missing": 0}
    assert (
        observe_inventory(inventory, "2026-01-01", "BTCUSDC", "v2")["audit_size_matched"] is False
    )
    path.unlink()
    assert observe_inventory(inventory, "2026-01-01", "BTCUSDC", "v1")["status"] == "missing"


def test_remote_and_unavailable_roots_are_not_observed(tmp_path):
    assert observe_inventory({"node": "remote"}, "2026-01-01", "BTCUSDC", "v1") == {
        "reason": "remote_replica_not_observed",
    }
    result = observe_inventory(
        {"node": "local", "directory": str(tmp_path / "absent"), "pattern": "*.parquet"},
        "2026-01-01",
        "BTCUSDC",
        "v1",
    )
    assert result["status"] == "unknown"
    assert result["snapshot"] is None


@pytest.mark.parametrize(
    "pattern", ["**/*.parquet", "../*.parquet", "/tmp/*.parquet", "*/*.parquet"]
)
def test_rejects_unbounded_inventory_patterns(tmp_path, pattern):
    with pytest.raises(ValueError, match="bounded non-recursive"):
        observe_inventory(
            {"node": "local", "directory": str(tmp_path), "pattern": pattern},
            "2026-01-01",
            "BTCUSDC",
            "v1",
        )


def test_daily_inventory_count_and_absolute_path_limits(tmp_path):
    for paths in [["relative.parquet"], [str(tmp_path / f"{i}.parquet") for i in range(33)]]:
        with pytest.raises(ValueError):
            observe_inventory(
                {"node": "local", "files_by_day": {"2026-01-01": paths}},
                "2026-01-01",
                "BTCUSDC",
                "v1",
            )
