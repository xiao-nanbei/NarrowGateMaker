import json
from pathlib import Path

import data_paths
import pytest


def test_self_aggregate_path_is_derived_and_not_native_raw(tmp_path, monkeypatch):
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path / "products"))
    assert data_paths.daily_trade_aggregate_path("2026-01-01", "BTCUSDC") == (
        tmp_path / "products/binance_futures/BTCUSDC/2026-01-01/trade_aggregates_100ms.parquet"
    )
    assert data_paths.daily_trade_aggregate_path("2026-01-01", "BTCUSDC", tmp_path) == (
        tmp_path / "derived/binance_futures/BTCUSDC/2026-01-01/trade_aggregates_100ms.parquet"
    )
    with pytest.raises(ValueError, match="Unsupported daily raw"):
        data_paths.daily_market_path("2026-01-01", "BTCUSDC", "trade_aggregates_100ms", tmp_path)
    with pytest.raises(ValueError):
        data_paths.daily_trade_aggregate_path("2026-01-01", "../BTCUSDC", tmp_path)


def test_daily_market_paths_separate_raw_and_generated(tmp_path, monkeypatch):
    pointer = tmp_path / "roots.json"
    pointer.write_text(json.dumps({
        "visibility": "local_only_do_not_publish",
        "prepared_data_root": str(tmp_path / "derived"),
        "raw_data_root": str(tmp_path / "raw"),
    }))
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", pointer)
    monkeypatch.delenv("NARROWGATE_DATA_ROOT", raising=False)
    monkeypatch.delenv("MM_DATA_ROOT", raising=False)
    monkeypatch.delenv("NARROWGATE_RAW_DATA_ROOT", raising=False)
    assert data_paths.data_root() == tmp_path / "derived"
    assert data_paths.daily_market_path("2026-09-08", "BTCUSDC", "trades") == (
        tmp_path / "raw/binance_futures/BTCUSDC/2026-09-08/trades.parquet"
    )


def test_explicit_daily_root_and_relocation_without_symlinks(tmp_path, monkeypatch):
    assert data_paths.daily_market_path("2026-09-08", "BTCUSDC", "funding", tmp_path) == (
        tmp_path / "raw/binance_futures/BTCUSDC/2026-09-08/funding.parquet"
    )
    pointer = tmp_path / "roots.json"
    pointer.write_text(json.dumps({
        "visibility": "local_only_do_not_publish",
        "path_prefix_relocations": {str(tmp_path / "old"): str(tmp_path / "derived")},
    }))
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", pointer)
    assert data_paths.relocate_marketdata_path(tmp_path / "old/bars/day.parquet") == (
        tmp_path / "derived/bars/day.parquet"
    )


def test_marketdata_root_honors_explicit_environment(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", "/tmp/narrowgate-marketdata")
    assert data_paths.marketdata_root() == Path("/tmp/narrowgate-marketdata").resolve()


def test_default_roots_are_siblings_and_derived_override_does_not_rebind_raw(tmp_path, monkeypatch):
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", tmp_path / "missing.json")
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", str(tmp_path / "MarketData"))
    for name in ("NARROWGATE_DATA_ROOT", "MM_DATA_ROOT", "NARROWGATE_RAW_DATA_ROOT"):
        monkeypatch.delenv(name, raising=False)
    workspace = tmp_path / "MarketData/NarrowGate_BTCUSDC"
    assert data_paths.data_root() == workspace / "derived"
    assert data_paths.raw_data_root() == workspace / "raw"
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path / "other-products"))
    assert data_paths.data_root() == tmp_path / "other-products"
    assert data_paths.raw_data_root() == workspace / "raw"


def test_legacy_volume_then_physical_split_is_resolved_once(tmp_path, monkeypatch):
    current = tmp_path / "volume"
    old = tmp_path / "old-volume"
    pointer = tmp_path / "roots.json"
    pointer.write_text(json.dumps({
        "visibility": "local_only_do_not_publish",
        "legacy_marketdata_roots": [str(old)],
        "path_prefix_relocations": {
            str(current / "NarrowGate_BTCUSDC/bars_1s"):
                str(current / "NarrowGate_BTCUSDC/derived/bars_1s"),
        },
    }))
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", pointer)
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", str(current))
    assert data_paths.relocate_marketdata_path(old / "NarrowGate_BTCUSDC/bars_1s/day.parquet") == (
        current / "NarrowGate_BTCUSDC/derived/bars_1s/day.parquet"
    )
    raw = current / "NarrowGate_BTCUSDC/raw/binance_futures/BTCUSDC/2026-09-08/trades.parquet"
    assert data_paths.relocate_marketdata_path(raw) == raw


def test_portable_raw_root_is_independent_of_derived(tmp_path, monkeypatch):
    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path / "derived"))
    assert data_paths.resolve_portable_path("${NARROWGATE_RAW_DATA_ROOT}/binance_futures") == (
        tmp_path / "raw/binance_futures"
    )


def test_marketdata_root_uses_ignored_owner_pointer(monkeypatch, tmp_path: Path) -> None:
    pointer = tmp_path / "storage-roots.json"
    pointer.write_text(
        '{"visibility":"local_only_do_not_publish",'
        '"marketdata_root":"/srv/narrowgate-marketdata",'
        '"legacy_marketdata_roots":[]}\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("NARROWGATE_MARKETDATA_ROOT", raising=False)
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", pointer)

    assert data_paths.marketdata_root() == Path("/srv/narrowgate-marketdata")


def test_data_root_honors_current_and_legacy_environment(monkeypatch) -> None:
    monkeypatch.setenv("MM_DATA_ROOT", "/tmp/legacy-data-root")
    assert data_paths.data_root() == Path("/tmp/legacy-data-root").resolve()

    monkeypatch.setenv("NARROWGATE_DATA_ROOT", "/tmp/current-data-root")
    assert data_paths.data_root() == Path("/tmp/current-data-root").resolve()


def test_cache_roots_are_independent_from_external_data(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", "/srv/removable/project-data")
    monkeypatch.setenv("NARROWGATE_CACHE_ROOT", "/tmp/narrowgate-cache")

    assert data_paths.data_root() == Path("/srv/removable/project-data")
    assert data_paths.cache_root() == Path("/tmp/narrowgate-cache").resolve()
    assert data_paths.window_cache_root() == Path(
        "/tmp/narrowgate-cache/window_cache"
    ).resolve()
    assert data_paths.replay_dag_cache_root() == Path(
        "/tmp/narrowgate-cache/replay_dag"
    ).resolve()
    assert data_paths.native_exchange_book_cache_root() == Path(
        "/tmp/narrowgate-cache/replay_dag/native_exchange_book_hour_v1"
    ).resolve()


def test_cache_root_uses_xdg_cache_home(monkeypatch, tmp_path: Path) -> None:
    xdg_cache_home = tmp_path / "xdg-cache"
    monkeypatch.delenv("NARROWGATE_CACHE_ROOT", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(xdg_cache_home))

    assert data_paths.cache_root() == (
        xdg_cache_home / data_paths.PROJECT_DATASET_NAME
    ).resolve()


def test_cache_root_falls_back_to_dot_cache(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / "home"
    monkeypatch.delenv("NARROWGATE_CACHE_ROOT", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("HOME", str(home))

    assert data_paths.cache_root() == home / ".cache" / data_paths.PROJECT_DATASET_NAME


def test_tick_window_cache_honors_specific_override(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_CACHE_ROOT", "/tmp/narrowgate-cache")
    monkeypatch.setenv(
        "NARROWGATE_TICK_WINDOW_CACHE_DIR",
        "/tmp/narrowgate-window-cache",
    )

    assert data_paths.window_cache_root() == Path(
        "/tmp/narrowgate-window-cache"
    ).resolve()


def test_replay_dag_cache_honors_specific_override(monkeypatch) -> None:
    monkeypatch.setenv(
        "NARROWGATE_REPLAY_DAG_CACHE_DIR",
        "/tmp/narrowgate-replay-dag",
    )

    assert data_paths.replay_dag_cache_root() == Path(
        "/tmp/narrowgate-replay-dag"
    ).resolve()


def test_data_root_does_not_fall_back_when_external_volume_is_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    missing_marketdata = tmp_path / "missing-volume" / "MarketData"
    monkeypatch.delenv("NARROWGATE_MARKETDATA_ROOT", raising=False)
    monkeypatch.delenv("NARROWGATE_DATA_ROOT", raising=False)
    monkeypatch.delenv("MM_DATA_ROOT", raising=False)
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", tmp_path / "missing.json")
    monkeypatch.setattr(data_paths, "DEFAULT_MARKETDATA_ROOT", missing_marketdata)

    assert data_paths.data_root(Path("/tmp/NarrowGate_BTCUSDC")) == (
        missing_marketdata / "NarrowGate_BTCUSDC/derived"
    )


def test_relocate_legacy_marketdata_path(monkeypatch, tmp_path) -> None:
    # Test generic root relocation without the owner's explicit file moves.
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", tmp_path / "missing.json")
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", "/srv/current-marketdata")
    legacy = data_paths.LEGACY_MARKETDATA_ROOT / "NarrowGate_BTCUSDC" / "reports"
    assert data_paths.relocate_marketdata_path(legacy) == Path(
        "/srv/current-marketdata/NarrowGate_BTCUSDC/reports"
    )


def test_relocate_legacy_window_cache_to_internal_cache(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_CACHE_ROOT", "/tmp/narrowgate-cache")
    monkeypatch.delenv("NARROWGATE_TICK_WINDOW_CACHE_DIR", raising=False)
    legacy = (
        data_paths.LEGACY_MARKETDATA_ROOT
        / "NarrowGate_BTCUSDC"
        / "window_cache"
        / "day.pkl"
    )

    assert data_paths.relocate_marketdata_path(legacy) == Path(
        "/tmp/narrowgate-cache/window_cache/day.pkl"
    ).resolve()


def test_relocate_frozen_other_host_marketdata_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", "/tmp/current-marketdata")
    private_pointer = tmp_path / "storage-roots.json"
    private_pointer.write_text(
        '{"visibility":"local_only_do_not_publish",'
        '"legacy_marketdata_roots":["/srv/retired-user/MarketData"]}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(data_paths, "PRIVATE_STORAGE_ROOTS_PATH", private_pointer)
    frozen = Path("/srv/retired-user/MarketData/NarrowGate_BTCUSDC/reports/report.json")

    assert data_paths.relocate_marketdata_path(frozen) == Path(
        "/tmp/current-marketdata/NarrowGate_BTCUSDC/reports/report.json"
    ).resolve()


def test_relocate_leaves_unrelated_path_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", "/srv/current-marketdata")
    path = Path("/tmp/unrelated")
    assert data_paths.relocate_marketdata_path(path) == path


def test_resolve_portable_public_paths(monkeypatch, tmp_path: Path) -> None:
    marketdata = tmp_path / "marketdata"
    data = marketdata / "NarrowGate_BTCUSDC"
    cache = tmp_path / "cache"
    private_config = tmp_path / "private-config"
    private_research = tmp_path / "private-research"
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", str(marketdata))
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(data))
    monkeypatch.setenv("NARROWGATE_CACHE_ROOT", str(cache))
    monkeypatch.setenv("NARROWGATE_PRIVATE_CONFIG_ROOT", str(private_config))
    monkeypatch.setenv("NARROWGATE_PRIVATE_RESEARCH_ROOT", str(private_research))

    assert data_paths.resolve_portable_path(
        "${NARROWGATE_DATA_ROOT}/reports/result.json"
    ) == (data / "reports/result.json").resolve()
    assert data_paths.resolve_portable_path(
        "${NARROWGATE_RETIRED_DATA_ROOT}/raw/file.csv"
    ) == (data / "raw/file.csv").resolve()
    assert data_paths.relocate_marketdata_path(
        "${NARROWGATE_MARKETDATA_ROOT}/tardis/manifest.json"
    ) == (marketdata / "tardis/manifest.json").resolve()
    assert data_paths.resolve_portable_path(
        "${NARROWGATE_CACHE_ROOT}/window.pkl"
    ) == (cache / "window.pkl").resolve()
    assert data_paths.resolve_portable_path(
        "${NARROWGATE_PRIVATE_CONFIG_ROOT}/historical.yaml"
    ) == (private_config / "historical.yaml").resolve()
    assert data_paths.resolve_portable_path(
        "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/historical.json"
    ) == (private_research / "historical.json").resolve()


def test_resolve_portable_path_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("NARROWGATE_REMOTE_ROOT", raising=False)
    monkeypatch.delenv("NARROWGATE_PRIVATE_RESEARCH_ROOT", raising=False)
    try:
        data_paths.resolve_portable_path("${NARROWGATE_REMOTE_ROOT}/logs")
    except RuntimeError as exc:
        assert "requires private configuration" in str(exc)
    else:
        raise AssertionError("missing private remote root must fail closed")

    try:
        data_paths.resolve_portable_path(
            "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/historical.json"
        )
    except RuntimeError as exc:
        assert "requires private configuration" in str(exc)
    else:
        raise AssertionError("missing private research root must fail closed")

    try:
        data_paths.resolve_portable_path("prefix/${NARROWGATE_DATA_ROOT}/file")
    except ValueError as exc:
        assert "embedded" in str(exc)
    else:
        raise AssertionError("embedded placeholder must fail closed")
