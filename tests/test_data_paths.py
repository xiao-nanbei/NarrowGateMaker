from pathlib import Path

import data_paths


def test_marketdata_root_honors_explicit_environment(monkeypatch) -> None:
    monkeypatch.setenv("NARROWGATE_MARKETDATA_ROOT", "/tmp/narrowgate-marketdata")
    assert data_paths.marketdata_root() == Path("/tmp/narrowgate-marketdata").resolve()


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
        missing_marketdata / "NarrowGate_BTCUSDC"
    )


def test_relocate_legacy_marketdata_path(monkeypatch) -> None:
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
