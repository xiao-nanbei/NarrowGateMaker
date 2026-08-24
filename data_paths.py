"""Shared data-root helpers for large market data outside the workspace."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

ENV_MARKETDATA_ROOT = "NARROWGATE_MARKETDATA_ROOT"
ENV_DATA_ROOT = "NARROWGATE_DATA_ROOT"
ENV_CACHE_ROOT = "NARROWGATE_CACHE_ROOT"
ENV_TICK_WINDOW_CACHE_DIR = "NARROWGATE_TICK_WINDOW_CACHE_DIR"
ENV_REPLAY_DAG_CACHE_DIR = "NARROWGATE_REPLAY_DAG_CACHE_DIR"
ENV_RESULTS_DIR = "NARROWGATE_RESULTS_DIR"
ENV_PRIVATE_EVIDENCE_ROOT = "NARROWGATE_PRIVATE_EVIDENCE_ROOT"
ENV_REMOTE_ROOT = "NARROWGATE_REMOTE_ROOT"
ENV_REMOTE_HOME = "NARROWGATE_REMOTE_HOME"
ENV_STORAGE_ROOT = "NARROWGATE_STORAGE_ROOT"
ENV_EPHEMERAL_ROOT = "NARROWGATE_EPHEMERAL_ROOT"
ENV_LIVE_CONFIG = "NARROWGATE_LIVE_CONFIG"
ENV_LIVE_REMOTE_POINTER = "NARROWGATE_LIVE_REMOTE_POINTER"
ENV_LIVE_ENV = "NARROWGATE_LIVE_ENV"
ENV_PRIVATE_CONFIG_ROOT = "NARROWGATE_PRIVATE_CONFIG_ROOT"
IMMUTABLE_BACKTEST_V12_CONFIG_FILENAME = "live_config.backtest_v12.800f4c025663.local.yaml"
IMMUTABLE_BACKTEST_V12_CONFIG_LOCATOR = (
    f"${{NARROWGATE_PRIVATE_CONFIG_ROOT}}/{IMMUTABLE_BACKTEST_V12_CONFIG_FILENAME}"
)
IMMUTABLE_BACKTEST_V12_CONFIG_SHA256 = (
    "800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85"
)
LEGACY_ENV_DATA_ROOT = "MM_DATA_ROOT"
PRIVATE_STORAGE_ROOTS_PATH = (
    Path(__file__).resolve().parent / "data/private/storage_roots.current.local.json"
)
PORTABLE_MARKETDATA_FALLBACK = Path.home() / "MarketData"
DEFAULT_MARKETDATA_ROOT = PORTABLE_MARKETDATA_FALLBACK
DEFAULT_CACHE_PARENT = Path.home() / "Library" / "Caches"
LEGACY_MARKETDATA_ROOT = Path.home() / "MarketData"
FROZEN_LEGACY_MARKETDATA_ROOTS: tuple[Path, ...] = ()
PROJECT_DATASET_NAME = "NarrowGate_BTCUSDC"
NORMALIZED_L2_DATASET = "normalized_l2_100ms_v2"
ROOT = Path(__file__).resolve().parent
PORTABLE_PATH_RE = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}(?:/(.*))?$")


def _private_storage_roots() -> dict[str, object]:
    """Load machine-specific roots from the ignored data-owner contract."""

    if not PRIVATE_STORAGE_ROOTS_PATH.is_file():
        return {}
    payload = json.loads(PRIVATE_STORAGE_ROOTS_PATH.read_text(encoding="utf-8"))
    if payload.get("visibility") != "local_only_do_not_publish":
        raise RuntimeError("private storage-root pointer has invalid visibility")
    return payload


def _private_path(name: str) -> Path | None:
    raw = _private_storage_roots().get(name)
    if not isinstance(raw, str) or not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise RuntimeError(f"private storage-root value must be absolute: {name}")
    return path.resolve(strict=False)


def legacy_marketdata_roots() -> tuple[Path, ...]:
    """Return configured historical roots used only for path relocation."""

    payload = _private_storage_roots()
    values = payload.get("legacy_marketdata_roots", [])
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise RuntimeError("legacy_marketdata_roots must be a list of absolute paths")
    roots = tuple(Path(value).expanduser().resolve(strict=False) for value in values)
    if any(not root.is_absolute() for root in roots):
        raise RuntimeError("legacy market-data roots must be absolute")
    return tuple(dict.fromkeys((LEGACY_MARKETDATA_ROOT, *FROZEN_LEGACY_MARKETDATA_ROOTS, *roots)))


def marketdata_root() -> Path:
    """Return the external data root without an internal-disk fallback."""

    env_root = os.environ.get(ENV_MARKETDATA_ROOT)
    if env_root:
        return Path(env_root).expanduser().resolve()
    return _private_path("marketdata_root") or DEFAULT_MARKETDATA_ROOT


def storage_root() -> Path:
    """Return the configured external storage mount or namespace root."""

    env_root = os.environ.get(ENV_STORAGE_ROOT)
    if env_root:
        return Path(env_root).expanduser().resolve()
    return _private_path("storage_root") or marketdata_root().parent


def default_external_data_root(root: Path | None = None) -> Path:
    """Default external data directory for a repo checkout."""
    # The data identity is stable even when GitHub checks the repository out
    # under its remote name (``NarrowGateMaker``).
    del root
    return marketdata_root() / PROJECT_DATASET_NAME


def data_root(root: Path | None = None) -> Path:
    """Return the project data root without falling back into the checkout."""
    env_root = os.environ.get(ENV_DATA_ROOT) or os.environ.get(LEGACY_ENV_DATA_ROOT)
    if env_root:
        # 训练/回测脚本依赖这个函数定位同一套日度容器；改环境变量时要确认
        # features/raw_trades/bbo/l2 都指向同一个 UTC 数据根。
        return Path(env_root).expanduser().resolve()

    return default_external_data_root(root)


def cache_root(root: Path | None = None) -> Path:
    """Return the configured root for disposable, reproducible artifacts.

    The default remains the internal disk. An explicit environment override
    may select a removable cache tier; callers must still fail closed when the
    selected volume is absent and must never fall back into the repository.
    """

    env_root = os.environ.get(ENV_CACHE_ROOT)
    if env_root:
        return Path(env_root).expanduser().resolve()
    del root
    return DEFAULT_CACHE_PARENT / PROJECT_DATASET_NAME


def window_cache_root(root: Path | None = None) -> Path:
    """Return the tick-replay cache root, honoring its legacy override."""

    env_root = os.environ.get(ENV_TICK_WINDOW_CACHE_DIR)
    if env_root:
        return Path(env_root).expanduser().resolve()
    return cache_root(root) / "window_cache"


def replay_dag_cache_root(root: Path | None = None) -> Path:
    """Return the configured root for reusable replay DAG artifacts."""

    env_root = os.environ.get(ENV_REPLAY_DAG_CACHE_DIR)
    if env_root:
        return Path(env_root).expanduser().resolve()
    return cache_root(root) / "replay_dag"


def external_cache_root(root: Path | None = None) -> Path:
    """Return the removable project cache namespace, separate from sources."""

    return data_root(root) / "cache"


def native_exchange_book_cache_root(root: Path | None = None) -> Path:
    """Return the strategy-independent native book-event cache root."""

    return replay_dag_cache_root(root) / "native_exchange_book_hour_v1"


def resolve_portable_path(path: Path | str, *, root: Path | None = None) -> Path:
    """Resolve one allowlisted public path placeholder without shell expansion.

    Public Specs use a small placeholder vocabulary so they remain readable on
    GitHub while private path resolution stays local. Unknown or embedded
    placeholders fail closed instead of being passed to ``Path`` literally.
    """

    raw = str(path)
    match = PORTABLE_PATH_RE.fullmatch(raw)
    if match is None:
        if "${" in raw:
            raise ValueError(f"unsupported or embedded portable path placeholder: {raw}")
        return Path(raw).expanduser()

    name, suffix = match.groups()
    repository_root = (root or ROOT).resolve()
    configured: dict[str, Path | None] = {
        "NARROWGATE_ROOT": repository_root,
        "NARROWGATE_MARKETDATA_ROOT": marketdata_root(),
        "NARROWGATE_DATA_ROOT": data_root(repository_root),
        "NARROWGATE_RETIRED_MARKETDATA_ROOT": marketdata_root(),
        "NARROWGATE_RETIRED_DATA_ROOT": data_root(repository_root),
        "NARROWGATE_CACHE_ROOT": cache_root(repository_root),
        "NARROWGATE_RESULTS_DIR": Path(
            os.environ.get(ENV_RESULTS_DIR, data_root(repository_root) / "backtest_results_btcusdc")
        ),
        "NARROWGATE_PRIVATE_EVIDENCE_ROOT": Path(
            os.environ.get(ENV_PRIVATE_EVIDENCE_ROOT, data_root(repository_root) / "reports")
        ),
        "NARROWGATE_STORAGE_ROOT": storage_root(),
        "NARROWGATE_LOCAL_HOME": Path.home(),
        "NARROWGATE_EPHEMERAL_ROOT": Path(
            os.environ.get(ENV_EPHEMERAL_ROOT, tempfile.gettempdir())
        ),
        "NARROWGATE_REMOTE_ROOT": Path(os.environ[ENV_REMOTE_ROOT])
        if os.environ.get(ENV_REMOTE_ROOT)
        else None,
        "NARROWGATE_REMOTE_HOME": Path(os.environ[ENV_REMOTE_HOME])
        if os.environ.get(ENV_REMOTE_HOME)
        else None,
        "NARROWGATE_LIVE_CONFIG": Path(
            os.environ.get(
                ENV_LIVE_CONFIG, repository_root / "docs/private/live_config.current.local.yaml"
            )
        ),
        "NARROWGATE_LIVE_REMOTE_POINTER": Path(
            os.environ.get(
                ENV_LIVE_REMOTE_POINTER,
                repository_root / "docs/private/live_remote.current.local.json",
            )
        ),
        "NARROWGATE_LIVE_ENV": Path(os.environ.get(ENV_LIVE_ENV, repository_root / "live/.env")),
        "NARROWGATE_PRIVATE_CONFIG_ROOT": Path(
            os.environ.get(ENV_PRIVATE_CONFIG_ROOT, repository_root / "docs/private")
        ),
    }
    if name not in configured:
        raise ValueError(f"unsupported portable path placeholder: {name}")
    base = configured[name]
    if base is None:
        raise RuntimeError(f"portable path placeholder requires private configuration: {name}")
    resolved = base.expanduser()
    if suffix:
        resolved = resolved / suffix
    return resolved.resolve(strict=False)


def immutable_backtest_v12_config_path(*, root: Path | None = None) -> Path:
    """Resolve the versioned v12 replay config, never the mutable live alias.

    This helper is a locator only. Current replay governance validates the
    bytes and owner/private-checkout availability in ``models.backtest_config``;
    frozen consumers additionally enforce their own exact SHA256 contract.
    """

    return resolve_portable_path(
        IMMUTABLE_BACKTEST_V12_CONFIG_LOCATOR,
        root=root,
    )


def relocate_marketdata_path(path: Path | str) -> Path:
    """Map a legacy ``~/MarketData`` provenance path onto the active volume.

    Frozen experiment specifications retain their original path strings and
    hashes. Consumers may call this helper at the filesystem boundary so a
    storage relocation does not rewrite frozen evidence bytes.
    """

    candidate = resolve_portable_path(path)
    legacy_roots = legacy_marketdata_roots()
    project_names = tuple(dict.fromkeys((PROJECT_DATASET_NAME, ROOT.name)))

    for legacy_root in legacy_roots:
        for project_name in project_names:
            legacy_window_cache = legacy_root / project_name / "window_cache"
            try:
                cache_relative = candidate.relative_to(legacy_window_cache)
            except ValueError:
                continue
            return window_cache_root(ROOT) / cache_relative

    for legacy_root in legacy_roots:
        try:
            relative = candidate.relative_to(legacy_root)
        except ValueError:
            continue
        return marketdata_root() / relative
    return candidate


def data_subdir(name: str, root: Path | None = None) -> Path:
    return data_root(root) / name


def normalized_l2_root(root: Path | None = None) -> Path:
    """Return the sole normalized 100 ms BBO/L2 dataset root."""

    return data_root(root) / NORMALIZED_L2_DATASET
