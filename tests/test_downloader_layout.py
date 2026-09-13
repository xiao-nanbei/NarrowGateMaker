"""Acquisition layout must preserve roots and explicit CLI routing."""

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

from data.__main__ import main
from pipeline import LEGACY_COMMANDS


ADAPTERS = (
    "binance_vision", "bitget_reference", "bybit_reference",
    "cryptohft_orderbook", "cryptohft_trades", "infoway",
    "okx_archive", "tardis_archive",
)


@pytest.mark.parametrize("name", ADAPTERS)
def test_adapter_import_root_and_cli_help(name):
    module = importlib.import_module(f"data.downloaders.{name}")
    root = Path(__file__).resolve().parents[1]
    if hasattr(module, "ROOT"):
        assert module.ROOT == root
    result = subprocess.run(
        [sys.executable, "-m", f"data.downloaders.{name}", "--help"],
        cwd=root, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout.lower()


def test_public_download_keeps_archive_only(monkeypatch, tmp_path):
    from data.downloaders import tardis_archive

    forwarded = []
    monkeypatch.setattr(tardis_archive, "main", lambda args: forwarded.extend(args) or 0)
    config = tmp_path / "private.json"
    assert main(["download", "--config", str(config)]) == 0
    assert forwarded == ["--delivery-config", str(config), "--archive-only"]


def test_explicit_legacy_acquisition_routes_use_downloaders():
    for name, (module, _) in LEGACY_COMMANDS.items():
        if name.startswith("download-"):
            assert module.startswith("data.downloaders.")


def test_historical_panel_binds_current_downloader_source():
    from research.families.f06_placement_fill_cif.audit.placement_fill_panel import (
        FROZEN_IMPLEMENTATION_PATHS,
    )
    from data.downloaders import cryptohft_orderbook

    # The dictionary key is a legacy contract label, not a filesystem locator.
    path = FROZEN_IMPLEMENTATION_PATHS["download_cryptohft_orderbook.py"]
    assert path.resolve() == Path(cryptohft_orderbook.__file__).resolve()
    assert path.is_file()
