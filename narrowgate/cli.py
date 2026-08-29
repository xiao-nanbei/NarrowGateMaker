"""Small public CLI for NarrowGate.

The CLI is deliberately thin: it points new users at safe demos and canonical
research runners without hiding the underlying modules.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import (  # noqa: E402
    cache_root,
    data_root,
    marketdata_root,
    window_cache_root,
)

REDACTED_PATH = "<redacted; run `narrowgate paths`>"


def _has_module(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:
        return False


def _environment_state(name: str) -> str:
    return "<set>" if os.environ.get(name) else "<unset>"


def cmd_doctor(_args: argparse.Namespace) -> int:
    """Print a compact environment report."""
    resolved_marketdata_root = marketdata_root()
    resolved_data_root = data_root(ROOT)
    resolved_cache_root = cache_root(ROOT)
    checks: dict[str, Any] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "root": REDACTED_PATH,
        "marketdata_root": REDACTED_PATH,
        "marketdata_root_exists": resolved_marketdata_root.is_dir(),
        "narrowgate_data_root": REDACTED_PATH,
        "narrowgate_data_root_exists": resolved_data_root.is_dir(),
        "narrowgate_cache_root": REDACTED_PATH,
        "narrowgate_cache_root_exists": resolved_cache_root.is_dir(),
        "tick_window_cache_root": REDACTED_PATH,
        "narrowgate_marketdata_root_env": _environment_state(
            "NARROWGATE_MARKETDATA_ROOT"
        ),
        "narrowgate_data_root_env": _environment_state("NARROWGATE_DATA_ROOT"),
        "narrowgate_cache_root_env": _environment_state("NARROWGATE_CACHE_ROOT"),
        "xdg_cache_home_env": _environment_state("XDG_CACHE_HOME"),
        "narrowgate_tick_window_cache_dir_env": _environment_state(
            "NARROWGATE_TICK_WINDOW_CACHE_DIR"
        ),
        "legacy_mm_data_root": _environment_state("MM_DATA_ROOT"),
        "narrowgate_live_config": _environment_state("NARROWGATE_LIVE_CONFIG"),
        "path_details_command": "narrowgate paths",
        "numpy": _has_module("numpy"),
        "pandas": _has_module("pandas"),
        "pyarrow": _has_module("pyarrow"),
        "lightgbm": _has_module("lightgbm"),
        "narrowgate_cpp": _has_module("narrowgate_cpp"),
    }
    print(json.dumps(checks, indent=2, sort_keys=True))
    return 0


def cmd_quote_demo(_args: argparse.Namespace) -> int:
    """Run a no-data quote-core demo."""
    from strategy.quote_core import (
        DepthSnapshot,
        QuoteCoreConfig,
        QuotePrediction,
        QuoteState,
        compute_quote_core,
    )

    result = compute_quote_core(
        QuoteState(
            mid=60_000.0,
            inventory=0.0,
            sigma_sq=4.0,
            best_bid=59_999.9,
            best_ask=60_000.1,
            trade_intensity=100.0,
        ),
        QuoteCoreConfig(
            gamma=0.01,
            kappa=1.0,
            tick_size=0.1,
            lot_size=0.001,
            maker_fee=0.0,
            order_size=0.001,
            max_inventory=0.01,
            max_spread_bps=20.0,
        ),
        QuotePrediction(dir_10s=0.5, vol_10s=2.0, ret_10s=0.0, tox_bid=0.5, tox_ask=0.5),
        DepthSnapshot(
            bids=((59_999.9, 1.2), (59_999.8, 2.0)),
            asks=((60_000.1, 1.1), (60_000.2, 2.2)),
        ),
    )
    print(
        json.dumps(
            {
                "bid_price": result.bid_price,
                "ask_price": result.ask_price,
                "spread": result.spread,
                "raw_half_spread": result.raw_half_spread,
                "raw_mid_shift": result.raw_mid_shift,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_paths(_args: argparse.Namespace) -> int:
    paths = {
        "repo_root": str(ROOT),
        "marketdata_root": str(marketdata_root()),
        "data_root": str(data_root(ROOT)),
        "cache_root": str(cache_root(ROOT)),
        "window_cache_root": str(window_cache_root(ROOT)),
        "results_dir": os.environ.get(
            "NARROWGATE_RESULTS_DIR",
            str(data_root(ROOT) / "backtest_results_btcusdc"),
        ),
        "public_config": str(ROOT / "live" / "config.yaml"),
        "private_config_env": os.environ.get("NARROWGATE_LIVE_CONFIG", "<unset>"),
    }
    print(json.dumps(paths, indent=2, sort_keys=True))
    return 0


def cmd_replay_demo(args: argparse.Namespace) -> int:
    """Run the deterministic, non-economic public replay demonstration."""
    from narrowgate import replay_demo

    forwarded: list[str] = []
    if args.output_dir is not None:
        forwarded.extend(("--output-dir", str(args.output_dir)))
    if args.contract is not None:
        forwarded.extend(("--contract", str(args.contract)))
    if args.verify_reference:
        forwarded.append("--verify-reference")
    return int(replay_demo.main(forwarded))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="narrowgate", description="NarrowGate public CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="print environment and optional dependency status")
    doctor.set_defaults(func=cmd_doctor)

    quote_demo = sub.add_parser("quote-demo", help="run a no-data quote-core demo")
    quote_demo.set_defaults(func=cmd_quote_demo)

    paths = sub.add_parser("paths", help="print resolved repo/data/config paths")
    paths.set_defaults(func=cmd_paths)

    replay_demo = sub.add_parser(
        "replay-demo",
        help="run the offline synthetic queue-to-campaign replay",
    )
    replay_demo.add_argument(
        "--output-dir",
        type=Path,
        help="directory for deterministic summary, trace, and receipt artifacts",
    )
    replay_demo.add_argument(
        "--contract",
        type=Path,
        help="alternate hash-bound synthetic fixture contract",
    )
    replay_demo.add_argument(
        "--verify-reference",
        action="store_true",
        help="require byte equality with the distributed reference output",
    )
    replay_demo.set_defaults(func=cmd_replay_demo)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))
