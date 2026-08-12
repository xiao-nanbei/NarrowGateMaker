#!/usr/bin/env python3
"""Canonical NarrowGate data and feature pipeline.

This is the single public dispatcher for daily downloads, source imports,
data-quality audits, preprocessing, and feature engineering. Underlying
modules retain their own argparse contracts; this file only owns command
names and the ``features-all`` sequence.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_SYMBOL = "BTCUSDT" if ROOT.name.upper().endswith("BTCUSDT") else "BTCUSDC"

COMMANDS: dict[str, tuple[str, str]] = {
    "download-agg-trades": (
        "data.download_binance_vision",
        "Download Binance daily aggTrades",
    ),
    "download-raw-trades": (
        "data.download_binance_vision",
        "Download Binance USD-M daily raw trades",
    ),
    "download-metrics": (
        "data.download_binance_vision",
        "Download Binance USD-M daily metrics",
    ),
    "download-orderbook": (
        "data.download_cryptohft_orderbook",
        "Download and normalize CryptoHFT orderbook BBO/L2",
    ),
    "download-tardis": (
        "data.download_tardis_archive",
        "Plan and integrity-check a bounded one-off Tardis historical delivery",
    ),
    "audit-tardis": (
        "data.audit_tardis_archive",
        "Audit Tardis daily boundary and raw-repair readiness",
    ),
    "normalize-tardis": (
        "data.normalize_tardis_orderbook",
        "Build source-separated Tardis top-20/100ms books and quality audits",
    ),
    "freeze-research-days": (
        "data.build_research_day_universe",
        "Freeze a source-aware training/replay day universe",
    ),
    "prewarm-tick-cache": (
        "models.prewarm_tick_cache",
        "Prewarm source-bound C++ tick-replay windows on the internal disk",
    ),
    "source-aware-cpp-baseline": (
        "models.source_aware_cpp_baseline",
        "Run source-separated C++ core baseline and 13-head ML A/B",
    ),
    "import-bitget": (
        "data.import_bitget_archive",
        "Import Bitget archives into retained UTC days",
    ),
    "download-bitget": (
        "data.download_bitget_reference",
        "Download recent retained Bitget trades through public REST",
    ),
    "download-okx-archives": (
        "data.download_okx_archive",
        "Download retained OKX UTC+8 archive files",
    ),
    "import-okx": (
        "data.import_okx_archive",
        "Import OKX UTC+8 archives into retained UTC days",
    ),
    "download-bybit": (
        "data.download_bybit_reference",
        "Download retained Bybit reference trades",
    ),
    "external-features": (
        "data.build_external_reference_features",
        "Build causal daily external-venue 1s features",
    ),
    "external-consensus": (
        "research.families.f04_external_market_alpha.external_consensus_layer",
        "Build retained daily multi-venue consensus/reference",
    ),
    "audit-raw": (
        "data.audit_raw_trades",
        "Audit raw trades, BBO/L2, and bars coverage",
    ),
    "bars": ("features.preprocess", "Convert raw aggTrades CSV into 1s bars"),
    "preprocess-metrics": (
        "features.preprocess_metrics",
        "Convert raw daily metrics CSV into parquet",
    ),
    "engineer": (
        "features.feature_engineer",
        "Build model features and datasets",
    ),
}

ALIASES = {
    "agg-trades": "download-agg-trades",
    "raw-trades": "download-raw-trades",
    "metrics": "download-metrics",
    "orderbook": "download-orderbook",
    "bitget-import": "import-bitget",
    "bitget-reference": "download-bitget",
    "bybit-reference": "download-bybit",
    "okx-import": "import-okx",
    "preprocess": "bars",
    "features": "engineer",
}


def _print_help() -> None:
    print("usage: python pipeline.py <command> [args...]\n")
    print(__doc__.strip())
    print("\ncommands:")
    print("  features-all          Run bars, preprocess-metrics, then engineer")
    width = max(len(name) for name in (*COMMANDS, "features-all"))
    for name, (_, help_text) in COMMANDS.items():
        print(f"  {name:<{width}}  {help_text}")
    print("\naliases:")
    for alias, command in sorted(ALIASES.items()):
        print(f"  {alias:<{width}}  -> {command}")
    print("\nUse '<command> --help' for the underlying module options.")


def _run_module_main(module_name: str, argv: list[str]) -> int:
    if argv and argv[0] == "--":
        argv = argv[1:]
    module = importlib.import_module(module_name)
    main_func: Callable[[], object] | None = getattr(module, "main", None)
    if main_func is None:
        raise SystemExit(f"{module_name} does not expose main()")
    old_argv = sys.argv
    sys.argv = [f"{module_name.rsplit('.', 1)[-1]}.py", *argv]
    try:
        result = main_func()
        return int(result) if isinstance(result, int) else 0
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    finally:
        sys.argv = old_argv


def _append_if(args: list[str], flag: str, value: str | None) -> None:
    if value is not None:
        args.extend([flag, value])


def _run_or_exit(
    stage: str,
    module_name: str,
    argv: list[str],
    *,
    allow_failure: bool = False,
) -> None:
    print(f"\n=== {stage} ===")
    code = _run_module_main(module_name, argv)
    if code and allow_failure:
        print(f"[WARN] {stage} exited with {code}; continuing")
        return
    if code:
        raise SystemExit(code)


def run_features_all(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description="Run bars, metrics preprocessing, and feature engineering"
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--file", default=None)
    parser.add_argument("--market-type", choices=["perp", "spot"], default="perp")
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--market-stage",
        choices=["single", "minimal", "enhanced", "full"],
        default=None,
    )
    parser.add_argument("--reference-symbol", default=None)
    parser.add_argument("--lambda", type=float, default=None, dest="lam")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--strict-metrics", action="store_true")
    args = parser.parse_args(argv)

    common = ["--symbol", args.symbol]
    file_args: list[str] = []
    _append_if(file_args, "--file", args.file)
    verbose_args = ["--verbose"] if args.verbose else []
    bars_args = [
        *common,
        "--market-type",
        args.market_type,
        *file_args,
        *verbose_args,
    ]
    metrics_args = [
        *common,
        *file_args,
        *(["--overwrite"] if args.overwrite else []),
        *verbose_args,
    ]
    engineer_args = [
        *common,
        *file_args,
        *(["--force"] if args.force else []),
        *verbose_args,
    ]
    _append_if(engineer_args, "--config", args.config)
    _append_if(engineer_args, "--market-stage", args.market_stage)
    _append_if(engineer_args, "--reference-symbol", args.reference_symbol)
    if args.lam is not None:
        engineer_args.extend(["--lambda", str(args.lam)])

    _run_or_exit("bars", COMMANDS["bars"][0], bars_args)
    _run_or_exit(
        "preprocess-metrics",
        COMMANDS["preprocess-metrics"][0],
        metrics_args,
        allow_failure=not args.strict_metrics,
    )
    _run_or_exit("engineer", COMMANDS["engineer"][0], engineer_args)


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help", "help"}:
        _print_help()
        return
    command = ALIASES.get(args[0], args[0])
    if command == "features-all":
        run_features_all(args[1:])
        return
    if command not in COMMANDS:
        print(f"Unknown pipeline command: {args[0]}\n")
        _print_help()
        raise SystemExit(2)
    module_name, _ = COMMANDS[command]
    forwarded = list(args[1:])
    dataset_flags = {
        "download-agg-trades": ["--dataset", "aggTrades"],
        "download-raw-trades": ["--dataset", "trades"],
        "download-metrics": ["--dataset", "metrics"],
    }
    if command in dataset_flags:
        forwarded = [*dataset_flags[command], *forwarded]
    code = _run_module_main(module_name, forwarded)
    if code:
        raise SystemExit(code)


if __name__ == "__main__":
    main()
