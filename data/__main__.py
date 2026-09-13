"""One source-neutral command surface for the current data contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(prog="data", description="Acquire, inventory and normalize market data.")
    commands = parser.add_subparsers(dest="command", required=True)
    download = commands.add_parser("download", help="Resume the configured private delivery")
    download.add_argument("--config", type=Path, required=True)
    download.add_argument("--poll-interval", type=float)
    download.add_argument("--max-polls", type=int)
    inventory = commands.add_parser("inventory", help="Inventory every date without reading economic results")
    inventory.add_argument("--root", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    normalize = commands.add_parser("normalize", help="Materialize atomic facts from an explicit input plan")
    selection = normalize.add_mutually_exclusive_group(required=True)
    selection.add_argument("--plan", type=Path)
    selection.add_argument("--root", type=Path, help="Configured raw archive directory")
    normalize.add_argument("--start")
    normalize.add_argument("--end")
    normalize.add_argument("--symbol", action="append", choices=("BTCUSDC", "BTCUSDT"))
    normalize.add_argument("--channel", action="append", choices=("incremental_book_L2", "trades"))
    normalize.add_argument("--output", type=Path, required=True)
    normalize.add_argument("--row-group-size", type=int, default=8192)
    validate = commands.add_parser("validate", help="Read a bound fact bundle and verify continuous book states")
    validate.add_argument("--bundle", type=Path, required=True)
    validate.add_argument("--output", type=Path, required=True)
    calendar = commands.add_parser("build-calendar", help="Resume full-content daily conversion and boundary acceptance")
    calendar.add_argument("--root", type=Path, required=True)
    calendar.add_argument("--output", type=Path, required=True)
    calendar.add_argument("--start", required=True)
    calendar.add_argument("--end", required=True)
    calendar.add_argument("--symbol", action="append", choices=("BTCUSDC", "BTCUSDT"), required=True)
    calendar.add_argument("--workers", type=int, default=4)
    derive = commands.add_parser("derive", help="Build causal Bars, depth observations and model FeatureFrames")
    derive.add_argument("--plan", type=Path, required=True)
    derive.add_argument("--output", type=Path, required=True)
    acceptance = commands.add_parser("validate-calendar", help="Verify every full-scan output and retain all daily findings")
    acceptance.add_argument("--bundle", type=Path, required=True)
    acceptance.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "download":
        # Only the private resumable archive route is current. Never enter the
        # legacy downloader's implicit fusion/retirement or public URL defaults.
        from data.downloaders.tardis_archive import main as acquire
        forwarded = ["--delivery-config", str(args.config), "--archive-only"]
        for field in ("poll_interval", "max_polls"):
            value = getattr(args, field)
            if value is not None:
                forwarded += ["--" + field.replace("_", "-"), str(value)]
        return acquire(forwarded)
    from data.facts import calendar_plan, inventory_calendar, materialize, save_private_json, validate_bundle
    if args.command == "validate-calendar":
        from data.facts import validate_calendar
        result = validate_calendar(args.bundle)
        save_private_json(args.output, result)
        print(json.dumps({"status": result["status"], "dates": result["dates"], "totals": result["totals"]}))
        return 0
    if args.command == "derive":
        from data.runtime import derive_inputs
        result = derive_inputs(json.loads(args.plan.read_text()), args.output)
        print(json.dumps({"stats": result["stats"], "rows": {n: x["rows"] for n, x in result["files"].items()}}))
        return 0
    if args.command == "build-calendar":
        from data.facts import materialize_calendar
        result = materialize_calendar(args.root, args.output, start=args.start, end=args.end,
                                      symbols=args.symbol, workers=args.workers)
        return 0 if result["status"] == "full_content_scanned" else 2
    if args.command == "validate":
        result = validate_bundle(args.bundle)
        save_private_json(args.output, result)
        print(json.dumps({"status": result["status"], "files": len(result["files"])}))
        return 0
    if args.command == "inventory":
        result = inventory_calendar(args.root)
        save_private_json(args.output, result)
        print(json.dumps(result["summary"], sort_keys=True))
        return 0
    plan = args.plan
    if args.root:
        if not args.start or not args.end or not args.symbol:
            parser.error("normalization from --root requires --start, --end and --symbol")
        plan = calendar_plan(args.root, start=args.start, end=args.end, symbols=args.symbol,
                             channels=args.channel or ["incremental_book_L2", "trades"])
    elif args.start or args.end or args.symbol or args.channel:
        parser.error("a saved plan cannot be silently overridden by date/channel flags")
    result = materialize(plan, args.output, row_group_size=args.row_group_size)
    print(json.dumps({"status": result["status"], "files": len(result["files"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
