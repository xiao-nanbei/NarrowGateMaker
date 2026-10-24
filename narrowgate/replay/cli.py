"""Stable plan/run/control commands; all execution is offline and local."""

import argparse
import json
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="narrowgate replay",
        epilog=(
            "Legacy --data-bundle/--data-replay-config/--summary-json "
            "remains a summary-only adapter."
        ),
    )
    sub = parser.add_subparsers(dest="action", required=True)
    plan = sub.add_parser("plan", help="resolve one explicit JSON plan; no economic replay")
    plan.add_argument("--config", type=Path, required=True)
    run = sub.add_parser("run", help="run one account in the foreground; retain complete artifacts")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--executor", choices=("local",), default="local")
    resume = sub.add_parser(
        "resume", help="restore the original account from its durable checkpoint"
    )
    resume.add_argument("--run", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "plan":
            from .plan import resolve

            value = resolve(args.config)
        elif args.action == "run":
            from .runs import run

            value = run(args.plan)
        else:
            from .runs import resume

            value = resume(args.run)
    except (ValueError, KeyError, OSError, TypeError) as exc:
        print(json.dumps({"status": "ERROR", "error": str(exc)}))
        return 2
    print(json.dumps(value, sort_keys=True))
    return 0


def runs_main(argv=None):
    parser = argparse.ArgumentParser(prog="narrowgate runs")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("status", "pause", "report"):
        command = sub.add_parser(name)
        command.add_argument("run", type=Path, help="existing run output directory")
    args = parser.parse_args(argv)
    from .runs import report, request_pause, status

    try:
        value = {"status": status, "pause": request_pause, "report": report}[args.action](args.run)
    except (ValueError, KeyError, OSError, TypeError) as exc:
        print(json.dumps({"status": "ERROR", "error": str(exc)}))
        return 2
    print(json.dumps(value, sort_keys=True))
    return 0
