from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
from pathlib import Path
from typing import Any

from models.cache_tier_lru import (
    DEFAULT_ALLOWED_CACHE_ROOTS,
    DEFAULT_COLD_ROOT,
    DEFAULT_HOT_ROOT,
    DEFAULT_LEDGER_PATH,
    CacheTierConfig,
    CacheTierError,
    apply_cache_tier_plan,
    build_cache_tier_plan,
    load_plan,
    record_cache_access,
    register_cache_write,
    scan_cache_tiers,
    validate_cache_tiers,
    validate_plan,
    write_plan,
)


def _json(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)


def _parse_reference(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("reference must use RELATIVE_PATH=CLASS")
    relative, reference_class = value.rsplit("=", 1)
    if not relative or not reference_class:
        raise argparse.ArgumentTypeError("reference must use RELATIVE_PATH=CLASS")
    return relative, reference_class


def _add_config_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hot-root", type=Path, default=DEFAULT_HOT_ROOT)
    parser.add_argument("--cold-root", type=Path, default=DEFAULT_COLD_ROOT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument(
        "--allow-root",
        action="append",
        default=None,
        help="Allowed top-level cache root; repeat to replace the default allowlist.",
    )
    parser.add_argument("--hot-safety-reserve-gib", type=int, default=60)
    parser.add_argument("--hot-target-free-gib", type=int, default=70)
    parser.add_argument("--cold-ttl-days", type=int, default=180)
    parser.add_argument("--symlink-mode", choices=("relative", "absolute"), default="relative")
    parser.add_argument(
        "--allow-unknown-migration",
        action="store_true",
        help="Explicitly permit transparent migration of unknown-reference cache entries.",
    )
    parser.add_argument("--lock-timeout-s", type=float, default=5.0)


def _config(args: argparse.Namespace) -> CacheTierConfig:
    allowed = tuple(args.allow_root or DEFAULT_ALLOWED_CACHE_ROOTS)
    return CacheTierConfig(
        hot_root=args.hot_root,
        cold_root=args.cold_root,
        ledger_path=args.ledger,
        hot_safety_reserve_bytes=args.hot_safety_reserve_gib * 1024**3,
        hot_target_free_bytes=args.hot_target_free_gib * 1024**3,
        cold_ttl_days=args.cold_ttl_days,
        allowed_cache_roots=allowed,
        symlink_mode=args.symlink_mode,
        allow_unknown_migration=args.allow_unknown_migration,
        lock_timeout_s=args.lock_timeout_s,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Govern NarrowGate hot/cold cache tiers with a fail-closed LRU plan."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("scan", help="Discover allowed cache artifacts into the ledger.")
    _add_config_arguments(scan)
    scan.add_argument(
        "--reference",
        action="append",
        type=_parse_reference,
        default=[],
        metavar="RELATIVE_PATH=CLASS",
    )
    scan.add_argument(
        "--reference-audit-csv",
        type=Path,
        help="Hash-bound reference audit CSV used to classify deletion authority.",
    )

    touch = subparsers.add_parser("touch", help="Record a cache access or completed write.")
    _add_config_arguments(touch)
    touch.add_argument("path", type=Path)
    touch.add_argument("--cache-root", type=Path)
    touch.add_argument("--identity-sha256")
    touch.add_argument("--reference-class", default="unknown")
    touch.add_argument("--write", action="store_true", help="Register a completed cache write.")
    touch.add_argument("--strict", action="store_true", help="Fail if ledger recording fails.")

    plan = subparsers.add_parser("plan", help="Freeze a dry-run migration/deletion plan.")
    _add_config_arguments(plan)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--operation", choices=("all", "migrate", "delete"), default="all")

    apply = subparsers.add_parser(
        "apply",
        help="Validate a plan; execute only with --execute and the operation-specific owner token.",
    )
    _add_config_arguments(apply)
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--operation", choices=("migrate", "delete"), required=True)
    apply.add_argument("--owner-token")
    apply.add_argument("--execute", action="store_true")
    apply.add_argument("--receipt", type=Path)

    validate = subparsers.add_parser("validate", help="Validate ledger, tiers, links, and plan.")
    _add_config_arguments(validate)
    validate.add_argument("--plan", type=Path)

    return parser


def _set_api_environment(config: CacheTierConfig) -> None:
    os.environ["NARROWGATE_CACHE_HOT_ROOT"] = str(config.hot_root)
    os.environ["NARROWGATE_CACHE_COLD_ROOT"] = str(config.cold_root)
    os.environ["NARROWGATE_CACHE_LEDGER_PATH"] = str(config.ledger_path)


def _record_to_dict(record: object) -> object:
    return dataclasses.asdict(record) if dataclasses.is_dataclass(record) else record


def run(args: argparse.Namespace) -> dict[str, Any]:
    config = _config(args)
    if args.command == "scan":
        return scan_cache_tiers(
            config,
            reference_classes=dict(args.reference),
            reference_audit_csv=args.reference_audit_csv,
        )
    if args.command == "touch":
        _set_api_environment(config)
        function = register_cache_write if args.write else record_cache_access
        record = function(
            args.path,
            cache_root=args.cache_root,
            identity_sha256=args.identity_sha256,
            reference_class=args.reference_class,
            strict=args.strict,
        )
        return {
            "operation": "register_cache_write" if args.write else "record_cache_access",
            "recorded": record is not None,
            "strict": args.strict,
            "record": _record_to_dict(record),
        }
    if args.command == "plan":
        plan = build_cache_tier_plan(
            config,
            include_migrations=args.operation in {"all", "migrate"},
            include_deletions=args.operation in {"all", "delete"},
        )
        write_plan(args.output, plan)
        return {
            "status": "dry_run_plan_frozen",
            "output": str(args.output),
            "plan_sha256": plan["plan_sha256"],
            "migration_count": len(plan["migrations"]),
            "deletion_count": len(plan["deletions"]),
            "owner_token_format": plan["authorization"]["token_format"],
        }
    if args.command == "apply":
        plan = load_plan(args.plan)
        return apply_cache_tier_plan(
            plan,
            config=config,
            operation=args.operation,
            owner_token=args.owner_token,
            execute=args.execute,
            receipt_path=args.receipt,
        )
    if args.command == "validate":
        result = validate_cache_tiers(config)
        if args.plan is not None:
            plan = load_plan(args.plan)
            validate_plan(plan, config)
            result["plan"] = {
                "path": str(args.plan),
                "plan_sha256": plan["plan_sha256"],
                "valid": True,
            }
        return result
    raise AssertionError(f"unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = run(args)
    except CacheTierError as error:
        print(
            _json(
                {
                    "status": "failed_closed",
                    "error_type": type(error).__name__,
                    "detail": str(error),
                }
            ),
            file=sys.stderr,
        )
        return 2
    print(_json(payload))
    return 0 if payload.get("valid", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
