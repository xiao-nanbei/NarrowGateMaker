"""Quota-bounded Infoway samples, deliberately separate from replay inputs.

Public contract: https://docs.infoway.io/rest-api/http-endpoints
One invocation makes at most one authenticated request. No pagination, retries,
WebSocket subscriptions, exchange inference, normalization or gap filling.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://data.infoway.io"
INTERVALS = {"1m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "1d": 8}


def request_plan(channel: str, code: str, *, interval=None, count=None, end=None) -> dict:
    """Build an offline plan. Product identifiers are not exchange identities."""
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,63}", code):
        raise ValueError("Select one exact ASCII crypto product code (no batches)")
    plan = {
        "provider": "infoway",
        "channel": channel,
        "code": code,
        "method": "GET",
        "params": None,
        "json": None,
        "request_count_ceiling": 1,
        "automatic_retries": 0,
        "venue": "UNKNOWN",
        "contract": "UNVERIFIED",
        "replay_eligible": False,
        "account_quota_remaining": "UNKNOWN",
    }
    if channel == "candles":
        if interval not in INTERVALS or not isinstance(count, int) or not 1 <= count <= 500:
            raise ValueError("Candles require --interval and --count in 1..500")
        if not end:
            raise ValueError("Candles require an explicit timezone-aware --end")
        dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        if dt.utcoffset() is None or dt.microsecond or dt.timestamp() <= 0:
            raise ValueError("--end requires a positive whole-second timestamp with timezone")
        plan.update(
            method="POST",
            url=f"{BASE_URL}/crypto/v2/batch_kline",
            json={
                "codes": code,
                "klineType": INTERVALS[interval],
                "klineNum": count,
                "timestamp": int(dt.timestamp()),
            },
        )
    elif channel in {"info", "depth", "trade"}:
        if any(v is not None for v in (interval, count, end)):
            raise ValueError("Only candles support --interval, --count and --end")
        if channel == "info":
            plan.update(
                url=f"{BASE_URL}/common/basic/symbols/info",
                params={"type": "CRYPTO", "symbols": code},
            )
        else:
            plan["url"] = f"{BASE_URL}/crypto/batch_{channel}/{code}"
    else:
        raise ValueError("Unsupported channel; historical depth/trades are not documented")
    return plan


def _create_json(path: Path, value: dict) -> bytes:
    content = (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as out:
        out.write(content)
        out.flush()
        os.fsync(out.fileno())
    return content


def _reserve_request(budget_file: Path, max_requests: int) -> int:
    """Durably count attempts before dispatch, including timeouts and failures.

    The shared append-only ledger bounds this tool's attempts, not provider credits
    or other applications. Corrupt/interrupted records stop instead of resetting.
    """
    if max_requests < 1:
        raise ValueError("--max-requests must be positive")
    fd = os.open(budget_file, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+", encoding="utf-8") as out:
        fcntl.flock(out, fcntl.LOCK_EX)
        records = [json.loads(line) for line in out]
        for i, row in enumerate(records, 1):
            if row.get("attempt") != i or not isinstance(row.get("dispatched_at"), (int, float)):
                raise ValueError("Invalid budget ledger; do not reset it")
        used = len(records)
        if used >= max_requests:
            raise ValueError("Local request budget exhausted; no request sent")
        if records:
            time.sleep(max(0, 1.1 - (time.time() - records[-1]["dispatched_at"])))
        out.write(json.dumps({"attempt": used + 1, "dispatched_at": time.time()}) + "\n")
        out.flush()
        os.fsync(out.fileno())
        return used + 1


def validate_payload(payload: dict, plan: dict) -> dict:
    """Validate attribution only; preserve provider values without dedup/fill."""
    if not isinstance(payload, dict) or payload.get("ret") != 200:
        raise ValueError("Provider rejected request; no retry (check key, entitlement or quota)")
    data = payload.get("data")
    key = "symbol" if plan["channel"] == "info" else "s"
    if (
        not isinstance(data, list)
        or len(data) != 1
        or not isinstance(data[0], dict)
        or data[0].get(key) != plan["code"]
    ):
        raise ValueError("Missing, extra or mismatched product in response; no admission")
    summary = {"items": len(data), "coverage": "UNKNOWN", "source_clock_verified": False}
    if plan["channel"] == "candles":
        rows = data[0].get("respList")
        if not isinstance(rows, list) or len(rows) > plan["json"]["klineNum"]:
            raise ValueError("Invalid candle count")
        times = [int(row["t"]) for row in rows]
        if any(ts <= 0 or ts > plan["json"]["timestamp"] for ts in times):
            raise ValueError("Candle timestamps exceed requested cutoff or have wrong units")
        summary.update(
            rows=len(rows),
            min_provider_timestamp_s=min(times, default=None),
            max_provider_timestamp_s=max(times, default=None),
            duplicate_timestamp_rows=len(times) - len(set(times)),
        )
    return summary


def download_sample(
    plan: dict, *, api_key: str, output_dir: Path, budget_file: Path, max_requests: int = 1
) -> dict:
    if not api_key or any(ch.isspace() for ch in api_key):
        raise ValueError("Set INFOWAY_API_KEY locally; never pass it on the command line")
    if max_requests < 1:
        raise ValueError("--max-requests must be positive")
    # Restrict credential delivery to the documented provider endpoints.
    allowed = {
        f"{BASE_URL}/common/basic/symbols/info",
        f"{BASE_URL}/crypto/v2/batch_kline",
        f"{BASE_URL}/crypto/batch_depth/{plan['code']}",
        f"{BASE_URL}/crypto/batch_trade/{plan['code']}",
    }
    if plan["url"] not in allowed:
        raise ValueError("Untrusted endpoint")
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    _create_json(output_dir / "request.json", plan)
    attempt = _reserve_request(budget_file, max_requests)
    started = datetime.now(timezone.utc).isoformat()
    http_status = None
    provider_ret = None
    try:
        with requests.Session() as session:
            session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            with session.request(
                plan["method"],
                plan["url"],
                params=plan["params"],
                json=plan["json"],
                headers={"apiKey": api_key},
                timeout=(10, 30),
                allow_redirects=False,
            ) as response:
                http_status = response.status_code
                if response.status_code != 200:
                    raise ValueError(f"HTTP {response.status_code}; no retry or redirect")
                payload = response.json()
                received = datetime.now(timezone.utc).isoformat()
                if isinstance(payload, dict) and type(payload.get("ret")) is int:
                    provider_ret = payload["ret"]
        # Do not persist reflected credentials, including in provider error bodies.
        if api_key in json.dumps(payload, ensure_ascii=False):
            raise ValueError("Response unexpectedly contains credentials; not saved")
        summary = validate_payload(payload, plan)
        content = _create_json(output_dir / "response.json", payload)
        receipt = {
            "status": "SAVED_NOT_ADMITTED",
            "provider": "infoway",
            "channel": plan["channel"],
            "code": plan["code"],
            "attempt": attempt,
            "request_started_utc": started,
            "response_received_utc": received,
            "response_sha256": hashlib.sha256(content).hexdigest(),
            "replay_eligible": False,
            **summary,
        }
        _create_json(output_dir / "receipt.json", receipt)
        return receipt
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        # Exceptions/messages can contain secrets; save only class and numeric codes.
        _create_json(
            output_dir / "failure.json",
            {
                "status": "FAILED_OR_UNKNOWN",
                "attempt": attempt,
                "error_type": type(exc).__name__,
                "automatic_retry": False,
                "http_status": http_status,
                "provider_ret": provider_ret,
            },
        )
        raise ValueError(
            "Infoway request failed/unknown; attempt consumed locally; no retry"
        ) from None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("channel", choices=("info", "candles", "depth", "trade"))
    parser.add_argument("--code", required=True, help="One exact provider crypto code")
    parser.add_argument("--interval", choices=INTERVALS)
    parser.add_argument("--count", type=int, help="Candles only, 1..500; no implicit bulk range")
    parser.add_argument("--end", help="Candles only, explicit ISO timestamp including timezone")
    parser.add_argument("--execute", action="store_true", help="Spend at most one API request")
    parser.add_argument("--output-dir", type=Path, help="New private source-sample directory")
    parser.add_argument("--budget-file", type=Path, help="Reuse the SAME ledger across all calls")
    parser.add_argument(
        "--max-requests", type=int, default=1, help="Total attempt cap in this ledger"
    )
    args = parser.parse_args(argv)
    try:
        plan = request_plan(
            args.channel, args.code, interval=args.interval, count=args.count, end=args.end
        )
        if not args.execute:
            print(json.dumps({"status": "PREVIEW_NO_NETWORK", **plan}, indent=2))
            return 0
        if args.output_dir is None or args.budget_file is None:
            raise ValueError("--execute requires --output-dir and --budget-file")
        receipt = download_sample(
            plan,
            api_key=os.environ.get("INFOWAY_API_KEY", ""),
            output_dir=args.output_dir,
            budget_file=args.budget_file,
            max_requests=args.max_requests,
        )
        print(json.dumps(receipt, indent=2))
        return 0
    except (ValueError, OSError) as exc:
        print(f"Stopped: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
