"""Bound a Shanghai research subprocess to the owner's daytime window.

Use for reference derivation, panel construction and fitting on the research
host. A transfer receiver must never invoke this automatically at night.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
from datetime import datetime, time
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def remaining_seconds(now: datetime | None = None, *, minimum_seconds: int) -> float:
    if type(minimum_seconds) is not int or minimum_seconds <= 0:
        raise ValueError("positive minimum daylight budget required")
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None:
        raise ValueError("aware Shanghai clock required")
    local = now.astimezone(SHANGHAI)
    end = datetime.combine(local.date(), time(22), tzinfo=SHANGHAI)
    remaining = (end - local).total_seconds()
    if local.time() < time(8) or remaining <= minimum_seconds + 60:
        raise RuntimeError("Shanghai research compute is outside its admitted daytime budget")
    return remaining


def run_daylight(command: list[str], *, minimum_seconds: int) -> int:
    """Kill the entire study process group before night; never auto-retry."""
    if not command or any(not item for item in command):
        raise ValueError("an explicit nonempty command is required")
    remaining = remaining_seconds(minimum_seconds=minimum_seconds)
    process = subprocess.Popen(command, start_new_session=True)
    try:
        return process.wait(timeout=remaining - 60)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        raise TimeoutError("Shanghai F04 compute stopped before 22:00; inspect partial output") from exc


def admit_worker(*, minimum_seconds: int) -> float:
    """Recheck at the actual compute process, including direct worker entry."""
    remaining = remaining_seconds(minimum_seconds=minimum_seconds)

    def cutoff(*_args):
        raise TimeoutError("Shanghai F04 worker reached the 22:00 compute cutoff")

    signal.signal(signal.SIGALRM, cutoff)
    signal.setitimer(signal.ITIMER_REAL, max(1, remaining - 60))
    return remaining


def main() -> None:
    parser = argparse.ArgumentParser(description="Daylight-only bounded F04 study launcher")
    parser.add_argument("--minimum-seconds", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    raise SystemExit(run_daylight(command, minimum_seconds=args.minimum_seconds))


if __name__ == "__main__":
    main()
