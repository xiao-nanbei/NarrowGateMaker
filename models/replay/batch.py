"""Bounded spawn workers; descriptors in, per-task files out, no trace IPC."""

import argparse
import hashlib
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import time

_prepared_key = None
_prepared = None


def run_task(task):
    global _prepared_key, _prepared
    from models.backtest_tick import prepare_public_inputs
    from models.replay.benchmark import measure

    params = json.loads(Path(task["params"]).read_text())
    key = (
        task["bundle"],
        params["tick_size"],
        task["cache"],
        hashlib.sha256((Path(task["bundle"]) / "manifest.json").read_bytes()).hexdigest(),
    )
    start = time.perf_counter()
    if key != _prepared_key:
        _prepared = None  # Release previous shard before loading the next.
        _prepared = prepare_public_inputs(
            task["bundle"], tick_size=params["tick_size"], cache_dir=task["cache"]
        )
        _prepared_key = key
    preparation = time.perf_counter() - start
    out = Path(task["output"])
    out.mkdir(parents=True, exist_ok=False)
    funding = json.loads(Path(task["funding"]).read_text()) if task.get("funding") else None
    row, result = measure(
        task["bundle"],
        params,
        model=task.get("model"),
        funding=funding,
        prepared=_prepared,
        result_path=out / "result.json",
    )
    row["preparation_seconds"] = preparation
    row["task_id"] = task["id"]
    (out / "metrics.json").write_text(json.dumps(row, indent=2))
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tasks", type=Path, required=True)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    if a.output.exists():
        p.error("summary output already exists")
    if (
        not 1
        <= a.workers
        <= len(
            os.sched_getaffinity(0)
            if hasattr(os, "sched_getaffinity")
            else range(os.cpu_count() or 1)
        )
    ):
        p.error("workers must fit the actual CPU affinity; memory admission remains caller-owned")
    tasks = json.loads(a.tasks.read_text())
    if len({t["id"] for t in tasks}) != len(tasks) or len(
        {str(Path(t["output"]).resolve()) for t in tasks}
    ) != len(tasks):
        p.error("duplicate task IDs or output paths")
    for key in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[key] = "1"
    start = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=a.workers, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        rows = list(pool.map(run_task, tasks, chunksize=1))
    a.output.write_text(
        json.dumps(
            dict(wall_seconds=time.perf_counter() - start, workers=a.workers, tasks=rows), indent=2
        )
    )


if __name__ == "__main__":
    main()
