"""Offline, explicit-input replay measurement. Never selects research candidates."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import cProfile
import hashlib
import json
from pathlib import Path
import resource
import time
from unittest.mock import patch

import numpy as np


def measure(
    bundle, params, *, model=None, funding=None, profile=None, prepared=None, result_path=None
):
    from models import backtest_tick as replay
    from models.replay import public_input
    from models.replay.public_accounting import settle_public_replay
    from strategy.signal import SignalEngine

    stages = {}

    def timed(name, function):
        def call(*args, **kwargs):
            start = time.perf_counter()
            try:
                return function(*args, **kwargs)
            finally:
                stages[name] = stages.get(name, 0.0) + time.perf_counter() - start

        return call

    start = time.perf_counter()
    cpu = time.process_time()
    engine = (
        None
        if model is None
        else SignalEngine.from_public_models(model, symbol="BTCUSDC", ret_demean_halflife=0)
    )
    profiler = cProfile.Profile() if profile else None
    with ExitStack() as stack:
        for module, name, label in (
            (replay, "load_public_inputs", "input_load"),
            (public_input, "public_predictions", "prediction_and_postprocessing"),
            (replay, "simulate_tick", "event_loop"),
        ):
            stack.enter_context(patch.object(module, name, timed(label, getattr(module, name))))
        if profiler:
            profiler.enable()
        result = (
            replay.simulate_public_inputs(bundle, params, signal_engine=engine)
            if prepared is None
            else replay.simulate_prepared_inputs(prepared, params, signal_engine=engine)
        )
        if profiler:
            profiler.disable()
            profiler.dump_stats(profile)
    def convert(value):
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, np.generic):
            return value.item()
        raise TypeError(type(value).__name__)

    try:
        account = timed("accounting", settle_public_replay)(
            bundle, result, initial_capital=10000.0, max_mark_age_ns=1_000_000_000, funding=funding
        )
    except Exception as error:
        # Keep the producer's original list order and unknown values.  A failed
        # settlement must not destroy the only replay evidence or look complete.
        if result_path is not None:
            failed_path = Path(str(result_path) + ".accounting-failed.json")
            try:
                with failed_path.open("x") as output:
                    json.dump(
                        {"status": "accounting_failed", "replay": result,
                         "error": {"type": type(error).__name__, "message": str(error)},
                         "stages_before_failure_write": stages},
                        output, default=convert, sort_keys=True, allow_nan=True,
                    )
                error.add_note(f"Unsettled replay preserved at {failed_path}")
            except Exception as write_error:
                error.add_note(f"Failed to preserve unsettled replay: {write_error!r}")
        raise

    serialization_start = time.perf_counter()
    economic = json.dumps(
        {"replay": result, "accounting": account}, default=convert, sort_keys=True, allow_nan=True
    ).encode()
    stages["serialization"] = time.perf_counter() - serialization_start
    if result_path is not None:
        write_start = time.perf_counter()
        Path(result_path).write_bytes(economic)
        stages["result_write"] = time.perf_counter() - write_start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    memory = {}
    try:
        import psutil

        full = psutil.Process().memory_full_info()
        memory = {name: getattr(full, name, None) for name in ("rss", "uss", "pss")}
    except (ImportError, OSError):
        pass
    return dict(
        wall_seconds=time.perf_counter() - start,
        cpu_seconds=time.process_time() - cpu,
        stages=stages,
        maxrss_platform_units=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        output_bytes=len(economic),
        result_sha256=hashlib.sha256(economic).hexdigest(),
        fills=len(result.get("_fill_trace", [])),
        economic_complete=account["economic_complete"],
        cache_condition="OS page cache uncontrolled; not claimed disk cold",
        includes_result_write=result_path is not None,
        memory_at_end_bytes=memory,
        memory_measurement="RSS high-water; USS/PSS end sample, not peak",
        minor_faults=usage.ru_minflt,
        major_faults=usage.ru_majflt,
        block_inputs=usage.ru_inblock,
        block_outputs=usage.ru_oublock,
        execution_trades=result.get("n_trades"),
        merged_clock_events=result.get("n_clock_events"),
    ), economic


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--params", type=Path, required=True)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--funding", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("repeat must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    params = json.loads(args.params.read_text())
    funding = None if args.funding is None else json.loads(args.funding.read_text())
    prepared = None
    if args.cache:
        from models.backtest_tick import prepare_public_inputs

        start = time.perf_counter()
        prepared = prepare_public_inputs(
            args.bundle, tick_size=params["tick_size"], cache_dir=args.cache
        )
        (args.output / "preparation.json").write_text(
            json.dumps({"seconds": time.perf_counter() - start})
        )
    for index in range(args.repeat):
        row, economic = measure(
            args.bundle,
            params,
            model=args.model,
            funding=funding,
            profile=str(args.output / f"{index}.prof") if args.profile else None,
            prepared=prepared,
            result_path=args.output / f"{index}.result.json",
        )
        (args.output / f"{index}.metrics.json").write_text(json.dumps(row, indent=2) + "\n")
        print(json.dumps(row), flush=True)


if __name__ == "__main__":
    main()
