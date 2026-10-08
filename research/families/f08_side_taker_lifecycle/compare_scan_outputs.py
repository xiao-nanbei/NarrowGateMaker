"""Exact business oracle for bounded market scans; never compares timing costs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from itertools import zip_longest


def compare(reference, candidate, path="root"):
    """Fail at the first differing field, including row order and missing keys."""
    if type(reference) is not type(candidate):
        raise AssertionError(f"{path}: types differ")
    if isinstance(reference, dict):
        if reference.keys() != candidate.keys():
            raise AssertionError(f"{path}: keys differ")
        for key in reference:
            compare(reference[key], candidate[key], f"{path}.{key}")
    elif isinstance(reference, list):
        if len(reference) != len(candidate):
            raise AssertionError(f"{path}: lengths differ")
        for i, (left, right) in enumerate(zip(reference, candidate, strict=True)):
            compare(left, right, f"{path}[{i}]")
    elif reference != candidate:
        raise AssertionError(f"{path}: {reference!r} != {candidate!r}")


def compare_parquet(reference, candidate):
    import pyarrow.parquet as pq
    left, right = pq.ParquetFile(reference), pq.ParquetFile(candidate)
    if not left.schema_arrow.equals(right.schema_arrow):
        raise AssertionError("market-panel schema differs")
    def rows(file):
        for batch in file.iter_batches(batch_size=8192):
            yield from batch.to_pylist()
    missing = object()
    count = 0
    for count, (a, b) in enumerate(zip_longest(rows(left), rows(right), fillvalue=missing), 1):
        if a is missing or b is missing:
            raise AssertionError("market-panel row count differs")
        compare(a, b, f"market-panel[{count - 1}]")
    return count


def compare_paths(reference, candidate):
    reference, candidate = Path(reference), Path(candidate)
    if reference.is_dir():
        compare_parquet(reference / "market-panel.parquet", candidate / "market-panel.parquet")
        left = json.loads((reference / "market-status.json").read_text())
        right = json.loads((candidate / "market-status.json").read_text())
        # Only explicit operational metadata is excluded, never arbitrary fields.
        operational = {"wall_seconds", "cpu_seconds", "peak_rss_bytes", "output_bytes", "input_contract"}
        compare({k: v for k, v in left.items() if k not in operational},
                {k: v for k, v in right.items() if k not in operational}, "status")
    elif reference.suffix == ".parquet":
        compare_parquet(reference, candidate)
    else:
        compare(json.loads(reference.read_text()), json.loads(candidate.read_text()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    compare_paths(args.reference, args.candidate)
    print("exact business parity")


if __name__ == "__main__":
    main()
