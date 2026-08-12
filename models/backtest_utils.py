"""Shared helpers for backtest scripts."""

import os
from multiprocessing import cpu_count

import numpy as np


def _normalized_tied_ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    sorted_values = values[order]
    positions = np.linspace(0.0, 1.0, len(values), dtype=np.float64)
    sorted_ranks = np.empty(len(values), dtype=np.float64)

    start = 0
    while start < len(values):
        end = start + 1
        while end < len(values) and np.isclose(sorted_values[end], sorted_values[start], rtol=1e-12, atol=1e-12):
            end += 1
        sorted_ranks[start:end] = positions[start:end].mean()
        start = end

    ranks = np.empty(len(values), dtype=np.float64)
    ranks[order] = sorted_ranks
    return ranks


def default_backtest_workers() -> int:
    return max(1, int(os.environ.get("MM_BACKTEST_WORKERS", min(cpu_count(), 6))))


def attach_selection_scores(results):
    # 这是 sweep 表格里的轻量排序分数，不是参数晋级标准。
    # 正式 daily selection 必须先过机制 hard gate：placed/fills/spread/block reason/
    # side markout/inventory time/tail day，再看 PnL 或 InvAdj。
    if not results:
        return results
    if len(results) == 1:
        results[0]["selection_score"] = 1.0
        return results

    score = np.zeros(len(results), dtype=np.float64)
    fields = (("pnl", 0.4), ("inventory_adjusted_pnl", 0.4), ("avg_markout", 0.2))
    for field, weight in fields:
        values = np.array([result.get(field, 0.0) for result in results], dtype=np.float64)
        ranks = _normalized_tied_ranks(values)
        score += weight * ranks

    for index, result in enumerate(results):
        result["selection_score"] = float(score[index])
    return results
