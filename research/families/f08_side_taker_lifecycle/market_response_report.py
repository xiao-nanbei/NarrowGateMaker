"""Offline market probes of a completed source/ready response panel.

This module cannot launch accounts or admit a strategy. Calendar, transforms,
feature groups and outcomes are explicit; no economic outcomes are read.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from dataclasses import replace
import json
from pathlib import Path
import resource
import sys
import time

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from research.families.f03_causal_13_head.time_weighted_evaluation import CALENDAR, SPLITS

SECOND = 1_000_000_000
CONTEXT = ("spread", "own_depth", "opposite_depth", "own_touch_qty", "side_sign", "book_age_s",
           "utc_time_sin", "utc_time_cos", "past_price_variation_10s",
           "past_mid_move_1s", "past_mid_move_5s", "past_mid_move_10s")
TRADE = tuple(f"trade_{kind}_{w}s" for w in (1, 5, 10) for kind in ("pressure", "activity")) + ("pressure_acceleration",)
BOOK = ("touch_delta_qty", "neighbor_delta_qty")
HISTORY = ("recovery_age_s", "observed_recovery_fraction", "pressure_x_depth_change")
GROUPS = {"TRADE_ONLY": CONTEXT + TRADE, "BOOK_ONLY": CONTEXT + BOOK,
          "TRADE_BOOK_BASE": CONTEXT + TRADE + BOOK,
          "TRADE_BOOK_HISTORY": CONTEXT + TRADE + BOOK + HISTORY}
# Diagnostic only: isolate temporal history from a same-time algebraic product.
# This is not another action model or candidate strategy.
DIAGNOSTIC_GROUP = "BASE_WITH_SAME_TIME_PRODUCT"
OUTCOMES = tuple(f"{kind}_{h * SECOND}" for h in (1, 5, 30)
                 for kind in ("mid_move", "same_aggressor_qty", "net_aggressor_qty", "fixed_qty", "still_best", "execution_range"))
OUTCOMES += tuple(f"recovered_{h}" for h in (100_000_000, 500_000_000, SECOND, 5 * SECOND))


@dataclass(frozen=True)
class TrainingTransform:
    columns: tuple[str, ...]
    median: np.ndarray
    mean: np.ndarray
    scale: np.ndarray

    @classmethod
    def fit(cls, frame, columns, weights):
        values = frame.loc[:, list(columns)].to_numpy(dtype=float)
        if np.isinf(values).any():
            raise ValueError("infinite feature is not missing data")
        median = np.array([np.median(x[np.isfinite(x)]) if np.isfinite(x).any() else 0.
                           for x in values.T])
        filled = np.where(np.isnan(values), median, values)
        weights = np.asarray(weights, dtype=float)
        weights = weights / weights.sum()
        mean = weights @ filled
        scale = np.sqrt(weights @ ((filled - mean) ** 2))
        scale = np.where(scale > 0, scale, 1.)
        return cls(tuple(columns), median, mean, scale)

    def apply(self, frame):
        values = frame.loc[:, list(self.columns)].to_numpy(dtype=float)
        if np.isinf(values).any():
            raise ValueError("infinite feature is not missing data")
        missing = np.isnan(values)
        filled = np.where(missing, self.median, values)
        return np.concatenate(((filled - self.mean) / self.scale, missing.astype(float)), axis=1)


def validate_panel(frame, *, require_complete=True):
    days = set(frame["day"])
    if not days <= set(CALENDAR[:300]):
        raise ValueError("Final or out-of-calendar data is forbidden")
    if require_complete and days != set(CALENDAR[:300]):
        raise ValueError("incomplete market panel; do not fit on a convenient prefix")
    if not set(frame["panel"]) <= {"source", "visible"}:
        raise ValueError("unknown panel clock")
    if frame.groupby("day").size().max() > 3000:
        raise ValueError("daily sample cap exceeded")
    if (frame.loc[frame.day.isin(SPLITS["T"])].shape[0]) > 300_000:
        raise ValueError("T sample cap exceeded")
    probabilities = frame["inclusion_probability"].to_numpy(float)
    if not np.all(np.isfinite(probabilities) & (probabilities > 0) & (probabilities <= 1)):
        raise ValueError("invalid sampling probability")
    if (frame["source_asof_ns"] > frame["now_ns"]).any():
        raise ValueError("future source input")
    starts = pd.to_datetime(frame.day, utc=True).dt.as_unit("ns").astype("int64").to_numpy()
    if not np.all((frame.now_ns >= starts) & (frame.now_ns < starts + 86400 * SECOND)):
        raise ValueError("row day does not match clock")
    for name in OUTCOMES:
        horizon = int(name.rsplit("_", 1)[1])
        available = frame[name].notna().to_numpy()
        if np.any(available & (frame.now_ns.to_numpy() + horizon >= starts + 86400 * SECOND)):
            raise ValueError("outcome crosses split date")


def block_interval(days, benefits, weights, *, draws=2000, seed=20261004):
    """Four contiguous calendar-day blocks, not four adjacent filtered dates."""
    index = {day: i for i, day in enumerate(CALENDAR[:300])}
    numerator, denominator = np.zeros(300), np.zeros(300)
    for day, benefit, weight in zip(days, benefits, weights, strict=True):
        numerator[index[day]] += benefit * weight
        denominator[index[day]] += weight
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, 297, size=(draws, 75))
    selected = (starts[:, :, None] + np.arange(4)).reshape(draws, 300)
    totals = denominator[selected].sum(axis=1)
    values = numerator[selected].sum(axis=1)[totals > 0] / totals[totals > 0]
    return {"low": float(np.quantile(values, .025)) if len(values) else None,
            "high": float(np.quantile(values, .975)) if len(values) else None,
            "supported_days": int(np.count_nonzero(denominator)), "valid_draws": len(values)}


def evaluate_probes(frame):
    """Fixed normalized Ridge penalty=1; T-only transforms, A/B/C evaluation.

    Binary outcomes use a linear probability probe clipped to [0,1] for Brier
    evaluation; this is not an action-value model or a hyperparameter search.
    """
    scores, increments, missing = [], [], []
    for panel in ("source", "visible"):
        data = frame.loc[frame.panel == panel].reset_index(drop=True)
        for outcome in OUTCOMES:
            eligible = data[outcome].notna() & np.isfinite(data[outcome])
            train = eligible & data.day.isin(SPLITS["T"])
            counts = {group: int((eligible & data.day.isin(SPLITS[group])).sum()) for group in ("T", "A", "B", "C")}
            missing.append({"panel": panel, "outcome": outcome, "available": counts,
                            "unavailable": int((~eligible).sum())})
            if counts["T"] < 32 or data.loc[train, "day"].nunique() < 2:
                continue
            y_train = data.loc[train, outcome].to_numpy(float)
            w_train = 1 / data.loc[train, "inclusion_probability"].to_numpy(float)
            w_train /= w_train.sum()
            binary = outcome.startswith(("still_best_", "recovered_"))
            predictions = {}
            groups = {**GROUPS, DIAGNOSTIC_GROUP: CONTEXT + TRADE + BOOK + ("pressure_x_depth_change",)}
            for group, columns in groups.items():
                transform = TrainingTransform.fit(data.loc[train], columns, w_train)
                model = Ridge(alpha=1., fit_intercept=True, solver="cholesky")
                model.fit(transform.apply(data.loc[train]), y_train, sample_weight=w_train)
                prediction = model.predict(transform.apply(data))
                predictions[group] = np.clip(prediction, 0, 1) if binary else prediction
                for split in ("A", "B", "C"):
                    mask = eligible & data.day.isin(SPLITS[split])
                    if not mask.any():
                        continue
                    actual = data.loc[mask, outcome].to_numpy(float)
                    predicted = predictions[group][mask]
                    weights = 1 / data.loc[mask, "inclusion_probability"].to_numpy(float)
                    scores.append({"panel": panel, "outcome": outcome, "group": group, "split": split,
                                   "rows": int(mask.sum()), "days": int(data.loc[mask, "day"].nunique()),
                                   "mse_or_brier": float(np.average((actual - predicted) ** 2, weights=weights)),
                                   "mean_actual": float(np.average(actual, weights=weights)),
                                   "mean_predicted": float(np.average(predicted, weights=weights))})
            for split in ("A", "B", "C"):
                mask = eligible & data.day.isin(SPLITS[split])
                if not mask.any():
                    continue
                actual = data.loc[mask, outcome].to_numpy(float)
                joint = (actual - predictions["TRADE_BOOK_HISTORY"][mask]) ** 2
                weights = 1 / data.loc[mask, "inclusion_probability"].to_numpy(float)
                for comparator in ("TRADE_BOOK_BASE", DIAGNOSTIC_GROUP):
                    base = (actual - predictions[comparator][mask]) ** 2
                    interval = block_interval(data.loc[mask, "day"], base - joint, weights)
                    increments.append({"panel": panel, "outcome": outcome, "split": split,
                                       "comparator": comparator,
                                       "base_minus_history_loss": float(np.average(base - joint, weights=weights)), **interval})
    return scores, increments, missing


def response_curves(frame):
    """T-defined pressure terciles; probability weighting, no PnL bin choice."""
    rows, cutoffs = [], {}
    for panel in ("source", "visible"):
        data = frame.loc[frame.panel == panel].copy()
        training = data.loc[data.day.isin(SPLITS["T"]), "trade_pressure_1s"].dropna()
        if training.empty:
            continue
        cuts = np.unique(np.quantile(training, [1/3, 2/3]))
        cutoffs[panel] = cuts.tolist()
        data["pressure_bin"] = np.where(data.trade_pressure_1s.isna(), -1,
                                        np.searchsorted(cuts, data.trade_pressure_1s, side="right"))
        data["support"] = np.where(data.touch_delta_qty.isna(), "unknown",
                                   np.where(data.touch_delta_qty < 0, "declining", "nondeclining"))
        for split in ("T", "A", "B", "C"):
            subset = data.loc[data.day.isin(SPLITS[split])]
            for (pressure_bin, support), group in subset.groupby(["pressure_bin", "support"]):
                for h in (100_000_000, 500_000_000, SECOND, 5 * SECOND):
                    weights = 1 / group.inclusion_probability.to_numpy(float)
                    observable = group[f"missing_{h}"].isna().to_numpy()
                    has_anchor = group.recovery_start_ns.notna().to_numpy()
                    event = group[f"recovery_event_{h}"].to_numpy()
                    at_risk = has_anchor & observable
                    recovered = at_risk & (event == "recovered")
                    competing = at_risk & np.isin(event, ["outside_depth_or_invalid", "history_limit"])
                    # Competing exits stay in the denominator; not coded as
                    # failures or silently discarded from recovery support.
                    rows.append({"panel": panel, "split": split, "pressure_bin": int(pressure_bin),
                                 "support": support, "horizon_ns": h, "rows": len(group),
                                 "anchor_rows": int(has_anchor.sum()), "observable_anchor_rows": int(at_risk.sum()),
                                 "recovered_weight": float(weights[recovered].sum()),
                                 "competing_weight": float(weights[competing].sum()),
                                 "observable_anchor_weight": float(weights[at_risk].sum()),
                                 "missing_anchor_weight": float(weights[has_anchor & ~observable].sum())})
    return rows, cutoffs


def render_report(result):
    """Descriptive report only; do not infer actionability or economic admission."""
    counts = result["market_status"]["days"]
    totals = {}
    for day in counts.values():
        for key, value in day.items():
            totals[key] = totals.get(key, 0) + value
    lines = ["# Independent trade–book market report", "",
             "Private research evidence. No strategy replay, action model or PnL validation.", "",
             f"Coverage: {len(counts)} non-Final UTC dates; {result['sample_rows']} sampled rows.",
             "T fits only; A/B/C are interleaved, previously used development dates, not a new chronological holdout.", "",
             "## Full-stream denominators", "", "| Counter | Count |", "| --- | ---: |"]
    lines.extend(f"| {key} | {value} |" for key, value in sorted(totals.items()))
    lines += ["", "## Paired prediction error increments", "",
              "Positive means lower squared/Brier error with history. Units are squared outcome units; not USDC action value. Intervals use four calendar-day blocks, 2000 draws, seed 20261004; not corrected for many comparisons.",
              "The same-time-product comparator prevents an arithmetic interaction alone being called temporal information.", "",
              "| Clock | Outcome | Split | Comparator | Loss reduction | 95% interval |", "| --- | --- | --- | --- | ---: | --- |"]
    for row in result["paired_loss_increments"]:
        lines.append(f"| {row['panel']} | {row['outcome']} | {row['split']} | {row['comparator']} | "
                     f"{row['base_minus_history_loss']:.8g} | [{row['low']:.8g}, {row['high']:.8g}] |")
    lines += ["", "## Recovery / migration diagnostic", "",
              "Publication-conditioned samples are not independent decline-onset cohorts. Pressure bins are fixed T sample terciles; bin -1 is unknown. Probabilities use inverse inclusion weights. Outside-depth/history-limit exits are competing events, not failures. Reset/stale/missing references stay missing.", "",
              "| Clock | Split | Pressure bin | Support | Horizon ms | Observable anchors | Recovered weight / at-risk weight | Competing weight / at-risk weight |",
              "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |"]
    for row in result["recovery_curves"]:
        denom = row["observable_anchor_weight"]
        recovery = f"{row['recovered_weight']/denom:.6g}" if denom else "NA"
        competing = f"{row['competing_weight']/denom:.6g}" if denom else "NA"
        lines.append(f"| {row['panel']} | {row['split']} | {row['pressure_bin']} | {row['support']} | "
                     f"{row['horizon_ns']/1e6:g} | {row['observable_anchor_rows']} | {recovery} | {competing} |")
    lines += ["", "## Limitations and next-stage boundary", ""]
    lines.extend(f"- {item}." for item in result["limitations"])
    lines += ["", "No B0 opportunity/action-time linkage has been inferred from these sparse market samples. A sampled touch frame is not the complete state at an arbitrary B0 order price or decision time.", "",
              f"Measured report cost: wall {result['wall_seconds']:.3f}s, CPU {result['cpu_seconds']:.3f}s, peak RSS {result['peak_rss_bytes']} bytes.", ""]
    return "\n".join(lines)


def trade_first_ties(tick):
    """Diagnostic cross-channel tie convention; preserve within-channel order.

    This function does not change timestamps, delivery samples or production
    ordering. Book views retain the same order as their complete messages.
    """
    from data.tardis_input import BookMessage
    from data.observation import TradeContribution
    events = tuple(e for e in tick.exchange_events if not isinstance(e, BookMessage))
    events += tuple(e for e in tick.exchange_events if isinstance(e, BookMessage))
    observations = []
    for ready in dict.fromkeys(o.ready_ns for o in tick.observations):
        batch = [o for o in tick.observations if o.ready_ns == ready]
        observations.extend(o for o in batch if isinstance(o.payload, TradeContribution))
        observations.extend(o for o in batch if not isinstance(o.payload, TradeContribution))
    return replace(tick, exchange_events=events, observations=tuple(observations))


def audit_ties(config, output, *, seconds=900):
    """One bounded input pass, two explicit tie conventions, no strategy."""
    from data.runtime import ObservationProfile, PublicInputStream
    from data.tardis_input import CONTRACT, BookMessage
    from data.observation import TradeContribution
    from research.families.f08_side_taker_lifecycle.joint_trade_book_response import MarketPanels
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    start = config["start_ns"]
    end = start + seconds * SECOND
    if seconds <= 0 or seconds > 900:
        raise ValueError("tie diagnostic is bounded to 900 seconds")
    day = pd.Timestamp(start, unit="ns", tz="UTC").strftime("%Y-%m-%d")
    if day not in CALENDAR[:300]:
        raise ValueError("Final is forbidden")
    profile = ObservationProfile(**config["observation_profile"])
    states = [MarketPanels(start_ns=start, end_ns=end, profile=profile,
                           fast_response_state=True, range_block_size=64, compact_pending=True)
              for _ in range(2)]
    stream = PublicInputStream([Path(config["facts_root"]) / day], profile=profile,
        start_ns=start, end_ns=end, market_id=config["market_id"], input_contract_id=CONTRACT,
        verify=False, capture_source_books=True, incremental_top_cache=True,
        fast_fact_decode=True, consumer_mode="market_response_scan", scan_skip_book_view=True)
    wall, cpu, source_ties, visible_ties = time.monotonic(), time.process_time(), 0, 0
    for tick in stream:
        source_ties += int(any(isinstance(e, BookMessage) for e in tick.exchange_events)
                           and any(not isinstance(e, BookMessage) for e in tick.exchange_events))
        visible_ties += int(any(isinstance(o.payload, TradeContribution) for o in tick.observations)
                            and any(not isinstance(o.payload, TradeContribution) for o in tick.observations))
        states[0].advance(tick)
        states[1].advance(trade_first_ties(tick))
    frames = []
    keys = ["panel", "now_ns", "side", "book_version"]
    for state in states:
        state.flush(end)
        rows = [row for sample in state.samples.values() for row in sample.selected()]
        frame = pd.DataFrame(rows).set_index(keys)
        if frame.index.has_duplicates:
            raise ValueError("ambiguous sampled identity in tie diagnostic")
        frames.append(frame)
    if set(frames[0].index) != set(frames[1].index):
        raise ValueError("tie convention changed sampling support; do not silently pair")
    changed = {}
    for panel in ("source", "visible"):
        left = frames[0].xs(panel, level="panel")
        right = frames[1].xs(panel, level="panel").reindex(left.index)
        changed[panel] = {name: int((~((left[name] == right[name]) |
                                            (left[name].isna() & right[name].isna()))).sum())
                          for name in left.columns}
    result = dict(status="bounded_tie_diagnostic_not_full_calendar_robustness", start_ns=start,
                  end_ns=end, rows=len(frames[0]), source_tied_batches=source_ties,
                  visible_mixed_batches=visible_ties, changed_sample_fields=changed,
                  wall_seconds=time.monotonic()-wall, cpu_seconds=time.process_time()-cpu,
                  strategy_replays=0,
                  limitation="Same input and samples; alternative source cross-channel ordering is not an identified native order. Does not refit full-calendar probes.")
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    return result


def audit_baseline_logs(manifest, output):
    """Inventory existing Development evidence, without reconstructing events.

    E/C opportunities are explicitly not optional KEEP/UPDATE opportunities.
    Missing event logs are not zeros for the underlying economic quantities.
    """
    rows = []
    wall, cpu = time.monotonic(), time.process_time()
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    ids = [x["account_id"] for x in manifest["accounts"]]
    if len(ids) != len(set(ids)) or any(not name.startswith("development-") for name in ids):
        raise ValueError("only distinct Development accounts may enter")
    for item in manifest["accounts"]:
        directory = item.get("directory")
        if directory is None:
            rows.append({"account_id": item["account_id"], "status": "local_result_missing"})
            continue
        root = Path(directory)
        accounting = json.loads((root / "accounting.json").read_text())
        if accounting.get("economic_complete") is not True:
            raise ValueError("baseline accounting incomplete")
        replay = json.loads((root / "replay.json").read_text())
        trace_counts = {key: len(replay.get(key, [])) for key in (
            "_economic_fill_trace", "_fill_trace", "_quote_trace", "_decision_trace",
            "_local_order_lifecycle_trace", "_risk_action_trace", "_risk_selection_opportunities")}
        kinds = {}
        for event in replay.get("_risk_selection_opportunities", []):
            kind = event.get("kind", "unknown")
            kinds[kind] = kinds.get(kind, 0) + 1
        rows.append({"account_id": item["account_id"], "status": "existing_result_read",
                     "economic_complete": True, "trace_rows": trace_counts,
                     "legacy_opportunity_kinds": kinds,
                     "fill_clock_basis": accounting.get("fill_clock_basis"),
                     "all_in_net_pnl": accounting["all_in_net_pnl"],
                     "public_input_contract": replay.get("public_input_contract"),
                     "new_update_opportunity_stream_verified": False})
        del replay
    result = {"status": "baseline_evidence_inventory_not_order_attribution",
              "accounts": rows, "strategy_replays": 0,
              "wall_seconds": time.monotonic()-wall, "cpu_seconds": time.process_time()-cpu,
              "limitations": ["Absent traces do not mean absent requests, updates or opportunities",
                              "E/C POST/WAIT and KEEP/CANCEL logs do not identify this study's pre-price-throttle KEEP/UPDATE roots",
                              "Sampled market touch frames are not exact old-order-price states",
                              "Old transport scenarios must not be silently compared with a changed ordered-delivery candidate"]}
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    return result


def load_panel_sources(manifest):
    """Read accepted closed scan outputs once, rejecting overlapping dates.

    Explicitly accepted interrupted prefixes retain their original status. Their
    day list must match physical rows, rather than the last in-memory progress.
    No fitting or outcome-based selection occurs here.
    """
    frames, covered, denominators, bindings = [], set(), {}, []
    for item in manifest["sources"]:
        root = Path(item["directory"])
        status = json.loads((root / "market-status.json").read_text())
        days = set(item["days"])
        if not days or covered & days:
            raise ValueError("empty or overlapping accepted dates")
        complete = status["status"] == "market_panel_complete_not_economic_validation"
        prefix = (item["acceptance"] == "previously_verified_closed_prefix"
                  and status["status"] in {"failed", "interrupted"})
        if not (complete or prefix):
            raise ValueError("unaccepted or potentially active writer")
        part = pd.read_parquet(root / "market-panel.parquet")
        if set(part.day) != days:
            raise ValueError("physical rows differ from accepted dates")
        if complete and len(part) != status["written_sample_rows"]:
            raise ValueError("completed output row count mismatch")
        validate_panel(part, require_complete=False)
        for day, group in part.groupby("day"):
            counts = status["days"][day]
            eligible = counts.get("source:eligible", 0) + counts.get("visible:eligible", 0)
            if len(group) != min(3000, eligible):
                raise ValueError("sample count differs from eligible denominator")
            if not np.all(group.eligible_day_rows == eligible):
                raise ValueError("stored denominator differs from scan")
            if not np.allclose(group.inclusion_probability, len(group) / eligible, rtol=1e-14, atol=0):
                raise ValueError("stored inclusion probability differs from scan")
            denominators[day] = counts
        covered.update(days)
        frames.append(part)
        bindings.append({"directory": str(root), "days": sorted(days), "acceptance": item["acceptance"]})
    frame = pd.concat(frames, ignore_index=True)
    validate_panel(frame)
    return frame, {"status": "market_panel_complete_not_economic_validation",
                   "days": denominators, "sources": bindings}


def report_market(panel_dir, output, *, manifest=None):
    output = Path(output)
    if output.exists() or output.with_suffix('.md').exists():
        raise FileExistsError("inspect existing report before replacing it")
    wall, cpu = time.monotonic(), time.process_time()
    if manifest is not None:
        frame, status = load_panel_sources(json.loads(Path(manifest).read_text()))
    else:
        root = Path(panel_dir)
        status = json.loads((root / "market-status.json").read_text())
        if status["status"] != "market_panel_complete_not_economic_validation":
            raise ValueError("market scan is not complete; do not read its active Parquet writer")
        frame = pd.read_parquet(root / "market-panel.parquet")
        validate_panel(frame)
    scores, increments, missing = evaluate_probes(frame)
    curves, cutoffs = response_curves(frame)
    result = {"visibility": "local_only_do_not_publish", "status": "market_probes_complete_review_required",
              "market_status": status, "sample_rows": len(frame), "scores": scores,
              "paired_loss_increments": increments, "outcome_coverage": missing,
              "recovery_curves": curves, "T_pressure_cutoffs": cutoffs,
              "feature_groups": GROUPS, "strategy_replays": 0, "action_model_fits": 0,
              "wall_seconds": time.monotonic() - wall, "cpu_seconds": time.process_time() - cpu,
              "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
              "limitations": [
                  "source timestamp proxy is not proven native exchange order",
                  "ordered modeled delivery is not a live transport distribution validation",
                  "fixed-price changes and ready-window trades are not aligned conservation residuals",
                  "recovery curves are publication-conditioned, not an equally weighted cohort of decline onsets",
                  "alternative same-time assignment sensitivity is not yet measured",
                  "A/B/C have previous use and are not new blind samples",
                  "many overlapping outcomes: block intervals are descriptive, not multiplicity-adjusted discovery",
                  "no strategy admission follows automatically from prediction or probe completion"]}
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    output.with_suffix(".md").write_text(render_report(result))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--panel-dir", type=Path)
    source.add_argument("--manifest", type=Path)
    source.add_argument("--tie-config", type=Path)
    source.add_argument("--baseline-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.baseline_manifest:
        result = audit_baseline_logs(json.loads(args.baseline_manifest.read_text()), args.output)
    elif args.tie_config:
        result = audit_ties(json.loads(args.tie_config.read_text()), args.output)
    else:
        result = report_market(args.panel_dir, args.output, manifest=args.manifest)
    print(json.dumps({"status": result["status"], "sample_rows": result.get("sample_rows", result.get("rows")), "strategy_replays": 0}))


if __name__ == "__main__":
    main()
