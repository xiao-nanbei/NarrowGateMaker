"""Causal quote-opportunity inputs and explicitly observed outcome labels."""

import math

import pandas as pd

from data.feature_cursor import FeatureCursor


def iter_prefill_parent_segments(prepared, params, *, cut_times_ms,
                                prepared_predictions=None, resume_checkpoint=None):
    """Advance one B0 parent through declared cuts, never replay its prefix.

    Each yielded checkpoint is the *preceding* cut for the newly observed
    opportunities. The caller may run isolated suffixes before requesting the
    next segment. Cuts and sampling must be fixed without reading outcomes.
    The final yield contains the original full-account result for settlement.
    """
    from models.backtest_tick import simulate_prepared_inputs
    from strategy.risk_selection import PREFILL_FEATURE_CONTRACT

    if params.get('risk_selection_opportunity_retention', 'full') != 'full':
        raise ValueError('prefill labels require full opportunity details')
    if (params.get('risk_selection_mode', 'B') != 'B'
            or params.get('risk_selection_control', 'learned') != 'learned'
            or params.get('risk_selection_scope') != 'visible_inventory'
            or params.get('risk_selection_feature_contract') != PREFILL_FEATURE_CONTRACT
            or not params.get('risk_selection_collect_opportunities')
            or params.get('risk_selection_intervention')):
        raise ValueError('prefill parent requires observable unchanged current B0')
    plan = prepared.inputs['bundle'].manifest['plan']
    start = (params.get('account_start_ns', plan['start_ns']) + 999_999)//1_000_000
    end = (plan['end_ns'] + 999_999)//1_000_000
    cuts = tuple(cut_times_ms)
    if resume_checkpoint is not None:
        start = resume_checkpoint['cut_ts_ms']
    if (not cuts or cuts[0] != start
            or any(type(t) is not int or not start <= t < end for t in cuts)
            or any(a >= b for a, b in zip(cuts, cuts[1:], strict=False))):
        raise ValueError('strictly increasing parent cuts must begin at account start')
    checkpoint = resume_checkpoint
    resuming = checkpoint is not None
    del resume_checkpoint
    observed = (len(checkpoint['runtime'].risk_selection.rows) if checkpoint is not None else 0)
    for cut in (*(cuts[1:] if resuming else cuts), None):
        result = simulate_prepared_inputs(prepared, params,
            prepared_predictions=prepared_predictions,
            checkpoint_at_ts_ms=cut, resume_checkpoint=checkpoint)
        following = result.get('_replay_checkpoint')
        rows = (following['runtime'].risk_selection.rows if following is not None
                else result['_risk_selection_opportunities'])
        if len(rows) < observed:
            raise ValueError('parent opportunity stream was truncated')
        new_rows = tuple(rows[observed:])
        observed = len(rows)
        yield dict(checkpoint=checkpoint, opportunities=new_rows,
                   next_checkpoint=following,
                   result=result if following is None else None)
        checkpoint = following
        del result, following, rows, new_rows


def iter_prefill_suffix_labels(prepared, params, *, checkpoint, opportunities,
                              prepared_predictions, funding, initial_capital,
                              max_mark_age_ns, verify_first_control=False):
    """Consume an outcome-blind selected batch, settling each pair immediately.

    The caller owns sampling and durable output. No branch state survives a
    yield except its compact label/accounting receipt. Market predictions and
    terminal marks are shared; orders, signal state and economic paths are not.
    """
    import time

    from models.backtest_tick import simulate_prefill_branch
    from models.replay.public_accounting import (
        funding_window, settle_public_replay, terminal_valuations,
    )
    from models.replay.risk_selection import assemble_prefill_label, _same_trace_value

    rows = tuple(opportunities)
    if len({r['opportunity_id'] for r in rows}) != len(rows):
        raise ValueError('duplicate suffix opportunity')
    if rows and checkpoint is None:
        raise ValueError('suffixes require a preceding parent checkpoint')
    root = prepared.inputs['bundle'].root
    start = params.get('account_start_ns', prepared.inputs['bundle'].manifest['plan']['start_ns'])
    ends = [r['decision_ts_ns'] + 30_000_000_000 for r in rows]
    marks = terminal_valuations(prepared.inputs['bundle'], ends, max_mark_age_ns=max_mark_age_ns)
    for index, (row, end) in enumerate(zip(rows, ends, strict=True)):
        started = time.perf_counter()
        alternative_action = {'E': 'WAIT', 'C': 'CANCEL'}[row['kind']]
        schedule = funding_window(funding, start_ns=start, end_ns=end)
        results, accounts = [], []
        for action in (row['baseline_action'], alternative_action):
            result = simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
                opportunity=row, action=action, prepared_predictions=prepared_predictions)
            account = settle_public_replay(root, result, initial_capital=initial_capital,
                max_mark_age_ns=max_mark_age_ns, funding=schedule,
                terminal_valuation=marks[end])
            results.append(result)
            accounts.append(account)
        label = assemble_prefill_label(*results,
            intervention={'opportunity_id': row['opportunity_id'], 'action': alternative_action},
            baseline_accounting=accounts[0], alternative_accounting=accounts[1],
            fork=results[0]['prefill_fork'])
        receipt = dict(label=label, accounting=accounts,
            branch_calls=2, elapsed_s=time.perf_counter()-started,
            matched_fact_counts=[a['economic_fills_total'] for a in accounts],
            notification_counts=[len(r['_fill_trace']) for r in results],
            pending_notification_counts=[r.get('private_fill_pending_visibility_count', 0)
                                         for r in results])
        receipt['same_state_same_action_control_delta'] = None
        receipt['control_branch_calls'] = 0
        if verify_first_control and index == 0:
            control = simulate_prefill_branch(prepared, params, checkpoint=checkpoint,
                opportunity=row, action=row['baseline_action'], prepared_predictions=prepared_predictions)
            control_account = settle_public_replay(root, control, initial_capital=initial_capital,
                max_mark_age_ns=max_mark_age_ns, funding=schedule, terminal_valuation=marks[end])
            for key in ('_fill_trace', '_economic_fill_trace', '_risk_selection_opportunities'):
                if not _same_trace_value(results[0].get(key), control.get(key)):
                    raise ValueError(f'no-intervention control differs: {key}')
            if not _same_trace_value(accounts[0], control_account):
                raise ValueError('no-intervention accounting differs')
            receipt['same_state_same_action_control_delta'] = 0.0
            receipt['control_branch_calls'] = 1
            del control
        receipt['elapsed_s'] = time.perf_counter()-started
        del results, result
        yield receipt


def _outcome_support(rows, boundary, intervals=None):
    """Half-open fitting support; an outcome may not cross a support gap."""
    intervals = intervals if intervals is not None else [(-2**63, boundary)]
    previous = None
    for start, end in intervals:
        if (type(start) is not int or type(end) is not int or start >= end
                or end > boundary or (previous is not None and start < previous)):
            raise ValueError("ordered disjoint half-open training intervals required")
        previous = end
    if not intervals:
        raise ValueError("nonempty training intervals required")
    eligible, reports = [], {}
    for item in rows.to_dict("records"):
        key = f"{item['side']}:{item['horizon_ns']}"
        report = reports.setdefault(key, dict(total=0, complete=0, no_fill=0,
                                              censored=0, out_of_support=0,
                                              missing_metadata=0, eligible=0))
        report['total'] += 1
        end, decision, censored = item['actual_outcome_end_ns'], item['decision_ns'], item['right_censored']
        if type(censored) is not bool:
            report['missing_metadata'] += 1
            eligible.append(False)
            continue
        if not pd.isna(end) and end < decision:
            raise ValueError("actual outcome ends before decision")
        if censored:
            report['censored'] += 1
            eligible.append(False)
            continue
        if pd.isna(end):
            report['missing_metadata'] += 1
            eligible.append(False)
            continue
        report['complete'] += 1
        if item['filled_quantity'] == 0:
            report['no_fill'] += 1
        accepted = any(start <= decision <= end < stop for start, stop in intervals)
        report['eligible' if accepted else 'out_of_support'] += 1
        eligible.append(accepted)
    return pd.Series(eligible, index=rows.index, dtype=bool), reports


def build_opportunity_panel(root, opportunities, outcomes, *, feature_columns,
                            missing_policy, max_age_ns, training_boundary_ns,
                            training_intervals_ns=None, support_role='training'):
    """One outcome per opportunity/horizon; censoring is not a zero label.

    Outcomes come from a declared fill/markout experiment, never from a touch
    count. ``markout_bps`` is quantity-weighted maker-signed conditional return.
    A fully observed no-fill outcome has zero opportunity return and unknown
    conditional markout. Actual outcome end, not the horizon name, purges rows.
    """
    if support_role not in ('training', 'engineering_observation'):
        raise ValueError('explicit training or engineering observation support required')
    if (support_role == 'training' and 'training_admitted' in outcomes
            and not outcomes['training_admitted'].eq(True).all()):
        raise ValueError('unadmitted engineering outcomes cannot become training support')
    required = {"opportunity_id", "decision_ns", "side"}
    if opportunities.empty or outcomes.empty:
        raise ValueError("nonempty opportunities and explicit outcomes required")
    if not required <= set(opportunities) or opportunities["opportunity_id"].duplicated().any():
        raise ValueError("unique quote opportunities with decisions and side required")
    required_outcomes = {"opportunity_id", "horizon_ns", "actual_outcome_end_ns",
                         "right_censored", "filled_quantity", "markout_bps"}
    if not required_outcomes <= set(outcomes):
        raise ValueError("complete explicit outcome schema required")
    if outcomes.duplicated(["opportunity_id", "horizon_ns"]).any():
        raise ValueError("duplicate opportunity outcome")
    if not set(outcomes["opportunity_id"]) <= set(opportunities["opportunity_id"]):
        raise ValueError("outcome refers to unknown opportunity")
    if set(opportunities["opportunity_id"]) - set(outcomes["opportunity_id"]):
        raise ValueError("opportunity is missing its outcome; cannot label no-fill")
    for field in ("horizon_ns", "actual_outcome_end_ns"):
        if (not pd.api.types.is_integer_dtype(outcomes[field])
                or (field == "horizon_ns" and outcomes[field].isna().any())):
            raise ValueError("outcome clocks must be integer nanoseconds")
    if not opportunities["side"].isin(["bid", "ask"]).all():
        raise ValueError("maker side must be bid or ask")
    cursor = FeatureCursor(root)
    cursor.require_binding(opportunities.to_dict("records"))
    cursor.require_binding(outcomes.to_dict("records"))
    if set(feature_columns) & (set(opportunities) | set(outcomes)):
        raise ValueError("feature names collide with opportunity/outcome fields")
    feature_rows = []
    for item in opportunities.to_dict("records"):
        decision = item["decision_ns"]
        feature_rows.append({"opportunity_id": item["opportunity_id"],
                             **cursor.row(decision, columns=feature_columns,
                                          missing_policy=missing_policy, max_age_ns=max_age_ns)})
    joined = opportunities.merge(outcomes.drop(columns="input_manifest_id"),
                                 on="opportunity_id", how="left", validate="one_to_many")
    support, support_counts = _outcome_support(joined, training_boundary_ns, training_intervals_ns)
    rows = []
    for item in joined.loc[support].to_dict("records"):
        end, decision = item["actual_outcome_end_ns"], item["decision_ns"]
        if pd.isna(end):
            raise ValueError("opportunity is missing its outcome; cannot label no-fill")
        if (not math.isfinite(end) or int(end) != end or end < decision
                or not math.isfinite(item["horizon_ns"]) or item["horizon_ns"] <= 0):
            raise ValueError("invalid outcome time boundary")
        if type(item["right_censored"]) is not bool:
            raise ValueError("explicit boolean censoring required")
        if item["right_censored"] or decision >= training_boundary_ns or end >= training_boundary_ns:
            continue
        quantity = float(item["filled_quantity"])
        if not math.isfinite(quantity) or quantity < 0:
            raise ValueError("invalid observed filled quantity")
        markout = item["markout_bps"]
        # Observed execution is not evidence of an observed future price.
        # Keep the fill-probability target without inventing a conditional
        # markout or forcing all heads to share the same valid-row mask.
        markout = (float(markout) if not pd.isna(markout) and math.isfinite(float(markout))
                   else math.nan)
        item.update(fill_label=int(quantity > 0),
                    conditional_markout_bps=markout if quantity else math.nan,
                    opportunity_markout_bps=markout if quantity else 0.0)
        rows.append(item)
    result = pd.DataFrame(rows, columns=[*joined.columns, "fill_label", "conditional_markout_bps",
                                        "opportunity_markout_bps"])
    result = result.merge(pd.DataFrame(feature_rows), on="opportunity_id", validate="many_to_one")
    result.attrs.update(input_manifest_id=cursor.input_manifest_id,
                        input_contract_id=cursor.bundle.manifest["input_contract_id"],
                        observation_contract_id=cursor.bundle.manifest["observation_contract_id"],
                        feature_contract_id=cursor.bundle.manifest["feature_contract_id"],
                        feature_columns=list(feature_columns), missing_policy=missing_policy,
                        training_intervals_ns=(None if training_intervals_ns is None else
                                               [list(pair) for pair in training_intervals_ns]),
                        outcome_support_counts=support_counts,
                        support_role=support_role,
                        training_admitted=support_role == 'training',
                        label_units="maker_signed_bps_per_opportunity_not_net_pnl")
    return result


def train_opportunity_models(panel, output, *, input_identity, feature_columns, side,
                             missing_policy, training_boundary_ns, bucket_edges,
                             bucket_values, adverse_threshold_bps, parameters,
                             num_boost_round, training_intervals_ns=None):
    """Fit the complete quote-EV bundle without the historical trace cleaner.

    All fitting rows are pre-bound opportunity outcomes. Hyperparameters,
    buckets and adverse threshold are explicit experiment inputs; this function
    does not search them or read a validation/final set. Missing labels are not
    eligible training rows. Publication is create-only and atomic.
    """
    import json
    import os
    import shutil
    import tempfile
    from pathlib import Path

    import lightgbm as lgb
    import numpy as np

    from data.tardis_input import CONTRACT
    from research.families.f05_fill_quality_quote_ev.quote_ev import quote_side_model_names

    if panel.attrs.get('training_admitted') is False:
        raise ValueError('engineering observation support is not a training contract')

    identity_keys = ("input_contract_id", "observation_contract_id", "feature_contract_id",
                     "training_contract_id", "label_contract_id")
    if (any(not input_identity.get(key) for key in identity_keys)
            or input_identity["input_contract_id"] != CONTRACT
            or any(input_identity[key] != panel.attrs.get(key) for key in identity_keys[:3])):
        raise ValueError("training requires compatible prepared panel contracts")
    if (panel.attrs.get("feature_columns") != list(feature_columns)
            or panel.attrs.get("missing_policy") != missing_policy
            or missing_policy not in {"reject", "native_nan"}):
        raise ValueError("training feature/missing contract differs from panel")
    if side not in {"bid", "ask"} or num_boost_round < 1:
        raise ValueError("explicit maker side and positive boosting rounds required")
    if (len(bucket_values) != len(bucket_edges) + 1
            or any(a >= b for a, b in zip(bucket_edges, bucket_edges[1:], strict=False))
            or not all(math.isfinite(v) for v in [*bucket_values, *bucket_edges, adverse_threshold_bps])):
        raise ValueError("explicit finite ordered markout buckets required")
    if {"objective", "num_class", "metric"} & set(parameters):
        raise ValueError("objective and class semantics are fixed by the label contract")
    training_intervals_ns = (None if training_intervals_ns is None else
                             [list(pair) for pair in training_intervals_ns])
    if panel.attrs.get('training_intervals_ns') != training_intervals_ns:
        raise ValueError("training interval support differs from prepared panel")
    support, fit_support_counts = _outcome_support(panel, training_boundary_ns, training_intervals_ns)
    rows = panel[(panel.side == side) & support].copy()
    if rows.empty or rows.duplicated(["opportunity_id", "horizon_ns"]).any():
        raise ValueError("nonempty unique opportunity/horizon fitting rows required")
    names = quote_side_model_names(side)
    end = rows[rows.horizon_ns == 30_000_000_000]
    heads = [(names["fill_prob"], end, end.fill_label, "binary")]
    for horizon, name in names["markout_buckets"].items():
        part = rows[(rows.horizon_ns == horizon * 1_000_000_000) & (rows.fill_label == 1)]
        part = part[np.isfinite(part.conditional_markout_bps)]
        target = np.digitize(part.conditional_markout_bps.to_numpy(), bucket_edges)
        heads.append((name, part, target, "multiclass"))
    filled = end[end.fill_label == 1]
    filled = filled[np.isfinite(filled.conditional_markout_bps)]
    heads.append((names["extreme_adverse"], filled,
                  (filled.conditional_markout_bps <= adverse_threshold_bps).astype(int), "binary"))
    for _, part, target, _ in heads:
        if part.empty or len(np.unique(target)) < 2:
            raise ValueError("each quote-EV head needs observed support for at least two classes")
        matrix = part[list(feature_columns)].to_numpy(dtype=float)
        if np.isinf(matrix).any() or (missing_policy == "reject" and np.isnan(matrix).any()):
            raise ValueError("training matrix violates declared missing policy")
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".quote-ev-", dir=output.parent))
    try:
        for name, part, target, objective in heads:
            horizon_ns = int(part.horizon_ns.iloc[0])
            horizon_rows = rows[rows.horizon_ns == horizon_ns]
            head_support = {
                "horizon_ns": horizon_ns,
                "panel_time_eligible_rows": len(horizon_rows),
                "observed_no_fill_rows": int((horizon_rows.fill_label == 0).sum()),
                "filled_missing_conditional_markout_rows": int((
                    (horizon_rows.fill_label == 1)
                    & ~np.isfinite(horizon_rows.conditional_markout_bps)).sum()),
                "fit_rows": len(part),
                "producer_time_support": panel.attrs.get("outcome_support_counts", {}).get(
                    f"{side}:{horizon_ns}"),
            }
            params = {**parameters, "objective": objective}
            if objective == "multiclass":
                params["num_class"] = len(bucket_values)
            model = lgb.train(params, lgb.Dataset(part[list(feature_columns)], label=target,
                                                feature_name=list(feature_columns)),
                              num_boost_round=num_boost_round)
            model.save_model(str(stage / (name + ".txt")))
            from .quote_ev import MODEL_SCHEMA
            meta = {**input_identity, "schema": MODEL_SCHEMA, "feature_cols": list(feature_columns),
                    "missing_policy": missing_policy, "name": name, "side": side,
                    "training_boundary_ns": training_boundary_ns, "fit_rows": len(part),
                    "actual_outcome_end_max_ns": int(part.actual_outcome_end_ns.max()),
                    "decision_min_ns": int(part.decision_ns.min()),
                    "decision_max_ns": int(part.decision_ns.max()),
                    "training_intervals_ns": training_intervals_ns,
                    "panel_outcome_support_counts": panel.attrs.get('outcome_support_counts'),
                    "parent_manifests": panel.attrs.get('parent_manifests'),
                    "fit_outcome_support_counts": fit_support_counts,
                    "head_support": head_support,
                    "parameters": params, "num_boost_round": num_boost_round,
                    "adverse_threshold_bps": adverse_threshold_bps,
                    "label_units": panel.attrs["label_units"]}
            if objective == "multiclass":
                meta.update(bucket_values=list(bucket_values), bucket_edges=list(bucket_edges),
                            classes=list(range(len(bucket_values))))
            (stage / (name + "_meta.json")).write_text(json.dumps(meta, allow_nan=False), encoding="utf-8")
        os.rename(stage, output)
    except BaseException:
        shutil.rmtree(stage)
        raise
    return {"model_directory": str(output), "heads": [head[0] for head in heads],
            "training_contract_id": input_identity["training_contract_id"],
            "economic_evaluation": "not_run"}
