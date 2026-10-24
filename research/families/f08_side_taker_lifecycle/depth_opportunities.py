"""Exact producer links; raw future outcomes are never decision covariates."""

from collections import Counter
import json


# Explicit pre-action fields from the frozen decision callback. Simulator queue
# state, activation evidence, future outcomes and final actions are NOT inputs.
VISIBLE_FIELDS = frozenset("""
best_bid best_ask mid inventory inventory_ratio pred_dir pred_ret toxicity
depth_age_s l2_book_cancel_ratio l2_book_refresh_ratio l2_near_depth_total
l2_quote_flip_rate markout_ema sigma_sq_raw sigma_sq_blended
fill_cooldown_elapsed_ms fill_cooldown_remaining_ms fill_cooldown_total_ms
fill_cooldown_consecutive_units fill_cooldown_clock_mode last_side_fill_ts_ms
order_active_before same_side_pending_before managed_order_ids
prediction_generation_index feature_ready_generation_index feature_ready_ts_ms
decision_visible_bbo_index decision_visible_l2_index
decision_visible_trade_cutoff_ts_ms decision_visible_trade_index
inventory_lifecycle_id inventory_lifecycle_active inventory_lifecycle_side
inventory_lifecycle_age_s inventory_lifecycle_pnl_so_far
inventory_lifecycle_adverse_excursion_so_far inventory_lifecycle_max_abs_qty_so_far
inventory_lifecycle_exposure_increasing_fills_so_far
inventory_lifecycle_reducing_fills_so_far
""".split())


def build_opportunity_links(connection):
    """One row per routed OR excluded decision, including KEEP/nonfill.

    Optional callback joins require unique exact (read clock, side), strictly
    earlier producer sequence. No nearest-time matching or outcome-dependent
    sampling. Ambiguity is retained explicitly instead of guessed.
    """
    connection.execute("CREATE TABLE decision_opportunities(sequence INTEGER PRIMARY KEY, read_ns INTEGER, side TEXT, payload TEXT)")
    connection.execute("CREATE TABLE event_links(kind TEXT, source_sequence INTEGER, target_sequence INTEGER, order_id TEXT)")
    connection.execute("CREATE INDEX event_clock ON events(event_ns,side,kind)")
    counts = Counter()
    with connection:
        for sequence, kind, now, side, payload in connection.execute(
                "SELECT sequence,kind,event_ns,side,payload FROM events WHERE kind IN ('decision','decision_route_excluded') ORDER BY sequence"):
            decision = json.loads(payload)
            candidates = list(connection.execute(
                "SELECT sequence,payload FROM opportunities WHERE read_ns=? AND side=? AND sequence<?", (now, side, sequence)))
            peer_count = connection.execute(
                "SELECT count(*) FROM events WHERE event_ns=? AND side=? AND kind IN ('decision','decision_route_excluded')", (now, side)).fetchone()[0]
            optional = None
            join = "not_emitted_for_excluded_route" if kind == "decision_route_excluded" else "missing_or_ambiguous"
            if kind == "decision" and len(candidates) == 1 and peer_count == 1:
                optional_sequence, raw = candidates[0]
                optional = json.loads(raw)
                join = "unique_exact_clock_side_and_producer_order"
                connection.execute("INSERT INTO event_links VALUES(?,?,?,NULL)",
                                   ("optional_to_decision", optional_sequence, sequence))
            counts[join] += 1
            # Missing original context remains missing, never defaulted to zero.
            visible = {k: decision[k] for k in VISIBLE_FIELDS if k in decision}
            visible.update({k: v for k, v in decision.items() if k.startswith("decision_feature_")})
            if kind == 'decision_route_excluded':
                visible['prediction_generation_index'] = decision.get('prediction_index')
                visible['feature_ready_generation_index'] = decision.get('feature_ready_index')
            if optional:
                visible["optional_context"] = {k: optional.get(k) for k in (
                    "features", "action_context", "compute", "local_inventory", "order_id", "old_price", "old_quantity", "role")}
            row = dict(read_ns=now, side=side, decision_event_id=sequence,
                       kind=kind, action=decision.get("action"),
                       old_price=optional.get("old_price") if optional else None,
                       target_price=optional.get("target_price") if optional else decision.get("final_price"),
                       book_version=optional.get("book_version") if optional else None,
                       optional_link=join, I_current=visible,
                       original_context_complete=False,
                       context_note="retained visible callback fields; full baseline information support requires separate verification")
            connection.execute("INSERT INTO decision_opportunities VALUES(?,?,?,?)",
                               (sequence, now, side, json.dumps(row, allow_nan=False)))
            counts["decision_opportunities"] += 1
        # Explicit producer IDs are authoritative; zero is a valid order ID.
        for sequence, kind, order_id, payload in connection.execute(
                "SELECT sequence,kind,order_id,payload FROM events WHERE kind IN ('order_created','decision_action_revision')"):
            row = json.loads(payload)
            target = row.get("creation_opportunity_event_id" if kind == "order_created" else "opportunity_event_id")
            if target is None:
                counts["explicit_link_missing"] += 1
                continue
            found = connection.execute("SELECT sequence FROM decision_opportunities WHERE sequence=?", (target,)).fetchone()
            if found is None or target >= sequence:
                raise ValueError("invalid explicit decision producer link")
            connection.execute("INSERT INTO event_links VALUES(?,?,?,?)", (kind, target, sequence, order_id))
            counts[kind+"_linked"] += 1
        # Link full lifecycle through stable order identity, without exposing
        # terminal payloads to I_current. Censored orders remain visible.
        for creation, order_id in connection.execute("SELECT sequence,order_id FROM events WHERE kind='order_created'"):
            if order_id is None or connection.execute("SELECT count(*) FROM events WHERE kind='order_created' AND order_id=?", (order_id,)).fetchone()[0] != 1:
                raise ValueError("ambiguous order creation identity")
            for terminal, in connection.execute("SELECT sequence FROM events WHERE order_id=? AND kind IN ('order_outcome','order_observation_cutoff') ORDER BY sequence", (order_id,)):
                if terminal <= creation:
                    raise ValueError("terminal event precedes order creation")
                connection.execute("INSERT INTO event_links VALUES(?,?,?,?)", ("order_terminal", creation, terminal, order_id))
                counts["order_terminal_linked"] += 1
    return dict(counts)
