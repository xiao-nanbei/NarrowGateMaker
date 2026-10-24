"""Complete journal ingestion and causal, fixed-price deep observation panels.

Raw order events remain separate from features: terminal outcomes never enter
the decision-time covariates. No strategy fitting or action intervention.
"""

import argparse
from collections import Counter
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from data.depth_observation import DepthStateTap
from data.facts import save_private_json
from research.families.f08_side_taker_lifecycle.depth_dynamics import VisibleDepthHistory, VisibleTradeHistory
from research.families.f08_side_taker_lifecycle.depth_information import canonical_tick, streams
from research.families.f08_side_taker_lifecycle.depth_opportunities import build_opportunity_links
from research.families.f08_side_taker_lifecycle.depth_history_log import VisibleHistoryWriter
from research.families.f08_side_taker_lifecycle.depth_current_information import CurrentInformation
from research.families.f08_side_taker_lifecycle.depth_regions import canonical_order_price


def ingest_journal(manifest, database, production):
    """Store every audited journal row, not the observer's bounded examples."""
    from models.replay.l2_journal import audit_l2_delivery
    from execution.chunked_parquet_journal import iter_chunked_parquet_journal
    audit_l2_delivery(manifest, production)
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE events(sequence INTEGER PRIMARY KEY, kind TEXT, event_ns INTEGER, side TEXT, order_id TEXT, payload TEXT)")
        connection.execute("CREATE TABLE opportunities(sequence INTEGER PRIMARY KEY, read_ns INTEGER, side TEXT, payload TEXT)")
        counts = Counter()
        with connection:
            for row in iter_chunked_parquet_journal(manifest, verify_checksum=True):
                payload = row["record"]
                encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False)
                order = payload.get("order_id", payload.get("trace_id"))
                connection.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (
                    row["sequence"], row["event_type"], row["event_ts_ns"], row["side"],
                    None if order is None else str(order), encoded))
                counts[row["event_type"]] += 1
                if row["event_type"] == "optional_update_observation":
                    if payload["side"] != row["side"] or type(payload["read_ns"]) is not int:
                        raise ValueError("opportunity identity or clock differs")
                    connection.execute("INSERT INTO opportunities VALUES(?,?,?,?)", (
                        row["sequence"], payload["read_ns"], row["side"], encoded))
            connection.execute("CREATE INDEX opportunity_clock ON opportunities(read_ns,sequence)")
            connection.execute("CREATE INDEX order_events ON events(order_id,sequence)")
        linkage = build_opportunity_links(connection)
        return dict(events=sum(counts.values()), counts=dict(counts), linkage=linkage,
                    opportunity_rows=counts["optional_update_observation"],
                    decision_rows=counts["decision"], route_excluded_rows=counts["decision_route_excluded"],
                    note="All callbacks/outcomes retained; spatial rows cover routed and excluded decisions with exact optional links, never the bounded sample list",
                    production=production, sample_truncation=False)
    finally:
        connection.close()


def run(config, output, *, journal=None, production=None, compare=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    save_private_json(output / "config.json", config)
    if journal is not None:
        receipt = ingest_journal(journal, output / "order-events.sqlite", production)
    else:
        receipt = None
    tap = DepthStateTap()
    verified = {}
    stream = streams(config, tap, verified)
    current_information = CurrentInformation(stream.profile, config.get('frozen_information'))
    history_log = VisibleHistoryWriter(output)
    reference = iter(streams(config, None, verified)) if compare else None
    history = VisibleDepthHistory(windows_ns=tuple(config["windows_ns"]),
                                  max_age_ns=stream.profile.max_book_age_ns)
    trade_history = VisibleTradeHistory(start_ns=config['start_ns'], windows_ns=config['windows_ns'],
                                        coverage=stream.profile.trade_coverage)
    connection = sqlite3.connect(output / "order-events.sqlite") if receipt else None
    opportunity_table = "decision_opportunities" if receipt and receipt['linkage'].get('decision_opportunities') else "opportunities"
    opportunities = iter(connection.execute(f"SELECT sequence,read_ns,side,payload FROM {opportunity_table} ORDER BY read_ns,sequence")) if connection else iter(())
    opportunity = next(opportunities, None)
    counters = Counter()
    wall = progress_at = time.monotonic()
    digests = [hashlib.sha256(), hashlib.sha256()]
    schema = pa.schema([("entrance", pa.string()), ("event_id", pa.string()),
                        ("read_ns", pa.int64()), ("side", pa.string()),
                        ("region", pa.string()), ("original", pa.string()),
                        ("depth", pa.string())])
    writer = pq.ParquetWriter(output / "depth-panel.parquet", schema, compression="zstd")
    rows = []
    last_ns = config["start_ns"]

    def flush():
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
            rows.clear()

    def emit(entrance, identity, now, side, name, original, low, high):
        value = history.query(side="bid" if side == "BUY" else "ask",
                              low=low, high=high, read_ns=now)
        value['visible_trades'] = trade_history.query(low=low, high=high, read_ns=now)
        # These are region support fields, not a claim that I_current is complete.
        value["complete_information_support"] = "not_certified"
        rows.append(dict(entrance=entrance, event_id=str(identity), read_ns=now,
                         side=side, region=name, original=json.dumps(original, allow_nan=False),
                         depth=json.dumps(value, allow_nan=False)))
        counters[entrance+"_region_rows"] += 1
        counters[entrance+"_covered"] += bool(value["available"])
        if len(rows) >= 1024:
            flush()

    def consume_opportunities(until, inclusive):
        nonlocal opportunity
        while opportunity is not None and (opportunity[1] <= until if inclusive else opportunity[1] < until):
            sequence, now, side, payload = opportunity
            original = json.loads(payload)
            original['original_visible_history'] = current_information.query(now, side, original)
            counters["opportunities_seen"] += 1
            if history.current is None:
                counters["opportunities_no_book"] += 1
            else:
                snap = history.current.snapshot
                points = snap.book.bids if side == "BUY" else snap.book.asks
                expected = original.get("book_version")
                # A failed join is missing, never silently replaced by nearest state.
                if expected is not None and expected != snap.book.version:
                    counters["opportunities_book_mismatch"] += 1
                elif points:
                    touch = points[0][0]
                    for name in ("old_price", "target_price"):
                        raw = original.get(name)
                        if raw is None or raw <= 0:
                            counters[name+"_not_applicable"] += 1
                            continue
                        price = canonical_order_price(raw, config['tick_size'])
                        emit("strategy", sequence, now, side, name+"_path", original,
                             min(touch, price), max(touch, price))
                        width = Decimal(str(config["tick_size"]))*config["neighbor_ticks"]
                        emit("strategy", sequence, now, side, name+"_neighborhood", original,
                             price-width, price+width)
            opportunity = next(opportunities, None)

    try:
        for tick in stream:
            if reference is not None:
                old = next(reference, None)
                left, right = canonical_tick(old), canonical_tick(replace(tick, research_depth_observations=()))
                if old is None or left != right:
                    raise ValueError(f"original input divergence at {tick.now_ns}")
                digests[0].update(left)
                digests[1].update(right)
            consume_opportunities(tick.now_ns, False)
            deep_by_id = {o.original.event_id: o for o in tick.research_depth_observations}
            for delivered in tick.observations:
                current_information.observe(delivered)
                trade_history.observe(delivered)
                history_log.trade(delivered)
                observation = deep_by_id.pop(delivered.event_id, None)
                if observation is None:
                    continue
                history.advance(observation)
                history_log.depth(observation)
                counters["depth_publications_delivered"] += 1
                if observation.original.publish_ns % config["sample_period_ns"] == 0:
                    for side in ("BUY", "SELL"):
                        points = sorted(observation.snapshot.points("bid" if side == "BUY" else "ask"),
                                        reverse=side == "BUY")
                        original_market = current_information.query(tick.now_ns, side, {})
                        for count in (10, 20, 800):
                            selected = points[:count]
                            if selected:
                                low, high = sorted((selected[0][0], selected[-1][0]))
                                emit("market", observation.original.event_id, tick.now_ns, side,
                                     f"top{count}_fixed_at_read", {"actual_levels": len(selected),
                                     "original_visible_history": original_market,
                                     "source_asof_ns": observation.original.source_asof_ns}, low, high)
            if deep_by_id:
                raise ValueError('deep observation without original delivery')
            consume_opportunities(tick.now_ns, True)
            last_ns = tick.now_ns
            if time.monotonic()-progress_at > 20:
                flush()
                save_private_json(output / "progress.json", dict(status="RUNNING", last_ns=last_ns,
                    counters=dict(counters), wall_seconds=time.monotonic()-wall))
                progress_at = time.monotonic()
        consume_opportunities(config["end_ns"]-1, True)
        if reference is not None and next(reference, None) is not None:
            raise ValueError("original input length changed")
        if opportunity is not None:
            raise ValueError("journal exceeds bound observation interval")
        flush()
    except BaseException as exc:
        save_private_json(output / "failure.json", dict(error=f"{type(exc).__name__}: {exc}", last_ns=last_ns))
        raise
    finally:
        writer.close()
        history_log.close()
        if connection:
            connection.close()
    result = dict(status="DEPTH_PANEL_COMPLETE", counters=dict(counters), order_journal=receipt,
                  observation_parity="PASS" if compare else "NOT_RUN",
                  input_hashes=[d.hexdigest() for d in digests] if compare else None,
                  resets=history.resets, source_regressions=history.regressions,
                  pending_publications=len(tap.pending), wall_seconds=time.monotonic()-wall,
                  information_increment="NOT_ESTIMATED", strategy_parity="NOT_CERTIFIED",
                  all_order_opportunities_ingested=receipt is not None,
                  opportunity_table=opportunity_table,
                  visible_history_rows=history_log.count,
                  interval=[config["start_ns"], config["end_ns"]])
    save_private_json(output / "result.json", result)
    save_private_json(output / "progress.json", result)
    return result


def validate_output(output, identity):
    """PoolRun business validator: panel completeness, not economic completion."""
    import pyarrow.parquet as pq
    output = Path(output)
    result = json.loads((output / "result.json").read_text())
    config = json.loads((output / "config.json").read_text())
    if (result.get("status") != "DEPTH_PANEL_COMPLETE" or
            config.get("identity") != identity or
            result["interval"] != [identity["start_ns"], identity["end_ns"]]):
        raise ValueError("panel identity/interval/completion differs")
    if (output / "failure.json").exists():
        raise ValueError("failed panel is not admitted")
    journal = result["order_journal"]
    if not journal or journal["sample_truncation"] or not result["all_order_opportunities_ingested"]:
        raise ValueError("complete producer-audited order journal required")
    with sqlite3.connect(f"file:{output / 'order-events.sqlite'}?mode=ro", uri=True) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("order event database corrupt")
        if (db.execute("SELECT count(*) FROM events").fetchone()[0] != journal["events"] or
                db.execute("SELECT count(*) FROM opportunities").fetchone()[0] != journal["opportunity_rows"] or
                result["counters"].get("opportunities_seen", 0) != db.execute(
                    "SELECT count(*) FROM " + ("decision_opportunities" if result["opportunity_table"] == "decision_opportunities" else "opportunities")).fetchone()[0]):
            raise ValueError("order opportunities dropped")
    count = pq.ParquetFile(output / "depth-panel.parquet").metadata.num_rows
    if pq.ParquetFile(output / "visible-history.parquet").metadata.num_rows != result['visible_history_rows']:
        raise ValueError('visible history rows dropped')
    if count != sum(v for k, v in result["counters"].items() if k.endswith("_region_rows")):
        raise ValueError("depth panel row count differs")
    return dict(valid=True, files=["config.json", "result.json", "progress.json", "depth-panel.parquet", "order-events.sqlite", "visible-history.parquet"],
                summary=dict(identity=identity, panel_rows=count, journal_events=journal["events"],
                             economic_complete=False, information_increment="NOT_ESTIMATED"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--journal")
    parser.add_argument("--production")
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    if bool(args.journal) != bool(args.production):
        parser.error("journal and producer receipt must be supplied together")
    print(json.dumps(run(json.loads(Path(args.config).read_text()), args.output,
        journal=args.journal, production=json.loads(Path(args.production).read_text()) if args.production else None,
        compare=args.compare)))


if __name__ == "__main__":
    main()
