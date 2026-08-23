# Public Replay Demo

Last materially modified: 2026-08-23

Status: Distributed synthetic mechanics fixture; non-economic and not promotion-eligible.

## Question And Result

Can a new user exercise NarrowGate's market-event, maker-queue, order-fill, fill-denominator, inventory-campaign, terminal-PnL, and evidence-gate path without downloading market data or contacting an exchange? Yes. This fixture runs that path deterministically and its passing gate means only that the published demonstration mechanics match their frozen contract.

## Run

From the repository root with Python 3.11 or newer:

```bash
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

The command writes `summary.json`, `trace.jsonl`, and `receipt.json`. A second run with the same source bytes produces byte-identical files. The receipt timestamp is a frozen contract value; the runner does not read the runtime clock.

## Fixture

- [`contract.json`](contract.json) freezes the synthetic classification, denied permissions, input SHA256, denominator, expected terminal values, and gate authority.
- [`synthetic_tape.jsonl`](synthetic_tape.jsonl) is a hand-authored top-of-book and trade sequence. It is not exchange data and has no empirical or economic authority.
- [`reference/`](reference/) contains the byte-for-byte expected summary, event trace, and receipt for the distributed engine bytes.
- [`../../narrowgate/cli.py`](../../narrowgate/cli.py) exposes the canonical public command and delegates to the existing [`../../scripts/narrowgate_replay_demo.py`](../../scripts/narrowgate_replay_demo.py) engine. That reference engine supports only this small FIFO top-book teaching contract; it is not the private-data full replay and does not claim live fidelity.

## Evidence Boundary

The runner imports the repository's continuous accounting ledger for cash, inventory, campaign closure, and marked equity. Queue depletion and synthetic order lifecycle mechanics are intentionally implemented by the documented reference engine because the complete historical replay requires separately governed market tapes and calibration artifacts that are not part of this fixture.

The gate always keeps `economic_evidence_eligible`, `promotion_eligible`, and `live_action_eligible` false. It cannot access the network, import the live runtime, submit external orders, read private evidence, or silently substitute local market data. A tape hash mismatch fails before replay; a denominator, accounting, or terminal mismatch writes a `failed_closed` gate.

Evidence availability: every tape, contract, engine, summary, trace, and receipt byte referenced by this demo is distributed in this public directory or at the repository-relative source paths named by the receipt. No private evidence is used.
