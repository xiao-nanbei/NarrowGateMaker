# Public Replay Demo

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

Status: Runnable offline teaching example with bundled inputs and reference output.

## What You Can Try

Follow a buy order as it joins a queue, waits through a trade without filling, receives two partial fills, and creates inventory. A later sell closes that inventory; another sell is canceled without a fill. No account, API key, market-data download, or C++ build is required after installation. The prices and trades are hand-authored examples, not a strategy performance sample.

## Run

From the repository root in an activated Python 3.11-or-newer environment:

```bash
python -m pip install -e .
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

After installation, the command runs without network access. `--verify-reference` compares its output with the bundled reference before publishing files; it is optional when simply exploring the example.

| Output | What to inspect |
| --- | --- |
| `trace.jsonl` | One row per input event: submission, queue depletion, fills, cancellation, and terminal mark |
| `summary.json` | All three orders, filled and unfilled counts, inventory campaign, fees, cash, and terminal PnL |
| `receipt.json` | Input/output identity and the result of the mechanics checks; not permission to trade |

A second run with the same source bytes produces byte-identical files. The receipt timestamp is a frozen fixture value, not the current time.

## Follow The First Order

The following steps come directly from the published [tape](../../narrowgate/fixtures/replay_demo/synthetic_tape.jsonl) and [reference trace](../../narrowgate/fixtures/replay_demo/reference/trace.jsonl). Times are seconds after the synthetic tape starts; quantities are BTC.

| Time | Event | Queue and account effect |
| --- | --- | --- |
| 2s | Submit `demo-buy-001`: BUY 0.010 at 100.00 | Joins behind 0.015; inventory remains zero |
| 3s | Aggressive sell of 0.010 at 100.00 | Queue ahead falls to 0.005; our order does **not** fill |
| 4s | Aggressive sell of 0.009 at 100.00 | Consumes the remaining queue, then fills 0.004; inventory is 0.004 |
| 5s | Aggressive sell of 0.006 at 100.00 | Fills the remaining 0.006; inventory is 0.010 |
| 7–8s | Submit and fill `demo-sell-001` at 100.20 | Its own 0.005 queue is consumed first; the 0.010 sell returns inventory to zero |
| 10–12s | Submit then cancel `demo-sell-002` | No fill; this order still belongs in the order count |

With the fixture's zero fees, the completed 0.010 round trip earns `0.010 × (100.20 − 100.00) = 0.002 USDC`. The terminal mark adds no inventory value because the account is flat. This is an accounting example, not an expected return. Three fill events belong to two filled orders; a trade at the order's price does not itself imply a fill.

## Fixture

- [`contract.json`](../../narrowgate/fixtures/replay_demo/contract.json) freezes the fixture schema/engine version, input SHA256, denominator, expected terminal values, and deterministic receipt time. Historical classification/permission fields in that JSON are descriptive compatibility metadata; they do not grant capabilities or control output eligibility.
- [`synthetic_tape.jsonl`](../../narrowgate/fixtures/replay_demo/synthetic_tape.jsonl) is a hand-authored top-of-book and trade sequence. It is not exchange data and has no empirical or economic authority.
- [`reference/`](../../narrowgate/fixtures/replay_demo/reference/) contains the byte-for-byte expected summary, event trace, and receipt for the distributed engine bytes.
- [`../../narrowgate/cli.py`](../../narrowgate/cli.py) exposes the canonical public command and delegates to the packaged [`../../narrowgate/replay_demo.py`](../../narrowgate/replay_demo.py) engine. That reference engine supports only this small FIFO top-book teaching contract; it is not the private-data full replay and does not claim live fidelity.

## Evidence Boundary

The runner imports the repository's continuous accounting ledger for cash, inventory, campaign closure, and marked equity. The small FIFO top-of-book engine does not reproduce measured feed/order latency, cancel-ACK races, hidden liquidity, full historical L2, or actual exchange queue position. A passing reference check proves this teaching example is reproducible, not that historical maker fills or live PnL are exact.

The packaged engine fixes the demo classification, permissions, and output eligibility in constants: `economic_evidence_eligible`, `promotion_eligible`, and `live_action_eligible` always remain false, regardless of input declarations. The implementation has no exchange/network client or external-order path, does not import the live runtime, and reads only its supplied public fixture/reference inputs. A JSON field such as `network_access=false` is not an operating-system sandbox. Tape identity is verified once and reused in the receipt; a mismatch fails before replay. A denominator, accounting, or terminal mismatch writes a `failed_closed` gate.

Evidence availability: every tape, contract, engine, summary, trace, and receipt byte referenced by this demo is distributed in the public package or at the repository-relative source paths named by the receipt. No private evidence is used.

Next: use the [one-day data tutorial](../../docs/opensource/one_day_data_pipeline.md) for your own public trade archives, and check its data-status table before interpreting an order-book result.
