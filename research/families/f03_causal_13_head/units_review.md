# F03 input migration: dimensional review map

[English](units_review.md) | [简体中文](units_review.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

This is a review map, not certification that all consumers are dimensionally correct. The completed economic results describe the frozen execution implementation; a subsequently confirmed defect requires impact analysis and a separately identified correction, never editing the old receipts or retuning against opened Final outcomes.

| Quantity | Required distinction | Review boundary |
| --- | --- | --- |
| Price / absolute distance | USDC/BTC, not ticks, bps or fractional return | P3 calibration → labels → quote distance |
| Position / trade size | BTC, not quote notional or trade count | Input parser → fills → cash and inventory |
| Variance rate | (USDC/BTC)² per second, not standard deviation or dimensionless return variance | Label/model metadata → vol prediction → quote risk |
| Direction / toxicity | Probabilities or declared scores, not currency amounts | Per-head label → inference → action mapping |
| Return | Inspect its touch-conditioned target and normalization; do not infer units from a variable name | Label generator → replay bridge → live signal |
| Fee / funding | Dimensionless rate versus signed USDC cash flow; pay/receive sign and notional basis | Fill and funding tape → account ledger |
| Time | CSV µs, internal ns, API ms, horizons seconds and half-life UTC days are distinct | Parsing → observation → labels → funding cutoff |
| Counts | Individual trades, aggregate messages and unavailable native packet counts differ | Source adapter → rolling windows → model features |
| Terminal value | BTC inventory × USDC/BTC mark; not free liquidation | Per-shard equity → daily attribution → aggregate |

Inspect [public input labels](public_input_panel.py), [model validation](../../../strategy/public_model_contract.py), [variance contract](../../../strategy/model_contract.py), [live protocol](../../../live/feature_protocol.py), [observation clocks](../../../data/observation.py), [economic accounting](../../../models/replay/public_accounting.py) and their actual callers. Paths from this family to repository roots use three parent levels.

Tests should include dimensional scaling (price ×10, size ÷10 where notional should remain fixed), seconds/ms/µs/ns conversions with nonzero offsets, bps ↔ fraction ↔ absolute price, funding pay/receive signs and exact boundary timestamps, unknown versus observed zero, trade aggregation count changes, and variance versus standard-deviation transformations. Scaling is a metamorphic check, not permission to change tick/lot constraints or frozen data.

The boundary funding at 2026-09-12 00:00:00.001 UTC must remain after the account ending at exact midnight. Provider receipt is not local strategy receipt. NTP synchronization status alone is not a measured latency or causal-clock proof. ML-OFF bypasses model loading; thirteen trained heads do not mean thirteen active replay fields. Report confirmed defects, plausible risks and unavailable evidence separately, with exact producer/consumer locations and a reproducing test.
