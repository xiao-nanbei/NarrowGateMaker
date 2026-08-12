# BER Role-Safe Add-Only Development Result

Last materially modified: 2026-08-09

Status: Closed on the frozen 40-day Development panel. No continuous confirmation, action authority, or live authority was created.

## Decision

The candidate kept the current BER signal, threshold `1.2`, and multiplier `2.0`, but bypassed BER for flat openers and reducing quotes. BER remained on exposure-increasing adds only.

Mechanically, the action was strong and correctly implemented:

- `172,328` effective side-price changes across all 40 days.
- Effective change rate `14.96%`.
- BUY/SELL changes `82,682 / 89,646`, each supported on 40 days.
- Python/C++ fill-path mismatch, BER-state mismatch, source mismatch, and infeasible cap count were all zero.

Economically, it failed:

| Metric | Control | Candidate | Candidate - Control |
|---|---:|---:|---:|
| Terminal MTM PnL | -144.2517 | -155.9180 | **-11.6663 USDC** |
| Closed-campaign value | -147.4663 | -156.8508 | **-9.3845 USDC** |
| Fills | 17,118 | 19,488 | **+13.85%** |

Terminal PnL changed by `-0.2917 USDC/day`, with paired 95% interval `[-1.1439, +0.6813]`; only `13/40` days improved. Negative-terminal protection also worsened by `-0.4621 USDC/day`.

The candidate did improve campaign q10, CVaR10, MAE, maximum inventory, inventory time, and both multi-level LONG/SHORT point estimates. Those proxy improvements did not compensate for the worse terminal and closed-campaign value. This is another direct example of faster repair and lower inventory not being sufficient economic objectives.

## Boundary

The current global BER remains enabled. This result does not prove threshold `1.2`, multiplier `2.0`, or the global signal definition is optimal. It does close the proposed B1 foundation, "same BER but add-only with opener/reducing bypass." The directional empirical BER v2 proposed on top of B1 is therefore not advanced on this consumed panel.

The control uses corrected live held-feature BER clock semantics. It is not numerically interchangeable with the older BER-retirement control, which used the prior replay update semantics.

The first scorecard emission misplaced support metadata. An append-only repair restored canonical support (`5,067` rows, 40 days, no support failures) and the economic hard-gate decision remained closed.
