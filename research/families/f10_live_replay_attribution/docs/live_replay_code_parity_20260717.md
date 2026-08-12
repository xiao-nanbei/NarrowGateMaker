# Live/Replay Code Parity Check (2026-07-17)

> Current status (2026-07-27): retain this as an implementation-parity record for the 2026-07-17 code identity. It does not certify current model/data parity and its named model artifacts are superseded. Later repairs changed calendar conversion, commission handling, daily PnL, tick/lot propagation, post-fill volatility and legacy bar clocks; current claims must bind the `causal-v7` and `time_unit_contract_repair_20260726` identities.

## Scope

This check deliberately excludes historical EC2 fills. Pre-repair live days used the old volatility-unit and markout timing semantics, so comparing those fills with the corrected replay would mix two policies.

The target is narrower and auditable: given the same current state and configuration, live and the authoritative Python replay must consume the same quote/risk policy contracts.

## Fixed code-level drift

- `circuit_breaker_sigma` and `pnl_volatility_horizon_s` now map from live config into Python and C++ replay. All three use the same absolute-price variance predicate:

  ```text
  loss_threshold_usdc = sigma_multiple
                      * sqrt(sigma2_price_per_s * horizon_s)
                      * abs(inventory_btc)
  ```

- The duplicated live/replay side policy was replaced by `strategy.policy_guards.evaluate_common_side_policy`. It shares stale, toxicity, markout, adverse/local-extreme, defense, burst, thin-depth, inventory-limit, spread, size, and exposure-only semantics.
- C++ replay now evaluates that common policy unconditionally for both sides. The older `replay_side_policy_mult` path only inspected a shallow depth field and could apply a false `1.1` thin-depth multiplier even when exact L2 showed sufficient depth. Spread, size, hard-pause, and exposure decisions now use the same common-policy result as Python/live.
- Exposure-only adverse/local-extreme/burst guards no longer become accidental all-side pauses in replay.
- The BUY fill-selection scorer consumes the shared side-policy result and the same causal feature-row builder used by live. C++ replay ABI v3 now receives per-ready-row fold contributions compiled from causal `Prediction.feature_dict`, then combines them with quote-time dynamic features and the same actionable gate. Real models containing static features fail fast on older ABIs instead of silently scoring an incomplete row.
- The private deployment config makes the previously implicit horizons explicit: quote `1s`, markout `10s`, risk `300s`, EMA span `50 fills`.

## Verification

- Full local suite: `355 passed, 4 skipped`.
- The production five-fold BUY artifact has an end-to-end Python/C++ contract test covering score mean/max, evaluation count, and actionable hit count. Static payload transport, missing/used counts, inventory-limit actionability, and legacy-ABI fail-fast have separate native tests.
- Strict native extension rebuilt on the target Python runtime and passed the startup ABI preflight.
- All four real-data golden windows pass strict Python/C++ equality for summary, PnL path, fills, inventory time, and trace lengths. The former `final_cap_compress_rate` drift was a counting mismatch: Python omitted a post-policy-only compression already counted by C++. A second high-activity residual exposed the stale C++ side-policy path described above; after its removal, `avg_final_spread` is also identical without relaxing tolerances.
- The deploy config was compared leaf by leaf against the pre-deploy runtime config. The only additions were the four explicit timing fields above; strategy parameters, six external venue sources, the `USDCUSDT` anchor, and receive-time tape settings were preserved.

## Remaining non-parity boundaries

These are not hidden code-policy differences:

- user-stream reconciliation, sync-adjust degradation, watchdog reconnects, exchange ACK/fill races, and account-level loss controls are live-only;
- queue priority, hidden liquidity, and cancellation position are not publicly observable and require statistical queue calibration;
- historical replay requires deep queue L2 in addition to the top-20 feature stream;
- the public golden bundle lacks formal P3 calibration and is suitable for code-policy parity, not strategy promotion.

Python remains the easiest diagnostic authority, while C++ ABI v3 now covers the active BUY scorer and shared side-policy surface and passes the full golden set. Formal strategy promotion still requires a calibrated P3/queue/latency bundle; unsupported or incomplete model payloads continue to fail fast.
