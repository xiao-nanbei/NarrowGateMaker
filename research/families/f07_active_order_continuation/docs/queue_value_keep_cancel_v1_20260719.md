# Queue-Value Keep/Cancel v1

> Current status (2026-07-27): this exact v1 family remains closed. Its top-20/q0.70 coefficients, action values and numerical tables are historical counterfactuals, not native active-price queue evidence, and must not be used for current ranking. The non-promotion decision remains valid; a successor requires a new native exact-level estimand and family identity.

## Decision

`queue_value_keep_cancel_v1` did not pass the Development gate.

- Validation was not opened.
- The sealed holdout was not opened.
- External-market M1 was not evaluated.
- No replay candidate was promoted to live or shadow policy.
- SPIBB falls back to the `keep` baseline for every state.

This is a family-level rejection of the frozen v1 action definition, not a rejection of queue-value modeling in general.

## Frozen identity

The source universe contained 76 strict eligible UTC days:

| Panel | Days | Role |
|---|---:|---|
| Development | 56 | Chronological model fitting and OPE only |
| Embargo 1 | 1 | Excluded |
| Validation | 9 | Locked unless Development passes |
| Embargo 2 | 1 | Excluded |
| Sealed holdout | 9 | One-shot confirmation after Validation |

Development used an additional `44 fit / 1 embargo / 11 calibration` split for the queue model. The action family, thresholds, features, 50/50 propensities, random seed, data hashes, config hash, and code checkpoints were frozen before reading action outcomes.

The final direct-action-contrast code commits are:

- `2977b3d8`: queue-value OPE feature contract
- `bd5854c1`: frozen queue-family OPE report support
- `dc5767b1`: direct `K1 - K0` DR action contrast
- `6cf0db80`: SPIBB identity bound to uplift evidence

## State models

The local M0 state uses 100 ms reconstructed Binance BTCUSDC event-L2, individual trades, queue state, campaign-so-far state, and empirical microprice. It does not use Bitget, Bybit, or OKX.

The side-specific discrete-time queue-reactive approximation passed the Development calibration gate:

| Side | Adverse O/E | Cancel O/E | Refill O/E |
|---|---:|---:|---:|
| BUY | 1.016 | 1.225 | 1.270 |
| SELL | 0.972 | 1.161 | 1.199 |

The empirical one-tick first-hit model also improved over its fit-only constant null:

| Side | Multiclass Brier | Null | Direction Brier | Null |
|---|---:|---:|---:|---:|
| BUY | 0.6697 | 0.7046 | 0.2124 | 0.2500 |
| SELL | 0.6663 | 0.7007 | 0.2124 | 0.2500 |

This is a calibrated 100 ms discrete-time approximation, not a claim of a continuous-time Hawkes point-process fit. Historical L2 has exchange time but not a same-host receive-time clock, so it remains replay evidence rather than an executable latency proof.

## Randomized panel

Each inventory campaign received at most one intervention on its first active exposure-increasing add order that entered the frozen adverse state:

- `K0 = keep`
- `K1 = cancel_until_state_exit`
- exact logged propensity: `0.50 / 0.50`
- reducing orders, size, and inventory limit unchanged

The Development panel contains 3,263 campaigns:

| Side | K0 rows | K1 rows |
|---|---:|---:|
| BUY | 981 | 961 |
| SELL | 649 | 672 |
| Total | 1,630 | 1,633 |

The reward identity is:

\[
\text{reward}
=
\text{fill value}
-
\text{incremental campaign cost}
-
\text{queue-reset cost}.
\]

Its maximum numerical identity error was `1.39e-17`.

## Replay mechanism

The 50/50 randomized replay did not improve the aggregate baseline:

| Metric | Control | Randomized | Change |
|---|---:|---:|---:|
| Raw PnL | -303.28 | -306.61 | -3.33 |
| Fills | 32,021 | 31,542 | 98.50% retention |
| Campaign count | 10,351 | 10,634 | 1.027x |
| Inventory time | 1.000x | 0.986x | -1.37% |
| Positive PnL days | - | 24 / 56 | 42.9% |

K1 strongly changed the intended local mechanism. Relative to K0, the direct intervention-fill probability fell by `0.460` pooled, `0.475` on BUY, and `0.439` on SELL. The action is therefore not a no-op, but the removed fills did not translate into stable terminal value.

## Direct DR contrast

Promotion uses the paired action contrast

\[
\operatorname{DR}(K1)-\operatorname{DR}(K0),
\]

matched by the same decision and chronological fold. Candidate-versus-50/50 behavior-mixture estimates are retained only as diagnostics.

| Scope | Reward uplift | 95% day-bootstrap interval | Terminal uplift | Daily positive |
|---|---:|---:|---:|---:|
| Pooled | +0.01314 | [-0.03131, +0.06600] | +0.01315 | 15 / 25 |
| BUY | +0.01192 | [-0.03662, +0.06733] | +0.01190 | 10 / 25 |
| SELL | +0.01814 | [-0.05531, +0.11091] | +0.01836 | 13 / 25 |

Overlap itself was adequate:

| Scope | K1 ESS | K0 ESS | Unsupported mass | Max weight |
|---|---:|---:|---:|---:|
| Pooled | 733 | 769 | 0 | 2.0 |
| BUY | 428 | 458 | 0 | 2.0 |
| SELL | 305 | 311 | 0 | 2.0 |

The failure is therefore not missing action support. It is the negative lower bound, weak day-level sign consistency, and insufficient extreme-tail denominator. BUY had no terminal campaign at or below `-5 USDC` in either action arm; SELL had only one in each arm. Zero observed BUY tails is missing tail support, not proof that either action removes tail risk.

## SPIBB result

SPIBB required, per state:

- at least 100 K1 rows;
- at least 100 K0 rows;
- K1 effective sample size at least 100;
- day-clustered direct-uplift lower bound above zero.

| Side | States | Accepted | Policy ID | Result |
|---|---:|---:|---|---|
| BUY | 8 | 0 | `spibb-af6771ad87637d92` | Baseline fallback |
| SELL | 8 | 0 | `spibb-86df2a535ca799ad` | Baseline fallback |

Some supported states had a positive point estimate, but every state retained a non-positive lower bound. The artifact identity now includes the canonical uplift evidence hash, so different action evidence cannot reuse the same policy ID merely because both policies accept zero states.

## Interpretation

The local queue and first-hit models contain useful descriptive information: they calibrate event hazards and short-horizon price hitting better than a constant null. That information did not identify a stable policy value for the coarse action "cancel now and remain blocked until the full hysteresis exit."

The action removes many fills, but the value of the removed fills varies too much by day and side. This is exactly the distinction the randomized panel was meant to expose:

\[
\text{state prediction quality}
\not\Rightarrow
\text{action uplift}.
\]

Under the frozen governance rule, v1 stops here. Validation, sealed holdout, and external M1 remain untouched and cannot be used to rescue this family.
