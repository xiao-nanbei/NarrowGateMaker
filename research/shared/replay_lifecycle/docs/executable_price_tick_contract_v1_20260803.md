# Executable Price Tick Contract v1

Last materially modified: 2026-08-09

## Decision

The replay stack now separates continuous valuation from executable price identity:

\[
\text{continuous quote/value math}=\texttt{double},
\qquad
\text{order/trade/book identity}=\texttt{int64 price tick}.
\]

Theoretical mid, microprice, reservation price, volatility, markout, and PnL remain floating point. After a quote becomes executable, order prices, book levels, exact-level queue matching, and trade-cross boundaries use integer ticks.

The authoritative crossing rules are:

\[
\begin{aligned}
\text{BUY fill boundary}:&\quad t_{trade}\le t_{order},\\
\text{SELL fill boundary}:&\quad t_{trade}\ge t_{order},\\
\text{same queue level}:&\quad t_{trade}=t_{order}.
\end{aligned}
\]

Floating-point tolerance may validate whether an input lies on an exchange tick. It must not define level identity or executable crossing.

## Confirmed Failure Mode

The legacy C++ and Python replay compared executable prices as doubles. A SELL order reconstructed as `647146 * 0.1` can be represented as `64714.600000000006`, while the parsed trade is represented as `64714.6`. The old comparison rejected that nominally equal-price trade and could wait for the next tick.

The new replay exposes diagnostic counters comparing the authoritative integer decision with the old double decision. These counters never alter the new decision.

## Current Baseline

The pre-correction 71-day economic output was removed because its BER feature clock did not reproduce live. Integer-tick crossing remains a mandatory replay invariant, but that old PnL panel has no authority. The current compatibility control is the [`current_live_held_ber_replay_baseline_50d_20260810`](../../../families/f10_live_replay_attribution/docs/current_live_held_ber_replay_baseline_50d_20260810.json): its immutable first 40 days have terminal MTM `-144.251748 USDC`, while the pooled 50 days have terminal MTM `-165.566079 USDC`, closed-campaign value `-168.530979 USDC`, and 20,147 fills. This remains a top-20/100ms diagnostic, not raw-native order-path authority. The strict raw-native latency successor has passed one-day mechanics but has not completed its 50-day panel.

## Authority Boundary

This is a shared replay-infrastructure correction, not a strategy action. It does not authorize a live change or promote provider-normalized days to exact queue authority. The run is daily fresh-start, not continuous-live parity.

Frozen historical Specs and reports remain byte-identical. Any unexecuted research identity bound to the old replay implementation hash must fail closed and use a successor identity before execution.

Machine-readable contract: [executable_price_tick_contract_v1_20260803.json](./executable_price_tick_contract_v1_20260803.json)
