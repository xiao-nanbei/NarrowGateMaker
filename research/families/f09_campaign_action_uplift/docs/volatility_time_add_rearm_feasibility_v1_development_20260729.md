# Volatility-Time Add Rearm Feasibility v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Decision

`volatility_time_add_rearm_feasibility_v1` is closed on Development mechanics. No randomized action experiment was created, and Validation, sealed holdout, action, and live permissions remain closed.

This result closes only the frozen individual-trade-close variance-clock v1 identity. It does not establish that realized-variance time is intrinsically unsuitable for add rearm.

## Frozen contrast

The baseline clock is:

\[
T_{\mathrm{baseline}}
=85\,\mathrm{s}\times
\max(1,n_{\mathrm{same\mbox{-}side\ fill\ units}}).
\]

The candidate freezes an episode-start budget:

\[
B_{s,e}=\nu_{\mathrm{ref},s}\times85\,\mathrm{s}\times
\max(1,n_{\mathrm{same\mbox{-}side\ fill\ units}}),
\qquad
QV_e(u)=\int_0^u \nu_t\,dt.
\]

It rearms when the cumulative variance budget is exhausted, subject to a 5-second minimum and a 600-second liveness cap. Add and reducing fills both increment same-side fill units; only an opposite-side fill resets them. Reducing quotes, size, inventory limit, P3, queue, and latency are unchanged.

The causal rate source is a 60-second raw variance of completed BTCUSDC individual-trade 1-second closes:

\[
\nu_t=10^8\frac{\sigma^2_{p,t}}{m_t^2}
\quad [\mathrm{bps}^2/\mathrm{s}].
\]

Missing, older-than-2-second, or invalid variance freezes the clock. ML volatility and quote-core blended volatility are forbidden.

## Data identity

- Development: 40 days, 2026-04-17 through 2026-06-26 on the frozen native-strict split.
- Reference-rate fit: first 20 Development days.
- Mechanics evaluation: last 20 Development days.
- Loaded fill rows: 22,854 across exactly 40 days; maximum 1,629 rows/day, below the 50,000 trace cap.
- Reference rates: BUY `1.4143509228` and SELL `1.5505750658` bps2/s.
- Python/C++ variance-clock mismatch count: zero.
- Validation and sealed holdout days read: zero.

The general campaign replay executable computes its normal accounting outputs, but the feasibility evaluator loads only lifecycle/mechanics columns and does not load reward, PnL, markout, EV, toxicity, MAE, or terminal fields. No gate or implementation was changed in response to those accounting outputs.

The source arm explicitly disables the concurrently enabled BUY q90 cancel/re-enter treatment because formal replay does not implement its complete cancel-ACK and queue-reset lifecycle. Therefore this is a cooldown-mechanics source, not a claim of complete current-live policy reproduction.

## Development result

| Gate | BUY | SELL |
|---|---:|---:|
| Episodes / days | 3,376 / 20 | 2,107 / 20 |
| n=1 / n=2 episodes | 1,684 / 1,168 | 888 / 843 |
| Earlier by more than 5s | 6.84% | 9.49% |
| Later by more than 5s | 49.73% | 39.44% |
| Candidate effective rate | 56.58% | 48.93% |
| Maximum-wall-time cap | 4.24% | 5.17% |
| Episode-start variance valid | 37.17% | 50.21% |
| Aggregate valid clock time | 20.42% | 28.67% |
| Python/C++ mismatches | 0 | 0 |
| Support gate | pass | pass |
| Two-sided timing gate | pass | pass |
| Clock-quality gate | **fail** | **fail** |

The failure is not caused by a no-op candidate or a stop-add liveness collapse. It is caused by the frozen trade-close variance source. Across the 20 evaluation days, only about 30,076 to 67,772 seconds/day contain a BTCUSDC trade bar. The share of seconds whose carried close is older than two seconds ranges from 3.18% to 34.70%. Requiring a clean 60-second window amplifies those gaps: the daily variance-valid share ranges from 1.67% to 50.66%, with a mean of 23.44%.

## Boundary

Do not create `volatility_time_add_rearm_action_v1` from this identity. Do not relax the frozen 95% quality gate, extend carry-forward, or search min/max wall times after observing this result.

A future re-open must use a genuinely new, live-reproducible causal clock source such as timer-driven executable-mid/BBO observations, with its own stale-state semantics, artifact identity, Python/C++ parity, and Development-only feasibility contract. It cannot relabel or patch this v1 result.

## Frozen evidence

- Spec SHA256: `170b0bb904757e8ba8f520dcf55d8a965680a72aa929f0ea2dd8b5f17fa6e98f`
- Source arm SHA256: `774706562610ba38d828665823c2bd45a56d5ccf81f00b952bdc7424ed27a21b`
- Source replay metadata SHA256: `578c89af6cae54a9f3a840b1e3c1d4740493553f9a8efbea8aee32c3a627ee48`
- Source fill trace SHA256: `c64df393ffd2ab1624ba067563a22295702471e6dec17ee7ae54d1927803832b`
- Development report SHA256: `2c83011087cbbdbbffafd82995d3b56709309fc5c98dcbe96d10f5e2393cc779`
- Development manifest SHA256: `15384c09b98c62ae76ba84f9fd01a39a56e423d3be695f28fc0574cfd951a1fd`
