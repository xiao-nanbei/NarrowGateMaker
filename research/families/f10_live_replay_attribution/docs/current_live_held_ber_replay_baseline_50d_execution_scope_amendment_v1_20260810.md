# Current Live-Held BER 50-Day Replay Execution Scope Amendment v1

Date: 2026-08-10

This amendment corrects the execution-evidence scope of `btc_usdc_current_live_held_ber_replay_baseline_50d_20260810`. It does not rewrite the frozen 40-day prefix or the reported economics.

## Reported Diagnostic Economics

| Panel | Terminal MTM | Per day | Closed campaign | Fills |
|---|---:|---:|---:|---:|
| Immutable first 40 days | -144.251748 USDC | -3.606294 | -147.466348 USDC | 17,118 |
| Added 10 Grade-A days | -21.314331 USDC | -2.131433 | -21.064631 USDC | 3,029 |
| Pooled 50 days | -165.566079 USDC | -3.311322 | -168.530979 USDC | 20,147 |

All 50 days use the same replay implementation. The first 40 days reproduce the previous output exactly; the added dates do not use a different queue or latency path.

## Correct Execution Classification

The implementation passes `trades`, normalized `bbo_data`, and normalized top-20/100ms `l2_data` to the C++ replay. It does not pass a raw native snapshot/delta `exchange_book_event_tape`. Consequently:

- `exchange_book_queue_mode` is effectively `disabled`;
- no raw native queue scheduler events or exact queue lookups are consumed;
- new-order and cancel-order latency remain at the zero-latency defaults;
- no AWS Tokyo execution-book visibility-age profile is loaded;
- WebSocket receive/feature-ready lag, packet gaps, reconnects, and lock wait are not represented.

The correct evidence label is therefore:

`native_derived_top20_100ms_cpp_daily_fresh_start_diagnostic`

The output remains useful as a common-simulator paired diagnostic and as an immutable historical denominator. It is not strict raw-native queue evidence, live transport evidence, or continuous-live PnL evidence. It cannot by itself authorize a price, replace, cooldown, cancel/re-entry, fill-selection, or other order-path action.

## Strict-Native Latency Successor

The next order-path baseline must use all of the following in both arms:

1. Python-authoritative raw CryptoHFT snapshot/delta replay in `strict` mode.
2. The complete previous natural UTC day as 24-hour book warmup.
3. Individual exchange trades as fill truth.
4. A frozen AWS Tokyo receive/feature-ready visibility-latency profile applied only to the strategy-visible book and trade cursors.
5. Frozen empirical new-order and cancel-order latency samples.
6. Nonzero native event and queue lookup counters, zero missing queue seeds, and zero source sequence/gap/time-reversal violations.
7. Separate exchange-time truth, policy-visible time, and private fill-visible time in the emitted identity.

Raw snapshot/delta files and D-1 warmup paths are present for all 50 registered dates. Exact historical AWS receive-time tapes do not exist for those dates; the Development successor must therefore use a preregistered empirical Tokyo latency distribution and keep exact live-transport authority false. A separate prospective receive-time transport panel remains required before live action promotion.

As of 2026-08-10, this successor passes all-50-day source preflight and a one-day strict mechanics run on `2026-06-29`. That run consumed 5,086,247 raw events, completed 19,460 queue lookups with zero missing, and applied the sampled visibility delay 14,825 times. Its one-day economics differ materially from the compatibility path, so the old 50-day PnL must not be treated as a proxy for the pending strict panel. The full result and limitations are in [`current_live_held_ber_strict_native_latency_baseline_50d_v1_one_day_mechanics`](current_live_held_ber_strict_native_latency_baseline_50d_v1_one_day_mechanics_20260810.md).

## Permission Boundary

- Current 50-day output: diagnostic paired-control compatibility only.
- Strict-native latency successor: required before interpreting order-path PnL.
- Prospective AWS receive-time transport: required before live action authority.
- Existing live configuration and order policy are unchanged by this amendment.
