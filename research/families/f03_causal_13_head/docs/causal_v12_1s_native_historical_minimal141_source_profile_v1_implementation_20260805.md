# causal-v12 1s Native Historical minimal141 Source Profile v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

`native_historical_minimal141_individual_reference_v1` is implemented as an explicit `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` source profile. The frozen 40-day Development denominator was resolved and probed read-only with 40 accepted days and zero rejected days.

This amendment converts the coverage-audited candidate into a resolver profile. It does not materialize features, train or score a model, read predictions or PnL, authorize an economic replay, or grant live authority.

The machine-readable implementation record is [`causal_v12_1s_native_historical_minimal141_source_profile_v1_implementation_20260805.json`](causal_v12_1s_native_historical_minimal141_source_profile_v1_implementation_20260805.json).

## Exact Profile

All paths are relative to `${NARROWGATE_DATA_ROOT}`:

| Component | Exact path or identity |
|---|---|
| Local trade tempo | `trade_features_causal_v5_expanded_20250801_20260725/BTCUSDC` |
| Local manifest | `trade_features_causal_v5_expanded_20250801_20260725/manifest.json` |
| Native L2 | `normalized_l2_100ms_v2_minimal141_20260727/l2` |
| Native L2 manifest | `normalized_l2_100ms_v2_minimal141_20260727/manifest.json` |
| Native L2 quality | `normalized_l2_100ms_v2_minimal141_20260727/daily_quality.csv` |
| Metrics | `raw_metrics` |
| BTCUSDT reference | `reference_bars_1s_trades_v1` |
| L2 clock | `cryptohft_transaction_time_100ms_grid` |
| Reference identity | `binance_futures_reference_individual_trades_1s.v1` |

The L2 manifest and daily-quality CSV are both included in the daily bundle identity. Each exact L2 Parquet is checked against both authorities for SHA256, size, row count, filename, reconstruction mode, source label, and 20-level schema.

All non-L2 authority checks, including metrics source-clock semantics, are delegated directly to the current `daily.probe_source_bundle()`. The profile does not copy the coverage auditor's former physical-row-order assumption.

## Role Contract

The previous natural UTC day is resolved directly as D-1. It must have `warmup_valid=true`. The target day must have `formal_eligible=true`, `target_source_valid=true`, and `sequence_valid=true`, with no formal exclusion reason. A target-rejected day may therefore serve as warmup only when its separate warmup role is valid; no older day may be substituted.

No glob lookup, alternate D-1 search, or aggregate-trade `bars_1s` replacement is present. Missing exact files fail before probing.

The existing `provider_normalized_v1` and `native_normalized_v1` profiles retain their original per-day JSON quality-authority semantics.

## Verification

Using `.venv/bin/python`:

```text
unit/default: 11 passed, 2 skipped
${NARROWGATE_PRIVATE_EVIDENCE_ROOT} read-only 40-day plus 07-19/07-20 integration: 13 passed in 162.08s
ruff check: passed
ruff format --check: passed
```

The integration test invokes `build_orico_daily_source_spec()` for every frozen Development day. It runs the full physical probe rather than trusting the coverage report's prior accepted flags.

## Provenance

The implementation binds the read-only coverage audit JSON SHA256 `2b2e4551977e3efa43072dab0b9e1e8cfdc94fb46eb720c4916cf33fae86ed8e`. The resolver implementation SHA256 is `a86850a66e138e12772206effc0b823d163efb9f4b3702332b05254e3cfd667a`.

The coverage audit's earlier `2026-07-19` metrics rejection is superseded by the repaired `daily_sources` source-clock contract. Direct probes now accept both `2026-07-19` and `2026-07-20` with 288 unique start-stamped metrics rows stably ordered by timestamp.

The wider 22+22 native transport denominator now has no metrics blocker. Its remaining physical gap is the seven individual-trade reference artifacts for `2026-04-12`, `2026-05-07`, `2026-05-08`, `2026-05-10`, `2026-05-11`, `2026-05-14`, and `2026-05-16`. Once those exact artifacts and authorities are admitted, this profile is expected to cover 44/44. This amendment does not authorize aggregate-bar substitution or denominator expansion before that admission.
