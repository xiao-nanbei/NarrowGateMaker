# Causal v12 Owner-Authorized Live Canary

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: active operational live canary; research promotion remains closed.

## Boundary

The project owner explicitly authorized a reversible live validation because its operational cost is low relative to continued infrastructure work. This changes the rolling live baseline, but it does not rewrite the frozen v12 economic screen:

- `ranking_score` remains null;
- the two 22-day native panels were previously read and are historical diagnostics, not independent confirmation;
- the earlier panel failed PnL, fill-retention, inventory-time, q10 and CVaR gates;
- the later panel had a positive PnL lower bound but still failed fill retention and campaign q10;
- prediction, automatic promotion and research-derived live authority remain false.

## Runtime Identity

- Effective UTC: `2026-08-01T23:41:55Z`.
- Python: `3.12.13`; `.venv` and `.venv-active` both resolve to `.venv-py312`.
- Model: 13 strict LightGBM heads, 173 features per head, `feature_semantics_version=6`.
- Feature DAG: `live_10s_signal_cutoff.v1`, SHA256 `aeaa171295f9b815d864ffa8242c55003cf6d25aa104301a1144a6eafd06e517`.
- Config SHA256: `93a1a203aacee95466dc032f8b9fc7916b7a2daaf2ee31a1b1142506362ebc4f`.
- Canary authorization SHA256: `903864ba8cc5497044d90257fc6f65d31d9bf1490b1a51e2779ef185cef5d589`.
- Empirical P3 is unchanged, SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`.
- q90, queue, latency, cooldown, size, inventory limits and hard safety gates are unchanged.

## Startup Repair

The first activation failed closed after the model loaded because REST warmup ended in a partial 10-second bucket and the first execution-market WS trade arrived later. The warmup path had not emitted the intervening causal flat 1-second bars, so the exact-grid invariant stopped the process.

The service was restored to ML-OFF from a hash-bound rollback archive. The live ingestion path was then fixed to bridge the final prefill bar to the first newer WS trade. Exact-grid validation remains strict for ordinary runtime gaps. The repair passed `1258` tests with `4` skips and a Python 3.12/C++ strict smoke using the actual v12 bundle.

After the second flat-state restart, the process crossed multiple 10-second buckets, emitted non-neutral model predictions, continued quote decisions and reported no new fatal error.

## Authority

This document records what is running. It is not evidence that v12 passed its frozen prediction or economic gates. Future experiments must use this canary as the rolling operational baseline while separately preserving the historical ML-OFF comparator and the rollback artifacts.

The machine-readable identity is `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260802.json`.
