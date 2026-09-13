# Placement Fill Request-State Race v2: Implementation Freeze

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: mechanics, corrected replay ML ABI and causal feature-context identity frozen; corrected Development is complete. The full learned three-phase family closed because pending-cancel fill added no stable proper-score value. Validation and sealed holdout remain unread. No action or live deployment is authorized.

## Research Boundary

This family keeps the new-placement estimand:

\[
P(T_{fill}\le t,\ T_{fill}<T_{cancelACK}\mid do(a),x_0).
\]

It replaces the rejected marginal cancel-latency race with three explicit phases:

1. dynamic pre-request fill risk from activation to the deterministic baseline cancel request;
2. pending-cancel fill risk after the request while remaining quantity is live;
3. conditional request-to-ACK survival from causal state sampled again at the request.

The pending-fill and ACK heads share the same request-time system-load and market-shock features. Cancel request remains a frozen policy stopping time, not a natural stationary market hazard. KEEP/REPLACE remains a separate active-order estimand.

## Frozen Data Identity

- Strict order-level native-L2 universe: 76 UTC days, 2026-04-13 through 2026-07-25.
- Fully continuous campaign days: 46.
- Gap-censored order-level days: 30, containing 48 gaps over five seconds.
- Development: 50 days through 2026-06-25.
- Embargo: 2026-06-26.
- Validation: 10 sealed days, 2026-06-27 through 2026-07-07.
- Embargo: 2026-07-08.
- Family-specific sealed holdout: 14 days, 2026-07-09 through 2026-07-25.
- Causal feature context: 140 days through 2026-07-25; this artifact supplies only decision-visible state and has no prediction or action authority.
- Corrected individual-trade tempo source: 141 days through 2026-07-25; the feature-context manifest consumes the matching 140-day subset.

The gap registry censors each order risk interval at the last visible native-L2 event plus five seconds. No pre-request or pending-cancel interval may cross a segment boundary. Grade-B days are valid only for these censored order-level estimands, not whole-day campaign outcomes.

The authoritative specification is `docs/placement_fill_request_state_race_v2_spec_20260728.json`, SHA256 `5b5c89b5c43ea0ca9f3a186da759551be43effbeaca6eb0936bf4fedfaeb3721`. The builder now verifies every frozen source hash, implementation hash and the installed native-module hash before reading a panel.

The frozen feature-context manifest is `${NARROWGATE_RETIRED_DATA_ROOT}/features_btcusdc_causal_v10_minimal141_context_20260728/causal_feature_manifest.json`, SHA256 `4b4cef9fb3542badfd552f51f0b973a13d5af62dfbbf7b92d3826dd3002d7e3c`. Its corrected taker-tempo parent manifest is `${NARROWGATE_RETIRED_DATA_ROOT}/trade_features_causal_v4_minimal141_20260728/manifest.json`, SHA256 `c767991459a38d4bd82e13879e1658f9f4af2b1bd141c6218a866a60ba60d38b`. The CLI config, normalized L2, native book, queue, latency, visibility and feature-context paths must resolve to the exact source identities frozen in the specification; compatible-looking alternate inputs are rejected.

Formal day admission is owned by the frozen strict-day manifest and normalized quality registry. A legacy whole-day CryptoHFT audit remains the default for unregistered replay, but cannot veto an explicitly listed rebuilt day. The manifest-backed exception is limited to the target day and its causal warmup context, is included in window-cache v12 identity, and does not weaken native gap censoring. All 50 Development dates have exactly one BTCUSDC individual trade file, the required target-day normalized BBO/L2 files and a target-day causal feature-context file.

## Replay ML ABI Correction

The first attempted v2 Development build exposed a deterministic parity bug before any model outcome was used. The private live configuration has `ml.enabled=false`, but the live-to-replay parameter map omitted `ml_enabled`, so formal replay defaulted the 13-head quote model to enabled. At the same time, replay coupled `Prediction.feature_dict` construction to 13-head inference even though the live BUY fill-selection scorer still requires that causal context when the 13-head model is off.

The corrected ABI now carries `ml_enabled` explicitly. Feature-context loading and 13-head inference are separate switches: replay loads target-day causal features for the BUY scorer, emits neutral 13-head predictions, and applies no 13-head quote contribution when `ml_enabled=false`. Missing target-day context fails fast rather than falling back to a stale warmup row.

The partial output at `${NARROWGATE_RETIRED_DATA_ROOT}/reports/placement_fill_request_state_race_v2_development_20260728_v2/placement` is therefore invalid and cannot be resumed or used as evidence. It is retained only for lineage until separately authorized for deletion. Validation and sealed holdout were not read, and no Development model result from those partitions was used.

## Native Lifecycle Path

The native pipeline is:

```text
corrected baseline trace
  -> paired closer/current/farther placement cohorts
  -> sparse active-price watch manifest
  -> native snapshot/delta queue tape
  -> C++ event/trade merge
  -> exact start/request/pending/ACK/terminal lifecycle rows
  -> request-time causal feature compiler
  -> three-phase prediction panel
```

The sparse tape uses `active_order_queue_tape_v3`. Its seed ABI includes BBO, as-of time and native segment identity. Existing v2 tapes remain read-only compatible, but formal v2-family mechanics require the v3 schema.

Real-day parity on 2026-04-13 covered:

- 17,261 paired cohorts and 51,783 placement children;
- 111,389 watched native level changes;
- 1,062,056 individual trades;
- zero distance-monotonicity violations;
- zero field differences against the Python authority for closer, current and farther actions.

The parity artifact is `${NARROWGATE_RETIRED_DATA_ROOT}/reports/active_order_queue_tape_v3_smoke_20260728/native_lifecycle_benchmark_final.json`.

## Cache Contract And Timing

Two immutable caches are now separated:

- sparse queue tape: watch manifest + raw native-L2 hashes + builder identity;
- paired mechanics: baseline traces + individual trades + sparse tape + queue mechanics + C++ source and installed ABI identity.

Every load verifies completion markers, exact file sets, file sizes and SHA256. Raw L2 identity is content-based; filesystem mtime does not invalidate an otherwise identical cache. Models and thresholds are deliberately excluded from mechanics keys.

On the 2026-04-13 smoke:

- prior baseline trace: about 189 seconds;
- sparse tape construction: about 62 seconds;
- C++ lifecycle merge: 2.444 seconds;
- complete first build after integration: 293.16 seconds;
- paired-mechanics checksum reload: 0.042 seconds for 17,261 rows;
- sparse-tape checksum reload: 0.0026 seconds for four files.

The old path performed a second full Python native replay and took roughly ten minutes. The new path removes that duplicate replay. Baseline cohort generation still dominates first-build runtime; subsequent model/calibration iterations reuse mechanics and request-state caches.

The full-window pickle is about 1.1 GiB for a representative day. Formal 50-day construction therefore disables that disposable cache to preserve the 60 GiB disk reserve. The much smaller sparse-tape and paired-mechanics caches remain enabled and are the authoritative reuse boundary for subsequent model iterations.

## What Has And Has Not Been Learned

The mechanics smoke proves causal ordering, lifecycle field parity, gap censoring, cache integrity and native acceleration. It does not estimate fill or ACK probabilities and does not change the closed v1 result.

Corrected Development produced 800,853 placement cohorts and 2,402,559 request-state action rows. All six pre-request fill curves and all six conditional ACK curves passed support, proper-score and absolute-calibration gates. All six pending-fill curves failed the proper-score gate; two also failed event support. The complete family therefore closed on Development. Detailed identities and intervals are in `docs/placement_fill_request_state_race_v2_development_20260728.md`.

Validation remains locked. A future specification may use the passing pre-request/ACK components while treating the 6-9ms pending fill as an empirical nuisance cost, but it must establish a new Development identity. Prediction qualification still cannot authorize a KEEP/CANCEL action or live policy.
