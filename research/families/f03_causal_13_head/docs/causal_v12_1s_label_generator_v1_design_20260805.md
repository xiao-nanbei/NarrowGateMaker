# causal-v12 1s label generator v1 design

Last materially modified: 2026-08-05

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

Implemented and unit-tested locally. Training remains fail-closed until real daily feature/source manifests, the exact label quote/P3 identities, and real-day Python/C++ fingerprints are bound.

## Time origin

Every target UTC day contains exactly 86,400 decisions on the canonical 1-second grid. The label origin is the already-frozen feature cutoff:

\[
t_{label}=t_{decision}=t_{cutoff\ exclusive}.
\]

The legacy `RESAMPLE_SEC=10` offset is not applied. Feature readiness must satisfy `feature_ready_ts_ms <= decision_ts_ms`. The durable output is a separate label overlay keyed by decision/cutoff and the feature-row fingerprint; it does not copy any of the 173 feature columns. Labels and weights remain outside the trainable namespace and are joined only in memory for training.

## Future ownership

The label math preserves the 13 causal-v12 estimands, while censoring uses each head's actual maximum future dependency. Return/direction labels own up to two times their fill horizon; volatility owns one horizon; toxicity owns twice its fill horizon. Labels may not cross UTC midnight in this training identity.

Each future interval must remain inside one continuous 1-second source segment; the maximum observed-bar gap is 1.5 seconds. Legitimate no-trade seconds must already exist as explicit source-layer synthetic bars with a lag-state. Missing support is censored and receives zero weight; it is never forward-filled.

## Weighting

The inherited base weight remains

\[
w_0=\exp\left(-0.1\,\frac{days\_ago}{30.44}\right),
\]

with the frozen reference date `2026-07-23`. At 1-second cadence, overlapping labels are adjusted by interval-average reciprocal concurrency and normalized within UTC-day x head so valid rows retain the inherited total base-weight scale.

## Boundary

This artifact does not train the 13 heads and does not read prediction or economic outcomes. It cannot authorize an action or live deployment. The next gate is a real `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` daily source probe plus Python/C++ feature fingerprint parity, followed by binding the exact label quote configuration and P3-v2 artifact.
