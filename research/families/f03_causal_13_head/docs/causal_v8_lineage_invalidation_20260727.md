# Causal V8 Taker-Tempo Lineage Invalidation

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-07-27

Status: the `causal_v8_through_20260725_20260727` feature and model artifacts are invalid for prediction, replay, or live use.

The first v8 rebuild resolved `MM_TRADE_FEATURE_DIR` to the mutable `trade_features/` workspace. That root still contained pre-repair sidecars for 2026-07-04 through 2026-07-11: each corrupted file assigned all individual trades to one taker side. It also differed from the frozen causal-v2 sidecar in 90 of 133 days, including material differences in derived interarrival features. The resulting validation panel and early stopping identity were therefore mixed and cannot be repaired by replacing eight files after model fit.

The invalid artifacts were renamed to:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/
  features_btcusdc_causal_v8_invalid_unversioned_tempo_20260727

models/saved_btcusdc_causal_v8_invalid_unversioned_tempo_20260727
```

No v8 prediction metric or PnL result may be used for selection. The successor uses the immutable 133-day manifest:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/
  trade_features_causal_v3_20260727/manifest.json
```

Its manifest SHA256 is `af385300d2852c0e6cc8d3e5f1b50984e649dee6e33450cf44740dc189a72292`. The successor family is `causal_v9_through_20260725_20260727`; its split and deployment gates are frozen independently of v8 outcomes.
