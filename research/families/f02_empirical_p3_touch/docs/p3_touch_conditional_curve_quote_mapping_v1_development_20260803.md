# Conditional P3 Curve To Quote Mapping v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: closed on historical OOF economic gates. Evidence only; no prediction, quote-mapping, action, artifact-replacement, operational, or live authority.

## Question

This identity separately tested whether the supported historical conditional touch curve could improve the full maker path when mapped into the existing P3 quote ABI. It did not retrain the conditional model and did not replace the current v2 P3 artifact.

The two frozen arms were:

- `current_v2`: static `delta_star=13.9991 USDC/BTC` and `kappa_eff=0.067356 (USDC/BTC)^-1`;
- `conditional_v4_1_oof`: the strictly chronological OOF curve for each day, mapped every 10 seconds to a pair-curve `delta_star` and local `kappa_eff`.

The pair curve was the equal arithmetic mean of the BUY and SELL touch curves. `delta_star` was the smallest grid maximizer of `d * P_pair(d|x)`, and `kappa_eff` was the adjacent-grid central log-probability slope. Missing, invalid, or boundary-optimum buckets fell back to current v2. There was no gain, cap, multiplier, clipping rule, or parameter search.

## Identity and Availability

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| [Public Spec projection](p3_touch_conditional_curve_quote_mapping_v1_spec_20260803.json) | `03da69ee64deb6b384ee86be614ea3301ff4ac522bf2b6d639be1c1cb7e6b6ad` | Public repository |
| Executed private Spec source | `c88e99fdeb69b0654a4b4a79c3e31bda190ffdfaf61f3b6b82db78298016c59a` | Private evidence store; not distributed with public repository; canonical Spec identity `95a17af64f56d4769ecaece730d8c7e4b8b995e9272c966f4840a884e16102eb` |
| [Mapping implementation](../audit/p3_touch_conditional_quote_mapping.py) | `ed2fd184e7ba6e3148b82bcd4c1d9d416f37ce7dbfe8841b244c26de3e480cf3` | Public repository |
| [Full-path implementation](../audit/p3_touch_conditional_quote_path.py) | `971ffdd6c406abab8cccb4f8f1f44bafbb6fc9a4969b6994eac266a34ea2be03` | Public repository |
| Compiled native module | `5498880a645656a8bb6732f3c02a0b6b17a938a832102a389012815fd4bea03e` | Build-local binary; not distributed with public repository |
| Authoritative report | `e55d3355fdde31f83e921d6e8cb18ba9ff57a1539bac981d430d13cdc64d7fcd` | Private evidence store; not distributed with public repository |
| Output manifest | `83763d89bc633b1f696ff35e62393c86633a8764f3f265e87476436aa5834369` | Private evidence store; not distributed with public repository |

The authoritative report has logical evidence ID `reports/p3_touch_conditional_curve_quote_mapping_v1_20260803/report.json`. SHA256 values identify retained bytes; they are not download links.

All 24 days used the appropriate historical OOF fold artifact. Nine dates came from the already-read historical native Validation panel and 15 from the already-read late diagnostic panel. They are not independent confirmation.

## Mechanics

The mapping had ample valid support and materially changed execution:

| Metric | Result |
|---|---:|
| Minimum daily causal-context coverage | 99.7917% |
| Minimum daily valid-mapping coverage | 99.7569% |
| Matched executable quote change rate | 99.0730% |
| Mean raw half-spread, current v2 | 24.5822 USDC/BTC |
| Mean raw half-spread, conditional mapping | 13.4900 USDC/BTC |
| Mean quote-time `delta_star`, current v2 | 13.9991 USDC/BTC |
| Mean quote-time `delta_star`, conditional mapping | 10.3851 USDC/BTC |
| Mean quote-time `kappa_eff`, current v2 | 0.067356 `(USDC/BTC)^-1` |
| Mean quote-time `kappa_eff`, conditional mapping | 0.168434 `(USDC/BTC)^-1` |

The failure is therefore not attributable to the owner-approved 95% context coverage threshold or to an action that was too weak. The conditional mapping substantially tightened the quote path.

## Economic Result

| Metric | Current v2 | Conditional mapping | Candidate minus current |
|---|---:|---:|---:|
| Terminal MTM PnL | -59.4706 USDC | -175.1314 USDC | -115.6608 USDC |
| Mean daily PnL | -2.4779 USDC | -7.2971 USDC | -4.8192 USDC |
| Fills | 8,799 | 21,597 | +145.45% |
| Absolute inventory time | 3,160.81 BTC-s | 2,309.01 BTC-s | ratio 0.7305 |
| Daily PnL q10 | -6.0534 USDC | -10.3795 USDC | -4.3261 USDC |

The day-clustered 95% interval for mean daily PnL delta was `[-5.9779, -3.6146] USDC/day`. Only 1/24 days improved. The frozen relative PnL-improvement divided by absolute relative fill-change metric was `-1.3371`.

Both temporal panels agreed:

- historical native Validation OOF: `-50.8004 USDC`, or `-5.6445 USDC/day`, with 1/9 positive days;
- historical native late diagnostic OOF: `-64.8604 USDC`, or `-4.3240 USDC/day`, with 0/15 positive days.

Context coverage, mapping coverage, executable-coordinate change, and inventory-time gates passed. Pooled PnL direction, both-panel PnL direction, fill-retention, daily-q10, confidence-bound, and positive-day-rate gates failed.

## Decision

Close `p3_touch_conditional_curve_quote_mapping_v1` on historical OOF economic evidence. The conditional prediction surface remains historical prediction evidence, but this direct compression into dynamic `delta_star` and local `kappa_eff` does not have quote authority.

The current v2 P3 artifact remains the operational baseline. Do not rescue this identity by tuning the distance grid, adding a kappa cap, shrinking the curve effect, changing side weights, or rerunning only the favorable day. A genuinely different quote mapping would require a new ex-ante identity and cannot inherit authority from v4.1 prediction quality.

Validation and sealed holdout are not opened as new evidence by this identity, and all action/live permission fields remain false.

## Public References

See the [family README](../README.md), [contract errata](p3_touch_conditional_curve_quote_mapping_v1_contract_errata_20260803.md), [conditional v4.1 predecessor report](p3_touch_volatility_conditioned_v4_1_development_20260803.md), and [quote-trace comparison implementation](../audit/p3_touch_quote_path_comparison.py).
