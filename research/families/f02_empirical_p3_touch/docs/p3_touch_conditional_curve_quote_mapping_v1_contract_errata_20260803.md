# Conditional P3 Quote Mapping v1 Contract Errata

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: frozen interpretive errata. The research conclusions and machine artifacts remain unchanged; public Markdown may receive publication-only link and layout maintenance.

## Correction

The canonical experiment identity remains `p3_touch_conditional_curve_quote_mapping_v1`. Its tested mechanism must be interpreted more narrowly as:

```text
conditional_p3_scalar_compression_adapter_v1
```

The experiment implemented this pipeline:

```text
P_BUY(d | x), P_SELL(d | x)
  -> equal-weight side average
  -> argmax_d d * P_pair(d | x)
  -> scalar delta_star and scalar kappa_eff
  -> legacy AS/GLFT quote ABI
```

It therefore closes only the pair-averaged scalar compression adapter. It does not close the broader conditional-P3-curve-to-quote route. A side-specific mapping that jointly accounts for queue conversion, fill value, and campaign economics has not been tested.

## Estimand Boundary

The failed adapter contains three distinct estimand mismatches:

1. Ten-second touch probability is not one-second fill-arrival intensity.
2. The distance derivative of log touch probability is not automatically the Avellaneda-Stoikov fill-intensity elasticity.
3. The same compressed curve changed both `kappa_eff` and the spread floor, transmitting one signal twice through the quote formula.

The negative 24-day historical OOF result remains valid for this exact adapter: fills rose from 8,799 to 21,597 while terminal MTM PnL changed by `-115.660788 USDC`. That result must not be generalized to an untested full side-specific curve mapping.

## Evidence Retained

`p3_touch_volatility_conditioned_v4_1` remains historical Development prediction evidence under its disclosed owner coverage override. Its proper score, calibration, monotonicity, and source-transport results are unchanged. It is not independent confirmation and grants no quote, action, artifact replacement, or live authority.

The current v2 P3 artifact remains the operational baseline. No Validation or sealed holdout is opened by this errata.

## Immutable Research Bindings

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| [Public Spec projection](p3_touch_conditional_curve_quote_mapping_v1_spec_20260803.json) | `03da69ee64deb6b384ee86be614ea3301ff4ac522bf2b6d639be1c1cb7e6b6ad` | Public repository |
| Original executed Spec source | `c88e99fdeb69b0654a4b4a79c3e31bda190ffdfaf61f3b6b82db78298016c59a` | Private evidence store; not distributed with public repository |
| [Original Development report](p3_touch_conditional_curve_quote_mapping_v1_development_20260803.md) | `8bb6186f40ed121a0d333470a216ff884fe89386569904bd6863a842f5201e73` | Historical pre-publication-format binding; current public Markdown preserves the conclusion but may have different bytes |
| Authoritative report | `e55d3355fdde31f83e921d6e8cb18ba9ff57a1539bac981d430d13cdc64d7fcd` | Private evidence store; not distributed with public repository |
| Output manifest | `83763d89bc633b1f696ff35e62393c86633a8764f3f265e87476436aa5834369` | Private evidence store; not distributed with public repository |

The [machine-readable errata companion](p3_touch_conditional_curve_quote_mapping_v1_contract_errata_20260803.json) is public. The private report has logical evidence ID `reports/p3_touch_conditional_curve_quote_mapping_v1_20260803/report.json`; SHA256 values identify retained bytes and are not download links. See also the [family README](../README.md), [mapping implementation](../audit/p3_touch_conditional_quote_mapping.py), and [full-path implementation](../audit/p3_touch_conditional_quote_path.py).
