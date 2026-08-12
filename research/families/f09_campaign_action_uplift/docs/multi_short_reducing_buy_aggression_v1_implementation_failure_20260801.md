# Multi-Short Reducing-BUY Aggression v1 Implementation Failure

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

The frozen v1 implementation identity failed its one-day mechanics smoke before formal Development outcomes were read. The 40-day randomized replay was not started, and no PnL, markout, campaign reward, Validation, or holdout result was used.

## Frozen Identity

- Spec: `multi_short_reducing_buy_aggression_v1_spec_20260801.json`
- Canonical spec identity: `0df7bb926b4554f4583694d8c9a712b9d850263a698c6c9cab29dc5071fb199f`
- Smoke day: `2026-06-20`

## Failure

The action converted valid BTCUSDC prices to floating-point values and checked the maker ceiling with an absolute `1e-12` tolerance. At BTC price scale, normal binary floating-point representation error can exceed that tolerance, causing a valid `ask1 - tick` GTX BUY quote to fail with:

`ValueError: candidate reducing BUY is not a valid maker quote`

This is a tick-grid implementation defect, not an economic result and not evidence for or against the treatment.

## Resolution Boundary

The failed frozen Spec remains unchanged. A new v1.1 implementation identity must use integer exchange tick indices for baseline, BBO, maker ceiling, selected price, and improvement. It must rerun the action-contract JUnit evidence and the same one-day mechanics smoke before any formal Development outcome read. All actions, thresholds, panels, reward definitions, scorecard gates, and permissions remain unchanged.
