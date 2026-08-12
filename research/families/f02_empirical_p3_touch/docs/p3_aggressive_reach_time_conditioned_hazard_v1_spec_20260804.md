# P3 Aggressive-Reach Conditioned Hazard v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: training Spec frozen; prediction and economic results unread.

This identity estimates the side-specific 100ms first-passage hazard through a 30-second administrative right censor and integrates it into the complete reach-time CDF. The censor is a reporting/support boundary, not an assumed order lifetime.

The weighted source panel contains 200 unique UTC source-days: 93 2025 provider fit days, 63 2026 native fit days, and 44 previously read 2026 diagnostic days. The 48 provider/native overlap days are transport comparisons only and are never double weighted.

Four expanding chronological OOF folds are frozen before model fitting. Raw distance and both volatility-normalized distances are constrained nonincreasing. Source identity and calendar year are excluded from the tradable feature vector.

Passing this Spec can establish prediction evidence only. It cannot replace operational P3 v2, generate a quote, authorize an action, create a shadow, or authorize live deployment. Any economic use requires an independently frozen full-path action identity.

## Public Identity and References

| Artifact | SHA256 | Availability |
| --- | --- | --- |
| Frozen Spec | `943b3f9a11bad31bfb378c78df0ffff5bfff29fcb2ae40c26c6842ddea9b7bbf` | This public file; the machine-readable [Spec JSON](p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.json) is also public |

See the [family README](../README.md), [design note](p3_aggressive_reach_time_surface_v1_design_20260804.md), [Development report](p3_aggressive_reach_time_conditioned_hazard_v1_development_20260804.md), and [hazard implementation](../audit/p3_reach_time_conditioned_hazard.py).
