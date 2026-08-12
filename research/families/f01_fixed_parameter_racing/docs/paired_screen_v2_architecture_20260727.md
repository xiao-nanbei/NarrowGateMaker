# Paired Screen v2 Architecture

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-27

## Problem

The historical `paired_daily_selection()` performed two different jobs:

1. it classified and sorted arms with legacy tiers, Pareto status and `joint_paired_t`;
2. it appended a `paired_screen_v1` scorecard whose score did not control the final order.

That made tier, blocked-OOS candidate, scorecard and promotion-like fields appear authoritative at the same time.

## Canonical Flow

```text
paired replay daily rows
        |
        v
build_paired_daily_evidence
        |
        v
paired_screen_v2 scorecard
        |
        v
rank_paired_daily_evidence
        |
        v
panel_promotion_controller (separate state transition)
```

### Evidence Builder

`models.parameter_selection.build_paired_daily_evidence()` calculates paired PnL, terminal, inventory-adjusted, activity, campaign, tail, repair and mechanism-distance evidence. It does not emit tier, candidate, scorecard, rank or promotion fields.

### Canonical Screening

`models.audit.paired_screening.screen_paired_daily_arms()` is the only paired ranking entrypoint for new experiments. It sorts by `scorecard_ranking_score`. Pareto status and `joint_paired_t` are deterministic tie-break diagnostics only.

The v2 profile freezes non-compensable support and mechanism limits, including day coverage, fill/activity support, campaign-count drift, action-mix drift, spread drift and side support. A weighted score cannot buy through these failures.

Profile identities at freeze time:

- `paired_screen_v1`: `d31c1e62ae4142d06329729cce5e07d9e39a18536d44d7fb667e4844e7598c41`
- `paired_screen_v2`: `e8eac2a55015701ed8a817c012d59da1b0f95df2cfd8e51ad0e6fe66fe7d386e`

v1 remains unchanged for reproduction. v2 is rankable but screening-only:

```text
ranking_authority = paired_screen_v2.scorecard_ranking_score
promotion_authority = false
scorecard_screening_status = screening_rank_only
```

### Promotion Controller

`models.audit.panel_promotion_controller.control_panel_promotion()` consumes a scorecard plus frozen family/panel state. It alone may authorize reading the next panel:

```text
Development pass -> Validation may be opened
Validation pass  -> sealed holdout may be opened
sealed pass      -> shadow candidate
```

It always emits `live_promotion_allowed=false`. A screening profile can never unlock a panel.

## Compatibility

`paired_daily_selection()` is deprecated. It delegates to v2 ranking, then adds historical fields for old reports. They are explicitly marked:

```text
selection_tier_compatibility_only = true
candidate_for_blocked_oos_promotion_authority = false
scorecard_promotion_status_compatibility_only = true
```

`parameter_racing_sweep --rescore-daily-csv` calls the canonical v2 entrypoint directly. New experiments must not call the deprecated adapter.
