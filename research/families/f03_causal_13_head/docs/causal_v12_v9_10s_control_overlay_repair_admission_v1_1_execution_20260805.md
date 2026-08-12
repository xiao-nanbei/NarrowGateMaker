# F03 v9 10s Control Overlay Repair Admission v1.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

`control_overlay_successor_panel_admitted_outcomes_unread`

This is an execution-only repair/admission identity. It grants no economic, action, Validation, holdout, or live permission.

## Predecessor ABI Blocker

The predecessor `causal_v12_1s_ml_ab_replay.run_paired_tick_replay()` compares ML-OFF with the 1s ML-ON candidate. That is not the frozen F03 cadence estimand. The formal comparison requires:

\[
\text{v9 10s ML-ON control}
\quad\text{vs}\quad
\text{1s 13-head ML-ON candidate}.
\]

The 40-day runner now accepts the control only through this successor panel with an explicit panel path, file SHA256, and panel identity SHA256. Both replay arms require non-empty `ml_data` and `ml_enabled=true`; q90 action and BUY fill-selection remain OFF.

## Ready-Time Audit

The 8,640-row grid is the correct v9 visibility grid, not an execution-trade grid:

- `00:00:00` is the D-1 `23:59:50` bucket becoming feature-ready.
- `23:59:50` is the target-day `23:59:40` bucket becoming feature-ready.
- Overlay generation may not clip to the first or last execution trade.

When a D-1 feature file did not contain `23:59:50`, the repair rebuilt that boundary from contiguous D-1 warmup plus target-day clock closure. Only the D-1 terminal feature row was admitted; target-future values and prediction forward-fill were not used.

## Admission Result

The `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` panel contains 40 complete daily components:

- 28 existing overlays were reference-admitted after payload, model, source, market-context, and window identity checks.
- 12 days were regenerated as full 8,640-row overlays: `2026-04-17`, `2026-04-22`, `2026-05-01`, `2026-05-06`, `2026-05-13`, `2026-05-29`, `2026-05-31`, `2026-06-02`, `2026-06-05`, `2026-06-16`, `2026-06-17`, and `2026-06-26`.
- No overlay combines old prediction rows with regenerated prediction rows.
- Publication uses staging, file fsync, directory fsync, atomic rename, per-day receipts, and resume-safe validation.

Panel manifest:

`${NARROWGATE_DATA_ROOT}/cache/f03_v9_10s_control_overlay_repair_v1/control_overlay_panel_admission_v1_1/panel-manifest.json`

- SHA256: `9c36f54d34bf55da634d10a09b472c4cfcd82357e0d7da50e873cfb1ed383447`
- Panel identity: `939518843cdcfc72842782da6ff390ba73269fb5199c29598bf6d408c4ab6827`

## Permission Boundary

No PnL, fills, campaign outcomes, Validation, or holdout were read. Old v13 windows, old overlays, the frozen source plan, the frozen precommit, and the frozen amendment were not modified.
