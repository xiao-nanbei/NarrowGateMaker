# Markout Side-Asymmetry Sign Engineering Release v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: **engineering correction released to EC2; no alpha, PnL, action-family, or closed-family authority**.

## Scope

BUY and SELL markout EMAs are maker-signed, so positive values are favorable on both sides. The quote asymmetry parameter therefore requires sign `+1`. The running YAML omitted this field and inherited the historical `-1` default.

This release corrects that semantic contract. It does not claim that the fix explains or improves PnL: the frozen 360-hour shadow audit estimates only `-0.000407 bps` mean corrected-minus-observed coordinate movement.

## Release boundary

The remote patch contains only:

- one explicit `markout_side_asymmetry_sign: 1.0` YAML field;
- two defaults in `live/config.py`;
- three defaults in `strategy/quote_core.py`;
- one C++ header default.

The release waited for the natural flat trade at `2026-08-01 10:21:02.950 UTC`, atomically replaced the config, and sent SIGHUP. PID `1371345` did not change, so no process restart or campaign truncation occurred. The q90 policy SHA256 remained `3bbb56e192cd92b2118e84c0dc0e23d9a9ea2d9018b5721f1f73921efa5a641a`; its threshold was not changed and no economic result was read.

## Verification

- Remote same-version preflight passed with ML disabled and unchanged P3/model identities.
- Local full suite: `1222 passed, 4 skipped`.
- Remote effective-config smoke passed Python/C++ quote parity, spread cap and GTX checks with sign `+1`.
- Post-release soak: 46 decisions, exactly 23 BUY and 23 SELL; 12 placements, 10 cancels, zero rejects and zero severe logs.
- Bounded receive-time capture remained inactive and recording stayed disabled.

The C++ source default is corrected on disk, but the remote editable rebuild was blocked because the existing Python 3.9 environment cannot import `scikit_build_core.build`. The loaded extension's empty-constructor default is therefore still `-1`; live does not consume that default because YAML now explicitly supplies `+1`, and the effective value passed into the existing C++ binding was verified directly. Rebuilding the extension belongs to the already required Python >=3.10 environment migration, not to strategy research.

## Routing

The project states remain independent:

- F09: `inventory_suppression_and_passive_repair_action_subspace_exhausted`.
- q90: `baseline_integrity_repair_active`.
- PnL: `decision_visible_fill_quality_alpha_missing`.

Only q90-ON live-equivalent replay waits for terminal-risk-set repair. New fill-quality feasibility may freeze q90 OFF in all arms and proceed using exact lifecycle identity. Missing lifecycle rows are excluded; fallback mid is forbidden. This engineering release does not reopen inventory suppression, quote gradients, cooldown timing, or aggressive repair.

Machine result: [`markout_side_asymmetry_sign_engineering_release_v1_20260801.json`](markout_side_asymmetry_sign_engineering_release_v1_20260801.json).
