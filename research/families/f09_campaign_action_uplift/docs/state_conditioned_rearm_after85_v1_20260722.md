# State-Conditioned Rearm After 85 Seconds v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): the exact old-denominator reward, fill, campaign, duration and recovery values below are withdrawn. The frozen BUY and SELL rules remain closed because data repair cannot reopen an already inspected family. Any new rearm mechanism needs a new family ID and current normalized/native, time/unit and scorecard contracts.

## Decision

Both side-specific Development families are closed at the current panel:

- `sell_state_conditioned_rearm_after85_v1` failed support, reward, campaign, lifecycle, and daily-consistency gates.
- `buy_state_conditioned_rearm_after85_v1` had a positive reward point estimate, but failed active-state support and its confidence, repair, duration, and daily-consistency gates.

Neither 9-day Validation panel nor either 9-day sealed holdout was replayed or read. No live, C++, configuration, order-size, reducing-side, inventory-limit, or rolling-baseline change is authorized.

This closes the exact frozen rule "after the current 85-second add cooldown, continue skipping add quote cycles while adverse move, persistent adverse flow, weak refill, and weak recovery all remain active." It does not close every possible state-conditioned lifecycle action.

## Frozen Action

| Item | Value |
|---|---|
| Control | `baseline_rearm`: resume the current add quote after the actual baseline cooldown |
| Candidate | `continue_block_until_recovery`: keep skipping add cycles while the frozen adverse state remains active |
| Sides | BUY and SELL frozen and evaluated as separate family identities; SELL first |
| Behavior policy | exact 50/50 randomization |
| Intervention unit | exactly one randomized assignment per day/campaign |
| Candidate persistence | multiple quote cycles until a frozen hysteresis exit |
| Reducing quotes | unchanged and always governed by the baseline |
| Order size / inventory limit | unchanged |
| External reference | excluded; local M0 only |
| Replay | Python-authoritative formal replay; C++ fails fast until this state machine has parity |
| Reward | `fill_value - incremental_campaign_cost - queue/reset_cost` |

The candidate starts only after the baseline cooldown has actually expired, including the baseline consecutive-fill multiplier. It therefore does not search for a replacement fixed cooldown. Entry requires causal local adverse move, adverse flow persistence, weak refill, and weak price/microprice recovery. Exit uses separately frozen recovery hysteresis rather than an elapsed-second threshold.

The immutable code checkpoint is commit `172fcf9392122bf76c58059092ee957809b9460f`. Both sides used:

- config SHA256 `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`;
- empirical P3 SHA256 `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`;
- queue-v3 q0.70 SHA256 `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`;
- AWS Tokyo 2-vCPU/4-GiB latency tape SHA256 `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`;
- strict 100ms historical BBO/L2 build identity;
- formal fresh-start replay contract and keyed shared latency randomness.

## Evidence Split

The same 76 strict event-L2 good days were assigned independently to each side-specific identity:

| Panel | Days | Access |
|---|---:|---|
| Development | 56 | replayed; chronological OOF evaluation |
| Embargo 1 | 1 | excluded |
| Validation | 9 | locked because Development failed |
| Embargo 2 | 1 | excluded |
| Sealed holdout | 9 | locked and unread |

SELL evidence-split SHA256 is `68d865914f2178428f3d3bbb0a50dbe921f47d4a6b5adcfd17caa8127822c560`; BUY evidence-split SHA256 is `212380e39da89b5ce30cef94ee39bf5c569dae668c158ce7d413ef7a8e51af81`.

## Replay Integrity And Support

| Check | SELL | BUY |
|---|---:|---:|
| Development rows/campaigns | 1,646 | 2,396 |
| Baseline / candidate assignments | 829 / 817 | 1,180 / 1,216 |
| Entry-active rows | 81 | 82 |
| Entry-active days | 43 | 44 |
| Active baseline / effective candidate rows | 42 / 39 | 35 / 47 |
| Effective candidate days | 29 | 30 |
| Multi-cycle candidate rate | 53.85% | 53.19% |
| Total blocked add cycles | 174 | 150 |
| Maximum reward-identity error | `1.11e-16` | `5.55e-17` |

Every row was the only intervention for its day/campaign, had propensity 0.5, was side-specific `add`, left size unchanged, used no external reference, and incurred no queue-reset cost before state exit.

The preregistered active-action minimum was 50 rows in each arm across at least 10 days. Both sides failed this support gate. The candidate nevertheless had real multi-cycle effect, so this is not the old one-cycle near-no-op family.

## Chronological Development OPE

After the 30-day nuisance warmup and one-day embargo, the OOF contrast covered 25 future Development days. Higher is better for every row below.

| Outcome | SELL DR uplift | SELL 95% day CI | BUY DR uplift | BUY 95% day CI |
|---|---:|---:|---:|---:|
| Decision-to-terminal reward, USDC/decision | -0.00504 | [-0.01314, +0.00226] | +0.00493 | [-0.00166, +0.01275] |
| Campaign-cost avoidance | -0.00467 | [-0.01250, +0.00189] | +0.00443 | [-0.00189, +0.01195] |
| Negative-terminal protection | -0.00425 | [-0.01141, +0.00240] | +0.00314 | [-0.00168, +0.00927] |
| Development-q10 shortfall protection | -0.00255 | [-0.00852, +0.00221] | +0.00218 | [-0.00064, +0.00667] |
| Repair within 30m | -0.00658 | [-0.01718, +0.00337] | -0.00251 | [-0.00810, +0.00363] |
| Duration avoidance beyond 30m | -0.00737 | [-0.01479, +0.00037] | -0.00603 | [-0.01204, -0.00025] |
| Intervention-fill avoidance | -0.01194 | [-0.02795, +0.00319] | -0.00364 | [-0.01305, +0.00521] |

SELL reward was positive on 7 of 25 evaluated days, or 28%. BUY reward was positive on 8 of 25 days, or 32%. The 5-USDC tail threshold had no events in either logged arm, so its zero contrast is missing tail information, not proof of safety.

SELL is directionally unfavorable on reward and every campaign/lifecycle co-primary. BUY leaves a weak positive value clue, but its lower confidence bounds include zero, active support is insufficient, repair is negative, and duration is significantly worse at the 95% day-cluster level. A reward-only interpretation would therefore be unsafe.

## Interpretation

The 48-hour live attribution correctly identified post-cooldown add campaigns as a useful causal intervention surface, but the frozen four-way conjunction is too sparse and does not identify a safe action region. In particular:

1. Continuing to block a SELL add after 85 seconds did not cut campaign cost; it more often delayed repair and reduced value.
2. BUY showed a positive reward point estimate, but the same action prolonged campaigns and lacked support. It is a diagnostic clue, not an eligible policy.
3. The current 85-second control remains the rolling baseline by default. This result does not prove that 85 seconds is optimal; it only rejects this state-conditioned extension on the frozen evidence.

Do not reopen v1 by loosening the conjunction, moving the adverse threshold, changing the hysteresis, lowering the support gate, or reading Validation on the same family identity. A successor needs a new economic mechanism and new pre-outcome identity. The most defensible next question is action value on an already active order, where keep/cancel/re-enter changes queue exposure more directly than another post-cooldown gate.

Fixed cooldown, gamma/kappa, and global max-inventory searches remain paused.

## Artifacts And Commands

Private artifacts live under:

```text
${NARROWGATE_RESULTS_DIR}/sell_state_conditioned_rearm_after85_v1_20260722/
${NARROWGATE_RESULTS_DIR}/buy_state_conditioned_rearm_after85_v1_20260722/
```

Canonical panel / summary hashes:

| Side | Development panel SHA256 | OPE summary SHA256 |
|---|---|---|
| SELL | `dd53d5401f5e68c5b3f81a2279fb275d70c52985f716f7ab6d572f29e0b7db63` | `46be934dac076a3136ba51e7245b3c4309c4c8eab8cf95c17c771d921f189360` |
| BUY | `2bc2e9d9da56b42819de96b8fa992e200470140415d614790cc70a1c64ede144` | `d1a3d16f1aec05573b379ef2a06c9ee9e996043c374ac7e882334235f5b06625` |

Executable entrypoints:

```bash
python -m models.audit.state_conditioned_rearm_randomized --help
python -m models.audit.state_conditioned_rearm_ope --help
```
