# Historical backtest evidence revalidation

Last materially modified: 2026-08-30

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-20

> Current status (2026-07-27): the operational identity and exact values below describe the 2026-07-20 deployment only. Later normalized-L2, trade-side, calendar, bar-clock, commission, daily-PnL, tick/lot and volatility repairs superseded causal-v3/v4/v5/v6 model and replay evidence. Current maintained semantics are `causal-v7`; its 13-head inference is disabled. The deployment guard and conservative withdrawal logic remain valid engineering conclusions.

> Superseded input note, 2026-07-25: the old top-level BTCUSDC `bbo/l2` identity was later confirmed to mix approximately 1-second/top-10 and 100ms/top-20 files. The exact empirical P3 values and every model or replay number derived from that mixed identity was withdrawn pending the reruns in [Legacy L2 Evidence Revalidation](../../../../docs/legacy_l2_evidence_revalidation_20260725.md). The P3 touch curve has since been reproduced within 0.2% on normalized 100ms BBO, but the old artifact/hash remains superseded. Correctness fixes and conservative `do not promote` decisions remain valid.

## Historical decision at 2026-07-20

The operational baseline at that time used the empirical P3 artifact instead of the historical `p3_kappa_eff_override=0.055`.

- Config SHA256: `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`
- P3 SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- `delta_star=13.9990859817 USDC/BTC`
- `effective_kappa=0.0674381136`
- Effective source: calibrated artifact, not YAML override
- Live model bundle: `saved_btcusdc_causal_v3_calonly_20260717`

The causal-v4 13-head bundle and rebuilt BUY scorers remain research artifacts. Changing P3 did not silently promote those models to live.

The immutable operational identity is owner-private and `private_not_distributed`; this historical report grants no current runtime authority.

## Deployment guard

At the time, the now-retired `make deploy` and `make deploy-dry` targets ran `scripts/preflight_live_deploy.py`. The current tree separates local admission (`make deploy-preflight`) from source-only publication (`make publish-source-dry` / `make publish-source`). The historical preflight:

- requires an explicit model directory and `fill_prob_params.json`;
- validates positive `delta_star` and `kappa_eff`;
- prints config, model and P3 hashes plus the effective kappa source;
- rejects a nonzero P3 override unless `NARROWGATE_ALLOW_P3_OVERRIDE_DEPLOY=1` explicitly identifies a trial.

The two private files previously used as current EC2 snapshots now have the same config hash. Historical dated configs remain untouched.

## Evidence classes

### Retained as current evidence

These results depend on corrected definitions or explicitly bind the empirical P3 and corrected replay identity.

| Evidence | Current interpretation |
|---|---|
| Unit and causality repair | Price variance units, explicit horizons, bucket-end feature visibility, merged event clock, quantity-weighted markout and terminal MTM are correctness fixes. |
| Empirical P3 | Reproduced on normalized 100ms BBO: 10s `delta_star` was unchanged and `kappa_eff` moved from `0.06743811` to `0.06735643` (-0.121%). The old artifact/hash is superseded; effective kappa remains an execution artifact, not a PnL knob. |
| Python/C++ same-input parity | Corrected real-data windows establish implementation parity for the frozen inputs. They do not establish true exchange queue priority. |
| Causal-v4 model fitting | Train/validation dates remain causally timed, but the published test metrics are withdrawn pending a corrected feature rebuild. |
| Causal-v4 ML A/B | Same-input parity and development diagnostics remain; test/all-122 values are withdrawn because three test days exposed futures metrics five minutes early. |
| Causal-v4 BUY scorers | Exact bucket results are withdrawn because blocked cross-fitting can propagate the affected feature days across folds. The scorers must be rebuilt before use. |
| Fixed local action family | Conservative `do not promote` governance remains safe; exact DR/fill/campaign values are withdrawn pending a v2 denominator rebuild. |
| BUY conditional widen | The family was not promoted. Its exact Development estimate is withdrawn; reopen only with a new frozen v2 identity. |
| SELL one-cycle skip | The family was not promoted. The historical 3.52% mechanics estimate is withdrawn with the old denominator. |
| Queue keep/cancel v1 | The coarse family remains closed. Later top20/q0.70 results are conditional evidence, while native keep/cancel diagnostics remain valid under their native identity. |
| Lifecycle audit | The conceptual warning survives: 30-second markout is early toxicity, not full lifecycle value. The 5.8-minute and 92.1% values must be recomputed. |
| Three-venue Stage 0 | External trade-derived states and leave-one-venue-out architecture remain valid. Exact maker markout/campaign values are withdrawn, and Stage 0 did not establish an action rather than proving external data has no value. |
| Fixed queue warning | q0.70 fit two live days but failed later fill-count stability. A universal queue-ahead multiplier is not supported. |
| 2026-07-18 mechanism parity | Corrected replay reproduced 139 versus 135 fills, 11 versus 11 breakers and decisions within about 1%. This is a mechanism gate, not exact historical PnL parity. |

### Removed historical claims

Pre-repair gamma, cap, guard, cooldown, execution-ablation, legacy BUY-score and maker-action result tables have been deleted. They are not retained as directional evidence. The separate external-venue granularity report keeps only its causal price-direction diagnostic; it does not preserve the deleted maker action conclusions.

### Invalid for current selection

The following must not be compared with the current baseline:

- exact PnL, fill, tail, winner and arm rankings from the pre-2026-07-15 trade-clock or left-labelled ML replay;
- historical survivor rankings and run-local arm IDs;
- claims that any fixed gamma, P3, cap, guard or cooldown value is optimal;
- old multi-market stop-add, safe-rearm and fixed re-center PnL;
- old cap-compression and markout-sign policy conclusions;
- 2026-07-12 exact live/replay PnL under the old unit/scorer/L2 contract;
- all 2026-07-13 results generated before the cross-day snapshot repair; the current 24-hour-warmup rebuild is a new data identity and old PnL/queue outputs must not be reused;
- causal-v4 test/all-122 ML values and BUY scorer bucket values built from `features_btcusdc_causal_v3_empirical_p3_20260718`, because 2026-07-12, 2026-07-14, and 2026-07-15 exposed five-minute metrics at interval start instead of feature-ready interval end;
- direct PnL comparison between the 2026-07-18 historical live process and the corrected replay, because the former used P3 `0.055` and broken GTX/IOC close behavior.

The conservative historical decision "do not promote" remains safe. The historical numeric estimate that produced that decision does not automatically remain valid.

## What requires a paired rerun

Only hypotheses still under consideration should be rerun. Every rerun must pair its arm with the current corrected baseline and freeze:

1. code, config, P3 and model hashes;
2. event-L2 and individual-trade data identity;
3. initial inventory, active orders and campaign state;
4. strict queue artifact and empirical latency profile;
5. random seed and action propensity.

If global parameter work resumes, the minimum order is:

1. strict baseline under the empirical P3 identity;
2. `gamma`;
3. `kappa_ratio/depth_kappa_ratio/cap` interaction;
4. guard interaction;
5. cooldown/execution interaction;
6. only then a new Sobol generation.

This is revalidation work, not a recommendation to return to global parameter search. The current research priority remains action value with causal overlap.

## Open boundary

The formal q0.70 queue artifact is still calibration-conditional, while the default queue file is a legacy one-day file without the complete v3 identity. A strict runner must receive the formal artifact explicitly and fail otherwise. No new absolute PnL claim should be made until a healthy full UTC day after the empirical-P3 and IOC fixes is available for mechanism calibration.

## Final read

No old result currently proves a profitable live action on the new baseline. What survives is still useful:

- corrected accounting and causal replay;
- empirical P3 and same-input parity;
- weak but real state-ranking clues;
- several well-identified negative action-family results;
- a clear warning that fixed global knobs mostly redistribute activity, inventory and tail risk.

That is a narrower evidence set, but it is substantially more trustworthy.
