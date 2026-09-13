# E/C risk selection: first Development results

Last materially modified: 2026-09-07

Last materially synchronized: 2026-09-07

[中文报告](README.zh-CN.md) · [Training interface](../risk_selection_training.md)

Question: did deciding whether to open or keep an exposure-increasing order improve the existing baseline? **Not in this small completed diagnostic. E changed decisions but did not improve net PnL; C never selected cancellation.** These are simulated research results, not the owner's live account results. The owner authorized this compact research evidence publication; no live configuration, deployed policy, credentials or purchased raw market tape is included.

## What actually happened

B0 is the existing replay baseline, including its unchanged post-fill protections. E compares POST against WAIT when flat; C compares KEEP against CANCEL for a remaining exposure-increasing order. EC enables both. Neither candidate changes the baseline's quote price, size or risk limits. All actual arms carry their own inventory and order state across May 1–5; this is **one continuous five-day segment**, not five independent trials.

| Arm | Net PnL / USDC | Difference to B0 / USDC | Simulated fills | Terminal inventory / BTC |
| --- | ---: | ---: | ---: | ---: |
| B0 | -17.19351104 | 0 | 3062 | 0 |
| E | -17.21765670 | -0.02414566 | 3039 | -0.001 |
| C | -17.19351104 | 0 | 3062 | 0 |
| EC | -17.21765670 | -0.02414566 | 3039 | -0.001 |
| R_E | -17.33945670 | -0.14594566 | 3038 | 0 |
| R_EC | -17.33945670 | -0.14594566 | 3038 | 0 |
| Flat | 0 | +17.19351104 | 0 | 0 |

R_C is an analytical B0 alias, **not another executed arm**. Its supported cancellation probability is zero, with baseline fallback on the missing side. The six actual new arms all completed with exit code zero. The original B0 was reused, not rerun for this study or this publication.

E improves on R_E by 0.1218 USDC but not on B0. The training-frozen R probabilities were E/BUY=0, E/SELL=1 and C/SELL=0: these are deterministic boundary controls, not a nondegenerate randomized trial. Flat merely illustrates no-trade PnL before hosting costs; it cannot establish useful selection. No confidence interval, Sharpe, positive economic admission or deployment follows from this segment.

### E acted, but costs outweighed the gross change

| E-only decision surface | POST | WAIT |
| --- | ---: | ---: |
| BUY | 1023 | 2 |
| SELL | 3 | 1262 |

This is almost a side filter, not a broadly demonstrated conditional value selector. There were 1,264 WAIT decisions, so "E did not execute" would be incorrect. These are eligible decision counts, not fills or unique independent campaigns. WAIT suppresses that submission; it does not prohibit every future SELL or remove baseline reducing orders.

The completed-path accounting difference is:

```text
price PnL plus terminal MTM, before fees/funding:  +0.096300000 USDC
additional signed fee cost:                       -0.113016276 USDC
funding cashflow difference:                      -0.007429383 USDC
net difference:                                  -0.024145659 USDC
```

Thus fewer fills did not imply lower fees. This is an arithmetic decomposition of the recorded whole paths, not causal attribution of individual WAIT decisions. Fees are already included in trading PnL; funding is added once. The remaining short inventory is marked, not liquidated. The summary's terminal mark of about 80,865.6 was inferred from recorded cash/inventory/MTM and was not independently reverified against raw market data during finalization.

### C did not choose CANCEL

| C-only decision surface | Opportunities | Result | Reason |
| --- | ---: | --- | --- |
| BUY | 1957 | KEEP throughout | No fitted C/BUY model; explicit baseline fallback |
| SELL | 4302 | KEEP throughout | Every predicted KEEP-minus-CANCEL value was positive |

C/SELL predictions range from +0.05501618 to +0.07797208 USDC/action. The frozen rule cancels only below zero, so those scores select KEEP. EC likewise has zero C cancels: 2,198 absent-model BUY decisions and 4,223 positive-score SELL decisions. The published evidence does not show a cancel request being swallowed by the gateway; it shows that the policy did not request one.

The C/SELL training labels help explain the positive fit: 15 rows contain six positive, six zero and three negative values; the negatives are only -0.0001, -0.0003 and -0.0003 USDC, while a positive reaches +0.3841. The mean is about +0.0639933 and the median zero. The fitted intercept is +0.0603349. This descriptive imbalance and narrow feature support explain the model's tendency, but do not establish whether cancellation is economically valuable in a wider sample.

## Training support was very narrow

| Surface | Training labels | Available varying fitted inputs |
| --- | ---: | --- |
| E/BUY | 14 | Microprice shift only |
| E/SELL | 14 | Microprice shift only |
| C/BUY | 0 | No model |
| C/SELL | 15 | Microprice shift and two inventory values |

All 43 decisions were in UTC hour 00 on April 20 or April 22, sharing just two one-hour outcome windows. Quantity was always 0.001 BTC; E inventory was always zero by definition. Three feature names therefore did not provide three varying signals. The minimal Ridge fit uses alpha=1 and an engineering minimum of eight rows per surface, not a claim that eight rows suffice for inference.

The complete later May 1 one-hour diagnostic retained 18 labels: E/BUY 5, E/SELL 5, C/BUY 8, C/SELL 0. E/BUY direction was correct on 1/5; E/SELL on 5/5. **Both supported models had worse MSE than their past-only mean comparator.** C/BUY remained unsupported, while C/SELL had no later labeled targets. These correlated rows are chronological Development, not untouched confirmation.

The predeclared two-hour sensitivity retained the first available April 22 opportunity on three surfaces. E/BUY value changed from 0.3676 to 0.3619; E/SELL from 0.0174 to 0.0120; C/SELL remained zero. No sign changed in these three cases, but three examples cannot establish horizon robustness. These labels were not added to training.

## Files and what can be checked

| File | Contents |
| --- | --- |
| [study.json](study.json) | All eight arm summaries including the analytical alias, monetary/native counters, all-surface action counts, score-extrema examples, all 18 later predictions, original and extended training coverage, control rates and source provenance |
| [policy.json](policy.json) | The exact frozen **research-only** fit used in the completed E/C paths; no deployed model |
| [feature_units.json](feature_units.json) | The three fitted inputs, units and original column order |
| [training_labels.jsonl](training_labels.jsonl) | All 43 original training rows, concatenated in original input order; no row or feature filtering |
| [evaluation_labels.jsonl](evaluation_labels.jsonl) | All 18 later May 1 labels, including the eight unsupported C/BUY rows |
| [horizon_labels.jsonl](horizon_labels.jsonl) | All three separate two-hour sensitivity labels |

`study.json.sources` records named private-source digests and transformations; a source digest identifies the original bytes, **not** a differently serialized public projection. Policy and the two standalone label files are byte-identical copies; the training file concatenates four inputs. Full market/order/fill/opportunity tapes and private B0 inputs remain in the private evidence store; not distributed with the public repository. The compact action counts use every final opportunity row, not partial outputs or selected profitable examples. Score examples are explicitly extrema, not representative random samples.

Readers can reproduce the fitted policy and score arithmetic without private access. They cannot independently rerun the market replay or reconstruct all fills from this compact package. The original training report is retained separately from the publication-time descriptive coverage check, which reproduced the original policy without changing parameters or original artifacts.

From the repository root after installing `.[research]`, with a new output directory:

```bash
EVIDENCE=research/families/f05_fill_quality_quote_ev/docs/risk_selection_development_20260907
"$NARROWGATE_ROOT/.venv/bin/python" -m research.families.f05_fill_quality_quote_ev.risk_selection_training \
  --labels "$EVIDENCE/training_labels.jsonl" \
  --feature-units "$EVIDENCE/feature_units.json" \
  --validation-start-ns 1777593600000000000 --alpha 1 --min-train-rows 8 \
  --policy-id ec-development-ridge-apr20-apr22-constantfixed \
  --output-dir "$NARROWGATE_RESULTS_DIR/ec-publication-refit"
```

This command only fits the published labels; it launches no replay or trading. Coefficients should agree within numerical tolerance across BLAS/platforms; it is not a new model selection. Do not add May 1 or two-hour labels to the fit to improve these already-read results.

## Limits and next step

The reused B0 ran source `743de049`, whereas the six new arms ran `c9e95576`; full commits are recorded in the machine report. The latter includes actual risk-hook and quote-action changes. **This is not an identical-source paired baseline.** C's agreement with B0 in recorded economics and native counters is a useful no-change check, not proof of every event's equality. `passed=true` in the monetary summary means its recorded ledger reconciled; it does not establish source parity, model quality or deployment admission.

Native reconstruction was active and source gap/sequence/reversal counters were zero, but B0/C had four missing queue lookups and 70 invalidated order paths, and E/EC had two and 62 respectively. Exact queue placement is not established. Latency uses the existing measured pilot plus historical priors, not a stable multi-regime tail qualification, and incremental candidate inference delay remains unmodeled. These limits prevent treating the tiny net difference as a precisely measured live effect.

Keep this fit and all completed results unchanged. The next research task is broader outcome-blind Development opportunity coverage and valid paired labels, particularly missing C/BUY and non-midnight conditions; then assess a genuinely specified small feature/model experiment. Do not lower support thresholds or tune on May 1 to manufacture C cancellations. This result does not close the broader E/C research direction and grants no live deployment permission.
