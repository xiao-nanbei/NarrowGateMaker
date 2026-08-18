# F05 Formal-v24 mmap Lifetime Failure

Last materially modified: 2026-08-18

Status: `failed_closed_after_buy_complete_before_sell_outer1_one_shot_completion`.

## Question

This receipt records why formal-v24 stopped after completing the BUY side and before producing a formal nested out-of-fold result, and whether any incomplete work may be reused by a successor execution identity.

## Result

Formal-v24 completed 577 BUY cache units: 67 outer-train one-shot units, 250 inner-OOF sequential units, and 260 outer-OOF sequential units. SELL outer1 then opened ten one-shot day tasks and the Python process terminated with `SIGSEGV / EXC_BAD_ACCESS` before admitting any SELL cache entry or writing `formal_result.json`.

The native crash report identifies NumPy's signed-64 SIMD comparison path on the faulting worker while the main thread was closing a Python mmap. The fixed global one-shot scheduler reused a borrowed thread pool across days; if one arm raised, `_execute_one_shot_day` unwound without cancelling and draining every sibling future because it did not own the pool. The day projection context could therefore close mmap-backed arrays while another arm still read them. This is a use-after-unmap lifetime race, not evidence about BUY or SELL economics.

## Evidence

Evidence availability: the crash report, process log, and completed-cache census are owner-private evidence and are not distributed with the public repository; the SHA256 values below identify those retained bytes but are not public download locators. The public failure interpretation and successor contract are the repository documents linked from the F05 README.

The owner-private native crash report has SHA256 `70c2bfa723930f15278134683fab7971f7fe171cb2886f608637fa40e52fe198`. The owner-private formal-v24 process log has SHA256 `e8ef36072bae983e405fe09c49c46e1992cea6ed583032a6cf9caad3999c9a38`. These private artifacts are not distributed with the public repository.

An outcome-blind cache census verified all 577 completed BUY progress receipts, cache manifests, and Parquet byte hashes. Its ordered completed-unit set has SHA256 `be4d2e4f4c1b7bf63f62e10c4ee84c522f8f8a4a418246ef2e813e7dab8cca68`. The same census found exactly ten SELL progress receipts in `running` state and no corresponding admitted SELL entry.

## Reuse Boundary

Only the 577 hash-complete BUY entries may be rebound by a successor. Every source, panel, fold, owner-policy, candidate-policy, side, stage, fold, day, and day-input field must remain identical; only the adapter artifact SHA and execution-manifest SHA may change. Missing BUY entries fail closed and may not be recomputed under the resume identity. SELL `running`, failed, partial, staging, or absent entries may not be inherited.

Formal-v24 did compute internal Development economics while producing BUY caches, but no intermediate economic values were read or exposed during crash diagnosis, no formal aggregate report or scorecard was admitted, and neither Validation nor sealed holdout was read. It grants no research promotion, action, live, C++ outer-test, or deployment authority.

## Preflight Finding

The existing all-fold zero-economic contract walk checked folds, schemas, identities, and task slots; it did not inject an arm failure while sibling threads retained mmap-backed arrays. It therefore could not detect this lifetime race. A failure-injection test that proves all sibling futures stop before mmap closure is now a mandatory execution preflight for the successor.

The active owner policy and EC2 runtime were not changed. No shadow, companion, observer, writer, or candidate telemetry was created.
