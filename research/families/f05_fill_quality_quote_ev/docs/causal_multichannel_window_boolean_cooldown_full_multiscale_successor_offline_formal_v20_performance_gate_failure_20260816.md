# F05 Offline Formal V20 Performance Gate Failure

Last materially modified: 2026-08-16

Status: `closed_execution_performance_failure_no_economic_read`.

Evidence availability: SHA256 values below are integrity metadata, not download links. This public Markdown report, its companion public JSON record, and the referenced repository commit and annotated tag are available through the public repository; the formal execution manifest, preflight receipt, one-day mechanics receipt, launch and performance receipts, mmap cache, atomic progress receipts, and run payloads are owner-side evidence retained in the private evidence store and are not distributed with the public repository.

## Scope

Formal-v20 was an execution-only successor for the frozen F05 full-multiscale offline nested out-of-fold study. It preserved the 30-day Development panel, opportunity denominator, duration vocabulary, candidate hierarchy, exact owner B0, Python modeled-queue semantics, statistical contract, and permissions. The only change from formal-v19 was the one-shot scheduler topology: two day parents and one shared eight-arm pool under the same ten-compute-token limit, while retaining the candidate-independent acceleration-v2 read-only mmap bytes.

## Pre-Economic Gates

The clean public commit was `174ad0e59565ffd58b3998e5866edaee4eec8513`, and the annotated tag object was `2201e0cc64995af8243391bbd9ae4ff5cbe6cead`. The formal execution manifest had canonical SHA256 `065ec5c89449a7085696be238d907753758c37dd4ec929eb8a5d6d178e951c56` and file SHA256 `35a9daabce045f9982a14b5fbf3cd197f1a0837350f116b565cb7c442ca6d297`.

The zero-economic preflight completed 506 fold-day contract slots across both sides, four outer folds, twelve inner folds, nine candidate families, matched controls, and the continuous comparator. It bound two day parents, two globally bounded coordination supervisors, eight shared arm slots, and the acceleration-v2 read-only mmap identity. Its canonical receipt SHA256 was `accf89155225f247c370c2caac75b76a4a2b6116f162712fbdadd2726abf7fb0`, and no economic outcome, Validation, or sealed holdout was read.

The exact-owner one-day mechanics gate completed 81 opportunities with 81 no-op parity matches, 81 complete washouts, zero right censoring, and an eight-arm worker identity. Replay wall time was 327.559 seconds. Its canonical receipt SHA256 was `8af899d941d29f11c3581361146bec7c18499631fbb34bf0b9cb5dca8ab906ef`.

## Frozen Performance Gate

The preregistered launch gate required at least 30 completed opportunities, at least five elapsed progress minutes, throughput of at least 4.85 opportunities per minute, and at least a 1.2-times speedup over formal-v19's 2.58696 opportunities per minute. Intermediate economic values were forbidden from the decision.

At termination, 32 opportunities and 256 arms had completed in 15.842802 progress minutes, for 2.019845 opportunities per minute. The observation count and elapsed-time requirements passed; both throughput requirements failed. The process was terminated immediately, and no automatic formal-v21 identity was created.

## Diagnosis

The run reused admitted mmap days and opened two day parents successfully. Runtime snapshots showed the shared arm limit operating globally rather than per parent, with arm workers occupying the available compute slots while day parents were mostly waiting or advancing sparse event intervals. The completed-arm rate was 16.1588 arms per minute; because every opportunity requires eight frozen duration arms, this maps directly to the observed 2.019845 opportunities per minute. The v20 failure therefore does not support another day-parent or mmap rearrangement as the next optimization.

The next meaningful execution optimization must reduce work per arm or share event traversal across arms. Examples are a lockstep multi-arm Python event loop that scans each market event once, or a C++ arm engine only after complete event, fill, campaign, checkpoint, and terminal-PnL parity. Increasing the core budget is a separate engineering identity and must not be described as an algorithmic speedup.

## Evidence Boundary

Formal-v20 produced no returned label batch, fitted candidate, outer-test policy, scorecard, nested OOF report, or formal result. Its partial strategy-dependent shards are not reusable. Complete candidate-independent mmap bundles remain reusable under their unchanged content identity. This is an execution-performance closure only and says nothing about EMA, Boolean, cooldown, or campaign economic value.

The private performance receipt has file SHA256 `f26ef49ac67c037e5d95260c119b1173100e20b635928d95778058a01d8157c9` and canonical receipt SHA256 `552c2b1f41e986d902687864e2f3db5f320426e408b7b2c8a73fea89fc48aa71`. Action authority and live authority remain false; EC2 and the active owner policy were unchanged.
