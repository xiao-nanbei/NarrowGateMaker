# F07 Lifecycle-v2 Runtime Compatibility Successor v1.6

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: source and three-day boundary diagnostics passed; homogeneous 40-day successor re-emission required

## Incident Boundary

The frozen v1.5 execution plan binds `execution/order_lifecycle.py` at SHA256 `447c21ef8150891e8faffefbb16665d33cf4325e214ac1e5538b902ccf137ec8`. During the 40-day emitter run, the live journal performance work added the read-only `journal_snapshot()` and `latest_event()` helpers plus their `copy` import. The current source SHA256 is `2981b6154f8e7e5aaa2af6c8f2e2720877f7ad214b2a2692f31f8af291496d33`.

The v1.5 hash rejection remains correct and unchanged. This successor never admits a panel by treating two hashes as interchangeable.

## Compatibility Proof

The execution-only attestation requires all of the following:

1. Removing exactly one frozen `copy` import and one frozen helper block from the current source reproduces the 23,802-byte predecessor exactly.
2. AST comparison finds no removed or changed shared function or method. The only added definitions are the two journal helpers.
3. Every Python artifact bound into the F07 v1.5 runtime identity is scanned; before scanning, every non-lifecycle runtime artifact must still match its v1.5 path, size, and SHA256. None references either helper. The live writer is outside this strict replay call surface.
4. A deterministic lifecycle corpus covering full fill, partial fill, cancel-pending fill, cancel reject, cancel ACK recovery, missing exchange clocks, and pre-activation reject produces exact predecessor/successor snapshots and event streams.

The successor plan intentionally changes the plan-derived namespace prefix in `lifecycle_id`, `source_callback_id`, and therefore `event_id`. The comparison reports both physical fingerprints and a mechanics fingerprint that normalizes only this verified bijective namespace. Event IDs must remain unique in each panel, lifecycle/callback suffixes must match, and every other field must be byte-equivalent. A non-identity field difference fails closed.

The machine-readable source attestation is the sibling JSON file. It reads no PnL, markout, reward, or campaign outcome.

## Boundary Diagnostic

The homogeneous successor plan was executed for `2026-06-24` through `2026-06-26`: one day before the source update boundary and both days whose original worker launch could have loaded the successor. The three days contain 228,083 journal rows.

After the explicit plan-namespace normalization, every row and all mechanics fields match. The three semantic fingerprints are:

- `2026-06-24`: `da9c09da5553142364ed8c05822aa88f11af1a4e3aa1418c0c14779a2035ff70`
- `2026-06-25`: `d2e298d58c806b291c691dcb3b512c579f2cd556c6b11705a3b8ed76cc5a015d`
- `2026-06-26`: `26f15f7df7547f088e4c06300eef0a21e2b70ece59cb8280fa8c571a80d574b8`

There are zero unexpected differing fields and event IDs are unique in both panels. The report remains `diagnostic_subset`; it cannot create the formal successor amendment or authorize lockstep.

## Homogeneous Successor Rule

The old run4 panel remains provenance-diagnostic because its process launches crossed the source update. Source compatibility alone does not authorize lockstep on that panel.

The v1.6 tool creates a new execution plan with the current source SHA, a new global runtime identity, new per-day runtime identities, and a new canonical plan SHA. All 40 days must be emitted under that plan. The old and new journal rows must then match exactly, in order, for every frozen day.

An affected-day or boundary-day comparison is useful diagnostic evidence but cannot build the successor amendment. `build-successor-amendment` requires:

- exactly 40 ordered days;
- exact row count and mechanics payload equality for every day after the explicit three-field plan-namespace normalization;
- a complete homogeneous successor panel;
- the passed source attestation;
- mechanics-only permissions.

For the formal comparison, `--successor-plan` supplies the frozen ordered day list. Hand-written 40-day argument lists are not accepted as formal amendment evidence unless they exactly match that plan.

Only the homogeneous successor panel may proceed to the new v1.6 C++ event lockstep. The old mixed panel is never promoted by compatibility declaration. The lockstep entry point is `order_lifecycle_v2_40day_cpp_lockstep_v1_6.py`; it requires the successor amendment and has no fallback to the frozen v1.5 provenance validator.

## Permissions

Economic evaluation, q90 action, strategy action, live transport, and live deployment are all false. CIF training remains blocked until the full 40-day successor emission, exact fingerprint comparison, successor amendment, and C++ event lockstep pass.
