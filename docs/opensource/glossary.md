# Open Source Glossary

Last materially modified: 2026-08-23

Status: Current contributor-facing terminology.

## Authority and Evidence

**Action authority**
: Permission for a tested policy to alter an order-path action. Prediction quality,
  shadow output, a positive point estimate, or a merged pull request does not grant
  it.

**Active**
: A mode in which a policy can change effective quoting or order behavior. It is
  distinct from `shadow` and requires explicit action and live governance.

**Artifact availability**
: Where named bytes can actually be obtained. Common public-document values include
  `public_repository`, `public_release`, `private_not_distributed`,
  `restricted_raw_source`, and `derived_reproducible_not_distributed`.

**Development, Validation, sealed holdout**
: Chronological evidence roles with separate read permissions. Development supports
  construction and initial gates; Validation is read only after its declared gate;
  a sealed holdout remains unread until its contract permits access. Evidence must
  not flow backward into earlier choices.

**Evidence**
: Inputs, protocols, outputs, and receipts whose identity and permissions support a
  bounded claim. File existence, a bare hash, or an unavailable private locator is
  not evidence admission.

**Live authority**
: Permission for a bound policy and runtime identity to operate on the live path.
It is separate from research, prediction, and action authority.

**Owner-risk-accepted promotion**
: A permanently labeled operational route accepted by the owner despite research
hard gates not establishing research-supported promotion. It does not rewrite the
failed research result.

**Promotion**
: A governed transition in permission or operating status. It is never implied by
a tag name, merge, score, shadow run, or receipt alone.

**Public projection**
: A sanitized public representation of a private machine record. It has its own
public byte identity and must not be described as byte-identical to the private
source.

## Source and Execution

**Annotated execution tag**
: An annotated Git tag naming the exact tested clean source admitted for one formal
execution attempt. It is provenance, not proof of completion or authority.

**Clean commit / clean worktree**
: The exact tracked public source has no uncommitted overlay. Formal execution starts
from this identity; authorized runtime overlays are recorded separately.

**Execution attempt**
: One admitted implementation run under an `attempt-*` identity. An ordinary bug,
crash, cache mismatch, race, serialization error, or performance repair creates a
new attempt after requalification, not a new research `vXX`.

**Final receipt**
: An immutable completion record that binds named result hashes to the pre-run
manifest and unchanged research contract. A failed run uses an immutable failure
receipt and cannot support economic inference.

**Pre-run manifest**
: The immutable, SHA-bound declaration frozen before economics are read. It binds
research contract, clean source, annotated tag, source artifacts, runtime, cache,
output schema, and permissions for one attempt.

**Research identity / research `vXX`**
: The identity of a scientific question, defined by frozen sample, baseline and
candidate ladder, folds, estimand, and statistics. It changes only when one of those
elements changes.

**Research attempt tag**
: Contributor shorthand for the annotated execution tag bound to one `attempt-*`
manifest. The canonical attempt ID is in the manifest; this tag is neither a
version/stability tag nor a research `vXX`.

**Stability gates**
: Pre-economic qualification covering representative single-day output, an all-fold
zero-economic walk, concurrency/cache durability, regression/parity, complete
output shape, and a clean worktree.

**Version or stability tag**
: A source publication or maintenance milestone. It is distinct from a research
identity, an execution attempt, and the annotated tag bound to a formal attempt.

## Shadow

`Shadow` is intentionally overloaded in historical project material. Read the noun
it modifies and the local contract; do not assume every shadow surface has the same
mechanics.

**Policy or live shadow**
: A candidate is evaluated beside the effective path and may write decisions or
diagnostics, but the candidate must not change effective quotes or orders. If it
changes the action, it is active rather than shadow.

**Data or connector shadow**
: A read-only market-data stream used for reference, freshness, flow, toxicity, or
transport evidence. External venue shadow connectors are not execution feeds, do
not place orders, and cannot silently become quote authority.

**Replay or what-if shadow**
: A diagnostic comparison evaluated on historical or replay state. Its causal and
execution meaning depends on the declared simulator. A fill-sequence what-if, for
example, is not automatically a full queue/latency/order-lifecycle counterfactual.

**Shadow log or artifact**
: A record emitted by one of the preceding modes. The name describes collection or
comparison behavior, not evidence quality, privacy, or promotion status.

Project-wide constraints apply to every meaning:

- shadow does not grant research, action, deployment, or live authority;
- shadow cannot substitute for authoritative full-path replay or causal action
  evidence;
- observation-only shadow is not a mandatory promotion stage;
- engineering shadow for parity, transport, or incident diagnosis must be bounded
  by an explicit owner, byte or rate budget, and expiry;
- `shadow` does not mean private: public/private classification is governed
  separately by artifact availability and the documentation contract.

See the [direct promotion contract](../../research/shared/experiment_governance/docs/action_bound_full_path_direct_promotion_contract_v1.md)
for the current project constraint.
