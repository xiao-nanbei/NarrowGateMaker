# Git History Reconstruction Receipt

This repository history is a reconstructed public import created on 2026-08-12. It is not a fabricated timeline of when the research originally occurred. Dates retained in filenames and reports describe the research record; Git author and committer timestamps describe the reconstruction itself.

## Result

- Old public root: `774a4d03f5d025e82cc82c224db1d6cd3e25e819`.
- New reconstructed root: `f150ef7b7a99258c5b04fd06a1750e3377470c05`.
- Planned public history: 116 commits and 1,678 paths.
- Old history is retained only in a verified, FileVault-backed owner-only backup and a local archive ref. Neither is distributed.
- Private owner evidence, machine-specific paths, raw market data, private bundles, environment files, logs, and the two legacy snapshot archives are excluded from the reconstructed public tree.
- The final operational pointer is `operational_baseline_identity_20260812_v11` and retains its explicit `owner_risk_accepted_promotion` and `research_hard_gate_passed=false` semantics.

## Construction

Every existing public input was assigned exactly once by a machine-readable NUL-delimited commit plan. The path sets are disjoint, and the final registry and projection manifests were committed only after their target records existed.

Commits 001 through 115 were each exported with `git archive` and checked independently for planned tree membership, forbidden paths, JSON/YAML parsing, Python syntax, package import when available, and links to already committed targets. Staged paths were compared exactly with each planned path set.

Two auditable input amendments were required by the staged whitespace gate:

- one Python file had a redundant final blank line removed;
- 44 files received only CRLF-to-LF or single-final-newline normalization.

Both amendments retain before/after SHA256 records in the owner-only reconstruction ledger and do not change research semantics or path assignment.

## Tags

The `rebuild/*` annotated tags identify reconstructed public research records, not original execution commits:

- `rebuild/f02/p3-aggressive-reach-time-conditioned-hazard/v1-20260804`
- `rebuild/f05/causal-multichannel-window-boolean-cooldown/v2-20260812`
- `rebuild/f05/causal-multichannel-window-boolean-cooldown-persistent-policy/v3-20260812`
- `rebuild/f10/operational-baseline/v11-20260812`

## Publication Gate

The reconstructed history must first be pushed to `rebuilt-main-20260812`, pass repository validation and GitHub CI, and pass validation from a fresh clone. The public `main` branch may then be replaced only with `--force-with-lease` against the frozen old remote commit. Archive refs and the old-history bundle must never be pushed.

The machine-readable receipt contains the plan, amendment, result-ledger, root, and tag hashes used to audit this reconstruction.

## Post-Reconstruction Validation Fix

The frozen plan produced 116 commits. The first public-documentation audit then correctly found that two public projection SHA values still described the pre-normalization CRLF bytes of their CSV targets. A 117th, explicitly post-reconstruction validation commit synchronizes those two projection hashes and records this correction here. It changes neither research semantics nor private-source identity.
