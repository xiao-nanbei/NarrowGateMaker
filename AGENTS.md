# NarrowGate repository instructions

Last materially modified: 2026-09-21

## Scope and authority

The owner-authorized 407-day recomputation is indexed by research/recompute_407.json. Historical closed/exhausted/promotion decisions do not block new question-specific experiments. Preserve historical identities, defect regression tests and previous-use; do not relabel old artifacts as new results. Preserve the running F03 frozen implementation and later-authorized ML-OFF while developing other families. No unlimited candidate budget or live/publication authority follows from the total research plan.

These instructions apply to the checkout containing this file, not to another remote's release. Read the actual branch, commit/tree and dirty state; preserve unrelated edits and active jobs. A published copy governs only its own tree. Never infer a repository's visibility from its name.

Current dataset scope, source policy and migration status are defined once in [data/dataset_scope.json](data/dataset_scope.json). Read the selected experiment's actual plan and frozen manifests for splits, model/calibration policy, baseline, latency, accounting and current stage. A policy change is not proof of implemented consumers or completed validation. Do not extend dates with today's clock or copy historical exclusions into a new source identity.

## Task routing

Load only the route needed for the request:

- Data parsing, quality, acquisition or cleanup: [data workflow](.agents/skills/mm-as-glft-ml-spread/references/data-workflow.md) and the current [data guide](data/README.md).
- Supervised learning, replay comparison or OPE: [research methods](.agents/skills/mm-as-glft-ml-spread/references/research-methods.md), then the owning family in [research/registry.json](research/registry.json) and its selected experiment contract.
- Quote units, P3 or quote/action semantics: [.agents/skills/mm-as-glft-ml-spread/SKILL.md](.agents/skills/mm-as-glft-ml-spread/SKILL.md).
- Actual live operations or historical live matching: [live workflow](.agents/skills/mm-as-glft-ml-spread/references/live-workflow.md); this route never grants deployment authority.
- Commit, push or tag work: [publication protocol](.agents/skills/mm-as-glft-ml-spread/references/publication.md). Do not load Git history-rewrite instructions for ordinary research.
- Pure engineering changes: inspect actual callers and test behavior preservation; no PnL, propensity or deployment requirement unless the requested change needs it.

## Development and ownership

Save authorized source changes by local commit/amend. Push only when the owner explicitly requests a push for the current task; previous automatic-publication instructions do not authorize future pushes.

Use the project `.venv/bin/python`, checking Python >=3.11 before Python work. Use it for pytest, ruff and compilation. Run focused tests proportional to changed behavior, applicable lint and `git diff --check`; expand when shared consumers are affected. Do not claim unrun checks passed. Keep long local macOS jobs under `caffeinate`; remote jobs need their own durable runner.

Test doubles must implement the supported production interface. Dependency injection is encouraged; do not add alternative business-method protocols just to accommodate an obsolete fixture. Normalize genuinely supported legacy callers once at the installation/startup boundary, and retain real assembly and failure-path tests.

Read [module ownership](docs/architecture.md) before relocating code. Reuse maintained modules rather than creating parallel frameworks or one-off runners. Acquisition adapters belong to `data/downloaders/` when that migration exists in the actual checkout; facts/observations belong to `data/`, shared features to `features/`, runtime contracts to `narrowgate/runtime/`, family-specific research to its `research/families/` owner. Existing `models/audit/` locations are transitional, not a default destination for all new diagnostics. Verify imports and actual CLI help before recommending commands. Do not recreate removed paths.

No project symlinks, supplier aliases, duplicate raw trees or placeholder data. Keep raw and derived directories physically separate. Before cleanup verify replacement, active users and index references; age alone does not authorize deletion. Preserve purchased originals, unique edits and frozen research/live evidence.

Before substantial batches, check actual resource health and bounded per-shard disk/RAM, including concurrent jobs. Resolve resource placement from the current owner request/private inventory, not dates or addresses in historical notes. Do not create/start cloud resources or change live because a skill mentions them.

## Documentation, privacy and results

Read [path conventions](docs/path_conventions.md) and [public/private contract](docs/public_private_documentation_contract.md) before editing or publishing documents. Agent instructions stay English; materially changed maintained human guides retain their bilingual pair and synchronization dates. Keep prose paragraphs on one physical line. Do not upload purchased data, credentials, private locators, models or sealed results with source.

Report what this task changed, tested and left incomplete. Quote analysis needs units/clocks; a downloader or directory repair does not need a quote decomposition. Use one run identity for source tree, configuration, data/model roots and output receipt; do not proliferate leaf hashes across prose, code and tests. Source identity need not be public: preserve an exact local/private commit or immutable source snapshot and explicitly bind any authorized overlay.
