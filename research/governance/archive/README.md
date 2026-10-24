# Legacy Research Snapshot Availability

Last materially modified: 2026-08-12

The exact pre-migration snapshot archives are private historical evidence because their frozen payloads contain owner-specific locators from before the public/private documentation boundary was established. The archives are not distributed with the public repository, and this directory intentionally contains no substitute archive.

Evidence availability: both named archive artifacts are retained in the private evidence store and are not distributed with the public repository.

| Artifact ID | Migration contract | SHA256 | Bytes | Availability |
|---|---|---|---:|---|
| `research-layout-legacy-snapshot-v1` | [`layout_v1.json`](../migrations/layout_v1.json) | `6358405985e3f9bad937b0560f7fb8ed3724db528ef220f577ddc056773a7aaa` | 937843 | `private_not_distributed` |
| `research-layout-legacy-snapshot-v2` | [`layout_v2.json`](../migrations/layout_v2.json) | `f2444e06a09c69d26eabcd75b4687982a76ed1a6189c38b44c30493846f2e034` | 1933702 | `private_not_distributed` |

The migration manifests retain the historical logical paths, member identities, hashes, and byte counts. On an authorized owner checkout, historical reproduction resolves the corresponding artifact from the ignored `research/shared/experiment_governance/private/` evidence surface, where its semantic identity is registered in the local catalog, and verifies the complete archive hash before reading any member. A public checkout without those bytes fails closed; current canonical source must use a new experiment identity.
