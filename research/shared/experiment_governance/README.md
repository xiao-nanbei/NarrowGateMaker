# G: Experiment Governance

Last materially modified: 2026-08-12

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

The exact v1 and v2 research-layout migration snapshots are owned by this governance unit as private historical evidence. Their public identities and availability are documented in [`research/governance/archive/README.md`](../../governance/archive/README.md); the archive bytes are not distributed with the public repository, and historical reproduction fails closed when the private artifact is absent or does not match its frozen SHA256 and byte count.

Canonical code remains at `models/audit/experiment_manifest.py`, `models/audit/evidence_split.py`, `models/audit/experiment_scorecard.py`, and `models/audit/panel_promotion_controller.py`.

Dataset selection for new experiments is governed by [`unified_data_universe_and_split_contract_v1`](docs/unified_data_universe_and_split_contract_v1_20260812.md) and `models/audit/dataset_governance.py`. The project has one append-only daily capability universe, while each experiment derives its source-compatible denominator and chronological evidence split from explicit requirements. New full-path action studies use the canonical 50-day execution denominator unless they register a capability-driven reduced-support identity.

Research continuation after a frozen gate is governed by [`dual_path_research_progression_contract_v1`](docs/dual_path_research_progression_contract_v1.md) and `models/audit/research_progression.py`. Hard-gate evidence remains immutable; an explicit owner path may continue and can ultimately reach live only under the separately labeled `owner_risk_accepted_promotion` route.

Strategy promotion after a concrete action has been frozen is additionally governed by [`action_bound_full_path_direct_promotion_contract_v1`](docs/action_bound_full_path_direct_promotion_contract_v1.md) and `models/audit/action_bound_full_path_promotion.py`. Observation-only shadow is not a mandatory promotion stage. Both research-supported and owner-risk routes require authoritative full-path economics, execution parity, production preflight, and automatic rollback before direct active deployment.
