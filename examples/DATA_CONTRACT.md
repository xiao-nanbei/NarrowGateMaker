# Synthetic data contract walkthrough

[English](DATA_CONTRACT.md) | [简体中文](DATA_CONTRACT.zh-CN.md)

Last materially synchronized: 2026-09-19

Run `python -m examples.data_contract_demo` from an installed repository. The example creates invented Tardis-format CSV in a temporary directory, invokes the current fact parser and observation/feature consumers, prints the results, then removes only its temporary files. No purchase, network access, model or licensed market record is required.

The controlled book contains an atomic two-level snapshot and one absolute-quantity update. Trades include a duplicate ID and a distinct ID at the same timestamp: only the duplicate is removed, leaving two executions and quantity 3. The observation scenario explicitly models delay using a source-timestamp proxy; unknown historical clock evidence remains unknown. Initial undelivered features remain missing. Later frames retain causal readiness. The generated products include observations, visible Bars, independent outcome Bars and feature frames, not labels or trained models.

This tiny fixture demonstrates interfaces, not Top20 coverage, 407-day acceptance, actual supplier mapping, native packet parity, economic validity or migration of every research family. Inspect [data interfaces](../data/README.md), [scope](../data/dataset_scope.json), [runtime](../data/runtime.py), [observation semantics](../data/observation.py), [family registry](../research/registry.json), and [tests](../tests/test_data_contract_demo.py). Historical artifacts cannot be relabelled as current inputs merely because the fixture runs. Additional causality, snapshot, trade identity and consumer tests live in `tests/test_data_facts.py`, `tests/test_data_observation.py` and `tests/test_public_input_panel.py`.
