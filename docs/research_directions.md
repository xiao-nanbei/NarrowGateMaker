# Basic research directions

[English](research_directions.md) | [简体中文](research_directions.zh-CN.md)

Last materially synchronized: 2026-10-29

Detailed research, experimental plans, strategy implementations, effective parameters and results are not public. For interest or questions about the research topics, please email me privately at [xuantan650@gmail.com](mailto:xuantan650@gmail.com).

This is a qualitative map of questions, not a disclosure of implementations, tuned values, experiment designs, rankings or returns. The demo does not implement these research families.

| Family | Basic question |
| --- | --- |
| F01 — Parameter robustness | How sensitive is a simple market maker to fixed parameter choices? |
| F02 — Price touching | How does quote distance relate to the chance of a future price touch? |
| F03 — Causal prediction | Can a prediction use only information available at its decision time? |
| F04 — External market information | Can related markets add information after availability and timing are accounted for? |
| F05 — Fill quality | How should execution probability be separated from what happens after a fill? |
| F06 — Quote placement | How do price location and displayed liquidity relate to execution opportunities? |
| F07 — Resting-order decisions | When is retaining an existing quote preferable to replacing it? |
| F08 — Execution choices | How do passive and aggressive execution differ across an order's lifecycle? |
| F09 — Inventory lifecycle | How should opening, holding and reducing inventory be evaluated consistently? |
| F10 — Replay comparison | How can simulated behavior be compared with observations under explicit assumptions? |

Shared engineering questions concern reproducibility, causality and transparent accounting. No scheduler or orchestration implementation is included. These questions do not assert that any particular mechanism works, is used in production or improves returns. Answers require independent data rights, appropriate baselines and out-of-sample validation.
