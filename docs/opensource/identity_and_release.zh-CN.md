# 源码、研究与执行身份

[English](identity_and_release.md) | [简体中文](identity_and_release.zh-CN.md)

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

状态：当前公共命名与 provenance 指南。

当前公开软件 release：annotated tag `v0.1.1`，其对应源码树使用包版本 `0.1.1`。当前 `main` 开发版本：Python 与 C++ distribution metadata 均为 `0.1.2.dev0`。开发版本不是 release tag。不可变的 `v0.1.1` tag 继续标识其经过测试的公共 clone surface，并与所有 research reconstruction 和 execution-attempt tag 保持独立。

NarrowGateMaker 使用多种身份，因为源码发布、科学问题、执行 attempt 和结果字节分别回答不同的审计问题。不得将它们合并成一个版本号或 tag。

## 身份层级

| 身份 | 它标识什么 | 它不能证明什么 |
| --- | --- | --- |
| Git commit 与 tree | 受 Git 跟踪的精确公共源码 | 外部数据、runtime config、结果字节或 authority |
| Version 或 stability tag | 一次公共源码 release 或受维护的稳定性里程碑 | 新研究问题或已完成的 formal run |
| Research identity 或 research `vXX` | 一组冻结的 sample、baseline/candidate ladder、fold、estimand 和 statistical contract | 某次具体 executor repair 或 run |
| Execution attempt ID | `attempt-*` namespace 中的一次 admitted run | 包版本、scientific-contract 变更或成功结果 |
| Annotated execution tag | 为某次 attempt 准入的精确 clean source | 输入字节、完成状态、经济有效性、action authority 或 live authority |
| Pre-run attempt manifest | Research contract、source、artifact、runtime、cache、schema 与 permission 之间的绑定 | 结果完成，或超出其显式字段的 permission |
| Final 或 failure receipt | 绑定回 pre-run manifest 的不可变完成或失败记录 | 未由独立治理显式授予的 authority |
| Artifact SHA256 | 某个命名 artifact 的精确字节 | 公共可用性或获取位置 |

## 必需的 Formal Chain

```text
development branch
-> stability gates
-> clean commit
-> annotated execution tag
-> SHA-bound pre-run manifest
-> final receipt
```

Final receipt 记录结果 artifact hash，并将其绑定到不可变的 pre-run manifest。失败时使用 failed-attempt receipt。绝不能在 run 结束后编辑 manifest 来迎合结果。

## 身份何时改变

| 变更 | Research identity | Execution attempt |
| --- | --- | --- |
| 修复 implementation bug 或 crash | 保持 | 所有 gate 通过后创建新 attempt |
| 修复 cache、concurrency、resume、serialization 或 performance 行为 | 保持 | 所有 gate 通过后创建新 attempt |
| 仅修改公共说明，不改变 execution 或 conclusion | 保持 | 无需新 attempt |
| 修改 sample、baseline 或 candidate ladder、fold、estimand 或 statistics | 新建 | 在新 identity 下创建新 attempt |
| 在一次已准入的 infrastructure failure 后，使用相同 source 和 contract 重跑 | 保持 | 使用自己的 manifest 和 receipt 创建新 attempt |

负面或不确定证据不是创建新 research identity 的理由。想得到更整洁的版本号同样不是理由。

## Tag 纪律

Version 或 stability tag 与 research execution tag 面向不同读者。前者标识一个 source-release milestone；后者是某次 admitted attempt 的 annotated provenance object。Execution attempt ID 使用 `attempt-*`；不得将它伪装成 `formal-vXX` 或其他 research version。

贡献讨论中的“research attempt tag”指绑定到一个 `attempt-*` manifest 的 annotated execution tag。Manifest 保存规范的 attempt ID。Release 或 stability tag 都不能替代其中任一身份。

对于 formal execution：

- 只为经过精确测试的 clean commit 添加 tag；
- 使用 annotated tag，并在 pre-run manifest 中绑定该 tag；
- execution 之后绝不移动、替换、删除或复用 tag；
- repair 后的 run 使用新的 attempt ID、manifest、annotated tag 和 receipt；
- 除非独立治理另有说明，否则明确声明 action authority 与 live authority 均为 false。

历史 F05 `formal-v13` 至 `formal-v27` 标签早于当前命名合同。它们作为同一 research identity 下不可变的历史 attempt alias 保留，不能成为将 implementation fix 晋级为 research version 的先例。[Formal execution contract](../../research/shared/experiment_governance/docs/formal_execution_attempt_and_evidence_freeze_contract_v1_20260821.md)记录了这项规范化。

## Authority 边界

任何身份对象都不能授予超出其显式声明的权限。尤其是：

- stable 或 release tag 不验证 research；
- attempt tag 或 manifest 不证明 completion；
- final receipt 本身不授权 action 或 deployment；
- research result 不会静默成为当前 live baseline；
- SHA 不证明公共读者能够访问对应字节。

应使用显式 permission field 和当前公共治理文档，不得根据 filename、date、version、tag 或某次检查成功来推断 authority。
