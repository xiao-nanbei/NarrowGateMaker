# 源码、研究与执行身份

[English](identity_and_release.md) | [简体中文](identity_and_release.zh-CN.md)

Last materially modified: 2026-09-26

Last materially synchronized: 2026-09-26

状态：当前公共命名与 provenance 指南。

历史事件：所有者于 2026-09-13 授权整合历史并移除当时的 Git 标签，以 `version-alpha-20260913` 标记该阶段。当前 `main` 已有后续提交链；这不是只许保留一个可变根提交的现行要求。普通工作遵守当前 AGENTS 和本轮明确权限，以本地提交保存；推送、标签、合并及历史重写不自动授权。Python/C++ 包版本 `0.1.2.dev0` 不能唯一标识执行，应分别记录准确 commit/tree 和外部输入／配置。

私有恢复资料保留此前 Git 身份，不随仓库分发。历史引用不得重新绑定到当前提交；冻结实验保留原身份。下文身份层级用于解释证据；历史标签流程不是要求普通文档修改重写 Git 历史或创建新标签。

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

## 明确授权时的常规 Formal Chain

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

## 明确授权时的 Tag 纪律

Version 或 stability tag 与 research execution tag 面向不同读者。前者标识一个 source-release milestone；后者是某次 admitted attempt 的 annotated provenance object。Execution attempt ID 使用 `attempt-*`；不得将它伪装成 `formal-vXX` 或其他 research version。

贡献讨论中的“research attempt tag”指绑定到一个 `attempt-*` manifest 的 annotated execution tag。Manifest 保存规范的 attempt ID。Release 或 stability tag 都不能替代其中任一身份。

对于 formal execution：

- 只为经过精确测试的 clean commit 添加 tag；
- 使用 annotated tag，并在 pre-run manifest 中绑定该 tag；
- execution 之后绝不移动、替换、删除或复用 tag；
- repair 后的 run 使用新的 attempt ID、manifest、annotated tag 和 receipt；
- 除非独立治理另有说明，否则明确声明 action authority 与 live authority 均为 false。

## Authority 边界

任何身份对象都不能授予超出其显式声明的权限。尤其是：

- stable 或 release tag 不验证 research；
- attempt tag 或 manifest 不证明 completion；
- final receipt 本身不授权 action 或 deployment；
- research result 不会静默成为当前 live baseline；
- SHA 不证明公共读者能够访问对应字节。

应使用显式 permission field 和当前公共治理文档，不得根据 filename、date、version、tag 或某次检查成功来推断 authority。
