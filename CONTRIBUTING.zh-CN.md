# 为 NarrowGateMaker 做贡献

<p><a href="CONTRIBUTING.md">English</a> | <a href="CONTRIBUTING.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

感谢你帮助改进 NarrowGateMaker。本仓库同时包含常规软件工程和受证据治理的做市研究，因此审查路径取决于改动声称的内容。

请先阅读[按目标组织的贡献者指南](docs/opensource/README.zh-CN.md)。本项目按 [PolyForm Noncommercial License](LICENSE) 公开源码（source-available）；该许可证允许其条款中规定的非商业用途，并限制商业使用。提交 pull request 并不授予商业使用权限。

## 选择正确的路径

| 目标 | 起点 |
| --- | --- |
| 报告可复现的代码 bug | 使用 GitHub bug report form |
| 提议功能或机制变更 | 使用 GitHub feature request form |
| 更正或澄清公开文档 | 使用 documentation report form |
| 提议或发布研究证据 | 先阅读[研究证据 PR 规则](docs/opensource/research_contributions.md)，再使用 research evidence form |
| 报告漏洞 | 遵循[安全策略](SECURITY.zh-CN.md)；不要创建包含详细信息的公开 issue |
| 询问商业授权 | 使用公开的 commercial-license issue form；本仓库未公布专用授权邮箱 |

对于重大变更，请在投入实现之前先创建 issue。每个 pull request 应聚焦于一个可审查的目标。

## 开发工作流

1. 从当前受维护的公开分支创建分支。
2. 实施能解决所述问题的最小改动。
3. 按行为风险的高低添加或更新测试。
4. 运行 [Developer Checks](docs/dev/ci.md) 中相关的检查；维护者还需保持准确的 [branch-protection check names](docs/dev/branch_protection.md)。
5. 检查完整 diff 中涉及的隐私、证据和权限声称。
6. 使用仓库模板创建 pull request。

普通代码、构建、测试和文档变更都遵循这套工作流。一项 bug fix 不会仅因它影响了研究 executor 就变成新的研究结果。

发布 wheel 应从干净的源码 checkout 或临时源码树构建。旧 `build/lib` 目录可能保留已删除模块，并在下次构建时重新把它们打入 wheel；不要把其中内容当作受维护源码。应在 checkout 之外测试实际安装的 wheel，包括它需要的包内资源。

## 研究身份与正式执行

研究身份和执行身份彼此独立。只有下列至少一项冻结的科学合同元素改变时，研究身份才会改变：

- sample；
- baseline 或 candidate ladder；
- folds；
- estimand；
- statistical contract。

只要这些科学合同元素不变，实现 bug、crash、cache mismatch、concurrency race、serialization error 或 performance repair 仍保持原有研究身份。不得将它们重命名为新的研究 `vXX`。普通代码审查需要与风险相称的软件检查，不需要新建经济研究。修复后的执行器用于下一次正式运行时，应记录新的执行尝试及实际测试的源码与输入身份，不得覆盖失败尝试或复用其部分经济结果。

正式顺序为：

```text
development branch
-> stability gates
-> exact tested clean commit
-> annotated execution tag
-> SHA-bound pre-run manifest with an attempt-* identity
-> formal execution
-> immutable final receipt binding result hashes to the pre-run manifest
```

正式研究的执行合同定义所需稳定性检查：代表性单日输出、fold/输入覆盖、并发与 cache 持久性、regression/parity、输出形状 smoke，以及精确源码身份。每项检查在其实际负责的边界触发。纯文档修改不需要全量市场回放；局部运行时修复需要针对性回归及受影响 backend/状态机检查。在发布新的正式经济结果之前，验证该研究要求的执行拓扑、输入与输出完整性。只有测试输入及行为未变、且研究合同允许时，才可复用既有检查。

失败的正式运行会获得一份不可变的 failed-attempt receipt，且没有资格用于经济推断。在 development line 上修复问题，说明影响和验证范围，然后为下一次正式运行创建新的 `attempt-*` 身份。该运行已冻结合同要求的更严格检查仍需保留，不得静默弱化历史证据要求。不得移动、改写或删除失败的 tag、manifest 或 receipt。

完整边界见[正式执行合同](research/shared/experiment_governance/docs/formal_execution_attempt_and_evidence_freeze_contract_v1_20260821.md)与[身份指南](docs/opensource/identity_and_release.md)。

## 公开贡献边界

不要将下列任何内容提交或粘贴到 issue、pull request、test fixture、screenshot、log 或 document 中：

- 个人绝对路径或物理存储位置；
- 私有 hostname、IP address、SSH target、cloud 或 account identifier，或 process identifier；
- credential、token、environment file、signing material 或包含秘密的 configuration；
- 私有 dataset、受许可的 source bytes、owner-side evidence、原始 live account state、position、order 或 fill；
- 泄露私有 locator 的生成报告，即使报告本身在其他方面无害。

使用 [Path Conventions](docs/path_conventions.md) 中的占位符。SHA256 是验证 metadata，不是可下载的位置。每个被引用的 artifact 都必须链接到公开 bytes，或明确标记其真实可用性，例如 `private_not_distributed`。绝不得虚构链接，也不得用当前 bytes 替代缺失的冻结 artifact。

公开研究材料必须在无法访问 owner 计算机的情况下仍可独立理解。在添加 evidence artifact 前，请阅读[公开/私有文档合同](docs/public_private_documentation_contract.zh-CN.md)和[公开研究证据布局](research/PRIVATE_EVIDENCE.md)。

## Pull request 要求

每个 pull request 都应说明：

- 问题和行为变更；
- 受影响的文件和 authority surface；
- 已运行的测试和公开文档审计；
- 该变更属于普通工程改动、不改变研究身份的新执行尝试，还是科学合同变更；
- 研究涉及的 artifact availability 和 evidence permissions；
- 任何仍存在的局限，或有意不提供的私有依赖。

研究证据 pull request 还需满足[研究证据指南](docs/opensource/research_contributions.md)中的额外要求。无论是已合并的 pull request，还是已通过的研究运行，都不授予 action 或 live authority。

## 文档审计

对任何公开文档或机器可读记录的变更，运行：

```bash
.venv/bin/python scripts/audit_public_documentation.py --repo-root .
git diff --check
```

公开审计必须报告零项发现。不要求公开贡献者检查 owner-private evidence store，也不授权他们这样做。
