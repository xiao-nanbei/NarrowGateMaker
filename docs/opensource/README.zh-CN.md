# 公开源码（Source-Available）贡献者指南

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

状态：当前公共参与和导航指南。

请使用本页寻找通往目标工作的最短路径。NarrowGateMaker 包含公共软件、公共研究合同，以及对未随仓库分发的所有者侧证据的引用。这些表面适用不同的审查规则。

NarrowGateMaker 按 [PolyForm Noncommercial License 1.0.0](../../LICENSE) 公开源码。该许可证允许其条款中规定的非商业用途，并限制商业使用。因此，项目文档使用**公开源码（source-available）**，而不把本仓库描述为不限使用领域的开源软件。贡献、公共 clone 或已合并的 pull request 都不授予商业使用权限。

## 按目标导航

| 我想要…… | 前往…… |
| --- | --- |
| 了解并运行公共项目 | [README](../../README.md)、[中文 README](../../README.zh-CN.md) 和[开发者检查](../dev/ci.md) |
| 将一个 UTC 日从下载推进到诊断 replay | [单日数据流水线](one_day_data_pipeline.md) |
| 了解许可用途 | [许可证](../../LICENSE) |
| 报告代码缺陷 | [Bug 报告表单](../../.github/ISSUE_TEMPLATE/bug_report.yml)和[贡献工作流](../../CONTRIBUTING.zh-CN.md) |
| 提议一项聚焦的功能 | [功能请求表单](../../.github/ISSUE_TEMPLATE/feature_request.yml) |
| 改进公共文档 | [文档报告表单](../../.github/ISSUE_TEMPLATE/documentation.yml)和[公共文档合同](../public_private_documentation_contract.zh-CN.md) |
| 提议或发布研究证据 | [研究证据 PR 规则](research_contributions.md)和[研究证据表单](../../.github/ISSUE_TEMPLATE/research_evidence.yml) |
| 了解版本、tag、attempt、manifest 和 receipt | [源码、研究与执行身份](identity_and_release.zh-CN.md) |
| 配置合并保护 | [必需检查与分支保护](../dev/branch_protection.md) |
| 理解项目术语 | [术语表](glossary.md) |
| 报告安全漏洞 | [安全策略](../../SECURITY.zh-CN.md) |
| 咨询商业授权 | [许可证](../../LICENSE)和[商业许可表单](../../.github/ISSUE_TEMPLATE/commercial_license.yml) |

GitHub 在线表单可通过 [issue 选择器](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new/choose)访问。

## 公共 clone 的边界

公共 clone 应当支持代码审查、synthetic 测试、公共文档检查，以及研究审查中的公共部分。它不包含所有者侧数据集、证据存储、模型 bundle、live 配置、凭证、私有主机定位符或原始账户状态。

不得通过虚构路径、下载未经批准的替代品，或将冻结身份重新绑定到当前字节来填补这些缺口。缺失的私有证据必须保持明确不可用并 fail-closed。在发布 artifact 或命令前，请阅读[路径约定](../path_conventions.md)和[公共/私有文档合同](../public_private_documentation_contract.zh-CN.md)。

## 联系边界

仓库不公布专用的安全或商业许可邮箱。安全细节只能提交到[私有漏洞报告表单](https://github.com/xiao-nanbei/NarrowGateMaker/security/advisories/new)。当该表单不可用时，可以通过公共 issue 请求建立私有沟通渠道，但不得披露漏洞。商业许可问题可以使用公共 issue 表单，但 issue 本身既不保密，也不构成授权。
