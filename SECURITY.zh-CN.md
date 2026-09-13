# 安全政策

<p><a href="SECURITY.md">English</a> | <a href="SECURITY.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

## 报告漏洞

请通过仓库的 [GitHub private vulnerability reporting form](https://github.com/xiao-nanbei/NarrowGateMaker/security/advisories/new) 报告疑似漏洞。这是首选渠道，因为在问题评估期间，报告和后续沟通都会保持私密。

如果 GitHub 未向你的账户提供该表单，请创建一个[信息最少化的公开 issue](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new/choose)，仅请求维护者建立私密报告渠道。不要在该 issue 中包含漏洞、exploit、proof of concept、受影响的 host、credential、私有数据或安全敏感日志。

本仓库未公布专用的公开安全邮箱。不要根据贡献者姓名或 GitHub 账户猜测或构造邮箱地址。

## 私密报告应包含什么

- 对漏洞和可能影响的简明描述；
- 已知情况下受影响的 version、tag 或 commit；
- 使用 synthetic 或公开数据的最小复现步骤；
- 已移除所有 secret 和私有 locator 的相关配置假设；
- 任何建议的 mitigation 或 disclosure constraints。

绝不要发送 exchange credential、signing key、原始 account state、私有 host 详细信息、owner-side evidence 或 restricted dataset。维护者可能会要求一份更小、已脱敏的复现材料。

## 范围与披露

安全报告的范围是仓库中受维护的公开代码和已记录的接口里的漏洞。普通正确性 bug、研究分歧、性能问题和不受支持的部署问题，除非会造成具体安全影响，否则应使用普通 issue tracker。

请在公开披露前，给维护者留出分类评估和协调修复的时间。本政策不承诺回复时限、bounty、embargo length 或 support window。历史研究 tag 和 execution tag 是不可变的 provenance；它们的存在并不声明每个历史 revision 都能获得安全更新。

## 商业授权

安全报告不是商业授权渠道。NarrowGateMaker 将 [PolyForm Noncommercial License 1.0.0](LICENSE) 用作公开源码（source-available）许可证。该许可证允许其条款中规定的非商业用途，并限制商业使用；商业使用需要另行获得书面许可。本仓库未公布专用授权邮箱；对于非机密询问，请使用 [commercial-license issue form](https://github.com/xiao-nanbei/NarrowGateMaker/issues/new?template=commercial_license.yml)。公开 issue、discussion 或 pull request 本身不构成授权。
