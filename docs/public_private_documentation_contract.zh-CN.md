# 公开与私有文档及证据合同

[English](public_private_documentation_contract.md) | [简体中文](public_private_documentation_contract.zh-CN.md)

Last materially modified: 2026-08-29

Last materially synchronized: 2026-08-29

状态：当前公开文档合同。

实施回执：[公开/私有证据治理交接](public_private_governance_handoff_20260812.md)。

本仓库应当能够直接在 GitHub 上被理解，而不要求读者访问所有者的工作站、存储卷、云账户或 live 交易凭证。除非 Markdown 位于[非研究类私有证据所有者映射](non_research_private_evidence_owners.md)定义的 Git 忽略私有根目录，或具体研究单元自身被忽略的 `private/` 目录下，否则面向人的 Markdown 默认属于公开文档。

## 面向人的公开文档

公开 Markdown 必须用普通语言说明研究问题、数据权威、估计目标、数据划分、结果、局限和当前状态。仓库文件必须使用仓库相对 Markdown 链接。外部公开来源必须使用稳定的公开 URL。

不得公开个人绝对路径、物理卷名称、私有 IP 地址、SSH 目标、实例标识符、进程 ID、账户状态、原始 live 仓位或凭证。应改用[路径约定](path_conventions.md)中定义的占位符和逻辑部署 epoch 名称。

SHA256 是验证元数据，不是定位器。报告只有在同时给出产物名称和明确可用性时才可以展示哈希。每个被引用的产物都必须链接到公开仓库或 release asset，或者标记为 `private evidence store; not distributed with the public repository`。不得只留下裸哈希或本机路径，再要求公开读者自行推断产物所在位置。

除非文档提供了更具体的产物表，历史公开研究报告中嵌入的哈希均表示保存在私有证据存储中、未随公开仓库分发的所有者侧证据。所属单元的公开 README 和私有 catalog 共同定义可用性；哈希绝不表示对应字节可以从 GitHub 下载。

仅整理展示形式不会改变历史结论的历史身份。重新排版段落、将本机定位器替换为逻辑定位器，或补充可用性声明，都不得被描述成重新运行或新的科学结果。

## 语言与翻译身份

只供 agent 使用的指令统一使用英文正文，不创建翻译副本。这包括仓库内和所有者安装的 `SKILL.md`、skill reference 以及 agent metadata。代码标识符、路径、公式、哈希和协议 token 保持原样。

持续维护且面向人的叙述性指南采用成对文件：`name.md` 是规范英文文档，`name.zh-CN.md` 是对应的简体中文文档。两份文件均在顶部附近提供互相指向的语言链接；除代码、标识符、路径、公式、专有名词和首次出现时确有必要的技术术语外，每份正文只使用一种自然语言。

对状态、结论、安全边界、命令、链接或证据可用性的任何实质修改，都必须在同一次变更中同步更新两个语言版本。两份文件必须记录相同的 `Last materially synchronized: YYYY-MM-DD`。译文可以符合目标语言习惯，但必须保留相同的实质主张、局限和权限边界，不能缩写成摘要。

机器可读记录、manifest、receipt、生成输出以及受哈希约束的冻结证据保持单一字节身份，不制作翻译。两种语言的指南必须引用同一个底层产物。若编辑或复制不可变历史 Markdown 会破坏证据身份，则允许保留其原始语言；应由持续维护的双语索引或读者摘要解释，而不能把翻译冒充为冻结原件。

不得批量为整个历史文档树制造翻译副本。优先维护当前 README、quickstart、架构、贡献、安全、运营和研究导航指南。其他持续维护且面向人的指南在发生实质修改时再补齐或修复语言配对。英文许可证文本是唯一权威版本；任何面向读者的中文解释都必须明确声明其不具权威性。

## 公开机器可读记录

受 Git 跟踪的 JSON 和其他机器可读记录可以保留已执行产物的哈希和冻结身份字段。面向读者的定位器应使用仓库相对路径、批准的占位符、逻辑 artifact ID 或公开 URL。若公开 projection 隐去了私有定位器，它必须通过哈希标识私有源产物，并说明变换只修改了定位器；不得声称与私有源字节完全相同。

`research/` 之外的仓库级公开 projection 记录在 [`public_machine_document_projections.json`](public_machine_document_projections.json) 中；研究单元拥有的 projection 记录在 [`research/public_machine_document_projections.json`](../research/public_machine_document_projections.json) 中。这些索引在不公开私有定位器的前提下，将每个公开 projection 绑定到其私有源字节的 SHA256。

当可变公开机器记录的 successor 改变了治理语义，而不是仅隐藏定位器时，该记录必须停止声称 projection 身份。应将 successor 发布为普通、安全的公开 JSON，删除其 active projection-manifest 条目，并记录已退役 predecessor 的公开 projection 哈希、predecessor 私有源哈希、私有可用性，以及被忽略的 predecessor 源未被重写的声明。这些 predecessor 哈希只是历史退役元数据；它们不会使 successor 成为 projection，也不会授予对私有字节的访问权。

只有当机器记录的字节可由公开 checkout 获得，或在同一次变更中加入受 Git 跟踪的公开树时，它才是公开 projection。仍位于 Git 忽略 model bundle 目录下的脱敏记录是私有 working-tree projection，不是 GitHub 产物。其身份保存在被忽略的 `models/private/` 索引中，可用性为 `private_working_tree_projection_not_distributed`；不得把它们计入公开 projection manifest。

产物可用性字段应使用以下值之一：`public_repository`、`public_release`、`private_not_distributed`、`private_working_tree_projection_not_distributed`、`restricted_raw_source` 或 `derived_reproducible_not_distributed`。消费含占位符记录的代码只能解析 allowlist 中的占位符，并在占位符不可用时 fail closed。

## 私有文档与定位器

跨项目私有运营细节属于被忽略的 `docs/private/`；live、data、model 和 execution 组件的证据属于[非研究类私有证据所有者映射](non_research_private_evidence_owners.md)中定义的对应私有根目录；研究专用定位器和证据索引属于[公开研究与私有证据布局](../research/PRIVATE_EVIDENCE.md)所定义的所属单元被忽略 `private/` 目录。私有 Markdown 以 `Local only — do not publish.` 开头。私有 JSON 或 YAML 使用顶层 `visibility` 或 `documentation_scope` 值 `local_only_do_not_publish`。位于 `private/original_public_machine_records/` 下、按原字节保存的历史源，以及 consumer 拒绝未知字段的 schema 受限 runtime/config 记录，应保持不变，并从其被忽略目录、`README.local.md` 和 catalog 条目继承私有分类。当前主机 pointer、物理存储映射、含 secret 的配置和仅供所有者使用的产物 catalog 均位于这些私有表面。由于它们不属于公开仓库，所以可以包含精确的本地定位器。

一个产物只能有一个规范私有所有者。consumer 应引用所有者和稳定 artifact ID，而不是把本地路径复制到另一个 catalog。仓库级当前 host/config 权威仍属于 `docs/private/`；组件私有根目录不得覆盖它。公开运营 pointer 或治理 identity 可以概述当前 live 与 replay 默认值的分离，但不能取代私有 current-host pointer、精确 live-config alias、owner release 或已准入证据链。冻结 health receipt 不是最新 liveness 权威，运营证据也不会自动成为动作实际发生、经济效果或 backtest 权威。secret 永远不是证据产物；`live/.env` 的内容和哈希都不得进入 catalog 或文档。

当 manifest 和哈希验证通过时，私有证据仍是所有者侧字节验证的权威。除非确实发布了脱敏证据包，否则公开文档必须如实说明这一限制，不能构造并不存在的链接。

## 审查门

发布前应运行 [`audit_public_documentation.py`](../scripts/audit_public_documentation.py)；在获得授权的所有者 checkout 上还应运行 [`audit_private_evidence.py`](../scripts/audit_private_evidence.py)。公开审计会扫描面向人的文档、机器文档、源代码、公开 archive、结构化进程标识符、projection 可用性和仓库链接。私有审计会验证所有者根目录、marker 与 catalog policy、权限、语义产物哈希、公开/私有 projection 绑定，以及未公开的 model projection 索引。同时还应验证 Markdown reflow 具有幂等性，并确保受保护的代码块、表格、公式、front matter 和显式 hard-break block 保持不变。
