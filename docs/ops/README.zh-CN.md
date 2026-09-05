# 运维文档

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

本目录保存可复用的公共运维合同，不得包含当前主机、credential、账户/订单/持仓状态、
active release identity、private artifact location、策略参数或 live economics。

## 运维手册

- [本地 live dry-run](live_dry_run.md)：使用公共 synthetic material 的 no-trading
  integration check。
- [AWS EC2 live](aws_ec2_live.zh-CN.md)：一次性主机准备、完整与 Python-only 增量
  release、systemd ownership、fresh reconciliation、health admission、current pointer、
  stop/resume 与 rollback。
- [Azure Batch 私有 replay](azure_batch_replay.zh-CN.md)：persistent pool definition、
  zero-node idle state、每个 task 一个 UTC day、`_SUCCESS` admission、故障处理、缩容与
  最终清理。
- 仓库 [README](../../README.zh-CN.md)：公共 quickstart 与项目级边界。

五分钟上手指本地 synthetic demo，不表示 credentialed live host 或私有 replay estate
可以在五分钟内安全创建。

## Authority budget

SHA256 只证明 byte identity，不能替代 deployment、research 或 health semantics。Leaf
identity 留在负责它的 manifest 内；下一层边界只暴露所需的最小 root set。

| 边界 | 外部身份 | 不向外重复复制 |
| --- | --- | --- |
| 公共 source | Git commit/tree 与一个 annotated release tag | tracked source 的逐文件 hash |
| Build/runtime | runtime 或 wheelhouse manifest root | 每个 dependency、installed `RECORD` 与 native leaf |
| Live release | deployment-envelope root | config、model、policy、runtime 与 native leaf |
| Stopped exchange barrier | reconciliation root | service configuration 中重复的 account/order/position leaf |
| Activated live release | activation-receipt root | receipt 已绑定的 runtime identity 与 reconciliation leaf |
| Research run | source identity、runtime root、input-manifest root 与 output-receipt root | cache key 与每个 input/output leaf |

Mutable current pointer 不自哈希，只包含 release selector、activation-receipt root、schema
与 status；它不是 health record。Cache hash 只是 cache key，不授予 research 或 live
authority。不得把 leaf identity 复制进 Python constant、environment variable、test、
Markdown、current pointer 或多层 receipt。

## Provider-neutral deployment boundary

公共 deployment kernel 明确分成两层：

- `make publish-source-dry` 与 `make publish-source` 只发布 clean public source
  checkout，不读取或传输 private config、model、credential、receipt 或
  process-control authority，也不启动进程。
- Runtime construction 与 activation 消费 checkout 之外由 operator 提供的 private
  material。发布源码永远不授权交易。

安全顺序：

1. 创建 non-root service identity，并分离 release/private root；
2. 发布精确 clean source release；
3. materialize 完整 private runtime 与 artifact closure；
4. 在目标机不联网解析依赖的前提下安装并验证 commit-bound environment；
5. 根据已验证的运行环境、配置、模型和策略工件构建唯一 deployment envelope，只登记操作者明确批准的策略；
6. live 停止时创建 fresh exchange reconciliation；
7. 通过唯一 process owner 启动并观察 runtime health；
8. admission 通过后才构建 activation receipt 并原子发布紧凑 current pointer。

命令帮助是 canonical field reference，不要把 private deployment transcript 复制进公共
文档：

```bash
python3.12 -m live.deployment_runtime --help
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime verify-static-tree --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.deployment_runtime verify-envelope-startup --help
python3.12 -m live.deployment_runtime build-activation-receipt --help
python3.12 -m live.deployment_runtime publish-current-pointer --help
python3.12 -m live.native_build_receipt --help
python3.12 scripts/live_deploy_common.py source-release --help
python3.12 scripts/live_deploy_common.py activate-prepared-release --help
```

`build-envelope` 绑定所选配置实际需要的工件。ML 开启时，保留 `--model-authorization` 和全部 13 个 head 的 schema/私有授权检查。不提供 `--model-authorization` 时，必须通过 `--p3 <artifact>` 为 ML-OFF 独立绑定 P3 工件；关闭推理不会取消 P3 验证。状态条件策略处于 `shadow` 或 `active` 模式时，需提供 `--state-conditioned-policy <artifact>`。这些字节由现有 envelope 绑定，不需要再增加 manifest 或审批文件。

策略许可保存在现有 deployment envelope 中，不由环境变量或研究结论授予。只有获得明确批准后，才为 `build-envelope` 添加对应的 `--approve-policy <policy>`，可重复指定 `q90_action`、`f05_boolean_cooldown`、`f05_buy_e3` 或 `state_conditioned_quote_policy`。状态条件策略仅在 `active` 模式需要动作批准；`disabled` 和 `shadow` 不授予动作权限，说明已有 shadow 模式也不代表允许启用新的 shadow 机制。不得根据配置字段自动生成批准参数。Envelope 将批准列表与精确配置、策略工件绑定；启动入口在创建 engine 前只判断一次启用动作是否已获批准，之后将该结果直接传给运行身份记录和日志，不新增 receipt 或哈希链。

配置校验与普通 `preflight_live_deploy.py` 只检查配置和工件兼容性；普通预检明确输出“未判断策略准入”，不能授权启动。现有发布事务的 `candidate-verify` 另以 `--check-policy-approval` 消费经过验证的 deployment envelope 并调用同一个授权函数，使缺少批准的候选在停旧服务前失败。这项停机前诊断不是启动凭据：新进程只判断一次自己已验证的 release，不能信任以前的预检输出。BUY E3 加载器不把 `research_supported`、`owner_risk_accepted` 或历史 `evidence_route` 说明当作许可，状态条件策略的研究 `promotion_status` 也不是 live 授权。历史结论继续保存在研究记录中。旧的 `NARROWGATE_ALLOW_*_PRIVATE_DEPLOY` 和 `NARROWGATE_ALLOW_STATE_CONDITIONED_POLICY_LIVE` 环境变量不再授予权限。状态策略工件结构、支持的动作、重叠度/提升下界/新鲜度限制及运行数值检查继续生效；模式和工件路径仍只能通过重启变更，reload 保留启动时加载的字节。没有 `policy_approvals` 的旧 envelope 不批准任何可选策略：全部关闭时仍可使用；启用策略则必须构建经过批准的新 envelope，不能原地修改不可变发布。将本次变更发布为新的源码 commit 不会部署或重启 live。

创建 envelope 时执行完整的嵌套 build 校验，包括原始 lock、wheelhouse 和 wheel archive。普通 envelope 加载及启动不再重新读取这些构建输入，但仍验证绑定的 native-build receipt、install receipt 和当前 native module；启动还验证完整已安装 `RECORD` 清单、解释器和 ABI。这是将构建证据与已安装运行环境验证分开，并未取消运行时完整性检查。

Release、private environment、active config、locked runtime 与 deployment envelope
已经在主机上准备好后，`activate-prepared-release` 用一次 SSH 事务完成余下 activation。
缺省只输出 dry-run plan；只有指定 `--execute` 才改变远端状态。正常模式先在旧服务仍运行时
验证 candidate，然后严格执行 stop/quiescence、fresh reconciliation、start、bounded health
admission、activation receipt 与 current-pointer publication。Runtime-fatal recovery 则先证明
已经停止的 selected release 的 lineage、fail-closed health、systemd exit 与进程静默，再从
fresh reconciliation 汇入同一后续顺序；pointer 最后发布。Pointer
rename 前的失败不会发布 pointer；candidate 已启动但未通过 health admission 时会停止
candidate，并且不会自动重启旧 release。`--service-user` 默认使用经过校验的 EC2 contract
用户 `ec2-user`，也可指定
另一个合法 service identity。只能通过本地 SOCKS5 到达的主机使用受限的
`--socks5-proxy HOST:PORT`；命令不接受任意 SSH option。

正常事务接受由文档化 `systemd-run` contract 创建、正在运行的 transient
`narrowgate.service`。停机前必须证明 `active/running`、`Transient=yes`、精确 previous
working directory、正数 `MainPID`、匹配的 `/proc/<pid>/cwd`，以及 previous release 的
`live/main.py` command line。Persistent unit 或不明确进程会在 stop 前失败，并保持不变。
如果上一次事务已经停止这个精确 release、但尚未创建 reconciliation 就失败，唯一续接入口是
`--resume-stopped`。它要求 maker/supervisor 均不存在、unit inactive 或不存在、current
pointer 仍指向 previous release，且 reconciliation/activation output 都尚未出现；随后会重做
candidate verification 并生成 fresh reconciliation。它不是绕过正常停机前证明的通用开关。

已经成功选中的服务后来以 78 退出，属于另一种情况：只能使用独立的
`--recover-runtime-fatal`，不能使用 `--resume-stopped`。该模式要求 compact current
pointer、对应 activation receipt、deployment envelope、旧 stopped reconciliation 与
activation 绑定的 runtime identity 构成完整一致链；runtime-health 必须属于同一 PID，正常情况下应记录最终 fail-closed 且需要 reconciliation 的状态。如果 writer 在发布最终 health 前失败，则必须由同一真实进程与 journal invocation 记录最终 health 发布失败及随后的 operator-gated exit。两种路径都必须有匹配的 systemd `EXIT_STATUS=78`；仅有过期健康快照不能恢复。Unit 必须 inactive 或不存在，maker/supervisor 进程必须完全
不存在。新 candidate 必须使用从未存在的新 stopped-reconciliation 和 activation-receipt
路径，旧 release 的文件不能冒充 fresh evidence。随后仍必须执行 fresh signed
reconciliation、start、health admission、新 activation receipt 和 pointer-last publication。
若 journal 或 lineage 证明缺失，主机保持 stopped。

Journal 证明分两段有界流式读取：先读取 runtime identity 时间戳之后的旧 PID，再读取匹配的
systemd invocation；小内存 live 主机不会把无界历史日志载入内存。

在正常 candidate 参数之后，显式加入失败 current release 的三个不可变 lineage 文件：

```bash
python3.12 scripts/live_deploy_common.py activate-prepared-release \
  <正常-candidate-参数> \
  --recover-runtime-fatal \
  --previous-deployment-envelope <失败-release-envelope> \
  --previous-activation-receipt <失败-release-activation-receipt> \
  --previous-stopped-reconciliation <失败-release-stopped-reconciliation> \
  --execute
```

本次 transaction 的 stopped-reconciliation 和 activation-receipt 输出必须使用尚不存在的
新路径，且不能指向上述三个旧文件；已有 current pointer 仅在最终提交步骤被原子替换。

私有 systemd `EnvironmentFile` 必须由独立的 `NAME=value` 行组成。追加运行配置选择项前必须确保上一行以换行结束，否则新字段会粘到前一个密钥值后面。策略许可保存在 deployment envelope 中，不放在该文件里。验证时不得打印该文件。

Health admission 还要求 `reconciliationPending=false`，并且 `lastTickAge` 是零到一秒内的
有限值。一秒上限来自现有 100ms 主循环安全时钟，并为有界 scheduler jitter 留出空间；
同时要求 private user stream 已连接，且 advancing health observations 使用同一个正数
connection generation；不要求 admission 期间必须发生 private fill 或 user-stream event。
Pointer rename 是提交点；若随后 parent-directory `fsync` 失败，命令报告 commit uncertain，
保留 candidate 运行供人工核验，不会停止一个可能已经发布的 release。

通过 admission 的进程内部也采用固定启动顺序：engine 先完成 warmup 与遗留订单撤销，再启动并
确认 private user stream。随后在完整 private callback 串行屏障内执行精确账户、仓位与
open-order 对账，冻结 prospective epoch 并挂载异步 lifecycle writer；屏障中等待的 callback
再按 FIFO 原序释放。之后才启动 public market stream，最后启动周期 metrics polling。因此
public 行情既不能修改尚未完成的 initial checkpoint，也不能早于对应 evidence writer。

Remote upload 或 task 启动前，递归计算完整 materialization closure。每个 manifest
reference 必须在 admitted bundle 内，或作为显式 immutable resource 提供。Archive 上传
成功不等于 closure admission。

## 不可妥协的运维不变量

- 一个 process owner：live 使用 systemd；offline task 使用 Azure Batch。
- Source upload 永远不 restart 或 activate live。
- 每次 live activation 都使用新建 stopped reconciliation。
- PID、`LoadState`、exit code 或 current pointer 单独都不构成 health admission。
- 一个 formal Batch task 只代表一个注册 UTC day。
- `_SUCCESS` 最后写入，并且只有和匹配 output manifest 一起才有效。
- Queue 排空后 paid Batch node 回到零。
- Unknown execution state、missing closure 或 ambiguous ownership 必须 fail closed。
- Private live 与 replay material 保存在 public Git checkout 之外。

公开边界参见[公开/私有文档合同](../public_private_documentation_contract.zh-CN.md)。
