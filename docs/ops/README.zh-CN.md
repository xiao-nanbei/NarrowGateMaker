# 运维文档

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-02

Last materially synchronized: 2026-09-02

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
5. 根据准入的 runtime/config/model/policy closure 构建唯一 deployment envelope；
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
```

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
