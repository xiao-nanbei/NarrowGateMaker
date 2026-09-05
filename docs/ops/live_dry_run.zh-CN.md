# Live 与 Dry-Run 的边界

[English](live_dry_run.md) | [简体中文](live_dry_run.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

证据可用性：下述 SHA256 是身份元数据，不是下载链接。仓库相对链接指向公开文件。除上下文明确指出公共源码或 release 外，无公共链接的工件属于私有证据库，不随公共仓库分发。

受 Git 跟踪的 `live/config.yaml` 是公共模板，可安全阅读和加载，但不是私有实盘参数快照。

私有运行配置应位于公开文档之外，例如：

```bash
export NARROWGATE_LIVE_CONFIG="$PWD/docs/private/live_config.current.local.yaml"
bash live/run.sh start
```

准备启动、重启或部署之前，可用项目虚拟环境运行本地 preflight 诊断：

```bash
.venv/bin/python scripts/preflight_live_deploy.py \
  --config "$NARROWGATE_LIVE_CONFIG" \
  --repo-root .
```

Preflight 验证所选本地配置并解析模型/策略工件，但不能证明远端进程使用同一 release，也不授权激活。Live 启动独立消费 deployment envelope 和停机交易所对账。每次激活后，用 `live/run.sh status`、`live/run.sh profile` 和启动日志确认实际运行 release。

在已经准入的 live 部署中，完整配置字节与 release 绑定，只能通过重启变更，不仅限于各个热更新检查列出的字段。即使修改描述性字段也会改变字节；再次运行 preflight 不能使它获得热更新能力。应准备绑定目标配置的新 deployment envelope，完成所需停机对账，并通过正常部署事务激活。库中保留的通用 SIGHUP handler 不会覆盖部署配置绑定。见[运维流程](README.zh-CN.md)。

`make deploy-preflight` 拒绝标记为 `PUBLIC TEMPLATE` 的配置，仅接受 head 和 bundle manifest 明确允许 live 用途、且有字节绑定的模型包。这仍是本地验证结果，不是部署批准。公共合成、`public_dry_run_only`、`research_only`、缺失授权或 `authority.live=false` 工件都会被该检查拒绝。`make publish-source-dry`/`make publish-source` 是独立源码发布操作，不检查私有部署输入。

公共示例：

```bash
narrowgate doctor
narrowgate paths
bash live/run.sh dry-run
```

## 正式 dry-run

`bash live/run.sh dry-run` 是唯一公共运维 dry-run。没有环境覆盖时，它使用最小 [`live/formal_dry_run_public.yaml`](../../live/formal_dry_run_public.yaml) 辅助配置和受 Git 跟踪的合成模型包。它通过严格 live 配置解析器加载配置，并用 `strategy.model_contract.validate_model_bundle` 验证全部 model head，即使公共示例关闭了 ML。

命令不读取 `live/.env` 或 runtime profile，并在日志初始化、交易所/网络客户端构造、`MakerEngine`、WebSocket、worker thread 和任何报单路径之前退出。默认期限 30 秒，超时退出码为 124。可以显式选择其他正期限：

```bash
NARROWGATE_DRY_RUN_TIMEOUT_S=10 bash live/run.sh dry-run
```

stdout 只输出一个 JSON 对象。`status=passed` 和退出码 0 仅表示本地配置与模型合同验证在期限内完成。验证失败为 1，超时为 124。汇总包含配置/模型身份，以及明确为零的交易所客户端、线程和报单数量，不包含 API key 或完整配置。

检查其他本地配置而不改变命令合同：

```bash
NARROWGATE_LIVE_CONFIG="${NARROWGATE_ROOT}/docs/private/live_config.current.local.yaml" bash live/run.sh dry-run
```

公共合成工件没有研究、action、baseline、live 或部署权限。成功的本地 dry-run 不调用、不削弱、不替代 `scripts/preflight_live_deploy.py`、`start`/`restart` 的远端部署检查或 owner-side 证据要求。

`bash live/run.sh status` 不是 dry-run，只报告 maker 是否已运行，没有进程时返回非零。

## Runtime profile

不要用一次性 shell export 启动 `live/main.py`。`live/run.sh` 先加载未跟踪的凭据文件，再加载一个受 Git 跟踪的计算 profile：

```bash
NARROWGATE_LIVE_PROFILE=python bash live/run.sh profile
NARROWGATE_LIVE_PROFILE=native bash live/run.sh profile
```

Native profile 使用严格模式：连接报单路径前，启动过程验证扩展源码与全部必需 quote/signal/routing API。每次重启后用 `run.sh status` 和启动日志 `NATIVE_PROFILE` 确认实际实现。

## 报单网关

持久 REST 加一条全局串行 order-write lane 仍是默认 adapter。之前的 async latest-wins gateway 因目标主机 soak 恶化 p99/p99.9、且几乎没有有效 coalescing，已保持删除。当前有界 async-response lane、cross-side lane 和 USD-M WebSocket API adapter 是相互独立的 restart-only 实验，默认全部关闭；它们不是 latest-wins 队列或热更新开关。激活前需要 matched-host 延迟与经济验证。

不要公布 hostname、process id、原始 live PnL、账户规模或完整私有参数快照。

## 外部交易所凭据边界

Bitget、Bybit、OKX 的公共成交与 BBO/order-book channel 不需要认证。因此 NarrowGate 不保存它们的 API key；只有 Binance 执行凭据位于 `live/.env`。

这一边界仍允许原计划的 receive-time 研究 feed：

- Bitget `publicTrade`/`trade` 和 `books1` 或公共 `books`；
- Bybit `publicTrade` 与 `orderbook.1` 或公共 depth；
- OKX `trades`、`bbo-tbt` 或公共 `books`。

OKX 仅对 VIP 提供的 10ms `books-l2-tbt`、`books50-l2-tbt` 有意排除，因为它们需要登录及费率等级资格。除非另有经过审查的私有账户/VIP depth 需求，不要引入凭据管理。
