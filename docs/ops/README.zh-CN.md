# 运维说明

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-30

Last materially synchronized: 2026-08-30

运维文档涵盖 dry-run 设置、私有配置边界、部署 guardrail 和 telemetry。这些文档不得包含私有主机、账户详情或原始 live PnL。

- 本地 dry-run 工作流：[live_dry_run.md](live_dry_run.md)
- Live 部署回执与历史路由属于所有者私有信息，不随公共仓库分发。
- Binance USD-M 1000-level snapshot/diff 健康探针：`.venv/bin/python scripts/probe_binance_deep_book.py --help`
- 当前部署和 live 命令：仓库 `README.md` / `README.zh-CN.md`

## 通用部署流程

公共部署内核不绑定云厂商，并明确分成两条边界：

- `make deploy-dry` / `make deploy` 只发布一个精确的公共源码 checkout；它们不会复制 config、model、credential、envelope、reconciliation receipt、runtime receipt，也不会执行任何进程控制命令。
- Runtime 构建与 activation 使用 checkout 之外、由部署者提供的私有材料。发布源码不等于取得交易授权，也不会启动交易。

源码 transport 要求本地 Git checkout 完全 clean。它从 `HEAD` 创建并验证 bundle，通过 SSH 流式传输到远端，在同一文件系统的 staging 目录 clone，验证精确 commit、tree 与 clean status，然后原子 rename 到指定的绝对 release path。若目标已是同一精确 release，则幂等接受；若已有 release 或 staging 的身份不同，则 fail closed。

安全顺序是：

1. 创建无特权 service user、由该用户拥有的 release parent，以及单独的 mode-`0700` private root；SSH 与出站凭证只开放 venue 连接所需的最小权限。
2. 从 clean public clone 运行 `make deploy-dry`，核对绑定的 commit/tree，再运行 `make deploy`。这一步只发布源码，绝不重启进程。
3. 把私有 config、model bundle 及其 authorization manifest、wheel、lock、wheelhouse 与已准入的输入 receipt 放在 private root 下，不能放进 Git checkout。Deployment envelope 与 stopped-exchange reconciliation 稍后必须针对精确 release 在该目录中构建。
4. 构建或接收 content-addressed wheelhouse，再用 `python3.12 -m live.deployment_runtime install` 创建 commit-bound 环境。Release wheel 必须来自 clean checkout/worktree；构建前清除生成的 `build/`、`dist/` 与 `*.egg-info` 状态，避免已删除文件从陈旧构建树进入 wheel。目标机器安装时不得从 package index 解析依赖。
5. 同时执行 `verify-install` 与 `verify-static-tree`，再把绝对 `venv-<execution-commit>` 路径绑定为 release 内被 Git 忽略的 `.venv-active` selector。
6. 从精确 checkout/runtime authority 构建 deployment envelope。Model authorization manifest 是必需的 envelope member；可选 policy artifacts 只能按完整组加入。针对私有配置执行 `make deploy-preflight`，并在 maker 完全停止时通过 `live/run.sh reconcile-stopped` 生成 exchange barrier。
7. 只有 process、runtime health、仓位/订单 reconciliation 和日志检查都通过后才能准入。然后构建紧凑的 activation receipt，再原子发布 current selector；精确 activation/rollback evidence 仅保存在私有侧。

Authority 命令都已经公开且通用：`live.deployment_runtime build-envelope` 从精确文件与 receipt 推导 deployment envelope；`live/run.sh reconcile-stopped ABSOLUTE_PATH` 只有在证明 maker 完全停止后才执行 signed exchange reads；`build-activation-receipt` 绑定已验证的 envelope、stopped reconciliation 和实际 live runtime identity；`publish-current-pointer` 在原子更新 selector 前会重新验证这条 lineage。必须使用它们输出的 canonical root，不能手写这些 JSON，也不能把旧 private receipt 复制进 current authority。

无需私有数据即可查看通用子命令与必填字段：

```bash
python3.12 -m live.deployment_runtime --help
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.deployment_runtime verify-envelope-startup --help
python3.12 -m live.deployment_runtime build-activation-receipt --help
python3.12 -m live.deployment_runtime publish-current-pointer --help
python3.12 -m live.native_build_receipt --help
python3.12 scripts/live_deploy_common.py source-release --help
```

## AWS EC2 示例

以下全部为占位符，不代表当前主机或 release identity。AWS live 部署不是“五分钟完成”的承诺；仓库 README 中的五分钟入口只指本地、不会交易的 demo。

EC2 创建检查清单：

- 使用带 systemd 的 64-bit Linux；locked live runtime 要求 CPython 3.12。
- 根卷启用加密，镜像中不保存可复用数据或凭证。
- SSH 入站只允许 operator CIDR，不开放公共应用端口。
- 出站只保留所配置 venue 的 HTTPS/WSS 端点；依赖在 build host 解析，目标安装时不联网解析 package。
- 使用专用无特权 `narrowgate` service user；root 仅用于主机与 service 设置。
- 在 operator/build host 使用下面的真实公开 URL clone；所有 operational authority 另行私有注入。

生产访问优先使用 [AWS Systems Manager Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)，或者通过 [EC2 security-group rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules.html) 严格限制 SSH。源码 transport 使用 operator 现有的 OpenSSH 配置，因此 SSM `ProxyCommand` 应放在 `~/.ssh/config`，不能作为不安全的命令行选项传入。

在实例上创建两个互相隔离的根目录。Release parent 由 service user 写入，且 group/other 不可写；private root 权限为 `0700`：

```bash
sudo useradd --system --create-home --shell /bin/bash narrowgate
sudo install -d -o narrowgate -g narrowgate -m 0755 /opt/narrowgate/releases
sudo install -d -o narrowgate -g narrowgate -m 0700 /opt/narrowgate/private
```

在 operator/build host clone 并发布一个精确、clean 的源码 release。locked live/native build 还必须提供一个显式的 annotated public release tag。传输层会验证 tag object 能 peel 到 `HEAD`，并且只传这个 tag，绝不传输本地全部 tags。纯源码 dry-run 可以省略它，但这样的结果不足以生成 live/native receipt。

```bash
git clone https://github.com/xiao-nanbei/NarrowGateMaker.git
cd NarrowGateMaker

export NARROWGATE_RELEASE_TAG="v0.1.2"
git checkout --detach "$NARROWGATE_RELEASE_TAG^{commit}"
export NARROWGATE_DEPLOY_TARGET="narrowgate@<ec2-address>"
export NARROWGATE_RELEASE_DIR="/opt/narrowgate/releases/<release-id>"

make deploy-dry
make deploy
```

该命令拒绝 dirty 本地 checkout，也拒绝相对 release path；它不会复制或读取任何私有材料。另行创建每个 release 的私有目录：

```bash
sudo install -d -o narrowgate -g narrowgate -m 0700 \
  /opt/narrowgate/private/<release-id>
```

通过部署者批准的 secret/artifact channel，把私有 config、model bundle 及其 authorization manifest、lock、wheelhouse、wheel 与已准入的输入 receipts 传到这个 private directory。不要预先复制其他 release 的 envelope 或 reconciliation；二者必须按下文构建。不得把私有材料放进 `/opt/narrowgate/releases/<release-id>`。

在实例上推导精确的 commit-bound venv 名称，并只使用已经传输且由 hash 绑定的 artifacts 安装。Install receipt 和 `venv-<commit>` 必须具有同一个私有 parent，因为 live startup 会强制检查这一关系：

```bash
RELEASE_DIR=/opt/narrowgate/releases/<release-id>
PRIVATE_DIR=/opt/narrowgate/private/<release-id>
COMMIT=$(git -C "$RELEASE_DIR" rev-parse HEAD)
VENV_DIR="$PRIVATE_DIR/venv-$COMMIT"
RECEIPT="$PRIVATE_DIR/install-receipt.json"
cd "$RELEASE_DIR"

python3.12 -m live.deployment_runtime install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT"
```

记录 `install` 输出的 canonical receipt SHA256，在暴露 selector 前完成动态与静态两条验证命令：

```bash
python3.12 -m live.deployment_runtime verify-install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

python3.12 -m live.deployment_runtime verify-static-tree \
  --venv "$VENV_DIR" \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

test ! -e "$RELEASE_DIR/.venv-active" && test ! -L "$RELEASE_DIR/.venv-active"
ln -s "$VENV_DIR" "$RELEASE_DIR/.venv-active"
test "$(readlink "$RELEASE_DIR/.venv-active")" = "$VENV_DIR"
```

从已经安装且绑定 commit 的环境执行通用 Linux native receipt。它会独立要求唯一 annotated tag、精确 wheel/runtime authority、native ABI 与 parity smoke tests：

```bash
NATIVE_RECEIPT="$PRIVATE_DIR/native-build-receipt.json"

"$VENV_DIR/bin/python3" -I -B "$RELEASE_DIR/live/native_build_receipt.py" \
  --repository-root "$RELEASE_DIR" \
  --annotated-tag "$NARROWGATE_RELEASE_TAG" \
  --wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --builder-python /usr/bin/python3.12 \
  --runtime-lock "$PRIVATE_DIR/runtime.lock.json" \
  --runtime-lock-sha256 <lock-sha256> \
  --dependency-wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --dependency-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --install-receipt "$RECEIPT" \
  --install-receipt-sha256 <install-receipt-canonical-sha256> \
  --output "$NATIVE_RECEIPT"
```

使用精确的私有 active config、native-build receipt 和由 model contract 选中并验证的 model authorization manifest 构建 deployment envelope。这一个必需 manifest 在内部绑定已准入的 model heads 与 P3 artifact；不得在部署命令、service environment 或 pointer 中重复它们的 leaf hashes。即使所有可选 action policy 都关闭，`--model-authorization` 也始终必需。如果 active config 绑定 SELL Boolean cooldown，必须同时提供 `--boolean-policy-file` 和
`--boolean-predicate-bundle`。如果绑定 BUY E3 policy，必须同时提供
`--policy-artifact-manifest`、`--policy-file` 和 `--predicate-bundle`。未启用的 policy
应整组省略。
将 `MODEL_AUTHORIZATION` 设置为 `scripts/preflight_live_deploy.py` 输出的精确 `model_authorization_path`；不得复制或重命名该文件。

```bash
ENVELOPE="$PRIVATE_DIR/deployment-envelope.json"
MODEL_AUTHORIZATION=<preflight-输出的精确-model_authorization_path>

python3.12 -m live.deployment_runtime build-envelope \
  --repository-root "$RELEASE_DIR" \
  --active-config "$PRIVATE_DIR/live-config.yaml" \
  --native-build-receipt "$NATIVE_RECEIPT" \
  --model-authorization "$MODEL_AUTHORIZATION" \
  --output "$ENVELOPE"
```

Config、model bundle、已构建的 deployment envelope 与已构建的 stopped-exchange reconciliation 都放在 `PRIVATE_DIR` 下，由 `narrowgate` 所有，私有文件权限为 `0600`。分两阶段准备 root-owned、mode-`0600` 的 service environment file，例如 `/etc/narrowgate/live.env`：先写入 config、envelope root 与 trusted Python locator，并为 stopped reconciliation 命令导出同一组值；再加入该命令打印的 reconciliation path 与 canonical root。Deployment envelope 是外部唯一的 release digest；nested manifests 在内部派生并验证 lock、wheelhouse、wheels、native module、interpreter、installed `RECORD`、config 与 policy members，不再把这些 leaf hashes 重复写进 service environment。

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_PATH=<absolute-private-envelope-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH=<absolute-private-reconciliation-path>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_TRUSTED_PYTHON_PATH=<absolute-cpython-3.12-builder-path-with-pip>
```

如果所选私有 policy 启用了对应 gated mechanism，其 invocation environment 还必须携带匹配的私有批准 flag：`NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY`、`NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY` 或 `NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY`。不得设置无关 flag，也不得把这些批准提交到仓库。

Activation 前，在精确公共 checkout 中单独执行私有配置 preflight：

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path> make deploy-preflight
```

Maker 完全停止后，在已经导出私有 config 与 deployment-envelope root 的环境中，以 `narrowgate` service user 生成 create-only exchange barrier。该命令会执行 signed venue REST reads，要求 open orders 为零且精确仓位稳定，并打印上述 reconciliation 环境字段所需的 path 与 canonical root。Account identity 保留在该 canonical payload 内，并与运行时 credential 比较，不再作为另一个外部 digest。

```bash
RECONCILIATION="$PRIVATE_DIR/stopped-exchange-reconciliation.json"
"$RELEASE_DIR/live/run.sh" reconcile-stopped "$RECONCILIATION"
```

`live/run.sh` 对 release startup authority 只接受 envelope path/root 与 trusted Python locator。它先用 trusted standard library 校验 canonical envelope，并在执行仓库代码前证明 Git commit/tree 与 clean checkout；随后调用 `verify-envelope-startup`，由 nested manifests 验证 runtime、installed distributions、全部 installed `RECORD` 与 `pip check`。Trusted Python path 是 OS bootstrap trust anchor：路径必须 canonical，文件必须 root-owned、非符号链接、单硬链接、可执行且 group/world 不可写。不要把 API credentials 或私有 authority 值写进 unit 或仓库。下面是对 NarrowGate 内置 supervisor 的最小 systemd 包装：

```ini
[Unit]
Description=NarrowGate live supervisor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=narrowgate
WorkingDirectory=/opt/narrowgate/current
EnvironmentFile=/etc/narrowgate/live.env
ExecStart=/opt/narrowgate/current/live/run.sh service
ExecStop=/opt/narrowgate/current/live/run.sh stop
ExecReload=/opt/narrowgate/current/live/run.sh reload
Restart=no
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

只有全部私有 authority gate 都可用后，operator 才能原子切换另行准备的 `/opt/narrowgate/current` 文件系统 selector 并启动 service。源码发布本身绝不改变这个 selector。执行 `systemctl start narrowgate` 后，检查 `systemctl status narrowgate`、`live/run.sh status`、`logs/runtime_health.json` 以及最近的 supervisor/engine logs。PID 存活并不充分：仓位与 open-order reconciliation 必须收敛，而且不能存在 ownership 或 execution-state safety latch。

上述准入通过后，只绑定三个已验证的 activation 输入，然后发布紧凑的私有 current pointer。使用前面命令输出的 canonical root，以及已准入进程写入的绝对 runtime identity 路径：

```bash
ACTIVATION_RECEIPT="$PRIVATE_DIR/activation-receipt.json"
CURRENT_POINTER=/opt/narrowgate/private/current.json
RUNTIME_IDENTITY=<absolute-runtime-identity-path>

python3.12 -m live.deployment_runtime build-activation-receipt \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --stopped-reconciliation-sha256 <stopped-reconciliation-canonical-sha256> \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$ACTIVATION_RECEIPT"

python3.12 -m live.deployment_runtime publish-current-pointer \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --activation-receipt "$ACTIVATION_RECEIPT" \
  --activation-receipt-sha256 <activation-receipt-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$CURRENT_POINTER"
```

JSON current pointer 只是一个五字段 release selector。其 `release_id` 通过 owner-private routing inventory 与对应 release directory 解析；pointer 本身不包含 host routing 或 leaf artifact inventory。`status=selected_activation` 只表示该 activation 在 lineage 验证后已被选中，不是 live-health 断言，也绝不替代 service、exchange 或 runtime-health 检查。旧的 verbose receipts、command transcripts 和逐文件 hash inventory 可以作为审计附件继续保存在私有侧，但它们只属于 audit，不得参与 startup authority，也不得复制进 current pointer。

Rollback 是另一次经过验证的部署，不是盲目 restart。先停止 service 并要求 clean stop，核对交易所仓位与 open orders，再把 release/config/envelope selectors 切到已验证的上一私有 release，重新运行 preflight 与 static-runtime verification，然后启动并重复 health admission。如果 stop 报告 uncertain execution state，在人工 reconciliation 完成前不得激活任一 release。
