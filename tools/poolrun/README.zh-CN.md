# PoolRun Lite

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-04

Last materially synchronized: 2026-10-04

心跳丢失时，Attempt 仍占槽并处于不确定状态，不等于失败。仅当新鲜、已认证的 Agent 轮询确认 runner 与业务进程均存活、PID/进程创建时间/系统启动时间与原进程一致，且当前 Attempt 和代次未变时，才能恢复 `RUNNING`。待执行停止命令、其他不确定原因、终态及已被替代的 Attempt 不会被此逻辑清除。旧 Agent 未提供存活证据时仍保守保持 `UNKNOWN`。SSH 隧道需要操作系统级保活与断线重连托管，单独修改空闲超时无法恢复已退出的隧道。

通用、有限批次、数据感知的任务池。一个 Python 主控、每台主机一个出站轮询 Agent、独立 runner；只用 JSON 快照和本机文件锁。**不绑定上海、Azure、某个研究项目或固定机器规格。** 人工提供主机与资源预算，调度器不购买、扩容或删除机器。

独立调度子项目，源码与测试由 NarrowGate 仓库管理，安装与运行环境独立。未接管任何现有 NarrowGate 任务，未连接研究主机，未传输生产数据。当前为可运行首版，不等于任意业务的无缝迁移承诺。

## 1. SSH 与同属主本地控制

### 已有 prepare 缓存优先分配

Agent 在通过既有 SSH 会话报告就绪状态前，扫描管理员配置的 `prepared_cache_roots`，例如 `{"replay-tape":"/approved/shared/prepared"}`。每个直接子缓存目录须有 `manifest.json`，包含准确的 `identity` 对象，以及相对文件名到字节数的 `files` 映射。未完成目录、符号链接、缺文件、大小不符均不计就绪。盘点只读，每30秒刷新，不替代业务读取器原有的缓存验证。

提交适配器按实际缓存生产者的身份加入 `"cache_affinity":{"namespace":"replay-tape","identity":{"schema":"public-prepared-v2","manifest":"<input-manifest-identity>","tick":"0.1"}}`，不能用账户名或日期代替。所有候选主机配置同一命名空间，各自指向实际物理目录。主控先收集新鲜清单，再只向已有匹配缓存的兼容主机分配，即使需要等待该主机当前任务结束。offer和START均检查此条件。提交或主控重启后，缺少其他主机报告最多等待30秒；离线或排空主机不会永久占住任务。自身盘点未成功的主机不能领取该类任务。没有可达且符合条件的缓存持有者时，仍允许原准备流程；`why`会显示盘点或缓存主机等待原因。超过60秒的缓存清单或超过30秒的主机报告不参与判断。

未声明 `cache_affinity` 的任务保持原行为：输入文件就绪不能描述应用内部隐藏的prepare缓存。提交适配器负责声明绑定，不回写正在运行的任务规格。本功能不传输缓存、不重启或迁移活动Attempt；并发、资源限制及数据传输政策不变。

Python 3.12+，Linux/macOS；运行依赖 `psutil==7.2.2`，测试依赖 `pytest==9.1.1`，远程控制使用系统 OpenSSH 客户端和服务端。PoolRun 不提供 HTTP/HTTPS 服务、TCP 控制监听、TLS 配置、Bearer token 或 HTTP 隧道。远程控制路径是 Agent/CLI → 主动出站 SSH 标准输入输出 → 固定 gateway → 本机 Unix socket → 唯一常驻 Master。同一系统属主的本地 Agent 可以指定 `socket` 和原有 `host`，不再指定 `ssh`；请求仍使用 Agent 身份，只具有 Agent 操作权限，系统属主的信任边界不变。gateway 不构建 Master，也不打开 state.json。工作站主导的任务优先将主控放在本地；远程 Agent 仍需获准且可达的控制端点。迁移主控不迁移或重启 runner。

```bash
cd "${NARROWGATE_ROOT}/tools/poolrun"
python3 -m venv .venv
.venv/bin/python -m pip install '.[test]'
.venv/bin/python -m pytest -q -rs
```

集成测试创建临时、仅 loopback 监听的独立 sshd、全新测试主机密钥及客户端密钥和隔离目录。不修改系统 sshd、不开远程登录、不读取生产密钥、不连接真实网盘账户。缺少服务端或权限时明确跳过 SSH 验收，不用本地管道假冒网络。socket/管道故障测试单独标为单元测试。真实 loopback SSH 仍不等于两台物理主机或生产网络验收。

### 服务端固定身份

管理员准备获准的 SSH 接入账户与只读安装的 PoolRun 环境。每台 Agent 使用独立公钥，管理员另用独立密钥。在管理员控制的 authorized_keys 中配置固定命令（替换示例路径、公钥占位符）：

```text
restrict,command="/opt/poolrun/venv/bin/python -I -m poolrun ssh-gateway --socket /approved/ipc/master.sock --principal agent:host-a" ssh-ed25519 AGENT_PUBLIC_KEY
restrict,command="/opt/poolrun/venv/bin/python -I -m poolrun ssh-gateway --socket /approved/ipc/master.sock --principal admin" ssh-ed25519 ADMIN_PUBLIC_KEY
```

这是完整的 authorized_keys 条目，不是客户端选项。专用账户应禁用密码、交互认证及用户自定义 SSH 环境，不授予无限制密钥、shell、PTY、端口转发和 agent forwarding。安装代码、固定命令、authorized_keys 及父目录不得由不可信 gateway 账户改写。`SSH_ORIGINAL_COMMAND` 被忽略。任务 argv、文件名与 payload 均放 JSON，不拼接到 SSH 命令。这是单所有者工具，不是恶意多租户沙箱。

默认 state 根目录仅所有者可访问（0700），快照 0600，socket 目录 0700、socket 文件 0600。Master 的本机 OS 所有者是可信本机管理员。如使用独立的获准 gateway OS 账户，管理员预先创建短路径 IPC 目录及专用批准组（0770），在 control.json 设置 `socket`、`socket_gid`、`gateway_uids`，例如 `{"502":["agent:host-a"],"503":["admin"]}`。state 根目录放在共享 IPC 目录之外并保持 0700。Master 核验内核提供的对端 UID 与允许的 principal，不信任客户端 role；即使能连到 socket，其他 UID 仍被拒绝。同一个 OS UID 下的本机进程天然属于可信边界；固定 SSH 命令限制远端密钥，不隔离所有者 UID 下的恶意本机进程。

Master 先取得 Store 单实例锁，再处理残留 socket；遇到仍在服务的 socket 拒绝启动，不删除普通文件或符号链接。Unix socket 路径必须较短，受操作系统长度限制。

### 启动与连接

修改 [control.json](examples/control.json)、[host.json](examples/host.json)、[client.json](examples/client.json)。在用户批准的 SSH 配置里设置 alias，指向当时的主控，不绑定永久机器。Agent 与 CLI 明确指定 `identity_file`、`known_hosts_file`；可用 `config_file` 指定批准的 OpenSSH 配置，包括已有获准 ProxyJump。通过独立可信渠道核验服务端 host key；不自动信任密钥、不改防火墙、不搭建组网。

```bash
python -m poolrun master --root /approved/private-state --config control.json
python -m poolrun agent --config host-a.json
python -m poolrun --config client.json status
python -m poolrun --config client.json submit --file tasks.jsonl
```

本机管理员可用 `{"socket":"/approved/ipc/master.sock"}` 替代 SSH，仍然经常驻主控业务处理，不直接编辑快照；Agent 必须用 SSH。旧端点/TLS/token 配置给出迁移错误，没有兼容模式。保留已有业务 state、Attempt、runner、缓存和 release 身份；通信配置应在批准的服务交接边界修改，不能热改活动 worker。

每个 Agent 保持一个 SSH 子进程，串行复用小请求。OpenSSH 使用 -T、BatchMode、严格主机密钥、keepalive、连接超时，禁止端口转发。最多接纳 32 个请求，单次交换默认 30 秒超时，每个请求最多四次连接尝试、退避有上限。JSON 行上限 24MiB；代码包另限 16MiB，使用独立短 SSH 会话。stdin/stdout 只传协议，stderr 持续排空并只留有界诊断尾部。runner 显式隔离文件描述符。

断线代表执行结果未知，不是失败或重新 spawn 的授权。重连先核对本地 Attempt；待确认 START、事件和传输请求保留原编号与 payload。CLI 变更命令输出请求编号，并将完整请求及回执保存到 `request_dir`（默认 `~/.local/state/poolrun/requests`，私有，不是发布产物）。CLI 异常退出后，使用 `--request-id ID` 重发同一命令；同编号修改 payload 或端点会被拒绝。Store 保存成功后才返回成功，旧 Attempt 代次仍被隔离。

故障诊断查看 Agent stderr、有界 OpenSSH stderr 尾部、服务端 sshd 日志、gateway stderr、本机 socket/UID 权限。不能绕过主机密钥变化或未授权密钥。SSH 不会自动穿透所有 NAT/防火墙：必须提供已批准的可达 alias/ProxyJump，否则明确报告连接阻塞；个人电脑无需开放入站 SSH。

可用 `examples/demo.py --root NEW_DIR --init-only` 生成模板。实际演示还需要 `--connections ISOLATED_CONFIG_DIR`，其中 admin.json、host-a/b/c.json 的固定命令须绑定演示 control/master.sock，不能指向生产 gateway。完整自动 SSH 演示由集成测试执行。

## 2. 任务、槽位与数据

槽位是并发上限，不保证所有槽位都在回放。管理员可通过 `host-update --resources` 显式设置主机 `memory_admission: false`，关闭该主机的内存估算与实时内存准入检查；这不会伪造内存读数、关闭槽位/CPU/磁盘检查、改变 runner 终止策略或自动重试失败任务。默认仍检查内存，只在所有者批准后覆盖。业务命令内部的准备仍占用槽位；应通过 prepared-cache 接口让调度器识别准备阶段。

主机稳定 ID 与 IP 无关。主控 `hosts` 配置人工给定 `slots/cpus/memory_bytes/disk_bytes`、内存/磁盘余量、平台和能力。任务独立声明资源峰值预算；磁盘预算应包含临时文件、输出、检查点峰值。所有预算是字节，时间是 Unix 秒，平台如 `linux-x86_64`、`darwin-arm64`。

全局有限队列不按主机预分份额。优先级加随时间增长的 aging，项目轮转；就绪数据优先，准备目标结合已有副本、可用预算和链路样本选择。未知带宽按非零保守值估计，不称为精确完成时间。等待较久的大内存任务会阻止小任务不断占用释放的余量。未就绪的输入/代码/环境不占业务槽；每任务仅一个准备目标、每主机最多一个预取，限制预取字节量。准备、结果上传共用本机重型操作串行锁。

注册不可变输入对象后，在任务 `inputs` 引用 SHA256：

```json
{
  "id": "实际64位sha256",
  "sha256": "实际64位sha256",
  "size": 12345,
  "reconstructible": true,
  "sources": [{"route":"netdisk","zone":"noncloud","source_id":"workstation","account":"existing-account","object_ref":"现有工具的对象引用"}]
}
```

```bash
python -m poolrun --config client.json object-register --file object.json
python -m poolrun --config client.json submit --file tasks.jsonl
```

也接受文档的 `task_id/business_spec/resources.memory_gib` 形式；`business_spec.input_refs[].object_id` 必须先解析为已注册的内容哈希。`project_default` 需要先 `rollout` 设置项目默认版本；缺省不猜版本。`depends_on` 未完成或上游被 hold 时不派发。重复导入同业务任务幂等，不撤销后续版本政策；同 ID 的不同业务规范被拒绝。

可选 `prepared` 是内容化配方，包含 `platform/contract/recipe_version/input_ids` 和 `memory_bytes/disk_bytes/cpus` 预算。发布声明 `prepared_contract/prepare_argv`，builder 从 stdin 接收配方、输入缓存目录、临时输出目录，返回 `{"status":"READY","files":[{"file":"name","sha256":"..."}]}`。接收验收后发布到共享 prepared 缓存；配方身份不包含整仓 Git SHA。首版不自动删除 prepared 缓存。

## 3. 传输适配器

`transports`、`result_adapter` 均是**明确 argv 数组**，无 shell 拼接，凭证留在工具自己的本地配置。统一 stdin JSON / stdout JSON；stderr 不上传到控制面。适配器必须遵守请求的限速，内部不得再无界启动传输；需要审计其真实行为。若适配器只在同一主机校验并持久保存结果，将 Agent 的 `result_storage` 设为 `host_disk`，不申请网络传输票据或带宽配额。这不代表结果已回传其他机器，网络上传适配器不能使用此模式；持久结果校验和幂等完成确认仍然保留。

| 情况 | 规则 |
|---|---|
| 云→云 | 配置允许的目标拉取 `direct`；或显式批准的 netdisk 来源 |
| 非云→云、云→非云、非云→非云大文件 | 只能 `netdisk`，不偷偷回退 SSH/scp |
| 代码/小控制文件 | 受限 SSH packages 操作，按 project/release 身份分发，代码包上限 16 MiB |
| 不可直连 | 配置已经批准的单中转来源；不会自动探索多跳或购买中继 |

主控对输入与结果传输都持久化预约：全局、账户、来源、目标、链路并发；默认每目标一个重型传输。断线不凭超时释放未知传输名额。输入用 `.part`，大小+SHA256验证、fsync、原子发布后才 READY；同机多个任务共享同一份文件。重试不消耗业务重算次数。

输入请求/回执：

```json
{"action":"fetch","object":{"id":"sha256","size":123},"source":{"route":"netdisk","object_ref":"..."},"destination":"/absolute/incoming/hash.part","host":"host-a","rate_bytes_per_second":10485760,"transfer_id":"..."}
```

返回 `{"status":"READY"}`，或 `{"status":"NEED_AUTH"}`。HTTP 403/凭证失效应归为 NEED_AUTH，不能伪造 READY。原有网盘工具负责按内容 ID 复用上传物、目标隔离下载回执；scheduler 不实现特定网盘登录协议。工具不支持字节续传时可以重新下载**同一文件**，不能把不同来源的未知半文件拼接。

结果请求包含 `action=save_result`、attempt ID、outputs 目录、带 SHA256/大小的文件清单、限速。返回 `{"durable":true,"uri":"已验证外部位置","files":[...]}`。适配器必须在校验并持久化后才返回；相同 attempt 的重复请求必须幂等。业务计算退出后释放 CPU 槽，磁盘和产物在 RESULT_PENDING 继续保护；上传失败只重传，不重算。

失败最多重试五轮并退避，阻塞原因出现在 `status.live.*.errors`。修复凭证/链路后，停 **Agent 服务而不是 runner**，执行以下命令清除指定传输重试预算，再启动 Agent：

```bash
python -m poolrun agent-reset-transfer --root HOST_ROOT --target TASK_OR_ATTEMPT_ID
```

该操作只清准备/上传错误，不撤销业务代次，不重新计算。现有网盘、SSH、中转和真实带宽尚未跨机测试；接真实工具前先用非生产合成文件验收。

## 4. 代码封存、切换、检查点

```bash
python -m poolrun release create --project calc-demo --root examples/synthetic \
  --include examples/synthetic/include.txt --id r2 \
  --contract examples/release-contract.json --destination packages
python -m poolrun --config client.json release register --file packages/r2/release.json
python -m poolrun --config client.json release prepare --project calc-demo --release r2 --hosts host-a,host-b
python -m poolrun --config client.json rollout --project calc-demo --release r2 --scope pending --mode future-only --missing wait
python -m poolrun --config client.json status --versions
```

源文件仅从显式白名单导出；拒绝私有产物、凭证、`.git/.venv`、软链接和越界路径，导出期间修改则拒绝。Agent 验证后发布只读 `releases/PROJECT/RELEASE`。START 授权把 release、argv、环境映射、输出契约、业务规范一起冻结。运行时不执行工作目录代码、不导航 `current` 链接、不做 `git pull/importlib.reload`，每个 Attempt 有独立输出目录。

未来任务切换是默认模式。只改变没有 Attempt 的 pending 任务，已授权的 STARTING 和运行中任务不变。允许显式任务 `code_policy.missing=fallback` 加 `allowed_fallback` 白名单，否则版本没就绪就等，不自行选旧版本。

环境在**最终路径**建立并由用户批准；不要拷贝/改名 venv 或原地升级包。Agent 拒绝同 env_id 映射改变。stdlib 演示只校验解释器；有业务依赖/原生扩展时需完整文件清单：

```bash
python -m poolrun env-seal --root /final/env --python /final/env/bin/python \
  --include environment-files.txt --output environment.json
```

把环境描述放入 host 的 `environments[env_id]`，把其 `manifest_sha256` 放入 release 的 `environment_manifests[平台]`。批准清单必须完整覆盖实际依赖、native 和配置；程序不会自动判断两个任意环境语义等价。不允许管理员在运行期原地改批准环境；权限只读不是对同一 OS 账户恶意篡改的沙箱。

显式更新运行中任务：

```bash
python -m poolrun --config client.json upgrade --task demo-001 --release r2 --mode checkpoint
# 明确允许中断并从头重算时才用 restart；旧目录保留
python -m poolrun --config client.json upgrade --task demo-002 --release r2 --mode restart
```

checkpoint 模式先让目标版本就绪，再原子写 `control/pause.json`。业务在合法边界发布 checkpoint + receipt 并退出；只写 checkpoint 但未退出不能迁移。目标 adapter 检查源绑定、哈希、契约并返回 DIRECT + `resume_argv`；随后新代次恢复。示例真实保留 cursor/RNG/输出前缀并由 validator 与从头计算结果比较。无 checkpoint、不兼容、UNKNOWN、加载失败均不能自动从零开始。

首版只执行 DIRECT；CONVERT 被明确阻塞，需要为项目另行批准/验证转换器，不能偷偷改 pickle/public_binding。检查点默认留在源主机，跨机检查点及依赖运输尚未开放自动执行。目标恢复失败时可显式恢复原版本的旧 checkpoint：

```bash
python -m poolrun --config client.json resume --task demo-001 --release r1
```

普通回退是再次 future-only rollout 到旧版。`hold --project PROJECT` 保留已接受结果，阻止该项目后续派发/结果接受及依赖任务；它不是自动重算命令。

## 5. 热插拔、停止、OOM

```bash
python -m poolrun --config client.json drain --host host-a
python -m poolrun --config client.json drain --host host-a --resume
python -m poolrun --config client.json host-update --host host-a --resources new-budget.json
python -m poolrun --config client.json pending-placement --project demo --hosts host-a,host-b
python -m poolrun --config client.json stop --task TASK --mode checkpoint
python -m poolrun --config client.json stop --task TASK --mode terminate
python -m poolrun --config client.json retry --task TASK --memory-bytes 8589934592
```

`pending-placement` 仅修改从未启动过 Attempt 的待执行任务的主机范围；保留不可变业务配置及其哈希，不修改既有 Attempt、重试和结果。显式主机列表必须由已登记且不重复的主机组成，原环境、release、数据和资源检查仍然生效。该操作不复制输入，也不增加并发。

减槽/减预算不杀在途任务，只阻止不满足新预算的新启动。添加新主机：在 master 配置添加主机策略并绑定独立受限 SSH 密钥，重启**主控服务**，启动新 Agent；旧业务 runner 不重启。Agent 只出站，但至少要有一个当时可达的主控端点。无端点时无法协调。

Agent 配置 `stop_accepting_at`（停止接新任务）、`checkpoint_at`（runner 本地申请检查点）、`hard_deadline`（明确批准的强停）分别持久化。主控离线也执行 runner 的本地截止政策。未批准强停时 checkpoint 请求没有自动杀进程 fallback。

runner 使用 PID+创建时间+boot identity、进程组、已发现后代和本机 attempt 锁；启动意图已保存而 PID 回执缺失时保守 UNKNOWN。Agent 重启不重发业务 spawn。主机断联也不等于死亡，UNKNOWN 继续保留预算。

Linux有委派权限时可设置host `cgroup_root`，子进程在exec前加入cgroup v2，读取`memory.events`；此路径**本机macOS未实测**。其他情况下记录RSS，但默认不因内存用量终止任务，只有下述所有者策略显式开启时才执行软终止。这不能保证宿主机绝不OOM；`requires_hard_isolation=true`不派给软保护机器。macOS软跟踪不能承诺抓住主动脱离进程组且在发现前消失的全部孙进程；需要强保证的业务必须使用已验收的cgroup主机，或禁止应用daemonize。

137/SIGKILL 不推断为 OOM；证据区分 watchdog 与 cgroup。失败只有显式 retry 才重试，有 `max_attempts` 上限；OOM 重试排除原主机，允许提高下一次预约而不改业务 spec。新主机仍须满足新预算。未知执行只有明确 `retry --uncertain` 且任务声明 `side_effects=false` 才能撤销旧代次；有外部副作用的任务留待人工。网络分区下不承诺物理计算 exactly-once，只保证状态完整时一个代次的结果有权成为权威结果。

## 6. 状态、备份与回收

主控 `control/state.json` 是唯一事实来源。先复制状态→临时文件 fsync→原子 replace→目录 fsync，再更新内存/ACK；任何快照写入失败使该 master 进入 fail-closed，必须重启核对。心跳/进度在内存，不每秒整写磁盘。副作用请求可通过全局 `--request-id` 重试；同 ID 不同内容被拒绝。

```bash
python -m poolrun --config client.json backup --destination ./private-backup
python -m poolrun --config client.json gc --host host-a            # 只列计划
python -m poolrun --config client.json gc --host host-a --apply    # 明确回收
```

备份同时留主控最近五份和客户端指定目录副本；要另外备份配置、packages 和本地凭证引用。原始输入/结果仍按自身持久化策略保护。GC 只删可重建、无引用的 blobs 和无引用 release；运行/STARTING/UNKNOWN/待归档/检查点依赖、pending 输入、当前默认及最近两版回退引用受保护；不删结果、checkpoint、原始唯一文件或外部环境。GC 期间封锁该主机的新 START。prepared 与外部 env 首版保守保留，需人工审计回收，不宣称已经解决所有历史磁盘增长。

计划迁移主控：先停止并证明旧主控不会再启动，复制整个 control 与配置/packages，更新批准的 SSH alias 并核验新服务端 host key，再启动新主控。`flock` 只防同机双开，不提供跨机选举。state 损坏拒绝启动，不自动倒退备份。恢复陈旧备份可能丢代次；遇到 Agent 报告未知于备份的 attempt，整体进入 reconciliation_required，不派新任务。应恢复更晚的真实状态或进行人工证据重建；没有一键“不管旧进程直接恢复备份”。

## 7. 验收与未测范围

见 [验收记录](docs/ACCEPTANCE.zh-CN.md)。保留原本地业务验收，SSH 专项结果与剩余边界单独记录；以下不能用本机通过替代：

- 真实网盘认证/上传去重/限速/断点续传、云→云和中继链路。
- Linux cgroup v2、Linux 断电文件系统保证及跨主机计划迁移。
- 任意生产模型/native 环境的完整依赖封存；NarrowGate 或其他真实项目的检查点兼容。
- 检查点 CONVERT、自动跨机 checkpoint 迁移；首版遇到这些要求会阻塞而不是猜测执行。

首版不带网页、数据库、自动机器创建、跨机主控自动选举或生产平台登录模块。不要在未完成以上现场验收前直接替换现有生产调度器。

### 用户指定的软内存终止策略

资源准入预估与终止策略分开。runner默认不设置软内存终止上限，新项目也一样：`memory_bytes`不会自动成为杀进程阈值。用户可在Agent根目录的`runtime-memory-policy.json`中通过`{"defaults":{"soft_memory_watchdog":true}}`或`{"projects":{"project-id":{"soft_memory_watchdog":true}}}`显式开启；项目设置覆盖默认设置，`false`表示关闭。开启后才以任务的`memory_bytes`作为软RSS上限。资源预留、主机准入、槽数、截止与显式停止请求不变，也不会关闭已配置的cgroup限制或操作系统OOM。runner会动态读取策略，但旧进程不会自动获得新代码：更新安装代码前，应先在旧进程支持的策略中显式关闭受影响项目。纠正后，可用`retry --task ID --override-soft-memory-failure`对确认由软监控导致的失败额外重试一次，保留任务绑定和旧输出；不能覆盖UNKNOWN或内核OOM失败。
