# Azure Batch 私有 Replay 运维手册

<p><a href="azure_batch_replay.md">English</a> | <a href="azure_batch_replay.zh-CN.md">简体中文</a></p>

Last materially synchronized: 2026-09-03

本文定义离线 NarrowGate replay 的可复用私有 Azure Batch executor，不包含 subscription、
account、region、resource ID、storage locator、private path、策略参数、dataset identity
或结果。

Azure 只是计算资源，不是新的 source of truth。Canonical 行情数据、冻结日期集合、queue
与 latency contract、seed、research authority 和 final admission 仍位于本地私有边界。

## 公共/私有边界

公共仓库可以描述运维合同与通用 Batch 命令。私有 submission layer 负责：

- Azure tenant/subscription/resource identity 与 credential；
- container registry 与 storage location；
- input data、manifest、model/policy material 与 runtime image identity；
- 注册的 replay 日期与 task command；
- output、failure log、receipt 与 economic aggregation。

不得提交生成的 Azure 配置或输出。可用时使用 Microsoft Entra/RBAC 与 managed identity，
不要使用长期 account key。

## 持久环境、临时计算节点

只创建一个有边界的 resource group，其中包含私有 executor 必需的资源：

- 一个 storage account，分别存放 input、output、manifest 与 failure log；
- 一个 container registry 或等价 immutable runtime source；
- 一个 Batch account；
- 一个只具有最小 Blob 与 registry 权限的 managed identity；
- 一个 persistent pool definition。

Pool definition 可以在研究期间保留，但常态 idle 配置是 dedicated 与 low-priority node
都为零。不能为了保持 executor 可用而保留 control VM、database、NAT gateway、大型
logging workspace 或 persistent per-node disk。

Linux image 与 VM architecture 必须兼容冻结 Python/native runtime。除非通过内存和
copy-on-write 实测证明其他配置安全，否则每个 node 使用一个 task slot。Pool start task
只有在受限 host preparation 时可以 elevated；replay task 本身以 non-admin user 运行。

一次性 pool bootstrap 必须：

1. materialize 精确 runtime 与完整 manifest closure；
2. 创建冻结合同要求的真实 canonical directory；
3. immutable contract 需要特定 absolute directory 时，只使用 bind mount，不使用
   symlink；
4. extraction 后规范 ownership 与 mode；
5. 拒绝 group/world-writable private material；
6. 运行 closure/import/native smoke probe；
7. 最后写 pool-ready marker。

Batch 自己的 node-shared 与 task-working directory 必须和 compatibility mount 分离。
Closure 缺失属于 bootstrap failure，不能因此允许 formal task 联网下载依赖。

## 受控 native wheel builder

一个独立、有边界的 build task 可以复用至少 16 GiB RAM 的 Linux x86_64 node，产出 EC2
native artifact。它是 build task，不是 replay day，也不能与 replay task 共用一个并发
slot。构建必须进入 Amazon Linux 2023 或 manylinux_2_34-compatible 的 glibc 2.34
container/rootfs；通用 Ubuntu 24.04/glibc 2.39 产物不能部署到 EC2。Materialize 精确
source commit、CPython 3.12 build environment 与 GNU C++ 11.5.0 toolchain。关闭 task
网络前，先从受控本地 wheelhouse 将 `cpp/pyproject.toml` 声明的 requirements 及其传递
依赖安装进该专用 build environment，然后运行：

```bash
make native-live-wheel
```

该入口默认把 `CMAKE_BUILD_PARALLEL_LEVEL` 设为 `1`，检查 available memory，选择
live-only surface，并固定 `NARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2`。它在
`PIP_NO_INDEX=1` 下使用 `--no-build-isolation --check-build-dependencies`，因此缺少 build
dependency 时会在本地失败，不会访问 package index。在 Amazon Linux 2023 qualification
冻结它们之前，build-tool 版本仍只是实测 builder input。释放 node 前，将生成的
`dist/native/live/<full-git-commit>/*.whl` 作为 immutable build output 上传。不能改用
`-march=native`、portable
wheel 或不同 compiler 产出的 wheel。目标 EC2 release 仍必须通过 native build receipt
与 Python/C++ parity smoke 验证 installed wheel；Azure 不能替代 target-host performance
qualification。

## 从零扩容一个研究批次

通过 operator 批准的 Azure CLI context 登录目标 Batch account，然后扩容 persistent
pool：

```bash
az batch account login \
  --resource-group <resource-group> \
  --name <batch-account>

az batch pool resize \
  --pool-id <pool-id> \
  --target-dedicated-nodes <bounded-node-count> \
  --target-low-priority-nodes 0
```

等待 pool 报告请求数量的 usable node，并确认每个 node 都存在 start-task ready marker。
Resize 是 asynchronous；request accepted 不代表 node ready。

大规模提交前，先在一个 representative UTC day 上比较云端与本地的 input、action
count、terminal accounting、wall time、peak memory 和 output contract。只有兼容性验证
通过后才 fan-out；它不授予任何策略权限。

## 每个 task 只运行一个 UTC day

每个 formal task 精确对应一个注册 UTC day：

- task ID 是一个 job 内确定的日期 identity；
- 一个 task 使用一个 task slot；
- task 内不再次启动按日 multiprocessing；
- start/end、warmup、initial state、queue/latency contract 与 seed 均冻结；
- input read-only；
- output 使用唯一 attempt-specific directory；
- task 有明确 wall-clock 与 retry 上限。

Job 只创建一次，每个 day task 也只创建一次。提交前列出已有 task ID，拒绝 duplicate。
Retry 使用 Batch retry mechanism，或在同一个 logical day 下使用新的 opaque attempt
namespace；不能创建第二个并发 day task。

通用命令形态：

```bash
az batch job create \
  --id <job-id> \
  --pool-id <pool-id>

az batch task create \
  --job-id <job-id> \
  --task-id <utc-day-task-id> \
  --max-task-retry-count <bounded-retry-count> \
  --max-wall-clock-time <bounded-duration> \
  --command-line "<private-single-day-runner-command>"
```

Private runner 必须在进入 event loop 前验证 runtime root、input-manifest root、registered
plan、frozen date、warmup 与所有引用 closure。公共代码不解析私有 command 或 artifact。

## Output publication 与 `_SUCCESS`

进程完成不等于 replay day 已准入。按以下顺序发布 output：

1. 向 attempt-specific temporary namespace 写结果；
2. 验证 schema、day boundary、terminal accounting 与 required diagnostic；
3. 写覆盖已准入 result file 的 output manifest；
4. 原子 publish/promote attempt output；
5. **最后**写 `_SUCCESS`。

只有 success marker 与 expected logical day/input identity 的 output manifest 同时匹配时，
marker 才有效。Exit code 为零但没有 marker 属于 incomplete。从另一 attempt 复制的 marker
无效。

Resume 只能跳过 marker 与 manifest 都匹配的日期。注册 multi-day run 尚未完成时，monitor
只读取 task state、failure、elapsed time、node health 与 success-marker count，不能读取或
报告 partial economics。

## 避免重复工作的故障处理

发生 infrastructure 或 closure failure 时：

1. disable job，阻止新 task 启动；
2. 明确选择 active task 等待、终止或 requeue；
3. 只检查 task state、stderr/stdout、node state 与 missing closure；
4. 经济合理时，在短且有界的 repair window 内保留已初始化 node；
5. 仅当 base runtime 与 input bundle 未改变时，直接补充小型 immutable missing
   resource；
6. shared base materialization 改变时，更新 pool start task 并 reimage node；
7. 证明不存在 duplicate logical day task 后才能 resume。

例如，disable job 但允许 active work 完成，可以使用 Azure Batch 支持的 operator-selected
task policy：

```bash
az batch job disable \
  --job-id <job-id> \
  --disable-tasks wait
```

不得因为缺少一个小 receipt 就重建或上传大型 base bundle，也不能原位修改已经准入的
input。

## 一批结束后缩容到零

Queue 排空且每个 admitted day 都存在匹配 `_SUCCESS` 后：

1. 下载 output、manifest 与 failure log；
2. 运行同一个本地 finalizer 与 aggregation boundary；
3. 按 retention policy disable 或删除 completed job；
4. 两种 node class 都 resize 为零；
5. poll 直到 current 与 target node count 都为零；
6. 验证没有意外保留 compute、disk、public IP、load balancer 或 logging resource。

```bash
az batch pool resize \
  --pool-id <pool-id> \
  --target-dedicated-nodes 0 \
  --target-low-priority-nodes 0

az batch pool show --pool-id <pool-id>
```

只有批准期间内即将运行下一注册批次时，才保留 pool definition、Batch account、registry
与 storage。Zero nodes 避免 VM compute charge，但不表示 storage、registry、network 或
retained job data 免费。

## 最终清理

Budget 或 subscription window 结束前：

1. 停止新 submission；
2. 等待 active task 完成或显式终止；
3. 下载并在本地验证全部 retained output；
4. pool resize 为零并确认完成；
5. 删除 completed job 与 pool；
6. 删除 Batch account、registry、storage account，最后删除 resource group；
7. 验证 subscription 中没有 executor 遗留的 VM、managed disk、public IP、load
   balancer、network interface 或 logging workspace。

Resource-group deletion 必须是最后一步。Output 下载和本地验证完成前不得执行清理命令。

## 相关文档

- [运维目录](README.zh-CN.md)
- [AWS EC2 live 运维手册](aws_ec2_live.zh-CN.md)
- [公开/私有文档合同](../public_private_documentation_contract.zh-CN.md)
- [Azure Batch CLI quickstart](https://learn.microsoft.com/en-us/azure/batch/quick-create-cli)
- [Azure Batch pool commands](https://learn.microsoft.com/en-us/cli/azure/batch/pool?view=azure-cli-latest)
- [Azure Batch job commands](https://learn.microsoft.com/en-us/cli/azure/batch/job?view=azure-cli-latest)
- [Azure Batch task commands](https://learn.microsoft.com/en-us/cli/azure/batch/task?view=azure-cli-latest)
