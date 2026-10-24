# 本地验收记录

[English](ACCEPTANCE.md) | [简体中文](ACCEPTANCE.zh-CN.md)

Last materially modified: 2026-09-30

Last materially synchronized: 2026-09-30

环境：macOS arm64，Python 3.12.14。仅合成数据和临时测试 SSH 密钥。未访问生产主机、worker、网盘账户或业务数据。本次仅本地改动，不是部署。

## 基线与 SSH 回归

指定修改前版本在隔离副本中 **27 项测试通过**（20.90 秒）。原业务验收对其版本仍有效，没有判废。SSH 实现 **46 项测试通过**（31.40 秒，无跳过）。没有重新设计生产状态机、Store、调度器、资源规则、runner、验证器或数据适配器；替换了集成测试通信与认证，并新增通信故障和边界测试。

最终在仅含 psutil、不含 aiohttp 的独立依赖环境中再次运行，46 项全部通过（35.33 秒，无跳过）。源码编译、Ruff 的 F 类检查、git diff 空白检查与仓库公开文档审计通过。未触发远程 CI。

两种集成场景各运行七个任务，由三个独立 Agent 共享领取，分别进行 future-only 配合 checkpoint/restart 升级。使用真实临时 loopback sshd、系统 OpenSSH 子进程、独立 Agent 密钥、固定 gateway 命令和常驻 Unix-socket Master，不预先固定每机任务数量。合成数据/结果适配器仍是本地复制 mock，不是真实网盘传输。

| 内容 | 已执行证据 |
| --- | --- |
| 有限队列、READY、共享输入 | 三 Agent，每模式七任务，每 Agent 一次内容准备，哈希校验 |
| 持久 SSH | 同一 SSH 进程连续承载 poll/start/event |
| SSH 断线 | 只杀控制 SSH，业务 PID 继续，同 Attempt 接回，无第二次启动 |
| 主控/Agent 重启 | 已有 runner 继续，最终完成结果与 release 身份一致 |
| START/结果响应丢失 | 请求落盘后不读取回执即断开，同编号获得原 Attempt/回执 |
| 权限边界 | Agent key 不能管理或冒充别的 host；未授权 key、变化的 host key 被拒绝 |
| 固定命令 | 客户端指定 touch 不能绕过 gateway，不会产生标记文件 |
| 协议 | 半行、连续多行、无效/重复 JSON、超限、stdout 混入文本、2MB stderr 输出 |
| 本机 IPC | Master 仅 Unix socket；所有者权限、对端 UID 拒绝、双写者/活 socket 保护 |
| CLI | SSH status、管理请求持久化重发/冲突、backup 及已有快照相关路径 |
| 代码包 | SSH 按 project/release 登记和读取，内容一致，路径穿越拒绝 |
| 更新 | future-only 不改变活动 release；checkpoint/restart 保留代次及输出归属 |
| 正确性 | Store 持久化失败、状态损坏、UNKNOWN、旧代次、不可变回执、冲突结果拒收 |
| 资源与数据政策 | 原准入、OOM 与 SIGKILL 区分、RESULT_PENDING、drain/GC 保护、非云网盘约束 |
| 离线生命周期 | 保留本地截止/进程树、不兼容 checkpoint、环境冻结测试 |
| 移除依赖 | 控制代码导入扫描及 lockfile 断言无 HTTP 栈 |

socket/管道坏流测试是单元测试，不是双机验收。真实 SSH 测试是在同一机器上使用 loopback 网络，不是两台物理主机。其他环境没有 sshd 时明确跳过；跳过不能证明 SSH 验收通过。

## 完整控制进程资源实测

两次 SSH 集成运行的活动进程快照，包含管理员客户端会话：

| 角色 | 进程数 | RSS 合计 MiB | 自进程启动累计 CPU 秒合计 |
| --- | ---: | ---: | ---: |
| Master | 1 | 27.45–27.58 | 0.056–0.062 |
| Agent | 3 | 85.12–85.16 | 0.181–0.185 |
| OpenSSH 客户端 | 4 | 18.03–18.06 | 0.031–0.036 |
| 临时 sshd 监听及会话子进程 | 9 | 43.20–43.23 | 0.037–0.042 |
| gateway（三 Agent 加管理员） | 4 | 106.81 | 0.146–0.149 |
| runner/业务，单列 | 6 | 130.77–131.78 | 0.269–0.322 |

RSS 合计包含共享驻留页，不等于去重后的物理内存。这是短合成运行的快照，不是 CPU 百分比或稳态容量保证。短暂代码包会话在采样时可能已经退出，尚未测量并发峰值。新增 SSH/gateway 已计入，没有只报 Master/Agent。隔离 sshd 自身也计入；实际部署可能复用已有获准 SSH 服务。

小队列、本机持久会话十次 status 请求平均 2.54–3.09ms；空闲一秒采样快照写入零次，均不是远程网络 SLA。含本机路径的原始回执在 pytest 临时目录，属于私有证据，不随仓库分发。原 12 任务 demo 是历史验收，本轮重跑的是每模式七任务的 SSH 场景，没有把旧 demo 冒称新结果。

## 仍待现场验收

- 两台物理主机、实际 NAT/防火墙/ProxyJump、长时间网络分区和高并发负载尚未验证。
- 真实网盘登录、供应方故障、字节续传、上传去重、限速和云端中继未验证；保留了政策和适配器，没有替换网盘客户端。
- Linux cgroup v2、断电文件系统行为、独立账户/组部署和跨机主控迁移需要对应平台验收。
- 生产依赖/native 清单、真实项目 checkpoint 适配器、checkpoint CONVERT、自动跨机 checkpoint 迁移未验证。
- prepared 缓存及外部环境保守保留，没有新增长期磁盘压力或最优调度保证。
- 所有者 UID 下进程仍受信任；固定密钥与 UID 授权是基本身份区分，不是恶意多租户沙箱。

本次不推送、不生产部署。既有研究调度器保持原状，后续现场验收需另行授权。
