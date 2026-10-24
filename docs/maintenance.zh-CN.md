# 本机维护与文件索引

[English](maintenance.md) | [简体中文](maintenance.zh-CN.md)

Last materially modified: 2026-10-24

Last materially synchronized: 2026-10-24

`narrowgate maintenance` 负责一次有界的本机清理和原有离线文件索引。它不是第二个调度器，不派研究任务、不重启进程、不修改 PoolRun state、不访问远端、不操作 live。真实磁盘路径和回执保存在私有配置目录，不随源码发布。

## 命令

```bash
export NARROWGATE_MAINTENANCE_CONFIG="<private-maintenance-config.json>"
python3 -m narrowgate maintenance scan
python3 -m narrowgate maintenance run --apply
python3 -m narrowgate maintenance index
```

`scan` 和不带 `--apply` 的 `run` 都是预览：读取文件元信息及已有完成依据，不删除源文件、不重建全量索引。`run --apply` 只删除当前已核实可退役的终止对象运行锁；真实删除后合并刷新一次索引。`index` 明确要求重建元信息视图，不删除源文件。可以用 `--config` 替代环境变量。磁盘缺失、身份冲突、无法核对打开文件，都不构成删除许可。

私有 JSON 包含 `roots`（每项为绝对 `path` 和唯一显示 `label`）、`pool_state`（原唯一 PoolRun 的权威 JSON）、`index_directory`（原生成审阅目录），以及可选的绝对路径 `excluded_directories`。扫描根不能重叠。排除目录用于清理；只读索引仍描述配置的项目工作区，但排除自身输出、符号链接及所有 `.git` 条目。根目录只选项目拥有的工作区，不要选整个系统盘或用户主目录。

## 哪些能自动删除？

| 对象 | 必须具备的依据 |
| --- | --- |
| 原 Attempt 目录内的空 `runner.lock` | Task/Attempt 身份一致且已终止，原退出回执确认进程树退出，原 runner 和子进程 PID 已不存在，Attempt 目录无打开文件，能非阻塞独占锁且文件身份未变化 |
| 账户的空 `execution.lock` | 精确匹配 COMPLETE Task/Attempt 结果身份、经济完成及终点 durable cut，锁不属于正式载荷，没有未完成／held／恢复身份及账户目录打开占用，能独占锁且文件身份未变化 |

只处理单硬链接的普通文件。PoolRun 快照变化后，本轮不再删除剩余候选。正式结果、身份、回执和载荷不删除、不重新哈希。这是锁清理，不是宣布整个历史归档都可丢弃。

没有被锁住的锁文件不一定是垃圾：新的写入者或等待者可能已经打开旧 inode。因此主控／Agent、prepare、发布、结果退役及未知用途的持久协调锁保留，依赖锁文件也保留。文件年龄、空文件、缺 PID 元信息或文件名本身都不是删除依据。PID 被复用时保守保留，不杀进程来满足清理条件。新增清理类别必须接入真实 owner 并补齐依赖和并发测试，不能添加按年龄通配删除规则。

## 回执、索引与定时运行

私有 `maintenance-latest.json` 记录当前已删／保留路径及原因，`maintenance-last-change.json` 保留最近一次真实删除。空运行锁通常减少文件数量，不会释放有意义的数据容量；逻辑删除字节单列。持久 `.maintenance.lock` 防止维护并发，本身有意保留。

文件索引继续使用原 JSONL 清单、中文 HTML 分页和离线关键词搜索。只扫描元信息，不读取行情载荷内容，并显示锁文件保留原因。搜索可以按全部配置磁盘筛选。历史清理字段仍只是历史记录，索引分类不授权删除。索引生成失败时，已成功清理仍留回执；下一次实际执行只补待完成索引，不重复已完成删除。没有变化时不重建全量索引。

经操作者授权的每小时巡检可以调用同一个 `run --apply` 命令；无变化保持静默，仅真实清理或故障后通知。巡检不得改变规则、扩大扫描根、清理 live 数据或启动研究。无需额外常驻守护进程、第二份文件目录或自动实验晋升。

较大缓存／断点退役参见[模块归属](architecture.zh-CN.md)、[数据生命周期](../data/README.zh-CN.md)及[归档标准](../research/governance/EXPERIMENT_ARCHIVE.zh-CN.md)，仍由其原 owner 负责载荷。
