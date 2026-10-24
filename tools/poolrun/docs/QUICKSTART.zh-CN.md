# 自己运行一批任务

[English](QUICKSTART.md) | [简体中文](QUICKSTART.zh-CN.md)

Last materially modified: 2026-10-23

Last materially synchronized: 2026-10-23

既有主控、Agent、环境与传输适配器配好后，用一份批次文件代替逐个注册源码、输入和提交账户。这只是原 PoolRun 的客户端操作流程，不会另起调度器、改变主机预算或改写运行中的任务。在仓库根目录用选定的 Python 3.12+ 执行 `python3 -m pip install ./tools/poolrun` 安装客户端；更新客户端不要求重启既有主控或 Agent。

## 一份计划，一条命令

在原任务 JSONL 和清单旁建立 `batch.json`：

```json
{
  "schema": "poolrun.batch.v1",
  "releases": ["packages/r1/release.json"],
  "objects": ["inputs/object.json"],
  "tasks": "tasks.jsonl"
}
```

引用路径都相对于计划文件，而不是终端当前目录；`code.zip` 与各 release 清单放在一起。`releases` 和 `objects` 可为空列表，表示复用已注册身份。任务 JSONL、源码和输入的格式继续使用[原 PoolRun 合同](../README.zh-CN.md)，不要把 API 密钥或私有凭据写入计划。

```bash
poolrun --config client.json apply --plan batch.json
poolrun --config client.json status --summary
poolrun --config client.json status --summary --project my-project
```

`apply` 先一次读入全部本地提交文件、验证源码包，再注册源码与输入清单并提交整批有限任务。普通排队任务无需另行手动执行 release prepare：就绪检查、准备和完成后接续由原调度器与 Agent 负责。它不预复制整个输入日历、不任意指定主机、不增加并发，也不亲自执行经济回放。仍藏在外部脚本中的业务准备逻辑需要通过原 task/prepared 接口声明，本命令不会猜测或自动改造那些脚本。

返回 `ACKNOWLEDGED` 只表示已注册、已入队，不表示输入已传完或回测已启动。摘要把主控记录的 `RUNNING`、`STARTING`、不确定状态和待保存结果与准备阶段分开，展示主机容量、最后报告的资源、错误和待派原因。单独的 `RUNNING` 不能证明业务进度在增长。检查一个任务可用 `poolrun --config client.json why TASK_ID`，原详细 `status` 保留。

可选的离线检查，不读取客户端配置、不连接任何主机：

```bash
poolrun apply --plan batch.json --check
```

它只检查本地提交文件，不验证远端凭据、容量、业务语义或数据就绪。普通 `apply` 已自动执行这些本地检查，无需每次先运行 `--check`。

## 命令断线：重复命令，不重复回测

连接失败后，直接重复同一个 `apply`。请求编号根据准确的操作内容生成，原主控对已接受操作去重。整批不是一个原子事务：前面的注册可能成功，后面的阶段仍可能失败。重复命令接续这些控制操作，不新建任务身份、不重启已完成账户。改变已有任务的业务规格仍会报冲突，不静默替换冲突的源码或输入。业务和资源检查仍由原模块负责。

如果其它旧控制命令丢了回执，直接重放其已保存请求，不必重拼参数或重新读取可能已改变的文件：

```bash
poolrun --config client.json request-retry --id ORIGINAL_REQUEST_ID
```

命令从客户端既有 `request_dir` 读取原始内容，核对当前端点和请求编号后原样重发。找不到记录或端点不同会报错。这不是 `retry --task`，不会新建计算代次；在不确定结果核清前保留原记录。主控仍是唯一权威，不直接改状态文件。

## 还有哪些需要配置

初次主机和 SSH 设置、环境与源码包创建、业务输入定义、过期传输凭据及研究决策仍需显式配置。客户端不会绕过传输验证、降低磁盘/内存保护、自动重跑失败回测、启用停派主机或启动交易。传输完整性及业务/账户身份检查应是工具内部的正确性检查，不应变成你每个账户都要人工执行的审批链。本轮简化的是提交与恢复，不代表所有历史研究启动脚本都已经成为一份计划即可运行的应用。
