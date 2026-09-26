# 路径约定

[English](path_conventions.md) | [简体中文](path_conventions.zh-CN.md)

Last materially modified: 2026-09-26

Last materially synchronized: 2026-09-26

状态：当前公共路径与隐私合同。变量名和结构化标识与英文版本一致；私有位置不随源码分发。

运行时路径必须明确指定。`MM_DATA_ROOT`、历史根目录前缀映射及两个 `NARROWGATE_RETIRED_*` 溯源占位符不再是运行时别名。需要的定位应离线迁移；所选输入缺失不授权借用其他根目录的文件。历史证据继续保留原身份。

## 公共占位符

公共文档使用逻辑变量，不写个人绝对路径、私有主机、账户或下载地址。完整占位符表见[英文同版字段表](path_conventions.md#placeholders)；以下是日常使用的根目录。

| 变量 | 含义 |
| --- | --- |
| `${NARROWGATE_ROOT}` | 当前源码 checkout |
| `${NARROWGATE_MARKETDATA_ROOT}` | 本机行情工作区的父目录 |
| `${NARROWGATE_RAW_DATA_ROOT}` | 原件真实目录 |
| `${NARROWGATE_DATA_ROOT}` | 派生产物真实目录 |
| `${NARROWGATE_CACHE_ROOT}` | 可重新生成的临时缓存 |
| `${NARROWGATE_REPLAY_DAG_CACHE_DIR}` | 复用 DAG 缓存的显式位置 |
| `${NARROWGATE_RESULTS_DIR}` | 运行结果目录 |
| `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` | 不随源码发布的证据位置 |
| `${NARROWGATE_PRIVATE_RESEARCH_ROOT}` | 需要显式提供的研究私有输入，没有公开默认值 |
| `${NARROWGATE_LIVE_CONFIG}` | 当前私有 live 配置选择器，不是回测默认 |
| `${NARROWGATE_PRIVATE_CONFIG_ROOT}` | 只新建、不覆盖的私有版本化配置 |
| `<current-live-epoch>` | 当前选择器解析的运行 epoch；冻结文档中指冻结时的 epoch |

## 本机设置

```bash
export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_RAW_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/raw"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/derived"
export NARROWGATE_CACHE_ROOT="${NARROWGATE_CACHE_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/NarrowGate_BTCUSDC}"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/backtest_results_btcusdc"
export NARROWGATE_PRIVATE_EVIDENCE_ROOT="$NARROWGATE_DATA_ROOT/reports"
```

源码包名不代表物理数据根目录。`data/` 管理离线公共输入，`live/orderbook/` 是运行内盘口。raw 和 derived 必须分成真实目录，禁止软链接和供应商别名。缓存默认跟随根 README 的布局；外置 DAG 缓存覆盖只能在 derived/cache 下，不能指向 raw 或证据目录。本方订单、队列、成交、库存和 campaign 路径不能跨实验共享缓存。

跨项目运行选择器归 ignored `docs/private/`；组件本地证据归 `live/private/`、`data/private/`、`models/private/` 或 `execution/private/`。具体研究单元使用自己的 ignored `private/`。参见[私有组件归属](public_private_documentation_contract.zh-CN.md#证据归属与本地目录)和[研究证据布局](public_private_documentation_contract.zh-CN.md#证据归属与本地目录)。组件目录不得另建一份覆盖全仓当前权威的选择器。

## 当前行情目录

公开入口统一为 `data`，当前合同以[数据指南](../data/README.zh-CN.md)为准。下载地址和账户信息留在私有配置。已完成购买的压缩原件保留在 `raw/<保留批次>`，内部布局与交付状态不变；`.incoming` 只用于未完成传输，不作常驻原件库。目录名不授予删除权。搬迁前必须处理现有原件读取和冻结来源复用依赖，不引入兼容软链接或全局路径前缀回退。规范化事实和后续观察、Bar、特征写入独立 derived。

```text
<market-data-workspace>/
    raw/
        <retained-purchase-batch>/
    derived/
        <source-bound-fact-bundle>/
        <accepted-observation-or-feature-bundle>/
```

这是逻辑存储结构，不要求重命名正在下载的目录。通过 `python -m data inventory`、`normalize`、`validate` 显式传入真实根目录。事实包绑定实际原件、解析合同和 schema；文件夹同名不代表兼容。执行 BTCUSDC 和参考 BTCUSDT 不得互相别名替代。

早期 `raw/binance_futures/<SYMBOL>/YYYY-MM-DD/*.parquet`、混合日容器、辅助 spot/metrics 和版本化盘口缓存均属历史约定。`data_paths.daily_market_path` 保留为历史工具，不是新事实包定位器或回退入口。已有外部市场证据在明确退役前留在其原私有归属，不自动纳入当前来源；旧标签和模型也不能仅因目录名复用而绑定到新行情。

可用性以固定日历清单为准，研究权利保留 previous-use。文件存在、转换成功或覆盖恢复，都不代表模型支持、因果一致、资金费齐备或经济准入。原件、独有运行记录和冻结证据不是可随意删除的缓存。

## 私有运行配置

保留的资金费结算输入位于 `${NARROWGATE_RAW_DATA_ROOT}/accounting/funding/<SYMBOL>/YYYY-MM-DD.parquet`，与购买的盘口／成交及派生产物分开。`daily_market_path(..., "funding")` 解析该会计目录；回测接受工作区根目录或对应交易对的会计目录。迁移保持文件字节不变，更新当前逐日索引并写入私有资金费清单。历史转换回执保留历史定位，不作为当前读取索引。不创建软链接，也不回退到旧目录。

受版本控制的 `live/config.yaml` 只是公开模板。当前私有配置由 ignored 选择器解析，通过 `NARROWGATE_LIVE_CONFIG` 显式传入。current 可独立选择已存在但尚未启动的部署版本，不要求也不证明实际激活；激活证据单独保留。它不是启动权威；live 启动仍需独立部署 envelope 和停止状态的交易所核对。冻结历史记录中的 current 占位符不能重绑定到今天的主机。

`make deploy-preflight` 拒绝 `PUBLIC TEMPLATE`；`make publish-source-dry` 和 `make publish-source` 只传输干净公开 Git checkout，不读取私有配置。源码不分发当前运行或回测权威身份；缺失或不匹配的私有输入必须拒绝，live 配置不能代替回测权威。

## 文档隐私

公共文档不包含个人绝对路径、私有主机或 SSH 目标、账户路径、进程 ID、原始 live PnL/库存/订单快照，以及独有结果文件名。使用占位符和命令参数。未随源码公开的模型与证据不能伪装成读者可访问的资源。

遵循[公共/私有文档与证据合同](public_private_documentation_contract.md)：SHA256 只证明字节身份，不是可访问位置。私有当前选择器是可变定位器，不是不可变证据；独有运行身份继续保留在其私有证据中。
