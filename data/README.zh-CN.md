# 离线数据工具

Last materially modified: 2026-09-13
Last materially synchronized: 2026-09-13

[English](README.md) | [简体中文](README.zh-CN.md)

> 发布说明：`${NARROWGATE_*}` 和部署周期名称是逻辑定位符。主机侧数据及证据保存在私有存储中，除非给出仓库相对链接，否则不随公共仓库分发。参见[公共／私有文档约定](../docs/public_private_documentation_contract.md)。

本包包含离线下载、导入、标准化和质量登记入口，只保存代码，不保存行情载荷。

## 外部模型的基础特征必须显式指定

外部市场训练器的10秒路径必须提供`--base-feature-dir`，指向兼容的日级`features_YYYY-MM-DD.parquet`输入，不再假定使用已淘汰的无版本特征目录。旧10秒缓存没有输入绑定，因此这条路径重新生成而不复用旧缓存。fast1s路径不变；指定目录本身不代表特征schema或研究用途兼容。

## 队列校准必须显式指定

回测不再从数据根目录或环境变量自动加载共享的日级队列文件。已淘汰的单日校准不属于当前baseline输入。实验需要校准时应明确传入工件；指定文件不存在会报错，严格校准仍要求其声明的工件。校准生成器必须提供`--output`，不会重新创建共享默认目录。这次清理不代表重新校准或验证了新的队列模型。

## 共享旧 BBO 目录已退役

2026-09-13，用户授权删除 `${NARROWGATE_DATA_ROOT}/bbo`：共256个派生文件，BTCUSDC、BTCUSDT 各128日，日期跨度为2026-01-01至2026-07-20。不要仅为满足旧参考特征默认路径或历史 P3 定位而恢复这个共享目录。新的参考盘口特征应从选定的 BTCUSDT L2 输入重建；参考成交 Bar 从实际逐笔生成。本次不删除购买的归档、规范原始数据及另行登记的标准化 BBO/L2 目录。

现有可选参考 BBO 读取器在退役路径找不到文件。其仅使用 Bar 的回退不等于已经重建了 L2 参考盘口特征，也不证明模型预测不变。本次清理没有将该读取器接入购买归档、重建特征、重训或重跑经济结果；下一轮训练前必须绑定替代特征输入。已撤回数值、有条件保留的历史结果及未受影响的证据，见[历史失真与退役登记](../docs/legacy_l2_evidence_revalidation_20260725.zh-CN.md)；删除本身不意味着过去所有结果都失效。

## 当前数据集：一个连续日历

用户维护的 BTCUSDC 永续数据集覆盖 2025-08-01 至 2026-09-05 UTC，共401日，全部属于当前数据，不另设“旧142日数据集”。当前目录与研究界面按市场、日期、频道组织，原始行情和生成产物分开存放；供应商名称和过去的处理分类不再作为当前数据类别。实际行情文件保存在用户私有存储中，不随公有或私有源码仓库分发。

使用下方统一日路径和当前用户清单，不再使用按供应商区分的旧登记表或历史选日列表。日历保留空窗和真实观测年龄。空窗期间不产生缺乏行情支持的新策略报价，但时间及账户状态继续推进，包括已有订单、库存、费用和资金费。“静默”不等于确认没有成交，也不等于清仓重置。独立最佳买卖价可以改善最优档报价，不能刷新未观测深档或证明队列进度。

历史实验描述其当时的输入和行为，不代表当前数据集上的策略表现；修复行情本身不会重跑或推翻历史研究结果。已支持动作与只用过去数据的滚动验证建议见[当前 E/C 范围](../research/families/f05_fill_quality_quote_ev/docs/risk_selection_scope.zh-CN.md)。数据组织、实际观测支持和历史研究使用情况分别处理。

## Infoway：先选数据，再消耗额度

[`download_infoway.py`](download_infoway.py) 默认仅预览单个加密货币产品请求，不需要密钥、不联网；只有 `--execute` 才发送。支持产品 `info`、单页历史 `candles`，或一次当前 `depth`／`trade` 样本；不会订阅、翻页、重试、填缺口或接入标准回放数据。[K 线合同](https://docs.infoway.io/rest-api/http-endpoints/get-candles) 中最小粒度为一分钟，单产品最多 500 根，历史可查范围取决于套餐；本工具不支持多产品历史批量查询。[盘口](https://docs.infoway.io/rest-api/http-endpoints/get-depth)和[成交](https://docs.infoway.io/rest-api/http-endpoints/get-trade)接口描述的是当前数据，不是历史归档。文档中的成交回包没有可靠的交易所成交 ID。

2026-09-09 核对的用户提供产品表包含现货 `BTCUSDT`、数字币合约 `BTCUSDT.P`，未找到 `BTCUSDC`。这是该表的观察结果，不是经过鉴权的最新完整权限列表。[基础信息合同](https://docs.infoway.io/rest-api/basic-info/get-symbol-basic-info) 明确规定加密货币的 `exchange` 为 null。不能只凭代码推断 Binance、USD-M 永续身份、基础币成交量单位、撮合引擎时间或历史 BTCUSDC 盘口支持。概览示例的盘口数组／买卖侧说明存在冲突，并把 epoch 时间戳标成 UTC+8；因此样本保持原始字段，不标准化盘口、不自动加减八小时。交易所、合约、单位和时间语义须经供应商确认才能接入。

建议选择：不要花额度尝试用这些公开接口修补 BTCUSDC 历史盘口。如果确实需要独立参考样本，先查一次 `BTCUSDT.P` 基础信息，再按需查询 120 根一分钟 K 线，共最多两次请求，用于检查字段和时间对齐；这不是新的 alpha 实验，也不替代已有参考数据。更大下载计划之前，先向供应商咨询准确的 BTCUSDC 永续交易所、历史 L2／逐笔支持、历史可查范围和账户计费／剩余额度，不消耗数据调用。[官方 HTTP 限制](https://docs.infoway.io/getting-started/api-limitation/http)说明套餐请求频率，不代表这个账户实际剩余信用或计费单位。

离线预览：

```bash
.venv/bin/python -m data.download_infoway info --code BTCUSDT.P
.venv/bin/python -m data.download_infoway candles --code BTCUSDT.P \
  --interval 1m --count 120 --end 2026-09-08T00:00:00Z
```

用户选定样本和预算后，通过本机隐藏终端输入或密钥管理器设置 `INFOWAY_API_KEY`，不要发到聊天、命令行参数、Git 或已跟踪配置中。zsh 可用 `read -rs 'INFOWAY_API_KEY?Infoway API key: '; export INFOWAY_API_KEY` 隐藏输入。程序仅把密钥放入 `apiKey` 请求头。供应商样本隔离存放在 `${NARROWGATE_MARKETDATA_ROOT}/infoway/`，先创建该父目录。获准后单次基础信息请求示例：

```bash
.venv/bin/python -m data.download_infoway info --code BTCUSDT.P \
  --execute --max-requests 1 \
  --budget-file "$NARROWGATE_MARKETDATA_ROOT/infoway/request-budget.jsonl" \
  --output-dir "$NARROWGATE_MARKETDATA_ROOT/infoway/<new-sample-directory>"
```

同一密钥的所有调用复用同一个预算文件。`--max-requests` 限制本地累计尝试次数，失败或结果不明也计数；提高上限就是明确追加支出决定。发送前先加锁持久追加记录，共用账本的请求预约时间至少间隔 1.1 秒；账本损坏就停止，不重置。不得删除／更换账本或输出目录来自动重试。此上限不包含其他客户端调用或供应商信用权重。输出目录已存在时会在联网前拒绝；重定向、限流、超时、鉴权或字段错误均不自动重试。成功输出供应商 JSON、请求说明和 `SAVED_NOT_ADMITTED` 回执；仅表示已保存且归属所请求代码，不等于内容验收或经济准入。K 线重复时间戳与缺失值原样保留，不静默合并或填充。实现期间未发送鉴权 Infoway 请求，测试使用合成回包。

## 全日历可读性清单

用户固定数据集窗口时，可在私有来源清单设置 `calendar_window_policy: "owner_fixed"`，并明确 `start_day` 和 `end_day`。清单会保持该连续区间，不再自动扩展到昨天；起点不一致或终点不是完整 UTC 日时拒绝执行。区间内部即使缺文件也仍保留该日期。这是明确的数据集范围调整，不是对历史结果的追溯改善，也不授权抹去历史使用记录。

`.venv/bin/python -m data.quality.calendar_readability --owner-manifest <private-studio-source-manifest> --dataset <registered-source-id> --start-day 2025-08-01 --usage-config <metadata-only-use-map> --output <new-private-report.json>` 逐日盘点截至昨天的全部完整 UTC 日期；每个原始或处理后频道分别重复 `--dataset`。使用项目虚拟环境。当前 UTC 日单列为 partial；输出仅新建并使用私有权限。它读取登记文件元数据、Parquet footer 和已有质量元数据，不读取经济结果或全量原始流。缺失日期仍保留在总分母中。未知的真实观测／副源／沿用覆盖、新鲜度、去重与使用权不补零；历史质量与当前可读性分列。使用范围映射只指定元数据日期字段和用途，不授权读取封存结果。文件可读不代表训练或经济准入；本清单不能替代有限读取器测试或全日历源时钟验证。

### 逐日内容验收

核销历史原生聚合父消息时，使用 `--trade-mapping-only` 代替 `--relationships-only`。该模式不要求 Bar，保留结构化差异计数，并借用同一清单内相邻日作为 ID 上下文。聚合记录按自身 UTC 文件日归属；借入的逐笔仅作上下文，不重复计入逐笔分母。即使只核对一个子窗口，也应在输入清单保留相邻日。报告分别列出未覆盖逐笔、聚合价格／方向／数量差异、时间跨度差异和 ID 跳号；仅有 ID 跳号不能证明采集漏量。原生输入已经退役或未登记时明确标为不可用，100ms 自聚合不会冒充原生父消息。

完整内容验收使用 `.venv/bin/python -m data.quality.calendar_content --manifest <readability.json> --start-day YYYY-MM-DD --end-day YYYY-MM-DD --output-dir <new-private-directory> --workers 2`。它流式解码登记文件的全部行列、计算文件哈希，检查数值、时间、成交 ID、档位顺序及 BBO/L2 一致性，并在可用时读取身份绑定的观测时钟附表。每个日期单独保存，包括缺失或损坏输入。附加 `--relationships-only` 后，当前登记的 100ms 自聚合会从逐笔重新计算，核对分组成员、精确数量／笔数守恒及因果桶时钟；一秒 Bar 的 OHLC、量和笔数也按有序逐笔核对。历史原生数据继续使用独立的父消息 ID 映射与旧 Bar 检查。两种模式均不运行策略、不修改原件。`CONTENT_READABLE` 仅表示实现的内容检查未发现相应问题，不代表采集完整、报价新鲜、特征因果性、真实队列重建或经济准入。时间桶占用率不是新鲜度覆盖；缺失来源时钟仍为未知。特征空值、长观测缺口、来源时钟绑定和历史使用情况分别解释。输出目录仅新建；中断后保留已完成的逐日报告。

大型原始和派生数据位于仓库之外，主机本地根目录如下：

```text
NARROWGATE_MARKETDATA_ROOT=<local-marketdata-root>
NARROWGATE_RAW_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC/raw
NARROWGATE_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC/derived
NARROWGATE_CACHE_ROOT=$HOME/Library/Caches/NarrowGate_BTCUSDC
# 可选：NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_CACHE_ROOT/replay_dag
# 仅可复用 DAG 缓存的容量回退位置：
# NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_DATA_ROOT/cache/replay_dag
```

`data_paths.py` 负责运行时路径解析，新代码不得嵌入用户主目录。冻结证据可能保留迁移前的 `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` 来源标识，通过文件系统边界解析，不改写冻结字节，也不在旧位置建立兼容符号链接。

`raw/` 与 `derived/` 是两个真实目录，不是别名。项目维护的数据路径禁止软链接。执行行情采用不区分供应商的统一每日布局：

```text
NarrowGate_BTCUSDC/
  raw/binance_futures/BTCUSDC/YYYY-MM-DD/
    incremental_book_L2.parquet
    trades.parquet
    funding.parquet
  derived/binance_futures/BTCUSDC/YYYY-MM-DD/
    trade_aggregates_100ms.parquet
  derived/bars_1s/          从逐笔直接生成的 1s Bar
  derived/                 其他特征、回测输入、缓存和报告
```

BTCUSDT 参考行情的 `trades`、`aggTrades` 使用相同命名，不要求执行盘口。物理合同是**同一频道跨全部日期**的字段、Arrow 类型、单位和含义一致；盘口、逐笔、Bar 和特征是不同频道，不强造一套共同字段。价格和数量保留十进制字符串，`timestamp` 为交易所微秒；未知接收时间／原生序列保持 null。参考 1s Bar 从逐笔重建，`trade_count` 始终计逐笔成交，`last_event_ts_ms` 表示最后一笔实际成交的毫秒时间；特征索引与五分钟指标的 `create_time` 索引统一为 UTC 微秒。指标转换保留六个 double 数值字段、行序和原有可见时间约定，遇到亚微秒观测拒绝转换而不截断。`data.unify_daily_fields metrics` 核对并转换已有文件，`features/preprocess_metrics.py` 的新输出使用相同精度。当前登记不再按供应商或处理方式分类。格式转换不制造观测，也不证明精确队列。

### 逐笔并集与自聚合成交流

当前 BTCUSDC 成交源按交易所原生逐笔 ID 合并 Binance Vision 与 CryptoHFT 的真实逐笔。相同时间戳不等于同一成交；共享 ID 的价格、数量与 maker 方向必须精确一致。来源时间有差异时明确记录，由 Vision 决定共享 ID 的规范交易所时间和 UTC 日期；相邻日 ID 用于处理采集小时边界。副源中非正价格／数量保留为来源问题，不计入真实成交量；不可取得的小时是未知，不是成功下载的空数据。

`pipeline.py download-secondary-trades --help` 提供小时逐笔下载入口；`pipeline.py aggregate-trades --help` 根据来源清单生成并验证逐笔并集和日级自聚合，暂不替换规范原始文件。下载尚在继续时，必须等待所需采集小时上下文全部有明确终态，不能把已有文件列表解释为源覆盖完整。

自聚合按 UTC 对齐的 100ms `[左边界,右边界)` 窗口、精确价格与 maker 方向分组，求和数量并计数逐笔，使用合成 `group_id`、组内事件时间范围及窗口右边界的 `feature_ready_ts_ms`。首末逐笔 ID 不表示连续的成员 ID 区间。未知采集区间不凭空生成空桶或零成交。该产物是成交流特征，不是交易所原生 `aggTrade` 消息、执行 tick、队列事件或实测网络到达。1s OHLC、量和笔数直接从有序逐笔生成，避免按价格分组改变开收盘顺序。Bar 元数据绑定逐笔源哈希及笔数单位；不会仅因旧文件名存在就复用。

完成并集、守恒／往返验证及当前读取器／目录切换后，BTCUSDC 原生 `aggTrades` 已退役，以后不再下载。下载入口拒绝 BTCUSDC perpetual 原生聚合，包括显式指定其他输出目录；改为下载逐笔来源并在本地生成聚合。这不是原生消息的无损重建；历史 `f..l` 父消息合同保持输入不可用，不能偷偷换成自聚合，也不能据此自动重新下载。BTCUSDT 与 spot 参考聚合不在删除范围。已有特征缓存及冻结模型输入保留旧身份，不能自动改称并集产物或 live 对齐。

日常校验报表按市场／频道／日历范围只保留最新完整版本，包括当前仍需引用的逐日最新记录。核对替代版本并更新当前链接后，删除过期、重复及已经淘汰的失败／烟测报告。保留当前数据／来源身份、读取时钟附表、历史使用登记和唯一有效质量依据；冻结研究与实盘／账户记录不属于可随意清理的日常校验。直接更新现有最新摘要，不为每轮清理再新增报告。

`.venv/bin/python pipeline.py unify-raw --manifest <current-readability.json> --output-root <project-storage-root> --workers 2` 是既有 `data.daily_raw` 来源格式导入器，流式转换、原子发布并逐批核对逻辑往返一致性；历史清单可能仍包含原生聚合，已有兼容 Parquet 保持原字节。明确指定 `--retire-source` 后才在验证完成后删除冗余原件；仅选择部分日期不会删除仍供其他日期使用的资金费区间文件。Binance futures 成交下载默认直接发布每日 Parquet，随后删除临时 CSV；盘口下载走下述融合路径，不再整日用一个供应商覆盖另一个。显式其他输出目录仅用于下载或诊断，不是研究默认入口。

### 每天只维护一份融合盘口

当前物理合同为 `narrowgate.daily_book.v1`，由 `data/daily_raw.py` 的 `DAILY_BOOK_SCHEMA` 定义：每天完全相同的24个字段。保留精确十进制价格／数量、真实交易所时间、可用的原生 ID，以及匿名事件流／重置语义；不保留供应商或处理分类列。`timestamp` 是因果输出／事件排序时间，`observed_timestamp_us` 是实际交易所观测时间，`original_timestamp_us` 保留原事件时间。接收时间及其他原生时钟不替代交易所时间；新输出不能刷新旧观测。旧布局只在导入边界读取，随后投影到这一份统一合同。

`data.unify_book_calendar` 逐日转换和验证，把每日原始文件、生成的 BBO／clock 及全部相关当前索引一起准备后发布。持久化事务允许中断后续接，不把旧模型校验改名为新校验；受影响特征在重新生成前明确保持 `REFRESH_REQUIRED`。代码存在或目录标签统一不代表全部物理迁移完成：以当前任务逐日 `PUBLISHED_VERIFIED` 和最终全日历字段检查为准。

独立 BBO 中有用的真实观测作为 `top_only` 买卖侧成对记录纳入同一每日盘口。它只能更新 BBO 的观测钟，不能刷新深档时钟或队列进度。生成的时钟附表分别记录 BBO 真实观测时间、年龄、类型和可用性；无效／未知观测保留不可用标识，不通过删行暴露之前貌似有效的报价。缺失的原生事实保持 null，不伪造零。确认有用观测已保留、当前读取器和索引切换并验证后，才删除独立 BBO 原件。切换事件流不伪造原始删除／加回；相同时间戳本身不构成去重身份。

派生 100ms BBO/L2/clock 维持独立事件流盘口，选择真实交易所观测最新的有效状态；同钟按持久化的数字流优先级选择，兼容边界一次性转换原先按名字指定的优先规则。新的匿名流编号不需要在选择循环中比较供应商名称。切换时直接选择视图，不每次物化整个深度差集。浅档事件流里不存在的价层属于未知，不是观测到数量为零。replay 读取器将视图切换和快照重置视为队列谱系重置，不当成撤单；只有同一选中流的真实增量才可推进模拟撤单量。这不证明交易所全局原生序列或精确排队位置。无可用新状态时只保留最后有效视图并增加年龄。该模式不全量扫描同钟全盘口冲突，明确报告未检查，不报成零。

文件 footer 分开绑定日初和日末盘口连续状态；独立读取只能用已声明的日初状态，禁止用日末状态倒填前面的时间。跨日继承仅是盘口／时钟，不是库存、订单或账户 FIFO。内部快照／重置、只刷新观测的事件语义用于保护重建和队列行为，不是新旧日期分类；替代事件接口保持这些行为后，才能移除当前依赖的字段。用户已关闭无法找回原件的恢复事项，它不再是研究的常驻前置任务，也不能因此从当前日历删除日期。

CryptoHFT 下载默认把首次摄取的新日转换成统一24字段。采集边界流程使用次日采集来源中实际属于前日的交易所事件；上下文未齐时保持 `boundary_pending`，不报完成。当前新格式的更新适配尚未完成：重复处理已统一日期、下一次调用收尾其 pending 边界、或追加另一个来源，会在替换前明确拒绝；旧的采集小时／回执复用检查不能证明新 footer 的幂等。后继新日只能继承显式兼容的匿名流槽，不能把单流连续状态偷偷当作三流状态。历史 Tardis 补源入口也仍需切换到统一输出选项。完成这些路径并用真实统一字段的测试样本验收前，不能承诺尾日自动收尾或清理其必需暂存原件。这些下载更新限制不阻止 `data.unify_book_calendar` 转换当前已有日历。`--legacy-provider-normalize` 仍是显式诊断，不是日常路径。下载本身不刷新依赖盘口／特征：须发布核验后的标准化输出并重建受影响产物，再标为当前版本。

用户授权的历史迁移先核对输出往返一致性、来源包含关系、观测钟语义和当前读取器／目录切换，然后删除已被替代的供应商盘口。保留紧凑来源身份和当前诊断，不再保留重复来源目录或软链接。该删除授权不包含唯一未转换记录、其他频道、冻结研究或账户日志。每日统一容器与 BBO/L2／特征分别放在物理 `raw/`、`derived/` 目录。

运行时 WebSocket 状态属于 `live/`；Binance 执行市场的快照加增量盘口在 `live/orderbook/binance_usdm.py` 中实现。NarrowGate 自有活动订单的状态属于 `execution/`。

职责划分：

```text
data/                       离线 ETL 与数据质量工具
live/orderbook/             实盘公共盘口重建
execution/                  活动订单及队列路径状态
$NARROWGATE_RAW_DATA_ROOT/  统一每日真实观测
$NARROWGATE_DATA_ROOT/       标准化及生成的数据集
$NARROWGATE_CACHE_ROOT/      默认可丢弃缓存层
$NARROWGATE_DATA_ROOT/cache/ 显式可移动缓存层
```

可复用的源数据／特征物化由 `models/replay_cache_dag.py` 声明。原生 CryptoHFT 逻辑消息按源小时缓存，供目标日、D−1 预热和实验臂复用解析结果。依赖策略的订单、队列、成交、库存和 campaign 路径不得跨臂共用缓存。

不要在本包添加实盘网络状态机或策略逻辑。

## 公开合成回放演示

可分发的演示是外部行情存储规则的一个小型例外。人工编写、非经济证据的 JSONL 行情位于 [`examples/replay_demo/`](../examples/replay_demo/)，不能当作供应商原始数据或已准入研究证据。

无需网络、私有证据或外部报单即可运行完整演示：

```bash
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

合同通过 SHA256 绑定公开行情，回执绑定行情、合同、回放代码、会计约定、分母、campaign 终点价值、检查状态和输出。通过状态为 `passed_demo_mechanics_only`，不具备经济、晋级、实盘动作或真实市场保真声明资格。

供应商格式只存在于导入边界。客户专属发货页的数据先下载到独立暂存目录，不修改当前研究输入：

```bash
.venv/bin/python pipeline.py download-tardis \
  --delivery-config data/private/<delivery>/config.json --archive-only
```

私有 JSON（权限 `0600`，不提交）提供 `base_url`、`output_root`、含首尾的 UTC 日期 `start`／`end`，以及由 `VENUE,DATASET,SYMBOL` 字符串组成的 `contracts` 列表。L2 和逐笔成交分别使用 `incremental_book_L2`、`trades`。请求始终使用稳定的 `.csv.xz` 路径，根据实际响应文件名和压缩内容识别 XZ、Zstandard `.zst` 或 `.zstd`，不保存临时重定向地址。HTTP 202 保持等待状态；临时错误有限退避。默认四并发，保留磁盘余量，通过独占运行锁、断点对象身份检查及完整压缩帧校验后才原子完成。仅保留压缩归档，不展开全部 CSV；下载不等于数据准入。

`--archive-only` 即使续传也不融合、发布或删除规范输入。不加此参数时，旧 Tardis 下载器仍按有界历史获取方式运行：BTCUSDC perpetual L2 自动进入融合流程，其他频道保持下载归档并显式导入。旧路径在原子清单中记录远端大小、ETag、SHA256 和 zstd 完整性。以下是历史获取示例，不是第二套研究数据根：

```bash
.venv/bin/python pipeline.py download-tardis \
  --start 2026-01-01 --end 2026-07-30 \
  --contract binance-futures,book_ticker,BTCUSDC \
  --contract binance-futures,incremental_book_L2,BTCUSDC \
  --allow-missing
```

不要把 Tardis 加入周期日更新器。新日数据继续使用现有 Binance Vision、CryptoHFTData、Bitget、Bybit 和 OKX 下载／导入命令及其来源合同。

下载成功不等于该 UTC 日已准入研究。Tardis L2 具有独立供应商身份，必须通过自身初始化、因果时间、跨通道 BBO、覆盖和缺口检查，才可用于修复历史 CryptoHFTData 缺陷日。

使用冻结候选日列表和来源隔离的外部市场根目录，审计已完成的边界感知清单：

```bash
.venv/bin/python pipeline.py audit-tardis \
  --manifest "$NARROWGATE_MARKETDATA_ROOT/tardis/manifests/binance_futures_btcusdc_20260101_20260730_download.json" \
  --candidate-days "$NARROWGATE_DATA_ROOT/reports/marketdata_repair_20260727/cryptohft_bad_days_after_repair_20260727.csv" \
  --output-csv "$NARROWGATE_DATA_ROOT/reports/tardis_bad_day_repair_20260730/raw_admission.csv" \
  --output-json "$NARROWGATE_DATA_ROOT/reports/tardis_bad_day_repair_20260730/summary.json"
```

默认五秒边界范围仅验证事件驱动文件的跨日衔接，不是标准化 100ms 数据的新鲜度或最长连续缺口门槛。

所选日期的主 L2 文件可读且匹配清单身份时，就可以生成来源隔离的 Tardis 产物。整批未下载完成或辅助 bookTicker 缺失不再阻止 L2 重建；缺失或不可读的 bookTicker 保持 `unavailable`，不能通过跨通道准入：

```bash
.venv/bin/python pipeline.py normalize-tardis \
  --manifest "$NARROWGATE_MARKETDATA_ROOT/tardis/manifests/<manifest>.json" \
  --day 2025-08-01 \
  --output-root "$NARROWGATE_DATA_ROOT/normalized_tardis_l2_exchange_100ms_v1" \
  --workers 3
```

标准化默认使用 Tardis 的交易所 `timestamp`，`local_timestamp` 保留作供应商接收时间的来源记录。Replay 单独叠加模拟可见性与执行延迟，不能在已包含供应商延迟的事件时间上再叠加一次。`--timestamp-source provider` 仅作为显式诊断选项，使用独立输出根；旧供应商时钟合同见 `docs/tardis_normalized_l2_contract_20260731.md`。旧单来源 CryptoHFT 标准化器优先使用 transaction time，缺失时使用 event time；当前双源融合则按上述实际交易所 E 比较，单独保留 T。两者都不静默替换成 receive time。交易所时间缺失时应从真实来源补齐，不能只改字段标签。统一时钟或完整输出行都不等于精确队列位置或实盘传输一致。

### 局部缺口与连续 UTC 日

默认 `--gap-policy missing` 保留稀疏、有源更新的 BBO/L2 行。时钟附表的 `last_observation_timestamp_us` 使用所选供应商／交易所时钟，`observation_age_us = output_timestamp_ms * 1000 - last_observation_timestamp_us`。右边界时间是输出时间，不是一次新源观测。`source_observed` 表示桶内有源消息且盘口有效，并不能证明全部上游更新均已到达。

`--gap-policy carry_forward` 仅用于单独目录中的派生视图。沿用有效的上次盘口时，标记 `observation_kind=carried_forward`、`update_coverage=unknown`；源时间不变，年龄随输出时间增长，直到新观测到达。不用未来值回填，不生成虚构的成交／成交量，也不推进队列。缺失或盘口无效的区间仍记录在 `unobserved_intervals` 中；没有消息的桶无法区分采集丢失与真实静默。沿用行不会提高源新鲜度覆盖。数据集身份增加 `_carried_view_v2` 后缀，回放准入标志保持 false；现有严格准入审计器不是这种视图的可读性检查器。

对相邻 UTC 日添加 `--continuous --workers 1`，即可保留重建盘口和真实观测时钟。symbol／clock 不匹配、不相邻日期和所选时钟倒退会被拒绝。某日失败即停止该批次，后续日明确列入 `not_run_days`。这里仅继承数据重建状态，不继承库存、订单或策略状态。Python API 还接受同一天内对齐 cadence 的 `output_start_us`／`output_end_us`，读取必要的因果前缀以初始化盘口，仅输出该半开窗口。未处理完整日的 continuation 不能初始化下一 UTC 日。

不同 clock／gap／continuation 模式不能在同一个输出根目录互相覆盖。缺少观测 schema 的旧附表会重建，连续模式不会从缓存日输出恢复状态。每日日产物和审计先在临时区生成，发布输出和质量标记之后才提交 continuation。普通发布失败会恢复原有输出；进程在多次文件重命名之间崩溃时，仍需验证质量标记绑定的输出哈希，不得读取未验证的混合版本。原始归档不会被修改。

### 全日历观测验收与特征更新

`data.quality.calendar_content --book-continuity-only --book-root <selected-book-root> --workers 1` 验收整个预定日历：完整读取现有 BBO/L2、检查共同时间轴与最优价、逐列比较规范相邻原始输入中的源时钟，并跨午夜执行因果 100ms as-of 读取。明确来自不同保留源的重采样盘口，必须绑定原 quality／BBO／L2／clock 哈希；未匹配当前规范源的时刻照实报告，不静默置零，也不当作已重新重建的证明。另需常规 `--manifest`、`--start-day`、`--end-day`、`--output-dir`；`--resume` 只复用经过验证的成功前缀。仅验证器升级也要重核前缀文件哈希、重新计算因果网格，并保留原执行身份；中断残留的临时发布文件会安全拒绝恢复，可能需要检查。这不是经济回放，也不声称重新重建全部原始价层。缺失与陈旧区间仍保留在总分母。

重采样盘口必须绑定原有真实观测附表。若生产器用最后实际应用的交易所消息给输出打时间戳，只有保留生产器／文件身份并核对原始时钟后才能沿用这个时间，不能因为它看似某个网格就认定为真实观测。生成的 quality／clock／BBO／L2 绑定同时供 Python 和 native replay 读取。新鲜度按观测钟计算；再次沿用输出不刷新年龄，也不重新抽一次接收延迟。旧的未绑定输入明确为 `UNKNOWN`，不冒充本次已验收的新鲜盘口。

午夜前的交易所事件可能存放在次日采集分区。验收可通过已绑定身份的相邻文件核对这些事件，但只接受目标 UTC 日结束之前的时钟，不导入未来盘口状态。完整加载行数（含保留的预热行）与目标 UTC 日内行数分别统计。

逐笔或 1s Bar 变化后，先用相邻 D-1 上下文重建 taker-tempo，再用 `features.feature_engineer --features-only --warmup-days 7` 处理同一完整日历。绑定实际逐笔 Bar 来源和参考 Bar 根目录，不使用已退役的原生聚合替代。features-only 不拟合模型、不生成标签。预热不足或未定义比率保持缺失，不从未来倒填。逐一检查实际模型包的列 schema 与推理结果，并与市场采集完整性、经济表现、研究使用权限分开。

既有密集秒特征的零笔数／零成交量约定，只表示保留 tape 中没有记录，不是独立确认交易所无成交；没有采集缺口标记时，它不能区分真实静默与消息缺失。列结构和有限推理检查不证明与旧训练分布等价，也不会重新训练冻结模型权重。

只有供应商、非 CryptoHFT 和原生质量台账完整后，才冻结不可变、来源感知的研究日视图：

```bash
.venv/bin/python pipeline.py freeze-research-days \
  --start 2025-08-01 --end 2026-07-25 \
  --provider-quality-csv "$NARROWGATE_DATA_ROOT/reports/<provider-quality>.csv" \
  --non-cryptohft-csv "$NARROWGATE_DATA_ROOT/reports/<source-audit>.csv" \
  --native-quality-csv "$NARROWGATE_DATA_ROOT/<native-root>/daily_quality.csv" \
  --output-root "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1"
```

构建器要求同来源的前一自然日预热，创建不可变硬链接视图，不修改 canonical `normalized_l2_100ms_v2` 登记表。供应商标准化日期只作为敏感性证据，不能升级为原生序列、精确队列、动作或实盘权限。

在内部缓存卷预热可复用 tick 窗口，清单保存在 `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`：

```bash
.venv/bin/python pipeline.py prewarm-tick-cache \
  --days-file "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1/provider_replay_days.csv" \
  --book-root "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1" \
  --cache-dir "$NARROWGATE_CACHE_ROOT/window_cache" \
  --manifest-json "$NARROWGATE_DATA_ROOT/reports/cache_prewarm_provider_v13/manifest.json" \
  --workers 2 \
  --reserve-gib 60
```

只有模型和特征身份已冻结时才使用 `--with-ml`、`--feature-dir`、`--model-dir`，命令会把相应哈希绑定到缓存键。行情、特征、模型、清单和报告保留在外部存储；仅可丢弃缓存载荷属于 `NARROWGATE_CACHE_ROOT`。

来源隔离的 C++ 核心计算入口：

```bash
.venv/bin/python pipeline.py source-aware-cpp-baseline --help
```

一次调用只允许一个来源权限。供应商标准化结果仍是敏感性证据。此入口还排除仅 Python 支持的 BUY q90 和独立 BUY fill-selection 行为，因此不能声称完整 live stack 或部署一致性。
