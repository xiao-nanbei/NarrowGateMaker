# 一日数据流水线

[English](one_day_data_pipeline.md) | [简体中文](one_day_data_pipeline.zh-CN.md)

Last materially modified: 2026-09-12

Last materially synchronized: 2026-09-12

状态：当前公共数据获取与诊断回放教程。

## 范围

体验随包 demo 后，如果想查看自己的一日行情，可以使用本教程：将一个 UTC 日依次用于 Binance 公共成交下载、确定性一秒 bar 标准化、原始成交审计与诊断回放汇总。公开成交下载不需要交易所交易账户或 API key，但需要能够联网访问归档。严格排队或动作研究还需要合适的最优买卖价（BBO）与价位级订单簿（L2）来源、必要的前一自然日 warmup，以及该研究的数据质量检查。

这里的单日是入门/工程样本，不是经济研究的选日规则。新研究事先声明连续日历，保留所有日期，包括缺口与被阻塞的输入；不要只挑质量分数好看的日期拼接研究。数据修复不改变 previous-use 或 Validation/holdout 使用权限。

若只想在零网络环境下体验排队、成交共同分母、campaign、终端记账和证据检查，请先运行[合成回放示例](../../examples/replay_demo/README.zh-CN.md)。

## 安装与路径

使用已激活的 Python 3.11 或更新环境，同时安装数据获取与回放依赖。`.[data]` 仅覆盖下载和标准化；完整 tick replay 即使指定 `--no-ml`，仍会导入科学计算/研究模块：

```bash
python -m pip install -e ".[data,research]"

# 以下帮助命令不会下载数据或启动回放。
python pipeline.py download-raw-trades --help
python -m models.backtest_tick --help

export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_RAW_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/raw"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/derived"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/reports/public_one_day"
export MM_DATA_ROOT="$NARROWGATE_DATA_ROOT"
DAY=YYYY-MM-DD
```

`NARROWGATE_MARKETDATA_ROOT` 是用户管理的父目录，不是源码 checkout，也不应赋值为自身。下列命令按需创建 NarrowGate 子目录树。

`raw/` 与 `derived/` 是两个真实目录，禁止供应商别名和兼容软链接。同一频道跨日期使用相同文件名、字段、列顺序、类型、单位和含义；盘口与成交等不同频道各有自己的 schema。获取 adapter 不构成不同的研究日期分类。[日级数据合同](../../data/README.zh-CN.md#每天只维护一份融合盘口)记录维护中的物理格式与当前转换限制；合同存在不代表整个日历已经迁移完成。

## 下载

下载用于 Bar 和事件级审计的逐笔成交。BTCUSDC perpetual 不再下载原生聚合；可选的副源并集与本地成交流聚合见[数据指南](../../data/README.zh-CN.md#逐笔并集与自聚合成交流)。

```bash
python pipeline.py download-raw-trades \
  --symbols BTCUSDC --start "$DAY" --end "$DAY"
```

预期源文件为 `${NARROWGATE_RAW_DATA_ROOT}/binance_futures/BTCUSDC/${DAY}/trades.parquet`。下载或校验失败时，不能把不完整文件发布为完整文件；该 UTC 日仍须保留在日历清单中，标记 missing/blocked 并等待修复，失败不意味着可以从研究中删掉这一天。

## 标准化与审计

构建一秒 bar，然后审计原始成交顺序、方向覆盖、bar 和订单簿可用性：

```bash
python pipeline.py bars \
  --symbol BTCUSDC --data-type trades --file "$DAY"

python pipeline.py audit-raw \
  --symbols BTCUSDC \
  --data-dir "$NARROWGATE_DATA_ROOT" \
  --out-dir "$NARROWGATE_RESULTS_DIR/raw-audit" \
  --file "$DAY"
```

预期输出为 `${NARROWGATE_DATA_ROOT}/bars_1s/BTCUSDC-1s-${DAY}.parquet`、`${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-raw-trades-audit.csv` 和 `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-eligible-days.csv`。

### 如何阅读数据状态

以下是 [`audit_raw_trades.py`](../../data/audit_raw_trades.py) 实际输出的字段，不是能否使用 NarrowGate 的统一权限开关：

| 字段或结果 | 含义 | 下一步 |
| --- | --- | --- |
| `raw_ok=true` | 一个非空逐笔成交文件通过了审计中的 ID 顺序和方向检查 | 查看详细审计；这不证明完整订单簿或所有来源小时齐备 |
| `has_bars=true` | 存在对应的一秒 bar 文件 | 可继续下文的 bar 定价诊断 |
| `has_bbo=false` / `has_l2=false` | 预期数据目录中缺少对应订单簿文件 | 实验需要订单簿回放时，先获取或标准化这些来源 |
| `eligible=false`，`missing_bbo;missing_l2` | 成交/bar 可用，但审计的订单簿组合输入检查未完成 | 仍可做成交/bar 诊断，不应声称具备 native queue 或真实成交精度 |
| `eligible=true` | 原始成交检查、bar/BBO/L2 文件存在性和已知排除项检查通过 | 严格回放前仍需检查所选来源的覆盖、序列、时钟、warmup 与研究窗口 |

缺少 L2 是数据限制，不是安装损坏。Binance 成交归档不包含历史订单簿变化。保留该审计的实际发现，不要修改 `eligible`；它描述这组输入检查，不是要求从连续日历删掉日期。存在兼容真实观测时可以补齐输入，否则派生盘口只能沿用过去的有效观测，保留真实时间并增加年龄，或保持 unknown；不能用未来观测倒填，也不能冒充新鲜报价/队列进度。采集未知不等于确认零成交。可读性、机制支持和经济使用须分别报告，仅有文件并不证明 live/replay 一致。

## 诊断回放

用公共模板运行单日、关闭 ML、基于逐笔成交时钟的诊断：

```bash
python -m models.backtest_tick \
  --symbol BTCUSDC \
  --config live/config.yaml \
  --day "$DAY" \
  --execution-trade-source trades \
  --replay-event-clock trade \
  --pricing-mode bar \
  --no-ml \
  --summary-json "$NARROWGATE_RESULTS_DIR/replay-${DAY}.json"
```

成功后在 `${NARROWGATE_RESULTS_DIR}` 写入指定标量汇总和回放诊断，应结合数据状态报告阅读。这条路径明确使用成交时钟、bar 定价和替代排队假设；没有成交的时段，其定时器行为与合并事件回放不同。其 PnL 是模型诊断，不是精确订单簿成交估计或 live 盈利预测，也不能证明研究晋级或部署条件已满足。

## 严格订单簿路径

CryptoHFTData 是可选的、需要认证的获取 adapter。经授权的用户在上述环境中加装 `.[provider-cryptohft]`，在 Git 之外提供凭据，并遵循[当前日级盘口转换合同](../../data/README.zh-CN.md#每天只维护一份融合盘口)。只获取数据可使用 `.[data,provider-cryptohft]`；严格回放还需研究依赖。该客户端有意不包含在 `all` 中，安装不授予访问权或数据许可。下载文件是临时导入输入，不是按供应商分类的研究数据集：验证后将日容器发布到 `raw/`，将标准化视图放入 `derived/`。保留真实观测时钟、reset 语义、缺失区间与当前验证状态；只有确认发布有效后才清理冗余导入文件。日容器可读不表示已满足严格队列研究的序列、时钟、warmup 或机制要求，也不能手动改标为冻结的历史输入。
