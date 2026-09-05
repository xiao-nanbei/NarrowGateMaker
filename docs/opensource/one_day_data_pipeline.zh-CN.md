# 一日数据流水线

[English](one_day_data_pipeline.md) | [简体中文](one_day_data_pipeline.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

状态：当前公共数据获取与诊断回放教程。

## 范围

本教程将一个 UTC 日依次用于 Binance 公共成交下载、确定性一秒 bar 标准化、原始成交审计与诊断回放汇总。它不会将该日自动变成正式研究证据：严格排队或动作研究还需要满足研究合同的 BBO/L2 来源、必要的前一自然日 warmup、冻结质量清单和该研究的证据合同。

若只想在零网络环境下体验排队、成交共同分母、campaign、终端记账和证据检查，请先运行[合成回放示例](../../examples/replay_demo/README.zh-CN.md)。

## 安装与路径

使用已激活的 Python 3.11 或更新环境，同时安装数据获取与回放依赖。`.[data]` 仅覆盖下载和标准化；完整 tick replay 即使指定 `--no-ml`，仍会导入科学计算/研究模块：

```bash
python -m pip install -e ".[data,research]"

# 以下帮助命令不会下载数据或启动回放。
python pipeline.py download-agg-trades --help
python -m models.backtest_tick --help

export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/reports/public_one_day"
export MM_DATA_ROOT="$NARROWGATE_DATA_ROOT"
DAY=YYYY-MM-DD
```

`NARROWGATE_MARKETDATA_ROOT` 是用户管理的父目录，不是源码 checkout，也不应赋值为自身。下列命令按需创建 NarrowGate 子目录树。

## 下载

下载用于 bar 的公共聚合成交，以及用于事件级审计的逐笔成交：

```bash
python pipeline.py download-agg-trades \
  --symbol BTCUSDC --start "$DAY" --end "$DAY"

python pipeline.py download-raw-trades \
  --symbols BTCUSDC --start "$DAY" --end "$DAY"
```

预期源文件为 `${NARROWGATE_DATA_ROOT}/raw/BTCUSDC-aggTrades-${DAY}.csv` 和 `${NARROWGATE_DATA_ROOT}/raw_trades/BTCUSDC/BTCUSDC-trades-${DAY}.csv`。下载或校验失败必须中止；不完整文件不能被接纳为一个完整日期。

## 标准化与审计

构建一秒 bar，然后审计原始成交顺序、方向覆盖、bar 和订单簿可用性：

```bash
python pipeline.py bars \
  --symbol BTCUSDC --data-type aggTrades --file "$DAY"

python pipeline.py audit-raw \
  --symbols BTCUSDC \
  --data-dir "$NARROWGATE_DATA_ROOT" \
  --out-dir "$NARROWGATE_RESULTS_DIR/raw-audit" \
  --file "$DAY"
```

预期输出为 `${NARROWGATE_DATA_ROOT}/bars_1s/BTCUSDC-1s-${DAY}.parquet`、`${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-raw-trades-audit.csv` 和 `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-eligible-days.csv`。只有成交/bar、没有接纳的 BBO/L2 时，`raw_ok` 和 `has_bars` 可以通过，但 `eligible` 应保持 false，原因包含 `missing_bbo;missing_l2`。不得手改结果或静默换用其他来源。

## 诊断回放

用公共模板运行单日、关闭 ML、基于聚合成交时钟的诊断：

```bash
python -m models.backtest_tick \
  --symbol BTCUSDC \
  --config live/config.yaml \
  --day "$DAY" \
  --execution-trade-source aggTrades \
  --replay-event-clock trade \
  --pricing-mode bar \
  --no-ml \
  --summary-json "$NARROWGATE_RESULTS_DIR/replay-${DAY}.json"
```

成功后在 `${NARROWGATE_RESULTS_DIR}` 写入指定标量汇总和回放诊断。这是采用替代排队假设的机制诊断，不是精确订单簿、live parity、经济准入、研究晋级或部署证据。

## 严格订单簿路径

CryptoHFTData 是可选的、需要认证的第三方来源。经授权的用户在上述环境中加装 `.[provider-cryptohft]`，在 Git 之外提供凭据，并遵循[市场数据来源与标准化合同](../market_data.md#binance-and-cryptohftdata)。只获取数据可使用 `.[data,provider-cryptohft]`；严格回放还需研究依赖。该客户端有意不包含在 `all` 中，安装不授予访问权或数据许可。新标准化的日期应留在带来源标签的 staging 目录，直到覆盖、序列、因果性、BBO/L2 配对、warmup 和清单检查通过；不要手动复制进不可变正式登记目录。
