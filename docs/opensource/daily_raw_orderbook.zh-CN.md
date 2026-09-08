# 日级原始订单簿存储

更新：2026-09-08。[English](daily_raw_orderbook.md)

Last materially modified: 2026-09-08
Last materially synchronized: 2026-09-08

现有 CryptoHFTData 下载器在正常处理完成后，将每个完整的 24 小时数据转换为
**Tardis 兼容的 Parquet，保留扩展字段**。不是将来源冒充为 Tardis，也不是原版
Tardis CSV。默认输出到已解析小时原始目录旁的 `tardis_compatible`；可用
`--tardis-output-root` 指定位置。

仅转换已下载文件，不联网、不运行回测：

```bash
python -m data.download_cryptohft_orderbook \
  --start 2026-09-05 --end 2026-09-05 --symbols BTCUSDC \
  --raw-root /path/to/hourly-raw \
  --tardis-output-root /path/to/daily-raw --tardis-export-only
```

输出为 `exchange/symbol/incremental_book_L2/YYYY-MM-DD.parquet`。
标准别名包含 exchange、symbol、timestamp、local_timestamp、is_snapshot、side、
price、amount；时间别名使用微秒，价格与数量保留原始精确十进制字符串。
全部原始字段仍然保留，包括纳秒接收时间、毫秒事件/交易时间、序列号与订单数。
`source_hour` 和 `source_row` 保留源顺序。不排序、去重、填补缺口或编造时间。

发布前逐字段、逐行与 24 个小时输入核对。已验证日期可以复用；缺小时明确报告，
不发布不完整日文件。已有结果冲突或发布中断会报错，并保留源文件。

下载器现在会在日文件验证发布成功后删除小时暂存文件；使用 `--keep-hourly-raw` 可以保留。删除前核对日文件内容摘要及剩余小时文件身份，任何不一致都会保留源文件。中断的清理可以继续，不完整日期继续保留。此操作不会删除或覆盖购买的原生 Tardis 文件。

当前原生回测、缓存及标准化读取器可以将逻辑 UTC 小时定位到日文件，只提取该小时的原始字段，保留消息顺序、序列号和双时钟，不重新保存永久小时副本。默认日文件根目录是旁边的 `tardis_compatible`；使用自定义输出目录时，清理前及后续读取环境都需设置 `NARROWGATE_DAILY_ORDERBOOK_ROOT`。不要在旧执行器读取小时文件或写入器工作期间同时清理。

格式迁移改变存储／缓存身份，不改变市场事件。历史冻结清单不重写：原压缩文件身份保留作来源记录，新执行绑定日文件。没有本次读取器更新的旧执行器不能直接使用已删除的小时文件。界面登记日级容器，不再扫描下载暂存目录；保留供应商来源，不把所有文件称为购买的 Tardis 数据。
处理后的 BBO/L2、Bar、特征及分场景质量检查仍属另一层；统一编码不等于补出了缺失事件。
