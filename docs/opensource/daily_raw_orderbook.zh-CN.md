# 日级原始订单簿存储

更新：2026-09-08。[English](daily_raw_orderbook.md)

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

**原件清理另行执行：当前转换器不会删除小时原件。** 必须先迁移读取器与冻结输入引用，
不能只凭转换成功就破坏旧引用。原生 Tardis 文件保留独立来源，不被转换结果覆盖。
处理后的 BBO/L2、Bar、特征及分场景质量检查仍属另一层；统一编码不等于补出了缺失事件。
