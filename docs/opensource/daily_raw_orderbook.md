# Daily raw order-book storage

Updated: 2026-09-08. [中文](daily_raw_orderbook.zh-CN.md)

The existing CryptoHFTData downloader now exports each complete 24-hour day as
Tardis-compatible **Parquet with extension columns** after its normal processing.
This is not an assertion that CryptoHFTData is Tardis, nor an unmodified Tardis CSV.
The default destination is `tardis_compatible` beside the resolved hourly raw root.
Use `--tardis-output-root` to override it.

To convert already downloaded hours without a network request or replay:

```bash
python -m data.download_cryptohft_orderbook \
  --start 2026-09-05 --end 2026-09-05 --symbols BTCUSDC \
  --raw-root /path/to/hourly-raw \
  --tardis-output-root /path/to/daily-raw --tardis-export-only
```

Output layout is `exchange/symbol/incremental_book_L2/YYYY-MM-DD.parquet`.
Standard aliases include `exchange`, `symbol`, `timestamp`, `local_timestamp`,
`is_snapshot`, `side`, `price`, and `amount`. Timestamp aliases use microseconds;
price and amount retain their exact source decimal strings. Every original field,
including nanosecond receive time, millisecond event/transaction time, sequence
IDs and order count, remains present. `source_hour` and `source_row` preserve
physical source order. No sorting, deduplication, gap filling or time invention occurs.

Publication compares every original column and row against the 24 inputs, then
publishes the file and a conversion record. Reruns reuse a verified existing day;
missing hours are reported without publishing a partial day. A conflicting or
incompletely published day raises an error and retains its sources.

**Retirement is separate:** the exporter does not delete hourly originals yet.
Readers and frozen input references must first migrate to the daily representation.
Conversion success alone does not authorize breaking those references. Native
Tardis originals remain separate provider evidence, not overwritten by this export.
Processed BBO/L2, bars, features and scenario-specific quality checks remain a
separate layer; changing raw encoding does not certify missing market events.
