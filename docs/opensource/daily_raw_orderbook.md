# Daily raw order-book storage

Updated: 2026-09-08. [中文](daily_raw_orderbook.zh-CN.md)

Last materially modified: 2026-09-08
Last materially synchronized: 2026-09-08

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

The downloader now retires hourly staging files after verified daily publication. Use `--keep-hourly-raw` to retain them. Before deletion it checks the daily content digest and the remaining hourly source identities; any mismatch preserves the source. Interrupted retirement can resume. Incomplete days are retained. Native Tardis purchases are never deleted or overwritten by this operation.

Current native replay, cache and normalization readers can resolve their logical UTC-hour locators into the daily container. They extract only that hour's original columns and preserve message order, sequence IDs and both clocks. No permanent hourly duplicate is recreated. The default daily root is the sibling `tardis_compatible` directory; when using a custom output location, set `NARROWGATE_DAILY_ORDERBOOK_ROOT` to it before retirement and in subsequent reader environments. Do not run retirement concurrently with writers or old executors using the hourly files.

Raw-format migration changes storage/cache identity, not market events. Historical frozen manifests are not rewritten: their original compressed-byte identities remain provenance, while a new execution binds the daily representation. Old executors without this reader update cannot directly reuse removed hour files. Studio should register the daily containers, not scan downloader staging directories; retain provider provenance rather than labeling every file as purchased Tardis data.
Processed BBO/L2, bars, features and scenario-specific quality checks remain a
separate layer; changing raw encoding does not certify missing market events.
