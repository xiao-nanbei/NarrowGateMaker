# Operations Notes

Last materially modified: 2026-08-20

Operations docs cover dry-run setup, private config boundaries, deployment guardrails, and telemetry. They should not contain private hosts, account details, or raw live PnL.

- Local dry-run workflow: [live_dry_run.md](live_dry_run.md)
- Bounded receive-time capture workflow: [bounded_receive_time_capture_operations_20260724.md](../../research/system_engineering/docs/bounded_receive_time_capture_operations_20260724.md)
- Current AWS host, three predecessor archives, and four-epoch/three-gap query routing: [live_host_and_historical_data_access_20260811.md](../live_host_and_historical_data_access_20260811.md)
- Binance USD-M 1000-level snapshot/diff health probe: `.venv/bin/python scripts/probe_binance_deep_book.py --help`
- Current deployment and live commands: repository `README.md` / `README.zh-CN.md`
