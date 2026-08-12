# Market-data latency profile: `aws_tokyo_binance_perp_btcusdc_20260725_3597s`

## Environment

- `host`: `aws_tokyo_t3_medium_amzn2023`
- `capture_id`: `20260725T153151Z`
- `source_depth_recorded`: `False`
- `scope`: `receive_feature_ready_latency_profile_not_event_path`

## Measurement

- Window: `3597s`
- Rows: `83907`
- Transport lag is exchange-clock-sensitive; it is not pure one-way network latency.
- `captured` uses recorded visibility; `exchange_zero` is idealized; `profile_*` injects this host profile.

## Groups

| market | event | transport | n | p50 | p95 | p99 | p99.9 | max | feature p50 us |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| binance:perp:BTCUSDC | book | websocket | 80462 | 33.679 | 1593.199 | 3838.166 | 14291.910 | 14623.537 | 72.8 |
| binance:perp:BTCUSDC | trade | websocket | 3445 | 102.990 | 308.078 | 1055.043 | 1306.651 | 1788.515 | 324.9 |
