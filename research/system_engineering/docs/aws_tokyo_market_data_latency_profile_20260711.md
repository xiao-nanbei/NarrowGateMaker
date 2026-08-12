# Market-data latency profile: `aws_tokyo_t3_medium_amzn2023_native_public_ws_20260711_3600s`

Last materially modified: 2026-07-27

Status: Frozen environment-specific measurement. Re-profile after any host, region, network, feed, or process-layout change.

## Environment

- `cloud`: `AWS`
- `region`: `ap-northeast-1`
- `location`: `Tokyo`
- `instance_type`: `t3.medium`
- `vcpu`: `2`
- `memory_gib`: `3.75`
- `architecture`: `x86_64`
- `os`: `Amazon Linux 2023.12.20260622`
- `kernel`: `6.18.35-68.127.amzn2023.x86_64`
- `python`: `3.9.25`
- `live_compute_profile`: `native`
- `order_gateway`: `synchronous`
- `external_transport`: `public_websocket`
- `window_includes_controlled_restart`: `True`

## Measurement

- Window: `3600s`
- UTC window: `2026-07-11T08:54:29Z` to `2026-07-11T09:54:29Z`
- Rows: `682110`
- Transport lag is exchange-clock-sensitive; it is not pure one-way network latency.
- `captured` uses recorded visibility; `exchange_zero` is idealized; `profile_*` injects this host profile.

## Groups

| market | event | transport | n | p50 | p95 | p99 | p99.9 | max | feature p50 us |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| binance:perp:BTCUSDC | book | websocket | 97717 | 10.544 | 1037.920 | 2838.378 | 4387.427 | 5354.787 | 71.3 |
| binance:perp:BTCUSDC | trade | websocket | 4009 | 115.556 | 222.172 | 432.234 | 1456.859 | 1764.525 | 68.2 |
| binance:perp:BTCUSDT | book | websocket | 300907 | 11.295 | 1084.315 | 2777.963 | 4228.144 | 5381.308 | 72.0 |
| binance:perp:BTCUSDT | trade | websocket | 13835 | 153.854 | 180.553 | 442.959 | 1502.161 | 1712.649 | 93.0 |
| bitget:perp:BTCUSDT | book | websocket | 53008 | 5.649 | 177.703 | 701.315 | 1765.599 | 2157.286 | 93.0 |
| bitget:perp:BTCUSDT | trade | websocket | 10239 | 16.822 | 335.102 | 1420.990 | 32809.944 | 32812.086 | 196.9 |
| bitget:spot:BTCUSDT | book | websocket | 48788 | 5.411 | 73.897 | 268.434 | 1261.078 | 1645.885 | 94.8 |
| bitget:spot:BTCUSDT | trade | websocket | 4817 | 6.354 | 159.250 | 5355.593 | 138997.673 | 139035.673 | 100.5 |
| bybit:perp:BTCUSDT | book | websocket | 26337 | 37.949 | 104.964 | 529.888 | 1862.824 | 2787.019 | 133.0 |
| bybit:perp:BTCUSDT | trade | websocket | 17698 | 55.240 | 393.458 | 1310.360 | 1842.532 | 2687.657 | 224.4 |
| bybit:spot:BTCUSDT | book | websocket | 16553 | 39.880 | 75.947 | 440.486 | 1337.769 | 1724.596 | 132.1 |
| bybit:spot:BTCUSDT | trade | websocket | 9629 | 44.715 | 193.715 | 822.490 | 1354.355 | 1621.766 | 323.0 |
| okx:perp:BTCUSDT | book | websocket | 44674 | 28.509 | 55.086 | 460.074 | 2285.583 | 3358.901 | 135.5 |
| okx:perp:BTCUSDT | trade | websocket | 11148 | 33.464 | 312.367 | 1210.869 | 1882.100 | 3285.842 | 100.8 |
| okx:spot:BTCUSDT | book | websocket | 19515 | 36.391 | 60.299 | 451.214 | 1749.271 | 1811.693 | 130.1 |
| okx:spot:BTCUSDT | trade | websocket | 3236 | 37.886 | 244.683 | 475.994 | 852.850 | 1067.106 | 133.7 |

## Interpretation

The BBO medians are the most useful steady-state comparison:

| venue | perpetual p50 / p95 / p99 | spot p50 / p95 / p99 |
|---|---:|---:|
| Binance local | 11.3 / 1084.3 / 2778.0ms | not calibrated: spot bookTicker has no exchange timestamp |
| Bitget | 5.6 / 177.7 / 701.3ms | 5.4 / 73.9 / 268.4ms |
| Bybit | 37.9 / 105.0 / 529.9ms | 39.9 / 75.9 / 440.5ms |
| OKX | 28.5 / 55.1 / 460.1ms | 36.4 / 60.3 / 451.2ms |

This is not a pure venue-ranking table. The window includes one controlled live restart, and the large synchronized tails include callback/GIL/VM scheduling backlog on the 2-vCPU collector. The Binance perpetual p99 near 2.8 seconds is therefore a system warning, not a claim that Tokyo-to-Binance network propagation normally takes seconds.

Feature processing is usually much smaller than transport/callback delay: BBO feature-latency p50 is about 71-136us across these feeds. Its p99 still reaches roughly 0.8-25ms, and trade feature p99 can reach hundreds of milliseconds during process stalls. Trade transport tails also contain stale initial/reconnect batches. Trades older than one second are retained in the raw tape and profile but are excluded from 10-500ms `GlobalFlowState`.

## Replay Smoke

The same 3600-second tape contains four maker fills, all BUY. Five visibility modes were replayed with seed 7. This is an execution smoke, not alpha evidence.

| mode | 10ms mean flow | 100ms mean flow | 100ms sign agreement vs captured | 100ms mean markout |
|---|---:|---:|---:|---:|
| exchange_zero | -0.1278 | -0.9717 | 100% | +1.7826bps |
| captured | -0.1562 | -0.3308 | reference | +1.7826bps |
| profile_p50 | -0.2972 | -0.6191 | 100% | +1.7826bps |
| profile_empirical | -0.0848 | -0.5805 | 100% | +1.7826bps |
| profile_p99 | 0.0000 | -0.0633 | 50% | +1.7826bps |

Markout is intentionally unchanged because no quote action consumes the external state. The result demonstrates the intended replay semantics: latency changes what was visible at fill time, while economic labels remain fixed. Under the p99 profile, the 10ms flow is effectively unavailable and the 100ms state is materially weakened. No re-center/cancel/live-alpha claim can be made from four fills.
