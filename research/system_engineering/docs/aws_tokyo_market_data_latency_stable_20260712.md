# AWS Tokyo market-data latency: stable-window audit (2026-07-12)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Scope

The requested observation interval was the latest three hours. Exact exchange-event, local-receive, and feature-ready timestamps were recorded only for the final bounded hour:

- actual UTC coverage: `2026-07-12T09:54:16.046Z..10:54:16.332Z`;
- duration: `3600.285s`;
- parsed rows: `1,553,377` (`0` parse errors);
- exchange-timestamp rows admitted to the standard profile: `918,743`;
- seven gzip files, `70,468,087` bytes, all passing gzip and SHA-256 checks;
- no process restart inside the capture window.

The missing first two hours were not reconstructed from HEALTH logs. This is a one-hour environment calibration, not a three-hour or multi-day latency claim.

## Environment label

- cloud/region: AWS `ap-northeast-1` (Tokyo);
- instance: `t3.medium`, 2 vCPU, 3.75 GiB;
- OS: Amazon Linux 2023;
- compute path: native quote/global-flow path;
- order gateway: synchronous;
- market-data transport: public WebSocket;
- strategy/config hash during capture: `7cc305259e1efb7cfda586a229039149efa315521fcc877a16453381c1acf8d0`.

`exchange_event_ts -> feature_ready_ts` is called visibility lag below. It contains exchange clock offset, public-network delivery, WebSocket callback scheduling, GIL/VM scheduling, and local feature work. It is not pure one-way network latency.

## Stable BBO results

The user-requested stable baseline is the median. The 5%-95% trimmed mean, p90, and p95 are diagnostic only; p99 and p99.9 are deliberately excluded from parameter selection.

| market | median | 5%-95% trimmed mean | p90 | p95 |
|---|---:|---:|---:|---:|
| Binance BTCUSDC perp | 125.4ms | 298.6ms | 1208.9ms | 1772.5ms |
| Binance BTCUSDT perp | 68.1ms | 264.7ms | 1155.3ms | 1683.7ms |
| Bitget BTCUSDT perp | 5.8ms | 36.1ms | 183.5ms | 310.8ms |
| Bitget BTCUSDT spot | 5.4ms | 16.8ms | 80.5ms | 158.9ms |
| Bybit BTCUSDT perp | 37.4ms | 41.7ms | 64.7ms | 136.4ms |
| Bybit BTCUSDT spot | 40.6ms | 41.9ms | 48.7ms | 76.2ms |
| OKX BTCUSDT perp | 28.8ms | 30.5ms | 38.5ms | 74.8ms |
| OKX BTCUSDT spot | 29.6ms | 33.1ms | 49.6ms | 91.9ms |

The external BBO steady-state ordering is clear: Bitget is around 5-6ms, OKX around 29-30ms, and Bybit around 37-41ms on this host. Those medians are close to the prior 2026-07-11 capture. A 2-of-3 external BBO consensus therefore has a rough steady-state lower bound near the second-fastest source, about 29-30ms. The full reference state still depends on the Binance BTCUSDT bridge, whose observed median was about 68ms, so use 68ms rather than 30ms as the first full-state replay baseline for this exact host/profile.

The Binance book tails are not rare p99 noise. Five-minute bins repeatedly put p90 between roughly 0.4s and 4.6s, while median latency moved from single-digit milliseconds to hundreds of milliseconds. Recorder queues did not drop data (`marketTapeDropped=0`, `externalRecordDropped=0`), but the live process saw a market-tape queue high-water mark of 3,213 rows and maximum recorder queue age of 848.5ms. This points to callback/GIL/VM scheduling backlog before feature visibility. It must be treated as a system limitation, not as Tokyo-to-Binance network propagation.

## Stable trade results

Trade frames are reported separately because one WebSocket frame may carry multiple trades and reconnect/stale batches can distort their tail.

| market | median | p90 | p95 |
|---|---:|---:|---:|
| Binance BTCUSDT perp | 119.5ms | 158.4ms | 186.0ms |
| Binance BTCUSDT spot | 17.9ms | 1205.9ms | 2393.9ms |
| Bitget BTCUSDT perp | 34.6ms | 282.3ms | 373.2ms |
| Bitget BTCUSDT spot | 7.4ms | 135.7ms | 228.0ms |
| Bybit BTCUSDT perp | 63.7ms | 223.8ms | 290.6ms |
| Bybit BTCUSDT spot | 52.2ms | 133.4ms | 179.2ms |
| OKX BTCUSDT perp | 38.8ms | 165.2ms | 226.0ms |
| OKX BTCUSDT spot | 31.2ms | 115.9ms | 253.3ms |

Median feature-processing overhead after callback receipt is only about 0.1-0.7ms across these groups. The dominant delay is therefore before or at callback scheduling, not the native global-flow calculation itself.

## Replay policy

Freeze this environment profile under the exact environment/config label. Do not reuse it as a universal exchange-latency constant.

1. Primary research run: inject the source/event-specific visibility median (`profile_p50`).
2. Ordinary sensitivity run: use captured p90/p95 as explicit stress cases, not as the baseline.
3. Optional spike run: with a fixed random seed, draw the normal delay from the empirical distribution truncated at p95 and, independently, with `0.5%` probability draw one delay from the observed p95-p99 interval.
4. Report results both with and without the spike mixture. A candidate must not rely on zero latency, but occasional spikes do not determine its ranking.

This profile calibrates replay visibility only. It is not strategy evidence and does not activate re-center, cancel, stop-add, or any other arm. A different host or execution stack must be remeasured and rerun. Historical strategy deltas once computed with this profile were removed because their replay clock, feature visibility and operational baseline are no longer admissible.

## Artifacts

- machine-readable replay profile: `live/profiles/latency/aws_tokyo_t3_medium_amzn2023_native_public_ws_20260712_3600s.json`;
- robust group summary: `${NARROWGATE_DATA_ROOT}/reports/market_data_latency_20260712/robust_latency_summary.json`;
- five-minute bins: `${NARROWGATE_DATA_ROOT}/reports/market_data_latency_20260712/latency_5m_bins.json`;
- bounded-capture marker: `logs/receive_time_capture/20260712T095414Z/summary.json` on the labeled live host.
