# F03 71-day authoritative continuous tick execution v1.3

Last materially modified: 2026-08-05

## Status

v1.3 adds the concrete NarrowGate tick replay adapter that v1.2 lacked. The v1.2 execution plan remains frozen as historical orchestration/checkpoint framework evidence; it is not reclassified as an executable replay.

The v1.3 `prepare` and `validate` paths are outcome blind. The current formal 71-day run remains blocked until exact v9 10-second control and true 1-second candidate policy manifests bind all 71 model-free market windows and overlays.

## Execution semantics

- A real C++ tick replay call owns every active epoch from causal resume through the frozen maintenance cancel drain. It uses the current quote policy, order lifecycle, queue model, partial fills, cancel/ACK race, cooldowns, and model sample-and-hold implementation.
- Active epochs may cross UTC midnight. Midnight emits an accounting/cluster slice and never resets cash, inventory, campaign, orders, queue, cooldown, or model state.
- Quote stop is followed by the actual pre-gap market tape. Fills before cancel ACK remain in the order risk set. Checkpoint admission requires zero active, pending-new, and pending-cancel orders.
- Offline gaps submit no quote and admit no fill. Inventory is marked through gap prices while cash, position, and economic campaign ownership remain unchanged.
- Resume consumes hash-bound past-only market and prediction events with quoting disabled, then starts from a fresh exchange-book/order state.
- Runtime-only campaign, cooldown, EMA, order, queue, and cursor state follows the frozen production restart reset. Economic campaign ownership is retained separately for continuous attribution.
- Both arms consume the same epoch seed/random-path identity and maintain independent economic and order paths.

## Checkpoint scope

Each atomic checkpoint hashes economic carry, UTC accounting cursor/history, campaign/reward ownership, empty post-drain order/queue/cursor state, reset cooldown and held-feature state, and the completed RNG path identity. Resume revalidates plan, policy, market-window, overlay, compiled C++ extension, execution-source, and checkpoint hashes before advancing. The paired receipt is the sole admission commit for an epoch: both arm checkpoints must be present and hash-bound by that receipt. A checkpoint written before a crash but absent from the paired receipt is ignored and deterministically recomputed.

## Authority

Native segments may carry exact queue/lifecycle authority. Provider-normalized segments emit explicit authority flags and are limited to continuous PnL, inventory, and campaign sensitivity. They never grant exact queue, lifecycle, or q90 authority.

This layer does not read or aggregate PnL now. It grants no promotion, action, live, Validation, or holdout permission and cannot replace the 40-day exact-native primary economic test.
