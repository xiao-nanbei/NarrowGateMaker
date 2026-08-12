# BUY Fill-Selection Action Suspension Operational Release V1

Last materially modified: 2026-08-03

Status: deployed and verified.

At `2026-08-02T22:40:46Z`, the active BTCUSDC baseline changed only `strategy.buy_fill_selection_live_enabled` from `true` to `false`. Causal-v12 ML remains ON, q90 shadow remains ON, q90 action remains OFF, and the model, P3, queue, latency, cooldown, inventory, quote-core and safety settings are unchanged.

The operational reason is asymmetric and deliberately conservative. The exact current-stack 40-day diagnostic produced an ON-minus-OFF terminal-MTM point estimate of `-16.7946 USDC`, with `100.57%` fill retention and `103.21%` inventory-time ratio. Its day-clustered interval crosses zero, so it does not prove universal harm; it does show that the overlay lacks benefit evidence and has a low-cost OFF fallback.

The previous remote configuration is preserved at `deploy_backups/live_config_pre_buy_fill_selection_suspend_20260802T223936Z.yaml` with SHA256 `832e389e...`. The new config SHA256 is `55002a250...`. Remote preflight passed, PID `1721900` stopped cleanly, and PID `1756101` started under Python 3.12.13 and the native profile. All 13 causal-v12 heads loaded. The first post-restart health row reported zero BUY fill-selection evaluations and hits, q90 action unauthorized, and an active quote loop.

This release is an owner operational suspension, not a new research promotion. It does not rewrite the historical A/B identity, claim universal harm, or authorize automatic re-enable. Any reactivation requires a new prospective identity against the then-current operational baseline.
