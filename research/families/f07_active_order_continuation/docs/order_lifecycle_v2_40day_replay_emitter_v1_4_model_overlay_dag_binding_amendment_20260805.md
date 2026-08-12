# F07 Lifecycle-v2 40-Day Replay Emitter v1.4

Last materially modified: 2026-08-05

Status: operational model-overlay DAG binding implemented; formal 40-day replay not yet executed.

The v1.3 worker passed strict native queue admission and then stopped before tick replay because the selected v13 market-context window intentionally had no `ml_data`. Current v9 is ML-ON, so silently replaying without the operational causal-v12 predictions would change the baseline. No lifecycle row or economic outcome was admitted.

v1.4 binds the existing `model_overlay_day` component separately for every target day. The component identity is derived from the current native market context, causal feature sources, and frozen causal-v12 bundle. Its manifest, compressed payload, byte size, and SHA256 enter the daily source identity. The worker revalidates the component before attaching it to the market-context window. Mutable source locators are not used to recreate a historical cache key: the overlay's persisted market-context trades and rolling arrays must instead match the frozen v13 window exactly before admission.

This preserves the replay-cache DAG boundary: market data are not copied when the model changes, and model predictions are not embedded into a new 1.1GB window. The frozen 40-day denominator, q90 action-OFF baseline, strict native queue tape, lifecycle schema, and mechanics-only economic firewall remain unchanged. A new execution plan is required because the runner identity changed.
