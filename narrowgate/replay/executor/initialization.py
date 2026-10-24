"""Explicit replay account/input assembly; no event loop or venue operations."""

from __future__ import annotations

from .state import ReplayRuntimeState


class ReplayInitialization:
    def initialize(self):
        """
        Event-driven tick-level simulation with probabilistic FIFO fill model.

        Aligned with live maker_engine.py and backtest_ml.py feature set:
          - Regime L0 (liquidity spread scaling), L1 (empirical volatility scaling),
            L2 (P3 floor), L3 (inventory)
          - Dynamic RQ (fast/slow vol EMA → adaptive requote interval)
          - BER guard (trade intensity spike → spread widening)
          - Markout tracking (fill quality EMA → spread/asym adjustment)
          - CJP inventory skew (φ · q/q_max · δ)
          - Exit urgency (time + pnl + signal → asymmetry)
          - Position timeout (taker fee close)
          - Ret prediction: clamp + inventory-aware fade + EMA demean
          - Anti-flip cap, mid guard, max_spread_bps, kappa_ratio
          - Lazy requote (Step 26B)

        Unique to tick-level backtest (vs 1s-bar backtest_ml.py):
          - FIFO queue-based fill model (volume accumulation per side)
          - Per-trade granularity (no OHLC aggregation bias)
          - Depth microprice + depth-based dynamic κ from raw snapshots

        中文说明：这是“单段状态机”。调用方如果传入多天数据，本函数不会自动
        在 UTC 日界或长 gap 处重置 adverse/defense/markout/cooldown 状态；
        正式 daily evidence 应通过 `_simulate_daily_segmented_with_engine()` 进入。
        """
        self.context.generic_markout_enabled = self.context.params.get('_research_generic_markout_policy_enabled', True)
        if type(self.context.generic_markout_enabled) is not bool:
            raise ValueError('_research_generic_markout_policy_enabled must be boolean')
        self.context.inventory_cooldown_mode, self.context.inventory_markout_scope = self.services.resolve_inventory_soft_control_modes(self.context.params)
        self.context.phase_callback = self.context.params.get('_replay_phase_callback')
        if self.context.phase_callback is not None:
            self.context.phase_callback('account_and_clock_initialization')
        if self.context.resume_checkpoint is not None and self.context.resume_checkpoint.get("consumed", False):
            raise ValueError("runtime checkpoint ownership has already been consumed")
        if self.context.prefill_fork is not None and (self.context.resume_checkpoint is None or self.context.resume_input_batch
                                       or self.context.consume_resume_checkpoint or self.context.checkpoint_at_ts_ms is not None):
            raise ValueError('prefill branches require an independently cloned same-window checkpoint')
        if self.context.response_update_fork is not None and (self.context.resume_checkpoint is None or self.context.resume_input_batch
                or self.context.consume_resume_checkpoint or self.context.checkpoint_at_ts_ms is not None or self.context.prefill_fork is not None):
            raise ValueError('optional-update branches require an independently cloned same-window checkpoint')
        self.state = ReplayRuntimeState()
        self.state.quote_price_runtime = None
        if self.context.params.get('_quote_price_runtime') is not None:
            from copy import deepcopy
            runtime = self.context.params['_quote_price_runtime']
            if (not isinstance(getattr(runtime, 'contract', None), dict)
                    or runtime.contract.get('schema') not in {'quote-price-replay-v1', 'quote-price-replay-v2'}
                    or any(not callable(getattr(runtime, name, None)) for name in
                           ('begin_capture', 'correction', 'finish_capture', 'audit'))):
                raise ValueError('bound quote-price replay runtime required')
            self.state.quote_price_runtime = deepcopy(runtime)
        # Offline, explicitly bound cooldown-only extension. Existing workers omit
        # it; their original fills, cancellation path and admission remain exact.
        self.state.cooldown_rearm_runtime = None
        if self.context.params.get('_cooldown_rearm_runtime') is not None:
            from copy import deepcopy
            self.context.deepcopy = deepcopy
            self.context.runtime = self.context.params['_cooldown_rearm_runtime']
            if (not isinstance(getattr(self.context.runtime, 'contract', None), dict)
                    or self.context.runtime.contract.get('schema') != 'cooldown-rearm-replay-v1'
                    or any(not callable(getattr(self.context.runtime, name, None))
                           for name in ('on_fill', 'evaluate', 'audit'))
                    or self.context.inventory_cooldown_mode != 'original'
                    or self.context.inventory_markout_scope != 'original_all_roles'):
                raise ValueError('explicit original-mode cooldown rearm runtime required')
            self.state.cooldown_rearm_runtime = self.context.deepcopy(self.context.runtime)
        from strategy.quote_schedule import QuoteSchedule, quote_schedule_mode, strict_ready_ms
        self.context.QuoteSchedule = QuoteSchedule
        self.context.quote_schedule_mode = quote_schedule_mode
        self.context.strict_ready_ms = strict_ready_ms
        self.state.quote_schedule_mode = self.context.quote_schedule_mode(self.context.params.get("quote_schedule_mode", "existing"))
        self.state.quote_schedule = self.context.QuoteSchedule() if self.state.quote_schedule_mode == "state_event" else None
        self.state.quote_schedule_visible_ms = None
        from strategy.target_observation import TargetObservation
        self.context.TargetObservation = TargetObservation
        self.state.target_observation = (
            self.context.TargetObservation() if (self.context.params.get("target_observe_only", False) or
                self.context.params.get("replace_price_threshold_mode") in ("dynamic_outward", "static_outward")) else None
        )
        if self.state.target_observation is not None and (
            float(self.context.params.get("replay_main_loop_sleep_ms", 0) or 0) <= 0
            and self.context.params.get("replay_main_loop_wait_mode", "poll") != "event_deadline"
        ):
            raise ValueError("target observation requires the existing main-loop completion clock")
        self.state.public_strategy = self.context.public_strategy
        self.state.response_update_observer = None
        self.state.response_update_branch_enabled = self.context.params.get('response_update_branch_enabled', False)
        self.state.response_update_branch_evidence = None
        self.state.response_update_admission = {}
        self.state.response_action_sampler = None
        self.state.response_action_runtime = None
        if self.context.params.get('response_action_policy') is not None:
            if (not self.state.response_update_branch_enabled or self.context.response_update_fork is not None
                    or self.context.params.get('response_action_sampling_slots') is not None):
                raise ValueError('continuous response policy requires observation admission without forks/sampling')
            from strategy.response_action_runtime import ResponseActionRuntime
            self.context.ResponseActionRuntime = ResponseActionRuntime
            self.state.response_action_runtime = self.context.ResponseActionRuntime(self.context.params['response_action_policy'])
        if self.context.params.get('response_action_sampling_slots') is not None:
            if not self.state.response_update_branch_enabled:
                raise ValueError('action sampling requires enabled optional-update branches')
            from research.families.f08_side_taker_lifecycle.response_sampling import OptionalUpdateSampler
            self.context.OptionalUpdateSampler = OptionalUpdateSampler
            self.state.response_action_sampler = self.context.OptionalUpdateSampler(
                self.context.params['response_action_sampling_slots'], self.context.params['response_action_account_end_ns'])
        if type(self.state.response_update_branch_enabled) is not bool:
            raise ValueError('response_update_branch_enabled must be boolean')
        if self.state.response_update_branch_enabled and not self.context.params.get('response_observe_only', False):
            raise ValueError('optional-update branches require bound response observations')
        if type(self.context.params.get('response_observe_only', False)) is not bool:
            raise ValueError('response_observe_only must be boolean')
        if self.context.params.get('response_observe_only', False):
            from data.response_cursor import ResponseObservationCursor
            self.context.ResponseObservationCursor = ResponseObservationCursor
            from strategy.update_opportunity import UpdateOpportunityObserver
            self.context.UpdateOpportunityObserver = UpdateOpportunityObserver
            if self.context._prepared_clock_owner is None:
                raise ValueError('response observation requires bound prepared inputs')
            self.state.response_update_observer = self.context.UpdateOpportunityObserver(
                self.context.ResponseObservationCursor(self.context._prepared_clock_owner.inputs['bundle']))
        from models.replay.l2_journal import ReplayL2Production
        self.context.ReplayL2Production = ReplayL2Production
        self.state.l2_production = self.context.ReplayL2Production()
        self.state.target_journal = self.context.params.get("_target_journal")
        self.state.target_production = self.context.ReplayL2Production()
        if self.state.target_journal is not None and self.state.target_observation is None:
            raise ValueError("target journal requires observe-only mode")
        self.state.l2_decision_links = {}
        self.state.l2_journal = self.context.params.get("_l2_journal")
        if self.state.l2_journal is not None and (
            self.context.params.get("trace_external_market_release", False)
            or self.context.params.get("fill_cooldown_clock_mode", "wall_time") != "wall_time"
        ):
            raise ValueError("L2 output has not validated deferred release or mutable variance-clock diagnostics")
        self.services.validate_replay_initial_state(self.context.params.get("initial_live_state"), backend="python")
        if (self.context.params.get("signal_cold_start") or self.context.params.get("cold_flat_clock_start")) and (
            self.context.params.get("initial_live_state") is not None or float(self.context.params.get("initial_inventory", 0.0)) != 0.0
        ):
            raise ValueError("cold signal startup requires an empty experimental account")
        # A cold-start experiment can resume after acquiring inventory and signal
        # state. Its checkpoint is restored below; it is not a second cold start.
        # Keep rejecting conflicting non-empty initial-account parameters.
        self.state.risk_selection = None
        self.context.opportunity_retention = self.context.params.get("risk_selection_opportunity_retention", "full")
        if self.context.opportunity_retention not in {"full", "counts_only"}:
            raise ValueError("unknown risk-selection opportunity retention")
        if self.context.opportunity_retention == "counts_only" and (
            self.context.prefill_fork is not None or not self.context.params.get("risk_selection_collect_opportunities", False)
        ):
            raise ValueError("counts_only requires collection and cannot be used with PrefillFork")
        self.state.risk_selection_mode = self.context.params.get("risk_selection_mode", "B")
        self.state.risk_selection_control = self.context.params.get("risk_selection_control", "learned")
        self.state.risk_selection_feature_contract = self.context.params.get('risk_selection_feature_contract')
        if self.state.risk_selection_feature_contract not in (None, self.services.PREFILL_FEATURE_CONTRACT):
            raise ValueError('unknown risk selection feature contract')
        if (bool(self.context.params.get("risk_selection_collect_opportunities", False))
                or self.state.risk_selection_mode != "B" or self.state.risk_selection_control != "learned"):
            if self.state.risk_selection_control == "flat":
                self.state.initial_q = float(self.context.params.get("initial_inventory", 0.0) or 0.0)
                self.state.state = self.context.params.get("initial_live_state") or {}
                if (not self.services.math.isfinite(self.state.initial_q) or abs(self.state.initial_q) > 1e-10
                        or self.state.state.get("active_orders") or self.state.state.get("health_orders", 0)):
                    raise ValueError(
                        "Flat requires a known flat initial account without pending ownership"
                    )
            self.state.risk_selection = self.services.ReplayRiskSelection(
                retention=self.context.opportunity_retention,
                intervention=self.context.params.get("risk_selection_intervention"),
                sink=self.context.params.get("_risk_selection_opportunity_sink"),
                max_rows=int(self.context.params.get("risk_selection_opportunity_max_rows", 0)),
                mode=self.state.risk_selection_mode,
                policy=self.context.params.get("risk_selection_policy"),
                control=self.state.risk_selection_control,
                random_rates=self.context.params.get("risk_selection_random_rates"),
                random_seed=self.context.params.get("risk_selection_random_seed"),
                random_scope=self.context.params.get("risk_selection_random_scope", ""),
                selection_scope=self.context.params.get("risk_selection_scope", "reachable_inventory"),
            )
        elif (self.context.params.get("risk_selection_intervention") is not None
              or self.context.params.get("_risk_selection_opportunity_sink") is not None):
            raise ValueError("risk-selection intervention/sink requires opportunity collection")
        if self.state.risk_selection_feature_contract is not None and (
                self.state.risk_selection is None
                or self.state.risk_selection.selection_scope != 'visible_inventory'):
            raise ValueError('prefill features require visible-inventory opportunity collection')
        self.state.TICK = float(self.context.params.get("tick_size", self.services.TICK))
        self.state.LOT_SIZE = float(self.context.params.get("lot_size", self.services.LOT_SIZE))
        if not self.services.math.isfinite(self.state.TICK) or self.state.TICK <= 0.0:
            raise ValueError(f"tick_size must be positive, got {self.state.TICK!r}")
        if not self.services.math.isfinite(self.state.LOT_SIZE) or self.state.LOT_SIZE <= 0.0:
            raise ValueError(f"lot_size must be positive, got {self.state.LOT_SIZE!r}")
        self.state.replay_progress_callback = self.context.params.get("_replay_progress_callback")
        if self.state.replay_progress_callback is not None and not callable(
            self.state.replay_progress_callback
        ):
            raise ValueError("_replay_progress_callback must be callable")
        self.state.replay_progress_interval_events = int(
            self.context.params.get("_replay_progress_interval_events", 250_000) or 250_000
        )
        if self.state.replay_progress_interval_events <= 0:
            raise ValueError("_replay_progress_interval_events must be positive")
        self.state.planned_quote_stop_ts_ms = int(
            self.context.params.get("planned_quote_stop_ts_ms", 0) or 0
        )
        if self.state.planned_quote_stop_ts_ms < 0:
            raise ValueError("planned_quote_stop_ts_ms must be non-negative")
        self.state.planned_quote_stop_triggered = False
        self.state.planned_quote_stop_trigger_ts_ms = 0
        self.state.planned_shutdown_orders_at_trigger = 0
        self.state.ema_add_wait_fork_enabled = bool(
            self.context.params.get("ema_add_wait_fork_enabled", False)
        )
        self.state.ema_add_wait_fork_action = str(
            self.context.params.get("ema_add_wait_fork_action", "") or ""
        ).strip().upper()
        self.state.ema_add_wait_fork_target_ts_ms = int(
            self.context.params.get("ema_add_wait_fork_target_ts_ms", 0) or 0
        )
        self.state.ema_add_wait_fork_target_side = str(
            self.context.params.get("ema_add_wait_fork_target_side", "") or ""
        ).strip().upper()
        self.state.ema_add_wait_fork_target_inventory_lifecycle_id = int(
            self.context.params.get("ema_add_wait_fork_target_inventory_lifecycle_id", 0) or 0
        )
        self.state.ema_add_wait_fork_target_baseline_action = str(
            self.context.params.get("ema_add_wait_fork_target_baseline_action", "") or ""
        ).strip().upper()
        self.state.ema_add_wait_fork_target_generation = self.context.params.get(
            "ema_add_wait_fork_target_generation"
        )
        self.state.ema_add_wait_fork_target_market_event_index = int(
            self.context.params.get("ema_add_wait_fork_target_market_event_index", -1) or -1
        )
        self.state.ema_add_wait_fork_frozen_release_ts_ms = int(
            self.context.params.get("ema_add_wait_fork_release_ts_ms", 0) or 0
        )
        self.state.ema_add_wait_fork_frozen_release_event_index = int(
            self.context.params.get("ema_add_wait_fork_release_market_event_index", -1) or -1
        )
        self.state.ema_add_wait_fork_frozen_release_generation = self.context.params.get(
            "ema_add_wait_fork_release_generation"
        )
        self.state.ema_add_wait_fork_release_clock = str(
            self.context.params.get(
                "ema_add_wait_fork_release_clock",
                "frozen_scheduled_requote_v1",
            )
            or "frozen_scheduled_requote_v1"
        ).strip()
        if self.state.ema_add_wait_fork_enabled:
            if self.state.ema_add_wait_fork_action not in {"ADD_NOW", "WAIT_ONE_EPOCH"}:
                raise ValueError(
                    "EMA ADD/WAIT fork action must be ADD_NOW or WAIT_ONE_EPOCH"
                )
            if self.state.ema_add_wait_fork_target_baseline_action not in {
                "ADD_NOW",
                "WAIT_ONE_EPOCH",
            }:
                raise ValueError(
                    "EMA ADD/WAIT fork requires the frozen baseline action"
                )
            if (
                self.state.ema_add_wait_fork_target_ts_ms <= 0
                or self.state.ema_add_wait_fork_target_side not in {"BUY", "SELL"}
                or self.state.ema_add_wait_fork_target_inventory_lifecycle_id <= 0
            ):
                raise ValueError("EMA ADD/WAIT fork target identity is incomplete")
            if not isinstance(self.state.ema_add_wait_fork_target_generation, self.services.Mapping):
                raise ValueError("EMA ADD/WAIT fork target generation must be a mapping")
            self.state.expected_generation_fields = {
                "bbo_index",
                "l2_index",
                "trade_index",
                "feature_ready_index",
                "prediction_index",
                "snapshot_mid_tick_x2",
            }
            if set(self.state.ema_add_wait_fork_target_generation) != self.state.expected_generation_fields:
                raise ValueError(
                    "EMA ADD/WAIT fork target generation schema drifted"
                )
            if (
                self.state.ema_add_wait_fork_target_market_event_index < 0
                or self.state.ema_add_wait_fork_frozen_release_ts_ms
                < self.state.ema_add_wait_fork_target_ts_ms
                or self.state.ema_add_wait_fork_frozen_release_event_index
                <= self.state.ema_add_wait_fork_target_market_event_index
            ):
                raise ValueError("EMA ADD/WAIT fork release locator is invalid")
            if not isinstance(
                self.state.ema_add_wait_fork_frozen_release_generation, self.services.Mapping
            ) or set(self.state.ema_add_wait_fork_frozen_release_generation) != self.state.expected_generation_fields:
                raise ValueError(
                    "EMA ADD/WAIT fork release generation schema drifted"
                )
            if self.state.ema_add_wait_fork_release_clock not in {
                "frozen_scheduled_requote_v1",
                "next_ready_market_generation_v1",
            }:
                raise ValueError("EMA ADD/WAIT fork release clock is unsupported")
            if any(
                bool(self.context.params.get(name, False))
                for name in (
                    "buy_soft_widen_release_probe_enabled",
                    "conditional_p3_reach_gate_enabled",
                    "conditional_p3_reach_budget_policy_enabled",
                    "safe_add_rearm_randomized_enabled",
                    "state_conditioned_rearm_enabled",
                    "sell_add_skip_ope_enabled",
                    "state_conditioned_quote_policy_enabled",
                    "local_action_ope_enabled",
                    "queue_value_keep_cancel_enabled",
                )
            ):
                raise ValueError(
                    "EMA ADD/WAIT fork cannot be combined with another research action"
                )
            if bool(self.context.params.get("dynamic_fill_hazard_action_enabled", False)):
                raise ValueError("EMA ADD/WAIT fork requires q90 action OFF")
            if bool(self.context.params.get("buy_fill_selection_live_enabled", False)):
                raise ValueError("EMA ADD/WAIT fork requires BUY fill selection OFF")
        self.state.cooldown_duration_trace_max = int(
            self.context.params.get("trace_cooldown_duration_opportunities_max", 0) or 0
        )
        if self.state.cooldown_duration_trace_max < 0:
            raise ValueError(
                "trace_cooldown_duration_opportunities_max must be non-negative"
            )
        self.state.cooldown_v2_snapshot_emitter = self.context.params.get("cooldown_v2_snapshot_emitter")
        if self.state.cooldown_v2_snapshot_emitter is not None:
            self.state.required_methods = {"capture_exposure_fill", "audit"}
            self.state.missing_methods = sorted(
                name
                for name in self.state.required_methods
                if not callable(getattr(self.state.cooldown_v2_snapshot_emitter, name, None))
            )
            if self.state.missing_methods:
                raise ValueError(
                    "cooldown v2 snapshot emitter is incomplete: "
                    f"{self.state.missing_methods}"
                )
        self.state.cooldown_duration_policy_evaluator = self.services._configured_cooldown_evaluator(self.context.params)
        if self.context.checkpoint_at_ts_ms is not None or self.context.resume_checkpoint is not None:
            from models.replay.runtime_checkpoint_io import validate_native_cooldown_checkpoint
            self.context.validate_native_cooldown_checkpoint = validate_native_cooldown_checkpoint
            self.context.validate_native_cooldown_checkpoint(self.state.cooldown_duration_policy_evaluator)
        self.state.cooldown_duration_fork_baseline_policy_enabled = bool(
            self.context.params.get("cooldown_duration_fork_baseline_policy_enabled", False)
        )
        if self.state.cooldown_duration_policy_evaluator is not None:
            self.state.required_methods = {"evaluate", "audit"}
            self.state.missing_methods = sorted(
                name
                for name in self.state.required_methods
                if not callable(
                    getattr(self.state.cooldown_duration_policy_evaluator, name, None)
                )
            )
            if self.state.missing_methods:
                raise ValueError(
                    "cooldown duration policy evaluator is incomplete: "
                    f"{self.state.missing_methods}"
                )
            if self.state.cooldown_v2_snapshot_emitter is None:
                raise ValueError(
                    "repeated cooldown policy requires the atomic cooldown-v2 emitter"
                )
        self.state.cooldown_duration_shared_prefix_executor = self.context.params.get(
            "cooldown_duration_shared_prefix_executor"
        )
        self.state.cooldown_duration_shared_prefix_exact_owner = bool(
            getattr(
                self.state.cooldown_duration_shared_prefix_executor,
                "exact_owner_baseline_policy_enabled",
                False,
            )
        )
        self.state.cooldown_duration_parent_stop_ts_ms = int(
            self.context.params.get("cooldown_duration_parent_stop_ts_ms", 0) or 0
        )
        self.state.cooldown_duration_parent_stop_triggered = False
        self.state.cooldown_duration_parent_stop_trigger_ts_ms = 0
        if self.state.cooldown_duration_shared_prefix_executor is not None:
            self.state.required_methods = {
                "dispatch",
                "finalize_simulation_result",
                "audit",
            }
            self.state.missing_methods = sorted(
                name
                for name in self.state.required_methods
                if not callable(
                    getattr(self.state.cooldown_duration_shared_prefix_executor, name, None)
                )
            )
            if self.state.missing_methods:
                raise ValueError(
                    "cooldown duration shared-prefix executor is incomplete: "
                    f"{self.state.missing_methods}"
                )
            if self.state.cooldown_v2_snapshot_emitter is None:
                raise ValueError(
                    "shared-prefix execution requires the atomic cooldown-v2 emitter"
                )
            if self.state.cooldown_duration_shared_prefix_exact_owner:
                self.state.expected_policy_sha256 = str(
                    getattr(
                        self.state.cooldown_duration_shared_prefix_executor,
                        "exact_owner_policy_sha256",
                        "",
                    )
                    or ""
                )
                if (
                    self.state.cooldown_duration_policy_evaluator is None
                    or str(
                        getattr(
                            self.state.cooldown_duration_policy_evaluator,
                            "policy_sha256",
                            "",
                        )
                    )
                    != self.state.expected_policy_sha256
                ):
                    raise ValueError(
                        "exact-owner shared-prefix execution lacks its bound policy"
                    )
        if self.state.cooldown_duration_parent_stop_ts_ms < 0:
            raise ValueError("cooldown_duration_parent_stop_ts_ms must be non-negative")
        if (
            self.state.cooldown_duration_parent_stop_ts_ms > 0
            and self.state.cooldown_duration_shared_prefix_executor is None
        ):
            raise ValueError(
                "cooldown duration parent stop requires the shared-prefix executor"
            )
        self.state.cooldown_duration_fork_enabled = bool(
            self.context.params.get("cooldown_duration_fork_enabled", False)
        )
        self.state.cooldown_duration_fork_action = str(
            self.context.params.get("cooldown_duration_fork_action", "") or ""
        ).strip().upper()
        self.state.cooldown_duration_fork_target_ordinal = int(
            self.context.params.get("cooldown_duration_fork_target_ordinal", 0) or 0
        )
        self.state.cooldown_duration_fork_target_ts_ms = int(
            self.context.params.get("cooldown_duration_fork_target_ts_ms", 0) or 0
        )
        self.state.cooldown_duration_fork_target_side = str(
            self.context.params.get("cooldown_duration_fork_target_side", "") or ""
        ).strip().upper()
        self.state.cooldown_duration_fork_target_order_id = int(
            self.context.params.get("cooldown_duration_fork_target_order_id", -1)
        )
        self.state.cooldown_duration_fork_target_inventory_lifecycle_id = int(
            self.context.params.get("cooldown_duration_fork_target_inventory_lifecycle_id", 0) or 0
        )
        self.state.cooldown_duration_fork_expected_baseline_ms = float(
            self.context.params.get("cooldown_duration_fork_expected_baseline_ms", 0.0) or 0.0
        )
        self.state.cooldown_duration_fork_fixed_ms = float(
            self.context.params.get("cooldown_duration_fork_fixed_ms", 0.0) or 0.0
        )
        self.state.cooldown_duration_fork_expected_owner_action = str(
            self.context.params.get("cooldown_duration_fork_expected_owner_action", "") or ""
        ).strip()
        self.state.cooldown_duration_fork_expected_owner_policy_sha256 = str(
            self.context.params.get("cooldown_duration_fork_expected_owner_policy_sha256", "")
            or ""
        ).strip()
        if self.state.cooldown_duration_fork_enabled:
            if self.state.cooldown_duration_fork_action not in {
                "CONTROL_85N",
                "FIXED_DURATION_MS",
            }:
                raise ValueError(
                    "cooldown duration fork action must be CONTROL_85N or "
                    "FIXED_DURATION_MS"
                )
            if (
                self.state.cooldown_duration_fork_target_ordinal <= 0
                or self.state.cooldown_duration_fork_target_ts_ms <= 0
                or self.state.cooldown_duration_fork_target_side not in {"BUY", "SELL"}
                or self.state.cooldown_duration_fork_target_order_id < 0
                or self.state.cooldown_duration_fork_target_inventory_lifecycle_id <= 0
                or not self.services.math.isfinite(self.state.cooldown_duration_fork_expected_baseline_ms)
                or self.state.cooldown_duration_fork_expected_baseline_ms <= 0.0
            ):
                raise ValueError("cooldown duration fork target identity is incomplete")
            if self.state.cooldown_duration_fork_action == "FIXED_DURATION_MS" and (
                not self.services.math.isfinite(self.state.cooldown_duration_fork_fixed_ms)
                or self.state.cooldown_duration_fork_fixed_ms <= 0.0
            ):
                raise ValueError("fixed cooldown duration must be positive and finite")
            if self.state.cooldown_duration_fork_baseline_policy_enabled:
                if self.state.cooldown_duration_policy_evaluator is None:
                    raise ValueError(
                        "one-shot exact-owner baseline requires a policy evaluator"
                    )
                if not self.state.cooldown_duration_fork_expected_owner_action:
                    raise ValueError(
                        "one-shot exact-owner baseline lacks the expected owner action"
                    )
                if not self.services.re.fullmatch(
                    r"[0-9a-f]{64}",
                    self.state.cooldown_duration_fork_expected_owner_policy_sha256,
                ):
                    raise ValueError(
                        "one-shot exact-owner baseline lacks a policy SHA256"
                    )
                if str(
                    getattr(self.state.cooldown_duration_policy_evaluator, "policy_sha256", "")
                ) != self.state.cooldown_duration_fork_expected_owner_policy_sha256:
                    raise ValueError(
                        "one-shot exact-owner baseline evaluator identity drifted"
                    )
            if self.state.ema_add_wait_fork_enabled or any(
                bool(self.context.params.get(name, False))
                for name in (
                    "buy_soft_widen_release_probe_enabled",
                    "conditional_p3_reach_gate_enabled",
                    "conditional_p3_reach_budget_policy_enabled",
                    "safe_add_rearm_randomized_enabled",
                    "state_conditioned_rearm_enabled",
                    "sell_add_skip_ope_enabled",
                    "state_conditioned_quote_policy_enabled",
                    "local_action_ope_enabled",
                    "queue_value_keep_cancel_enabled",
                    "variance_time_lineage_randomized_enabled",
                )
            ):
                raise ValueError(
                    "cooldown duration fork cannot share a replay with another "
                    "research action"
                )
        if (
            self.state.cooldown_duration_shared_prefix_executor is not None
            and self.state.cooldown_duration_fork_enabled
        ):
            raise ValueError(
                "shared-prefix executor cannot start from an explicit single-arm fork"
            )
        if self.state.cooldown_duration_policy_evaluator is not None and (
            (
                self.state.cooldown_duration_shared_prefix_executor is not None
                and not self.state.cooldown_duration_shared_prefix_exact_owner
            )
            or (
                self.state.cooldown_duration_fork_enabled
                and not self.state.cooldown_duration_fork_baseline_policy_enabled
            )
        ):
            raise ValueError(
                "repeated cooldown policy cannot share one-shot cooldown execution"
            )
        if self.state.cooldown_duration_fork_baseline_policy_enabled and (
            not self.state.cooldown_duration_fork_enabled
            or self.state.cooldown_duration_shared_prefix_executor is not None
        ):
            raise ValueError(
                "one-shot exact-owner baseline is only valid for an explicit fork"
            )
        if self.state.cooldown_duration_policy_evaluator is not None and (
            self.state.ema_add_wait_fork_enabled
            or any(
                bool(self.context.params.get(name, False))
                for name in (
                    "buy_soft_widen_release_probe_enabled",
                    "conditional_p3_reach_gate_enabled",
                    "conditional_p3_reach_budget_policy_enabled",
                    "safe_add_rearm_randomized_enabled",
                    "state_conditioned_rearm_enabled",
                    "sell_add_skip_ope_enabled",
                    "state_conditioned_quote_policy_enabled",
                    "local_action_ope_enabled",
                    "queue_value_keep_cancel_enabled",
                    "variance_time_lineage_randomized_enabled",
                )
            )
        ):
            raise ValueError(
                "repeated cooldown policy cannot share another research action"
            )
        if self.state.cooldown_duration_shared_prefix_executor is not None and (
            self.state.ema_add_wait_fork_enabled
            or any(
                bool(self.context.params.get(name, False))
                for name in (
                    "buy_soft_widen_release_probe_enabled",
                    "conditional_p3_reach_gate_enabled",
                    "conditional_p3_reach_budget_policy_enabled",
                    "safe_add_rearm_randomized_enabled",
                    "state_conditioned_rearm_enabled",
                    "sell_add_skip_ope_enabled",
                    "state_conditioned_quote_policy_enabled",
                    "local_action_ope_enabled",
                    "queue_value_keep_cancel_enabled",
                    "variance_time_lineage_randomized_enabled",
                )
            )
        ):
            raise ValueError(
                "shared-prefix cooldown execution cannot share another research action"
            )
        if (
            self.state.cooldown_duration_fork_enabled
            or self.state.cooldown_duration_shared_prefix_executor is not None
            or self.state.cooldown_duration_policy_evaluator is not None
        ):
            if bool(self.context.params.get("dynamic_fill_hazard_action_enabled", False)):
                raise ValueError("cooldown duration fork requires q90 action OFF")
            if bool(self.context.params.get("buy_fill_selection_live_enabled", False)):
                raise ValueError("cooldown duration fork requires BUY fill selection OFF")
        self.state.conditional_p3_reach_gate_enabled = bool(
            self.context.params.get("conditional_p3_reach_gate_enabled", False)
        )
        self.state.raw_p3_reach_gate_ts = self.context.params.get("_conditional_p3_reach_gate_ts_ms")
        self.state.raw_p3_reach_gate_status = self.context.params.get("_conditional_p3_reach_gate_status")
        self.state.p3_reach_gate_has_ts = self.state.raw_p3_reach_gate_ts is not None
        self.state.p3_reach_gate_has_status = self.state.raw_p3_reach_gate_status is not None
        if self.state.p3_reach_gate_has_ts != self.state.p3_reach_gate_has_status:
            raise ValueError(
                "conditional P3 reach gate timestamp and status arrays must be supplied together"
            )
        if self.state.conditional_p3_reach_gate_enabled != self.state.p3_reach_gate_has_ts:
            raise ValueError(
                "conditional P3 reach gate requires enabled=true and both frozen arrays, "
                "or enabled=false with no reach-gate payload"
            )
        if self.state.conditional_p3_reach_gate_enabled and any(
            self.context.params.get(name) is not None
            for name in (
                "_conditional_p3_ts_ms",
                "_conditional_p3_distance_touch_product_argmax",
                "_conditional_p3_touch_log_probability_distance_slope",
            )
        ):
            raise ValueError(
                "retired conditional P3 scalar adapter and action-bound reach gate are "
                "mutually exclusive"
            )
        self.state.conditional_p3_reach_budget_enabled = bool(
            self.context.params.get("conditional_p3_reach_budget_policy_enabled", False)
        )
        self.state.raw_p3_reach_budget_ts = self.context.params.get("_conditional_p3_reach_budget_ts_ms")
        self.state.raw_p3_reach_budget_selected_k = self.context.params.get(
            "_conditional_p3_reach_budget_selected_k"
        )
        self.state.p3_reach_budget_has_ts = self.state.raw_p3_reach_budget_ts is not None
        self.state.p3_reach_budget_has_selected_k = self.state.raw_p3_reach_budget_selected_k is not None
        if self.state.p3_reach_budget_has_ts != self.state.p3_reach_budget_has_selected_k:
            raise ValueError(
                "conditional P3 reach budget timestamp and selected-k arrays must be "
                "supplied together"
            )
        if self.state.conditional_p3_reach_budget_enabled != self.state.p3_reach_budget_has_ts:
            raise ValueError(
                "conditional P3 reach budget requires enabled=true and both frozen arrays, "
                "or enabled=false with no reach-budget payload"
            )
        if self.state.conditional_p3_reach_budget_enabled:
            raise NotImplementedError(
                "adaptive persistent conditional P3 policy is C++-only until the Python "
                "full-path implementation and event-lockstep parity are complete"
            )
        self.state.conditional_p3_reach_gate_outward_ticks = int(
            self.context.params.get("conditional_p3_reach_gate_outward_ticks", 16) or 16
        )
        self.state.conditional_p3_reach_gate_grid_min_ticks = int(
            self.context.params.get("conditional_p3_reach_gate_grid_min_ticks", 5) or 5
        )
        self.state.conditional_p3_reach_gate_buy_toxicity_threshold = float(
            self.context.params.get("conditional_p3_reach_gate_buy_toxicity_threshold", 2.0)
        )
        self.state.conditional_p3_reach_gate_sell_toxicity_threshold = float(
            self.context.params.get("conditional_p3_reach_gate_sell_toxicity_threshold", 2.0)
        )
        if self.state.conditional_p3_reach_gate_enabled:
            if self.state.conditional_p3_reach_gate_outward_ticks <= 0:
                raise ValueError("conditional P3 reach gate outward ticks must be positive")
            if self.state.conditional_p3_reach_gate_grid_min_ticks <= 0:
                raise ValueError("conditional P3 reach gate grid minimum must be positive")
            for loop_item in (
                (
                    self.state.conditional_p3_reach_gate_buy_toxicity_threshold,
                    "conditional_p3_reach_gate_buy_toxicity_threshold",
                ),
                (
                    self.state.conditional_p3_reach_gate_sell_toxicity_threshold,
                    "conditional_p3_reach_gate_sell_toxicity_threshold",
                ),
            ):
                self.state.value, self.state.name = loop_item
                if not self.services.math.isfinite(self.state.value) or not 0.0 <= self.state.value <= 1.0:
                    raise ValueError(f"{self.state.name} must lie in [0, 1]")
            if not bool(self.context.params.get("ml_enabled", True)):
                raise ValueError("conditional P3 reach gate requires ml_enabled=true")
        self.state.conditional_p3_reach_gate_eval_count = 0
        self.state.conditional_p3_reach_gate_toxicity_trigger_count = 0
        self.state.conditional_p3_reach_gate_supported_count = 0
        self.state.conditional_p3_reach_gate_pass_count = 0
        self.state.conditional_p3_reach_gate_price_change_count = 0
        self.state.conditional_p3_reach_gate_spread_cap_noop_count = 0
        self.state.conditional_p3_reach_gate_buy_price_change_count = 0
        self.state.conditional_p3_reach_gate_sell_price_change_count = 0
        if bool(self.context.params.get("cross_venue_fair_center_shift_enabled", False)) and any(
            bool(self.context.params.get(name, False))
            for name in (
                "dynamic_fill_hazard_action_enabled",
                "dynamic_fill_hazard_cpp_parity_enabled",
                "dynamic_fill_hazard_mechanics_telemetry_enabled",
            )
        ):
            raise ValueError(
                "fair-center randomized replay requires BUY q90 off in both arms"
            )
        self.services._validate_replayed_live_actions(self.context.params)
        self.state.ranked_toxicity_guard_binding_active = (
            self.context.ranked_toxicity_guard_binding is not None
        )
        self.state.ranked_toxicity_guard_binding_audit = None
        self.state.ranked_toxicity_guard_last_prediction_idx = -1
        self.state.ranked_toxicity_guard_policy_fingerprint = self.services.hashlib.sha256(
            self.services.json.dumps(
                {
                    str(key): repr(value)
                    for key, value in sorted(self.context.params.items(), key=lambda item: str(item[0]))
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        if self.state.ranked_toxicity_guard_binding_active:
            self.context.ranked_toxicity_guard_binding.validate_replay_start(
                params=self.context.params,
                ml_data=self.context.ml_data,
            )
        if bool(self.context.params.get("fixed_spread_probe_enabled", False)):
            raise ValueError(
                "scalar fixed-spread probe was retired after it violated paired "
                "distance monotonicity; use the C++ paired probe"
            )
        if bool(self.context.params.get("paired_fixed_spread_probe_enabled", False)):
            raise NotImplementedError(
                "paired fixed-spread replay is C++-only; the Python engine must not "
                "silently ignore its cohort contract"
            )
        self.state.replay_event_clock = str(self.context.params.get("replay_event_clock", "trade") or "trade").lower()
        self.state.replay_clock_interval_ms = int(self.context.params.get("replay_clock_interval_ms", 100) or 100)
        self.state.replace_terminal_continuation = bool(
            self.context.params.get("replace_terminal_continuation", False)
        )
        if self.state.replace_terminal_continuation and (
            self.state.replay_event_clock != "merged"
            or self.state.replay_clock_interval_ms != 100
            or (
                float(self.context.params.get("replay_main_loop_sleep_ms", 0) or 0) != 0.0
                and self.context.params.get("rest_gateway_timing_mode") != "sampled_async_fifo"
            )
        ):
            raise ValueError(
                "replace_terminal_continuation requires merged 100ms replay clock"
            )
        self.state.exchange_book_queue_mode = str(
            self.context.params.get("exchange_book_queue_mode", "disabled") or "disabled"
        ).strip().lower()
        if self.state.exchange_book_queue_mode not in {
            "disabled",
            "diagnostic",
            "strict",
        }:
            raise ValueError(
                "exchange_book_queue_mode must be disabled, diagnostic, or strict"
            )
        if (self.context.exchange_book_event_tape is not None) != (
            self.state.exchange_book_queue_mode != "disabled"
        ):
            raise ValueError(
                "exchange_book_event_tape and a non-disabled "
                "exchange_book_queue_mode are required together"
            )
        self.state.order_lifecycle_strict_native_only = bool(
            self.context.params.get("order_lifecycle_journal_v2_strict_native_only", False)
        )
        if self.state.order_lifecycle_strict_native_only and (
            self.state.exchange_book_queue_mode != "strict"
            or not bool(self.context.params.get("order_lifecycle_journal_v2_enabled", False))
        ):
            raise ValueError(
                "order_lifecycle_journal_v2_strict_native_only requires strict native "
                "exchange-book mode and journal-v2 emission"
            )
        if (
            self.context.exchange_book_event_tape is not None
            and self.state.replay_event_clock not in {"merged", "empirical"}
        ):
            raise ValueError(
                "native exchange-book replay requires replay_event_clock=merged "
                "or empirical"
            )
        if self.context.exchange_book_event_tape is not None:
            if self.context.trades_df.empty:
                raise ValueError(
                    "native exchange-book replay requires execution trades"
                )
        self.state.sync_degrade_events = self.services._sync_degrade_events_for_replay(self.context.params, self.context.trades_df)
        self.context.retained_clock_price = None
        if self.context.resume_input_batch and self.context.resume_checkpoint is not None:
            self.context.saved = self.context.resume_checkpoint["runtime"]
            self.context.input_start = self.context.params.get("replay_event_clock_start_ts_ms", int(self.context.trades_df.transact_time.iloc[0]))
            self.context.previous = int(self.services.np.searchsorted(self.context.saved.trade_ts, self.context.input_start, side="left")) - 1
            if self.context.previous >= 0:
                # Timer/BBO events carry the last execution price, not a new book
                # midpoint merely because their execution prefix was unloaded.
                self.context.retained_clock_price = float(self.context.saved.trade_price[self.context.previous])
            elif len(self.context.saved.trade_ts) and int(self.context.saved.trade_ts[0]) == int(self.context.input_start):
                # A dormant consumer can retain the same input prefix for several
                # batches. Its first timer already carries the prior execution
                # price; falling back to BBO mid here would rewrite that prefix.
                self.context.retained_clock_price = float(self.context.saved.trade_price[0])
        clock_builder = self.services.build_replay_event_clock
        self.context.prepared_event_identity = []
        if self.context._prepared_clock_owner is not None:
            clock_builder = self._prepared_event_clock
        self.context.trades_df, self.state.n_execution_trades = clock_builder(
            self.context.trades_df,
            mode=self.state.replay_event_clock,
            interval_ms=self.state.replay_clock_interval_ms,
            start_ts_ms=self.context.params.get("replay_event_clock_start_ts_ms"),
            end_ts_ms=self.context.params.get("replay_event_clock_end_ts_ms"),
            bbo_data=self.context.bbo_data,
            l2_data=self.context.l2_data,
            empirical_ts_ms=self.context.params.get("_empirical_requote_ts_ms"),
            empirical_action=self.context.params.get("_empirical_requote_action"),
            system_event_ts_ms=self.state.sync_degrade_events.timestamps_ms,
            system_event_code=self.state.sync_degrade_events.event_code,
            initial_clock_price=self.context.retained_clock_price,
            cold_flat_start=bool(self.context.params.get("signal_cold_start") or self.context.params.get("cold_flat_clock_start")),
        )
        self.services.require_formal_dense_variance_timeline(
            self.context.var_ts_ms,
            self.context.var_ssq,
            self.context.params,
            trades_df=self.context.trades_df,
        )
        # ── Unpack params ──
        self.state.eta_inventory = self.context.params["eta_inventory"]
        self.state.risk_per_order = self.context.params["risk_per_order"]
        self.state.execution_intensity_slope = self.context.params["execution_intensity_slope"]
        self.state.order_size = self.context.params["order_size"]
        self.state.max_inv = self.context.params["max_inventory"]
        self.state.max_daily_loss, self.state.max_position_value, self.state.emergency_close_dd = self.services.replay_hard_risk_limits(self.context.params)
        self.state.hard_risk_enabled = any(self.services.math.isfinite(value) for value in (
            self.state.max_daily_loss, self.state.max_position_value, self.state.emergency_close_dd,
        ))
        self.state.rq_s = self.context.params["requote_interval"]
        self.state.rq_ms = int(self.state.rq_s * 1000)
        self.state.requote_clock = str(self.context.params.get("requote_clock", "fixed") or "fixed").lower()
        if self.state.requote_clock not in {"fixed", "trade"}:
            raise ValueError(f"Unknown requote_clock={self.state.requote_clock!r}; expected fixed or trade")
        self.state.maker_fee = self.context.params["maker_fee"]
        self.state.taker_fee = self.context.params.get("taker_fee", 0.00036)
        self.state.eta = self.context.params.get("eta", 0.0)
        self.state.queue_base = self.context.params.get("queue_base", 5.0)
        self.state.queue_decay = self.context.params.get("queue_decay", 0.1)
        self.state.queue_ahead_mode = str(self.context.params.get("queue_ahead_mode", "exact_level") or "exact_level").lower()
        if self.state.queue_ahead_mode in ("exact", "exact_level"):
            self.state.queue_ahead_mode = "exact_level"
        elif self.state.queue_ahead_mode in ("provider", "provider_visible_level"):
            self.state.queue_ahead_mode = "provider_visible_level"
        else:
            raise ValueError(
                "queue_ahead_mode must be exact_level or provider_visible_level"
            )
        self.state.queue_price_tolerance = max(0.0, float(self.context.params.get("queue_price_tolerance", 0.051) or 0.0))
        self.state.maker_fill_prob = float(self.context.params.get("maker_fill_prob", 1.0))
        self.state.maker_fill_prob = max(0.0, min(1.0, self.state.maker_fill_prob))
        self.state.queue_ahead_base_mult = max(0.0, float(self.context.params.get("queue_ahead_base_mult", 1.0) or 1.0))
        self.state.queue_deplete_base_mult = max(0.0, float(self.context.params.get("queue_deplete_base_mult", 1.0) or 1.0))
        self.state.queue_l2_cancel_ahead_enabled = bool(
            self.context.params.get("queue_l2_cancel_ahead_enabled", False)
        )
        self.state.active_order_queue_mode = str(
            self.context.params.get("active_order_queue_mode", "disabled") or "disabled"
        ).strip().lower()
        if self.state.active_order_queue_mode not in {
            "disabled",
            "diagnostic",
            "strict_sparse",
        }:
            raise ValueError(
                "active_order_queue_mode must be disabled, diagnostic, or "
                "strict_sparse"
            )
        if self.state.active_order_queue_mode == "disabled" and self.context.active_order_queue_data is not None:
            raise ValueError(
                "active-order queue data supplied while active_order_queue_mode "
                "is disabled"
            )
        if self.state.active_order_queue_mode != "disabled" and self.context.active_order_queue_data is None:
            raise ValueError(
                f"active_order_queue_mode={self.state.active_order_queue_mode} requires data"
            )
        if (
            self.context.active_order_queue_data is not None
            and self.context.exchange_book_event_tape is not None
        ):
            raise ValueError(
                "trajectory-dependent sparse queue data cannot be combined with "
                "the strategy-independent native exchange-book scheduler"
            )
        if self.context.active_order_queue_data is not None and self.state.queue_l2_cancel_ahead_enabled:
            raise ValueError(
                "sparse queue seed v1 cannot be combined with dense top-N "
                "queue_l2_cancel_ahead updates"
            )
        if (
            self.context.exchange_book_event_tape is not None
            and self.state.queue_l2_cancel_ahead_enabled
        ):
            raise ValueError(
                "native exchange-book queue updates cannot be combined with "
                "sampled top-N queue_l2_cancel_ahead updates"
            )
        self.state.active_order_queue_lookup_count = 0
        self.state.active_order_queue_exact_count = 0
        self.state.active_order_queue_known_zero_count = 0
        self.state.active_order_queue_missing_count = 0
        self.state.active_order_queue_unusable_count = 0
        self.state.active_order_queue_missing_trace: list[dict[str, object]] = []
        self.state.active_order_queue_missing_trace_unbounded = bool(
            self.context.params.get("trace_active_order_queue_missing_unbounded", False)
        )
        self.state.active_order_queue_missing_trace_max = max(
            0,
            int(self.context.params.get("trace_active_order_queue_missing_max", 100_000) or 0),
        )
        self.state.local_order_lifecycle_event_profile = str(
            self.context.params.get("local_order_lifecycle_event_profile", "full") or "full"
        )
        if self.state.local_order_lifecycle_event_profile not in {
            "full",
            "placement_start_stop",
        }:
            raise ValueError(
                "local_order_lifecycle_event_profile must be full or "
                "placement_start_stop"
            )
        self.state.exchange_book_scheduler = (
            self.services.HistoricalExchangeBookScheduler(
                self.context.exchange_book_event_tape,
                strict_sequence=self.state.exchange_book_queue_mode == "strict",
                strict_after_ns=int(
                    getattr(self.context.exchange_book_event_tape, "day_start_ns", 0)
                ),
                allow_delta_bootstrap=self.state.exchange_book_queue_mode != "strict",
                track_mid_changes=(
                    int(self.context.params.get("trace_local_order_lifecycle_max", 0) or 0) > 0
                    and self.state.local_order_lifecycle_event_profile == "full"
                ),
            )
            if self.context.exchange_book_event_tape is not None
            else None
        )
        self.state.dynamic_fill_hazard_shadow_enabled = bool(
            self.context.params.get("dynamic_fill_hazard_shadow_enabled", False)
        )
        self.state.dynamic_fill_hazard_action_enabled = bool(
            self.context.params.get("dynamic_fill_hazard_action_enabled", False)
        )
        self.state.dynamic_fill_hazard_action_application = str(
            self.context.params.get("dynamic_fill_hazard_action_application", "apply")
            or "apply"
        ).strip().lower()
        if self.state.dynamic_fill_hazard_action_application not in {"apply", "shadow"}:
            raise ValueError(
                "dynamic_fill_hazard_action_application must be apply or shadow"
            )
        self.state.dynamic_fill_hazard_visibility_clock_mode = str(
            self.context.params.get(
                "dynamic_fill_hazard_visibility_clock_mode",
                "legacy_mixed",
            )
            or "legacy_mixed"
        ).strip().lower()
        if self.state.dynamic_fill_hazard_visibility_clock_mode not in {
            "legacy_mixed",
            "provider_receive_sensitivity",
            "aws_profile",
        }:
            raise ValueError(
                "dynamic_fill_hazard_visibility_clock_mode must be "
                "legacy_mixed, provider_receive_sensitivity, or aws_profile"
            )
        self.state.dynamic_fill_hazard_visibility_scheduler = None
        self.state.dynamic_fill_hazard_visibility_profile = None
        self.state.dynamic_fill_hazard_visibility_profile_identity: dict[str, self.services.Any] = {
            "mode": self.state.dynamic_fill_hazard_visibility_clock_mode,
            "truth_clock": "exchange_time",
            "visibility_clock": (
                "legacy_mixed_exchange_decision_provider_receive_features"
                if self.state.dynamic_fill_hazard_visibility_clock_mode == "legacy_mixed"
                else "feature_ready_time"
            ),
        }
        self.state.dynamic_fill_hazard_visibility_seed = int(
            self.context.params.get("dynamic_fill_hazard_visibility_seed", 20260731) or 20260731
        )
        self.state.dynamic_fill_hazard_provider_feature_latency_ns = int(
            round(
                max(
                    0.0,
                    float(
                        self.context.params.get(
                            "dynamic_fill_hazard_provider_feature_latency_ms",
                            0.0,
                        )
                        or 0.0
                    ),
                )
                * 1_000_000.0
            )
        )
        self.state.dynamic_fill_hazard_provider_trade_delay_ns = int(
            round(
                max(
                    0.0,
                    float(
                        self.context.params.get(
                            "dynamic_fill_hazard_provider_trade_delay_ms",
                            0.0,
                        )
                        or 0.0
                    ),
                )
                * 1_000_000.0
            )
        )
        self.state.dynamic_fill_hazard_profile_market_id = str(
            self.context.params.get(
                "dynamic_fill_hazard_visibility_profile_market_id",
                "binance:perp:BTCUSDC",
            )
            or "binance:perp:BTCUSDC"
        )
        self.state.dynamic_fill_hazard_profile_transport = str(
            self.context.params.get(
                "dynamic_fill_hazard_visibility_profile_transport",
                "websocket",
            )
            or "websocket"
        ).strip().lower()
        self.state.dynamic_fill_hazard_profile_trade_market_id = str(
            self.context.params.get(
                "dynamic_fill_hazard_visibility_profile_trade_market_id",
                self.state.dynamic_fill_hazard_profile_market_id,
            )
            or self.state.dynamic_fill_hazard_profile_market_id
        )
        self.state.dynamic_fill_hazard_profile_mode = str(
            self.context.params.get(
                "dynamic_fill_hazard_visibility_profile_mode",
                "profile_empirical",
            )
            or "profile_empirical"
        ).strip().lower()
        self.state.dynamic_fill_hazard_bundle = None
        self.state.dynamic_fill_hazard_policy = None
        self.state.dynamic_fill_hazard_runtime = None
        self.state.dynamic_fill_hazard_cpp_parity_enabled = bool(
            self.context.params.get("dynamic_fill_hazard_cpp_parity_enabled", False)
        )
        self.state.dynamic_fill_hazard_cpp_parity_strict = bool(
            self.context.params.get("dynamic_fill_hazard_cpp_parity_strict", True)
        )
        self.state.dynamic_fill_hazard_cpp_runtime = None
        self.state.dynamic_fill_hazard_cpp_identity: dict[str, self.services.Any] = {}
        if self.state.dynamic_fill_hazard_action_enabled:
            if self.state.exchange_book_scheduler is None or self.state.exchange_book_queue_mode != "strict":
                raise ValueError(
                    "BUY q90 replay requires the strict native exchange-book scheduler"
                )
            from strategy.dynamic_fill_hazard_model import (
                DynamicFillHazardActionPolicy,
                DynamicFillHazardBundle,
                DynamicFillHazardShadowRuntime,
                load_cpp_dynamic_fill_hazard_runtime,
            )
            self.context.DynamicFillHazardActionPolicy = DynamicFillHazardActionPolicy
            self.context.DynamicFillHazardBundle = DynamicFillHazardBundle
            self.context.DynamicFillHazardShadowRuntime = DynamicFillHazardShadowRuntime
            self.context.load_cpp_dynamic_fill_hazard_runtime = load_cpp_dynamic_fill_hazard_runtime

            if self.state.dynamic_fill_hazard_visibility_clock_mode == "aws_profile":
                from research.system_engineering.audit.market_data_latency import (
                    MarketDataLatencySimulator,
                )
                self.context.MarketDataLatencySimulator = MarketDataLatencySimulator

                self.state.profile_path = self.services.Path(
                    str(
                        self.context.params.get(
                            "dynamic_fill_hazard_visibility_profile_path",
                            "",
                        )
                        or ""
                    )
                ).expanduser()
                if not str(self.state.profile_path):
                    raise ValueError(
                        "aws_profile q90 visibility requires "
                        "dynamic_fill_hazard_visibility_profile_path"
                    )
                if not self.state.profile_path.is_absolute():
                    self.state.profile_path = self.services.ROOT / self.state.profile_path
                if not self.state.profile_path.is_file():
                    raise FileNotFoundError(
                        f"q90 visibility profile not found: {self.state.profile_path}"
                    )
                self.state.profile_sha256 = self.services.hashlib.sha256(self.state.profile_path.read_bytes()).hexdigest()
                self.state.dynamic_fill_hazard_visibility_profile = (
                    self.context.MarketDataLatencySimulator.load(self.state.profile_path)
                )
                self.state.expected_profile_id = str(
                    self.context.params.get(
                        "dynamic_fill_hazard_visibility_profile_id",
                        "",
                    )
                    or ""
                ).strip()
                if not self.state.expected_profile_id:
                    raise ValueError(
                        "aws_profile q90 visibility requires an expected profile id"
                    )
                if (
                    self.state.dynamic_fill_hazard_visibility_profile.profile_id
                    != self.state.expected_profile_id
                ):
                    raise ValueError(
                        "q90 visibility profile id mismatch: "
                        f"expected={self.state.expected_profile_id} actual="
                        f"{self.state.dynamic_fill_hazard_visibility_profile.profile_id}"
                    )
                self.state.profile_payload = self.state.dynamic_fill_hazard_visibility_profile.profile
                self.state.dynamic_fill_hazard_visibility_profile_identity.update(
                    {
                        "path": str(self.state.profile_path.resolve()),
                        "sha256": self.state.profile_sha256,
                        "profile_id": self.state.expected_profile_id,
                        "profile_mode": self.state.dynamic_fill_hazard_profile_mode,
                        "profile_market_id": self.state.dynamic_fill_hazard_profile_market_id,
                        "profile_trade_market_id": (
                            self.state.dynamic_fill_hazard_profile_trade_market_id
                        ),
                        "profile_transport": self.state.dynamic_fill_hazard_profile_transport,
                        "environment": dict(self.state.profile_payload.get("environment", {})),
                        "provider_clock_authority": False,
                        "aws_profile_transport_sensitivity": True,
                        "feature_source_clock": "exchange_event_time",
                        "trade_latency_proxy": bool(
                            self.state.dynamic_fill_hazard_profile_trade_market_id
                            != self.state.dynamic_fill_hazard_profile_market_id
                        ),
                    }
                )
                for loop_item in (
                    (self.state.dynamic_fill_hazard_profile_market_id, "book"),
                    (self.state.dynamic_fill_hazard_profile_trade_market_id, "trade"),
                ):
                    self.state.market_id, self.state.event_type = loop_item
                    self.state.dynamic_fill_hazard_visibility_profile.delay_ms(
                        {
                            "market_id": self.state.market_id,
                            "event_type": self.state.event_type,
                            "transport": self.state.dynamic_fill_hazard_profile_transport,
                        },
                        mode=self.state.dynamic_fill_hazard_profile_mode,
                        rng=self.services.random.Random(0),
                    )
            elif (
                self.state.dynamic_fill_hazard_visibility_clock_mode
                == "provider_receive_sensitivity"
            ):
                self.state.dynamic_fill_hazard_visibility_profile_identity.update(
                    {
                        "provider_clock_authority": False,
                        "aws_profile_transport_sensitivity": False,
                        "feature_source_clock": "provider_receive_time",
                        "provider_feature_latency_ns": int(
                            self.state.dynamic_fill_hazard_provider_feature_latency_ns
                        ),
                        "trade_visibility_source": (
                            "fixed_exchange_offset_sensitivity"
                        ),
                        "provider_trade_delay_ns": int(
                            self.state.dynamic_fill_hazard_provider_trade_delay_ns
                        ),
                    }
                )

            if self.state.dynamic_fill_hazard_visibility_clock_mode != "legacy_mixed":
                self.state.dynamic_fill_hazard_visibility_scheduler = (
                    self.services.HistoricalExchangeBookVisibilityScheduler(
                        strict_sequence=True,
                        strict_after_ns=int(
                            getattr(self.context.exchange_book_event_tape, "day_start_ns", 0)
                        ),
                        allow_delta_bootstrap=False,
                    )
                )

            self.state.model_path = self.services.Path(
                str(self.context.params["dynamic_fill_hazard_shadow_model_path"])
            ).expanduser()
            self.state.policy_path = self.services.Path(
                str(self.context.params["dynamic_fill_hazard_action_policy_path"])
            ).expanduser()
            if not self.state.model_path.is_absolute():
                self.state.model_path = self.services.ROOT / self.state.model_path
            if not self.state.policy_path.is_absolute():
                self.state.policy_path = self.services.ROOT / self.state.policy_path
            self.state.dynamic_fill_hazard_bundle = self.context.DynamicFillHazardBundle.load(
                self.state.model_path,
                shadow_sides=("BUY",),
            )
            self.state.dynamic_fill_hazard_policy = self.context.DynamicFillHazardActionPolicy.load(
                self.state.policy_path,
                model_bundle=self.state.dynamic_fill_hazard_bundle,
            )
            self.state.dynamic_fill_hazard_runtime = self.context.DynamicFillHazardShadowRuntime(
                self.state.dynamic_fill_hazard_bundle,
                tick_size=float(self.state.TICK),
                lot_size=float(self.state.LOT_SIZE),
                exposure_ms=float(
                    self.context.params.get("dynamic_fill_hazard_shadow_exposure_ms", 100.0)
                    or 100.0
                ),
                price_jump_ticks=float(
                    self.context.params.get(
                        "dynamic_fill_hazard_shadow_price_jump_ticks",
                        1.0,
                    )
                    or 1.0
                ),
                evaluation_interval_ms=float(
                    self.state.dynamic_fill_hazard_policy.evaluation_interval_ms
                ),
            )
            if self.state.dynamic_fill_hazard_cpp_parity_enabled:
                self.state.dynamic_fill_hazard_cpp_runtime = (
                    self.context.load_cpp_dynamic_fill_hazard_runtime(
                        model_path=self.state.model_path,
                        policy_path=self.state.policy_path,
                        tick_size=float(self.state.TICK),
                        lot_size=float(self.state.LOT_SIZE),
                        exposure_ms=float(
                            self.context.params.get(
                                "dynamic_fill_hazard_shadow_exposure_ms", 100.0
                            )
                            or 100.0
                        ),
                        price_jump_ticks=float(
                            self.context.params.get(
                                "dynamic_fill_hazard_shadow_price_jump_ticks",
                                1.0,
                            )
                            or 1.0
                        ),
                        strict_sequence=True,
                        strict_after_ns=int(
                            getattr(self.context.exchange_book_event_tape, "day_start_ns", 0)
                        ),
                    )
                )
                self.state.dynamic_fill_hazard_cpp_identity = (
                    self.state.dynamic_fill_hazard_cpp_runtime.identity()
                )
        elif self.state.dynamic_fill_hazard_cpp_parity_enabled:
            raise ValueError(
                "dynamic fill-hazard C++ parity requires the Python q90 action path"
            )
        if (
            self.state.dynamic_fill_hazard_action_application == "shadow"
            and self.state.dynamic_fill_hazard_cpp_runtime is not None
        ):
            raise ValueError(
                "q90 shadow application mode cannot assert stateful C++ action "
                "parity; run a separate corrected apply path for C++ parity"
            )



        self.state.dynamic_fill_hazard_paths: dict[str, self.services.ReplayOrderDepthPath] = {}
        self.state.dynamic_fill_hazard_lifecycles: dict[
            str, self.services.QuantityWeightedOrderLifecycle
        ] = {}
        self.state.dynamic_fill_hazard_lifecycle_journal: list[dict[str, object]] = []
        self.state.dynamic_fill_hazard_hold: dict[str, self.services.Any] | None = None
        self.state.dynamic_fill_hazard_eval_count = 0
        self.state.dynamic_fill_hazard_valid_eval_count = 0
        self.state.dynamic_fill_hazard_invalid_eval_count = 0
        self.state.dynamic_fill_hazard_keep_count = 0
        self.state.dynamic_fill_hazard_cancel_request_count = 0
        self.state.dynamic_fill_hazard_cancel_ack_count = 0
        self.state.dynamic_fill_hazard_pre_ack_fill_count = 0
        self.state.dynamic_fill_hazard_recovery_count = 0
        self.state.dynamic_fill_hazard_reentry_count = 0
        self.state.dynamic_fill_hazard_post_cancel_recovery_count = 0
        self.state.dynamic_fill_hazard_prospective_eval_count = 0
        self.state.dynamic_fill_hazard_prospective_valid_count = 0
        self.state.dynamic_fill_hazard_prospective_invalid_count = 0
        self.state.dynamic_fill_hazard_post_terminal_hazard_reuse_count = 0
        self.state.dynamic_fill_hazard_post_terminal_cursor_reuse_count = 0
        self.state.dynamic_fill_hazard_unsupported_terminal_route_count = 0
        self.state.dynamic_fill_hazard_blocked_quote_count = 0
        self.state.dynamic_fill_hazard_retain_invalid_count = 0
        self.state.dynamic_fill_hazard_shadow_cancel_signal_count = 0
        self.state.dynamic_fill_hazard_mechanics_telemetry_enabled = bool(
            self.context.params.get("dynamic_fill_hazard_mechanics_telemetry_enabled", False)
        )
        self.state.dynamic_fill_hazard_eval_by_role: dict[str, dict[str, int]] = {}
        self.state.dynamic_fill_hazard_action_by_role: dict[str, dict[str, int]] = {}
        self.state.dynamic_fill_hazard_valid_scores: list[float] = []
        self.state.dynamic_fill_hazard_cpp_book_event_count = 0
        self.state.dynamic_fill_hazard_cpp_activation_count = 0
        self.state.dynamic_fill_hazard_cpp_evaluation_count = 0
        self.state.dynamic_fill_hazard_cpp_lifecycle_count = 0
        self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count = 0
        self.state.dynamic_fill_hazard_cpp_mismatch_count = 0
        self.state.dynamic_fill_hazard_cpp_mismatch_by_stage: dict[str, int] = {}
        self.state.dynamic_fill_hazard_cpp_mismatch_trace: list[dict[str, self.services.Any]] = []
        self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_synced_ids: set[str] = set()
        self.state.dynamic_fill_hazard_invalid_reason_counts: dict[str, int] = {}
        self.state.dynamic_fill_hazard_visible_trade_heap: list[
            tuple[int, int, int, float, float]
        ] = []
        self.state.dynamic_fill_hazard_visible_trade_ordinal = 0
        self.state.dynamic_fill_hazard_visible_trade_enqueued_count = 0
        self.state.dynamic_fill_hazard_visible_trade_delivered_count = 0
        self.state.dynamic_fill_hazard_visible_book_trade_tie_count = 0
        self.state.dynamic_fill_hazard_provider_receive_missing_count = 0
        self.state.dynamic_fill_hazard_cpp_mismatch_trace_max = max(
            1,
            int(self.context.params.get("dynamic_fill_hazard_cpp_parity_trace_max", 25) or 25),
        )

        self.state.dynamic_fill_hazard_lifecycle_emitters = {}




        self.state.exchange_book_queue_lookup_count = 0
        self.state.exchange_book_queue_exact_count = 0
        self.state.exchange_book_queue_known_zero_count = 0
        self.state.exchange_book_queue_missing_count = 0
        self.state.exchange_book_queue_invalidated_order_count = 0
        self.state.exchange_book_queue_cancel_ahead_event_count = 0
        self.state.exchange_book_queue_cancel_ahead_qty = 0.0
        self.state.exchange_book_queue_ambiguous_event_count = 0
        self.state.exchange_book_cancel_trade_ambiguous_order_count = 0
        self.state.exchange_book_cancel_book_ambiguous_order_count = 0
        self.state.exchange_book_predecision_invalid_inventory_lifecycles: set[int] = set()
        self.state.exchange_book_queue_missing_trace: list[dict[str, object]] = []
        self.state.exchange_book_queue_ambiguity_trace: list[dict[str, object]] = []
        self.state.exchange_book_queue_ambiguity_trace_max = max(
            0,
            int(self.context.params.get("exchange_book_queue_ambiguity_trace_max", 0) or 0),
        )

        if self.state.queue_l2_cancel_ahead_enabled:
            if self.state.queue_ahead_mode != "exact_level":
                raise ValueError(
                    "queue_l2_cancel_ahead_enabled requires queue_ahead_mode=exact_level"
                )
            if self.state.replay_event_clock not in {"merged", "empirical"}:
                raise ValueError(
                    "queue_l2_cancel_ahead_enabled requires replay_event_clock=merged or empirical"
                )
        self.state.queue_l2_cancel_ahead_event_count = 0
        self.state.queue_l2_cancel_ahead_bid_event_count = 0
        self.state.queue_l2_cancel_ahead_ask_event_count = 0
        self.state.queue_l2_cancel_ahead_qty = 0.0
        self.state.queue_ahead_buy_exposure_mult = max(
            0.0,
            float(self.context.params.get("queue_ahead_buy_exposure_mult", 1.0) or 1.0),
        )
        self.state.queue_ahead_buy_reducing_mult = max(
            0.0,
            float(self.context.params.get("queue_ahead_buy_reducing_mult", 1.0) or 1.0),
        )
        self.state.queue_ahead_sell_exposure_mult = max(
            0.0,
            float(self.context.params.get("queue_ahead_sell_exposure_mult", 1.0) or 1.0),
        )
        self.state.queue_ahead_sell_reducing_mult = max(
            0.0,
            float(self.context.params.get("queue_ahead_sell_reducing_mult", 1.0) or 1.0),
        )
        self.state.queue_calibration = self.context.params.get("_queue_calibration")
        self.state.trade_ts_seed = self.context.trades_df["transact_time"].to_numpy(dtype=self.services.np.int64, copy=False)
        if self.context.prepared_event_identity:
            self.state.trade_ts_seed = self.context._prepared_clock_owner.clock_arrays(self.context.prepared_event_identity[0])[0]
        if self.state.queue_calibration:
            self.state.queue_base_arr, self.state.queue_decay_arr, self.state.buy_fill_prob_arr, self.state.sell_fill_prob_arr = self.services.build_daily_queue_arrays(
                self.state.trade_ts_seed,
                self.state.queue_calibration,
                default_queue_base=self.state.queue_base,
                default_queue_decay=self.state.queue_decay,
                default_buy_fill_prob=self.context.params.get("buy_fill_prob", 1.0),
                default_sell_fill_prob=self.context.params.get("sell_fill_prob", 1.0),
            )
        else:
            self.state.queue_base_arr = self.services.np.full(len(self.state.trade_ts_seed), self.state.queue_base, dtype=self.services.np.float64)
            self.state.queue_decay_arr = self.services.np.full(len(self.state.trade_ts_seed), self.state.queue_decay, dtype=self.services.np.float64)
            self.state.buy_fill_prob_arr = self.services.np.full(len(self.state.trade_ts_seed), self.context.params.get("buy_fill_prob", 1.0), dtype=self.services.np.float64)
            self.state.sell_fill_prob_arr = self.services.np.full(len(self.state.trade_ts_seed), self.context.params.get("sell_fill_prob", 1.0), dtype=self.services.np.float64)
        self.state.queue_regime_calibration_enabled = bool(self.context.params.get("queue_regime_calibration_enabled", False))
        # 这些 lookup 只在显式 --queue-regime-calibration 时启用。
        # 默认回测仍保持原始队列口径，避免日常 sweep 意外混入 live-fit calibration。
        self.state.queue_regime_lookup = (
            self.services.build_queue_regime_lookup(self.state.queue_calibration)
            if self.state.queue_calibration and self.state.queue_regime_calibration_enabled
            else {}
        )
        self.state.queue_deplete_lookup = (
            self.services.build_queue_deplete_lookup(self.state.queue_calibration)
            if self.state.queue_calibration and self.state.queue_regime_calibration_enabled
            else {}
        )
        self.state.queue_mo_lookup = (
            self.services.build_queue_mo_lookup(self.state.queue_calibration)
            if self.state.queue_calibration and self.state.queue_regime_calibration_enabled
            else {}
        )
        self.state.rng_seed = int(self.context.params.get("rng_seed", 42))
        # Always available: side/day fill probabilities are multiplied by
        # maker_fill_prob in the tick fill gate.
        self.state.fill_rng = self.services.np.random.default_rng(self.state.rng_seed)
        # Executable passive null: this is a replay arm, not an opportunity-table
        # resample. It retains queue/lifecycle/inventory mechanics while breaking
        # the deterministic association between quote side geometry and market time.
        self.state.random_passive_enabled = bool(self.context.params.get("random_passive_enabled", False))
        self.state.random_passive_seed = int(self.context.params.get("random_passive_seed", self.state.rng_seed + 10_003))
        self.state.random_passive_side_mirror_prob = min(
            1.0, max(0.0, float(self.context.params.get("random_passive_side_mirror_prob", 0.5) or 0.0))
        )
        self.state.random_passive_timing_jitter_fraction = min(
            0.95,
            max(0.0, float(self.context.params.get("random_passive_timing_jitter_fraction", 0.35) or 0.0)),
        )
        self.state.random_passive_preserve_inventory_skew = bool(
            self.context.params.get("random_passive_preserve_inventory_skew", True)
        )
        self.state.trace_fills_max = int(self.context.params.get("trace_fills_max", 0) or 0)
        self.state.trace_window_s = float(self.context.params.get("trace_fills_window_s", 10.0) or 10.0)
        self.state.trace_window_ms = max(1000, int(self.state.trace_window_s * 1000))
        self.state.trace_fills = [] if self.state.trace_fills_max > 0 else None
        self.state.trace_quotes_max = int(self.context.params.get("trace_quotes_max", 0) or 0)
        self.state.trace_orders = [] if self.state.trace_quotes_max > 0 else None
        self.state.trace_decisions_max = int(self.context.params.get("trace_decisions_max", 0) or 0)
        self.state.trace_decisions = [] if self.state.trace_decisions_max > 0 else None
        self.state.trace_attempt_counts = {"fills": 0, "order_outcomes": 0, "routed_decisions": 0,
                                           "excluded_route_calls": 0}
        self.state.trace_external_market_release = bool(
            self.context.params.get("trace_external_market_release", False)
        )
        if self.state.trace_external_market_release and self.state.trace_decisions is None:
            raise ValueError(
                "external market release tracing requires decision tracing"
            )
        self.state.pending_external_market_release_rows: list[
            tuple[dict[str, self.services.Any], dict[str, int]]
        ] = []
        self.state.trace_first_add_decision_to_terminal_max = int(
            self.context.params.get("trace_first_add_decision_to_terminal_max", 0) or 0
        )
        self.state.trace_first_add_decision_to_terminal = (
            [] if self.state.trace_first_add_decision_to_terminal_max > 0 else None
        )
        self.state.first_add_decision_open_records: dict[int, dict[str, self.services.Any]] = {}
        self.state.first_add_decision_recorded_inventory_lifecycles: set[int] = set()
        self.state.first_add_decision_trace_audit: dict[str, self.services.Any] = {}
        self.state.trace_first_opener_decision_to_terminal_max = int(
            self.context.params.get("trace_first_opener_decision_to_terminal_max", 0) or 0
        )
        self.state.first_opener_trace_schema_version = str(
            self.context.params.get(
                "first_opener_trace_schema_version",
                "first_opener_decision_to_terminal_trace.v1",
            )
            or "first_opener_decision_to_terminal_trace.v1"
        )
        if self.state.first_opener_trace_schema_version not in {
            "first_opener_decision_to_terminal_trace.v1",
            "first_opener_decision_to_terminal_trace.v2",
        }:
            raise ValueError("unsupported first-opener trace schema")
        self.state.trace_first_opener_decision_to_terminal = (
            [] if self.state.trace_first_opener_decision_to_terminal_max > 0 else None
        )
        self.state.first_opener_decision_open_records: dict[int, dict[str, self.services.Any]] = {}
        self.state.first_opener_decision_recorded_inventory_lifecycles: set[int] = set()
        self.state.first_opener_decision_unsupported_inventory_lifecycles: list[dict[str, self.services.Any]] = []
        self.state.first_opener_decision_trace_audit: dict[str, self.services.Any] = {}
        self.state.decision_trace_profile = str(
            self.context.params.get("decision_trace_profile", "full") or "full"
        ).strip().lower()
        if self.state.decision_trace_profile not in {"full", "mechanics_only"}:
            raise ValueError(
                "decision_trace_profile must be full or mechanics_only"
            )
        self.state.trace_inventory_lifecycle_repair_max = int(self.context.params.get("trace_inventory_lifecycle_repair_max", 0) or 0)
        self.state.trace_inventory_lifecycle_repair = [] if self.state.trace_inventory_lifecycle_repair_max > 0 else None
        self.state.trace_post_fill_quote_response_max = int(
            self.context.params.get("trace_post_fill_quote_response_max", 0) or 0
        )
        self.state.trace_post_fill_quote_response = (
            [] if self.state.trace_post_fill_quote_response_max > 0 else None
        )
        self.state.trace_safe_add_rearm_max = int(
            self.context.params.get("trace_safe_add_rearm_max", 0) or 0
        )
        self.state.trace_safe_add_rearm = (
            [] if self.state.trace_safe_add_rearm_max > 0 else None
        )
        self.state.raw_rearm_bins = self.context.params.get(
            "safe_add_rearm_probe_bins_s",
            (5.0, 10.0, 20.0, 40.0, 60.0, 85.0),
        )
        if isinstance(self.state.raw_rearm_bins, str):
            self.state.raw_rearm_bins = [
                token.strip() for token in self.state.raw_rearm_bins.split(",") if token.strip()
            ]
        self.state.safe_add_rearm_probe_bins_ms = tuple(
            sorted(
                {
                    max(0, int(round(float(value) * 1000.0)))
                    for value in (self.state.raw_rearm_bins or ())
                    if float(value) >= 0.0
                }
            )
        )
        self.state.safe_add_rearm_probe_lifetime_ms = max(
            1,
            int(
                round(
                    float(self.context.params.get("safe_add_rearm_probe_lifetime_s", 5.0) or 5.0)
                    * 1000.0
                )
            ),
        )
        self.state.safe_add_rearm_rng = self.services.np.random.default_rng(self.state.rng_seed + 70_127)
        self.state.safe_add_rearm_latency_rng = self.services.np.random.default_rng(self.state.rng_seed + 70_129)
        self.state.safe_add_rearm_orders: list[dict] = []
        self.state.safe_add_rearm_emitted: set[tuple[str, int, int]] = set()
        self.state.safe_add_rearm_next_id = 0
        self.state.local_action_ope_enabled = bool(self.context.params.get("local_action_ope_enabled", False))
        self.state.trace_local_action_ope_max = int(
            self.context.params.get("trace_local_action_ope_max", 0) or 0
        )
        if self.state.local_action_ope_enabled and self.state.trace_local_action_ope_max <= 0:
            raise ValueError(
                "local_action_ope_enabled requires trace_local_action_ope_max > 0"
            )
        self.state.local_action_probabilities = self.services.normalize_action_probabilities(
            self.context.params.get("local_action_ope_probabilities"),
            baseline_probability=float(
                self.context.params.get("local_action_ope_baseline_probability", 0.90) or 0.90
            ),
            allow_zero_support=bool(
                self.context.params.get("local_action_ope_allow_zero_support", False)
            ),
        )
        self.state.raw_local_action_sides = self.context.params.get("local_action_ope_sides", ("BUY", "SELL"))
        if isinstance(self.state.raw_local_action_sides, str):
            self.state.raw_local_action_sides = [
                value.strip()
                for value in self.state.raw_local_action_sides.split(",")
                if value.strip()
            ]
        self.state.local_action_sides = {
            str(value).strip().upper() for value in self.state.raw_local_action_sides
        }
        if not self.state.local_action_sides or self.state.local_action_sides - {"BUY", "SELL"}:
            raise ValueError("local_action_ope_sides must be a non-empty BUY/SELL subset")
        self.state.local_action_seed = int(self.context.params.get("local_action_ope_seed", self.state.rng_seed + 90_131))
        self.state.local_action_rng = self.services.np.random.default_rng(self.state.local_action_seed)
        self.state.trace_local_action_ope = [] if self.state.local_action_ope_enabled else None
        self.state.local_action_intervened_inventory_lifecycles: set[int] = set()
        self.state.local_action_open_records: dict[int, dict] = {}
        self.state.local_action_assignment_counts = {action: 0 for action in self.services.LOCAL_ACTIONS}
        self.state.sell_add_skip_ope_enabled = bool(
            self.context.params.get("sell_add_skip_ope_enabled", False)
        )
        self.state.trace_sell_add_skip_ope_max = int(
            self.context.params.get("trace_sell_add_skip_ope_max", 0) or 0
        )
        if self.state.sell_add_skip_ope_enabled and self.state.trace_sell_add_skip_ope_max <= 0:
            raise ValueError(
                "sell_add_skip_ope_enabled requires "
                "trace_sell_add_skip_ope_max > 0"
            )
        self.state.sell_add_skip_mode = str(
            self.context.params.get("sell_add_skip_ope_mode", "one_cycle") or "one_cycle"
        ).strip().lower()
        if self.state.sell_add_skip_mode not in {"one_cycle", "until_flat"}:
            raise ValueError(
                "sell_add_skip_ope_mode must be one_cycle or until_flat"
            )
        self.state.sell_add_skip_actions = (
            self.services.INVENTORY_LIFECYCLE_STOP_ADD_ACTIONS
            if self.state.sell_add_skip_mode == "until_flat"
            else self.services.SELL_ADD_SKIP_ACTIONS
        )
        self.state.sell_add_skip_probabilities = (
            self.services.normalize_inventory_lifecycle_stop_add_probabilities(
                self.context.params.get("sell_add_skip_ope_probabilities")
            )
            if self.state.sell_add_skip_mode == "until_flat"
            else self.services.normalize_sell_add_skip_probabilities(
                self.context.params.get("sell_add_skip_ope_probabilities")
            )
        )
        self.state.sell_add_skip_seed = int(
            self.context.params.get("sell_add_skip_ope_seed", self.state.rng_seed + 91_037)
        )
        self.state.sell_add_skip_rng = self.services.np.random.default_rng(self.state.sell_add_skip_seed)
        self.state.raw_sell_add_skip_sides = self.context.params.get(
            "sell_add_skip_ope_sides", ("SELL",)
        )
        if isinstance(self.state.raw_sell_add_skip_sides, str):
            self.state.raw_sell_add_skip_sides = [
                value.strip()
                for value in self.state.raw_sell_add_skip_sides.split(",")
                if value.strip()
            ]
        self.state.sell_add_skip_sides = {
            str(value).strip().upper() for value in self.state.raw_sell_add_skip_sides
        }
        if not self.state.sell_add_skip_sides or self.state.sell_add_skip_sides - {"BUY", "SELL"}:
            raise ValueError(
                "sell_add_skip_ope_sides must be a non-empty BUY/SELL subset"
            )
        self.state.sell_add_skip_family_id = str(
            self.context.params.get(
                "sell_add_skip_ope_family_id",
                "sell_add_repair_trend_skip_causal_v4_v1",
            )
        )
        self.state.sell_add_skip_min_followup_s = max(
            0.0,
            float(self.context.params.get("sell_add_skip_min_followup_s", 1_800.0) or 0.0),
        )
        self.state.trace_sell_add_skip_ope = [] if self.state.sell_add_skip_ope_enabled else None
        self.state.sell_add_skip_intervened_inventory_lifecycles: set[int] = set()
        self.state.sell_add_skip_open_records: dict[int, dict] = {}
        self.state.sell_add_skip_assignment_counts = {
            action: 0 for action in self.state.sell_add_skip_actions
        }
        self.state.state_conditioned_rearm_enabled = bool(
            self.context.params.get("state_conditioned_rearm_enabled", False)
        )
        self.state.trace_state_conditioned_rearm_max = int(
            self.context.params.get("trace_state_conditioned_rearm_max", 0) or 0
        )
        if self.state.state_conditioned_rearm_enabled and self.state.trace_state_conditioned_rearm_max <= 0:
            raise ValueError(
                "state_conditioned_rearm_enabled requires "
                "trace_state_conditioned_rearm_max > 0"
            )
        self.state.raw_state_conditioned_rearm_sides = self.context.params.get(
            "state_conditioned_rearm_sides", ("SELL",)
        )
        if isinstance(self.state.raw_state_conditioned_rearm_sides, str):
            self.state.raw_state_conditioned_rearm_sides = (
                self.state.raw_state_conditioned_rearm_sides.split(",")
            )
        self.state.state_conditioned_rearm_sides = {
            str(side).strip().upper()
            for side in self.state.raw_state_conditioned_rearm_sides
            if str(side).strip()
        }
        if (
            not self.state.state_conditioned_rearm_sides
            or self.state.state_conditioned_rearm_sides - {"BUY", "SELL"}
        ):
            raise ValueError(
                "state_conditioned_rearm_sides must be a non-empty BUY/SELL subset"
            )
        self.state.state_conditioned_rearm_probabilities = (
            self.services.normalize_state_conditioned_rearm_probabilities(
                self.context.params.get("state_conditioned_rearm_probabilities")
            )
        )
        self.state.state_conditioned_rearm_seed = int(
            self.context.params.get("state_conditioned_rearm_seed", self.state.rng_seed + 93_047)
        )
        self.state.state_conditioned_rearm_rng = self.services.np.random.default_rng(
            self.state.state_conditioned_rearm_seed
        )
        self.state.state_conditioned_rearm_min_elapsed_ms = max(
            0,
            int(
                round(
                    float(
                        self.context.params.get("state_conditioned_rearm_min_elapsed_s", 85.0)
                        or 0.0
                    )
                    * 1_000.0
                )
            ),
        )
        self.state.state_conditioned_rearm_min_followup_s = max(
            0.0,
            float(
                self.context.params.get("state_conditioned_rearm_min_followup_s", 1_800.0)
                or 0.0
            ),
        )
        self.state.state_conditioned_rearm_policy_version = str(
            self.context.params.get(
                "state_conditioned_rearm_policy_version",
                "adverse_conjunction_v1",
            )
            or "adverse_conjunction_v1"
        ).strip()
        if self.state.state_conditioned_rearm_policy_version not in {
            "adverse_conjunction_v1",
            "composite_recovery_v2",
        }:
            raise ValueError(
                "unsupported state_conditioned_rearm_policy_version: "
                f"{self.state.state_conditioned_rearm_policy_version}"
            )
        self.state.state_conditioned_rearm_spec = self.services.StateConditionedRearmSpec(
            entry_min_current_adverse_bps=float(
                self.context.params.get("state_conditioned_rearm_entry_min_current_adverse_bps", 2.0)
            ),
            entry_min_adverse_flow_1s=float(
                self.context.params.get("state_conditioned_rearm_entry_min_adverse_flow_1s", 0.0)
            ),
            entry_min_adverse_flow_5s=float(
                self.context.params.get("state_conditioned_rearm_entry_min_adverse_flow_5s", 0.0)
            ),
            entry_min_adverse_flow_path=float(
                self.context.params.get("state_conditioned_rearm_entry_min_adverse_flow_path", 0.0)
            ),
            entry_max_refill_recovery_ratio=float(
                self.context.params.get("state_conditioned_rearm_entry_max_refill_recovery_ratio", 0.75)
            ),
            entry_max_refill_current_vs_start_ratio=float(
                self.context.params.get(
                    "state_conditioned_rearm_entry_max_refill_current_vs_start_ratio",
                    1.0,
                )
            ),
            entry_max_price_recovery_ratio=float(
                self.context.params.get("state_conditioned_rearm_entry_max_price_recovery_ratio", 0.50)
            ),
            entry_max_microprice_recovery_ratio=float(
                self.context.params.get(
                    "state_conditioned_rearm_entry_max_microprice_recovery_ratio",
                    0.50,
                )
            ),
            max_book_age_ms=float(
                self.context.params.get("state_conditioned_rearm_max_book_age_ms", 2_000.0)
            ),
            exit_max_current_adverse_bps=float(
                self.context.params.get("state_conditioned_rearm_exit_max_current_adverse_bps", 0.50)
            ),
            exit_max_adverse_flow_5s=float(
                self.context.params.get("state_conditioned_rearm_exit_max_adverse_flow_5s", 0.0)
            ),
            exit_max_adverse_flow_path=float(
                self.context.params.get("state_conditioned_rearm_exit_max_adverse_flow_path", 0.0)
            ),
            exit_min_refill_recovery_ratio=float(
                self.context.params.get("state_conditioned_rearm_exit_min_refill_recovery_ratio", 1.0)
            ),
            exit_min_refill_current_vs_start_ratio=float(
                self.context.params.get(
                    "state_conditioned_rearm_exit_min_refill_current_vs_start_ratio",
                    1.0,
                )
            ),
            exit_min_price_recovery_ratio=float(
                self.context.params.get("state_conditioned_rearm_exit_min_price_recovery_ratio", 0.75)
            ),
            exit_min_microprice_recovery_ratio=float(
                self.context.params.get(
                    "state_conditioned_rearm_exit_min_microprice_recovery_ratio",
                    0.75,
                )
            ),
        )
        self.state.recovery_event_rearm_spec = self.services.RecoveryEventSpec(
            score_threshold=float(
                self.context.params.get("recovery_event_rearm_score_threshold", 0.10)
            ),
            max_book_age_ms=float(
                self.context.params.get("recovery_event_rearm_max_book_age_ms", 2_000.0)
            ),
            adverse_flow_reference_floor=float(
                self.context.params.get("recovery_event_rearm_flow_reference_floor", 0.05)
            ),
            component_epsilon=float(
                self.context.params.get("recovery_event_rearm_component_epsilon", 1e-6)
            ),
        )
        self.state.trace_state_conditioned_rearm = (
            [] if self.state.state_conditioned_rearm_enabled else None
        )
        self.state.state_conditioned_rearm_intervened_inventory_lifecycles: set[int] = set()
        self.state.state_conditioned_rearm_open_records: dict[int, dict] = {}
        self.state.state_conditioned_rearm_active_episode_by_side: dict[str, int] = {}
        self.state.state_conditioned_rearm_assignment_counts = {
            action: 0 for action in self.services.STATE_CONDITIONED_REARM_ACTIONS
        }
        self.state.trace_local_order_value_max = int(
            self.context.params.get("trace_local_order_value_max", 0) or 0
        )
        self.state.local_order_value_fill_horizon_ms = max(
            1,
            int(
                self.context.params.get("local_order_value_fill_horizon_ms", 1_000)
                or 1_000
            ),
        )
        self.state.local_order_value_price_jump_ticks = max(
            0.0,
            float(
                self.context.params.get("local_order_value_price_jump_ticks", 1.0)
                or 0.0
            ),
        )
        self.state.trace_local_order_value = (
            [] if self.state.trace_local_order_value_max > 0 else None
        )
        self.state.local_order_value_open_records: dict[int, dict] = {}
        self.state.local_order_value_observed_order_ids: set[int] = set()
        self.state.trace_local_order_lifecycle_max = int(
            self.context.params.get("trace_local_order_lifecycle_max", 0) or 0
        )
        self.state.local_order_lifecycle = self.services.OrderLifecycleRecorder(
            symbol=self.services.SYMBOL,
            lot_size=self.state.LOT_SIZE,
            tick_size=self.state.TICK,
            price_jump_ticks=self.state.local_order_value_price_jump_ticks,
            max_orders=self.state.trace_local_order_lifecycle_max,
            event_profile=self.state.local_order_lifecycle_event_profile,
            risk_snapshot_edges_ms=self.context.params.get(
                "local_order_lifecycle_snapshot_edges_ms",
                (
                    0,
                    100,
                    200,
                    300,
                    500,
                    750,
                    1_000,
                    1_500,
                    2_500,
                    4_000,
                    6_000,
                    10_000,
                    20_000,
                    40_000,
                    85_000,
                ),
            ),
        )
        self.state.journal_adapter_type = (
            self.services.StrictNativeOrderLifecycleV2ReplayAdapter
            if self.state.order_lifecycle_strict_native_only
            else self.services.OrderLifecycleV2ReplayAdapter
        )
        self.state.order_lifecycle_journal_v2_adapter = (
            self.state.journal_adapter_type.from_replay_params(
                self.context.params,
                symbol=self.services.SYMBOL,
            )
        )
        self.state.order_lifecycle_journal_v2_health: dict[str, self.services.Any] | None = None








        self.state.trace_local_order_lifecycle: list[dict[str, self.services.Any]] = []
        self.state.local_order_lifecycle_mid_change_cursor = 0
        self.state.queue_value_keep_cancel_enabled = bool(
            self.context.params.get("queue_value_keep_cancel_enabled", False)
        )
        self.state.trace_queue_value_keep_cancel_max = int(
            self.context.params.get("trace_queue_value_keep_cancel_max", 0) or 0
        )
        if (
            self.state.queue_value_keep_cancel_enabled
            and self.state.trace_queue_value_keep_cancel_max <= 0
        ):
            raise ValueError(
                "queue_value_keep_cancel_enabled requires "
                "trace_queue_value_keep_cancel_max > 0"
            )
        if self.state.queue_value_keep_cancel_enabled and self.state.trace_local_order_value is not None:
            raise ValueError(
                "baseline local-order observation and randomized queue-value "
                "intervention must run in separate replays"
            )
        self.state.queue_value_action_family = str(
            self.context.params.get(
                "queue_value_action_family",
                "queue_value_keep_cancel",
            )
            or "queue_value_keep_cancel"
        ).strip().lower()
        if self.state.queue_value_action_family in {
            "queue_value_keep_cancel",
            "queue_value_net_keep_cancel",
        }:
            self.state.queue_value_actions = self.services.QUEUE_VALUE_KEEP_CANCEL_ACTIONS
            self.state.queue_value_probabilities = self.services.normalize_queue_value_probabilities(
                self.context.params.get("queue_value_keep_cancel_probabilities")
            )
            self.state.queue_value_action_chooser = self.services.choose_queue_value_action
        elif self.state.queue_value_action_family == "queue_value_cancel_reenter":
            self.state.queue_value_actions = self.services.QUEUE_VALUE_CANCEL_REENTER_ACTIONS
            self.state.queue_value_probabilities = (
                self.services.normalize_queue_value_cancel_reenter_probabilities(
                    self.context.params.get("queue_value_keep_cancel_probabilities")
                )
            )
            self.state.queue_value_action_chooser = self.services.choose_queue_value_cancel_reenter_action
        else:
            raise ValueError(
                "queue_value_action_family must be queue_value_keep_cancel, "
                "queue_value_net_keep_cancel, or queue_value_cancel_reenter"
            )
        self.state.raw_queue_value_sides = self.context.params.get(
            "queue_value_keep_cancel_sides",
            ("BUY", "SELL"),
        )
        if isinstance(self.state.raw_queue_value_sides, str):
            self.state.raw_queue_value_sides = self.state.raw_queue_value_sides.split(",")
        self.state.queue_value_sides = {
            str(side).strip().upper()
            for side in self.state.raw_queue_value_sides
            if str(side).strip()
        }
        if not self.state.queue_value_sides or self.state.queue_value_sides - {"BUY", "SELL"}:
            raise ValueError(
                "queue_value_keep_cancel_sides must be a non-empty BUY/SELL subset"
            )
        self.state.queue_value_seed = int(
            self.context.params.get("queue_value_keep_cancel_seed", self.state.rng_seed + 92_041)
        )
        self.state.queue_value_rng = self.services.np.random.default_rng(self.state.queue_value_seed)
        self.state.queue_value_fill_horizon_ms = max(
            1,
            int(self.context.params.get("queue_value_fill_horizon_ms", 1_000) or 1_000),
        )
        self.state.queue_value_price_jump_ticks = max(
            0.0,
            float(self.context.params.get("queue_value_price_jump_ticks", 1.0) or 0.0),
        )
        self.state.queue_value_evaluators: dict[
            str,
            self.services.QueueValueStateEvaluator | self.services.QueueValueNetEvaluator,
        ] = {}
        self.state.queue_value_runtime: dict[str, self.services.QueueReactiveRuntime] = {}
        self.state.queue_value_runtime_event_source: dict[str, str] = {}
        self.state.queue_value_input_scope = ""


        if self.state.queue_value_keep_cancel_enabled:
            self.state.raw_competing_bundle_path = str(
                self.context.params.get("queue_value_competing_risk_bundle_path", "") or ""
            ).strip()
            self.state.raw_bundle_path = str(
                self.context.params.get("queue_value_model_bundle_path", "") or ""
            ).strip()
            self.state.raw_queue_path = str(
                self.context.params.get("queue_value_hawkes_artifact_path", "") or ""
            ).strip()
            self.state.raw_microprice_path = str(
                self.context.params.get("queue_value_microprice_artifact_path", "") or ""
            ).strip()
            if self.state.raw_competing_bundle_path and (
                self.state.raw_bundle_path or self.state.raw_queue_path or self.state.raw_microprice_path
            ):
                raise ValueError(
                    "the competing-risk queue-value bundle cannot be combined "
                    "with legacy queue-value artifacts"
                )
            if (
                self.state.queue_value_action_family == "queue_value_net_keep_cancel"
                and not self.state.raw_competing_bundle_path
            ):
                raise ValueError(
                    "queue_value_net_keep_cancel requires a frozen "
                    "queue_value_competing_risk_bundle_path"
                )
            if (
                self.state.queue_value_action_family != "queue_value_net_keep_cancel"
                and self.state.raw_competing_bundle_path
            ):
                raise ValueError(
                    "a competing-risk bundle is valid only for "
                    "queue_value_net_keep_cancel"
                )
            if not self.state.raw_competing_bundle_path and not self.state.raw_bundle_path and (
                not self.state.raw_queue_path or not self.state.raw_microprice_path
            ):
                raise ValueError(
                    "queue-value randomization requires a frozen side-specific "
                    "bundle or pooled Hawkes and empirical-microprice artifacts"
                )
            if self.state.raw_competing_bundle_path:
                self.state.bundle_path = self.services.Path(self.state.raw_competing_bundle_path).expanduser()
                if not self.state.bundle_path.is_absolute():
                    self.state.bundle_path = self.services.ROOT / self.state.bundle_path
                self.state.net_bundle = self.services.CompetingRiskBundle.load(self.state.bundle_path)
                self.state.expected_evidence_hash = str(
                    self.context.params.get(
                        "queue_value_expected_evidence_split_sha256",
                        "",
                    )
                    or ""
                )
                if (
                    self.state.expected_evidence_hash
                    and self.state.net_bundle.evidence_split_sha256
                    != self.state.expected_evidence_hash
                ):
                    raise ValueError(
                        "competing-risk queue-value bundle evidence-split "
                        "identity differs from the formal replay"
                    )
                if (
                    bool(
                        self.context.params.get(
                            "queue_value_require_calibration_passed",
                            True,
                        )
                    )
                    and not self.state.net_bundle.calibration_passed
                ):
                    raise ValueError(
                        "competing-risk queue-value bundle did not pass its "
                        "frozen calibration gate"
                    )
                if self.state.net_bundle.input_scope != "local_only":
                    raise ValueError(
                        "queue_value_net_keep_cancel is local M0 only; external "
                        "M1 remains disabled until the M0 gate passes"
                    )
                self.state.queue_value_input_scope = self.state.net_bundle.input_scope
                for loop_item in ("BUY", "SELL"):
                    self.state.side = loop_item
                    self.state.side_model = self.state.net_bundle.side_artifact(self.state.side)
                    self.state.queue_value_evaluators[self.state.side] = self.state.net_bundle.evaluator(self.state.side)
                    self.state.queue_value_runtime[self.state.side] = self.services.QueueReactiveRuntime(
                        self.state.side_model.runtime_queue_artifact
                    )
                    self.state.queue_value_runtime_event_source[self.state.side] = (
                        self._queue_runtime_event_source(
                            self.state.side_model.runtime_queue_artifact
                        )
                    )
            elif self.state.raw_bundle_path:
                self.state.bundle_path = self.services.Path(self.state.raw_bundle_path).expanduser()
                if not self.state.bundle_path.is_absolute():
                    self.state.bundle_path = self.services.ROOT / self.state.bundle_path
                self.state.bundle = self.services.QueueValueModelBundle.load(self.state.bundle_path)
                self.state.expected_evidence_hash = str(
                    self.context.params.get(
                        "queue_value_expected_evidence_split_sha256",
                        "",
                    )
                    or ""
                )
                self.state.expected_source_hash = str(
                    self.context.params.get(
                        "queue_value_expected_source_manifest_sha256",
                        "",
                    )
                    or ""
                )
                if (
                    self.state.expected_evidence_hash
                    and self.state.bundle.evidence_split_sha256
                    != self.state.expected_evidence_hash
                ):
                    raise ValueError(
                        "queue-value bundle evidence-split identity differs "
                        "from the formal replay"
                    )
                if (
                    self.state.expected_source_hash
                    and self.state.bundle.source_manifest_sha256 != self.state.expected_source_hash
                ):
                    raise ValueError(
                        "queue-value bundle source-universe identity differs "
                        "from the formal replay"
                    )
                if (
                    bool(
                        self.context.params.get(
                            "queue_value_require_calibration_passed",
                            True,
                        )
                    )
                    and not self.state.bundle.calibration_passed
                ):
                    raise ValueError(
                        "queue-value model bundle did not pass its frozen "
                        "Development calibration gate"
                    )
                if self.state.bundle.input_scope != "local_only":
                    raise ValueError(
                        "queue_value_keep_cancel_v1 is local M0 only; external "
                        "M1 artifacts remain disabled until the M0 gate passes"
                    )
                self.state.queue_value_input_scope = self.state.bundle.input_scope
                for loop_item in ("BUY", "SELL"):
                    self.state.side = loop_item
                    self.state.side_model = self.state.bundle.side_model(self.state.side)
                    if (
                        self.state.side_model.microprice_artifact.horizon_ms
                        != self.state.queue_value_fill_horizon_ms
                    ):
                        raise ValueError(
                            "queue_value_fill_horizon_ms must match each frozen "
                            "empirical microprice horizon"
                        )
                    self.state.queue_value_evaluators[self.state.side] = self.state.bundle.evaluator(self.state.side)
                    self.state.queue_value_runtime[self.state.side] = self.services.QueueReactiveRuntime(
                        self.state.side_model.queue_artifact
                    )
                    self.state.queue_value_runtime_event_source[self.state.side] = (
                        self._queue_runtime_event_source(
                            self.state.side_model.queue_artifact
                        )
                    )
            else:
                self.state.queue_path = self.services.Path(self.state.raw_queue_path).expanduser()
                self.state.microprice_path = self.services.Path(self.state.raw_microprice_path).expanduser()
                if not self.state.queue_path.is_absolute():
                    self.state.queue_path = self.services.ROOT / self.state.queue_path
                if not self.state.microprice_path.is_absolute():
                    self.state.microprice_path = self.services.ROOT / self.state.microprice_path
                self.state.queue_artifact = self.services.QueueReactiveHawkesArtifact.load(self.state.queue_path)
                self.state.microprice_artifact = self.services.EmpiricalMicropriceArtifact.load(
                    self.state.microprice_path
                )
                if (
                    self.state.queue_artifact.input_scope != "local_only"
                    or self.state.microprice_artifact.input_scope != "local_only"
                ):
                    raise ValueError(
                        "queue_value_keep_cancel_v1 is local M0 only; external M1 "
                        "artifacts remain diagnostic until the M0 gate passes"
                    )
                self.state.queue_value_input_scope = self.state.queue_artifact.input_scope
                self.state.pooled_evaluator = self.services.QueueValueStateEvaluator(
                    self.state.queue_artifact,
                    self.state.microprice_artifact,
                    config=self.services.QueueValueStateConfig(
                        entry_expected_ticks=float(
                            self.context.params.get(
                                "queue_value_entry_expected_ticks", -0.10
                            )
                        ),
                        exit_expected_ticks=float(
                            self.context.params.get("queue_value_exit_expected_ticks", 0.0)
                        ),
                        entry_adverse_probability=float(
                            self.context.params.get(
                                "queue_value_entry_adverse_probability", 0.52
                            )
                        ),
                        exit_adverse_probability=float(
                            self.context.params.get(
                                "queue_value_exit_adverse_probability", 0.50
                            )
                        ),
                        entry_flow_ratio=float(
                            self.context.params.get("queue_value_entry_flow_ratio", 1.0)
                        ),
                        exit_flow_ratio=float(
                            self.context.params.get("queue_value_exit_flow_ratio", 0.9)
                        ),
                    ),
                )
                self.state.queue_value_evaluators = {
                    "BUY": self.state.pooled_evaluator,
                    "SELL": self.state.pooled_evaluator,
                }
                self.state.queue_value_runtime = {
                    "BUY": self.services.QueueReactiveRuntime(self.state.queue_artifact),
                    "SELL": self.services.QueueReactiveRuntime(self.state.queue_artifact),
                }
                self.state.runtime_event_source = self._queue_runtime_event_source(
                    self.state.queue_artifact
                )
                self.state.queue_value_runtime_event_source = {
                    "BUY": self.state.runtime_event_source,
                    "SELL": self.state.runtime_event_source,
                }
                if (
                    self.state.microprice_artifact.horizon_ms
                    != self.state.queue_value_fill_horizon_ms
                ):
                    raise ValueError(
                        "queue_value_fill_horizon_ms must match the frozen "
                        "empirical microprice horizon"
                    )
        self.state.trace_queue_value_keep_cancel = (
            [] if self.state.queue_value_keep_cancel_enabled else None
        )
        self.state.queue_value_intervened_inventory_lifecycles: set[int] = set()
        self.state.queue_value_open_records: dict[int, dict] = {}
        self.state.queue_value_active_episode_by_side: dict[str, int] = {}
        self.state.queue_value_runtime_event_counts = {
            side: {
                "adverse_market_order": 0,
                "cancel": 0,
                "refill": 0,
            }
            for side in ("BUY", "SELL")
        }
        self.state.queue_value_assignment_counts = {
            action: 0 for action in self.state.queue_value_actions
        }
        self.state.state_conditioned_policy_mode = str(
            self.context.params.get("state_conditioned_policy_mode", "disabled") or "disabled"
        ).strip().lower()
        if self.state.state_conditioned_policy_mode not in {"disabled", "shadow", "active"}:
            raise ValueError(
                "state_conditioned_policy_mode must be disabled, shadow, or active"
            )
        self.state.trace_state_conditioned_policy_max = int(
            self.context.params.get("trace_state_conditioned_policy_max", 0) or 0
        )
        self.state.state_conditioned_policy = None
        if self.state.state_conditioned_policy_mode != "disabled":
            self.state.raw_policy_path = str(
                self.context.params.get("state_conditioned_policy_model_path", "") or ""
            ).strip()
            if not self.state.raw_policy_path:
                raise ValueError(
                    "state-conditioned policy mode requires a model artifact path"
                )
            if self.state.trace_state_conditioned_policy_max <= 0:
                raise ValueError(
                    "state-conditioned policy mode requires "
                    "trace_state_conditioned_policy_max > 0"
                )
            self.state.policy_path = self.services.Path(self.state.raw_policy_path).expanduser()
            if not self.state.policy_path.is_absolute():
                self.state.policy_path = self.services.ROOT / self.state.policy_path
            self.state.state_conditioned_policy = self.services.StateConditionedQuotePolicy.load(
                self.state.policy_path,
                mode=self.state.state_conditioned_policy_mode,
            )
        self.state.trace_state_conditioned_policy = (
            [] if self.state.state_conditioned_policy is not None else None
        )
        self.state.state_conditioned_intervened_inventory_lifecycles: set[int] = set()
        self.state.state_conditioned_open_records: dict[int, dict] = {}
        self.state.state_conditioned_candidate_counts = {action: 0 for action in self.services.LOCAL_ACTIONS}
        self.state.state_conditioned_executed_counts = {action: 0 for action in self.services.LOCAL_ACTIONS}
        if self.state.state_conditioned_policy is not None and self.state.local_action_ope_enabled:
            raise ValueError(
                "state-conditioned policy and local-action randomization cannot "
                "share one replay"
            )
        if self.state.sell_add_skip_ope_enabled and (
            self.state.local_action_ope_enabled or self.state.state_conditioned_policy is not None
        ):
            raise ValueError(
                "add-skip randomization cannot share a replay with local quote "
                "actions or the state-conditioned policy"
            )
        if self.state.queue_value_keep_cancel_enabled and (
            self.state.local_action_ope_enabled
            or self.state.sell_add_skip_ope_enabled
            or self.state.state_conditioned_policy is not None
        ):
            raise ValueError(
                "queue-value keep/cancel randomization cannot share a replay with "
                "another randomized or state-conditioned action family"
            )
        self.state.safe_add_rearm_randomized_enabled = bool(
            self.context.params.get("safe_add_rearm_randomized_enabled", False)
        )
        self.state.trace_safe_add_rearm_intervention_max = int(
            self.context.params.get("trace_safe_add_rearm_intervention_max", 0) or 0
        )
        if (
            self.state.safe_add_rearm_randomized_enabled
            and self.state.trace_safe_add_rearm_intervention_max <= 0
        ):
            raise ValueError(
                "safe_add_rearm_randomized_enabled requires "
                "trace_safe_add_rearm_intervention_max > 0"
            )
        if self.state.safe_add_rearm_randomized_enabled and self.state.local_action_ope_enabled:
            raise ValueError(
                "safe-add rearm and local-action randomization cannot share one replay"
            )
        if self.state.safe_add_rearm_randomized_enabled and self.state.sell_add_skip_ope_enabled:
            raise ValueError(
                "safe-add rearm and SELL add-skip randomization cannot share one replay"
            )
        if self.state.safe_add_rearm_randomized_enabled and self.state.state_conditioned_policy is not None:
            raise ValueError(
                "safe-add rearm randomization and state-conditioned policy cannot "
                "share one replay"
            )
        if self.state.safe_add_rearm_randomized_enabled and self.state.queue_value_keep_cancel_enabled:
            raise ValueError(
                "safe-add rearm and queue-value keep/cancel randomization cannot "
                "share one replay"
            )
        self.state.safe_add_rearm_randomized_probabilities = (
            self.services.normalize_safe_add_rearm_probabilities(
                self.context.params.get("safe_add_rearm_randomized_probabilities"),
                baseline_probability=float(
                    self.context.params.get("safe_add_rearm_randomized_baseline_probability", 0.80)
                    or 0.80
                ),
            )
        )
        self.state.safe_add_rearm_randomized_elapsed_ms = max(
            0,
            int(
                round(
                    float(self.context.params.get("safe_add_rearm_randomized_elapsed_s", 20.0) or 0.0)
                    * 1000.0
                )
            ),
        )
        self.state.safe_add_rearm_randomized_seed = int(
            self.context.params.get("safe_add_rearm_randomized_seed", self.state.rng_seed + 100_151)
        )
        self.state.safe_add_rearm_randomized_rng = self.services.np.random.default_rng(
            self.state.safe_add_rearm_randomized_seed
        )
        self.state.trace_safe_add_rearm_intervention = (
            [] if self.state.safe_add_rearm_randomized_enabled else None
        )
        self.state.safe_add_rearm_intervened_inventory_lifecycles: set[int] = set()
        self.state.safe_add_rearm_open_records: dict[int, dict] = {}
        self.state.safe_add_rearm_assignment_counts = {
            action: 0 for action in self.services.SAFE_ADD_REARM_ACTIONS
        }
        if self.state.state_conditioned_rearm_enabled and (
            self.state.local_action_ope_enabled
            or self.state.sell_add_skip_ope_enabled
            or self.state.queue_value_keep_cancel_enabled
            or self.state.safe_add_rearm_randomized_enabled
            or self.state.state_conditioned_policy is not None
        ):
            raise ValueError(
                "state-conditioned post-cooldown rearm must run alone as its own "
                "randomized action family"
            )
        self.state.variance_time_lineage_randomized_enabled = bool(
            self.context.params.get("variance_time_lineage_randomized_enabled", False)
        )
        self.state.trace_variance_time_lineage_max = int(
            self.context.params.get("trace_variance_time_lineage_max", 0) or 0
        )
        if (
            self.state.variance_time_lineage_randomized_enabled
            and self.state.trace_variance_time_lineage_max <= 0
        ):
            raise ValueError(
                "variance_time_lineage_randomized_enabled requires "
                "trace_variance_time_lineage_max > 0"
            )
        if self.state.variance_time_lineage_randomized_enabled and (
            self.state.local_action_ope_enabled
            or self.state.sell_add_skip_ope_enabled
            or self.state.queue_value_keep_cancel_enabled
            or self.state.safe_add_rearm_randomized_enabled
            or self.state.state_conditioned_rearm_enabled
            or self.state.state_conditioned_policy is not None
        ):
            raise ValueError(
                "variance-time lineage randomization must run alone as its own "
                "action family"
            )
        self.state.variance_time_lineage_probabilities = (
            self.services.normalize_lineage_randomization_probabilities(
                self.context.params.get("variance_time_lineage_probabilities")
            )
        )
        if self.state.variance_time_lineage_randomized_enabled and any(
            not self.services.math.isclose(probability, 0.5, abs_tol=1e-12)
            for probability in self.state.variance_time_lineage_probabilities.values()
        ):
            raise ValueError(
                "volatility_time_add_rearm_randomized_replay_v1 requires exact "
                "0.5/0.5 behavior propensity"
            )
        self.state.variance_time_lineage_seed = int(
            self.context.params.get("variance_time_lineage_seed", self.state.rng_seed + 104_729)
        )
        self.state.lineage_outcome_contract_version = str(
            self.context.params.get("lineage_randomized_outcome_contract_version", "v1") or "v1"
        ).strip().lower()
        if self.state.lineage_outcome_contract_version not in {"v1", "v2"}:
            raise ValueError(
                "lineage_randomized_outcome_contract_version must be v1 or v2"
            )
        self.state.variance_time_lineage_rng = self.services.np.random.default_rng(
            self.state.variance_time_lineage_seed
        )
        self.state.lineage_randomized_family_id = str(
            self.context.params.get("lineage_randomized_family_id", "") or ""
        ).strip()
        if (
            self.state.variance_time_lineage_randomized_enabled
            and self.state.lineage_outcome_contract_version == "v2"
            and not self.state.lineage_randomized_family_id
        ):
            raise ValueError("lineage outcome contract v2 requires a frozen family id")
        self.state.variance_time_lineage_stratified_randomizer = (
            self.services.StratifiedBernoulliLineageRandomizer(
                self.state.variance_time_lineage_seed,
                self.state.lineage_randomized_family_id,
            )
            if self.state.variance_time_lineage_randomized_enabled
            and self.state.lineage_outcome_contract_version == "v2"
            else None
        )
        self.state.variance_time_lineage_markout_horizon_ms = max(
            1,
            int(
                self.context.params.get("variance_time_lineage_markout_horizon_ms", 30_000)
                or 30_000
            ),
        )
        self.state.variance_time_lineage_markout_max_age_ms = max(
            0,
            int(
                self.context.params.get("variance_time_lineage_markout_max_age_ms", 2_000)
                or 0
            ),
        )
        self.state.variance_time_lineage_fail_on_q90_pre_ack_fill = bool(
            self.context.params.get(
                "variance_time_lineage_fail_on_q90_pre_ack_fill",
                True,
            )
        )
        self.state.trace_variance_time_lineage = (
            [] if self.state.variance_time_lineage_randomized_enabled else None
        )
        self.state.trace_variance_time_lineage_events = (
            []
            if self.state.variance_time_lineage_randomized_enabled
            and self.state.lineage_outcome_contract_version == "v2"
            else None
        )
        self.state.variance_time_lineage_open_by_side: dict[str, dict[str, self.services.Any]] = {}
        self.state.variance_time_lineage_pending_inventory_lifecycle: dict[int, list[dict[str, self.services.Any]]] = {}
        self.state.variance_time_lineage_assigned_inventory_lifecycles: set[int] = set()
        self.state.lineage_randomized_trace_audit: dict[str, self.services.Any] = {}
        self.state.variance_time_lineage_assignment_counts = {
            self.services.LINEAGE_CONTROL_ACTION: 0,
            self.services.LINEAGE_CANDIDATE_ACTION: 0,
        }
        self.state.variance_time_lineage_next_id = 0
        self.state.post_cooldown_inventory_budget_enabled = bool(
            self.context.params.get(
                "post_cooldown_incremental_inventory_budget_enabled",
                False,
            )
        )
        self.state.post_cooldown_inventory_budget_units = float(
            self.context.params.get(
                "post_cooldown_incremental_inventory_budget_units",
                self.services.math.inf,
            )
        )
        self.state.post_cooldown_inventory_budget_arm_id = str(
            self.context.params.get(
                "post_cooldown_incremental_inventory_budget_arm_id",
                "control_infinity"
                if self.services.math.isinf(self.state.post_cooldown_inventory_budget_units)
                else f"budget_{self.state.post_cooldown_inventory_budget_units:g}_units",
            )
            or ""
        ).strip()
        self.state.post_cooldown_inventory_budget_target_side = str(
            self.context.params.get(
                "post_cooldown_incremental_inventory_budget_target_side",
                "BOTH",
            )
            or ""
        ).strip().upper()
        self.state.trace_post_cooldown_inventory_budget_max = int(
            self.context.params.get(
                "trace_post_cooldown_incremental_inventory_budget_max",
                0,
            )
            or 0
        )
        if self.state.post_cooldown_inventory_budget_enabled:
            if (
                self.services.math.isnan(self.state.post_cooldown_inventory_budget_units)
                or self.state.post_cooldown_inventory_budget_units <= 0.0
            ):
                raise ValueError(
                    "post-cooldown incremental inventory budget must be positive "
                    "or infinity"
                )
            if not self.state.post_cooldown_inventory_budget_arm_id:
                raise ValueError(
                    "post-cooldown incremental inventory budget requires an arm id"
                )
            if self.state.post_cooldown_inventory_budget_target_side not in {
                "BUY",
                "SELL",
                "BOTH",
            }:
                raise ValueError(
                    "post-cooldown incremental inventory budget target side must "
                    "be BUY, SELL, or BOTH"
                )
            if (
                self.services.math.isfinite(self.state.post_cooldown_inventory_budget_units)
                and not self.services.math.isclose(
                    self.state.post_cooldown_inventory_budget_units,
                    round(self.state.post_cooldown_inventory_budget_units),
                    abs_tol=1e-12,
                )
            ):
                raise ValueError(
                    "post-cooldown incremental inventory budget must use whole "
                    "planned fill units"
                )
            if self.state.trace_post_cooldown_inventory_budget_max <= 0:
                raise ValueError(
                    "post-cooldown incremental inventory budget requires a positive "
                    "trace limit"
                )
            if (
                self.state.variance_time_lineage_randomized_enabled
                or self.state.local_action_ope_enabled
                or self.state.sell_add_skip_ope_enabled
                or self.state.queue_value_keep_cancel_enabled
                or self.state.safe_add_rearm_randomized_enabled
                or self.state.state_conditioned_rearm_enabled
                or self.state.state_conditioned_policy is not None
            ):
                raise ValueError(
                    "post-cooldown incremental inventory budget must run alone as "
                    "its own mechanics identity"
                )
            from research.families.f09_inventory_lifecycle_action_uplift.audit.post_cooldown_incremental_inventory_budget import (
                PostCooldownIncrementalInventoryBudget,
            )
            self.context.PostCooldownIncrementalInventoryBudget = PostCooldownIncrementalInventoryBudget
        else:
            self.context.PostCooldownIncrementalInventoryBudget = None
        self.state.trace_post_cooldown_inventory_budget = (
            [] if self.state.post_cooldown_inventory_budget_enabled else None
        )
        self.state.post_cooldown_inventory_budget_pending_release: dict[str, dict[str, self.services.Any] | None] = {
            "BUY": None,
            "SELL": None,
        }
        self.state.post_cooldown_inventory_budget_active_by_side: dict[str, dict[str, self.services.Any]] = {}
        self.state.post_cooldown_inventory_budget_by_id: dict[int, dict[str, self.services.Any]] = {}
        self.state.post_cooldown_inventory_budget_prepared_orders: dict[str, int] = {}
        self.state.post_cooldown_inventory_budget_next_id = 0
        self.state.post_cooldown_inventory_budget_next_preparation_id = 0
        self.state.post_cooldown_inventory_budget_conservation_checks = 0
        self.state.post_cooldown_inventory_budget_conservation_failures = 0
        self.state.post_cooldown_inventory_budget_unsupported_count = 0
        self.state.post_cooldown_inventory_budget_block_count = 0

        self.state.sell_add_price_penalty_enabled = bool(
            self.context.params.get("sell_add_inventory_price_penalty_enabled", False)
        )
        self.state.sell_add_price_penalty_seed = int(
            self.context.params.get("sell_add_inventory_price_penalty_seed", self.state.rng_seed + 108_019)
        )
        self.state.sell_add_price_penalty_family_id = str(
            self.context.params.get("sell_add_inventory_price_penalty_family_id", "") or ""
        ).strip()
        self.state.sell_add_price_penalty_unit_btc = float(
            self.context.params.get("sell_add_inventory_price_penalty_unit_btc", 0.001) or 0.0
        )
        self.state.sell_add_price_penalty_step_bps = float(
            self.context.params.get("sell_add_inventory_price_penalty_step_bps", 0.5) or 0.0
        )
        self.state.sell_add_price_penalty_max_bps = float(
            self.context.params.get("sell_add_inventory_price_penalty_max_bps", 1.5) or 0.0
        )
        self.state.trace_sell_add_price_penalty_max = int(
            self.context.params.get("trace_sell_add_inventory_price_penalty_max", 0) or 0
        )
        if self.state.sell_add_price_penalty_enabled:
            from research.families.f09_inventory_lifecycle_action_uplift.audit.sell_add_inventory_price_penalty import (
                ACTIONS as SELL_ADD_PRICE_PENALTY_ACTIONS,
                CANDIDATE_ACTION as SELL_ADD_PRICE_PENALTY_CANDIDATE,
                CONTROL_ACTION as SELL_ADD_PRICE_PENALTY_CONTROL,
                SellAddPenaltyRandomizer,
                apply_sell_add_price_penalty,
                normalize_probabilities as normalize_sell_add_price_penalty_probabilities,
            )
            self.context.SELL_ADD_PRICE_PENALTY_ACTIONS = SELL_ADD_PRICE_PENALTY_ACTIONS
            self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE = SELL_ADD_PRICE_PENALTY_CANDIDATE
            self.context.SELL_ADD_PRICE_PENALTY_CONTROL = SELL_ADD_PRICE_PENALTY_CONTROL
            self.context.SellAddPenaltyRandomizer = SellAddPenaltyRandomizer
            self.context.apply_sell_add_price_penalty = apply_sell_add_price_penalty
            self.context.normalize_sell_add_price_penalty_probabilities = normalize_sell_add_price_penalty_probabilities

            if self.state.lineage_outcome_contract_version != "v2":
                raise ValueError(
                    "SELL-add inventory price penalty requires lineage outcome contract v2"
                )
            if not self.state.sell_add_price_penalty_family_id:
                raise ValueError(
                    "SELL-add inventory price penalty requires a frozen family id"
                )
            if self.state.trace_sell_add_price_penalty_max <= 0:
                raise ValueError(
                    "SELL-add inventory price penalty requires a positive trace limit"
                )
            if not self.services.math.isclose(self.state.sell_add_price_penalty_unit_btc, 0.001, abs_tol=1e-12):
                raise ValueError(
                    "SELL-add inventory price penalty v1 freezes inventory_unit_btc=0.001"
                )
            if not self.services.math.isclose(self.state.sell_add_price_penalty_step_bps, 0.5, abs_tol=1e-12):
                raise ValueError(
                    "SELL-add inventory price penalty v1 freezes step_bps=0.5"
                )
            if not self.services.math.isclose(self.state.sell_add_price_penalty_max_bps, 1.5, abs_tol=1e-12):
                raise ValueError(
                    "SELL-add inventory price penalty v1 freezes max_penalty_bps=1.5"
                )
            if str(self.context.params.get("fill_cooldown_clock_mode", "wall_time")).strip().lower() != "wall_time":
                raise ValueError(
                    "SELL-add inventory price penalty must preserve wall-time cooldown"
                )
            if (
                self.state.dynamic_fill_hazard_shadow_enabled
                or
                self.state.dynamic_fill_hazard_action_enabled
                or self.state.dynamic_fill_hazard_cpp_parity_enabled
                or self.state.dynamic_fill_hazard_mechanics_telemetry_enabled
            ):
                raise ValueError(
                    "SELL-add inventory price penalty requires BUY q90 off in both arms"
                )
            if (
                self.state.post_cooldown_inventory_budget_enabled
                or self.state.variance_time_lineage_randomized_enabled
                or self.state.local_action_ope_enabled
                or self.state.sell_add_skip_ope_enabled
                or self.state.queue_value_keep_cancel_enabled
                or self.state.safe_add_rearm_randomized_enabled
                or self.state.state_conditioned_rearm_enabled
                or self.state.state_conditioned_policy is not None
            ):
                raise ValueError(
                    "SELL-add inventory price penalty must run alone as its own action family"
                )
            self.state.sell_add_price_penalty_probabilities = (
                self.context.normalize_sell_add_price_penalty_probabilities(
                    self.context.params.get("sell_add_inventory_price_penalty_probabilities")
                )
            )
            if any(
                not self.services.math.isclose(probability, 0.5, abs_tol=1e-12)
                for probability in self.state.sell_add_price_penalty_probabilities.values()
            ):
                raise ValueError(
                    "SELL-add inventory price penalty requires exact 0.5/0.5 propensity"
                )
            self.state.sell_add_price_penalty_randomizer = self.context.SellAddPenaltyRandomizer(
                seed=self.state.sell_add_price_penalty_seed,
                family_id=self.state.sell_add_price_penalty_family_id,
            )
        else:
            self.context.SELL_ADD_PRICE_PENALTY_ACTIONS = (
                "baseline_current_quote",
                "sell_add_inventory_price_penalty",
            )
            self.context.SELL_ADD_PRICE_PENALTY_CONTROL = self.context.SELL_ADD_PRICE_PENALTY_ACTIONS[0]
            self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE = self.context.SELL_ADD_PRICE_PENALTY_ACTIONS[1]
            self.state.sell_add_price_penalty_probabilities = {
                self.context.SELL_ADD_PRICE_PENALTY_CONTROL: 0.5,
                self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE: 0.5,
            }
            self.state.sell_add_price_penalty_randomizer = None
            self.context.apply_sell_add_price_penalty = None
        self.state.trace_sell_add_price_penalty = (
            [] if self.state.sell_add_price_penalty_enabled else None
        )
        self.state.trace_sell_add_price_penalty_events = (
            [] if self.state.sell_add_price_penalty_enabled else None
        )
        self.state.sell_add_price_penalty_open_record: dict[str, self.services.Any] | None = None
        self.state.sell_add_price_penalty_assigned_inventory_lifecycles: set[int] = set()
        self.state.sell_add_price_penalty_assignment_counts = {
            action: 0 for action in self.context.SELL_ADD_PRICE_PENALTY_ACTIONS
        }
        self.state.sell_add_price_penalty_next_id = 0
        self.state.sell_add_price_penalty_trace_audit: dict[str, self.services.Any] = {}

        self.state.multi_short_repair_enabled = bool(
            self.context.params.get("multi_short_reducing_buy_aggression_enabled", False)
        )
        self.state.multi_short_repair_seed = int(
            self.context.params.get("multi_short_reducing_buy_aggression_seed", self.state.rng_seed + 108_031)
        )
        self.state.multi_short_repair_family_id = str(
            self.context.params.get("multi_short_reducing_buy_aggression_family_id", "") or ""
        ).strip()
        self.state.multi_short_repair_trigger_inventory_btc = float(
            self.context.params.get(
                "multi_short_reducing_buy_aggression_trigger_inventory_btc",
                -0.002,
            )
        )
        self.state.multi_short_repair_release_inventory_btc = float(
            self.context.params.get(
                "multi_short_reducing_buy_aggression_release_inventory_btc",
                -0.001,
            )
        )
        self.state.trace_multi_short_repair_max = int(
            self.context.params.get("trace_multi_short_reducing_buy_aggression_max", 0) or 0
        )
        if self.state.multi_short_repair_enabled:
            from research.families.f09_inventory_lifecycle_action_uplift.audit.multi_short_reducing_buy_aggression import (
                ACTIONS as MULTI_SHORT_REPAIR_ACTIONS,
                CANDIDATE_ACTION as MULTI_SHORT_REPAIR_CANDIDATE,
                CONTROL_ACTION as MULTI_SHORT_REPAIR_CONTROL,
                MultiShortRepairRandomizer,
                aggressive_maker_buy_price,
                normalize_probabilities as normalize_multi_short_repair_probabilities,
                treatment_should_end as multi_short_repair_should_end,
                treatment_should_start as multi_short_repair_should_start,
            )
            self.context.MULTI_SHORT_REPAIR_ACTIONS = MULTI_SHORT_REPAIR_ACTIONS
            self.context.MULTI_SHORT_REPAIR_CANDIDATE = MULTI_SHORT_REPAIR_CANDIDATE
            self.context.MULTI_SHORT_REPAIR_CONTROL = MULTI_SHORT_REPAIR_CONTROL
            self.context.MultiShortRepairRandomizer = MultiShortRepairRandomizer
            self.context.aggressive_maker_buy_price = aggressive_maker_buy_price
            self.context.normalize_multi_short_repair_probabilities = normalize_multi_short_repair_probabilities
            self.context.multi_short_repair_should_end = multi_short_repair_should_end
            self.context.multi_short_repair_should_start = multi_short_repair_should_start

            if self.state.lineage_outcome_contract_version != "v2":
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression requires lineage outcome contract v2"
                )
            if not self.state.multi_short_repair_family_id:
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression requires a frozen family id"
                )
            if self.state.trace_multi_short_repair_max <= 0:
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression requires a positive trace limit"
                )
            if not self.services.math.isclose(
                self.state.multi_short_repair_trigger_inventory_btc, -0.002, abs_tol=1e-12
            ):
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression v1 freezes trigger_inventory_btc=-0.002"
                )
            if not self.services.math.isclose(
                self.state.multi_short_repair_release_inventory_btc, -0.001, abs_tol=1e-12
            ):
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression v1 freezes release_inventory_btc=-0.001"
                )
            if str(self.context.params.get("fill_cooldown_clock_mode", "wall_time")).strip().lower() != "wall_time":
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression must preserve wall-time cooldown"
                )
            if (
                self.state.dynamic_fill_hazard_action_enabled
                or self.state.dynamic_fill_hazard_cpp_parity_enabled
                or self.state.dynamic_fill_hazard_mechanics_telemetry_enabled
            ):
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression requires BUY q90 off in both arms"
                )
            if (
                self.state.sell_add_price_penalty_enabled
                or self.state.post_cooldown_inventory_budget_enabled
                or self.state.variance_time_lineage_randomized_enabled
                or self.state.local_action_ope_enabled
                or self.state.sell_add_skip_ope_enabled
                or self.state.queue_value_keep_cancel_enabled
                or self.state.safe_add_rearm_randomized_enabled
                or self.state.state_conditioned_rearm_enabled
                or self.state.state_conditioned_policy is not None
            ):
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression must run alone as its own action family"
                )
            self.state.multi_short_repair_probabilities = (
                self.context.normalize_multi_short_repair_probabilities(
                    self.context.params.get("multi_short_reducing_buy_aggression_probabilities")
                )
            )
            if any(
                not self.services.math.isclose(probability, 0.5, abs_tol=1e-12)
                for probability in self.state.multi_short_repair_probabilities.values()
            ):
                raise ValueError(
                    "multi-SHORT reducing-BUY aggression requires exact 0.5/0.5 propensity"
                )
            self.state.multi_short_repair_randomizer = self.context.MultiShortRepairRandomizer(
                seed=self.state.multi_short_repair_seed,
                family_id=self.state.multi_short_repair_family_id,
            )
        else:
            self.context.MULTI_SHORT_REPAIR_ACTIONS = (
                "baseline_reducing_buy",
                "aggressive_maker_reducing_buy",
            )
            self.context.MULTI_SHORT_REPAIR_CONTROL = self.context.MULTI_SHORT_REPAIR_ACTIONS[0]
            self.context.MULTI_SHORT_REPAIR_CANDIDATE = self.context.MULTI_SHORT_REPAIR_ACTIONS[1]
            self.state.multi_short_repair_probabilities = {
                self.context.MULTI_SHORT_REPAIR_CONTROL: 0.5,
                self.context.MULTI_SHORT_REPAIR_CANDIDATE: 0.5,
            }
            self.state.multi_short_repair_randomizer = None
            self.context.aggressive_maker_buy_price = None
            self.context.multi_short_repair_should_start = None
            self.context.multi_short_repair_should_end = None
        self.state.trace_multi_short_repair = [] if self.state.multi_short_repair_enabled else None
        self.state.trace_multi_short_repair_events = [] if self.state.multi_short_repair_enabled else None
        self.state.multi_short_repair_open_record: dict[str, self.services.Any] | None = None
        self.state.multi_short_repair_pending_inventory_lifecycle: dict[int, dict[str, self.services.Any]] = {}
        self.state.multi_short_repair_assigned_inventory_lifecycles: set[int] = set()
        self.state.multi_short_repair_assignment_counts = {
            action: 0 for action in self.context.MULTI_SHORT_REPAIR_ACTIONS
        }
        self.state.multi_short_repair_next_id = 0
        self.state.multi_short_repair_trace_audit: dict[str, self.services.Any] = {}

        self.state.fair_center_shift_enabled = bool(
            self.context.params.get("cross_venue_fair_center_shift_enabled", False)
        )
        self.state.fair_center_shift_seed = int(
            self.context.params.get("cross_venue_fair_center_shift_seed", self.state.rng_seed + 108_047)
        )
        self.state.fair_center_shift_family_id = str(
            self.context.params.get("cross_venue_fair_center_shift_family_id", "") or ""
        ).strip()
        self.state.trace_fair_center_shift_max = int(
            self.context.params.get("trace_cross_venue_fair_center_shift_max", 0) or 0
        )
        self.state.fair_center_max_state_age_ms = float(
            self.context.params.get("cross_venue_fair_center_max_state_age_ms", 2_000.0)
            or 0.0
        )
        self.state.fair_center_forced_action = str(
            self.context.params.get("cross_venue_fair_center_shift_forced_action", "") or ""
        ).strip()
        if self.state.fair_center_shift_enabled:
            from research.families.f09_inventory_lifecycle_action_uplift.audit.cross_venue_fair_center_shift import (
                ACTIONS as FAIR_CENTER_ACTIONS,
                CANDIDATE_ACTION as FAIR_CENTER_CANDIDATE,
                CONTROL_ACTION as FAIR_CENTER_CONTROL,
                FairCenterRandomizer,
                assignment_lead_direction as fair_center_lead_direction,
                normalize_probabilities as normalize_fair_center_probabilities,
                project_action_pair as project_fair_center_action_pair,
            )
            self.context.FAIR_CENTER_ACTIONS = FAIR_CENTER_ACTIONS
            self.context.FAIR_CENTER_CANDIDATE = FAIR_CENTER_CANDIDATE
            self.context.FAIR_CENTER_CONTROL = FAIR_CENTER_CONTROL
            self.context.FairCenterRandomizer = FairCenterRandomizer
            self.context.fair_center_lead_direction = fair_center_lead_direction
            self.context.normalize_fair_center_probabilities = normalize_fair_center_probabilities
            self.context.project_fair_center_action_pair = project_fair_center_action_pair

            if self.context.historical_fair_price_data is None:
                raise ValueError(
                    "fair-center randomized replay requires historical fair-price data"
                )
            if self.state.lineage_outcome_contract_version != "v2":
                raise ValueError(
                    "fair-center randomized replay requires lineage outcome contract v2"
                )
            if not self.state.fair_center_shift_family_id:
                raise ValueError(
                    "fair-center randomized replay requires a frozen family id"
                )
            if self.state.trace_fair_center_shift_max <= 0:
                raise ValueError(
                    "fair-center randomized replay requires a positive trace limit"
                )
            if self.state.fair_center_max_state_age_ms <= 0.0:
                raise ValueError("fair-center state age must be positive")
            if (
                self.state.dynamic_fill_hazard_action_enabled
                or self.state.dynamic_fill_hazard_cpp_parity_enabled
                or self.state.dynamic_fill_hazard_mechanics_telemetry_enabled
            ):
                raise ValueError(
                    "fair-center randomized replay requires BUY q90 off in both arms"
                )
            if (
                self.state.multi_short_repair_enabled
                or self.state.sell_add_price_penalty_enabled
                or self.state.post_cooldown_inventory_budget_enabled
                or self.state.variance_time_lineage_randomized_enabled
                or self.state.local_action_ope_enabled
                or self.state.sell_add_skip_ope_enabled
                or self.state.queue_value_keep_cancel_enabled
                or self.state.safe_add_rearm_randomized_enabled
                or self.state.state_conditioned_rearm_enabled
                or self.state.state_conditioned_policy is not None
            ):
                raise ValueError(
                    "fair-center randomized replay must run alone as its own action family"
                )
            self.state.fair_center_probabilities = self.context.normalize_fair_center_probabilities(
                self.context.params.get("cross_venue_fair_center_shift_probabilities")
            )
            if any(
                not self.services.math.isclose(probability, 0.5, abs_tol=1e-12)
                for probability in self.state.fair_center_probabilities.values()
            ):
                raise ValueError(
                    "fair-center randomized replay requires exact 0.5/0.5 propensity"
                )
            self.state.fair_center_randomizer = self.context.FairCenterRandomizer(
                seed=self.state.fair_center_shift_seed,
                family_id=self.state.fair_center_shift_family_id,
            )
            if self.state.fair_center_forced_action and self.state.fair_center_forced_action not in {
                self.context.FAIR_CENTER_CONTROL,
                self.context.FAIR_CENTER_CANDIDATE,
            }:
                raise ValueError(
                    "fair-center forced action must be local_quote_center or "
                    "cross_venue_fair_quote_center"
                )
            self.state.fair_center_cursor = self.services.HistoricalFairPriceCursor(
                self.context.historical_fair_price_data,
                max_state_age_ms=self.state.fair_center_max_state_age_ms,
            )
        else:
            self.context.FAIR_CENTER_ACTIONS = (
                "local_quote_center",
                "cross_venue_fair_quote_center",
            )
            self.context.FAIR_CENTER_CONTROL = self.context.FAIR_CENTER_ACTIONS[0]
            self.context.FAIR_CENTER_CANDIDATE = self.context.FAIR_CENTER_ACTIONS[1]
            self.state.fair_center_probabilities = {
                self.context.FAIR_CENTER_CONTROL: 0.5,
                self.context.FAIR_CENTER_CANDIDATE: 0.5,
            }
            self.state.fair_center_randomizer = None
            self.state.fair_center_forced_action = ""
            self.state.fair_center_cursor = None
            self.context.fair_center_lead_direction = None
            self.context.project_fair_center_action_pair = None
        self.state.trace_fair_center_shift = [] if self.state.fair_center_shift_enabled else None
        self.state.trace_fair_center_shift_events = [] if self.state.fair_center_shift_enabled else None
        self.state.fair_center_open_record: dict[str, self.services.Any] | None = None
        self.state.fair_center_assignment_counts = {
            action: 0 for action in self.context.FAIR_CENTER_ACTIONS
        }
        self.state.fair_center_next_id = 0
        self.state.fair_center_trace_audit: dict[str, self.services.Any] = {}

        self.state.trace_queue_events_max = int(self.context.params.get("trace_queue_events_max", 0) or 0)
        self.state.trace_queue_events = [] if self.state.trace_queue_events_max > 0 else None
        self.state.trace_queue_start_ms = self.context.params.get("trace_queue_start_ms", None)
        self.state.trace_queue_end_ms = self.context.params.get("trace_queue_end_ms", None)
        if self.state.trace_queue_start_ms is not None:
            self.state.trace_queue_start_ms = int(self.state.trace_queue_start_ms)
        if self.state.trace_queue_end_ms is not None:
            self.state.trace_queue_end_ms = int(self.state.trace_queue_end_ms)
        self.state.raw_trace_queue_ids = self.context.params.get("trace_queue_order_ids", "")
        if isinstance(self.state.raw_trace_queue_ids, str):
            self.state.trace_queue_order_ids = {
                int(x.strip()) for x in self.state.raw_trace_queue_ids.split(",") if x.strip()
            }
        elif self.state.raw_trace_queue_ids:
            self.state.trace_queue_order_ids = {int(x) for x in self.state.raw_trace_queue_ids}
        else:
            self.state.trace_queue_order_ids = set()
        self.state.next_trace_order_id = 0
        self.state.new_order_latency_ms = max(0, int(self.context.params.get("new_order_latency_ms", 0.0) or 0.0))
        self.state.cancel_order_latency_ms = max(0, int(self.context.params.get("cancel_order_latency_ms", 0.0) or 0.0))
        self.state.latency_jitter_ms = max(0, int(self.context.params.get("latency_jitter_ms", 0.0) or 0.0))
        self.state.latency_seed = self.services.effective_latency_seed(self.context.params)
        self.state.latency_sampler_version = str(
            self.context.params.get("latency_sampler_version", self.services.LATENCY_SAMPLER_VERSION)
            or self.services.LATENCY_SAMPLER_VERSION
        )
        if self.state.latency_sampler_version != self.services.LATENCY_SAMPLER_VERSION:
            raise ValueError(
                f"unsupported latency_sampler_version={self.state.latency_sampler_version!r}"
            )
        self.state.decision_to_gateway_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_decision_to_gateway_latency_samples_ms", []),
            dtype=self.services.np.float64,
        ).ravel()
        self.state.decision_to_gateway_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.decision_to_gateway_latency_samples_ms[
                self.services.np.isfinite(self.state.decision_to_gateway_latency_samples_ms)
                & (self.state.decision_to_gateway_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.decision_to_gateway_latency_enabled = bool(
            self.state.decision_to_gateway_latency_samples_ms.size > 0
            and self.services.np.any(self.state.decision_to_gateway_latency_samples_ms > 0.0)
        )
        self.state.pre_snapshot_compute_samples_ms = self.services.np.asarray(
            self.context.params.get("_pre_snapshot_compute_latency_samples_ms", []), dtype=self.services.np.float64
        )
        if (
            not self.services.np.all(self.services.np.isfinite(self.state.pre_snapshot_compute_samples_ms))
            or self.services.np.any(self.state.pre_snapshot_compute_samples_ms < 0.0)
        ):
            raise ValueError("pre-snapshot compute samples must be finite and non-negative")
        self.state.pre_snapshot_compute_enabled = bool(
            self.services.np.any(self.state.pre_snapshot_compute_samples_ms > 0.0)
        )
        if self.state.pre_snapshot_compute_enabled:
            self.state.raw_total = self.services.np.asarray(
                self.context.params.get("_decision_to_gateway_latency_samples_ms", []), dtype=self.services.np.float64
            )
            if (
                self.state.pre_snapshot_compute_samples_ms.ndim != 1
                or self.state.raw_total.ndim != 1
                or self.state.pre_snapshot_compute_samples_ms.shape != self.state.raw_total.shape
                or not self.services.np.all(self.services.np.isfinite(self.state.raw_total))
                or not self.services.np.all(self.services.np.isfinite(self.state.pre_snapshot_compute_samples_ms))
                or self.services.np.any(self.state.pre_snapshot_compute_samples_ms < 0.0)
                or self.services.np.any(self.state.pre_snapshot_compute_samples_ms > self.state.raw_total)
            ):
                raise ValueError(
                    "pre-snapshot/total compute samples must be aligned finite 0 <= pre <= total"
                )
        self.state.decision_to_gateway_latency_seed = self.services.effective_decision_to_gateway_latency_seed(
            self.context.params
        )
        if (
            self.state.decision_to_gateway_latency_enabled
            and str(self.context.params.get("replay_purpose", "")).strip().lower()
            != "diagnostic"
        ):
            raise ValueError(
                "decision-to-gateway compute latency is diagnostic-only"
            )
        self.state.diagnostic_passive_close_bbo_clip = bool(
            self.context.params.get("_diagnostic_passive_close_bbo_clip", False)
        )
        if (
            self.state.diagnostic_passive_close_bbo_clip
            and str(self.context.params.get("replay_purpose", "")).strip().lower() != "diagnostic"
        ):
            raise ValueError("historical passive-close BBO clipping is diagnostic-only")
        self.state.latency_stress_enabled = bool(self.context.params.get("latency_stress_enabled", False))
        self.state.latency_stress_spike_probability = float(
            self.context.params.get("latency_stress_spike_probability", 0.0) or 0.0
        )
        self.state.latency_stress_spike_multiplier = float(
            self.context.params.get("latency_stress_spike_multiplier", 1.0) or 1.0
        )
        self.state.rest_gateway_timing_mode = str(
            self.context.params.get("rest_gateway_timing_mode", "disabled") or "disabled"
        ).strip().lower()
        if self.state.rest_gateway_timing_mode not in {
            "disabled", "paired_npz", "sampled_serial", "sampled_async_fifo",
        }:
            raise ValueError(
                "rest_gateway_timing_mode must be disabled, paired_npz, sampled_serial "
                "or sampled_async_fifo"
            )
        self.state.rest_gateway_timing_profile = None
        self.state.async_rest_gateway = self.state.rest_gateway_timing_mode == "sampled_async_fifo"
        self.state.sampled_serial_gateway = self.state.rest_gateway_timing_mode in {
            "sampled_serial", "sampled_async_fifo",
        }
        self.state.rest_gateway_timing_enabled = self.state.rest_gateway_timing_mode != "disabled"
        self.state.rest_gateway_timing_seed = int(
            self.context.params.get("rest_gateway_timing_seed", self.state.latency_seed) or self.state.latency_seed
        )
        if self.state.rest_gateway_timing_enabled:
            if str(self.context.params.get("replay_purpose", "")).strip().lower() != "diagnostic":
                raise ValueError(
                    "paired serial REST gateway timing is diagnostic-only"
                )
        self.state.raw_gateway_path = str(
            self.context.params.get("rest_gateway_timing_profile_path", "") or ""
        ).strip()
        self.state.operation_return_samples = self.services.rest_return_sample_rows(self.context.params)
        self.state.direct_return_samples = self.state.operation_return_samples is not None
        self.state.direct_return_semantics = str(
            self.context.params.get("_serial_rest_return_sample_semantics", "") or ""
        ).strip()
        self.state.http_result_status_by_operation = self.services.rest_http_result_statuses(self.context.params)
        self.state.serial_rest_return_enabled = bool(
            self.state.sampled_serial_gateway and (self.state.raw_gateway_path or self.state.direct_return_samples)
        )
        self.state.main_loop_sleep_raw = float(self.context.params.get("replay_main_loop_sleep_ms", 0) or 0)
        if (
            not self.services.math.isfinite(self.state.main_loop_sleep_raw)
            or self.state.main_loop_sleep_raw < 0
            or self.state.main_loop_sleep_raw != int(self.state.main_loop_sleep_raw)
        ):
            raise ValueError("replay_main_loop_sleep_ms must be a non-negative integer")
        self.state.main_loop_sleep_ms = int(self.state.main_loop_sleep_raw)
        self.state.main_loop_no_poll_wait = self.context.params.get("replay_main_loop_wait_mode", "poll") == "event_deadline"
        if self.context.params.get("replay_main_loop_wait_mode", "poll") not in ("poll", "event_deadline"):
            raise ValueError("unknown replay main-loop wait mode")
        if self.state.main_loop_no_poll_wait and self.state.main_loop_sleep_ms != 0:
            raise ValueError("event_deadline mode requires zero polling delay")
        self.state.main_loop_enabled = self.state.main_loop_sleep_ms > 0 or self.state.main_loop_no_poll_wait
        self.state.async_lane_capacity = int(self.context.params.get("async_order_lane_capacity", 8))
        if self.state.async_rest_gateway and (
            not self.state.main_loop_enabled or self.state.replay_event_clock != "merged"
            or not self.state.serial_rest_return_enabled or self.state.async_lane_capacity <= 0
            or bool(self.context.params.get("cross_side_order_lanes_enabled", False))
            or str(self.context.params.get("order_transport", "rest")).strip().lower() != "rest"
        ):
            raise ValueError(
                "sampled_async_fifo requires merged main-loop timing, REST-return samples, "
                "positive capacity and one GLOBAL REST lane"
            )
        if self.state.main_loop_enabled and (not self.state.serial_rest_return_enabled or self.state.replay_event_clock != "merged"):
            raise ValueError(
                "main-loop timing requires merged replay and sampled_serial REST-return timing"
            )
        if self.state.pre_snapshot_compute_enabled and not self.state.main_loop_enabled:
            raise ValueError("pre-snapshot compute requires main-loop sampled_serial timing")
        self.state.requote_tail_work_samples_ms, self.state.main_loop_work_samples_ms = self.services._local_work_samples(self.context.params)
        self.state.requote_tail_work_enabled = bool(self.services.np.any(self.state.requote_tail_work_samples_ms > 0.0))
        self.state.runtime_compute_by_path = self.services.runtime_compute_sample_rows(self.context.params)
        self.state.runtime_compute_path_counts = {path: 0 for path in self.state.runtime_compute_by_path}
        self.state.runtime_compute_last_bucket_end_ms = self.context.params.get("runtime_compute_initial_bucket_end_ms", 0)
        self.state.signal_cold_start = bool(self.context.params.get("signal_cold_start", False))
        self.state.signal_first_bucket_ms = None
        self.state.signal_completed_bars = 0
        self.state.signal_warmup_observed_ts_ms = None
        self.state.signal_warmup_skip_count = 0
        if self.state.signal_cold_start and (
            not self.state.main_loop_enabled or self.context.params.get("exec_book_visibility_mode") != "message_schedule"
            or not self.state.runtime_compute_by_path
            or self.state.runtime_compute_last_bucket_end_ms is not None
        ):
            raise ValueError("cold signal startup requires a fresh message-clock main loop and no computed bucket")
        self.state.runtime_compute_bucket_ms = int(self.context.params.get("runtime_compute_bucket_ms", 10_000))
        if self.state.runtime_compute_by_path:
            # Strata are chosen once at actual compute entry, never at every
            # timer or independently for each phase/request within the decision.
            self.state.decision_to_gateway_latency_enabled = True
            self.state.pre_snapshot_compute_enabled = True
            self.state.requote_tail_work_enabled = True
        if self.state.response_action_runtime is not None and not (
                self.state.pre_snapshot_compute_enabled and self.state.main_loop_enabled):
            raise ValueError('response policy requires the existing split-compute serial request clock')
        if self.state.quote_price_runtime is not None and not (
                self.state.pre_snapshot_compute_enabled and self.state.main_loop_enabled
                and self.state.async_rest_gateway):
            raise ValueError('quote-price policy requires split-compute async serial request clock')
        if self.state.cooldown_rearm_runtime is not None and not (
                self.state.pre_snapshot_compute_enabled and self.state.main_loop_enabled):
            raise ValueError('cooldown recovery requires the existing split-compute clock')
        self.state.main_loop_work_enabled = bool(self.services.np.any(self.state.main_loop_work_samples_ms > 0.0))
        if (self.state.requote_tail_work_enabled or self.state.main_loop_work_enabled) and not self.state.main_loop_enabled:
            raise ValueError("local work samples require main-loop sampled_serial diagnostic replay")
        self.state.pending_quote_compute = None
        self.state.active_quote_compute = None
        if self.state.quote_schedule is not None and not self.state.main_loop_enabled:
            raise ValueError("state_event requires the existing sampled main-loop clock")
        self.state.pre_snapshot_compute_count = 0
        self.state.pre_snapshot_compute_completed_count = 0
        self.state.pre_snapshot_compute_sum_ms = 0
        self.state.pre_snapshot_compute_discarded_count = 0
        self.state.main_loop_next_wake_ms: self.services.Optional[int] = None
        self.state.main_loop_next_start_ms = 0
        self.state.main_loop_idle_start_ms = 0
        self.state.main_loop_tick_complete_ms = 0
        self.state.main_loop_tick_count = 0
        self.state.main_loop_after_tick_ms = 0
        self.state.main_loop_quote_tail_ms = 0
        self.state.main_loop_work_pre_sum_ms = 0
        self.state.main_loop_work_post_sum_ms = 0
        self.state.requote_tail_work_count = 0
        self.state.requote_tail_work_sum_ms = 0
        self.state.is_main_loop_wake = False
        if self.state.rest_gateway_timing_mode == "paired_npz" or (
            self.state.serial_rest_return_enabled and not self.state.direct_return_samples
        ):
            if not self.state.raw_gateway_path:
                raise ValueError(
                    "paired serial REST gateway timing requires a profile path"
                )
            self.state.gateway_path = self.services.Path(self.state.raw_gateway_path).expanduser()
            if not self.state.gateway_path.is_absolute():
                self.state.gateway_path = self.services.ROOT / self.state.gateway_path
            self.state.rest_gateway_timing_profile = self.services._load_serial_rest_gateway_timing_profile(
                self.state.gateway_path
            )
        self.state.rest_return_samples: dict[str, self.services.np.ndarray] = {}
        if self.state.direct_return_samples:
            # Pool only the operation when telemetry cannot identify a side.  The
            # simulator samples each request independently, retaining row pairing;
            # it does not manufacture an observed multi-request or side assignment.
            self.state.rest_return_samples = {
                slot: self.state.operation_return_samples[slot.split("_", 1)[0]]
                for slot in self.services._REST_GATEWAY_SLOT_NAMES
            }
        elif self.state.serial_rest_return_enabled:
            self.state.profile = self.state.rest_gateway_timing_profile
            self.state.present = self.state.profile["request_present_mask"]
            self.state.shape = self.state.present.shape
            self.state.upper_mask = self.services.np.asarray(self.state.profile.get(
                "rest_completion_upper_bound_observed_mask", self.services.np.zeros(self.state.shape)
            ), dtype=self.services.np.bool_)
            self.state.upper = self.services.np.asarray(self.state.profile.get(
                "rest_completion_upper_bound_by_next_request_ms", self.services.np.full(self.state.shape, self.services.np.nan)
            ), dtype=self.services.np.float64)
            self.state.callback = self.services.np.asarray(self.state.profile.get(
                "completion_source_callback_type", self.services.np.full(self.state.shape, "")
            ))
            if any(value.shape != self.state.shape for value in (self.state.upper_mask, self.state.upper, self.state.callback)):
                raise ValueError("REST response-bound arrays must match request slots")
            for loop_item in enumerate(self.services._REST_GATEWAY_SLOT_NAMES):
                self.state.index, self.state.slot = loop_item
                self.state.effective = self.state.profile["exchange_effective_latency_ms"][:, self.state.index]
                self.state.visible = self.state.profile["local_visibility_latency_ms"][:, self.state.index]
                if self.state.slot.startswith("new_"):
                    # confirm_new is called after REST returns. WS-first activation
                    # does not observe that response and must not substitute for it.
                    self.state.valid = self.state.present[:, self.state.index] & (self.state.callback[:, self.state.index] == "rest_ack")
                    self.state.response = self.state.visible
                else:
                    self.state.valid = self.state.present[:, self.state.index] & self.state.upper_mask[:, self.state.index]
                    self.state.response = self.state.upper[:, self.state.index]
                self.state.valid &= self.services.np.isfinite(self.state.response) & (self.state.response >= self.state.effective)
                self.state.rest_return_samples[self.state.slot] = self.services.np.ascontiguousarray(
                    self.services.np.column_stack((self.state.effective[self.state.valid], self.state.visible[self.state.valid], self.state.response[self.state.valid]))
                )
                if not self.state.rest_return_samples[self.state.slot].shape[0]:
                    raise ValueError(f"no paired observed REST-return upper bounds for {self.state.slot}")
        self.state.active_rest_gateway_timing: dict[str, dict[str, int]] = {}
        self.state.rest_gateway_sampled_row_count = 0
        self.state.rest_gateway_busy_until_ms = 0
        # Queued requests reserve local ownership now, but obtain exchange/ACK
        # deadlines only when the sole network worker actually dispatches them.
        self.state.async_rest_queue: list[dict[str, self.services.Any]] = []
        self.state.async_rest_active: dict[str, self.services.Any] | None = None
        self.state.async_bulk_request: dict[str, self.services.Any] | None = None
        self.state.bulk_cancel_samples = self.services.bulk_cancel_sample_rows(self.context.params)
        self.state.bulk_cancel_request_count = 0
        self.state.main_loop_waiting_request: dict[str, self.services.Any] | None = None
        self.state.main_loop_synchronous_request_count = 0
        self.state.main_loop_synchronous_wait_ms = 0
        self.state.async_rest_sequence = 0
        self.state.async_rest_queue_high_watermark = 0
        self.state.async_rest_unset_ms = int(self.services.np.iinfo(self.services.np.int64).max // 1_000_000 - 1)
        self.state.rest_gateway_request_count = 0
        self.state.rest_gateway_busy_ms = 0
        self.state.rest_gateway_request_wait_ms = 0
        self.state.rest_gateway_decision_deferral_count = 0
        self.state.rest_gateway_last_timing: dict[str, self.services.Any] = {}
        self.state.serial_rest_decision: self.services.Optional[dict[str, self.services.Any]] = None
        self.state.serial_rest_sequence = 0
        self.state.rest_return_pending_coalesce_count = 0
        self.state.new_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_new_order_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.new_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.new_latency_samples_ms[
                self.services.np.isfinite(self.state.new_latency_samples_ms) & (self.state.new_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.new_exchange_effective_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_new_order_exchange_effective_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.new_exchange_effective_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.new_exchange_effective_latency_samples_ms[
                self.services.np.isfinite(self.state.new_exchange_effective_latency_samples_ms)
                & (self.state.new_exchange_effective_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.new_order_latency_split_enabled = bool(
            self.state.new_exchange_effective_latency_samples_ms.size > 0
            or self.state.rest_gateway_timing_enabled
            or self.state.decision_to_gateway_latency_enabled
        )
        if (
            self.state.new_exchange_effective_latency_samples_ms.size > 0
            and self.state.new_latency_samples_ms.size > 0
            and self.state.new_exchange_effective_latency_samples_ms.size
            != self.state.new_latency_samples_ms.size
        ):
            raise ValueError(
                "paired new-order effective/ACK latency samples must have equal length"
            )
        self.state.cancel_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_cancel_order_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.cancel_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.cancel_latency_samples_ms[
                self.services.np.isfinite(self.state.cancel_latency_samples_ms)
                & (self.state.cancel_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.cancel_exchange_effective_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_cancel_exchange_effective_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.cancel_exchange_effective_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.cancel_exchange_effective_latency_samples_ms[
                self.services.np.isfinite(self.state.cancel_exchange_effective_latency_samples_ms)
                & (self.state.cancel_exchange_effective_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.cancel_ack_visibility_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_cancel_ack_visibility_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.cancel_ack_visibility_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.cancel_ack_visibility_latency_samples_ms[
                self.services.np.isfinite(self.state.cancel_ack_visibility_latency_samples_ms)
                & (self.state.cancel_ack_visibility_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.cancel_latency_split_enabled = bool(
            self.state.cancel_exchange_effective_latency_samples_ms.size > 0
            or self.state.cancel_ack_visibility_latency_samples_ms.size > 0
            or self.state.rest_gateway_timing_enabled
            or self.state.decision_to_gateway_latency_enabled
        )
        if (
            self.state.cancel_exchange_effective_latency_samples_ms.size > 0
            and self.state.cancel_ack_visibility_latency_samples_ms.size > 0
            and self.state.cancel_exchange_effective_latency_samples_ms.size
            != self.state.cancel_ack_visibility_latency_samples_ms.size
        ):
            raise ValueError(
                "paired cancel effective/ACK latency samples must have equal length"
            )
        if self.state.sampled_serial_gateway and not self.state.serial_rest_return_enabled and not all(
            samples.size
            for samples in (
                self.state.new_exchange_effective_latency_samples_ms,
                self.state.new_latency_samples_ms,
                self.state.cancel_exchange_effective_latency_samples_ms,
                self.state.cancel_ack_visibility_latency_samples_ms,
            )
        ):
            raise ValueError(
                "sampled_serial requires paired NEW and CANCEL effective/ACK samples"
            )
        self.state.private_fill_visibility_latency_samples_ms = self.services.np.asarray(
            self.context.params.get("_private_fill_visibility_latency_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.private_fill_visibility_latency_samples_ms = self.services.np.ascontiguousarray(
            self.state.private_fill_visibility_latency_samples_ms[
                self.services.np.isfinite(self.state.private_fill_visibility_latency_samples_ms)
                & (self.state.private_fill_visibility_latency_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.private_fill_visibility_enabled = bool(
            self.state.private_fill_visibility_latency_samples_ms.size > 0
            and self.services.np.any(self.state.private_fill_visibility_latency_samples_ms > 0.0)
        )
        self.state.local_lifecycle_boundary_scheduler_enabled = bool(
            self.state.new_exchange_effective_latency_samples_ms.size > 0
            or self.state.cancel_latency_split_enabled
            or self.state.private_fill_visibility_enabled
        )
        self.state.local_lifecycle_boundary_scheduler = (
            self.services.LocalLifecycleBoundaryScheduler()
            if self.state.local_lifecycle_boundary_scheduler_enabled
            else None
        )
        self.state.private_fill_exchange_match_count = 0
        self.state.private_fill_visible_count = 0
        self.state.exec_book_visibility_delay_samples_ms = self.services.np.asarray(
            self.context.params.get("_exec_book_visibility_delay_samples_ms", []),
            dtype=self.services.np.float64,
        )
        self.state.exec_book_visibility_delay_samples_ms = self.services.np.ascontiguousarray(
            self.state.exec_book_visibility_delay_samples_ms[
                self.services.np.isfinite(self.state.exec_book_visibility_delay_samples_ms)
                & (self.state.exec_book_visibility_delay_samples_ms >= 0.0)
            ],
            dtype=self.services.np.float64,
        )
        self.state.exec_book_visibility_delay_mean_ms = max(
            0.0,
            float(self.context.params.get("exec_book_visibility_delay_mean_ms", 0.0) or 0.0),
        )
        self.state.exec_book_visibility_delay_jitter_ms = max(
            0.0,
            float(self.context.params.get("exec_book_visibility_delay_jitter_ms", 0.0) or 0.0),
        )
        self.state.exec_book_visibility_delay_seed = int(
            self.context.params.get("exec_book_visibility_delay_seed", 20260718)
        )
        self.state.exec_book_visibility_mode = str(
            self.context.params.get("exec_book_visibility_mode", "sampled") or "sampled"
        ).strip().lower()
        if self.state.exec_book_visibility_mode not in {
            "sampled",
            "sampled_joint",
            "paired",
            "profile_source_stratified",
            "message_schedule",
        }:
            raise ValueError(
                "exec_book_visibility_mode must be sampled, sampled_joint, paired, "
                "profile_source_stratified or message_schedule"
            )
        self.state.exec_source_profile = None
        self.state.exec_source_profile_sha256 = ""
        self.state.exec_source_profile_market_id = str(
            self.context.params.get("exec_source_stratified_profile_market_id", "") or ""
        ).strip()
        self.state.exec_source_profile_transport = str(
            self.context.params.get("exec_source_stratified_profile_transport", "") or ""
        ).strip().lower()
        if self.state.exec_book_visibility_mode == "profile_source_stratified":
            self.state.raw_profile_path = str(
                self.context.params.get("exec_source_stratified_profile_path", "") or ""
            ).strip()
            if not self.state.raw_profile_path:
                raise ValueError(
                    "profile_source_stratified visibility requires an explicit profile path"
                )
            self.state.exec_source_path = self.services.Path(self.state.raw_profile_path).expanduser()
            if not self.state.exec_source_path.is_absolute():
                self.state.exec_source_path = self.services.ROOT / self.state.exec_source_path
            self.state.exec_source_profile, self.state.exec_source_profile_sha256 = (
                self.services._load_exec_source_stratified_visibility_profile(
                    self.state.exec_source_path,
                    expected_profile_id=str(
                        self.context.params.get("exec_source_stratified_profile_id", "") or ""
                    ),
                    market_id=self.state.exec_source_profile_market_id,
                    transport=self.state.exec_source_profile_transport,
                )
            )
        self.state.exec_book_visibility_paired_ts_ms = self.services.np.asarray(
            self.context.params.get("_exec_book_visibility_paired_ts_ms", []), dtype=self.services.np.int64
        )
        self.state.exec_book_visibility_paired_delay_ms = self.services.np.asarray(
            self.context.params.get("_exec_book_visibility_paired_delay_ms", []), dtype=self.services.np.float64
        )
        self.state.exec_depth_visibility_paired_delay_ms = self.services.np.asarray(
            self.context.params.get("_exec_depth_visibility_paired_delay_ms", []), dtype=self.services.np.float64
        )
        self.state.exec_trade_visibility_paired_delay_ms = self.services.np.asarray(
            self.context.params.get("_exec_trade_visibility_paired_delay_ms", []), dtype=self.services.np.float64
        )
        self.state.exec_book_visibility_paired_mid = self.services.np.asarray(
            self.context.params.get("_exec_book_visibility_paired_mid", []), dtype=self.services.np.float64
        )
        self.state.paired_lengths = {
            self.state.exec_book_visibility_paired_ts_ms.size,
            self.state.exec_book_visibility_paired_delay_ms.size,
            self.state.exec_depth_visibility_paired_delay_ms.size,
            self.state.exec_trade_visibility_paired_delay_ms.size,
            self.state.exec_book_visibility_paired_mid.size,
        }
        if self.state.exec_book_visibility_mode == "paired" and (
            len(self.state.paired_lengths) != 1 or 0 in self.state.paired_lengths
        ):
            raise ValueError(
                "paired execution-book visibility requires aligned timestamp, "
                "book-age, depth-age, trade-age, and mid arrays"
            )
        self.state.joint_lengths = {
            self.state.exec_book_visibility_paired_delay_ms.size,
            self.state.exec_depth_visibility_paired_delay_ms.size,
            self.state.exec_trade_visibility_paired_delay_ms.size,
        }
        if self.state.exec_book_visibility_mode == "sampled_joint" and (
            len(self.state.joint_lengths) != 1 or 0 in self.state.joint_lengths
        ):
            raise ValueError(
                "sampled_joint execution-book visibility requires aligned "
                "book-age, depth-age, and trade-age arrays"
            )
        self.state.exec_book_visibility_paired_max_gap_ms = max(
            0,
            int(self.context.params.get("exec_book_visibility_paired_max_gap_ms", 5) or 0),
        )
        self.state.exec_depth_visibility_source_offset_ms = int(
            round(
                float(
                    self.context.params.get("exec_depth_visibility_source_offset_ms", 0.0)
                    or 0.0
                )
            )
        )
        self.state.exec_book_visibility_delay_enabled = bool(
            self.state.exec_book_visibility_mode
            in {"paired", "sampled_joint", "profile_source_stratified", "message_schedule"}
            or self.state.exec_book_visibility_delay_samples_ms.size > 0
            or self.state.exec_book_visibility_delay_mean_ms > 0.0
            or self.state.exec_book_visibility_delay_jitter_ms > 0.0
        )
        self.state.monotonic_exec_visibility_cutoffs_ms: dict[str, int] = {}
        self.state.monotonic_exec_visibility_enabled = bool(
            self.state.exec_book_visibility_delay_enabled
            and self.state.exec_book_visibility_mode != "message_schedule"
            and not bool(self.context.params.get("use_bar_pricing", True))
        )
        self.state.require_historical_bbo = bool(self.context.params.get("require_historical_bbo", False))

        # Step 26B: lazy requote
        self.state.requote_threshold_bps = self.context.params.get("requote_threshold_bps", 0.0)
        self.state.rq_threshold = self.state.requote_threshold_bps / 10000.0
        self.state.replace_min_price_change_ticks = max(0.0, float(self.context.params.get("replace_min_price_change_ticks", 0.0) or 0.0))
        self.state.replace_price_threshold = self.services.ReplacePriceThreshold.from_getter(self.context.params.get)
        self.state.directional_replace_counts = {}
        self.state.directional_replace_samples = []
        self.state.directional_replace_pending = {}
        self.state.replace_min_price_change_ticks_reducing = max(
            0.0,
            float(
                self.context.params.get(
                    "replace_min_price_change_ticks_reducing",
                    self.state.replace_min_price_change_ticks,
                )
                or 0.0
            ),
        )
        self.state.replace_min_interval_ms = max(0.0, float(self.context.params.get("replace_min_interval_ms", 0.0) or 0.0))
        self.state.replace_min_interval_ms_reducing = max(
            0.0,
            float(self.context.params.get("replace_min_interval_ms_reducing", self.state.replace_min_interval_ms) or 0.0),
        )
        self.state.replace_pending_coalesce = bool(self.context.params.get("replace_pending_coalesce", False))
        self.state.replace_cancel_first_exposure_increasing = bool(
            self.context.params.get("replace_cancel_first_exposure_increasing", False)
        )

        # ML params
        self.state.vol_blend = self.context.params.get("vol_blend", 0.0)
        self.state.skew_strength = self.context.params.get("skew_strength", 0.0)
        self.state.asym_strength = self.context.params.get("asym_strength", 0.0)
        self.state.ret_skew = self.context.params.get("ret_skew", 0.0)
        self.state.inventory_direction_alignment_strength = self.context.params.get("inventory_direction_alignment_strength", 0.0)
        self.state.ret_demean_halflife = self.context.params.get("ret_demean_halflife", 0)

        # Regime params
        self.state.liq_baseline = self.context.params.get("liq_baseline", 200.0)
        self.state.vol_power = self.context.params.get("vol_power", 1.5)

        # Risk / urgency params
        self.state.position_timeout_s = self.context.params.get("position_timeout", 0.0)
        self.state.position_timeout_ms = int(self.state.position_timeout_s * 1000)
        self.state.sync_adjust_degrade_enabled = bool(
            self.context.params.get("sync_adjust_degrade_enabled", False)
        )
        self.state.sync_adjust_pause_ms = max(
            0,
            int(
                round(
                    float(self.context.params.get("sync_adjust_pause_s", 0.0) or 0.0)
                    * 1000.0
                )
            ),
        )
        self.state.sync_adjust_cancel_orders = bool(
            self.context.params.get("sync_adjust_cancel_orders", True)
        )
        self.state.circuit_breaker_sigma = max(
            0.0, float(self.context.params.get("circuit_breaker_sigma", 0.0) or 0.0)
        )
        self.state.circuit_breaker_exit_mode = str(
            self.context.params.get("circuit_breaker_exit_mode", "maker_close") or "maker_close"
        ).lower()
        if self.state.circuit_breaker_exit_mode not in {"maker_close", "immediate_taker"}:
            raise ValueError(
                "circuit_breaker_exit_mode must be maker_close or immediate_taker"
            )
        self.state.pnl_volatility_horizon_s = max(
            1e-6, float(self.context.params.get("pnl_volatility_horizon_s", 300.0) or 300.0)
        )
        self.state.max_exec_book_age_ms = int(float(self.context.params.get("max_exec_book_age_s", 5.0)) * 1000.0)
        self.state.max_exec_book_visible_age_ms = float(self.context.params.get("max_exec_book_visible_age_s", 5.0)) * 1000.0
        self.state.max_exec_book_source_lag_ms = float(self.context.params.get("max_exec_book_source_lag_s", 5.0)) * 1000.0
        if self.state.exec_book_visibility_mode == "message_schedule" and any(
            not self.services.math.isfinite(value) or value <= 0.0
            for value in (self.state.max_exec_book_visible_age_ms, self.state.max_exec_book_source_lag_ms)
        ):
            raise ValueError("message delivery visible-age/source-lag limits must be positive")
        self.state.inventory_asym_strength = self.context.params.get("inventory_asym_strength", 0.0)
        self.state.inventory_signal_fade_strength = self.context.params.get("inventory_signal_fade_strength", 0.0)

        # Spread params
        self.state.kappa_ratio = self.context.params.get("kappa_ratio", 0.3)
        (self.state.dynamic_cap_enabled,
         self.state.max_spread_bps,
         self.state.dynamic_cap_base_bps,
         self.state.dynamic_cap_alpha,
         self.state.dynamic_cap_max_mult,
         self.state.dynamic_cap_var_baseline,
         self.state.dynamic_cap_liq_beta,
         self.state.dynamic_cap_liq_baseline,
         self.state.dynamic_cap_min_mult) = self.services._resolve_cap_params(self.context.params)

        # BER guard
        self.state.ber_guard_thresh = self.context.params.get("ber_guard_thresh", 0.0)
        self.state.ber_exposure_add_only = bool(self.context.params.get("ber_exposure_add_only", False))

        # Markout
        self.state.markout_ema_span_fills = max(
            0,
            int(self.context.params.get("markout_ema_span_fills", 0) or 0),
        )
        self.state.markout_horizon_s = max(1e-6, float(self.context.params.get("markout_horizon_s", 10.0)))
        self.state.markout_horizon_ms = max(1, int(round(self.state.markout_horizon_s * 1000.0)))
        self.state.markout_spread_scale = self.context.params.get("markout_spread_scale", 0.0)

        # Book imbalance
        self.state.book_imb_strength = self.context.params.get("book_imb_strength", 0.0)
        self.state.book_imb_source = str(self.context.params.get("book_imb_source", "depth_top"))
        self.state.book_imb_levels = max(1, int(self.context.params.get("book_imb_levels", 20)))

        # Dynamic RQ
        self.state.rq_min_s = self.context.params.get("rq_min", self.state.rq_s)
        self.state.rq_max_s = self.context.params.get("rq_max", self.state.rq_s)
        self.state.rq_min_ms = int(self.state.rq_min_s * 1000)
        self.state.rq_max_ms = int(self.state.rq_max_s * 1000)
        self.state.dynamic_rq = self.state.rq_min_ms < self.state.rq_max_ms

        # Depth params
        self.state.depth_liquidity_baseline = self.context.params.get("depth_liquidity_baseline", 50.0)
        self.state.thin_depth_threshold = max(0.0, float(self.context.params.get("thin_depth_threshold", 0.0) or 0.0))

        # Step 27: Fill cooldown
        self.state.fill_cooldown_s = self.context.params.get("fill_cooldown", 0.0)
        self.state.fill_cooldown_ms = int(self.state.fill_cooldown_s * 1000)
        (
            self.state.fill_cooldown_consecutive_reset_policy,
            self.state.fill_cooldown_reset_consec_on_expiry,
        ) = self.services._resolve_fill_cooldown_reset_policy(self.context.params)
        self.state.fill_cooldown_apply_reducing = bool(self.context.params.get("fill_cooldown_apply_reducing", False))
        self.state.fill_cooldown_reducing_s = max(0.0, float(self.context.params.get("fill_cooldown_reducing", 0.0) or 0.0))
        self.state.fill_cooldown_reducing_ms = int(self.state.fill_cooldown_reducing_s * 1000)
        self.state.fill_cooldown_reducing_inventory_lifecycle_only = bool(
            self.context.params.get("fill_cooldown_reducing_inventory_lifecycle_only", False)
        )
        self.state.fill_cooldown_reducing_inv_threshold = max(
            0.0,
            float(self.context.params.get("fill_cooldown_reducing_inv_threshold", 0.0) or 0.0),
        )
        self.state.fill_cooldown_reducing_inv_ratio = max(
            0.0,
            float(self.context.params.get("fill_cooldown_reducing_inv_ratio", 0.0) or 0.0),
        )
        self.state.fill_cooldown_reducing_age_s = max(
            0.0,
            float(self.context.params.get("fill_cooldown_reducing_age_s", 0.0) or 0.0),
        )
        self.state.fill_cooldown_reducing_vol_ref = max(
            0.0,
            float(self.context.params.get("fill_cooldown_reducing_vol_ref", 0.0) or 0.0),
        )
        self.state.fill_cooldown_reducing_vol_min_mult = float(
            self.context.params.get("fill_cooldown_reducing_vol_min_mult", 0.5) or 0.5
        )
        self.state.fill_cooldown_reducing_vol_max_mult = float(
            self.context.params.get("fill_cooldown_reducing_vol_max_mult", 2.0) or 2.0
        )
        if self.state.fill_cooldown_reducing_vol_max_mult < self.state.fill_cooldown_reducing_vol_min_mult:
            self.state.fill_cooldown_reducing_vol_min_mult, self.state.fill_cooldown_reducing_vol_max_mult = (
                self.state.fill_cooldown_reducing_vol_max_mult,
                self.state.fill_cooldown_reducing_vol_min_mult,
            )
        self.state.fill_cooldown_clock_mode = str(
            self.context.params.get("fill_cooldown_clock_mode", "wall_time") or "wall_time"
        ).strip().lower()
        if self.state.fill_cooldown_clock_mode not in {
            "wall_time",
            "variance_time",
            "randomized_lineage",
        }:
            raise ValueError(
                "fill_cooldown_clock_mode must be wall_time, variance_time, or "
                "randomized_lineage"
            )
        self.state.variance_time_enabled = self.state.fill_cooldown_clock_mode in {
            "variance_time",
            "randomized_lineage",
        }
        if self.state.variance_time_lineage_randomized_enabled != (
            self.state.fill_cooldown_clock_mode == "randomized_lineage"
        ):
            raise ValueError(
                "randomized_lineage clock mode and "
                "variance_time_lineage_randomized_enabled must be enabled together"
            )
        self.state.variance_time_reference_rate = {
            "BUY": float(
                self.context.params.get("variance_time_reference_rate_buy_bps2_per_s", 0.0)
                or 0.0
            ),
            "SELL": float(
                self.context.params.get("variance_time_reference_rate_sell_bps2_per_s", 0.0)
                or 0.0
            ),
        }
        self.state.variance_time_minimum_wall_time_ms = int(
            self.context.params.get("variance_time_minimum_wall_time_ms", 5_000) or 0
        )
        self.state.variance_time_maximum_wall_time_ms = int(
            self.context.params.get("variance_time_maximum_wall_time_ms", 600_000) or 0
        )
        self.state.variance_time_max_feature_age_ms = int(
            self.context.params.get("variance_time_max_feature_age_ms", 2_000) or 0
        )
        self.state.variance_rate_stream: self.services.CausalVarianceRateStream | None = None
        if self.state.variance_time_enabled:
            if self.state.fill_cooldown_ms <= 0:
                raise ValueError("variance-time rearm requires a positive fill cooldown")
            if self.state.fill_cooldown_consecutive_reset_policy != "opposite_fill_only":
                raise ValueError(
                    "variance-time rearm requires opposite_fill_only reset semantics"
                )
            if self.state.fill_cooldown_apply_reducing or self.state.fill_cooldown_reducing_ms > 0:
                raise ValueError(
                    "variance-time v1 changes only exposure-increasing cooldown; "
                    "reducing cooldown must remain disabled"
                )
            if bool(self.context.params.get("adaptive_add_cooldown_enabled", False)):
                raise ValueError(
                    "variance-time v1 cannot be combined with adaptive add cooldown"
                )
            if self.state.variance_time_minimum_wall_time_ms < 0:
                raise ValueError("variance-time minimum wall time must be non-negative")
            if (
                self.state.variance_time_maximum_wall_time_ms <= 0
                or self.state.variance_time_maximum_wall_time_ms
                < self.state.variance_time_minimum_wall_time_ms
            ):
                raise ValueError("variance-time wall-time bounds are invalid")
            if self.state.variance_time_max_feature_age_ms < 0:
                raise ValueError("variance-time feature age must be non-negative")
            if any(
                not self.services.math.isfinite(value) or value <= 0.0
                for value in self.state.variance_time_reference_rate.values()
            ):
                raise ValueError(
                    "variance-time BUY/SELL reference rates must be positive"
                )
            if not isinstance(self.context.variance_time_data, self.services.Mapping):
                raise ValueError(
                    "variance-time replay requires an aligned causal rate mapping"
                )
            try:
                self.state.variance_rate_stream = self.services.CausalVarianceRateStream(
                    self.context.variance_time_data["feature_ready_ts_ms"],
                    self.context.variance_time_data["rate_bps2_per_s"],
                    self.context.variance_time_data["valid"],
                )
            except KeyError as exc:
                self.context.exc = exc
                raise ValueError(
                    "variance_time_data requires feature_ready_ts_ms, "
                    "rate_bps2_per_s, and valid"
                ) from self.context.exc
        elif self.context.variance_time_data is not None:
            raise ValueError(
                "variance_time_data supplied while fill_cooldown_clock_mode is wall_time"
            )
        if self.state.post_cooldown_inventory_budget_enabled:
            if self.state.fill_cooldown_clock_mode != "wall_time":
                raise ValueError(
                    "post-cooldown incremental inventory budget requires the frozen "
                    "wall-time cooldown control"
                )
            if self.state.fill_cooldown_ms <= 0:
                raise ValueError(
                    "post-cooldown incremental inventory budget requires a positive "
                    "fill cooldown"
                )
            if self.state.fill_cooldown_consecutive_reset_policy != "opposite_fill_only":
                raise ValueError(
                    "post-cooldown incremental inventory budget requires "
                    "opposite_fill_only reset semantics"
                )
            if self.state.fill_cooldown_apply_reducing or self.state.fill_cooldown_reducing_ms > 0:
                raise ValueError(
                    "post-cooldown incremental inventory budget cannot alter the "
                    "reducing side"
                )
            if bool(self.context.params.get("adaptive_add_cooldown_enabled", False)):
                raise ValueError(
                    "post-cooldown incremental inventory budget cannot be combined "
                    "with adaptive add cooldown"
                )
            if str(self.context.params.get("decision_trace_profile", "full")) != "mechanics_only":
                raise ValueError(
                    "post-cooldown incremental inventory budget is mechanics-only"
                )
            if str(self.context.params.get("replay_initial_state_mode", "fresh_start")) != "fresh_start":
                raise ValueError(
                    "post-cooldown incremental inventory budget v1 requires daily "
                    "fresh-start replay"
                )
            if self.services.math.isfinite(self.state.post_cooldown_inventory_budget_units) and not self.services.math.isclose(
                self.state.post_cooldown_inventory_budget_units,
                round(self.state.post_cooldown_inventory_budget_units),
                abs_tol=1e-12,
            ):
                raise ValueError(
                    "post-cooldown incremental inventory budget v1 uses whole fill units"
                )
        # InventoryLifecycle stop-add is a replay shadow risk-control arm, not an alpha signal:
        # when an inventory inventory_lifecycle is already large/old, block only the side that
        # would add exposure and keep the reducing side available.
        self.state.inventory_lifecycle_stop_add_enabled = bool(self.context.params.get("inventory_lifecycle_stop_add_enabled", False))
        self.state.inventory_lifecycle_stop_add_inv_threshold = max(0.0, float(self.context.params.get("inventory_lifecycle_stop_add_inv_threshold", 0.0) or 0.0))
        self.state.inventory_lifecycle_stop_add_age_s = max(0.0, float(self.context.params.get("inventory_lifecycle_stop_add_age_s", 0.0) or 0.0))
        # Softer inventory_lifecycle control is the current research shape: large/old inventory_lifecycle
        # state is treated as a risk input, then only the exposure-increasing side is
        # lightly moved farther from mid. It should not hard-stop the reducing side.
        self.state.inventory_lifecycle_soft_control_enabled = bool(self.context.params.get("inventory_lifecycle_soft_control_enabled", False))
        self.state.inventory_lifecycle_soft_inv_threshold = max(0.0, float(self.context.params.get("inventory_lifecycle_soft_inv_threshold", 0.0) or 0.0))
        self.state.inventory_lifecycle_soft_age_s = max(0.0, float(self.context.params.get("inventory_lifecycle_soft_age_s", 0.0) or 0.0))
        self.state.inventory_lifecycle_soft_spread_mult = max(1.0, float(self.context.params.get("inventory_lifecycle_soft_spread_mult", 1.0) or 1.0))
        self.state.inventory_lifecycle_soft_gate_cfg = self.services.inventory_lifecycle_soft_gate_config_from_params(self.context.params)
        self.state.post_fill_quote_response_cfg = self.services.PostFillQuoteResponseConfig.from_params(self.context.params)
        self.state.post_fill_quote_response = self.services.PostFillQuoteResponse(self.state.post_fill_quote_response_cfg)
        self.state.multi_market_policy_cfg = self.services.MultiMarketPolicyConfig.from_params(self.context.params)
        self.state.multi_market_policy = self.services.MultiMarketPolicy(self.state.multi_market_policy_cfg)
        if self.state.multi_market_policy_cfg.enabled and self.state.multi_market_policy_cfg.mode != self.services.NOOP_MODE:
            if not self.context.reference_event_tapes and self.context.historical_global_flow_data is None:
                raise ValueError(
                    "active multi-market replay policy requires reference_event_tapes "
                    "or historical_global_flow_data"
                )
            if self.context.inventory_lifecycle_repair_data is None and self.context.inventory_lifecycle_repair_model is None:
                raise ValueError(
                    "post-fill multi-market replay policy requires causal "
                    "inventory_lifecycle_repair_data or inventory_lifecycle_repair_model"
                )
        if (
            self.state.post_fill_quote_response_cfg.requires_repair_model
            and self.context.inventory_lifecycle_repair_model is None
        ):
            raise ValueError(
                "flow-based post-fill quote response requires a causal "
                "inventory_lifecycle_repair_model so candidate-path repair probability is "
                "recomputed after every policy-induced fill"
            )
        if self.state.post_fill_quote_response_cfg.enabled and self.state.random_passive_enabled:
            raise ValueError(
                "post-fill quote response and randomized passive replay must be "
                "tested as separate arm families"
            )
        self.state.reference_scheduler = None
        if self.context.reference_event_tapes:
            self.state.flow_horizons = tuple(
                sorted(
                    set(self.services.DEFAULT_FLOW_HORIZONS_MS)
                    | {int(self.state.multi_market_policy_cfg.horizon_ms)}
                )
            )
            self.state.reference_scheduler = self.services.HistoricalReferenceScheduler(
                self.context.reference_event_tapes,
                engine=self.services.GlobalFlowEngine(
                    execution_symbol=self.services.SYMBOL,
                    reference_symbol=str(self.context.params.get("reference_symbol", "BTCUSDT") or "BTCUSDT"),
                    horizons_ms=self.state.flow_horizons,
                ),
            )
        self.state.inventory_lifecycle_repair_cursor = (
            self.services.InventoryLifecycleRepairCursor(self.context.inventory_lifecycle_repair_data)
            if self.context.inventory_lifecycle_repair_data is not None
            else None
        )
        self.state.historical_global_flow_cursor = (
            self.services.HistoricalGlobalFlowCursor(self.context.historical_global_flow_data)
            if self.context.historical_global_flow_data is not None
            else None
        )
        self.state.inventory_lifecycle_repair_history = self.services.InventoryLifecycleRepairProbabilityHistory(
            max_history_ms=max(
                60_000,
                int(self.state.multi_market_policy_cfg.repair_lookback_ms) * 4,
            )
        )
        self.state.buy_fill_selection_live_enabled = bool(self.context.params.get("buy_fill_selection_live_enabled", False))
        self.state.buy_fill_selection_model = None
        self.state.buy_fill_selection_live_score_threshold = float(self.context.params.get("buy_fill_selection_live_score_threshold", 0.50) or 0.50)
        self.state.buy_fill_selection_live_spread_mult_cap = max(
            1.0,
            float(self.context.params.get("buy_fill_selection_live_spread_mult_cap", 1.0) or 1.0),
        )
        self.state.buy_fill_selection_live_apply_reducing = bool(self.context.params.get("buy_fill_selection_live_apply_reducing", False))
        self.state.buy_fill_selection_live_max_missing = int(self.context.params.get("buy_fill_selection_live_max_missing_features", 99) or 99)
        self.state.buy_fill_selection_hit_count = 0
        self.state.buy_fill_selection_eval_count = 0
        self.state.buy_fill_selection_score_sum = 0.0
        self.state.buy_fill_selection_score_max = 0.0
        self.state.buy_fill_selection_score_ge_042 = 0
        self.state.buy_fill_selection_score_ge_043 = 0
        self.state.buy_fill_selection_score_ge_044 = 0
        self.state.buy_fill_selection_score_ge_045 = 0
        self.state.buy_fill_selection_scores: list[float] = []
        if self.state.buy_fill_selection_live_enabled:
            self.state.raw_model_path = str(self.context.params.get("buy_fill_selection_live_model_path", "") or "").strip()
            if not self.state.raw_model_path:
                raise FileNotFoundError("buy_fill_selection_live_enabled=true but buy_fill_selection_live_model_path is empty")
            self.state.model_path = self.services.Path(self.state.raw_model_path).expanduser()
            if not self.state.model_path.is_absolute():
                self.state.model_path = self.services.ROOT / self.state.model_path
            self.state.buy_fill_selection_model = self.services.FillSelectionScoreEnsemble.load(self.state.model_path)

        self.state.buy_soft_widen_release_probe_enabled = bool(
            self.context.params.get("buy_soft_widen_release_probe_enabled", False)
        )
        self.state.buy_soft_widen_release_probe_apply_candidate = bool(
            self.context.params.get("buy_soft_widen_release_probe_apply_candidate", True)
        )
        self.state.buy_soft_widen_release_target_ts_ms = int(
            self.context.params.get("buy_soft_widen_release_target_ts_ms", 0) or 0
        )
        self.state.buy_soft_widen_release_target_role = str(
            self.context.params.get("buy_soft_widen_release_target_role", "") or ""
        ).strip().lower()
        self.state.buy_soft_widen_release_spread_mult_cap = float(
            self.context.params.get("buy_soft_widen_release_spread_mult_cap", 1.0) or 1.0
        )
        if self.state.buy_soft_widen_release_probe_enabled:
            if self.state.buy_soft_widen_release_target_ts_ms <= 0:
                raise ValueError(
                    "buy_soft_widen_release_probe_enabled requires a positive target timestamp"
                )
            if self.state.buy_soft_widen_release_target_role not in {"opener", "add"}:
                raise ValueError(
                    "buy_soft_widen_release_target_role must be opener or add"
                )
            if not self.services.math.isclose(
                self.state.buy_soft_widen_release_spread_mult_cap, 1.0, abs_tol=1e-12
            ):
                raise ValueError("BUY soft-widen release v1 freezes spread_mult_cap=1.0")
            if self.state.buy_fill_selection_live_enabled:
                raise ValueError(
                    "BUY soft-widen release probe and legacy BUY fill selector are mutually exclusive"
                )
            if self.state.conditional_p3_reach_gate_enabled:
                raise ValueError(
                    "BUY soft-widen release probe and conditional-P3 action are mutually exclusive"
                )
        self.state.buy_soft_widen_release_target_consumed = False
        self.state.buy_soft_widen_release_target_reached_count = 0
        self.state.buy_soft_widen_release_eligible_count = 0
        self.state.buy_soft_widen_release_requested_count = 0
        self.state.buy_soft_widen_release_effective_mult_count = 0
        self.state.buy_soft_widen_release_effective_price_count = 0
        self.state.buy_soft_widen_release_role_observed = ""
        self.state.buy_soft_widen_release_reason = "not_reached"
        self.state.buy_soft_widen_release_baseline_spread_mult = 0.0
        self.state.buy_soft_widen_release_selected_spread_mult = 0.0
        self.state.buy_soft_widen_release_baseline_bid_price = 0.0
        self.state.buy_soft_widen_release_candidate_bid_price = 0.0

        self.state.ema_add_wait_fork_assigned = False
        self.state.ema_add_wait_fork_assignment_ts_ms = 0
        self.state.ema_add_wait_fork_assignment_equity_usdc = 0.0
        self.state.ema_add_wait_fork_assignment_inventory_btc = 0.0
        self.state.ema_add_wait_fork_wait_active = False
        self.state.ema_add_wait_fork_released = False
        self.state.ema_add_wait_fork_release_ts_ms = 0
        self.state.ema_add_wait_fork_release_generation: dict[str, int] | None = None
        self.state.ema_add_wait_fork_external_epoch_observed = False
        self.state.ema_add_wait_fork_quarantine = False
        self.state.ema_add_wait_fork_quarantine_ts_ms = 0
        self.state.ema_add_wait_fork_terminal = False
        self.state.ema_add_wait_fork_terminal_ts_ms = 0
        self.state.ema_add_wait_fork_target_actual_action = "not_reached"
        self.state.ema_add_wait_fork_target_order_id = -1
        self.state.ema_add_wait_fork_descendant_submit_count = 0
        self.state.ema_add_wait_fork_second_assignment_count = 0
        self.state.ema_add_wait_fork_reason = "not_reached"

        self.state.trace_cooldown_duration_opportunities = (
            [] if self.state.cooldown_duration_trace_max > 0 else None
        )
        self.state.cooldown_v2_snapshot_receipts: list[dict[str, self.services.Any]] = []
        self.state.cooldown_duration_policy_decisions: list[dict[str, self.services.Any]] = []
        self.state.cooldown_v2_lineage_revision = {"BUY": 0, "SELL": 0}
        self.state.cooldown_duration_shared_prefix_dispatch_enabled = (
            self.state.cooldown_duration_shared_prefix_executor is not None
        )
        self.state.cooldown_duration_exposure_fill_ordinal = 0
        self.state.cooldown_duration_fork_assigned = False
        self.state.cooldown_duration_fork_assignment_ts_ms = 0
        self.state.cooldown_duration_fork_assignment_equity_usdc = 0.0
        self.state.cooldown_duration_fork_assignment_inventory_btc = 0.0
        self.state.cooldown_duration_fork_baseline_ms = 0.0
        self.state.cooldown_duration_fork_exact_owner_baseline_ms = 0.0
        self.state.cooldown_duration_fork_exact_owner_action = ""
        self.state.cooldown_duration_fork_applied_ms = 0.0
        self.state.cooldown_duration_fork_deadline_ts_ms = 0
        self.state.cooldown_duration_fork_quarantine = False
        self.state.cooldown_duration_fork_quarantine_ts_ms = 0
        self.state.cooldown_duration_fork_terminal = False
        self.state.cooldown_duration_fork_terminal_ts_ms = 0
        self.state.cooldown_duration_fork_reason = "not_reached"
        self.state.cooldown_duration_target_override_active = False
        self.state.cooldown_duration_target_control_deadline_ts_ms = 0
        self.state.cooldown_duration_exposure_permission_change_count = 0
        self.state.cooldown_duration_reducing_permission_control_checks = 0
        self.state.cooldown_duration_reducing_quote_change_count = 0
        self.state.cooldown_duration_second_assignment_count = 0
        self.state.cooldown_duration_fork_assignment_buy_fills = 0
        self.state.cooldown_duration_fork_assignment_sell_fills = 0
        self.state.cooldown_duration_fork_path_last_ts_ms = 0
        self.state.cooldown_duration_fork_path_inventory_btc = 0.0
        self.state.cooldown_duration_fork_inventory_time_btc_s = 0.0
        self.state.cooldown_duration_fork_mae_usdc = 0.0
        self.state.cooldown_duration_fork_max_abs_inventory_btc = 0.0

        # Step 28: symmetric order sizing (mirror smaller side after eta decay)
        self.state.symmetric_size = bool(self.context.params.get("symmetric_size", False))

        # Pricing parity switch: config/bar matches use_bar_pricing=true;
        # microprice matches live when use_bar_pricing=false and depth is available.
        self.state.use_bar_pricing = bool(self.context.params.get("use_bar_pricing", True))
        self.state.depth_liquidity_ratio_floor = float(self.context.params.get("depth_liquidity_ratio_floor", 0.3))
        self.state.depth_liquidity_ratio_floor = max(0.05, min(3.0, self.state.depth_liquidity_ratio_floor))

        self.state.adverse_guard_enabled = bool(self.context.params.get("adverse_guard_enabled", False))
        self.state.adverse_toxicity_threshold = float(self.context.params.get("adverse_toxicity_threshold", 0.70))
        self.state.adverse_markout_threshold = abs(float(self.context.params.get("adverse_markout_threshold", 5.0)))
        self.state.adverse_markout_pause_threshold = abs(float(self.context.params.get("adverse_markout_pause_threshold", 0.0)))
        self.state.adverse_markout_pause_hybrid = bool(self.context.params.get("adverse_markout_pause_hybrid", False))
        self.state.adverse_markout_pause_base_s = max(0.0, float(self.context.params.get("adverse_markout_pause_base_s", 120.0)))
        self.state.adverse_markout_pause_min_s = max(0.0, float(self.context.params.get("adverse_markout_pause_min_s", 120.0)))
        self.state.adverse_markout_pause_max_s = max(self.state.adverse_markout_pause_min_s, float(self.context.params.get("adverse_markout_pause_max_s", 900.0)))
        self.state.adverse_markout_decay_tau_s = max(0.0, float(self.context.params.get("adverse_markout_decay_tau_s", 0.0)))
        self.state.adverse_markout_max_resolve_gap_s = max(
            self.state.markout_horizon_s,
            float(self.context.params.get("adverse_markout_max_resolve_gap_s", 30.0)),
        )
        self.state.adverse_dir_threshold = abs(float(self.context.params.get("adverse_dir_threshold", 0.0)))
        self.state.adverse_ret_bps_threshold = abs(float(self.context.params.get("adverse_ret_bps_threshold", 0.0)))
        self.state.adverse_weighted_mid_proxy_shift_bps = abs(float(self.context.params.get("adverse_weighted_mid_proxy_shift_bps", 0.0)))
        self.state.adverse_spread_mult = max(1.0, float(self.context.params.get("adverse_spread_mult", 1.10)))
        self.state.adverse_thin_depth_threshold = max(0.0, float(self.context.params.get("adverse_thin_depth_threshold", 0.0)))
        self.state.adverse_thin_depth_mult = max(1.0, float(self.context.params.get("adverse_thin_depth_mult", 1.0)))
        self.state.adverse_pause = bool(self.context.params.get("adverse_pause", True))
        self.state.local_extreme_guard_enabled = bool(self.context.params.get("local_extreme_guard_enabled", False))
        self.state.local_extreme_window_ms = int(max(5.0, float(self.context.params.get("local_extreme_window_s", 120.0))) * 1000.0)
        self.state.local_extreme_rank_threshold = min(0.999, max(0.501, float(self.context.params.get("local_extreme_rank_threshold", 0.80))))
        self.state.local_extreme_require_thin_depth = bool(self.context.params.get("local_extreme_require_thin_depth", True))
        self.state.local_extreme_thin_depth_threshold = max(0.0, float(self.context.params.get("local_extreme_thin_depth_threshold", 0.0)))
        self.state.local_extreme_spread_mult = max(1.0, float(self.context.params.get("local_extreme_spread_mult", 1.0)))
        self.state.local_extreme_pause = bool(self.context.params.get("local_extreme_pause", False))
        self.state.fragile_order_ttl_ms = int(max(0.0, float(self.context.params.get("fragile_order_ttl_s", 0.0))) * 1000.0)
        self.state.local_extreme_cfg = self.services.LocalExtremeGuardConfig(
            enabled=self.state.local_extreme_guard_enabled,
            window_s=self.state.local_extreme_window_ms / 1000.0,
            rank_threshold=self.state.local_extreme_rank_threshold,
            require_thin_depth=self.state.local_extreme_require_thin_depth,
            thin_depth_threshold=self.state.local_extreme_thin_depth_threshold,
            spread_mult=self.state.local_extreme_spread_mult,
            pause=self.state.local_extreme_pause,
            fragile_order_ttl_s=self.state.fragile_order_ttl_ms / 1000.0,
            depth_liquidity_baseline=float(self.state.depth_liquidity_baseline),
            tick_size=self.state.TICK,
        )
        self.state.adaptive_add_cooldown_cfg = self.services.adaptive_add_cooldown_config_from_params(self.context.params)
        self.state.defense_guard_enabled = bool(self.context.params.get("defense_guard_enabled", False))
        self.state.defense_markout_threshold = abs(float(self.context.params.get("defense_markout_threshold", 2.0)))
        self.state.defense_dir_threshold = abs(float(self.context.params.get("defense_dir_threshold", 0.05)))
        self.state.defense_ret_bps_threshold = abs(float(self.context.params.get("defense_ret_bps_threshold", 0.0)))
        self.state.defense_weighted_mid_proxy_shift_bps = abs(float(self.context.params.get("defense_weighted_mid_proxy_shift_bps", 0.0)))
        self.state.defense_spread_mult = max(1.0, float(self.context.params.get("defense_spread_mult", 1.35)))
        self.state.defense_pause = bool(self.context.params.get("defense_pause", True))
        self.state.defense_emergency_inventory_ratio = max(0.0, float(self.context.params.get("defense_emergency_inventory_ratio", 0.50)))
        self.state.defense_emergency_loss = max(0.0, float(self.context.params.get("defense_emergency_loss", 5.0)))
        self.state.flat_unilateral_max_s = max(0.0, float(self.context.params.get("flat_unilateral_max_s", 0.0)))
        self.state.flat_unilateral_max_ms = int(self.state.flat_unilateral_max_s * 1000)
        self.state.historical_bbo = self.context.bbo_data is not None
        self.state.historical_l2 = self.context.l2_data is not None
        if self.state.require_historical_bbo and not (self.state.historical_bbo or self.state.historical_l2):
            raise ValueError("Historical BBO/L2 data required for GTX simulation but not available")
        self.services._validate_historical_book_coverage(
            self.context.trades_df["transact_time"].to_numpy(dtype=self.services.np.int64, copy=False),
            self.context.bbo_data, self.context.l2_data, self.context.params,
            # No timeline slicing/mutation occurs between the admitted clock and
            # this call. External mutable-frame routes must not use this cache.
            _prepared_owner=self.context._prepared_clock_owner if self.context._prepared_account is not None else None,
            _clock_key=self.context.prepared_event_identity[0] if self.context.prepared_event_identity else None,
        )

        self.state.use_ml = self.services.replay_ml_enabled(
            self.context.params,
            self.context.ml_data is not None,
            self.state.vol_blend > 0 or self.state.skew_strength > 0 or self.state.asym_strength > 0 or
            self.state.ret_skew > 0 or self.state.inventory_direction_alignment_strength > 0 or
            self.state.adverse_guard_enabled or self.state.defense_guard_enabled or
            self.state.buy_fill_selection_live_enabled or self.state.conditional_p3_reach_gate_enabled,
        )

        # ── Floor order_size to lot_size ──
        if self.state.LOT_SIZE > 0:
            self.state.order_size = self.services.math.floor(self.state.order_size / self.state.LOT_SIZE) * self.state.LOT_SIZE
            if self.state.order_size < self.state.LOT_SIZE:
                self.state.order_size = self.state.LOT_SIZE

        self.state.quote_core_params = dict(self.context.params)
        self.state.quote_core_params.pop("_replay_progress_callback", None)
        self.state.quote_core_params.pop("_target_journal", None)
        self.state.quote_core_params["order_size"] = self.state.order_size
        self.state.quote_use_depth_microprice = not self.state.use_bar_pricing and self.state.historical_l2
        self.state.quote_use_depth_kappa = not self.state.use_bar_pricing and self.state.historical_l2
        self.state.quote_core_cfg = self.services.quote_core_config_from_params(
            self.state.quote_core_params,
            tick_size=self.state.TICK,
            lot_size=self.state.LOT_SIZE,
            use_ml=self.state.use_ml,
            use_depth_weighted_mid_proxy=self.state.quote_use_depth_microprice,
            use_depth_liquidity_scaling=self.state.quote_use_depth_kappa,
        )
        self.state.spread_cap_action_code = int(self.state.quote_core_cfg.spread_cap_mode)
        self.state.spread_cap_action_name = self.services.spread_cap_mode_name(self.state.spread_cap_action_code)

        # ── ML prediction arrays ──
        self.state.cur_dir = 0.5
        self.state.cur_vol = 0.0
        self.state.cur_ret = 0.0
        self.state.cur_tox_bid = 0.5
        self.state.cur_tox_ask = 0.5
        self.state.cur_xmarket = {col: 0.0 for col in self.services.XMARKET_REPLAY_FEATURE_COLUMNS}
        self.state.cur_prediction_features: dict[str, self.services.Any] = {}
        if self.context.ml_data is not None:
            self.state.ml_ts, self.state.ml_dir, self.state.ml_vol, self.state.ml_ret = self.context.ml_data[:4]
            self.state.has_tox = len(self.context.ml_data) >= 6
            if self.state.has_tox:
                self.state.ml_tox_bid, self.state.ml_tox_ask = self.context.ml_data[4:6]
            else:
                self.state.ml_tox_bid = None
                self.state.ml_tox_ask = None
            self.state.has_xmarket = len(self.context.ml_data) >= 6 + len(self.services.XMARKET_REPLAY_FEATURE_COLUMNS)
            if self.state.has_xmarket:
                self.state.ml_xmarket = {
                    col: self.services.np.asarray(self.context.ml_data[6 + idx], dtype=self.services.np.float64)
                    for idx, col in enumerate(self.services.XMARKET_REPLAY_FEATURE_COLUMNS)
                }
            else:
                self.state.ml_xmarket = {}
            self.state.feature_context_offset = 6 + len(self.services.XMARKET_REPLAY_FEATURE_COLUMNS)
            self.state.has_prediction_features = (
                len(self.context.ml_data) > self.state.feature_context_offset
                and isinstance(self.context.ml_data[self.state.feature_context_offset], dict)
            )
            self.state.ml_prediction_features = (
                self.context.ml_data[self.state.feature_context_offset] if self.state.has_prediction_features else {}
            )
            if self.state.buy_fill_selection_live_enabled and not self.state.has_prediction_features:
                raise ValueError(
                    "buy_fill_selection_live_enabled requires causal Prediction.feature_dict "
                    "arrays in replay ML data"
                )
            self.state.ml_idx = -1 if (self.state.signal_cold_start or self.context.params.get("cold_flat_clock_start")) else 0
        else:
            self.state.has_xmarket = False
            self.state.ml_xmarket = {}
            self.state.has_prediction_features = False
            self.state.ml_prediction_features = {}
            self.state.ml_idx = -1

        if self.state.conditional_p3_reach_gate_enabled:
            if self.context.ml_data is None or not self.state.has_tox:
                raise ValueError(
                    "conditional P3 reach gate requires side-specific toxicity ML arrays"
                )
            self.state.conditional_p3_reach_gate_ts = self.services.np.ascontiguousarray(
                self.state.raw_p3_reach_gate_ts,
                dtype=self.services.np.int64,
            ).reshape(-1)
            self.state.conditional_p3_reach_gate_status = self.services.np.ascontiguousarray(
                self.state.raw_p3_reach_gate_status,
                dtype=self.services.np.uint8,
            )
            if self.state.conditional_p3_reach_gate_status.ndim != 2:
                raise ValueError("_conditional_p3_reach_gate_status must be a 2D matrix")
            if self.state.conditional_p3_reach_gate_status.shape[0] != len(self.state.ml_ts):
                raise ValueError(
                    "conditional P3 reach gate row count must match ML prediction rows"
                )
            if (
                self.state.conditional_p3_reach_gate_status.shape[1] <= 0
                or self.state.conditional_p3_reach_gate_status.shape[1] % 4 != 0
            ):
                raise ValueError(
                    "conditional P3 reach gate status matrix must contain four equal blocks"
                )
            if self.services.np.any(self.state.conditional_p3_reach_gate_status > 2):
                raise ValueError("conditional P3 reach gate status must lie in {0,1,2}")
            if not self.services.np.array_equal(
                self.state.conditional_p3_reach_gate_ts,
                self.services.np.asarray(self.state.ml_ts, dtype=self.services.np.int64).reshape(-1),
            ):
                raise ValueError(
                    "conditional P3 reach gate timestamp differs from the ML overlay"
                )
            self.state.conditional_p3_reach_gate_grid_size = (
                self.state.conditional_p3_reach_gate_status.shape[1] // 4
            )
        else:
            self.state.conditional_p3_reach_gate_ts = self.services.np.empty(0, dtype=self.services.np.int64)
            self.state.conditional_p3_reach_gate_status = self.services.np.empty((0, 0), dtype=self.services.np.uint8)
            self.state.conditional_p3_reach_gate_grid_size = 0


        if self.state.historical_bbo:
            self.state.bbo_ts = self.context.bbo_data.ts_ms
            if self.context._prepared_clock_owner is not None:
                self.state.bbo_observation_us, self.state.bbo_usable, self.context._ = self.context._prepared_clock_owner.book_inputs(
                    'bbo', self.context.bbo_data, allow_source_regression=self.context.params.get("source_asof_order_policy") == "modeled_delivery_order")
            else:
                self.state.bbo_observation_us = self.services.book_observation_times_us(
                    self.context.bbo_data, allow_source_regression=self.context.params.get("source_asof_order_policy") == "modeled_delivery_order")
                self.state.bbo_usable = self.services.book_usable_mask(self.context.bbo_data)
            self.state.bbo_best_bid = self.context.bbo_data.best_bid
            self.state.bbo_best_ask = self.context.bbo_data.best_ask
            self.state.bbo_bid_qty = self.context.bbo_data.bid_qty
            self.state.bbo_ask_qty = self.context.bbo_data.ask_qty
            self.state.bbo_idx = max(
                0,
                int(
                    self.services.np.searchsorted(
                        self.state.bbo_ts,
                        int(self.state.trade_ts_seed[0]),
                        side="right",
                    )
                    - 1
                ),
            )
            self.state.book_source = self.context.bbo_data.source
        else:
            self.state.bbo_ts = None
            self.state.bbo_observation_us = None
            self.state.bbo_usable = None
            self.state.bbo_best_bid = None
            self.state.bbo_best_ask = None
            self.state.bbo_bid_qty = None
            self.state.bbo_ask_qty = None
            self.state.bbo_idx = -1
            self.state.book_source = "historical_l2" if self.state.historical_l2 else "inferred_trade"

        if self.state.historical_l2:
            self.state.l2_ts = self.context.l2_data.ts_ms
            if self.context._prepared_clock_owner is not None:
                self.state.l2_observation_us, self.context._, self.context._ = self.context._prepared_clock_owner.book_inputs(
                    'l2', self.context.l2_data, allow_source_regression=self.context.params.get("source_asof_order_policy") == "modeled_delivery_order")
            else:
                self.state.l2_observation_us = self.services.book_observation_times_us(
                    self.context.l2_data, allow_source_regression=self.context.params.get("source_asof_order_policy") == "modeled_delivery_order")
            self.state.l2_bid_px = self.context.l2_data.bid_px
            self.state.l2_bid_qty = self.context.l2_data.bid_qty
            self.state.l2_ask_px = self.context.l2_data.ask_px
            self.state.l2_ask_qty = self.context.l2_data.ask_qty
            self.state.l2_idx = max(
                0,
                int(
                    self.services.np.searchsorted(
                        self.state.l2_ts,
                        int(self.state.trade_ts_seed[0]),
                        side="right",
                    )
                    - 1
                ),
            )
            self.state.l2_near_levels = min(
                # Live SignalEngine._snapshot_l2_state defines near depth as the
                # first three levels.  Replay must use the same state variable;
                # the previous top-5 default changed guards and BUY scoring.
                max(1, int(self.context.params.get("l2_refill_cancel_near_levels", 3) or 3)),
                self.state.l2_bid_px.shape[1],
            )
            self.state.l2_policy_depth_levels = min(
                # Live SignalEngine._snapshot_l2_state summarizes at most ten
                # levels even though the execution stream carries top-20.
                max(1, int(self.context.params.get("l2_policy_depth_levels", 10) or 10)),
                self.state.l2_bid_px.shape[1],
            )
            self.state.cur_l2_bid_px_row = self.state.l2_bid_px[self.state.l2_idx]
            self.state.cur_l2_bid_qty_row = self.state.l2_bid_qty[self.state.l2_idx]
            self.state.cur_l2_ask_px_row = self.state.l2_ask_px[self.state.l2_idx]
            self.state.cur_l2_ask_qty_row = self.state.l2_ask_qty[self.state.l2_idx]
        else:
            self.state.l2_ts = None
            self.state.l2_observation_us = None
            self.state.l2_bid_px = None
            self.state.l2_bid_qty = None
            self.state.l2_ask_px = None
            self.state.l2_ask_qty = None
            self.state.l2_idx = -1
            self.state.l2_near_levels = 0
            self.state.l2_policy_depth_levels = 0
            self.state.cur_l2_bid_px_row = None
            self.state.cur_l2_bid_qty_row = None
            self.state.cur_l2_ask_px_row = None
            self.state.cur_l2_ask_qty_row = None
        self.state.book_resolution_candidates_ms: list[float] = []
        for loop_item in (self.state.bbo_ts, self.state.l2_ts):
            self.state.source_ts = loop_item
            if self.state.source_ts is None or len(self.state.source_ts) < 2:
                continue
            if self.context._prepared_clock_owner is not None:
                self.context.name = 'bbo' if self.state.source_ts is self.state.bbo_ts else 'l2'
                self.context.book = self.context.bbo_data if self.context.name == 'bbo' else self.context.l2_data
                self.context._, self.context._, self.context.resolution = self.context._prepared_clock_owner.book_inputs(
                    self.context.name, self.context.book, allow_source_regression=self.context.params.get("source_asof_order_policy") == "modeled_delivery_order")
                if self.context.resolution is not None:
                    self.state.book_resolution_candidates_ms.append(self.context.resolution)
                continue
            self.state.positive_deltas = self.services.np.diff(
                self.services.np.asarray(self.state.source_ts, dtype=self.services.np.int64)
            )
            self.state.positive_deltas = self.state.positive_deltas[self.state.positive_deltas > 0]
            if self.state.positive_deltas.size:
                self.state.book_resolution_candidates_ms.append(
                    float(self.services.np.median(self.state.positive_deltas))
                )
        self.state.book_state_resolution_ms = (
            min(self.state.book_resolution_candidates_ms)
            if self.state.book_resolution_candidates_ms
            else self.services.math.inf
        )
        if self.state.queue_l2_cancel_ahead_enabled and not self.state.historical_l2:
            raise ValueError(
                "queue_l2_cancel_ahead_enabled requires strict historical L2 data"
            )
        self.state.l2_refill_cancel_lookback_ms = int(
            max(0.0, float(self.context.params.get("l2_refill_cancel_lookback_s", 10.0) or 0.0)) * 1000.0
        )

        # ── Extract arrays ──
        self.state.trade_ts = self.context.trades_df["transact_time"].values
        self.state.trade_price = self.context.trades_df["price"].values
        self.state.trade_qty = self.context.trades_df["quantity"].values
        if self.context.prepared_event_identity:
            (self.state.trade_ts, self.state.trade_price,
             self.state.trade_qty) = self.context._prepared_clock_owner.clock_arrays(self.context.prepared_event_identity[0])
            (self.state.maker_event_code, self.state.is_seller,
             self.state.is_execution_trade) = self.context._prepared_clock_owner.execution_inputs(self.context.prepared_event_identity[0])
        else:
            self.state.maker_event_code = self.context.trades_df["is_buyer_maker"].to_numpy(dtype=self.services.np.uint8, copy=False)
            self.state.is_seller = self.state.maker_event_code == 1
            self.state.is_execution_trade = (
                self.context.trades_df["_is_execution_trade"].to_numpy(dtype=self.services.np.bool_, copy=False)
                if "_is_execution_trade" in self.context.trades_df.columns
                else self.services.np.ones(len(self.context.trades_df), dtype=self.services.np.bool_)
            )
        self.state.n_trades = len(self.state.trade_ts)
        self.state.loaded_variance_ts_ms = self.services.np.asarray(self.context.var_ts_ms)
        # _local_rank_at_ts reads 120 seconds at order activation. Keep its full
        # causal price history, plus any longer enabled local-extreme consumer.
        self.state.input_price_lookback_ms = max(
            120_000, self.state.local_extreme_window_ms if self.state.local_extreme_guard_enabled else 0
        )
        self.state.replay_origin_ts_ms = int(self.state.trade_ts[0])
        if self.state.signal_cold_start:
            self.state.signal_price_available_ts_ms = int(self.state.trade_ts[0])
            self.state.replay_origin_ts_ms = int(self.context.params.get("replay_event_clock_start_ts_ms", self.state.replay_origin_ts_ms))
        elif self.context.params.get("cold_flat_clock_start"):
            self.state.replay_origin_ts_ms = int(self.context.params.get("replay_event_clock_start_ts_ms", self.state.replay_origin_ts_ms))
        self.state.input_window_count = 1
        self.state.input_event_offset = 0
        self.state.input_clock_offsets = dict.fromkeys(("trade", "seed", "bbo", "l2", "variance", "prediction"), 0)
        self.state.policy_visible_execution = self.state.is_execution_trade.copy()
        self.state.last_processed_event_idx = -1
        # Message delivery is independent of the exchange matching clock.  These
        # arrays contain one assigned receive/ready time per source message, not a
        # new delay draw every time a quote or markout reads the same source.
        self.state.exec_message_schedules: dict[str, self.services.Any] = {}
        self.state.exec_message_payload: self.services.Mapping[str, self.services.Any] = {}
        self.state.exec_message_missing_source_skip_count = 0
        self.state.exec_message_trade_event_rows = self.services.np.empty(0, dtype=self.services.np.int64)
        if self.state.exec_book_visibility_mode == "message_schedule":
            from models.exchange_book_replay import HistoricalMessageDeliverySchedule
            self.context.HistoricalMessageDeliverySchedule = HistoricalMessageDeliverySchedule
            from strategy.signal import QuoteDecisionSnapshot
            self.context.QuoteDecisionSnapshot = QuoteDecisionSnapshot

            self.state.raw_delivery = self.context.params.get("_exec_message_delivery")
            if not isinstance(self.state.raw_delivery, self.services.Mapping):
                raise ValueError("message_schedule requires source message delivery arrays")
            self.state.child_time_consumers = [
                name for name in (
                    "local_extreme_guard_enabled",
                    "queue_value_keep_cancel_enabled",
                    "dynamic_fill_hazard_action_enabled",
                    "dynamic_fill_hazard_shadow_enabled",
                    "trace_local_order_value_max",
                    "trace_first_add_decision_to_terminal_max",
                    "trace_first_opener_decision_to_terminal_max",
                ) if self.context.params.get(name, False)
            ]
            if self.state.child_time_consumers:
                raise NotImplementedError(
                    "These legacy consumers still use individual child-time information, "
                    "not delivered aggregate packets: " + ", ".join(self.state.child_time_consumers)
                )
            self.state.exec_message_payload = self.state.raw_delivery
            self.state.required_sources = {"bbo", "depth", "trade", "variance"}
            if self.context.ml_data is not None:
                self.state.required_sources.add("prediction")
            if not self.state.required_sources.issubset(self.state.raw_delivery):
                raise ValueError(
                    "message_schedule is missing source clocks: "
                    + ", ".join(sorted(self.state.required_sources - set(self.state.raw_delivery)))
                )
            self.state.aligned_sources = {
                "bbo": self.state.bbo_ts,
                "depth": self.state.l2_ts,
                "variance": self.context.var_ts_ms,
                "prediction": self.state.ml_ts if self.context.ml_data is not None else None,
            }
            for loop_item in self.state.raw_delivery.items():
                self.state.feed, self.state.rows = loop_item
                if not isinstance(self.state.rows, self.services.Mapping):
                    raise ValueError(f"message_schedule {self.state.feed} must contain clock arrays")
                if {"connection_ids", "channel_ids", "source_ordinal"}.intersection(self.state.rows):
                    raise ValueError(
                        "message_schedule shared-connection ordering must be preprojected "
                        "across feeds before supplying source clock arrays"
                    )
                self.state.schedule = self.context.HistoricalMessageDeliverySchedule(
                    self.state.rows["exchange_ts_ns"],
                    self.state.rows["receive_ts_ns"],
                    self.state.rows["feature_ready_ts_ns"],
                )
                self.state.expected = self.state.aligned_sources.get(str(self.state.feed))
                if str(self.state.feed) in self.state.aligned_sources and (
                    self.state.expected is None
                    or not self.services.np.array_equal(
                        self.state.schedule.exchange_ns_for_channel(),
                        self.services.np.asarray(self.state.expected, dtype=self.services.np.int64) * 1_000_000,
                    )
                ):
                    raise ValueError(f"message_schedule {self.state.feed} is not aligned with its values")
                self.state.exec_message_schedules[str(self.state.feed)] = self.state.schedule
                if self.state.feed in {"bbo", "depth"}:
                    self.context.book = self.context.bbo_data if self.state.feed == "bbo" else self.context.l2_data
                    self.context.source_keys = {"source_observation_ts_ns", "source_receive_ts_ns", "source_feature_ready_ts_ns"}
                    if self.context.source_keys.intersection(self.state.rows):
                        if not self.context.source_keys.issubset(self.state.rows):
                            raise ValueError("book message source clocks must be supplied together")
                        self.context.physical = self.context.HistoricalMessageDeliverySchedule(
                            *(self.state.rows[key] for key in (
                                "source_observation_ts_ns", "source_receive_ts_ns", "source_feature_ready_ts_ns")),
                        )
                        if (not self.services.np.array_equal(self.context.physical.exchange_ns_for_channel(), self.services.book_observation_times_us(self.context.book) * 1000)
                                or not self.services.np.array_equal(self.context.physical.ready_ns_for_channel(), self.state.rows["source_feature_ready_ts_ns"])
                                or self.services.np.any(self.context.physical.ready_ns_for_channel() > self.state.schedule.ready_ns_for_channel())):
                            raise ValueError("book source clocks do not align with observation/visibility")
                        self.context.repeated = self.services.np.diff(self.context.physical.exchange_ns_for_channel()) == 0
                        if (self.services.np.any(self.services.np.diff(self.context.physical.receive_ns_for_channel())[self.context.repeated] != 0)
                                or self.services.np.any(self.services.np.diff(self.context.physical.ready_ns_for_channel())[self.context.repeated] != 0)):
                            raise ValueError("carried book must reuse its original source delivery")
                    elif getattr(self.context.book, "observation_ts_us", None) is not None and self.services.np.any(self.context.book.observation_ts_us >= 0):
                        raise ValueError("bound observation books require source message clocks")
            self.state.child_rows = self.services.np.asarray(self.state.raw_delivery["trade"].get("last_child_row_index", []))
            if (
                self.state.child_rows.dtype.kind not in "iu"
                or self.state.child_rows.ndim != 1
                or self.state.child_rows.size != len(self.state.exec_message_schedules["trade"].ready_ns_for_channel())
                or self.services.np.any(self.state.child_rows < -1)
                or self.services.np.any(self.state.child_rows >= self.state.n_execution_trades)
                or self.services.np.any(self.state.child_rows[1:] < self.state.child_rows[:-1])
            ):
                raise ValueError("trade messages require ordered last_child_row_index mapping")
            self.state.exec_message_trade_event_rows = self.services.np.flatnonzero(self.state.is_execution_trade)
            self.state.visible_children = self.services.np.asarray(self.state.raw_delivery["trade"].get(
                "visible_child_mask", self.services.np.ones(self.state.n_execution_trades, dtype=self.services.np.bool_),
            ))
            if self.state.visible_children.dtype != self.services.np.bool_ or self.state.visible_children.shape != (self.state.n_execution_trades,):
                raise ValueError("trade visible_child_mask must align with individual execution rows")
            if self.services.np.any(~self.state.visible_children[self.state.child_rows[self.state.child_rows >= 0]]):
                raise ValueError("trade parent last child must be policy-visible")
            self.state.policy_visible_execution[self.state.exec_message_trade_event_rows] = self.state.visible_children
            if "mark_price" in self.state.raw_delivery["trade"]:
                self.state.mark_prices = self.services.np.asarray(self.state.raw_delivery["trade"]["mark_price"])
                if (self.state.mark_prices.shape != self.state.child_rows.shape or self.state.mark_prices.dtype.kind not in "fi"
                        or not self.services.np.all(self.services.np.isfinite(self.state.mark_prices)) or self.services.np.any(self.state.mark_prices <= 0.0)):
                    raise ValueError("trade parent mark_price must be positive and aligned")
            self.state.parents_with_children = self.state.child_rows >= 0
            self.state.child_exchange_ns = (
                self.state.trade_ts[self.state.exec_message_trade_event_rows[self.state.child_rows[self.state.parents_with_children]]]
                .astype(self.services.np.int64, copy=False) * 1_000_000
            )
            if self.services.np.any(
                self.state.exec_message_schedules["trade"].ready_ns_for_channel()[self.state.parents_with_children]
                < self.state.child_exchange_ns
            ):
                raise ValueError("trade message cannot be ready before its final child execution")
            if self.state.use_bar_pricing or self.state.exec_depth_visibility_source_offset_ms:
                raise ValueError(
                    "message_schedule requires tick pricing without a source-time offset"
                )
        if (
            self.state.trace_first_add_decision_to_terminal is not None
            or self.state.trace_first_opener_decision_to_terminal is not None
            or self.state.trace_decisions is not None
            or self.state.ema_add_wait_fork_enabled
            or bool(self.state.exec_message_schedules)
        ):
            if self.context.prepared_event_identity and not self.state.exec_message_schedules:
                (self.state.execution_qty, self.state.taker_buy_qty_cumulative,
                 self.state.taker_sell_qty_cumulative, self.state.last_execution_event_index
                 ) = self.context._prepared_clock_owner.execution_inputs(self.context.prepared_event_identity[0], cumulative=True)
            else:
                # Message delivery modifies the visibility mask: never reuse the
                # ordinary clock-only cumulative arrays for that separate contract.
                self.state.execution_qty = self.services.np.where(
                    self.state.policy_visible_execution,
                    self.services.np.asarray(self.state.trade_qty, dtype=self.services.np.float64),
                    0.0,
                )
                self.state.taker_buy_qty_cumulative = self.services.np.concatenate(
                    ([0.0], self.services.np.cumsum(self.services.np.where(self.state.is_seller, 0.0, self.state.execution_qty)))
                )
                self.state.taker_sell_qty_cumulative = self.services.np.concatenate(
                    ([0.0], self.services.np.cumsum(self.services.np.where(self.state.is_seller, self.state.execution_qty, 0.0)))
                )
                self.state.last_execution_event_index = self.services.np.maximum.accumulate(
                    self.services.np.where(self.state.policy_visible_execution, self.services.np.arange(self.state.n_trades), -1)
                )
        else:
            self.state.taker_buy_qty_cumulative = self.services.np.empty(0, dtype=self.services.np.float64)
            self.state.taker_sell_qty_cumulative = self.services.np.empty(0, dtype=self.services.np.float64)
            self.state.last_execution_event_index = self.services.np.empty(0, dtype=self.services.np.int64)

        # ── State ──
        self.state.initial_inventory = float(self.context.params.get("initial_inventory", 0.0) or 0.0)
        self.state.initial_entry_price = float(self.context.params.get("initial_entry_price", 0.0) or 0.0)
        self.state.initial_live_state = self.context.params.get("initial_live_state") or {}
        if not isinstance(self.state.initial_live_state, dict):
            self.state.initial_live_state = {}
        self.state.initial_live_state_enabled = bool(self.state.initial_live_state)
        self.state.initial_live_state_warning = ""
        self.state.initial_live_state_orders_requested = int(len(self.state.initial_live_state.get("active_orders", []) or []))
        self.state.initial_live_state_orders_restored = 0
        self.state.initial_live_state_bid_orders_restored = 0
        self.state.initial_live_state_ask_orders_restored = 0
        self.state.initial_live_state_queue_estimated_count = 0
        self.state.q = self.state.initial_inventory
        self.state.exchange_inventory = self.state.initial_inventory
        self.state.cash = -self.state.initial_inventory * self.state.initial_entry_price if self.state.initial_entry_price > 0.0 else 0.0
        # Matching facts belong to the exchange simulation, not notification order.
        self.state.economic_fill_facts = []
        self.state.economic_match_cash = float(self.state.cash)
        self.state.economic_match_inventory = float(self.state.q)
        self.state.hard_risk_clock_ts_ms = int(self.state.trade_ts[0])
        self.state.hard_risk_mark_price = float(self.state.trade_price[0])
        self.state.initial_risk_equity = self.state.cash + self.state.q * self.state.hard_risk_mark_price
        self.state.risk_state = self.services.ReplayHardRiskState(
            utc_day=self.state.hard_risk_clock_ts_ms // 86_400_000,
            session_peak_pnl=max(0.0, self.state.initial_risk_equity),
            last_total_pnl=self.state.initial_risk_equity,
        )
        self.state.supplied_risk_state = self.state.initial_live_state.get("risk_state")
        if self.state.supplied_risk_state is not None:
            self.state.risk_state = self.services.ReplayHardRiskState.restore(
                self.state.supplied_risk_state, start_ts_ms=self.state.hard_risk_clock_ts_ms,
            )
        self.state.risk_daily_loss_block_count = 0
        self.state.risk_position_value_block_count = 0
        self.state.risk_emergency_close_count = 0
        self.state.risk_notional_cap_count = 0
        self.state.risk_emergency_latched = False
        self.state.risk_emergency_attempt_complete = False
        self.state.risk_emergency_ownership_conflict_count = 0
        self.state.trace_risk_actions = []
        (
            self.state.expected_loss_limit,
            self.state._,
            self.state.expected_loss_cooldown_ms,
        ) = self.services.loss_cooldown_config_values(
            self.context.params
        )
        self.state.loss_cooldown_snapshot = self.state.initial_live_state.get(
            "reward_path_loss_cooldown"
        ) or self.state.initial_live_state.get("loss_cooldown_snapshot")
        if isinstance(self.state.loss_cooldown_snapshot, self.services.Mapping) and self.state.loss_cooldown_snapshot:
            self.state.consecutive_loss_cooldown = self.services.ConsecutiveLossCooldown.restore(
                self.state.loss_cooldown_snapshot
            )
            if (
                self.state.consecutive_loss_cooldown.max_consecutive_losses
                != self.state.expected_loss_limit
                or self.state.consecutive_loss_cooldown.cooldown_ms
                != self.state.expected_loss_cooldown_ms
            ):
                raise ValueError(
                    "restored loss-cooldown config disagrees with replay params"
                )
            if not self.services.math.isclose(
                self.state.consecutive_loss_cooldown.inventory,
                self.state.initial_inventory,
                rel_tol=0.0,
                abs_tol=1e-10,
            ) or not self.services.math.isclose(
                self.state.consecutive_loss_cooldown.avg_entry,
                self.state.initial_entry_price,
                rel_tol=0.0,
                abs_tol=1e-10,
            ):
                raise ValueError(
                    "restored loss-cooldown inventory disagrees with replay initial state"
                )
        else:
            self.state.consecutive_loss_cooldown = self.services.ConsecutiveLossCooldown(
                max_consecutive_losses=self.state.expected_loss_limit,
                cooldown_ms=self.state.expected_loss_cooldown_ms,
                inventory=self.state.initial_inventory,
                avg_entry=self.state.initial_entry_price,
            )
        self.state.consecutive_loss_cooldown_block_count = 0
        self.state.consecutive_loss_cooldown_cancel_count = 0
        self.state.sync_adjust_degrade_until_ms = 0
        self.state.sync_adjust_degrade_trigger_count = 0
        self.state.sync_adjust_degrade_block_bid_count = 0
        self.state.sync_adjust_degrade_block_ask_count = 0
        self.state.sync_adjust_censored = False
        self.state.sync_adjust_censor_ts_ms = 0


        self.state.bp = 0.0
        self.state.ap = 0.0
        self.state.bid_sz = self.state.order_size
        self.state.ask_sz = self.state.order_size
        self.state.last_rq_ts = self.state.trade_ts[0] - self.state.rq_ms
        self.state.restored_last_requote_ts = self.state.initial_live_state.get("last_requote_ts_ms")
        if self.state.restored_last_requote_ts is not None:
            self.state.last_rq_ts = int(self.state.restored_last_requote_ts)
            if self.state.last_rq_ts > int(self.state.trade_ts[0]):
                raise ValueError("restored last_requote_ts_ms is after replay start")
        self.state.mid = self.state.trade_price[0]

        # σ² lookup
        self.state.var_idx = -1
        self.state.sigma_sq = 1.0

        # Stats
        self.state.nfb = 0
        self.state.nfa = 0
        self.state.nrq = 0
        self.state.nto = 0
        self.state.circuit_breaker_count = 0
        self.state.circuit_breaker_closing = False
        self.state.circuit_breaker_close_start_ts = -1
        self.state.circuit_breaker_close_place_count = 0
        self.state.circuit_breaker_close_keep_count = 0
        self.state.circuit_breaker_close_fill_count = 0
        self.state.circuit_breaker_close_gtx_reject_count = 0
        self.state.circuit_breaker_close_gtx_reject_streak = 0
        self.state.circuit_breaker_close_ioc_place_count = 0
        self.state.circuit_breaker_close_ioc_fill_count = 0
        self.state.circuit_breaker_close_ioc_expire_count = 0
        self.state.exec_book_visibility_delay_applied_count = 0
        self.state.exec_book_visibility_delay_sum_ms = 0.0
        self.state.exec_book_visibility_delay_max_ms = 0
        self.state.exec_depth_visibility_delay_sum_ms = 0.0
        self.state.exec_depth_visibility_delay_max_ms = 0
        self.state.exec_trade_visibility_delay_sum_ms = 0.0
        self.state.exec_trade_visibility_delay_max_ms = 0
        self.state.exec_book_visibility_paired_hit_count = 0
        self.state.exec_book_visibility_paired_miss_count = 0
        self.state.exec_book_visibility_mid_override_count = 0
        self.state.tsprd = 0.0
        self.state.tsprd_final = 0.0
        self.state.n_final_spread = 0
        self.state.cap_hit_count = 0
        self.state.delta_cap_hit_count = 0
        self.state.final_cap_compress_count = 0
        self.state.delta_pre_cap_sum = 0.0
        self.state.delta_after_cap_sum = 0.0
        self.state.cap_bps_sum = 0.0
        self.state.bid_dist_sum = 0.0
        self.state.ask_dist_sum = 0.0
        self.state.quote_skew_sum = 0.0
        self.state.quote_abs_skew_sum = 0.0
        self.state.quote_abs_skew_gt_10_count = 0
        self.state.quote_abs_skew_gt_25_count = 0
        self.state.bid_tighter_count = 0
        self.state.ask_tighter_count = 0
        self.state.quote_balanced_count = 0
        self.state.stale_book_skip_count = 0
        self.state.stale_book_cancel_count = 0
        self.state.stale_book_age_sum = 0.0
        self.state.stale_book_age_max = 0.0
        self.state.quote_spread_lt_100_count = 0
        self.state.quote_spread_lt_150_count = 0
        self.state.final_spread_lt_100_count = 0
        self.state.final_spread_lt_150_count = 0
        self.state.mx_inv = 0.0
        self.state.si_sum = 0.0
        self.state.buy_fill_qty_total = 0.0
        self.state.sell_fill_qty_total = 0.0
        self.state.buy_fill_notional = 0.0
        self.state.sell_fill_notional = 0.0
        self.state.signed_inventory_time_s = 0.0
        self.state.abs_inventory_time_s = 0.0
        self.state.sq_inventory_time_s = 0.0
        self.state.signed_notional_inventory_time_s = 0.0
        self.state.notional_inventory_time_s = 0.0

        # InventoryLifecycle-level inventory risk audit.
        # 中文说明：回测 summary 也按 flat->nonzero->flat 的库存周期统计风险，
        # 用来和 live HEALTH/影子日志比较“自然减仓是否拖成大库存 inventory_lifecycle”。
        self.state.inventory_lifecycle_active = abs(self.state.q) > 1e-10
        self.state.inventory_lifecycle_count = 1 if self.state.inventory_lifecycle_active else 0
        self.state.inventory_lifecycle_closed_count = 0
        self.state.inventory_lifecycle_start_ts = int(self.state.trade_ts[0]) if self.state.inventory_lifecycle_active and self.state.n_trades > 0 else 0
        self.state.inventory_lifecycle_start_pnl = self.state.cash + self.state.q * float(self.state.trade_price[0]) if self.state.inventory_lifecycle_active and self.state.n_trades > 0 else 0.0
        self.state.inventory_lifecycle_cur_max_abs = abs(self.state.q) if self.state.inventory_lifecycle_active else 0.0
        self.state.inventory_lifecycle_cur_min_pnl = 0.0
        self.state.inventory_lifecycle_max_abs_inventory = abs(self.state.q) if self.state.inventory_lifecycle_active else 0.0
        self.state.inventory_lifecycle_max_duration_s = 0.0
        self.state.inventory_lifecycle_max_adverse_excursion = 0.0
        self.state.inventory_lifecycle_exposure_increasing_fills = 0
        self.state.inventory_lifecycle_reducing_fills = 0
        self.state.inventory_lifecycle_buy_fills = 0
        self.state.inventory_lifecycle_sell_fills = 0
        # InventoryLifecycle-local counters feed decision covariates and reset at every
        # flat/flip boundary.  Separate run totals preserve the historical summary
        # denominator and start from zero for each replay segment.
        self.state.inventory_lifecycle_run_exposure_increasing_fills = 0
        self.state.inventory_lifecycle_run_reducing_fills = 0
        self.state.inventory_lifecycle_run_buy_fills = 0
        self.state.inventory_lifecycle_run_sell_fills = 0
        self.state.inventory_lifecycle_economic_fill_leg_trace: list[dict[str, self.services.Any]] = []
        self.state.inventory_lifecycle_inventory_time_to_date_btc_s = 0.0
        self.state.inventory_lifecycle_inventory_time_last_ts_ms = (
            int(self.state.trade_ts[0]) if self.state.inventory_lifecycle_active and self.state.n_trades > 0 else 0
        )
        self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(self.state.q)
        # Keep F10 inventory_lifecycle-local counters aligned with live InventoryManager.
        # Run-total counters above remain isolated summary/denominator fields.
        self.state.first_add_inventory_lifecycle_exposure_increasing_fills = 0
        self.state.first_add_inventory_lifecycle_reducing_fills = 0
        self.state.inventory_lifecycle_shadow_inv_006_blocks = 0
        self.state.inventory_lifecycle_shadow_inv_008_blocks = 0
        self.state.inventory_lifecycle_shadow_inv_010_blocks = 0
        self.state.inventory_lifecycle_shadow_age_20m_blocks = 0
        self.state.inventory_lifecycle_shadow_age_40m_blocks = 0
        self.state.inventory_lifecycle_shadow_age_60m_blocks = 0
        self.state.inventory_lifecycle_shadow_reducing_only_blocks = 0
        self.state.inventory_lifecycle_stop_add_count = 0
        self.state.bid_inventory_lifecycle_stop_add_count = 0
        self.state.ask_inventory_lifecycle_stop_add_count = 0
        self.state.inventory_lifecycle_soft_control_count = 0
        self.state.bid_inventory_lifecycle_soft_control_count = 0
        self.state.ask_inventory_lifecycle_soft_control_count = 0
        self.state.multi_market_policy_eval_count = 0
        self.state.multi_market_policy_hit_count = 0
        self.state.multi_market_policy_effective_block_count = 0
        self.state.bid_multi_market_policy_hit_count = 0
        self.state.ask_multi_market_policy_hit_count = 0
        self.state.bid_multi_market_policy_effective_block_count = 0
        self.state.ask_multi_market_policy_effective_block_count = 0
        self.state.adaptive_add_cooldown_hit_count = 0
        self.state.adaptive_add_cooldown_bid_hit_count = 0
        self.state.adaptive_add_cooldown_ask_hit_count = 0
        self.state.adaptive_add_cooldown_trace: list[dict] = []
        self.state.post_fill_quote_response_eval_count = 0
        self.state.post_fill_quote_response_active_count = 0
        self.state.post_fill_quote_response_inventory_count = 0
        self.state.post_fill_quote_response_flow_count = 0
        self.state.post_fill_quote_response_cap_limited_count = 0
        self.state.post_fill_quote_response_post_only_clamp_count = 0
        self.state.post_fill_quote_response_add_fill_count = 0
        self.state.post_fill_quote_response_bid_add_count = 0
        self.state.post_fill_quote_response_ask_add_count = 0
        self.state.post_fill_quote_response_inventory_ticks_sum = 0.0
        self.state.post_fill_quote_response_add_ticks_sum = 0.0
        self.state.post_fill_quote_response_add_ticks_max = 0.0
        self.state.post_fill_quote_response_half_life_sum = 0.0
        self.state.post_fill_quote_response_force_bid_requote = False
        self.state.post_fill_quote_response_force_ask_requote = False



























        if self.state.initial_live_state_enabled and isinstance(self.state.initial_live_state.get("inventory_lifecycle"), dict):
            self.state.inventory_lifecycle_state = self.state.initial_live_state.get("inventory_lifecycle") or {}
            if bool(self.state.inventory_lifecycle_state.get("active", self.state.inventory_lifecycle_active)) and self.state.inventory_lifecycle_active and self.state.n_trades > 0:
                # 中文说明：小时级 live/replay 对齐时，不能把已有库存 inventory_lifecycle
                # 当成刚开始。这里用起点前 HEALTH 的 inventory_lifecycle age/MAE/counters
                # 恢复风险状态，但不把它用于日度 fresh-start promotion 证据。
                self.state.start_ts_ms = int(self.state.inventory_lifecycle_state.get("start_ts_ms", 0) or 0)
                if self.state.start_ts_ms > 0:
                    self.state.inventory_lifecycle_start_ts = min(int(self.state.trade_ts[0]), self.state.start_ts_ms)
                    self.state.age_s = max(
                        0.0,
                        float(int(self.state.trade_ts[0]) - self.state.inventory_lifecycle_start_ts) / 1000.0,
                    )
                else:
                    self.state.age_s = max(
                        0.0,
                        float(self.state.inventory_lifecycle_state.get("age_s", 0.0) or 0.0),
                    )
                    self.state.inventory_lifecycle_start_ts = int(self.state.trade_ts[0] - self.state.age_s * 1000.0)
                self.state.inventory_lifecycle_pnl = float(self.state.inventory_lifecycle_state.get("pnl", 0.0) or 0.0)
                self.state.inventory_lifecycle_start_pnl = self._inventory_lifecycle_pnl(float(self.state.trade_price[0])) - self.state.inventory_lifecycle_pnl
                self.state.inventory_lifecycle_state_max_inv = abs(float(self.state.inventory_lifecycle_state.get("max_inventory", self.state.inventory_lifecycle_cur_max_abs) or self.state.inventory_lifecycle_cur_max_abs))
                self.state.inventory_lifecycle_cur_max_abs = max(abs(self.state.q), self.state.inventory_lifecycle_state_max_inv)
                self.state.inventory_lifecycle_max_abs_inventory = max(self.state.inventory_lifecycle_max_abs_inventory, self.state.inventory_lifecycle_cur_max_abs)
                self.state.mae = float(self.state.inventory_lifecycle_state.get("mae", 0.0) or 0.0)
                self.state.inventory_lifecycle_cur_min_pnl = min(0.0, self.state.mae)
                self.state.inventory_lifecycle_max_adverse_excursion = min(self.state.inventory_lifecycle_max_adverse_excursion, self.state.inventory_lifecycle_cur_min_pnl)
                self.state.inventory_lifecycle_max_duration_s = max(self.state.inventory_lifecycle_max_duration_s, self.state.age_s)
                self.state.inventory_lifecycle_exposure_increasing_fills = int(self.state.inventory_lifecycle_state.get("inc_fills", self.state.inventory_lifecycle_exposure_increasing_fills) or 0)
                self.state.inventory_lifecycle_reducing_fills = int(self.state.inventory_lifecycle_state.get("red_fills", self.state.inventory_lifecycle_reducing_fills) or 0)
                self.state.inventory_lifecycle_buy_fills = int(self.state.inventory_lifecycle_state.get("buy_fills", self.state.inventory_lifecycle_buy_fills) or 0)
                self.state.inventory_lifecycle_sell_fills = int(self.state.inventory_lifecycle_state.get("sell_fills", self.state.inventory_lifecycle_sell_fills) or 0)


















        # PnL time series
        self.state.eff_rq_ms = self.state.rq_min_ms if self.state.dynamic_rq else self.state.rq_ms
        self.state.elapsed_ms_for_samples = max(0, int(self.state.trade_ts[-1] - self.state.trade_ts[0]))
        self.state.max_samples = min(
            4096, self.state.elapsed_ms_for_samples // max(1, self.state.eff_rq_ms) + 100,
        )
        self.state.pnl_arr = self.services.np.empty(self.state.max_samples, self.services.np.float64)
        self.state.inv_arr = self.services.np.empty(self.state.max_samples, self.services.np.float64)
        self.state.ts_arr = self.services.np.empty(self.state.max_samples, self.services.np.int64)
        self.state.si = 0

        # Order lifecycle state: each side can have multiple overlapping orders
        # during cancel/new latency windows, matching live pending states.
        self.state.bid_orders = []
        self.state.ask_orders = []
        self.state.ORDER_PENDING_NEW = 0
        self.state.ORDER_OPEN = 1
        self.state.ORDER_PENDING_CANCEL = 2
        self.state.inferred_best_bid = self.state.trade_price[0] - self.state.TICK
        self.state.inferred_best_ask = self.state.trade_price[0] + self.state.TICK
        self.state.cur_best_bid = self.state.inferred_best_bid
        self.state.cur_best_ask = self.state.inferred_best_ask
        self.state.queue_value_prev_bid_qty: float | None = None
        self.state.queue_value_prev_ask_qty: float | None = None
        self.state.post_only_guard_hits = 0
        self.state.post_only_bid_hits = 0
        self.state.post_only_ask_hits = 0
        self.state.mid_guard_bid_hits = 0
        self.state.mid_guard_ask_hits = 0
        self.state.gtx_rejects = 0
        self.state.replace_throttle_count = 0
        self.state.replace_throttle_bid_count = 0
        self.state.replace_throttle_ask_count = 0
        self.state.replace_throttle_price_count = 0
        self.state.replace_throttle_age_count = 0
        self.state.decision_place_count = 0
        self.state.decision_replace_count = 0
        self.state.decision_keep_count = 0
        self.state.decision_pause_count = 0
        self.state.decision_none_count = 0
        self.state.decision_skip_filter_count = 0
        self.state.decision_pending_coalesce_count = 0
        self.state.decision_cancel_first_count = 0
        self.state.replacement_terminal_due_ts = {"BUY": -1, "SELL": -1}
        self.state.replacement_terminal_cadence_defer_until_ms = -1
        self.state.replacement_terminal_terminal_count = 0
        self.state.replacement_terminal_decision_count = 0
        self.state.replacement_terminal_bid_decision_count = 0
        self.state.replacement_terminal_ask_decision_count = 0
        self.state.replacement_terminal_decision_latency_sum_ms = 0
        self.state.replacement_terminal_decision_latency_max_ms = 0
        self.state.decision_side_role_action_counts: dict[str, int] = {}
        self.state.fills_while_pending_cancel = 0
        self.state.max_pending_new_orders = 0
        self.state.max_pending_cancel_orders = 0
        self.state.final_cap_rounding_count = 0
        self.state.final_cap_mid_guard_count = 0
        self.state.final_cap_post_only_count = 0
        self.state.final_cap_delta_count = 0
        self.state.cap_exposure_block_count = 0
        self.state.bid_cap_exposure_block_count = 0
        self.state.ask_cap_exposure_block_count = 0
        self.state.random_passive_mirror_count = 0
        self.state.random_passive_mirror_eligible_count = 0
        self.state.random_passive_timing_jitter_count = 0
        self.state.final_cap_excess_sum = 0.0
        self.state.final_cap_excess_max = 0.0
        self.state.post_policy_cap_count = 0
        self.state.post_policy_cap_excess_sum = 0.0
        self.state.post_policy_cap_excess_max = 0.0
        self.state.fills_bid_final_compressed = 0
        self.state.fills_ask_final_compressed = 0
        self.state.fills_bid_not_final_compressed = 0
        self.state.fills_ask_not_final_compressed = 0
        self.state.fills_bid_mid_guard = 0
        self.state.fills_ask_mid_guard = 0
        self.state.fills_bid_post_only = 0
        self.state.fills_ask_post_only = 0
        self.state.adverse_guard_count = 0
        self.state.adverse_pause_count = 0
        self.state.bid_adverse_guard_count = 0
        self.state.bid_adverse_toxicity_count = 0
        self.state.bid_adverse_markout_count = 0
        self.state.bid_adverse_direction_count = 0
        self.state.bid_adverse_ret_count = 0
        self.state.bid_adverse_weighted_mid_proxy_count = 0
        self.state.bid_adverse_thin_depth_count = 0
        self.state.bid_adverse_pause_count = 0
        self.state.fills_bid_adverse_guard = 0
        self.state.ask_adverse_guard_count = 0
        self.state.ask_adverse_toxicity_count = 0
        self.state.ask_adverse_markout_count = 0
        self.state.ask_adverse_direction_count = 0
        self.state.ask_adverse_ret_count = 0
        self.state.ask_adverse_weighted_mid_proxy_count = 0
        self.state.ask_adverse_thin_depth_count = 0
        self.state.ask_adverse_pause_count = 0
        self.state.fills_ask_adverse_guard = 0
        self.state.defense_guard_count = 0
        self.state.defense_pause_count = 0
        self.state.bid_defense_guard_count = 0
        self.state.bid_defense_pause_count = 0
        self.state.ask_defense_guard_count = 0
        self.state.ask_defense_pause_count = 0
        self.state.fills_bid_defense_guard = 0
        self.state.fills_ask_defense_guard = 0
        self.state.local_extreme_guard_count = 0
        self.state.local_extreme_pause_count = 0
        self.state.bid_local_extreme_guard_count = 0
        self.state.ask_local_extreme_guard_count = 0
        self.state.fills_bid_local_extreme_guard = 0
        self.state.fills_ask_local_extreme_guard = 0
        self.state.fragile_ttl_cancel_count = 0
        self.state.flat_unilateral_release_count = 0
        self.state.flat_unilateral_bid_release_count = 0
        self.state.flat_unilateral_ask_release_count = 0
        self.state.flat_unilateral_bid_started_ms = 0
        self.state.flat_unilateral_ask_started_ms = 0

        # Position tracking
        self.state.pos_open_ts = self.state.trade_ts[0]
        self.state.entry_price = self.state.initial_entry_price if abs(self.state.initial_inventory) > 1e-10 else 0.0

        # Dynamic RQ state
        self.state.ema_var_fast = 0.0
        self.state.ema_var_slow = 0.0
        self.state.dyn_rq_inited = False
        self.state.main_loop_rq_var_idx = 0
        # Cursor ownership follows the actual consumer, not just its config switch.
        # Public-input replay supplies variance but no squared-return sequence.
        self.state.main_loop_rq_input_active = bool(
            self.state.main_loop_enabled and self.state.dynamic_rq and self.context.var_retsq is not None
        )
        self.state.cur_rq_ms = self.state.rq_ms
        self.state.rq_sum = 0.0

        # BER state
        self.state.ema_ti_fast = 0.0
        self.state.ema_ti_slow = 0.0
        self.state.trade_intensity_acceleration_guard_active = False
        self.state.ber_inited = False
        self.state.ber_held_ti = 50.0
        self.state.ber_pending_ti = 50.0
        self.state.ber_pending_feature_ts_ms = -1
        self.state.ber_published_feature_ts_ms = -1
        self.state.ber_feature_publish_count = 0
        self.state.ber_active_count = 0
        self.state.ber_role_safe_decision_count = 0
        self.state.ber_role_safe_buy_add_count = 0
        self.state.ber_role_safe_sell_add_count = 0
        self.state.ber_role_safe_flat_bypass_count = 0
        self.state.ber_role_safe_mixed_fail_closed_count = 0
        self.state.ber_role_safe_pair_change_count = 0
        self.state.ber_role_safe_bid_change_count = 0
        self.state.ber_role_safe_ask_change_count = 0
        self.state.ber_role_safe_source_mismatch_count = 0
        self.state.ber_role_safe_cap_collision_count = 0
        self.state.ber_role_safe_cap_infeasible_count = 0
        self.state.cur_ti = self.state.liq_baseline  # current trade intensity
        # var_ti is mean aggregate count per 1s bar. Live QuoteState consumes
        # mean count per completed 10s bar, held between feature publications.
        self.state.quote_ti_source_indices = (
            self.services.np.flatnonzero(
                (self.services.np.asarray(self.context.var_ts_ms[:len(self.context.var_ti)], dtype=self.services.np.int64) + 1_000) % 10_000 == 0
            ) if self.context.var_ti is not None else self.services.np.empty(0, dtype=self.services.np.int64)
        )
        self.state.quote_ti_cursor = 0
        self.state.quote_ti_publish_count = 0
        self.state.quote_ti_completed_ts_ms = -1
        self.state.exec_signal_visible_ts_ms = (
            0 if self.state.exec_message_schedules else int(self.state.trade_ts[0])
        )

        # Markout state
        self.state.mo_alpha = (
            2.0 / (self.state.markout_ema_span_fills + 1.0)
            if self.state.markout_ema_span_fills > 0
            else 0.0
        )
        self.state.mo_ema_bid = 0.0
        self.state.mo_ema_ask = 0.0
        self.state.mo_ema_all = 0.0
        self.state.mo_ref = 50.0
        self.state.mo_pending = []  # (fill_ts_ms, fill_price, is_bid_fill, flags, fill_qty_btc)
        self.state.mo_observation_sequence = 0
        self.state.mo_observed_pause = {}
        self.state.mo_observation_price = {}
        self.state.mo_last_decay_ts = int(self.state.trade_ts[0]) if self.state.n_trades > 0 else 0
        self.state.mo_pause_until_bid_ms = 0
        self.state.mo_pause_until_ask_ms = 0
        self.state.mo_pause_extend_bid_count = 0
        self.state.mo_pause_extend_ask_count = 0
        self.state.mo_stale_drop_count = 0
        self.state.mo_sum_all = 0.0
        self.state.mo_sum_bid = 0.0
        self.state.mo_sum_ask = 0.0
        self.state.mo_count_all = 0
        self.state.mo_count_bid = 0
        self.state.mo_count_ask = 0
        self.state.mo_qty_all = 0.0
        self.state.mo_qty_bid = 0.0
        self.state.mo_qty_ask = 0.0
        self.state.mo_sum_final_compressed = 0.0
        self.state.mo_count_final_compressed = 0
        self.state.mo_qty_final_compressed = 0.0
        self.state.mo_sum_not_final_compressed = 0.0
        self.state.mo_count_not_final_compressed = 0
        self.state.mo_qty_not_final_compressed = 0.0

        self.state.inventory_pnl = 0.0
        self.state.prev_mark_px = self.state.trade_price[0]
        self.state.local_path_accrual_ts_ms = int(self.state.trade_ts[0])

        # Ret demean state
        self.state.ema_pr = 0.0
        self.state.alpha_dm = 2.0 / (self.state.ret_demean_halflife + 1.0) if self.state.ret_demean_halflife > 0 else 0.0

        # Step 27: Fill cooldown state
        self.state.last_buy_fill_ts = self.state.trade_ts[0] - 999999999
        self.state.last_sell_fill_ts = self.state.trade_ts[0] - 999999999
        self.state.consec_buy = 0.0
        self.state.consec_sell = 0.0
        self.state.last_buy_fill_cooldown_ms = 0.0
        self.state.last_sell_fill_cooldown_ms = 0.0
        # Preserve the original episode duration after the active countdown clears.
        # This lets the audit compare early probes with the actual baseline rearm
        # boundary, including 85s * consecutive-fill episodes.
        self.state.last_buy_fill_cooldown_total_ms = 0.0
        self.state.last_sell_fill_cooldown_total_ms = 0.0
        self.state.variance_episode_buy: self.services.OnlineVarianceTimeEpisode | None = None
        self.state.variance_episode_sell: self.services.OnlineVarianceTimeEpisode | None = None


























        self.state.rest_gateway_plan_active = False
        self.state.rest_gateway_last_slot_index = -1









































































































        self.state.same_ms_trade_extremes_cache_ts: int | None = None
        self.state.same_ms_trade_extremes_cache: tuple[int | None, int | None] = (None, None)





































































        self.state.adverse_markout_pause_threshold_eff = self.state.adverse_markout_pause_threshold
        if self.state.adverse_markout_pause_threshold_eff <= 0.0:
            self.state.adverse_markout_pause_threshold_eff = self.state.adverse_markout_threshold
        self.state.adverse_markout_pause_threshold_eff = max(self.state.adverse_markout_pause_threshold_eff, 1e-9)
        self.state.adverse_markout_max_resolve_gap_ms = int(self.state.adverse_markout_max_resolve_gap_s * 1000.0)








        if self.state.initial_live_state_enabled:
            self.state.markout_state = self.state.initial_live_state.get("markout") or {}
            if isinstance(self.state.markout_state, dict):
                self.state.mo_ema_bid = float(self.state.markout_state.get("mo_ema_bid", self.state.mo_ema_bid) or self.state.mo_ema_bid)
                self.state.mo_ema_ask = float(self.state.markout_state.get("mo_ema_ask", self.state.mo_ema_ask) or self.state.mo_ema_ask)
                self.state.mo_ema_all = float(self.state.markout_state.get("mo_ema_all", self.state.mo_ema_all) or self.state.mo_ema_all)
                self.state.mo_pause_until_bid_ms = int(self.state.markout_state.get("mo_pause_until_bid_ms", self.state.mo_pause_until_bid_ms) or self.state.mo_pause_until_bid_ms)
                self.state.mo_pause_until_ask_ms = int(self.state.markout_state.get("mo_pause_until_ask_ms", self.state.mo_pause_until_ask_ms) or self.state.mo_pause_until_ask_ms)
                if "mo_last_decay_ts_ms" in self.state.markout_state:
                    self.state.mo_last_decay_ts = int(self.state.markout_state["mo_last_decay_ts_ms"])
                    if self.state.mo_last_decay_ts > int(self.state.trade_ts[0]):
                        raise ValueError("restored markout decay clock is after replay start")

        if self.state.initial_live_state_enabled:
            self.state.fill_cd_state = self.state.initial_live_state.get("fill_cooldown") or {}
            if isinstance(self.state.fill_cd_state, dict):
                self.state.last_buy_fill_ts = int(self.state.fill_cd_state.get("last_buy_fill_ts_ms", self.state.last_buy_fill_ts) or self.state.last_buy_fill_ts)
                self.state.last_sell_fill_ts = int(self.state.fill_cd_state.get("last_sell_fill_ts_ms", self.state.last_sell_fill_ts) or self.state.last_sell_fill_ts)
                self.state.consec_buy = float(self.state.fill_cd_state.get("consec_buy", self.state.consec_buy) or self.state.consec_buy)
                self.state.consec_sell = float(self.state.fill_cd_state.get("consec_sell", self.state.consec_sell) or self.state.consec_sell)
                self.state.last_buy_fill_cooldown_ms = float(
                    self.state.fill_cd_state.get("last_buy_fill_cooldown_ms", self.state.last_buy_fill_cooldown_ms)
                    or self.state.last_buy_fill_cooldown_ms
                )
                self.state.last_sell_fill_cooldown_ms = float(
                    self.state.fill_cd_state.get("last_sell_fill_cooldown_ms", self.state.last_sell_fill_cooldown_ms)
                    or self.state.last_sell_fill_cooldown_ms
                )
                self.state.last_buy_fill_cooldown_total_ms = float(
                    self.state.fill_cd_state.get(
                        "last_buy_fill_cooldown_total_ms",
                        self.state.last_buy_fill_cooldown_ms,
                    )
                    or self.state.last_buy_fill_cooldown_ms
                )
                self.state.last_sell_fill_cooldown_total_ms = float(
                    self.state.fill_cd_state.get(
                        "last_sell_fill_cooldown_total_ms",
                        self.state.last_sell_fill_cooldown_ms,
                    )
                    or self.state.last_sell_fill_cooldown_ms
                )
                if self.state.variance_time_enabled:
                    self.state.buy_episode_state = self.state.fill_cd_state.get("variance_time_buy_episode")
                    self.state.sell_episode_state = self.state.fill_cd_state.get("variance_time_sell_episode")
                    if isinstance(self.state.buy_episode_state, self.services.Mapping):
                        self.state.variance_episode_buy = self.services.OnlineVarianceTimeEpisode.restore(
                            self.state.buy_episode_state
                        )
                        if self.state.variance_episode_buy.side != "BUY":
                            raise ValueError("restored BUY variance episode has wrong side")
                    if isinstance(self.state.sell_episode_state, self.services.Mapping):
                        self.state.variance_episode_sell = self.services.OnlineVarianceTimeEpisode.restore(
                            self.state.sell_episode_state
                        )
                        if self.state.variance_episode_sell.side != "SELL":
                            raise ValueError("restored SELL variance episode has wrong side")





        self._restore_initial_live_orders()
        self.state.active_decision_bbo_idx: self.services.Optional[int] = None

        self.state.replay_event_cursor = {"index": 0, "started": False, "after_event": None}
        self.state.utc_accounting_marks = (
            self.services.UtcAccountingMarkRecorder(int(
                self.context.params.get("replay_event_clock_start_ts_ms", self.state.trade_ts[0])
            ))
            if self.context.params.get("record_utc_accounting", False) else None
        )


        from models.replay.runtime_input_window import (
            ensure_runtime_sample_capacity,
            release_runtime_input_scratch,
        )
        self.context.ensure_runtime_sample_capacity = ensure_runtime_sample_capacity
        release_runtime_input_scratch(self.state)
        self.context.pending_resume_event = None
        if self.context.resume_checkpoint is not None:
            if self.context.resume_checkpoint.get("schema") != "tick_replay_runtime.v1":
                raise ValueError("unsupported tick runtime checkpoint")
            self.context.window = (int(self.state.trade_ts[0]), int(self.state.trade_ts[-1]),
                      int(self.state.n_trades))
            if not self.context.resume_input_batch and tuple(self.context.resume_checkpoint["loaded_window"]) != self.context.window:
                raise ValueError("runtime checkpoint currently requires the same loaded window")
            if (self.context.checkpoint_at_ts_ms is not None
                    and int(self.context.checkpoint_at_ts_ms) <= int(self.context.resume_checkpoint["cut_ts_ms"])):
                raise ValueError("the next checkpoint must advance beyond the saved cut")
            from itertools import islice
            self.context.islice = islice
            self.context.fresh_book_scheduler = self.state.exchange_book_scheduler
            self.context.fresh_progress_callback = self.state.replay_progress_callback
            self.context.fresh_l2_journal = self.state.l2_journal
            self.context.fresh_target_journal = self.state.target_journal
            self.context.fresh_runtime = self.state
            # Admission is read-only: a rejected successor still leaves its input
            # checkpoint usable. Writer restoration and replay may mutate state,
            # so failures after the explicit transfer must leave it consumed.
            self.context.saved_runtime = self.context.resume_checkpoint["runtime"]
            saved_price = getattr(self.context.saved_runtime, 'quote_price_runtime', None)
            fresh_price = self.state.quote_price_runtime
            if ((saved_price is None) != (fresh_price is None)
                    or (fresh_price is not None and saved_price.contract != fresh_price.contract)):
                raise ValueError('checkpoint quote-price policy identity differs')
            self.context.saved_rearm = getattr(self.context.saved_runtime, 'cooldown_rearm_runtime', None)
            self.context.fresh_rearm = self.state.cooldown_rearm_runtime
            if ((self.context.saved_rearm is None) != (self.context.fresh_rearm is None)
                    or (self.context.fresh_rearm is not None and self.context.saved_rearm.contract != self.context.fresh_rearm.contract)):
                raise ValueError('checkpoint cooldown recovery identity differs')
            if getattr(self.context.saved_runtime, "quote_schedule_mode", "existing") != self.state.quote_schedule_mode:
                raise ValueError("checkpoint quote scheduling mode differs")
            if (getattr(self.context.saved_runtime, "target_observation", None) is None) != (
                self.state.target_observation is None
            ):
                raise ValueError("checkpoint target observation mode differs")
            self.context.saved_strategy = self.context.saved_runtime.public_strategy
            if self.context.public_strategy is not None and (
                self.context.saved_strategy is None or type(self.context.public_strategy) is not type(self.context.saved_strategy)
                or self.context.public_strategy.contract != self.context.saved_strategy.contract
            ):
                raise ValueError("checkpoint action policy identity differs")
            self.context.prefix = self.context.resume_checkpoint.get("l2_prefix")
            self.context.target_prefix = self.context.resume_checkpoint.get("target_prefix")
            if (self.context.target_prefix is None) != (self.context.fresh_target_journal is None):
                raise ValueError("target output resume requires its prefix and a fresh writer")
            if (self.context.prefix is None) != (self.context.fresh_l2_journal is None):
                raise ValueError("checkpoint L2 output requires its complete prefix and a fresh writer")
            self.context.saved_scheduler = self.context.saved_runtime.exchange_book_scheduler
            if self.context.saved_scheduler is not None and not self.context.resume_input_batch:
                if self.context.fresh_book_scheduler is None:
                    raise ValueError("resume requires the original exchange-book event source")
                self.context.unread_offset = (self.context.saved_scheduler.source_read_count
                                 - self.context.fresh_book_scheduler.source_read_count)
                if self.context.unread_offset < 0:
                    raise ValueError("exchange-book source cursor precedes initialized source")
            self.context.saved_collector = self.context.saved_runtime.risk_selection
            if getattr(self.context.saved_collector, "retention", "full") != self.context.opportunity_retention:
                raise ValueError("checkpoint opportunity retention differs")
            del self.context.saved_collector
            self.context.pending_resume_event = self.context.resume_checkpoint["next_event"]
            del self.context.saved_runtime, self.context.saved_scheduler, self.context.saved_strategy
            self.state = ReplayRuntimeState.restore_checkpoint_runtime(
                self.context.resume_checkpoint,
                consume=self.context.consume_resume_checkpoint,
                immutable_inputs=(
                    self.context._prepared_clock_owner.immutable_clone_inputs(
                        self.context._prepared_predictions_owner,
                    )
                    if not self.context.consume_resume_checkpoint
                    and self.context._prepared_clock_owner is not None else ()
                ),
            )
            # Checkpoints are captured between replay events, never between the
            # two decision callbacks and their consumers. Pending REST plans
            # already own copied sequence IDs, so legacy decision-map history is
            # not recovery state (and its Python object-ID keys cannot be reused).
            self.state.l2_decision_links.clear()
            if not hasattr(self.state, 'quote_price_runtime'):
                self.state.quote_price_runtime = None
            self.state.replay_progress_callback = self.context.fresh_progress_callback
            self.state.main_loop_rq_input_active = self.context.fresh_runtime.main_loop_rq_input_active
            self.state.input_price_lookback_ms = self.context.fresh_runtime.input_price_lookback_ms
            self.context.public_strategy = self.state.public_strategy
            if self.context.fresh_l2_journal is not None:
                self.context.fresh_l2_journal.restore_prefix(self.context.prefix)
            self.state.l2_journal = self.context.fresh_l2_journal
            if self.context.fresh_target_journal is not None:
                self.context.fresh_target_journal.restore_prefix(self.context.target_prefix)
            self.state.target_journal = self.context.fresh_target_journal
            self.context.restored_book_scheduler = self.state.exchange_book_scheduler
            if self.context.restored_book_scheduler is not None and not self.context.resume_input_batch:
                from models.replay.public_tape_cache import NumericPublicTape
                self.context.NumericPublicTape = NumericPublicTape
                if isinstance(self.context.exchange_book_event_tape, self.context.NumericPublicTape):
                    self.context.restored_book_scheduler._iterator = self.context.exchange_book_event_tape.iter_from(
                        self.context.restored_book_scheduler.source_read_count)
                else:
                    self.context.restored_book_scheduler._iterator = self.context.islice(
                        self.context.fresh_book_scheduler._iterator, self.context.unread_offset, None,
                    )
            if self.context.resume_input_batch:
                from models.replay.runtime_input_window import rotate_runtime_inputs
                self.context.rotate_runtime_inputs = rotate_runtime_inputs
                self.context.pending_resume_event = self.context.rotate_runtime_inputs(
                    self.state, self.context.fresh_runtime, self.context.pending_resume_event,
                    exchange_book_event_tape=self.context.exchange_book_event_tape,
                )
            # The rotated state owns its actual input arrays. Keep no otherwise
            # unused initialized runtime/scheduler graph alive for the whole batch.
            del self.context.fresh_runtime, self.context.fresh_book_scheduler

        if self.context.phase_callback is not None:
            self.context.phase_callback('event_loop')
        self.context.prefill_cut_event_ns = None
        self.context.prefill_evidence = None
        self.context.response_update_cut_event_ns = None
        if self.context.response_update_fork is not None:
            from strategy.optional_update import OptionalUpdateFork
            self.context.OptionalUpdateFork = OptionalUpdateFork
            if not isinstance(self.context.response_update_fork, self.context.OptionalUpdateFork):
                raise TypeError('explicit OptionalUpdateFork required')
            self.context.response_update_fork.validate(self.context.resume_checkpoint,
                account_end_ns=(int(self.state.trade_ts[-1])+1)*1_000_000)
        if self.context.prefill_fork is not None:
            from models.replay.runtime_checkpoint_io import PrefillFork
            self.context.PrefillFork = PrefillFork
            if not isinstance(self.context.prefill_fork, self.context.PrefillFork):
                raise TypeError('explicit PrefillFork required')
            self.context.prefill_fork.validate(self.context.resume_checkpoint,
                account_end_ns=(int(self.state.trade_ts[-1])+1)*1_000_000)
            if self.context.prefill_fork.action in {'WAIT', 'CANCEL'}:
                self.state.risk_selection.target = dict(
                    opportunity_id=self.context.prefill_fork.opportunity_id, action=self.context.prefill_fork.action)

    def _prepared_event_clock(self, trades, **kwargs):
        return self.context._prepared_clock_owner.event_clock(trades, self.services.build_replay_event_clock,
            _account=self.context._prepared_account, _identity_out=self.context.prepared_event_identity, **kwargs)
