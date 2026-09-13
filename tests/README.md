# Regression scope

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially synchronized: 2026-09-19

Current regressions should exercise maintained behavior, not treat a historical report's literal assertions as proof that today's implementation behaves correctly. Historical evidence and its research-use restrictions remain intact when document-only tests are retired. A closed research family, version suffix or past CI failure is not a deletion criterion.

## First bounded retirement

Removed three document-only modules (five test functions): `test_ber_guard_role_safe_add_only_current_stack_owner_v1_1.py`, `test_buy_q90_dual_clock_terminal_routing_contract_v2.py`, and `test_buy_q90_runtime_authority_contract_v3.py`. Their fixed JSON flags and English text are no longer daily software regression requirements. No other test imports their helpers; none are entries in the existing historical availability list. No production behavior test was removed or claimed as newly covered.

The original records remain unchanged: [F09 execution errata](../research/families/f09_campaign_action_uplift/docs/ber_guard_role_safe_add_only_current_stack_owner_v1_execution_estimand_errata_v1_20260808.json), [F10 dual-clock implementation](../research/families/f10_live_replay_attribution/docs/buy_q90_dual_clock_terminal_routing_contract_v2_implementation_20260802.json), and [F10 runtime-authority implementation](../research/families/f10_live_replay_attribution/docs/buy_q90_runtime_authority_contract_v3_implementation_20260802.json). Retirement does not amend their historical conclusions or grant current runtime authority.

Mixed ABI/history tests, time-scale contract integrity, prediction compatibility, startup/reload rejection, native parity, downloader migration and Studio economic-completeness tests remain in scope. Further consolidation requires identifying retained behaviors and their destination tests before removing a module. Historical reproduction continues to use [the existing availability list](fixtures/public_clone_historical_test_availability.json) and `NARROWGATE_RUN_HISTORICAL_REPRODUCTION_TESTS=1`; no second exclusion list is introduced. Removing these five static assertions makes no measured CI speedup claim.
