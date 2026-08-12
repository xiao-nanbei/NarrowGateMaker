# Active Order Lifecycle CIF Live Transport v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `failed_closed_producer_activation_cardinality_and_feature_visibility`

The first outcome-blind live/AWS transport audit was run against the atomically admitted one-hour prospective epoch. No PnL, reward, markout, campaign outcome, or economic result was read.

The input integrity layer passed: the epoch was fully bound, all 2,664 journal rows used the exact v2 schema, exchange-clock coverage was 100%, drops and errors were zero, post-terminal fill-risk reuse was zero, unsupported CIF exposure was zero, and the observed span was 0.991 hours.

Transport did not pass. Of 589 order lifecycles, 330 contained two activation events. The REST acknowledgement had already produced `SUBMITTED -> ACTIVE`, then the WebSocket `NEW` callback appended an idempotent `ACTIVE -> ACTIVE` activation. The frozen training contract requires exactly one activation, so only 258 lifecycles remained eligible. Live valid fraction was 0.4380 versus 0.9703 in the 40-day reference; the absolute difference was 0.5323, above the frozen 0.05 limit. Cancel-role TV was 0.1904 and side/phase/cause TV was 0.1936, both above 0.15.

The admitted session also lacks the exact feature visibility companion `feature_source_exchange_ts_ns`, `feature_ready_ts_ns`, and `decision_ts_ns`. These values were not inferred. Therefore both the lifecycle-CIF layer and the q90 feature-visibility layer remain unsupported, and no economic replay may be opened.

The duplicate-activation producer bug is fixed locally by making an already ACTIVE WebSocket `NEW` acknowledgement lifecycle-idempotent. This does not change exchange order behavior. The fix passed 49 targeted lifecycle, journal, remote-admission, and transport tests. A new fully bound prospective epoch is required to re-run transport; the failed report remains immutable evidence.

Frozen Spec: [`active_order_lifecycle_cif_live_transport_v1_spec_20260808.json`](active_order_lifecycle_cif_live_transport_v1_spec_20260808.json)

Authoritative report: `${NARROWGATE_DATA_ROOT}/reports/f07_active_order_lifecycle_cif_live_transport_v1_20260808/report.json`

Report file SHA256: `ddea4d3e1734f0305603a399b8d9129077714b6cc870b3e99e2ec871efc2a93d`

Canonical report SHA256: `eb02b0b2530e3b8417b67961bc84a128171b98107c1573f2c218cb6b35212b4d`
