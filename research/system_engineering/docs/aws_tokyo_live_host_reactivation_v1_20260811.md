# AWS Tokyo Live Host Reactivation v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

Status: `deployed_current_active`

## Decision

NarrowGate BTCUSDC returned from the intermediate Vultr Tokyo host to AWS Tokyo. The current live authority is:

- instance `<current-live-instance>`;
- Elastic IP `<current-live-host>` (`<current-live-eip-allocation>`);
- SSH target `<current-live-ssh-target>`;
- `t3.medium`, x86_64, 2 vCPU/4 GiB, Amazon Linux 2023;
- repository `${NARROWGATE_REMOTE_ROOT}`;
- maker start `2026-08-11T15:48:00Z`;
- prospective epoch `prospective-1786463282415286221-dca0650f299a`, bound at `2026-08-11T15:48:02Z`.

The address `<never-authoritative-host>` belonged to a terminated misprovisioned instance and never had live authority. It is forbidden as an operational pointer.

## Preserved runtime identity

The source was the stopped, verified Vultr deployment tree rather than the local development worktree. The current AWS deployment reproduced:

- runtime-code SHA256 `eedc6c44871af3d2ee31f68d4c1d8f181eccd89ad8c583c559f23d8e8a925291`;
- config SHA256 `62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18`;
- model-bundle SHA256 `65792e9b672813ddf9ccb8a3504f998a828cd3c893e970fb0a073747685ee83c`;
- P3 SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`;
- Python `3.12.13` and native x86 profile with every strict C++ flag enabled.

The Amazon-Linux native extension was rebuilt locally on the target host. Its SHA256 is `343d92127a80cb6fefe10cddd21e70f8c1bf22674c19874c7fb0971d052b45f0`. Preflight validated all 13 strict model heads, P3, feature DAG, configuration, and native imports. The core target test set passed 35/35. Three tests in the frozen fill-cooldown test file remain a known stale-stub issue: the stub accepts one argument while the already-deployed method supplies `(mid, quote_snapshot)`; this is not an AWS runtime mismatch.

## Account and startup gate

The Binance allowlist was changed to `<current-live-host>` before activation. A signed read-only request succeeded with zero BTCUSDC open orders and position `-0.001 BTC @ 63731.9`. The secret moved directly from the stopped host to the new host with mode `0600`; it was not written into the ordinary local archive or command output.

Startup synchronized that exchange position, cancelled stale orders, loaded all 13 heads, synchronized the 1,000-level Binance book, established the user stream, and connected all six external shadow sources. Within the initial health window the engine completed real SELL-increasing and BUY-reducing fills, continued quoting. A later public-WebSocket disconnect triggered the intended stale-data quote block; the stream recovered and HEALTH returned to a valid deep book with live orders.

At `2026-08-11T16:00:06Z`, BUY order `mm_B_1786464006030_148` received Binance GTX `-5022` with the explicit exchange statement that the order would not be recorded. Trading and quoting continued, but the lifecycle writer reproduced the known never-activated reject encoding defect: snapshot exchange exposure was exact zero while the last event exposed it as unavailable. The session therefore has `error_count=1`, `formal_collection_valid=false`, and is not eligible for local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` admission. This is evidence-only failure, not strategy or order failure, and remains a hard blocker for any future exact-lifecycle M0/M1 gate until a successor contract/code identity is implemented and recollected.

## Vultr retirement and local evidence

Vultr maker shutdown completed at `2026-08-11T15:17:38Z`. The final local archive is:

`${NARROWGATE_DATA_ROOT}/remote_live_retirement/vultr_<retired-intermediate-vultr-epoch>_20260811/final_stopped`

Its logs/formal collection contain 6,834 SHA256-verified files and 59,361,363 logical bytes. The source-manifest SHA256 is `d3b27e2b0bca3299ab219a1934078cf5d99c205a290b7b440814089794b86e30`. Credentials were excluded. The Vultr instance was then destroyed; the console showed no remaining instances by `2026-08-11T16:02:02Z`.

The intermediate lifecycle session remains an invalid diagnostic because one never-activated GTX `-5022` rejection produced a callback/exposure-contract mismatch. It was preserved rather than falsely admitted. The independent Vultr bounded capture `20260811T123346Z` is valid and admitted.

## Capture and research boundary

Historical AWS v1 rows, the admitted Vultr row, and the reactivated AWS epoch remain separate source strata. Outcome-blind transport-source amendment v2 adds source key `aws:ap_northeast_1:<current-live-epoch>` without changing the target, 30-day requirement, prediction gates, or permissions. The existing heartbeat automation was updated in place and reactivated for the new AWS source; it is data collection only and grants no strategy/action authority.

## Remaining infrastructure debt

At provisioning time the AWS security group exposed SSH/22 to `0.0.0.0/0`, and the 32-GiB gp3 root volume was unencrypted. Key-only authentication is in use, but these are not the desired long-term production settings. Restrict SSH to an operator allowlist or SSM and use an encrypted replacement volume/AMI in a separately authorized maintenance window; neither change may be conflated with strategy evidence.
