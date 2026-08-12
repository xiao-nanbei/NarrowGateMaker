# Vultr Tokyo Live Host Migration v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

Status: `deployed_current_active`

The NarrowGate BTCUSDC live process moved from the retired AWS Tokyo host `<retired-original-aws-host>` to the current Vultr Tokyo host `<retired-intermediate-vultr-host>` on 2026-08-11. The predecessor instance has been terminated and its Elastic IP released. It has no live-process, rollback, SSH, capture, or historical-query authority; all old-host evidence retrieval is local.

## Runtime identity

- Remote root: `${NARROWGATE_REMOTE_ROOT}`
- Architecture: `x86_64`
- Python: `3.12.13`
- Runtime code SHA256: `eedc6c44871af3d2ee31f68d4c1d8f181eccd89ad8c583c559f23d8e8a925291`
- Config SHA256: `62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18`
- Model bundle SHA256: `65792e9b672813ddf9ccb8a3504f998a828cd3c893e970fb0a073747685ee83c`
- P3 SHA256: `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`
- Vultr native extension SHA256: `701e241d72c00c69889bae8be8292a87e0f47cc2cca6445452c09a0e32b86aba`
- Initial Vultr epoch: `prospective-1786448832438246112-6b120998db60`
- Initial Vultr epoch identity: `6b120998db6045f67099c2f722462295705f297e2f43f8cc2de1b57c5be2b224`

The source/config/model identities are unchanged. The native extension was rebuilt with GCC 11 for Ubuntu 24.04, so its binary hash intentionally differs from the retired Amazon Linux build and belongs to a new host/runtime epoch.

## Cutover contract and result

The source was copied from the frozen AWS deployment, not from the locally modified development tree. Before stopping the old maker, the new IP passed signed read-only Binance position/open-order calls and all deployment identity preflights. The old process received one SIGTERM, logged `Shutdown complete`, and exited without escalation. Exchange open orders changed from two to zero; the `+0.001 BTC` position and entry price were retained. Only then was the new maker started.

The new process synchronized the retained position, loaded all 13 model heads and P3, opened the Binance market/public/user streams, synchronized the deep book, started all six external market-data streams, bound a new prospective baseline epoch, and resumed the two-sided quoting path with strict native compute enabled.

After ten minutes the process remained healthy with both exchange orders present, strict native counters active, lifecycle journal rows committed with zero drops/errors, and ordinary BUY/SELL fills observed. The only logged error after the initial missing-book fail-closed block was one Bybit shadow-feed disconnect; it automatically reconnected and resubscribed after two seconds.

This is a planned restart boundary, not an in-memory state migration. Cash, exchange position and entry price were retained; active orders were canceled. Process-local EMA, cooldown, campaign, order-age and queue state restarted under production startup semantics.

## Historical archive

The stopped AWS `logs/` and `formal_collection/` trees were synchronized to:

`${NARROWGATE_DATA_ROOT}/remote_live_retirement/aws_<retired-original-aws-epoch>_20260811/final_stopped`

The final stopped tree contains 389 log files and 16,307 formal-collection files (16,696 verified files total). The online first-pass `maker.pid` was preserved separately as a pre-cutover marker and is not part of the stopped source tree. A source-host SHA256 manifest was checked against the local copy with zero missing, extra, or mismatched files. Credentials were excluded from the ordinary archive.

Historical AWS latency/tape artifacts remain valid for their frozen AWS epochs and may be reused as an explicitly labelled historical prior, source stratum, or replay sensitivity because the strategy identities and 2-vCPU/4-GiB resource class were preserved. They do not become measurements of Vultr transport. A new Vultr receive-time and REST/user-stream transport profile is required before making current-host latency or live-parity claims.

Routine queries for pre-cutover live history must use the local stopped archive above, not SSH to the retired host. Current live status and new capture work must use `<retired-intermediate-vultr-host>`. The complete routing and reuse contract is recorded in [`docs/live_host_and_historical_data_access_20260811.md`](../../../docs/live_host_and_historical_data_access_20260811.md).

## Bounded-capture successor

The capture manager and remote wrapper were upgraded and deployed to the Vultr host after cutover. New summaries/local validations/ledger rows use host-bound v3/v3/v2 schemas and the storage prefix `vultr_tokyo_<retired-intermediate-vultr-epoch>_*`. The frozen AWS `capture_ledger.v1.jsonl` is read-only; Vultr admission writes `capture_ledger.v2.jsonl`. Status and duplicate-day checks read both ledgers without rewriting the predecessor.

The active Codex heartbeat is `collect-vultr-tokyo-bounded-market-tapes` (`Collect Vultr Tokyo bounded market tapes`), scheduled daily at 23:30 local time. It runs a read-only status check and starts a detached 3,600-second capture only when no local collection is active. The first Vultr capture was started at 2026-08-11 12:33 UTC; it is data collection only and does not create or enable a strategy arm.

## Predecessor termination status

The predecessor maker is stopped and the final local archive has passed its 16,696-file SHA256 verification. That archive covers the repository `logs/` and `formal_collection/` trees, not every file in the old home directory. A host-level audit found an additional 351-MB BUY-q90 shadow export, a separate home-level log tree, deployment backups, and older release source/manifests. They have now been admitted to the local `supplemental_home_admitted` archive: 166 files/660,908,196 bytes, zero missing/extra/hash mismatches, with credential files excluded. After the owner updated the Binance IP allowlist and explicitly waived creation of a fast rollback AMI/EBS snapshot, AWS instance `<retired-original-aws-instance>` was terminated and Elastic IP allocation `<retired-original-aws-eip-allocation>` (`<retired-original-aws-host>`) was released at approximately 2026-08-11 13:34 UTC. Its delete-on-termination root volume `<retired-original-aws-root-volume>` was also deleted; AWS showed no remaining EBS volumes in `ap-northeast-1`.

No bootable AWS rollback image exists. The verified `final_stopped` and `supplemental_home_admitted` local archives are now the sole recovery and historical-query sources for the predecessor. The pending Vultr bounded capture, invalid lifecycle evidence session, and locally retained final AWS diagnostic capture are separate research-data quality matters and neither blocked nor were resolved by deleting the old instance.
