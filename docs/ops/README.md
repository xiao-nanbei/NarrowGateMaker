# Operations Notes

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-30

Last materially synchronized: 2026-08-30

Operations docs cover dry-run setup, private config boundaries, deployment guardrails, and telemetry. They should not contain private hosts, account details, or raw live PnL.

- Local dry-run workflow: [live_dry_run.md](live_dry_run.md)
- Live deployment receipts and historical routing are owner-private and are not distributed in the public repository.
- Binance USD-M 1000-level snapshot/diff health probe: `.venv/bin/python scripts/probe_binance_deep_book.py --help`
- Current deployment and live commands: repository `README.md` / `README.zh-CN.md`

## Generic deployment flow

The public deployment kernel is provider-neutral and deliberately split into two boundaries:

- `make publish-source-dry` / `make publish-source` publish only an exact public source checkout. They never copy a config, model, credential, envelope, reconciliation receipt, runtime receipt, or process-control command.
- Runtime construction and activation consume operator-supplied private material outside the checkout. Publishing source does not authorize or start trading.

The source transport requires a clean local Git checkout. It creates a verified bundle from `HEAD`, streams it through SSH, clones it into a same-filesystem staging directory, verifies the exact commit, tree, and clean status, and atomically renames that directory to the requested absolute release path. An existing exact release is accepted idempotently; an existing mismatched release or staging directory fails closed.

The safe sequence is:

1. Provision an unprivileged service user, a service-user-owned release parent, and a separate mode-`0700` private root. Restrict SSH and outbound credentials to the minimum needed by the venue connection.
2. From a clean public clone, run `make publish-source-dry`, review the bound commit/tree, then run `make publish-source`. This publishes source only and never restarts a process.
3. Place the private config, model bundle and its authorization manifest, wheels, lock, wheelhouse, and admitted input receipts under the private root—not inside the Git checkout. Construct the deployment envelope and stopped-exchange reconciliation there later from the exact release.
4. Build or receive the content-addressed wheelhouse, then create a commit-bound environment with `python3.12 -m live.deployment_runtime install`. Build release wheels from a clean checkout/worktree; remove generated `build/`, `dist/`, and `*.egg-info` state before the build so deleted files cannot survive in a stale wheel tree. Never let the target resolve dependencies from an index during install.
5. Run both `verify-install` and `verify-static-tree`, then bind the absolute `venv-<execution-commit>` path as the release's ignored `.venv-active` selector.
6. Build the deployment envelope from the exact checkout/runtime authorities. The model authorization manifest is a required envelope member; optional policy artifacts are included only as complete groups. Run `make deploy-preflight` against the private config, and use `live/run.sh reconcile-stopped` while the maker is fully stopped to produce the exchange barrier.
7. Admit the release only after process, runtime health, position/order reconciliation, and log checks pass. Then build the compact activation receipt and atomically publish the current selector. Store exact activation and rollback evidence privately.

The authority commands are public and generic: `live.deployment_runtime build-envelope` derives the deployment envelope from exact files and receipts; `live/run.sh reconcile-stopped ABSOLUTE_PATH` performs signed exchange reads only after proving the maker is fully stopped; `build-activation-receipt` binds the validated envelope, stopped reconciliation, and observed live runtime identity; and `publish-current-pointer` revalidates that lineage before an atomic selector update. Use their emitted canonical roots; never hand-write these JSON objects or copy an old private receipt into current authority.

All locked-runtime subcommands and required fields are discoverable without private data:

```bash
python3.12 -m live.deployment_runtime --help
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.deployment_runtime verify-envelope-startup --help
python3.12 -m live.deployment_runtime build-activation-receipt --help
python3.12 -m live.deployment_runtime publish-current-pointer --help
python3.12 -m live.native_build_receipt --help
python3.12 scripts/live_deploy_common.py source-release --help
```

## AWS EC2 example

The following uses placeholders deliberately; it is not a current host or release identity. AWS live deployment is not a five-minute promise. The five-minute entry point in the repository README is the local no-trading demo only.

EC2 provision checklist:

- 64-bit Linux with systemd; the locked live runtime requires CPython 3.12.
- Encrypted root volume and no reusable data or credentials in the image.
- SSH ingress only from the operator CIDR; no public application ports.
- Minimum outbound access for the configured venue's HTTPS/WSS endpoints; resolve packages on a build host, not during target install.
- Dedicated unprivileged `narrowgate` service user; root is used only for host/service setup.
- Clone the public source on the operator/build host with the real public URL below; inject all operational authority separately.

For production access, prefer [AWS Systems Manager Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html), or restrict SSH with [EC2 security-group rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules.html). The source transport uses the operator's existing OpenSSH configuration, so an SSM `ProxyCommand` belongs in `~/.ssh/config`, not in an unsafe command-line option.

On the instance, create two disjoint roots. The release parent is writable by the service user and is not group/other-writable; the private root is mode `0700`:

```bash
sudo useradd --system --create-home --shell /bin/bash narrowgate
sudo install -d -o narrowgate -g narrowgate -m 0755 /opt/narrowgate/releases
sudo install -d -o narrowgate -g narrowgate -m 0700 /opt/narrowgate/private
```

On the operator/build host, clone and publish one exact clean source release. A locked live/native build also requires one explicit annotated public release tag. The transport verifies that the tag object peels to `HEAD` and transfers that tag only—never every local tag. A source-only dry-run may omit it, but that output is not sufficient for a live/native receipt.

```bash
git clone https://github.com/xiao-nanbei/NarrowGateMaker.git
cd NarrowGateMaker

export NARROWGATE_RELEASE_TAG="v0.1.2"
git checkout --detach "$NARROWGATE_RELEASE_TAG^{commit}"
export NARROWGATE_DEPLOY_TARGET="narrowgate@<ec2-address>"
export NARROWGATE_RELEASE_DIR="/opt/narrowgate/releases/<release-id>"

make publish-source-dry
make publish-source
```

The command refuses a dirty local checkout and refuses a relative release path. It does not copy or inspect private material. Create the per-release private directory separately:

```bash
sudo install -d -o narrowgate -g narrowgate -m 0700 \
  /opt/narrowgate/private/<release-id>
```

Transfer the private config, model bundle, its authorization manifest, lock, wheelhouse, wheels, and admitted input receipts into that private directory using the operator's approved secret/artifact channel. Do not pre-copy an envelope or reconciliation from another release; construct both below. Do not place private material under `/opt/narrowgate/releases/<release-id>`.

On the instance, derive the exact commit-bound venv name and install from already transferred, hash-bound artifacts. The install receipt and `venv-<commit>` must have the same private parent because live startup enforces that relationship:

```bash
RELEASE_DIR=/opt/narrowgate/releases/<release-id>
PRIVATE_DIR=/opt/narrowgate/private/<release-id>
COMMIT=$(git -C "$RELEASE_DIR" rev-parse HEAD)
VENV_DIR="$PRIVATE_DIR/venv-$COMMIT"
RECEIPT="$PRIVATE_DIR/install-receipt.json"
cd "$RELEASE_DIR"

python3.12 -m live.deployment_runtime install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT"
```

Record the canonical receipt SHA256 printed by `install`, then run the complete dynamic and static verification commands before exposing the selector:

```bash
python3.12 -m live.deployment_runtime verify-install \
  --builder-python /usr/bin/python3.12 \
  --venv "$VENV_DIR" \
  --lock "$PRIVATE_DIR/runtime.lock.json" \
  --expected-lock-sha256 <lock-sha256> \
  --wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --expected-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --native-wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --native-wheel-sha256 <native-wheel-sha256> \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

python3.12 -m live.deployment_runtime verify-static-tree \
  --venv "$VENV_DIR" \
  --receipt "$RECEIPT" \
  --expected-receipt-sha256 <install-receipt-canonical-sha256>

test ! -e "$RELEASE_DIR/.venv-active" && test ! -L "$RELEASE_DIR/.venv-active"
ln -s "$VENV_DIR" "$RELEASE_DIR/.venv-active"
test "$(readlink "$RELEASE_DIR/.venv-active")" = "$VENV_DIR"
```

Run the generic Linux native receipt from the installed commit-bound environment. It independently requires the one annotated tag, exact wheel/runtime authorities, native ABI, and parity smoke tests:

```bash
NATIVE_RECEIPT="$PRIVATE_DIR/native-build-receipt.json"

"$VENV_DIR/bin/python3" -I -B "$RELEASE_DIR/live/native_build_receipt.py" \
  --repository-root "$RELEASE_DIR" \
  --annotated-tag "$NARROWGATE_RELEASE_TAG" \
  --wheel "$PRIVATE_DIR/narrowgate-cpp.whl" \
  --builder-python /usr/bin/python3.12 \
  --runtime-lock "$PRIVATE_DIR/runtime.lock.json" \
  --runtime-lock-sha256 <lock-sha256> \
  --dependency-wheelhouse "$PRIVATE_DIR/wheelhouse" \
  --dependency-wheelhouse-sha256 <wheelhouse-sha256> \
  --root-wheel "$PRIVATE_DIR/narrowgate.whl" \
  --root-wheel-sha256 <root-wheel-sha256> \
  --install-receipt "$RECEIPT" \
  --install-receipt-sha256 <install-receipt-canonical-sha256> \
  --output "$NATIVE_RECEIPT"
```

Create the deployment envelope from the exact private active config, native-build
receipt, and model authorization manifest selected and validated by the model contract.
That single required manifest internally binds the admitted model heads and P3 artifact;
do not repeat their leaf hashes in the deployment command, service environment, or pointer.
`--model-authorization` is always required, even when every optional action policy is
disabled. If the active config binds the SELL Boolean cooldown, supply
`--boolean-policy-file` and `--boolean-predicate-bundle` together. If it binds
the BUY E3 policy, supply `--policy-artifact-manifest`, `--policy-file`, and
`--predicate-bundle` together. Omit an entire group when that policy is disabled.
Set `MODEL_AUTHORIZATION` to the exact `model_authorization_path` reported by `scripts/preflight_live_deploy.py`; do not copy or rename that file.

```bash
ENVELOPE="$PRIVATE_DIR/deployment-envelope.json"
MODEL_AUTHORIZATION=<exact-model_authorization_path-from-preflight>

python3.12 -m live.deployment_runtime build-envelope \
  --repository-root "$RELEASE_DIR" \
  --active-config "$PRIVATE_DIR/live-config.yaml" \
  --native-build-receipt "$NATIVE_RECEIPT" \
  --model-authorization "$MODEL_AUTHORIZATION" \
  --output "$ENVELOPE"
```

Keep the config, model bundle, constructed deployment envelope, and constructed stopped-exchange reconciliation under `PRIVATE_DIR`, owned by `narrowgate`, with private files mode `0600`. Prepare a root-owned mode-`0600` service environment file such as `/etc/narrowgate/live.env` in two phases: first populate the config, envelope root, and trusted Python locator and export those same values for the stopped reconciliation command; then add the reconciliation path and canonical root printed by that command. The deployment envelope is the only external release digest. Its nested manifests derive and verify the lock, wheelhouse, wheels, native module, interpreter, installed `RECORD`, config, and policy members without repeating those leaf hashes in the service environment.

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_PATH=<absolute-private-envelope-path>
NARROWGATE_DEPLOYMENT_ENVELOPE_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH=<absolute-private-reconciliation-path>
NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256=<sha256>
NARROWGATE_STARTUP_TRUSTED_PYTHON_PATH=<absolute-cpython-3.12-builder-path-with-pip>
```

If the selected private policy enables the corresponding gated mechanism, its invocation environment must also carry the matching private approval flag: `NARROWGATE_ALLOW_Q90_PRIVATE_DEPLOY`, `NARROWGATE_ALLOW_F05_BUY_E3_PRIVATE_DEPLOY`, or `NARROWGATE_ALLOW_F05_BOOLEAN_COOLDOWN_PRIVATE_DEPLOY`. Do not set an unrelated flag and do not commit these approvals.

Before activation, run the separate private-config preflight from the exact public checkout:

```bash
NARROWGATE_LIVE_CONFIG=<absolute-private-config-path> make deploy-preflight
```

With the maker fully stopped and the private config plus deployment-envelope root exported, generate the create-only exchange barrier as the `narrowgate` service user. This command performs signed venue REST reads, requires zero open orders and a stable exact position, and prints the path and canonical root needed by the reconciliation environment fields above. Account identity remains inside that canonical payload and is checked against the running credential; it is not another external digest.

```bash
RECONCILIATION="$PRIVATE_DIR/stopped-exchange-reconciliation.json"
"$RELEASE_DIR/live/run.sh" reconcile-stopped "$RECONCILIATION"
```

`live/run.sh` accepts only the envelope path/root and trusted Python locator for release startup authority. It validates the canonical envelope with the trusted standard library, proves the Git commit/tree and clean checkout before executing repository code, then invokes `verify-envelope-startup` so nested manifests verify the runtime, installed distributions, every installed `RECORD`, and `pip check`. The trusted Python path is an OS bootstrap trust anchor: it must be canonical, root-owned, regular, single-link, executable, and not group/world writable. Do not put API credentials or private authority values in the unit or repository. In service mode `run.sh` performs that proof once and then replaces itself with the maker process; systemd is the sole process owner:

```ini
[Unit]
Description=NarrowGate live maker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=narrowgate
WorkingDirectory=/opt/narrowgate/current
EnvironmentFile=/etc/narrowgate/live.env
ExecStart=/opt/narrowgate/current/live/run.sh service
ExecReload=/opt/narrowgate/current/live/run.sh reload
Restart=no
KillSignal=SIGTERM
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

Only after all private authority gates are available should an operator atomically move a separately prepared `/opt/narrowgate/current` filesystem selector and start the service. Source publication itself never changes that selector. After `systemctl start narrowgate`, verify `systemctl status narrowgate`, `live/run.sh status`, `logs/runtime_health.json`, and recent engine logs. A running PID is insufficient: position and open-order reconciliation must converge and no ownership or execution-state safety latch may be active. Do not add `ExecStop=live/run.sh stop` to the unit: systemd must send `SIGTERM` directly and allow the full `TimeoutStopSec` grace period. The `run.sh start|status|stop` commands remain compatibility tools for a manual, non-systemd launch.

After that admission succeeds, bind only the three validated activation inputs and publish the compact private current pointer. Use the canonical roots printed by the earlier commands and the absolute runtime identity written by the admitted process:

```bash
ACTIVATION_RECEIPT="$PRIVATE_DIR/activation-receipt.json"
CURRENT_POINTER=/opt/narrowgate/private/current.json
RUNTIME_IDENTITY=<absolute-runtime-identity-path>

python3.12 -m live.deployment_runtime build-activation-receipt \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --stopped-reconciliation-sha256 <stopped-reconciliation-canonical-sha256> \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$ACTIVATION_RECEIPT"

python3.12 -m live.deployment_runtime publish-current-pointer \
  --release-id <release-id> \
  --deployment-envelope "$ENVELOPE" \
  --deployment-envelope-sha256 <deployment-envelope-canonical-sha256> \
  --activation-receipt "$ACTIVATION_RECEIPT" \
  --activation-receipt-sha256 <activation-receipt-canonical-sha256> \
  --stopped-reconciliation "$RECONCILIATION" \
  --runtime-identity "$RUNTIME_IDENTITY" \
  --output "$CURRENT_POINTER"
```

The JSON current pointer is only a five-field release selector. Its `release_id` is resolved through the owner-private routing inventory and the corresponding release directory; the pointer itself contains no host routing or leaf artifact inventory. `status=selected_activation` means only that this activation was selected after lineage validation. It is not a live-health assertion and never replaces service, exchange, or runtime-health checks. Older verbose receipts, command transcripts, and per-file hash inventories may be retained privately as audit attachments, but they are audit-only and must not participate in startup authority or be copied into the current pointer.

Rollback is another verified deployment, not a blind restart. Stop the service, require a clean stop result, reconcile exchange position/open orders, switch the release/config/envelope selectors to a previously verified private release, rerun preflight and static-runtime verification, then start and repeat health admission. If stop reports uncertain execution state, do not activate either release until manual reconciliation completes.
