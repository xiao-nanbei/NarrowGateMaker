# ────────────────────────────────────────────────────────────
#  NarrowGate BTCUSDC — Project Makefile
# ────────────────────────────────────────────────────────────
SHELL   := /bin/bash
PYTHON  ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
EC2     ?= $(NARROWGATE_DEPLOY_TARGET)
EC2_DIR ?= $(NARROWGATE_RELEASE_DIR)
RELEASE_TAG ?= $(NARROWGATE_RELEASE_TAG)
LIVE_CONFIG ?= $(if $(NARROWGATE_LIVE_CONFIG),$(NARROWGATE_LIVE_CONFIG),live/config.yaml)
NATIVE_WHEEL_DIR ?= dist/native
NATIVE_BUILD_PARALLEL_LEVEL ?= 1
NATIVE_BUILD_MIN_AVAILABLE_MIB ?= 2048
NATIVE_BUILD_MEMINFO ?= /proc/meminfo
SYMBOL  := BTCUSDC
DAYS    := 5
START   ?= 2026-01-01

# ── Data ────────────────────────────────────────────────────
download:
	$(PYTHON) pipeline.py download-agg-trades --symbol $(SYMBOL) --day-start $(START) --days $(DAYS)

download-metrics:
	$(PYTHON) pipeline.py download-metrics --symbol $(SYMBOL) --start $(START) --workers 8

download-orderbook:
	$(PYTHON) pipeline.py download-orderbook --symbols $(SYMBOL) --start $(START)

audit-raw-trades:
	$(PYTHON) pipeline.py audit-raw --symbols $(SYMBOL) BTCUSDT

preprocess:
	$(PYTHON) pipeline.py features-all --symbol $(SYMBOL)

preprocess-bars:
	$(PYTHON) pipeline.py bars --symbol $(SYMBOL)

preprocess-metrics:
	$(PYTHON) pipeline.py preprocess-metrics --symbol $(SYMBOL)

features:
	$(PYTHON) pipeline.py engineer --symbol $(SYMBOL)

# ── Training ────────────────────────────────────────────────
TARGETS := dir_10s dir_30s dir_60s ret_10s ret_30s ret_60s vol_10s vol_30s vol_60s

train:
	$(PYTHON) models/experiment_runner.py train --symbol $(SYMBOL)

train-tune:
	@for t in $(TARGETS); do \
		echo "=== Tuning $$t ==="; \
		$(PYTHON) models/experiment_runner.py train --symbol $(SYMBOL) --target $$t --tune; \
	done

platform-describe:
	$(PYTHON) models/experiment_runner.py describe --symbol $(SYMBOL)

# ── Backtesting ─────────────────────────────────────────────
# backtest/backtest-sweep/backtest-as are legacy bar diagnostics only.
# Formal strategy evidence uses backtest-tick with a frozen replay contract.
backtest:
	$(PYTHON) models/experiment_runner.py backtest-ml --symbol $(SYMBOL)

backtest-sweep:
	$(PYTHON) models/experiment_runner.py backtest-ml --symbol $(SYMBOL) --sweep

backtest-as:
	$(PYTHON) models/experiment_runner.py backtest-as --symbol $(SYMBOL)

backtest-tick:
	$(PYTHON) models/experiment_runner.py backtest-tick --symbol $(SYMBOL)

# ── Native live wheel ─────────────────────────────────────────────
# This is a build-host operation. It is deliberately not a dependency of source
# publication, deployment preflight, installation, or activation.
native-live-build-preflight:
	@test "$$(uname -s)" = "Linux" || (echo "The EC2 native wheel requires a Linux x86_64 builder." >&2; exit 2)
	@case "$$(uname -m)" in x86_64|amd64) ;; *) echo "The EC2 native wheel requires an x86_64 builder." >&2; exit 2 ;; esac
	@case "$(NATIVE_BUILD_PARALLEL_LEVEL)" in ''|*[!0-9]*|0) echo "NATIVE_BUILD_PARALLEL_LEVEL must be a positive integer." >&2; exit 2 ;; esac
	@case "$(NATIVE_BUILD_MIN_AVAILABLE_MIB)" in ''|*[!0-9]*|0) echo "NATIVE_BUILD_MIN_AVAILABLE_MIB must be a positive integer." >&2; exit 2 ;; esac
	@if command -v systemctl >/dev/null 2>&1; then \
		for unit in narrowgate.service narrowgate-maker.service; do \
			if systemctl is-active --quiet "$$unit" 2>/dev/null; then \
				echo "Refusing native compilation while $$unit is active; use a stopped maintenance window or a controlled builder." >&2; \
				exit 2; \
			fi; \
		done; \
	fi
	@if command -v pgrep >/dev/null 2>&1 && pgrep -f '[/][l]ive/main[.]py|[[:space:]]-m[[:space:]]+[l]ive[.]main' >/dev/null; then \
		echo "Refusing native compilation while a live maker process is running." >&2; \
		exit 2; \
	fi
	@test -r "$(NATIVE_BUILD_MEMINFO)" || (echo "Cannot read Linux memory information from $(NATIVE_BUILD_MEMINFO)." >&2; exit 2)
	@total_kib="$$(awk '$$1 == "MemTotal:" {print $$2; exit}' "$(NATIVE_BUILD_MEMINFO)")"; \
	available_kib="$$(awk '$$1 == "MemAvailable:" {print $$2; exit}' "$(NATIVE_BUILD_MEMINFO)")"; \
	required_kib=$$(( $(NATIVE_BUILD_MIN_AVAILABLE_MIB) * 1024 )); \
	if [ -z "$$total_kib" ] || [ -z "$$available_kib" ]; then \
		echo "MemTotal or MemAvailable is missing from $(NATIVE_BUILD_MEMINFO)." >&2; \
		exit 2; \
	fi; \
	if [ "$$available_kib" -lt "$$required_kib" ]; then \
		echo "Native build needs at least $(NATIVE_BUILD_MIN_AVAILABLE_MIB) MiB available; found $$((available_kib / 1024)) MiB. Use the 16 GiB Azure builder." >&2; \
		exit 2; \
	fi; \
	if [ "$$total_kib" -le $$((3 * 1024 * 1024)) ] && [ "$(NATIVE_BUILD_PARALLEL_LEVEL)" -ne 1 ]; then \
		echo "Hosts with at most 3 GiB RAM must use NATIVE_BUILD_PARALLEL_LEVEL=1." >&2; \
		exit 2; \
	fi

native-live-wheel: native-live-build-preflight
	@mkdir -p "$(NATIVE_WHEEL_DIR)"
	CMAKE_BUILD_PARALLEL_LEVEL="$(NATIVE_BUILD_PARALLEL_LEVEL)" \
		$(PYTHON) -m pip wheel --no-deps \
		--wheel-dir "$(NATIVE_WHEEL_DIR)" \
		--config-settings=cmake.define.NARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2 \
		./cpp

# ── Live Trading ────────────────────────────────────────────
run:
	bash live/run.sh start

stop:
	bash live/run.sh stop

restart:
	bash live/run.sh restart

status:
	bash live/run.sh status

logs:
	bash live/run.sh logs

reload:
	bash live/run.sh reload

# ── Exact public source deployment ──────────────────────────
# This transport publishes only one clean Git checkout. Private config,
# models, envelopes, reconciliation receipts, credentials, and process control
# are deliberately outside this target.

deploy-preflight:
	@test -f "$(LIVE_CONFIG)" || (echo "Set NARROWGATE_LIVE_CONFIG to a private deploy config file." >&2; exit 2)
	@$(PYTHON) scripts/preflight_live_deploy.py --config "$(LIVE_CONFIG)"

publish-source:
	@test -n "$(EC2)" || (echo "Set EC2=user@host or NARROWGATE_DEPLOY_TARGET=user@host before running publish-source." >&2; exit 2)
	@test -n "$(EC2_DIR)" || (echo "Set EC2_DIR=/absolute/release/path or NARROWGATE_RELEASE_DIR=/absolute/release/path." >&2; exit 2)
	@$(PYTHON) scripts/live_deploy_common.py source-release \
		--repo-root "$(CURDIR)" \
		--target "$(EC2)" \
		--release-dir "$(EC2_DIR)" $(if $(RELEASE_TAG),--annotated-tag "$(RELEASE_TAG)",)

publish-source-dry:
	@test -n "$(EC2)" || (echo "Set EC2=user@host or NARROWGATE_DEPLOY_TARGET=user@host before running publish-source-dry." >&2; exit 2)
	@test -n "$(EC2_DIR)" || (echo "Set EC2_DIR=/absolute/release/path or NARROWGATE_RELEASE_DIR=/absolute/release/path." >&2; exit 2)
	@$(PYTHON) scripts/live_deploy_common.py source-release \
		--repo-root "$(CURDIR)" \
		--target "$(EC2)" \
		--release-dir "$(EC2_DIR)" $(if $(RELEASE_TAG),--annotated-tag "$(RELEASE_TAG)",) \
		--dry-run

# ── Cleanup ─────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

clean-logs:
	rm -f logs/maker.log logs/maker.log.*

.PHONY: download download-metrics download-orderbook audit-raw-trades \
	preprocess preprocess-bars preprocess-metrics features \
	train train-tune platform-describe \
	backtest backtest-sweep backtest-as backtest-tick \
	native-live-build-preflight native-live-wheel \
	run stop restart status logs reload \
	deploy-preflight publish-source publish-source-dry clean clean-logs
