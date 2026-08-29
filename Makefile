# ────────────────────────────────────────────────────────────
#  NarrowGate BTCUSDC — Project Makefile
# ────────────────────────────────────────────────────────────
SHELL   := /bin/bash
PYTHON  ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
EC2     ?= $(NARROWGATE_DEPLOY_TARGET)
EC2_DIR ?= $(NARROWGATE_RELEASE_DIR)
RELEASE_TAG ?= $(NARROWGATE_RELEASE_TAG)
LIVE_CONFIG ?= $(if $(NARROWGATE_LIVE_CONFIG),$(NARROWGATE_LIVE_CONFIG),live/config.yaml)
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

deploy:
	@test -n "$(EC2)" || (echo "Set EC2=user@host or NARROWGATE_DEPLOY_TARGET=user@host before running deploy." >&2; exit 2)
	@test -n "$(EC2_DIR)" || (echo "Set EC2_DIR=/absolute/release/path or NARROWGATE_RELEASE_DIR=/absolute/release/path." >&2; exit 2)
	@$(PYTHON) scripts/live_deploy_common.py source-release \
		--repo-root "$(CURDIR)" \
		--target "$(EC2)" \
		--release-dir "$(EC2_DIR)" $(if $(RELEASE_TAG),--annotated-tag "$(RELEASE_TAG)",)

deploy-dry:
	@test -n "$(EC2)" || (echo "Set EC2=user@host or NARROWGATE_DEPLOY_TARGET=user@host before running deploy-dry." >&2; exit 2)
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
	run stop restart status logs reload \
	deploy-preflight deploy deploy-dry clean clean-logs
