# ────────────────────────────────────────────────────────────
#  NarrowGate BTCUSDC — Project Makefile
# ────────────────────────────────────────────────────────────
SHELL   := /bin/bash
PYTHON  ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
REMOTE_PYTHON ?= .venv/bin/python3
EC2     ?= $(NARROWGATE_DEPLOY_TARGET)
EC2_DIR ?= ~/NarrowGate_BTCUSDC
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

# ── EC2 Deployment ──────────────────────────────────────────
DEPLOY_FILES := \
	calendar_features.py \
	market_fusion.py \
	$(wildcard execution/*.py) \
	$(filter-out live/config.yaml,$(wildcard live/*.py)) live/run.sh \
	$(wildcard live/orderbook/*.py) \
	$(wildcard live/venues/*.py) $(wildcard live/profiles/*.env) \
	$(wildcard strategy/*.py) \
	features/feature_dag.py \
	models/replay/__init__.py \
	models/replay/baseline_epoch_manifest.py \
	models/replay/prospective_baseline_epoch.py \
	models/symbol_paths.py \
	$(wildcard research/*.py) \
	$(wildcard research/families/*.py) \
	$(wildcard research/families/f02_empirical_p3_touch/*.py) \
	$(wildcard research/families/f03_causal_13_head/*.py) \
	$(wildcard research/families/f05_fill_quality_quote_ev/*.py) \
	scripts/preflight_live_deploy.py \
	cpp/pyproject.toml cpp/CMakeLists.txt \
	$(wildcard cpp/narrowgate_cpp/*.cpp) $(wildcard cpp/narrowgate_cpp/*.hpp) \
	$(wildcard research/families/f06_placement_fill_cif/cpp/*.cpp) \
	$(wildcard research/families/f06_placement_fill_cif/cpp/*.hpp) \
	$(wildcard research/families/f07_active_order_continuation/cpp/*.cpp) \
	$(wildcard research/families/f07_active_order_continuation/cpp/*.hpp)

DEPLOY_IDENTITY_FILES := \
	research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260812_v11.json

DEPLOY_FILES += $(DEPLOY_IDENTITY_FILES)

DEPLOY_MODEL_DIR := $(shell $(PYTHON) -c "from pathlib import Path; import yaml; cfg=yaml.safe_load(Path('$(LIVE_CONFIG)').read_text()) or {}; print((cfg.get('ml') or {}).get('model_dir') or 'models/saved_btcusdc')")
DEPLOY_MODELS := $(wildcard $(DEPLOY_MODEL_DIR)/*.txt $(DEPLOY_MODEL_DIR)/*.json)
DEPLOY_REMOVED_FILES := \
	live/binance_deep_book.py \
	market_data/__init__.py \
	market_data/orderbook/__init__.py \
	market_data/orderbook/binance_usdm.py
DEPLOY_REMOVED_DIRS := \
	research_01_fixed_parameter_racing \
	research_02_empirical_p3_touch \
	research_03_causal_13_head \
	research_04_external_market_alpha \
	research_05_fill_quality_quote_ev \
	research_06_placement_fill_cif \
	research_07_active_order_continuation \
	research_08_side_taker_lifecycle \
	research_09_campaign_action_uplift \
	research_10_live_replay_attribution \
	research_11_system_engineering \
	research_shared

deploy:
	@test -n "$(EC2)" || (echo "Set EC2=user@host or NARROWGATE_DEPLOY_TARGET=user@host before running deploy." >&2; exit 2)
	@test -f "$(LIVE_CONFIG)" || (echo "Set NARROWGATE_LIVE_CONFIG to a private deploy config file." >&2; exit 2)
	@! grep -q "PUBLIC TEMPLATE" "$(LIVE_CONFIG)" || (echo "$(LIVE_CONFIG) is a public template; set NARROWGATE_LIVE_CONFIG to a private live config before deploy." >&2; exit 2)
	@$(PYTHON) scripts/preflight_live_deploy.py --config "$(LIVE_CONFIG)"
	@echo "Syncing code/models to EC2 without restarting live engine..."
	@echo "Config: $(LIVE_CONFIG)"
	@echo "Model dir: $(DEPLOY_MODEL_DIR)"
	@ssh $(EC2) "mkdir -p $(EC2_DIR)/$(DEPLOY_MODEL_DIR) $(EC2_DIR)/strategy $(EC2_DIR)/execution $(EC2_DIR)/live/orderbook $(EC2_DIR)/live/venues $(EC2_DIR)/live/profiles $(EC2_DIR)/logs $(EC2_DIR)/data $(EC2_DIR)/features $(EC2_DIR)/models/replay $(EC2_DIR)/scripts $(EC2_DIR)/cpp/narrowgate_cpp $(EC2_DIR)/formal_collection/order_lifecycle_journal_v2 $(EC2_DIR)/formal_collection/prospective_baseline_epochs $(EC2_DIR)/research/families/f02_empirical_p3_touch $(EC2_DIR)/research/families/f03_causal_13_head $(EC2_DIR)/research/families/f05_fill_quality_quote_ev $(EC2_DIR)/research/families/f06_placement_fill_cif/cpp $(EC2_DIR)/research/families/f07_active_order_continuation/cpp $(EC2_DIR)/research/families/f10_live_replay_attribution/docs"
	@for f in $(DEPLOY_FILES); do \
		scp $$f $(EC2):$(EC2_DIR)/$$f; \
	done
	@ssh $(EC2) "cd $(EC2_DIR) && rm -f $(DEPLOY_REMOVED_FILES) && rm -rf $(DEPLOY_REMOVED_DIRS) && (rmdir market_data/orderbook market_data 2>/dev/null || true)"
	@echo "Rebuilding the remote C++ extension from the synchronized sources..."
	@ssh $(EC2) "cd $(EC2_DIR) && $(REMOTE_PYTHON) -m pip install -e cpp --no-build-isolation"
	@scp "$(LIVE_CONFIG)" $(EC2):$(EC2_DIR)/live/config.yaml
	@for f in $(DEPLOY_MODELS); do \
		scp $$f $(EC2):$(EC2_DIR)/$$f; \
	done
	@echo "Synced. Live engine was not restarted; reload/restart manually only after explicit confirmation."

deploy-dry:
	@$(PYTHON) scripts/preflight_live_deploy.py --config "$(LIVE_CONFIG)"
	@echo "Files to deploy:"
	@for f in $(DEPLOY_FILES); do echo "  $$f"; done
	@echo "Files removed after sync:"
	@for f in $(DEPLOY_REMOVED_FILES); do echo "  $$f"; done
	@for f in $(DEPLOY_REMOVED_DIRS); do echo "  $$f/"; done
	@echo "Config: $(LIVE_CONFIG) -> live/config.yaml"
	@echo "Model dir: $(DEPLOY_MODEL_DIR)"
	@for f in $(DEPLOY_MODELS); do echo "  $$f"; done

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
	deploy deploy-dry clean clean-logs
