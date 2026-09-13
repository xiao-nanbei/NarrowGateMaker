from __future__ import annotations

import yaml

from scripts.configure_receive_time_capture import update_capture_config

SAMPLE = """\
strategy:
  fill_cooldown: 85.0
external_venues:
  enabled: true
  shadow_only: true
  sources:
    - venue: "bitget"
      enabled: true
      instrument_type: "perp"
      record_enabled: false
      record_queue_size: 100
    - venue: "okx"
      enabled: true
      instrument_type: "spot"
      record_enabled: false
      record_queue_size: 100
logging:
  level: "INFO"
  market_tape_enabled: false
  market_tape_queue_size: 100
risk:
  max_daily_loss: 50.0
"""


def test_capture_toggle_changes_only_recorder_fields() -> None:
    before = yaml.safe_load(SAMPLE)
    updated, sources = update_capture_config(SAMPLE, enabled=True, queue_size=20_000)
    after = yaml.safe_load(updated)

    assert sources == ["bitget:perp", "okx:spot"]
    assert before["strategy"] == after["strategy"]
    assert before["risk"] == after["risk"]
    assert after["logging"]["market_tape_enabled"] is True
    assert after["logging"]["market_tape_queue_size"] == 20_000
    assert all(source["record_enabled"] for source in after["external_venues"]["sources"])
    assert all(
        source["record_queue_size"] == 20_000
        for source in after["external_venues"]["sources"]
    )


def test_capture_disable_is_idempotent() -> None:
    enabled, _ = update_capture_config(SAMPLE, enabled=True)
    disabled, _ = update_capture_config(enabled, enabled=False)
    disabled_again, _ = update_capture_config(disabled, enabled=False)

    parsed = yaml.safe_load(disabled)
    assert parsed["logging"]["market_tape_enabled"] is False
    assert not any(
        source["record_enabled"] for source in parsed["external_venues"]["sources"]
    )
    assert disabled_again == disabled
