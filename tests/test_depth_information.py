import json
from dataclasses import replace

import pyarrow.parquet as pq
import pytest

from research.families.f08_side_taker_lifecycle import depth_information as scan
from test_depth_observation import stream
from tests.test_research_public_inputs import bundle  # noqa: F401


CONFIG = dict(levels=800, sample_period_ns=100_000_000, start_ns=0, end_ns=400_000_000)


def test_scanner_checks_original_fields_and_writes_bounded_coverage(tmp_path, monkeypatch):
    monkeypatch.setattr(scan, "streams", lambda config, tap, verified: stream(tap))
    output = tmp_path / "comparison"
    result = scan.run(CONFIG, output)
    assert result["status"] == "OBSERVATION_PARITY_PASSED"
    assert result["original_stream_hashes"][0] == result["original_stream_hashes"][1]
    assert result["economic_replays"] == 0
    assert result["strategy_parity"] == "not_executed"
    rows = pq.read_table(output / "depth-coverage.parquet").to_pylist()
    assert len(rows) == result["counters"]["sample_side_rows"] == 8
    assert all(row["levels"] == 25 for row in rows)
    assert result["counters"]["fewer_than_requested"] == 8
    assert result["deep_pending"] == 0
    with pytest.raises(FileExistsError):
        scan.run(CONFIG, output)


def test_scanner_failure_is_not_a_success_receipt(tmp_path, monkeypatch):
    class Changed:
        def __init__(self, original):
            self.original = original
            self.stats = original.stats
            self.profile = original.profile

        def __iter__(self):
            for tick in self.original:
                yield replace(tick, now_ns=tick.now_ns+1)

    monkeypatch.setattr(scan, "streams", lambda config, tap, verified:
                        Changed(stream(tap)) if tap else stream())
    output = tmp_path / "failure"
    with pytest.raises(ValueError, match="first diverged"):
        scan.run(CONFIG, output)
    assert not (output / "result.json").exists()
    receipt = json.loads((output / "progress.json").read_text())
    assert receipt["status"] == "FAILED"
    assert receipt["error"]


def test_coverage_only_does_not_claim_parity(tmp_path, monkeypatch):
    monkeypatch.setattr(scan, "streams", lambda config, tap, verified: stream(tap))
    receipt = scan.run(CONFIG, tmp_path / "coverage", compare=False)
    assert receipt["status"] == "COVERAGE_SCAN_COMPLETE"
    assert not receipt["comparison_executed"]
    assert receipt["original_stream_hashes"] is None
    assert receipt["information_increment"] == "not_estimated"


def account_source(root):
    from data.runtime import ConsumerBundle
    from research.families.f08_side_taker_lifecycle.depth_price_execution import bind_depth_source
    reader = ConsumerBundle(root)
    plan = reader.manifest['plan']
    baseline = dict(spec=dict(input_manifest_id=reader.input_manifest_id,
        start_ns=plan['start_ns'], end_ns=plan['end_ns']),
        timeline_identity='frozen-original', timeline_model_identity='original-model')
    binding = dict(bundle=str(root), timelines='unchanged-timelines')
    return bind_depth_source(binding, baseline), binding, baseline


def test_full_account_depth_binding_preserves_all_original_tick_fields(bundle):  # noqa: F811
    from data.depth_observation import DepthStateTap
    from data.runtime import ConsumerBundle
    source, _, _ = account_source(bundle)
    expected = list(ConsumerBundle(bundle).stream())
    got = list(scan.streams(source, DepthStateTap()))
    assert [replace(t, research_depth_observations=()) for t in got] == expected
    assert any(t.research_depth_observations for t in got)


@pytest.mark.parametrize('field,value', [('input_manifest_id', 'different'),
    ('end_ns', 5_000_000_000), ('start_ns', 0)])
def test_full_depth_binding_rejects_wrong_input_and_short_engineering_cut(bundle, field, value):  # noqa: F811
    source, _, _ = account_source(bundle)
    source[field] = value
    with pytest.raises(ValueError):
        scan.admitted_observation(source)


def test_full_depth_binding_rejects_changed_source_manifest(bundle):  # noqa: F811
    from data.runtime import ConsumerBundle
    source, _, _ = account_source(bundle)
    manifest = ConsumerBundle(bundle).source_paths()[0]/'manifest.json'
    manifest.write_text(manifest.read_text()+'\n')
    with pytest.raises(ValueError, match='source manifest changed'):
        scan.admitted_observation(source)


def test_calibration_location_changes_no_delay_rule_or_input_identity(bundle, tmp_path):  # noqa: F811
    from narrowgate.replay.account import sha
    manifest = bundle/'manifest.json'
    data = json.loads(manifest.read_text())
    calibration = tmp_path/'original-latency.json'
    calibration.write_text('{"fixture":"original paired measurements"}')
    data['plan']['observation_profile'].update(market_delay_ns=0, processing_ns=0,
        measured_latency_path='/unavailable-original-location.json',
        measured_latency_sha256=sha(calibration), measured_latency_market_id='binance:perp:BTCUSDC')
    manifest.write_text(json.dumps(data))
    from data.runtime import ConsumerBundle
    current = ConsumerBundle(bundle)
    source = dict(schema='f08.depth-price-source.v1', bundle=str(bundle),
        input_manifest_id=current.input_manifest_id, start_ns=1_000_000_000, end_ns=4_000_000_000,
        measured_latency_location=str(calibration))
    _, profile, _ = scan.admitted_observation(source)
    expected = dict(data['plan']['observation_profile'], measured_latency_path=str(calibration))
    assert vars(profile) == vars(scan.ObservationProfile(**expected))
    assert ConsumerBundle(bundle).input_manifest_id == source['input_manifest_id']
    calibration.write_text('{"fixture":"different"}')
    with pytest.raises(ValueError, match='relocated measured latency differs'):
        scan.admitted_observation(source)


def test_full_depth_binding_matches_actual_main_account_paths(bundle):  # noqa: F811
    from research.families.f08_side_taker_lifecycle.depth_price_execution import validate_depth_source
    source, binding, baseline = account_source(bundle)
    validate_depth_source(source, binding, baseline, baseline['spec'])
    source['frozen_information']['root'] = 'different-predictions'
    with pytest.raises(ValueError, match='differs from account binding'):
        validate_depth_source(source, binding, baseline, baseline['spec'])


def test_original_preroll_kept_without_expanding_it_or_shortening_account(bundle):  # noqa: F811
    from data.depth_observation import DepthStateTap
    from research.families.f08_side_taker_lifecycle.depth_price_execution import validate_depth_source
    source,binding,baseline=account_source(bundle)
    source['start_ns']=baseline['spec']['start_ns']=2_000_000_000
    validate_depth_source(source,binding,baseline,baseline['spec'])
    ticks=list(scan.streams(source,DepthStateTap()))
    assert ticks[0].now_ns==1_000_000_000  # original input's one-second preroll
    assert all(t.now_ns<4_000_000_000 for t in ticks)
    source['end_ns']=3_000_000_000
    with pytest.raises(ValueError,match='differs from account binding'):
        validate_depth_source(source,binding,baseline,baseline['spec'])
