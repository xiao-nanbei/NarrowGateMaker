import copy
import json
import time
import pytest
from poolrun import model
from poolrun.cache_inventory import scan, locality_reason, Inventory
from poolrun.master import Master
from poolrun.store import digest
from test_core import state, task, report


IDENTITY = {'schema': 'public-prepared-v2', 'manifest': 'synthetic', 'tick': '0.1'}


def inventory_report(warm=False):
    return dict(report(), cache_inventory={'tape': {'complete': True, 'ready': [digest(IDENTITY)] if warm else []}}, cache_inventory_age_seconds=0)


def setup():
    s=state();s['hosts']['other']=copy.deepcopy(s['hosts']['h'])
    spec=task();spec['cache_affinity']={'namespace':'tape','identity':IDENTITY}
    model.submit(s,[spec]);return s,s['tasks']['t']


def test_scan_checks_identity_files_and_ignores_partial_or_symlink(tmp_path):
    good=tmp_path/'good';good.mkdir();(good/'data').write_bytes(b'abc')
    (good/'manifest.json').write_text(json.dumps({'identity':IDENTITY,'files':{'data':3}}))
    partial=tmp_path/'copy.part-building';partial.mkdir()
    (partial/'manifest.json').write_text((good/'manifest.json').read_text())
    (tmp_path/'link').symlink_to(good,target_is_directory=True)
    assert scan({'tape':str(tmp_path)})['tape']['ready']==[digest(IDENTITY)]
    (good/'data').unlink()
    row=scan({'tape':str(tmp_path)})['tape']
    assert row['complete'] and not row['ready'] and row['errors']
    assert not scan({'tape':str(tmp_path/'absent')})['tape']['complete']


def test_inventory_refresh_discovers_deletion(tmp_path):
    inv=Inventory({'tape':str(tmp_path)});first,_=inv.report()
    assert first['tape']['complete']
    tmp_path.rmdir();inv.checked-=31
    second,_=inv.report();assert not second['tape']['complete']


def test_wait_for_all_hosts_then_prefer_busy_warm_host():
    s,t=setup();now=time.time();t['submitted']=now
    reports={'h':inventory_report()};seen={'h':now}
    assert locality_reason(s,t,'h',reports,seen,now,now).startswith('WAIT_CACHE_INVENTORY')
    reports['other']=inventory_report(True);seen['other']=now
    s['hosts']['other']['slots']=1
    model.submit(s,[task('busy')]);model.start(s,'busy','other',report())
    assert locality_reason(s,t,'h',reports,seen,now,now).startswith('WAIT_CACHE_HOST')
    assert locality_reason(s,t,'other',reports,seen,now,now) is None
    assert model.eligible(s,t,'other',reports['other'],now)=='slots reserved'
    s['hosts']['other']['draining']=True
    assert locality_reason(s,t,'h',reports,seen,now,now) is None


def test_offline_host_timeout_but_local_scan_required():
    s,t=setup();now=time.time();t['submitted']=now-40
    reports={'h':inventory_report()};seen={'h':now}
    assert locality_reason(s,t,'h',reports,seen,now,now-40) is None
    reports['h']['cache_inventory_age_seconds']=90
    assert locality_reason(s,t,'h',reports,seen,now,now-40).startswith('WAIT_CACHE_INVENTORY')


def test_wrong_identity_not_a_cache_hit_and_no_hint_unchanged():
    s,t=setup();now=time.time();reports={h:inventory_report(True) for h in s['hosts']};seen={h:now for h in reports}
    t['spec']['cache_affinity']['identity']=dict(IDENTITY,tick='0.2')
    assert locality_reason(s,t,'h',reports,seen,now,now) is None
    del t['spec']['cache_affinity']
    assert locality_reason(s,t,'h',{}, {},now,now) is None


def test_pending_placement_excludes_old_warm_host():
    s,t=setup(); now=time.time()
    t['spec']['hosts']=['h']; t['placement_hosts']=['other']
    reports={'h':inventory_report(True),'other':inventory_report(False)}
    seen={h:now for h in reports}
    assert locality_reason(s,t,'other',reports,seen,now,now) is None
    assert model.eligible(s,t,'h',reports['h'],now)=='host restriction'
    reports['other']=inventory_report(True)
    assert locality_reason(s,t,'other',reports,seen,now,now) is None


def test_master_poll_and_start_enforce_same_locality(tmp_path):
    s,t=setup();m=Master(tmp_path,{'hosts':s['hosts']})
    try:
        m.store.transition(None,lambda st:st.update(tasks=s['tasks'],releases=s['releases']))
        assert 'offer' not in m.poll({'host':'h','report':inventory_report()})
        assert m.poll({'host':'other','report':inventory_report(True)})['offer']=='t'
        assert 'offer' not in m.poll({'host':'h','report':inventory_report()})
        with pytest.raises(ValueError,match='WAIT_CACHE_HOST'):
            m.start({'host':'h','task_id':'t','request_id':'cold-start'})
        a=m.start({'host':'other','task_id':'t','request_id':'warm-start'})
        assert a['host']=='other'
    finally:m.store.close()


def test_filtered_selection_keeps_dependency_state():
    s=state();up=task('up');down=task('down');down['depends_on']=['up']
    model.submit(s,[up,down]);s['tasks']['up']['status']='COMPLETE'
    assert model.choose(s,'h',report(),allowed={'down'})=='down'


def test_invalid_hint_rejected():
    s=state();spec=task();spec['cache_affinity']={'namespace':'tape','identity':{}}
    with pytest.raises(ValueError,match='cache_affinity'):model.submit(s,[spec])


def test_agent_report_includes_actual_directory_inventory(tmp_path):
    from poolrun.agent import Agent
    cache=tmp_path/'cache';cache.mkdir();entry=cache/'one';entry.mkdir()
    (entry/'array').write_bytes(b'123')
    (entry/'manifest.json').write_text(json.dumps({'identity':IDENTITY,'files':{'array':3}}))
    agent=Agent({'host':'h','root':str(tmp_path/'agent'),'ssh':{'target':'synthetic',
                 'identity_file':str(tmp_path/'unused-key'),'known_hosts_file':str(tmp_path/'unused-hosts')},
                 'prepared_cache_roots':{'tape':str(cache)}})
    try:
        r=agent.report()
        assert r['cache_inventory']['tape']['ready']==[digest(IDENTITY)]
        assert r['cache_inventory_age_seconds']<1
    finally:agent.store.close()
