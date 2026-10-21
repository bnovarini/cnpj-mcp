import asyncio
import json
import time
from types import SimpleNamespace
import pytest
from cnpj_mcp.error_triage import ExpectedValidationError, WorkerBusyError
from cnpj_mcp.observability import Observability
from cnpj_mcp.telemetry_context import instrument, calls
from test_observability import wire, SECRET


def exceptions(wire):
    return [e for _,b in wire for e in b.get('batch',[]) if e['event']=='$exception']


@pytest.mark.parametrize('outcome', ['validation','busy','not_found'])
def test_confirmed_expected_only_suppressed(wire, outcome):
    o=Observability('test', ['lookup'], error_triage=True, mcp_analytics=True)
    o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'tool_error',
             measurements=[{'tool':'lookup','error_class':'tool_error','expected_outcome':outcome}])
    o.shutdown()
    assert not exceptions(wire)
    assert any(e['event']=='$mcp_tool_call' for _,b in wire for e in b.get('batch',[]))
    assert any('resourceSpans' in b for _,b in wire)


@pytest.mark.parametrize('code', ['validation','timeout','db_error','internal','tool_error','rpc_error','rate_limit','http_error','aborted'])
def test_unconfirmed_errors_retained_grouped_and_marked(wire, code):
    o=Observability('test', ['lookup'], error_triage=True)
    o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'tool_error',
             measurements=[{'tool':'lookup','error_class':code}])
    o.shutdown(); ev=exceptions(wire)
    assert len(ev)==1
    p=ev[0]['properties']
    assert p['$exception_fingerprint']=='cnpj:v1:lookup:'+code
    assert p['$issue_name'].startswith('lookup: ')
    assert p['$exception_list'][0]['mechanism']=={'type':'cnpj_manual','handled':True,'synthetic':True}
    assert all(x not in json.dumps(ev) for x in [SECRET,'stacktrace','locals','$session_id'])


def test_escaping_exception_not_falsely_handled(wire):
    o=Observability('test', ['lookup'], error_triage=True)
    o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'exception','RuntimeError')
    o.shutdown()
    assert exceptions(wire)[0]['properties']['$exception_list'][0]['mechanism']['handled'] is False


def test_fingerprint_stable_across_releases_but_separate_tools(wire,monkeypatch):
    for version,tool in [('v1','lookup'),('v2','lookup'),('v2','search')]:
        monkeypatch.setenv('CNPJ_DEPLOY_VERSION',version)
        o=Observability('test',['lookup','search'],error_triage=True)
        o.record([('ts','tool_call',tool,None,None,'')],time.time(),1,'tool_error')
        o.shutdown()
    p=[e['properties'] for e in exceptions(wire)]
    assert p[0]['$exception_fingerprint']==p[1]['$exception_fingerprint']!=p[2]['$exception_fingerprint']
    assert p[0]['deploy_version']!=p[1]['deploy_version']


def test_expected_marker_uses_class_never_message():
    class Dangerous(ExpectedValidationError):
        def __str__(self):raise AssertionError('message inspected')
    async def run(exc):
        async def fail():raise exc
        t=SimpleNamespace(name='lookup',fn=fail);instrument(t)
        rows=[];token=calls.set(rows)
        try:
            with pytest.raises(type(exc)):await t.fn()
        finally:calls.reset(token)
        return rows[0]
    assert asyncio.run(run(Dangerous()))['expected_outcome']=='validation'
    assert asyncio.run(run(WorkerBusyError()))['expected_outcome']=='busy'
    assert 'expected_outcome' not in asyncio.run(run(ValueError('ordinary programming error')))


def test_flag_off_keeps_expected_exception(wire):
    o=Observability('test',['lookup'])
    o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'tool_error',
             measurements=[{'tool':'lookup','expected_outcome':'validation'}])
    o.shutdown(); assert len(exceptions(wire))==1
    assert '$exception_fingerprint' not in exceptions(wire)[0]['properties']


def test_real_not_found_marked_without_reading_message(tmp_path_factory):
    from test_server import srv
    from cnpj_mcp.telemetry_context import current
    server=srv.__wrapped__(tmp_path_factory)
    data={'db_calls':0,'db_ms':0}; token=current.set(data)
    try:
        assert 'error' in server.get_company('00000000000000')
    finally:current.reset(token)
    assert data['expected_outcome']=='not_found'
    with pytest.raises(ExpectedValidationError):server.get_company('bad')
