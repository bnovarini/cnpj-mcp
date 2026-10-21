import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from cnpj_mcp.telemetry_context import calls, current, instrument, bucket, error_class
from cnpj_mcp.observability import Observability, scrub_event
from test_observability import wire, SECRET, invoke
from test_telemetry_logs import logs_wire
from cnpj_mcp.usage import UsageLog


def test_size_bucket_and_type_only_errors():
    assert [bucket([None]*n) for n in (0,1,10,11,100,101)] == ['0','1-10','1-10','10-100','10-100','100+']
    assert bucket({'companies':[]})=='0'
    assert bucket({'partners':[{}]*15})=='10-100'
    class Dangerous(ValueError):
        def __str__(self):raise AssertionError('Message inspected')
    assert error_class(Dangerous())=='validation'
    assert error_class(TimeoutError(SECRET))=='timeout'
    import duckdb
    assert error_class(duckdb.IOException(SECRET))=='db_error'


def test_context_measurement_returns_original_value():
    async def fn(**kw):
        current.get()['db_ms'] += 2
        current.get()['db_calls'] += 1
        return [{'secret':SECRET}]*11
    tool=SimpleNamespace(name='search_companies',fn=fn)
    instrument(tool)
    rows=[];t=calls.set(rows)
    try:result=asyncio.run(tool.fn(uf=SECRET))
    finally:calls.reset(t)
    assert result[0]['secret']==SECRET
    assert rows[0]['result_bucket']=='10-100'
    assert rows[0]['db_ms']==2 and rows[0]['db_calls']==1
    assert SECRET not in json.dumps(rows)


def test_all_fields_actual_event_and_span_wire(wire,monkeypatch):
    monkeypatch.setenv('CNPJ_DEPLOY_VERSION','context-v1')
    obs=Observability('test_dummy_key',{'search_companies':{'uf','cnae'}})
    measured=[dict(tool='search_companies',error_class='validation',result_bucket='0',db_ms=3,db_calls=1)]
    obs.record([('ts','tool_call','search_companies','client','1','')],time.time(),9,'tool_error',measurements=measured,
               shapes=[('search_companies',['uf','cnae',SECRET])])
    obs.shutdown()
    text=json.dumps(wire)
    assert SECRET not in text
    for value in ['validation','context-v1','not_applicable','argument_names','cnae,uf','db_ms','local_ms','upstream_ms','utc_hour']:
        assert value in text
    # Additional properties, not additional events: one trace, one error event.
    events=[event for url,b in wire for event in b.get('batch',[]) if event.get('event')=='$exception']
    assert len(events)==1
    assert events[0]['properties']['upstream']=='DB'
    assert events[0]['properties']['db_ms']==3 and events[0]['properties']['local_ms']==6


def test_all_fields_logs_wire(logs_wire):
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}},logs=True)
    obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),2,'ok',
               measurements=[dict(tool='count_companies',error_class='none',db_calls=1,db_ms=1,result_bucket='1-10')],
               shapes=[('count_companies',['uf',SECRET])])
    obs.shutdown()
    text=json.dumps(logs_wire)
    assert SECRET not in text
    for field in ['error_class','argument_names','result_bucket','cache','app_version','deploy_version','utc_hour','db_ms']:
        assert field in text


def test_middleware_schema_shape_and_values_never_escape(wire):
    async def app(scope,receive,send):
        await receive()
        await send({'type':'http.response.start','status':200})
        await send({'type':'http.response.body','body':b'{"result":{"isError":true,"text":"' + SECRET.encode() + b'"}}'})
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}})
    request={'method':'tools/call','params':{'name':'count_companies','arguments':{'uf':SECRET,SECRET:SECRET}}}
    plain=asyncio.run(invoke(app,request))
    actual=asyncio.run(invoke(UsageLog(app,['count_companies'],obs,False),request))
    obs.shutdown()
    assert actual==plain and SECRET not in json.dumps(wire)
    assert 'argument_names' in json.dumps(wire)


def test_final_hook_rejects_untrusted_context_fields():
    props=dict(tool='count_companies',status='tool_error',error_class=SECRET,upstream=SECRET,argument_names='uf,'+SECRET,
               result_bucket=SECRET,cache=SECRET,deploy_version='https://'+SECRET,db_ms=SECRET,utc_hour=SECRET)
    safe=scrub_event({'event':'$exception','distinct_id':'sdk-anonymous','properties':props})
    assert SECRET not in json.dumps(safe)
    assert safe['properties']['argument_names']=='uf'


def test_real_tool_db_timing_and_result_measurement(monkeypatch):
    import os,importlib
    import cnpj_mcp.mcp_server as srv
    from starlette.testclient import TestClient
    from pathlib import Path
    # Dataset built by existing test_server fixture earlier in suite.
    root=Path(os.environ['CNPJ_DATA_DIR'])
    if not (root/'meta.json').exists():pytest.skip('Synthetic dataset not built')
    import cnpj_mcp.observability as obs_module
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}})
    spy=Mock()
    monkeypatch.setattr(obs_module,'from_env',lambda tools:spy)
    app=srv.http_app()
    with TestClient(app) as client:
        response=client.post('/mcp',headers={'accept':'application/json, text/event-stream'},json={
            'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'count_companies','arguments':{'uf':'SP'}}})
    assert response.status_code==200
    args=spy.record.call_args
    measured=args.kwargs['measurements'][0]
    assert measured['db_calls']>0 and measured['db_ms']>0
    assert measured['error_class']=='none' and measured['result_bucket']=='1-10'
    assert args.kwargs['shapes']==[('count_companies',['uf'])]
    obs.shutdown()
