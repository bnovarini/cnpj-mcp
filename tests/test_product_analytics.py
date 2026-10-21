import asyncio
import json
import time

import pytest
from cnpj_mcp.observability import Observability
from cnpj_mcp.product_analytics import duration_bucket, result_bucket, response_buckets
from cnpj_mcp.usage import UsageLog
from test_observability import wire, SECRET, invoke


def events(wire):
    return [e for url,b in wire for e in b.get('batch',[]) if e.get('event')=='tool_called']


def test_duration_and_result_boundaries():
    assert [duration_bucket(n) for n in (0,99,100,499,500,999,1000,4999,5000,9999,10000)] == [
        '<100ms','<100ms','100-499ms','100-499ms','500-999ms','500-999ms','1-5s','1-5s','5-10s','5-10s','10s+']
    assert [result_bucket([None]*n) for n in (0,1,10,11,100,101)]==['0','1-10','1-10','10-100','10-100','100+']
    assert response_buckets(json.dumps({'result':{'structuredContent':{'companies':[]}}}))==['0']
    assert response_buckets(json.dumps({'result':{'structuredContent':{'result':[]}}}))==['0']
    assert response_buckets(json.dumps({'result':{'structuredContent':{'result':[{'total_matching':0,'returned':0}]}}}))==['0']
    assert response_buckets(json.dumps({'result':{'isError':True,'content':[{'type':'text','text':SECRET}]}}))==['unknown']


def test_one_aggregate_event_per_call_no_argument_values(wire):
    obs=Observability('test_dummy_key',{'count_companies':{'uf','group_by'}},analytics=True)
    rows=[('ts','tool_call','count_companies','client','1','')]
    obs.record(rows,time.time(),201,'ok',shapes=[('count_companies',['uf','group_by',SECRET])],measurements=[dict(tool='count_companies',result_bucket='0')])
    obs.shutdown()
    ev=events(wire)
    assert len(ev)==1
    p=ev[0]['properties']
    assert set(p)=={'tool','status','duration_bucket','result_bucket','argument_names','client_name','client_version','$process_person_profile','$geoip_disable'}
    assert p['argument_names']==['group_by','uf'] and p['result_bucket']=='0'
    assert p['duration_bucket']=='100-499ms'
    assert p['$process_person_profile'] is False and p['$geoip_disable'] is True
    assert ev[0]['distinct_id']
    text=json.dumps(wire)
    for bad in [SECRET,'$session_id','posthogDistinctId','stacktrace','$ip','user_agent']:
        assert bad not in text


def test_anonymous_ids_are_per_event_not_retention(wire):
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}},analytics=True)
    for _ in range(2):obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),1,'ok')
    obs.shutdown()
    ev=events(wire)
    assert len(ev)==2 and ev[0]['distinct_id']!=ev[1]['distinct_id']


def test_failure_adds_one_analytics_not_duplicate_error(wire):
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}},analytics=True)
    obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),1,'exception','ValueError')
    obs.shutdown()
    all_events=[e for url,b in wire for e in b.get('batch',[])]
    assert len(events(wire))==1
    assert sum(e['event']=='$exception' for e in all_events)==1


def test_no_analytics_without_flag_and_no_handshake_event(wire):
    obs=Observability('test_dummy_key',['count_companies'])
    obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),1,'ok')
    obs.shutdown()
    assert not events(wire)
    obs=Observability('test_dummy_key',['count_companies'],analytics=True)
    obs.record([('ts','initialize',None,'client','1','')],time.time(),1,'ok')
    obs.shutdown()
    assert not events(wire)


def test_inherited_person_session_and_arbitrary_props_scrubbed(wire):
    from posthog import new_context,identify_context,set_context_session
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}},analytics=True)
    with new_context():
        identify_context(SECRET);set_context_session(SECRET)
        obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),1,'ok')
    obs.shutdown()
    assert SECRET not in json.dumps(wire)


def test_actual_middleware_one_event_and_exact_response(wire):
    async def app(scope,receive,send):
        await receive()
        await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':json.dumps({'result':{'structuredContent':{'companies':[]},'content':[{'type':'text','text':SECRET}]}}).encode()})
    req={'method':'tools/call','params':{'name':'count_companies','arguments':{'uf':SECRET,SECRET:SECRET}}}
    obs=Observability('test_dummy_key',{'count_companies':{'uf'}},analytics=True)
    assert asyncio.run(invoke(app,req))==asyncio.run(invoke(UsageLog(app,['count_companies'],obs,False),req))
    obs.shutdown()
    assert len(events(wire))==1 and SECRET not in json.dumps(wire)
    p=events(wire)[0]['properties']
    assert p['argument_names']==['uf'] and p['result_bucket']=='unknown'
