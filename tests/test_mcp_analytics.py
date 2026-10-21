import json
import time
import asyncio
from cnpj_mcp.observability import Observability, from_env
from cnpj_mcp.mcp_analytics import FIELDS, properties, scrub
from cnpj_mcp.usage import UsageLog
from test_observability import wire, SECRET, invoke


def event_list(wire):
    return [e for _, body in wire for e in body.get('batch', [])]


def test_disabled_by_default_and_old_analytics_retained(wire, monkeypatch):
    monkeypatch.setenv('CNPJ_POSTHOG_TOKEN', 'test_dummy_key')
    monkeypatch.setenv('CNPJ_POSTHOG_ANALYTICS', '1')
    monkeypatch.delenv('CNPJ_POSTHOG_MCP_ANALYTICS', raising=False)
    o = from_env({'lookup': set()})
    assert o.mcp_analytics is False
    o.record([('ts','tool_call','lookup',None,None,'')], time.time(), 201, 'ok')
    o.shutdown()
    assert [e['event'] for e in event_list(wire)] == ['tool_called']


def test_canonical_replaces_legacy_no_duplicate_and_no_values(wire):
    o = Observability('test_dummy_key', {'lookup': {'uf'}}, analytics=True, mcp_analytics=True)
    o.record([('ts','tool_call','lookup',SECRET,SECRET,'')], time.time(), 201.25, 'ok',
             shapes=[('lookup', ['uf', SECRET])], measurements=[{'tool':'lookup', 'result_bucket':'100+'}])
    o.shutdown()
    ev = event_list(wire)
    assert len(ev) == 1 and ev[0]['event'] == '$mcp_tool_call'
    assert set(ev[0]['properties']) == FIELDS
    p = ev[0]['properties']
    assert p['$mcp_duration_ms'] == 500 and not p['$mcp_is_error']
    assert p['$mcp_source'] == 'posthog_mcp_analytics'
    assert p['$mcp_tool_name'] == p['$mcp_resource_name'] == 'lookup'
    # Other diagnostics retain previously approved client metadata. Check canonical only.
    text = json.dumps(ev)
    for bad in [SECRET, '201.25', 'argument_names', 'result_bucket', '$session_id', '$mcp_parameters',
                '$mcp_response', '$mcp_intent', '$mcp_client', '$mcp_conversation', '$ip', 'user_agent']:
        assert bad not in text


def test_failure_only_existing_single_exception(wire):
    o = Observability('test_dummy_key', ['lookup'], analytics=True, mcp_analytics=True)
    o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'exception','ValueError')
    o.shutdown()
    ev = event_list(wire)
    assert [e['event'] for e in ev].count('$exception') == 1
    assert [e['event'] for e in ev].count('$mcp_tool_call') == 1
    assert 'tool_called' not in [e['event'] for e in ev]
    p = [e['properties'] for e in ev if e['event']=='$mcp_tool_call'][0]
    assert p['$mcp_is_error'] and p['$mcp_error_type'] == 'validation'


def test_no_identity_inheritance_and_ids_per_call(wire):
    from posthog import new_context, identify_context, set_context_session
    o = Observability('test_dummy_key', ['lookup'], mcp_analytics=True)
    with new_context():
        identify_context(SECRET); set_context_session(SECRET)
        for _ in range(2):o.record([('ts','tool_call','lookup',None,None,'')],time.time(),1,'ok')
    o.shutdown()
    ev = event_list(wire)
    assert len(ev)==2 and ev[0]['distinct_id'] != ev[1]['distinct_id']
    assert SECRET not in json.dumps(ev) and '$session_id' not in json.dumps(ev)
    assert all(not e['properties']['$process_person_profile'] for e in ev)


def test_hook_rebuild_and_invalid_input():
    p = properties('lookup', 'ok', 500, 'none', ['lookup'])
    p.update({'$session_id':SECRET, '$mcp_response':SECRET, '$mcp_parameters':SECRET})
    e = scrub({'properties':p, 'distinct_id':'sdk-generated'}, ['lookup'])
    assert set(e['properties']) == FIELDS and SECRET not in json.dumps(e)
    for t, s, d in [('evil','ok',1),('lookup','evil',1),('lookup','ok',float('nan')),('lookup','ok',-1)]:
        assert properties(t,s,d,'none',['lookup']) is None
    assert properties('lookup','ok',20000,'none',['lookup'])['$mcp_duration_ms'] == 10001


def test_middleware_response_exact_and_handshake_not_exported(wire):
    async def app(scope,receive,send):
        await receive()
        await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':b'{"result":{"content":[{"type":"text","text":"unchanged"}]}}'})
    o = Observability('test_dummy_key', ['lookup'], analytics=True, mcp_analytics=True)
    req = {'method':'tools/call','params':{'name':'lookup','arguments':{'uf':SECRET}}}
    assert asyncio.run(invoke(app,req)) == asyncio.run(invoke(UsageLog(app,['lookup'],o,False),req))
    o.record([('ts','initialize',None,None,None,'')],time.time(),1,'ok')
    o.shutdown()
    assert [e['event'] for e in event_list(wire)] == ['$mcp_tool_call']
