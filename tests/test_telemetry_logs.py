"""Inspect the real protobuf wire bytes. No live network or project token."""
import asyncio
import json
import logging
import time
from unittest.mock import Mock

import pytest
import requests
from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

from cnpj_mcp.observability import Observability
from cnpj_mcp.telemetry_logs import ToolLogs
from test_observability import invoke, SECRET
from cnpj_mcp.usage import UsageLog


@pytest.fixture
def logs_wire(monkeypatch):
    sent=[]
    def post(self,url,**kw):
        if url.endswith('/i/v1/logs'):
            decoded=ExportLogsServiceRequest.FromString(kw['data'])
            sent.append((url, MessageToDict(decoded), dict(kw.get('headers',{}))))
        r=requests.Response();r.status_code=200;r._content=b'{}';r.headers['Content-Length']='2'
        return r
    monkeypatch.setattr(requests.Session,'post',post)
    monkeypatch.setattr(requests.Session,'request',lambda self,method,url,**kw: post(self,url,**kw))
    monkeypatch.setattr(requests.Session,'get',lambda *a,**kw: pytest.fail('Unexpected GET'))
    return sent


def test_actual_log_payload_allowlist_and_fixed_body(logs_wire):
    logs=ToolLogs('test_dummy_key',['count_companies'])
    for status in ['ok','tool_error','exception','rpc_error','http_4xx','http_5xx','aborted']:
        logs.record(dict(tool='count_companies',status=status,duration_ms=9,client_name='TestClient',client_version='1',
                         arguments=SECRET,query=SECRET,exception=ValueError(SECRET),session_id=SECRET,ip=SECRET))
    logs.shutdown()
    assert logs_wire
    for url,batch,headers in logs_wire:
        assert url=='https://us.i.posthog.com/i/v1/logs'
        assert {k.lower():v for k,v in headers.items()}['authorization']=='Bearer test_dummy_key'
        assert SECRET not in json.dumps(batch)
        for resource in batch['resourceLogs']:
            assert resource['resource']['attributes']==[{'key':'service.name','value':{'stringValue':'cnpj-mcp'}}]
            for scope in resource['scopeLogs']:
                assert scope['scope']['name']=='cnpj_mcp.tool_events'
                for record in scope['logRecords']:
                    assert record['body']['stringValue'] in {'MCP tool request completed','MCP tool request failed'}
                    assert record['severityText'] in {'INFO','ERROR'}
                    assert not record.get('traceId') and not record.get('spanId')
                    assert {a['key'] for a in record['attributes']}=={'tool','status','duration_ms','client_name','client_version'}
    encoded=json.dumps(logs_wire)
    for forbidden in ['distinct_id','session_id','user_agent','stacktrace','exception.message','code.file','process.pid','host.name']:
        assert forbidden not in encoded


def test_third_party_and_access_logs_not_forwarded(logs_wire):
    root_handlers=list(logging.getLogger().handlers)
    logs=ToolLogs('test_dummy_key',['count_companies'])
    logging.getLogger('uvicorn.access').warning('198.51.100.2 GET /mcp?q=%s',SECRET)
    logging.getLogger('duckdb').error(SECRET)
    logging.getLogger('cnpj_mcp.tool_events').error(SECRET,exc_info=ValueError(SECRET))
    assert logging.getLogger().handlers==root_handlers
    logs.shutdown()
    assert not logs_wire


def test_logs_do_not_inherit_otel_context(logs_wire):
    from opentelemetry import trace
    from opentelemetry.trace import NonRecordingSpan,SpanContext,TraceFlags
    from opentelemetry.context import attach,detach
    token=attach(trace.set_span_in_context(NonRecordingSpan(SpanContext(123,456,False,TraceFlags(1)))))
    try:
        logs=ToolLogs('test_dummy_key',['count_companies'])
        logs.record({'tool':'count_companies','status':'ok','duration_ms':1})
        logs.shutdown()
    finally:detach(token)
    text=json.dumps(logs_wire)
    assert 'traceId' not in text and 'spanId' not in text


def test_integration_response_unchanged_logs_enabled(logs_wire):
    async def app(scope,receive,send):
        await receive()
        await send({'type':'http.response.start','status':200})
        await send({'type':'http.response.body','body':b'{"result":{"isError":true,"text":"' + SECRET.encode() + b'"}}'})
    request={'method':'tools/call','params':{'name':'count_companies','arguments':{'uf':SECRET}}}
    obs=Observability('test_dummy_key',['count_companies'],logs=True)
    assert obs.logs is not None
    assert asyncio.run(invoke(app,request))==asyncio.run(invoke(UsageLog(app,['count_companies'],obs,False),request))
    obs.shutdown()
    assert logs_wire and SECRET not in json.dumps(logs_wire)


def test_log_export_failure_keeps_existing_telemetry(logs_wire):
    obs=Observability('test_dummy_key',['count_companies'],logs=True)
    obs.logs.record=Mock(side_effect=RuntimeError(SECRET))
    obs.record([('ts','tool_call','count_companies',None,None,'')],time.time(),1,'ok')
    obs.shutdown()
