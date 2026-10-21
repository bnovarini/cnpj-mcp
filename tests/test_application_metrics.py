import gzip
import json
import contextvars
from unittest.mock import Mock
from cnpj_mcp.application_metrics import ApplicationMetrics
from cnpj_mcp.observability import Observability, from_env


def test_default_off(monkeypatch):
    monkeypatch.setenv('CNPJ_POSTHOG_TOKEN', 'test')
    monkeypatch.delenv('CNPJ_POSTHOG_METRICS', raising=False)
    o = from_env({'lookup': set()})
    assert o.metrics is None
    o.shutdown()


def test_allowlist_and_quantized_values():
    c = Mock()
    m = ApplicationMetrics(c, ['lookup'])
    m.record('lookup', 'ok', 123.45)
    c.metrics.count.assert_called_once_with('cnpj.tool.calls', 1, attributes={'tool': 'lookup', 'status': 'ok'}, unit=None)
    c.metrics.histogram.assert_called_once_with('cnpj.tool.duration', 500, attributes={'tool': 'lookup'}, unit='ms')
    for tool, status, duration in [('evil', 'ok', 1), ('lookup', 'SECRET', 1), ('lookup', 'ok', float('nan')), ('lookup', 'ok', -1)]:
        m.record(tool, status, duration)
    assert c.metrics.count.call_count == 1
    m.busy(True); m.sample()
    c.metrics.gauge.assert_called_with('cnpj.worker.busy', 1, attributes=None, unit=None)
    m.busy(False); m.sample()
    c.metrics.gauge.assert_called_with('cnpj.worker.busy', 0, attributes=None, unit=None)
    m.rejected(); m.shutdown()
    assert not m._thread.is_alive()


def test_wire_no_identity_exemplars_or_values(monkeypatch):
    import posthog.metrics_capture as capture
    sent = []
    session = Mock()
    session.post.side_effect = lambda url, **kw: sent.append(json.loads(gzip.decompress(kw['data']))) or Mock(status_code=200)
    monkeypatch.setattr(capture, '_get_session', lambda: session)
    # Fake ambient trace lookup: the real empty-context invariant is separately tested.
    from posthog import Posthog
    client = Posthog('test', host='https://us.i.posthog.com', enable_exception_autocapture=False,
                     metrics={'service_name': 'cnpj-mcp', 'flush_interval': 60, 'max_series_per_flush': 66})
    m = ApplicationMetrics(client, ['lookup'])
    with client.start_span('PRIVATE_TRACE'):
        m.record('lookup', 'ok', 123.45)
        m.rejected(); m.busy(True); m.sample()
    m.shutdown()
    payload = json.dumps(sent)
    for private in ('PRIVATE_TRACE', 'exemplars', 'traceId', 'spanId', 'distinct_id', 'session_id', 'host.name', 'process.pid', '123.45'):
        assert private not in payload
    resource = sent[0]['resourceMetrics'][0]['resource']['attributes']
    assert {x['key'] for x in resource} == {'service.name', 'telemetry.sdk.name', 'telemetry.sdk.version'}
    hist = [x for x in sent[0]['resourceMetrics'][0]['scopeMetrics'][0]['metrics'] if x['name']=='cnpj.tool.duration'][0]['histogram']['dataPoints'][0]
    assert hist['sum'] == hist['min'] == hist['max'] == 500
    assert sum(hist['bucketCounts']) == 1
    client.shutdown()


def test_failure_isolated():
    c = Mock(); c.metrics.count.side_effect = RuntimeError('private')
    m = ApplicationMetrics(c, ['lookup'])
    m.record('lookup', 'ok', 5)
    m.shutdown()


def test_worker_gate_rejects_and_releases(monkeypatch):
    import asyncio
    import threading
    from types import SimpleNamespace
    import cnpj_mcp.mcp_server as server
    entered, release = threading.Event(), threading.Event()
    def blocking():
        entered.set(); release.wait(timeout=3)
        return 'unchanged'
    tool = SimpleNamespace(name='lookup', fn=blocking, is_async=False)
    monkeypatch.setattr(server.mcp._tool_manager, 'list_tools', lambda: [tool])
    metrics = Mock()
    monkeypatch.setattr(server, '_metrics', metrics)
    server._offload_http_tools()
    async def run():
        first = asyncio.create_task(tool.fn())
        while not entered.is_set():
            await asyncio.sleep(.001)
        try:
            await tool.fn()
            assert False, 'second call should reject'
        except RuntimeError as exc:
            assert 'Server is busy' in str(exc)
        release.set()
        assert await first == 'unchanged'
    asyncio.run(run())
    metrics.rejected.assert_called_once_with()
    assert metrics.busy.call_args_list == [((True,),), ((False,),)]


def test_sampler_runs_while_worker_busy():
    import time
    c = Mock(); m = ApplicationMetrics(c, ['lookup'], interval=.01)
    m.busy(True)
    time.sleep(.035)
    assert any(call.args[1] == 1 for call in c.metrics.gauge.call_args_list)
    m.shutdown()
