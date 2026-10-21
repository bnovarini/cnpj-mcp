"""Exercise actual SDK wire encoding and ASGI response transparency offline."""
import asyncio
import gzip
import json
import time
from unittest.mock import Mock

import pytest
import requests

from cnpj_mcp.observability import Observability, from_env, scrub_event, scrub_span
from cnpj_mcp.usage import UsageLog

SECRET = "QUERY_VALUE_DO_NOT_EXPORT_9ab7"


@pytest.fixture
def wire(monkeypatch):
    sent = []
    def post(self, url, **kwargs):
        raw = kwargs["data"]
        if kwargs.get("headers", {}).get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        sent.append((url, json.loads(raw)))
        response = requests.Response()
        response.status_code = 200
        response._content = b'{}'
        response.headers["Content-Length"] = "2"
        return response
    monkeypatch.setattr(requests.Session, "post", post)
    # Any accidental GET, e.g. feature flag definitions, must fail the test.
    monkeypatch.setattr(requests.Session, "get", lambda *a, **kw: pytest.fail("Unexpected GET"))
    return sent


def test_actual_sdk_wire_has_no_identity_arguments_or_messages(wire):
    obs = Observability("test_dummy_key", ["count_companies"])
    rows = [("2026-10-21", "tool_call", "count_companies", "TestClient", "1.0", "")]
    obs.record(rows, time.time() - .01, 10, "exception", "ValueError")
    obs.shutdown()
    assert any(url.endswith("/i/v1/traces") for url, body in wire)
    assert any("$exception" in json.dumps(body) for url, body in wire)
    encoded = json.dumps(wire)
    for forbidden in ("session", "$ip", "user_agent", "os.version", "os.name", SECRET, "stacktrace", "variables"):
        assert forbidden not in encoded
    assert "ValueError" in encoded
    assert "distinct_id" in encoded
    assert "details withheld" in encoded
    traces = [b for u, b in wire if u.endswith("/i/v1/traces")]
    for batch in traces:
        for rs in batch["resourceSpans"]:
            assert {x["key"] for x in rs["resource"]["attributes"]} == {"service.name", "telemetry.sdk.name", "telemetry.sdk.version"}
            for scope in rs["scopeSpans"]:
                for span in scope["spans"]:
                    assert span["name"] == "count_companies"
                    assert not span.get("events")
                    assert {x["key"] for x in span["attributes"]} == {"tool", "status", "duration_ms", "client_name", "client_version", "error_class", "upstream", "upstream_ms", "db_ms", "local_ms", "argument_names", "result_bucket", "cache", "app_version", "deploy_version", "utc_hour"}


def test_scrubbers_rebuild_allowlist():
    event = {"event": "$exception", "distinct_id": "sdk-anonymous-test", "properties": {
        "tool": "count_companies", "status": "exception", "arguments": SECRET,
        "$exception_list": [{"type": "ValueError", "value": SECRET, "stacktrace": SECRET}],
        "$session_id": SECRET, "$ip": SECRET, "locals": SECRET}}
    assert SECRET not in json.dumps(scrub_event(event))
    assert scrub_event({"event": "unapproved"}) is None
    span = {"name": SECRET, "attributes": {"tool": "count_companies", "status": "exception", "query": SECRET,
        "posthogDistinctId": SECRET}, "events": [{"message": SECRET}], "status": {"message": SECRET}}
    assert SECRET not in json.dumps(scrub_span(span))


async def invoke(app, body, fail=False):
    messages = []
    async def receive():
        return {"type": "http.request", "body": json.dumps(body).encode(), "more_body": False}
    async def send(message):
        messages.append(message)
    scope = {"type": "http", "method": "POST", "path": "/mcp", "headers": [
        (b"fly-client-ip", b"198.51.100.123"), (b"user-agent", SECRET.encode()),
        (b"x-posthog-distinct-id", SECRET.encode()), (b"traceparent", SECRET.encode())]}
    if fail:
        with pytest.raises(ValueError, match=SECRET):
            await app(scope, receive, send)
    else:
        await app(scope, receive, send)
    return messages


@pytest.mark.parametrize("body_out,code,status", [
    (b'{"result":{"data":"EXACT"}}', 200, "ok"),
    (b'{"result":{"isError":true,"content":[{"text":"' + SECRET.encode() + b'"}]}}', 200, "tool_error"),
    (b'{"error":{"message":"' + SECRET.encode() + b'"}}', 200, "rpc_error"),
    (b'{"error":"' + SECRET.encode() + b'"}', 500, "http_5xx"),
    (b'unchanged', 429, "http_4xx")])
def test_response_bytes_unchanged_and_body_not_exported(wire, body_out, code, status):
    async def app(scope, receive, send):
        request = await receive()
        assert SECRET.encode() in request["body"]
        await send({"type": "http.response.start", "status": code, "headers": []})
        await send({"type": "http.response.body", "body": body_out[:7], "more_body": True})
        await send({"type": "http.response.body", "body": body_out[7:], "more_body": False})
    obs = Observability("test_dummy_key", ["count_companies"])
    request = {"method": "tools/call", "params": {"name": "count_companies", "arguments": {"uf": SECRET}}}
    plain = asyncio.run(invoke(app, request))
    wrapped = asyncio.run(invoke(UsageLog(app, ["count_companies"], obs, record_local=False), request))
    assert wrapped == plain
    obs.shutdown()
    encoded = json.dumps(wire)
    assert SECRET not in encoded
    assert "198.51.100.123" not in encoded
    assert status in encoded


def test_real_exception_message_never_reaches_sdk(wire):
    async def app(scope, receive, send):
        await receive()
        raise ValueError(SECRET)
    obs = Observability("test_dummy_key", ["count_companies"])
    asyncio.run(invoke(UsageLog(app, ["count_companies"], obs, record_local=False),
        {"method": "tools/call", "params": {"name": "count_companies", "arguments": {"q": SECRET}}}, fail=True))
    obs.shutdown()
    assert SECRET not in json.dumps(wire)
    assert "ValueError" in json.dumps(wire)


def test_disabled_without_token(monkeypatch):
    monkeypatch.delenv("CNPJ_POSTHOG_TOKEN", raising=False)
    assert from_env(["count_companies"]) is None


def test_export_failure_does_not_change_response():
    observer = Mock()
    observer.record.side_effect = RuntimeError(SECRET)
    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b'EXACT'})
    request = {"method": "tools/call", "params": {"name": "count_companies"}}
    assert asyncio.run(invoke(app, request)) == asyncio.run(invoke(UsageLog(app, ["count_companies"], observer, False), request))


def test_shutdown_flush_before_ack():
    observer = Mock()
    order = []
    observer.shutdown.side_effect = lambda: order.append("flush")
    async def app(scope, receive, send):
        await send({"type": "lifespan.shutdown.complete"})
    async def send(m):
        order.append("ack")
    asyncio.run(UsageLog(app, [], observer)({"type": "lifespan"}, None, send))
    assert order == ["flush", "ack"]


def test_inherited_person_session_and_trace_are_not_exported(wire):
    from posthog import new_context, identify_context, set_context_session
    obs = Observability("test_dummy_key", ["count_companies"])
    with new_context():
        identify_context(SECRET)
        set_context_session(SECRET)
        with obs.client.start_span("foreign-context"):
            obs.record([("ts", "tool_call", "count_companies", "client", "1", "")], time.time(), 0, "tool_error")
    obs.shutdown()
    encoded = json.dumps(wire)
    assert SECRET not in encoded
    for url, batch in wire:
        if not url.endswith("/i/v1/traces"):
            continue
        for rs in batch["resourceSpans"]:
            for ss in rs["scopeSpans"]:
                for span in ss["spans"]:
                    if span["name"] == "count_companies":
                        assert not span.get("parentSpanId")
