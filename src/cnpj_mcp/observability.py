"""Opt-in PostHog export. Never accept request bodies, headers or raw errors here."""
from __future__ import annotations

import builtins
import contextvars
import os
import re

FIELDS = frozenset({"tool", "status", "duration_ms", "client_name", "client_version"})
STATUSES = frozenset({"ok", "tool_error", "rpc_error", "http_4xx", "http_5xx", "exception", "aborted"})
ERROR_TYPES = frozenset(k for k, v in vars(builtins).items()
                        if isinstance(v, type) and issubclass(v, BaseException)) | {"ToolError"}
MESSAGE = "MCP operation failed; details withheld"


def _metadata(values):
    result = {k: v for k, v in values.items() if k in FIELDS and v is not None}
    if result.get("status") not in STATUSES:
        result["status"] = "exception"
    for key in ("client_name", "client_version"):
        if key in result:
            result[key] = re.sub(r"[^A-Za-z0-9 ._\-/+]", "", str(result[key]))[:64]
    return result


def scrub_span(span):
    """Final SDK hook: no events, messages, trace state or context attributes."""
    span["attributes"] = _metadata(span.get("attributes", {}))
    span["name"] = span["attributes"].get("tool", "(unknown)")
    span["events"] = []
    span["status"] = {"code": "ok" if span["attributes"]["status"] == "ok" else "error"}
    return span


def scrub_event(event):
    """Rebuild allowlist; only SDK-generated anonymous event identity is retained."""
    if event.get("event") != "$exception":
        return None
    props = event.get("properties", {})
    errors = props.get("$exception_list") or [{}]
    typ = errors[0].get("type", "Exception")
    safe = _metadata(props)
    safe.update({"$exception_list": [{"type": typ if typ in ERROR_TYPES else "Exception", "value": MESSAGE}],
                 "$geoip_disable": True, "$process_person_profile": False})
    return {"event": "$exception", "timestamp": event.get("timestamp"),
            "distinct_id": event.get("distinct_id"), "properties": safe}


class Observability:
    def __init__(self, token, tools, host="https://us.i.posthog.com"):
        from posthog import Posthog
        self.tools = frozenset(tools)
        self.client = Posthog(
            token, host=host, disable_geoip=True, enable_exception_autocapture=False,
            capture_exception_code_variables=False, enable_local_evaluation=False,
            before_send=scrub_event, max_queue_size=256, flush_interval=2,
            timeout=2, max_retries=0,
            traces={"service_name": "cnpj-mcp", "before_span_send": scrub_span,
                    "resource_attributes": {"os.name": None, "os.version": None},
                    "max_queue_size": 256, "max_export_batch_size": 64, "flush_interval": 2},
        )

    def record(self, rows, started, duration, status, error_type=None):
        # Never inherit a caller's PostHog person/session or active trace context.
        contextvars.Context().run(self._record, rows, started, duration, status, error_type)

    def _record(self, rows, started, duration, status, error_type):
        for row in rows:
            if row[1] != "tool_call":
                continue
            tool = row[2] if row[2] in self.tools else "(unknown)"
            values = _metadata(dict(tool=tool, status=status, duration_ms=duration,
                                    client_name=row[3], client_version=row[4]))
            span = self.client.start_span(tool, start_time=started)
            span.set_attributes(values)
            span.set_status("ok" if status == "ok" else "error")
            span.end()
            if status != "ok":
                typ = error_type if error_type in ERROR_TYPES else "ToolError"
                # Deliberately no real exception object, traceback, locals or message.
                self.client.capture("$exception", properties={**values,
                    "$exception_list": [{"type": typ, "value": MESSAGE}]})

    def shutdown(self):
        self.client.shutdown()


def from_env(tools):
    token = os.environ.get("CNPJ_POSTHOG_TOKEN")
    if not token:
        return None
    host = os.environ.get("CNPJ_POSTHOG_HOST", "https://us.i.posthog.com")
    if host not in {"https://us.i.posthog.com", "https://eu.i.posthog.com"}:
        return None
    try:
        return Observability(token, tools, host)
    except Exception:
        return None  # Telemetry must never prevent serving queries.
