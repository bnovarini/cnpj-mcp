"""Opt-in PostHog export. Never accept request bodies, headers or raw errors here."""
from __future__ import annotations

import builtins
import contextvars
import os
import re
from datetime import datetime, timezone
from .telemetry_context import version, ARGUMENT_NAMES

FIELDS = frozenset({"tool", "status", "duration_ms", "client_name", "client_version", "error_class", "upstream", "upstream_ms", "db_ms", "local_ms", "argument_names", "result_bucket", "cache", "app_version", "deploy_version", "utc_hour"})
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
    choices = {"error_class": {"none", "validation", "timeout", "db_error", "internal", "tool_error", "rpc_error", "rate_limit", "http_error", "aborted"},
               "upstream": {"none", "DB"}, "result_bucket": {"0", "1-10", "10-100", "100+", "unknown"}, "cache": {"not_applicable"}}
    for key, allowed in choices.items():
        if key in result and result[key] not in allowed:
            result.pop(key)
    for key in ("app_version", "deploy_version"):
        if key in result:
            result[key] = version(result[key])
    if "argument_names" in result:
        supplied = result["argument_names"]
        result["argument_names"] = ",".join(sorted(set(supplied.split(",")) & ARGUMENT_NAMES)) if isinstance(supplied, str) else ""
    for key in ("upstream_ms", "db_ms", "local_ms", "duration_ms"):
        if key in result and (not isinstance(result[key], (int, float)) or isinstance(result[key], bool) or not 0 <= result[key] <= 86400000):
            result.pop(key)
    if "utc_hour" in result and (not isinstance(result["utc_hour"], int) or not 0 <= result["utc_hour"] < 24):
        result.pop("utc_hour")
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
    def __init__(self, token, tools, host="https://us.i.posthog.com", logs=False, analytics=False, metrics=False, mcp_analytics=False):
        from posthog import Posthog
        self.tools = frozenset(tools)
        self.schemas = tools if isinstance(tools, dict) else {}
        self.analytics = analytics
        self.mcp_analytics = mcp_analytics
        self.app_version = "0.1.0"
        self.deploy_version = version(os.environ.get("CNPJ_DEPLOY_VERSION", "unknown"))
        self.logs = None
        if logs:
            try:
                from .telemetry_logs import ToolLogs
                self.logs = ToolLogs(token, self.tools, host)
            except Exception:
                pass  # A log exporter failure must not disable existing telemetry.
        self.metrics = None
        self.client = Posthog(
            token, host=host, disable_geoip=True, enable_exception_autocapture=False,
            capture_exception_code_variables=False, enable_local_evaluation=False,
            before_send=self._before_send, max_queue_size=256, flush_interval=2,
            timeout=2, max_retries=0,
            metrics={"service_name": "cnpj-mcp", "flush_interval": 60, "max_series_per_flush": 66} if metrics else None,
            traces={"service_name": "cnpj-mcp", "before_span_send": scrub_span,
                    "resource_attributes": {"os.name": None, "os.version": None},
                    "max_queue_size": 256, "max_export_batch_size": 64, "flush_interval": 2},
        )

        if metrics:
            from .application_metrics import ApplicationMetrics
            self.metrics = ApplicationMetrics(self.client, self.tools)

    def _before_send(self, event):
        if self.mcp_analytics and event.get("event") == "$mcp_tool_call":
            from .mcp_analytics import scrub
            return scrub(event, self.tools)
        if self.analytics and not self.mcp_analytics and event.get("event") == "tool_called":
            from .product_analytics import scrub
            return scrub(event, self.schemas, self.tools)
        return scrub_event(event)

    def record(self, rows, started, duration, status, error_type=None, measurements=None, shapes=None):
        # Never inherit a caller's PostHog person/session or active trace context.
        contextvars.Context().run(self._record, rows, started, duration, status, error_type, measurements or [], shapes or [])

    def _record(self, rows, started, duration, status, error_type, measurements, shapes):
        index = 0
        for row in rows:
            if row[1] != "tool_call":
                continue
            tool = row[2] if row[2] in self.tools else "(unknown)"
            values = _metadata(dict(tool=tool, status=status, duration_ms=duration,
                                    client_name=row[3], client_version=row[4]))
            measured = measurements[index] if index < len(measurements) else {}
            if measured.get("tool") != tool:
                measured = {}
            shape = shapes[index] if index < len(shapes) and shapes[index][0] == tool else (tool, [])
            names = sorted(set(shape[1]) & set(self.schemas.get(tool, set())))
            error_code = measured.get("error_class")
            if not error_code or (error_code == "none" and status != "ok"):
                from .telemetry_context import ERROR_CLASSES
                error_code = ERROR_CLASSES.get(error_type, {"ok": "none", "tool_error": "tool_error", "rpc_error": "rpc_error", "http_4xx": "http_error", "http_5xx": "http_error", "aborted": "aborted"}.get(status, "internal"))
            db_ms = max(0, int(measured.get("db_ms", 0)))
            values.update(error_class=error_code, upstream="DB" if measured.get("db_calls") else "none", upstream_ms=0,
                          db_ms=db_ms, local_ms=max(0, duration-db_ms), argument_names=",".join(names),
                          result_bucket=measured.get("result_bucket", "unknown"), cache="not_applicable",
                          app_version=self.app_version, deploy_version=self.deploy_version,
                          utc_hour=datetime.fromtimestamp(started, timezone.utc).hour)
            if self.metrics:
                self.metrics.record(tool, status, duration)
            if self.mcp_analytics:
                from .mcp_analytics import properties
                safe = properties(tool, status, duration, values.get("error_class"), self.tools)
                if safe is not None:
                    self.client.capture("$mcp_tool_call", properties=safe)
            elif self.analytics:
                from .product_analytics import duration_bucket
                self.client.capture("tool_called", properties={
                    "tool": tool, "status": status, "duration_bucket": duration_bucket(duration),
                    "result_bucket": values.get("result_bucket", "unknown"), "argument_names": names,
                    "client_name": row[3], "client_version": row[4]})
            index += 1
            if self.logs:
                try:
                    self.logs.record(values)
                except Exception:
                    pass
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
        try:
            if self.metrics:
                self.metrics.shutdown()
            self.client.shutdown()
        finally:
            if self.logs:
                self.logs.shutdown()


def from_env(tools):
    token = os.environ.get("CNPJ_POSTHOG_TOKEN")
    if not token:
        return None
    host = os.environ.get("CNPJ_POSTHOG_HOST", "https://us.i.posthog.com")
    if host not in {"https://us.i.posthog.com", "https://eu.i.posthog.com"}:
        return None
    try:
        return Observability(token, tools, host, logs=os.environ.get("CNPJ_POSTHOG_LOGS") == "1", analytics=os.environ.get("CNPJ_POSTHOG_ANALYTICS") == "1", metrics=os.environ.get("CNPJ_POSTHOG_METRICS") == "1", mcp_analytics=os.environ.get("CNPJ_POSTHOG_MCP_ANALYTICS") == "1")
    except Exception:
        return None  # Telemetry must never prevent serving queries.
