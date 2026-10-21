"""Fixed structured tool records only, never a Python/root logging handler."""
from __future__ import annotations

import contextvars
import time

from .observability import _metadata


class ToolLogs:
    def __init__(self, token, tools, host="https://us.i.posthog.com"):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource
        self.tools = frozenset(tools)
        # Direct Resource construction avoids default host/process/env discovery.
        self.provider = LoggerProvider(resource=Resource({"service.name": "cnpj-mcp"}),
                                       shutdown_on_exit=False)
        exporter = OTLPLogExporter(endpoint=host + "/i/v1/logs",
                                  headers={"Authorization": "Bearer " + token}, timeout=2)
        self.provider.add_log_record_processor(BatchLogRecordProcessor(
            exporter, max_queue_size=256, max_export_batch_size=64,
            schedule_delay_millis=2000, export_timeout_millis=2000))
        self.logger = self.provider.get_logger("cnpj_mcp.tool_events")

    def record(self, values):
        # OTel treats empty Context() as falsy and otherwise falls back to ambient.
        contextvars.Context().run(self._record, values)

    def _record(self, values):
        from opentelemetry._logs import LogRecord, SeverityNumber
        from opentelemetry.context import Context
        values = _metadata(values)
        values["tool"] = values.get("tool") if values.get("tool") in self.tools else "(unknown)"
        success = values["status"] == "ok"
        # Never accept a message argument, LogRecord from logging, exception or extra.
        self.logger.emit(LogRecord(
            timestamp=time.time_ns(), observed_timestamp=time.time_ns(), context=Context(),
            trace_id=0, span_id=0, severity_text="INFO" if success else "ERROR",
            severity_number=SeverityNumber.INFO if success else SeverityNumber.ERROR,
            body="MCP tool request completed" if success else "MCP tool request failed",
            attributes=values))

    def shutdown(self):
        try:
            self.provider.force_flush(timeout_millis=2000)
        finally:
            self.provider.shutdown()
