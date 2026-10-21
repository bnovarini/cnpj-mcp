"""Opt-in, bounded operational aggregates. No MCP values or identity accepted."""
import contextvars
import math
import threading

from .product_analytics import STATUSES

EDGES = (100, 500, 1000, 5000, 10000)


class ApplicationMetrics:
    def __init__(self, client, tools, interval=60):
        self.client, self.tools = client, frozenset(tools)
        self.interval = interval
        self._busy = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _emit(self, kind, name, value, attributes=None, unit=None):
        try:
            # Drop ambient OTel/PostHog trace context, including exemplars.
            contextvars.Context().run(getattr(self.client.metrics, kind), name, value,
                                      attributes=attributes, unit=unit)
        except Exception:
            pass

    def record(self, tool, status, duration):
        if tool not in self.tools or status not in STATUSES:
            return  # Unknown input cannot expand the series set.
        if not isinstance(duration, (int, float)) or isinstance(duration, bool) or not math.isfinite(duration) or duration < 0:
            return
        # Quantize BEFORE SDK aggregation, so even min/max/sum contain buckets,
        # never individual exact request durations. Overflow uses fixed 10001.
        bucket = next((edge for edge in EDGES if duration <= edge), 10001)
        self._emit('count', 'cnpj.tool.calls', 1, {'tool': tool, 'status': status})
        self._emit('histogram', 'cnpj.tool.duration', bucket, {'tool': tool}, 'ms')

    def busy(self, value):
        with self._lock:
            self._busy = int(bool(value))

    def rejected(self):
        self._emit('count', 'cnpj.worker.rejected', 1)

    def sample(self):
        with self._lock:
            busy = self._busy
        self._emit('gauge', 'cnpj.worker.busy', busy)

    def _sample_loop(self):
        while not self._stop.wait(self.interval):
            self.sample()

    def shutdown(self):
        self._stop.set()
        self._thread.join(timeout=1)
        self.sample()
        try:
            self.client.metrics.flush()
            self.client.metrics.reset()
        except Exception:
            pass
