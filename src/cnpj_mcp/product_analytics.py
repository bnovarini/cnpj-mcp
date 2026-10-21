"""Aggregate-only tool_called events. No identity/session tracking."""
import re

FIELDS = frozenset({"tool", "status", "duration_bucket", "result_bucket", "argument_names", "client_name", "client_version"})
DURATION_BUCKETS = ("<100ms", "100-499ms", "500-999ms", "1-5s", "5-10s", "10s+")
RESULT_BUCKETS = frozenset({"0", "1-10", "10-100", "100+", "unknown"})
STATUSES = frozenset({"ok", "tool_error", "rpc_error", "http_4xx", "http_5xx", "exception", "aborted"})


def duration_bucket(ms):
    return DURATION_BUCKETS[sum(ms >= edge for edge in (100, 500, 1000, 5000, 10000))]


def result_bucket(value):
    if isinstance(value, dict) and isinstance(value.get("result"), list):
        value = value["result"]
    if isinstance(value, list):
        n = len(value)
        # Pagination headers/sentinels are metadata, not returned data rows.
        n -= sum(isinstance(row, dict) and ("total_matching" in row or row.get("truncated") is True) for row in value)
    elif isinstance(value, dict):
        arrays = [value[k] for k in ("companies", "partners") if isinstance(value.get(k), list)]
        n = len(arrays[0]) if arrays else (0 if not value or "error" in value else 1)
    else:
        return "unknown"
    return "0" if n == 0 else "1-10" if n <= 10 else "10-100" if n <= 100 else "100+"


def response_buckets(body):
    """Inspect only structured MCP data to count rows, never emit response text."""
    import json
    try:
        messages = json.loads(body)
        messages = messages if isinstance(messages, list) else [messages]
        buckets = []
        for message in messages:
            result = message.get("result", {})
            if result.get("isError") or "error" in message:
                buckets.append("unknown")
                continue
            data = result.get("structuredContent")
            if data is None:
                # FastMCP serializes lists into one text JSON block per row.
                contents = result.get("content", [])
                decoded = [json.loads(c["text"]) for c in contents if c.get("type") == "text"]
                data = decoded[0] if len(decoded) == 1 else decoded
            buckets.append(result_bucket(data))
        return buckets
    except Exception:
        return []


def scrub(event, schemas, tools):
    props = event.get("properties", {})
    tool = props.get("tool")
    safe = {"tool": tool if tool in tools else "(unknown)",
            "status": props.get("status") if props.get("status") in STATUSES else "exception",
            "duration_bucket": props.get("duration_bucket") if props.get("duration_bucket") in DURATION_BUCKETS else "10s+",
            "result_bucket": props.get("result_bucket") if props.get("result_bucket") in RESULT_BUCKETS else "unknown"}
    names = props.get("argument_names", [])
    safe["argument_names"] = sorted(set(n for n in names if isinstance(n, str)) & set(schemas.get(tool, set()))) if isinstance(names, list) else []
    for key in ("client_name", "client_version"):
        value = props.get(key)
        if value is not None:
            safe[key] = re.sub(r"[^A-Za-z0-9 ._\-/+]", "", str(value))[:64]
    safe.update({"$process_person_profile": False, "$geoip_disable": True})
    return {"event": "tool_called", "timestamp": event.get("timestamp"),
            "distinct_id": event.get("distinct_id"), "properties": safe}
