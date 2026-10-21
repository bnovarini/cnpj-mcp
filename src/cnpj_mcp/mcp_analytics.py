"""Canonical MCP event adapter, not automatic posthog.mcp instrumentation."""
import math
from .product_analytics import STATUSES

SOURCE = 'posthog_mcp_analytics'
EDGES = (100, 500, 1000, 5000, 10000)
ERRORS = frozenset({'none', 'validation', 'timeout', 'db_error', 'internal', 'tool_error',
                    'rpc_error', 'rate_limit', 'http_error', 'aborted'})
FIELDS = frozenset({'$mcp_source', '$mcp_interface', '$mcp_tool_name', '$mcp_resource_name',
                    '$mcp_duration_ms', '$mcp_is_error', '$mcp_error_type',
                    '$mcp_server_name', '$mcp_server_version',
                    '$process_person_profile', '$geoip_disable'})


def properties(tool, status, duration, error_class, tools):
    if tool not in tools or status not in STATUSES:
        return None
    if not isinstance(duration, (int, float)) or isinstance(duration, bool) or not math.isfinite(duration) or duration < 0:
        return None
    return {'$mcp_source': SOURCE, '$mcp_interface': 'mcp',
            '$mcp_tool_name': tool, '$mcp_resource_name': tool,
            '$mcp_duration_ms': next((edge for edge in EDGES if duration <= edge), 10001),
            '$mcp_is_error': status != 'ok',
            '$mcp_error_type': (error_class if error_class in ERRORS and error_class != 'none' else 'internal') if status != 'ok' else 'none',
            '$mcp_server_name': 'cnpj-mcp', '$mcp_server_version': '0.1.0',
            '$process_person_profile': False, '$geoip_disable': True}


def scrub(event, tools):
    p = event.get('properties', {})
    status = 'exception' if p.get('$mcp_is_error') is True else 'ok'
    safe = properties(p.get('$mcp_tool_name'), status, p.get('$mcp_duration_ms'),
                      p.get('$mcp_error_type'), tools)
    if safe is None:
        return None
    return {'event': '$mcp_tool_call', 'timestamp': event.get('timestamp'),
            'distinct_id': event.get('distinct_id'), 'properties': safe}
