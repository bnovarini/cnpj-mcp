"""Fixed error taxonomy and issue metadata. Never inspect exception messages."""
class ExpectedValidationError(ValueError):
    pass


class WorkerBusyError(RuntimeError):
    pass


TYPES = {'validation': 'ValidationError', 'timeout': 'QueryTimeout', 'db_error': 'DatabaseError',
         'internal': 'InternalError', 'tool_error': 'ToolFailure', 'rpc_error': 'RPCFailure',
         'rate_limit': 'RateLimitError', 'http_error': 'HTTPFailure', 'aborted': 'AbortedRequest'}
EXPECTED = frozenset({'validation', 'busy', 'not_found'})


def issue(tool, error_class, handled):
    code = error_class if error_class in TYPES else 'internal'
    return {'$exception_fingerprint': 'cnpj:v1:' + tool + ':' + code,
            '$issue_name': tool + ': ' + TYPES[code],
            '$issue_description': 'MCP ' + code + ' failure; details withheld',
            '$exception_list': [{'type': TYPES[code], 'value': 'MCP operation failed; details withheld',
                                 'mechanism': {'type': 'cnpj_manual', 'handled': handled, 'synthetic': True}}]}
