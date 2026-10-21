"""Measured tool context only. No arguments, messages or results retained."""
import contextvars
import re
import time

calls = contextvars.ContextVar("cnpj_tool_measurements", default=None)
current = contextvars.ContextVar("cnpj_current_tool", default=None)
ERROR_CLASSES = {"ValueError": "validation", "TypeError": "validation", "ValidationError": "validation",
                 "TimeoutError": "timeout", "InterruptException": "timeout",
                 "ConnectionException": "db_error", "IOException": "db_error",
                 "OutOfMemoryException": "db_error", "BinderException": "db_error",
                 "CatalogException": "db_error", "ParserException": "db_error"}


def error_class(exc):
    # Class identities only, no exception str/repr, not even for classification.
    names = {base.__name__ for base in type(exc).__mro__}
    for name, code in ERROR_CLASSES.items():
        if name in names:
            return code
    if any(base.__module__.startswith("duckdb") for base in type(exc).__mro__):
        return "db_error"
    return "internal"


def bucket(value):
    if isinstance(value, list):
        n = len(value)
    elif isinstance(value, dict):
        arrays = [value[k] for k in ("companies", "partners") if isinstance(value.get(k), list)]
        n = len(arrays[0]) if arrays else (0 if not value or "error" in value else 1)
    else:
        return "unknown"
    return "0" if n == 0 else "1-10" if n <= 10 else "10-100" if n <= 100 else "100+"


def version(value):
    # Only build/version identifiers, not arbitrary environment strings.
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", value) else "unknown"


def instrument(tool):
    fn = tool.fn
    async def measured(*args, **kwargs):
        measurements = calls.get()
        if measurements is None:
            return await fn(*args, **kwargs)
        data = {"tool": tool.name, "error_class": "none", "result_bucket": "unknown", "db_ms": 0.0, "db_calls": 0}
        measurements.append(data)
        token = current.set(data)
        started = time.monotonic()
        try:
            result = await fn(*args, **kwargs)
            data["result_bucket"] = bucket(result)
            if isinstance(result, dict) and "error" in result:
                data["error_class"] = "tool_error"
            return result
        except BaseException as exc:
            if data["error_class"] == "none":
                data["error_class"] = error_class(exc)
            raise
        finally:
            data["tool_ms"] = max(0.0, (time.monotonic() - started) * 1000)
            current.reset(token)
    import functools
    tool.fn = functools.wraps(fn)(measured)

ARGUMENT_NAMES = frozenset(['capital_max', 'capital_min', 'cep', 'closed_from', 'closed_to', 'cnae', 'cnae_secao', 'cnpj', 'contains', 'cpf_middle', 'group_by', 'include_contacts', 'include_secondary', 'kind', 'limit', 'logradouro_contains', 'matriz_only', 'mei', 'metric', 'min_contact_score', 'municipio', 'municipio_codigo', 'name', 'name_contains', 'natureza_juridica', 'numero', 'offset', 'opened_from', 'opened_to', 'order', 'order_by', 'partner_name', 'porte', 'simples', 'situacao', 'text', 'uf', 'unit'])
