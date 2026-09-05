_exported_symbols = []


from .otterbrix import (
    OtterBrixPyConnection,
    OtterBrixPyRelation,
    OtterBrixPyResult,
    Expression,
    ConstantExpression,
    ColumnExpression,
    CountExpression,
)
_exported_symbols.extend([
    "OtterBrixPyConnection",
    "OtterBrixPyRelation",
    "OtterBrixPyResult",
    "Expression",
    "ConstantExpression",
    "ColumnExpression",
    "CountExpression",
])

from .otterbrix import (
    sqltype,
    dtype,
    type,
    array_type,
    list_type,
    union_type,
    string_type,
    enum_type,
    decimal_type,
    struct_type,
    row_type,
    map_type,
)
_exported_symbols.extend([
    "sqltype",
    "dtype",
    "type",
    "array_type",
    "list_type",
    "union_type",
    "string_type",
    "enum_type",
    "decimal_type",
    "struct_type",
    "row_type",
    "map_type",
])

from .otterbrix import connect
_exported_symbols.extend([
    "connect",
])

# The sql-based bindings. main.cpp defines Client / Connection / Cursor / to_aggregate
# unconditionally (no build option gates them), so their absence is a broken extension
# module, not a supported configuration -- import them like every other name above and let
# the ImportError name the missing symbol at import time. The try/except this replaces
# called itself "backwards compatibility" and swallowed exactly that failure: the names
# vanished from the package and the user met an AttributeError somewhere far from the cause.
from .otterbrix import Client, Connection, Cursor, to_aggregate
_exported_symbols.extend([
    "Client",
    "Connection",
    "Cursor",
    "to_aggregate",
])

import otterbrix.typing as typing

_exported_symbols.extend([
    "typing",
])

__all__ = _exported_symbols
