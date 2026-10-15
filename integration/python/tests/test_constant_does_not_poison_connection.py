"""A constant that is merely CREATED must not break unrelated queries on its connection.

`expression_factory_t::values` lives for the whole connection and is never cleared, and
`expression_factory_t::get_params` walks ALL of it on every relation query. It has no branch for
LIST / ARRAY / MAP / STRUCT (nor any compound value) and none for NA (a scalar NULL), so it throws
"Couldn't convert logical value to document value". Hence once e.g. `ConstantExpression({"k": 1}, conn)`,
`ConstantExpression([1, 2], conn)` or `ConstantExpression(None, conn)` exists on a connection, EVERY
later relation query on it fails -- even one that never mentions the constant, such as
`relation.filter(ColumnExpression("id") == ConstantExpression(1, conn))`.

Each case runs in a child interpreter so a failure mode that aborts instead of raising cannot take
the other tests down. The control and the convertible-constant guards (Decimal, float, 2**70, UUID)
pass today; the unconvertible-constant cases are red until every constant the module can build
converts to a parameter.
"""

import subprocess
import sys

import pytest

_BODY = """
import sys
import uuid
from decimal import Decimal

import pyarrow as pa
import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

conn = otterbrix.connect(sys.argv[1])
relation = conn.from_object(pa.table({"id": [1, 2, 3], "v": [10, 20, 30]}))
@@SETUP@@
rows = relation.filter(ColumnExpression("id", conn) == ConstantExpression(1, conn)).fetchall()
print("FILTER:%r" % (rows,))
"""

_WANTED = "FILTER:[(1, 10)]"


def _run(setup, database):
    return subprocess.run(
        [sys.executable, "-c", _BODY.replace("@@SETUP@@", setup), str(database)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


def _assert_filter_works(proc):
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert _WANTED in proc.stdout, proc.stdout[-600:]


def test_a_filter_on_a_fresh_connection_returns_its_row(tmp_path):
    _assert_filter_works(_run("", tmp_path / "db"))


@pytest.mark.parametrize(
    "setup",
    [
        pytest.param("ConstantExpression({'k': 1}, conn)", id="dict"),
        pytest.param("ConstantExpression([1, 2], conn)", id="list"),
        pytest.param("ConstantExpression((1, 2), conn)", id="tuple"),
        pytest.param("ConstantExpression({}, conn)", id="empty-dict"),
        pytest.param("ConstantExpression({'key': ['k'], 'value': [1]}, conn)", id="map-shaped-dict"),
        pytest.param("ConstantExpression(None, conn)", id="none"),
    ],
)
def test_an_unused_unconvertible_constant_does_not_break_a_later_filter(tmp_path, setup):
    _assert_filter_works(_run(setup, tmp_path / "db"))


@pytest.mark.parametrize(
    "setup",
    [
        pytest.param("ConstantExpression(Decimal('1.5'), conn)", id="decimal"),
        pytest.param("ConstantExpression(1.5, conn)", id="float"),
        pytest.param("ConstantExpression(2**70, conn)", id="int-beyond-64-bits"),
        pytest.param("ConstantExpression(uuid.uuid4(), conn)", id="uuid"),
    ],
)
def test_an_unused_convertible_constant_does_not_break_a_later_filter(tmp_path, setup):
    _assert_filter_works(_run(setup, tmp_path / "db"))
