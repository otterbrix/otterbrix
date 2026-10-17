"""A struct column compared with a dict constant (#729: the engine read every top-level STRUCT as an INTERVAL).

`column == ConstantExpression({'x': 2})` compares two rows field by field. Before the fix the comparison
read the row as an interval: a wrong answer (`[]` instead of `[2]`) or a SIGSEGV when another constant
had been created first or when the query ran twice. Each case runs in a child interpreter so a crash
cannot take the other tests down.
"""

import subprocess
import sys

import pytest

_BODY = """
import sys

import pyarrow as pa
import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

conn = otterbrix.connect(sys.argv[1])
ints = pa.array([{"x": 1}, {"x": 2}], type=pa.struct([("x", pa.int32())]))
texts = pa.array([{"x": "a"}, {"x": "b"}], type=pa.struct([("x", pa.string())]))


def ids(column, constant):
    relation = conn.from_object(pa.table({"id": [1, 2], "v": column}))
    rows = relation.filter(ColumnExpression("v", conn) == ConstantExpression(constant, conn)).fetchall()
    return [row[0] for row in rows]


@@SCRIPT@@
"""


def _run(script, database):
    return subprocess.run(
        [sys.executable, "-c", _BODY.replace("@@SCRIPT@@", script), str(database)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


@pytest.mark.parametrize(
    "script, wanted",
    [
        pytest.param('print("IDS:%r" % ids(ints, {"x": 2}))', "IDS:[2]", id="plain"),
        pytest.param(
            'print("IDS:%r" % ids(ints, {"x": 2}))\nprint("IDS:%r" % ids(ints, {"x": 1}))',
            "IDS:[2]\nIDS:[1]",
            id="twice",
        ),
        pytest.param(
            'ConstantExpression(5, conn)\nprint("IDS:%r" % ids(ints, {"x": 2}))', "IDS:[2]", id="after-another-constant"
        ),
        pytest.param(
            'ConstantExpression({"x": "b"}, conn)\nprint("IDS:%r" % ids(ints, {"x": 2}))',
            "IDS:[2]",
            id="after-a-struct-of-another-type",
        ),
        pytest.param(
            'print("IDS:%r" % ids(ints, {"x": 2}))\nprint("IDS:%r" % ids(texts, {"x": "b"}))',
            "IDS:[2]\nIDS:[2]",
            id="two-struct-types",
        ),
    ],
)
def test_a_dict_constant_matches_the_struct_it_describes(tmp_path, script, wanted):
    proc = _run(script, tmp_path / "db")
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert wanted in proc.stdout, proc.stdout[-600:]


_SQL_BODY = """
import sys

import otterbrix

print("ANSWER:%r" % (otterbrix.connect(sys.argv[1]).execute(sys.argv[2]).fetchall(),))
"""


@pytest.mark.parametrize(
    "query, answer",
    [
        ("SELECT ROW(1) = ROW(1);", True),
        ("SELECT ROW(1) = ROW(2);", False),
        ("SELECT ROW(1,NULL) = ROW(1,NULL);", None),
        ("SELECT ROW(1,'a') = ROW(1,'a');", True),
        ("SELECT ROW(1,'a') < ROW(2,'a');", True),
        ("SELECT ROW(1,2) = ROW(1,2);", True),
        ("SELECT ROW(1,2) = ROW(1,3);", False),
        ("SELECT ROW(2,0) > ROW(1,9);", True),
    ],
)
def test_row_constructors_compare_field_by_field(tmp_path, query, answer):
    proc = subprocess.run(
        [sys.executable, "-c", _SQL_BODY, str(tmp_path / "db"), query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "ANSWER:[(%r,)]" % (answer,) in proc.stdout, proc.stdout[-600:]


# PostgreSQL 18 (parse_expr.c make_row_comparison_op): `ROW(..) op ROW(..)` is rewritten field by field,
# so a NULL field makes the answer UNKNOWN unless an earlier pair already decided it.
@pytest.mark.parametrize(
    "query, answer",
    [
        ("SELECT ROW(1,NULL) = ROW(1,2);", None),
        ("SELECT ROW(1,NULL) = ROW(2,2);", False),
        ("SELECT ROW(1,NULL) <> ROW(1,2);", None),
        ("SELECT ROW(1,NULL) <> ROW(2,2);", True),
        ("SELECT ROW(1,NULL) < ROW(2,NULL);", True),
        ("SELECT ROW(NULL,1) < ROW(1,2);", None),
        ("SELECT ROW(1,2,NULL) < ROW(1,3,0);", True),
        ("SELECT ROW(1,2) <= ROW(1,2);", True),
        ("SELECT ROW(1,2) < ROW(1,2);", False),
        ("SELECT ROW(2,0) >= ROW(1,9);", True),
        ("SELECT (1,2) = (1,2);", True),
    ],
)
def test_row_constructor_comparison_is_rewritten_field_by_field(tmp_path, query, answer):
    proc = subprocess.run(
        [sys.executable, "-c", _SQL_BODY, str(tmp_path / "db"), query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "ANSWER:[(%r,)]" % (answer,) in proc.stdout, proc.stdout[-600:]


@pytest.mark.parametrize(
    "query, message",
    [
        ("SELECT ROW(1,2) = ROW(1);", "unequal number of entries in row expressions"),
        ("SELECT ROW(1,2) < ROW(1,2,3);", "unequal number of entries in row expressions"),
        ("SELECT ROW() = ROW();", "cannot compare rows of zero length"),
    ],
)
def test_row_constructors_of_different_length_are_refused(tmp_path, query, message):
    proc = subprocess.run(
        [sys.executable, "-c", _SQL_BODY, str(tmp_path / "db"), query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode != 0
    assert message in proc.stderr, proc.stderr[-600:]


_RECORD_BODY = """
import sys

import otterbrix

conn = otterbrix.connect(sys.argv[1])
for statement in (
    "CREATE DATABASE d;",
    "CREATE TYPE pair_t AS (x bigint, y bigint);",
    "CREATE TABLE d.t (id INTEGER, p pair_t, p2 pair_t);",
    "INSERT INTO d.t (id, p, p2) VALUES (1, ROW(1,NULL), ROW(1,NULL)), (2, ROW(1,2), ROW(1,NULL));",
):
    conn.execute(statement)
print("COMPARED:%r" % (conn.execute("SELECT p = p2, p < p2 FROM d.t ORDER BY id;").fetchall(),))
print("FILTERED:%r" % (conn.execute("SELECT id FROM d.t WHERE p = p2;").fetchall(),))
"""


def test_a_stored_record_compares_as_a_record(tmp_path):
    # Columns are not row constructors: two NULL fields are equal, a NULL field sorts after every value.
    proc = subprocess.run(
        [sys.executable, "-c", _RECORD_BODY, str(tmp_path / "db")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "COMPARED:[(True, False), (False, True)]" in proc.stdout, proc.stdout[-600:]
    assert "FILTERED:[(1,)]" in proc.stdout, proc.stdout[-600:]


@pytest.mark.xfail(
    strict=True,
    reason="the validator finds no common type for STRUCT<integer, NULL> against STRUCT<bigint, bigint> "
    "(nor for STRUCT<integer, integer> against it); the comparison itself has record semantics",
)
def test_a_dict_constant_with_a_null_field_matches_the_record_it_describes(tmp_path):
    script = (
        'ints = pa.array([{"x": 1, "y": None}, {"x": 1, "y": 2}], type=pa.struct([("x", pa.int64()), ("y", pa.int64())]))\\n'
        'print("IDS:%r" % ids(ints, {"x": 1, "y": None}))'
    ).replace("\\n", "\n")
    proc = _run(script, tmp_path / "db")
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "IDS:[1]" in proc.stdout, proc.stdout[-600:]
