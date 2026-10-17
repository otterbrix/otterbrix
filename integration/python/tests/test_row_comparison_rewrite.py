"""`ROW(..) op ROW(..)` is rewritten pairwise by the transformer; later passes then rewrite the result.

The ordering operators use each field in several comparisons. Constant folding and view expansion
rewrite expression nodes in place, so a field shared between two comparisons was rewritten twice:
`ROW(1+1, a) < ROW(2, c)` failed with "operand count does not match the operator", and a view
over `ROW(a + 1, b) <= ROW(2, 7)` returned nothing. Each case runs in a child interpreter.
"""

import json
import subprocess
import sys

import pytest

_BODY = """
import json
import sys

import otterbrix

conn = otterbrix.connect(sys.argv[1])
statements = json.loads(sys.argv[2])
for statement in statements[:-1]:
    conn.execute(statement)
print("ANSWER:%r" % (conn.execute(statements[-1]).fetchall(),))
"""

_TABLE = [
    "CREATE DATABASE d;",
    "CREATE TABLE d.t (id INTEGER, a INTEGER, b INTEGER, c INTEGER);",
    "INSERT INTO d.t (id, a, b, c) VALUES (1, 1, 1, 2), (2, 1, NULL, 2), (3, 2, 5, 1), (4, NULL, 1, 1);",
]


def _answer(tmp_path, statements):
    proc = subprocess.run(
        [sys.executable, "-c", _BODY, str(tmp_path / "db"), json.dumps(statements)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    line = next(line for line in proc.stdout.splitlines() if line.startswith("ANSWER:"))
    return line[len("ANSWER:") :]


@pytest.mark.parametrize(
    "query, answer",
    [
        ("SELECT id FROM d.t WHERE ROW(1+1, a) < ROW(2, c) ORDER BY id;", "[(1,), (2,)]"),
        ("SELECT id FROM d.t WHERE ROW(a + 1, b) < ROW(c, 7) ORDER BY id;", "[(1,)]"),
        ("SELECT id FROM d.t WHERE ROW(1+1, a, b) <= ROW(2, 1, 1) ORDER BY id;", "[(1,)]"),
    ],
)
def test_a_shared_field_is_not_rewritten_twice(tmp_path, query, answer):
    assert _answer(tmp_path, _TABLE + [query]) == answer


def test_a_view_over_a_row_comparison_expands_every_field_once(tmp_path):
    statements = _TABLE + [
        "CREATE VIEW d.v AS SELECT id, a, b, c FROM d.t WHERE ROW(a + 1, b) <= ROW(2, 7);",
        "SELECT id FROM d.v WHERE id < 100 ORDER BY id;",
    ]
    assert _answer(tmp_path, statements) == "[(1,)]"


@pytest.mark.parametrize(
    "query, answer",
    [
        ("SELECT NULL = NULL;", "[(None,)]"),
        ("SELECT NULL < NULL;", "[(None,)]"),
        ("SELECT NULL = NULL OR 1 = 1;", "[(True,)]"),
    ],
)
def test_two_untyped_nulls_compare_to_null(tmp_path, query, answer):
    assert _answer(tmp_path, [query]) == answer


_NAME_BODY = """
import sys

import otterbrix

print("NAMES:%r" % (list(otterbrix.connect(sys.argv[1]).execute(sys.argv[2]).df().columns),))
"""


@pytest.mark.parametrize(
    "query, names",
    [
        ("SELECT ROW(1,2) < ROW(1,3);", ["<"]),
        ("SELECT ROW(1,2) = ROW(1,2);", ["="]),
        ("SELECT ROW(1,2) >= ROW(1,3);", [">="]),
        ("SELECT ROW(1,2) < ROW(1,3) AS x;", ["x"]),
    ],
)
def test_a_rewritten_comparison_is_named_like_a_plain_one(tmp_path, query, names):
    proc = subprocess.run(
        [sys.executable, "-c", _NAME_BODY, str(tmp_path / "db"), query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "NAMES:%r" % (names,) in proc.stdout, proc.stdout[-600:]
