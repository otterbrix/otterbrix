"""The type a Python constant gets, observed through a comparison with a column of that exact type.

A constant is compared with a column only when both have the same type (the same rule that makes
`test_dict_constant_to_map.py` work), so each case builds a column of the type the constant is
expected to have and filters it with `column == ConstantExpression(value)`.

* A Decimal constant keeps its own DECIMAL(width, scale) type: `Decimal('1.5')` is DECIMAL(2,1) and
  matches a DECIMAL(2,1) column. Before, the constant was bound as the bare unscaled integer 15 and
  the filter failed with "out of range".
* The element type of a list / dict constant is the engine's common type of its elements (the one
  CASE / COALESCE use): [1, 2.5] is DOUBLE[], [[1], [2.5]] is DOUBLE[][], NULL takes the common type
  of the other elements, and elements with no common type are refused when the constant is built.

Each case runs in a child interpreter so a failure mode that aborts cannot take the other tests down.
"""

import subprocess
import sys

import pytest

_BODY = """
import sys
from decimal import Decimal

import pyarrow as pa
import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

conn = otterbrix.connect(sys.argv[1])
@@SETUP@@
rows = relation.filter(ColumnExpression("v", conn) == constant).fetchall()
print("IDS:%r" % ([row[0] for row in rows],))
"""


def _run(setup, database):
    return subprocess.run(
        [sys.executable, "-c", _BODY.replace("@@SETUP@@", setup), str(database)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


def _case(column_type, column_values, constant):
    return (
        "relation = conn.from_object(pa.table({'id': [1, 2], 'v': pa.array(%s, type=%s)}))\n"
        "constant = ConstantExpression(%s, conn)\n" % (column_values, column_type, constant)
    )


@pytest.mark.parametrize(
    "setup, wanted",
    [
        pytest.param(
            _case("pa.decimal128(2, 1)", "[Decimal('1.5'), Decimal('2.5')]", "Decimal('1.5')"),
            "IDS:[1]",
            id="decimal",
        ),
        pytest.param(
            _case("pa.list_(pa.float64())", "[[1.0, 2.5], [3.0]]", "[1, 2.5]"),
            "IDS:[1]",
            id="int-and-float-elements-are-double",
        ),
        pytest.param(
            _case("pa.list_(pa.decimal128(3, 2))", "[[Decimal('1.50'), Decimal('2.25')], []]", "[Decimal('1.5'), Decimal('2.25')]"),
            "IDS:[1]",
            id="decimal-elements-widen-to-one-decimal",
        ),
        pytest.param(
            _case("pa.list_(pa.list_(pa.float64()))", "[[[1.0], [2.5]], [[3.0]]]", "[[1], [2.5]]"),
            "IDS:[1]",
            id="nested-lists-are-double",
        ),
        pytest.param(
            _case("pa.list_(pa.int32())", "[[None, 1], [3]]", "[None, 1]"),
            "IDS:[1]",
            id="null-takes-the-common-type",
        ),
    ],
)
def test_a_constant_has_the_type_of_the_column_it_matches(tmp_path, setup, wanted):
    proc = _run(setup, tmp_path / "db")
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert wanted in proc.stdout, proc.stdout[-600:]


def test_elements_with_no_common_type_are_refused_when_the_constant_is_built(tmp_path):
    proc = _run(_case("pa.list_(pa.int32())", "[[1], [3]]", "[1, 'a']"), tmp_path / "db")
    assert proc.returncode != 0
    assert "Cannot infer common type for elements: integer, text" in proc.stderr, proc.stderr[-600:]


def test_a_bool_and_an_integer_have_no_common_type(tmp_path):
    proc = _run(_case("pa.list_(pa.int32())", "[[1], [3]]", "[True, 1]"), tmp_path / "db")
    assert proc.returncode != 0
    assert "Cannot infer common type for elements: boolean, integer" in proc.stderr, proc.stderr[-600:]


@pytest.mark.parametrize(
    "constant, message",
    [
        pytest.param("[1, 'a']", "elements: integer, text", id="scalars"),
        pytest.param("[1, 'a', 2]", "elements: integer, text", id="repeated-type-listed-once"),
        pytest.param("[None, 'a', 1]", "elements: text, integer", id="null-is-not-part-of-the-conflict"),
        pytest.param("[[1], 1]", "elements: integer[], integer", id="list-and-scalar"),
        pytest.param("[{'a': 1}, 1]", "elements: record(a integer), integer", id="unnamed-struct-and-scalar"),
        pytest.param("[{'a': 1}, {'b': 'x'}]", "elements: record(a integer), record(b text)", id="two-structs"),
        pytest.param("[{'key': ['k'], 'value': [1]}, 1]", "elements: map(text, integer), integer", id="map-and-scalar"),
    ],
)
def test_the_refusal_names_every_type_that_has_no_common_type(tmp_path, constant, message):
    proc = _run(_case("pa.list_(pa.int32())", "[[1], [3]]", constant), tmp_path / "db")
    assert proc.returncode != 0
    assert "Cannot infer common type for " + message in proc.stderr, proc.stderr[-600:]
