"""Snapshot of what Python gets back from a result today, per column type.

`fetchone()` / `fetchall()` / `df()` convert engine values to Python values in two
independent places: `py_result_t` (what `otterbrix.connect(...).execute(...)` and a
relation return) and `wrapper_cursor` (what `Client(...).execute(...)` returns). The two do
not agree with each other, and several answers are plainly wrong. This file pins ALL of it as
it is, wrong parts included, so that a change to the value-conversion layer shows up as a diff
in exactly the cells it changes. A line marked "today's behaviour, see #726" is a pinned
defect, not a specification: when it is fixed the expectation is updated, never the other
way round.

Every observation is a `repr`, so `True` vs `1`, `1` vs `1.0` and `Decimal` vs `float` are
distinguished; a DataFrame is pinned by columns, dtypes and the repr of each column's values.
pandas 3 infers a string column as dtype `str` with NaN for NULL; that is normalised back to
`object` / None so the pins do not depend on the pandas major version.

The rule "declared DOUBLE -> float" (util/convert_value.cpp, `cell_to_python`) cannot
be told apart from the native DOUBLE path by repr: the rows below that should reach it (CASE
mixing 1 and 2.5, COALESCE(int, 1.5)) come back as float on BOTH paths, and the cursor path does
not use that function, so no query was found that pins that line alone.

Not pinned because they take the interpreter down (reported separately): a multi-row
INSERT ... VALUES whose rows hold ARRAY['a','b'] for a LIST(VARCHAR) column together with a NULL
row (SIGBUS; the variant with the NULL row first, `(1, NULL), (2, ARRAY[...])`, also crashes, with
SIGSEGV on this build; only the two-row case ARRAY['a','b'] and ARRAY['c'] without a NULL row is
pinned, a single row works but is not pinned), and ingesting any Arrow binary column -- binary,
large_binary, fixed_size_binary(4/16), with or without NULL (SIGSEGV).
"""

import pytest

import otterbrix
from otterbrix import Client


def _one(literal):
    return f"(1, {literal}), (2, NULL)"


# (label, SQL type, VALUES rows for (id, v)).
TABLE_CASES = [
    ("BOOLEAN", "BOOLEAN", _one("true")),
    ("TINYINT", "TINYINT", _one("-5")),
    ("SMALLINT", "SMALLINT", _one("-300")),
    ("INTEGER", "INTEGER", _one("-70000")),
    ("BIGINT", "BIGINT", _one("-5000000000")),
    ("HUGEINT", "HUGEINT", _one("170141183460469231731687303715884105727")),
    ("UTINYINT", "UTINYINT", _one("200")),
    ("USMALLINT", "USMALLINT", _one("60000")),
    ("UINTEGER", "UINTEGER", _one("4000000000")),
    ("UBIGINT", "UBIGINT", _one("18446744073709551615")),
    ("FLOAT", "FLOAT", _one("1.5")),
    ("DOUBLE", "DOUBLE", _one("2.25")),
    ("DECIMAL(10,2)", "DECIMAL(10,2)", _one("12345.67")),
    ("DECIMAL(18,4)", "DECIMAL(18,4)", _one("-3.1415")),
    ("DECIMAL(5,0)", "DECIMAL(5,0)", _one("42")),
    ("DECIMAL(38,10)", "DECIMAL(38,10)", _one("123456.7890123456")),
    ("VARCHAR", "VARCHAR", _one("'hello'")),
    ("DATE", "DATE", _one("'2024-03-15'")),
    ("TIMESTAMP", "TIMESTAMP", _one("'2024-03-15 10:20:30'")),
    ("TIME", "TIME", _one("'10:20:30'")),
    ("INTERVAL", "INTERVAL", _one("'1 day'")),
    ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", _one("'2024-03-15 10:20:30+00'")),
    ("INTEGER[]", "INTEGER[]", _one("ARRAY[1,2,3]")),
    ("INTEGER[] with a NULL element", "INTEGER[]", _one("ARRAY[1,NULL]")),
    ("VARCHAR[] without a NULL row", "VARCHAR[]", "(1, ARRAY['a','b']), (2, ARRAY['c'])"),
]

# Run through BOTH paths, against the tables built by `_setup`.
QUERY_CASES = [
    "SELECT ROW(1,'a');",
    "SELECT ARRAY[1,2];",
    "SELECT 1.5::DECIMAL(10,2);",
    "SELECT DATE '2024-03-15';",
    "SELECT NULL;",
    "SELECT 1, 'x';",
    "SELECT i, i FROM d.q ORDER BY id;",
    "SELECT i AS a, s AS a FROM d.q ORDER BY id;",
    "SELECT i FROM d.q WHERE false;",
    "SELECT i FROM d.n ORDER BY id;",
    "SELECT i FROM d.q ORDER BY id;",
    "SELECT CASE WHEN id = 1 THEN 1 ELSE 2.5 END FROM d.q ORDER BY id;",
    "SELECT COALESCE(i, 1.5) FROM d.q ORDER BY id;",
    "SELECT AVG(i) FROM d.q;",
]

BIG_QUERY = "SELECT id, s FROM d.big ORDER BY id;"
BIG_ROWS = 5000
BIG_SAMPLE = [0, 1, 2046, 2047, 2048, 2049, 4999]


def _setup(execute):
    execute("CREATE DATABASE d;")
    for i, (_, sql_type, values) in enumerate(TABLE_CASES):
        execute(f"CREATE TABLE d.t{i} (id INTEGER, v {sql_type});")
        execute(f"INSERT INTO d.t{i} (id, v) VALUES {values};")
    execute("CREATE TABLE d.q (id INTEGER, i INTEGER, s VARCHAR);")
    execute("INSERT INTO d.q (id, i, s) VALUES (1, 10, 'a'), (2, 20, NULL), (3, NULL, 'c');")
    execute("CREATE TABLE d.n (id INTEGER, i INTEGER);")
    execute("INSERT INTO d.n (id, i) VALUES (1, 10), (2, 20), (3, 30);")
    execute("CREATE TABLE d.big (id INTEGER, s VARCHAR);")
    execute("INSERT INTO d.big (id, s) VALUES " + ", ".join(f"({n}, 'row {n}')" for n in range(1, BIG_ROWS + 1)) + ";")


def _arrow_cases():
    import datetime
    import decimal

    import pyarrow as pa

    d = decimal.Decimal
    return {
        "arrow bool": pa.array([True, None]),
        "arrow int8": pa.array([1, None], type=pa.int8()),
        "arrow list<int64>": pa.array([[1, 2], None]),
        "arrow list<string> with a NULL element": pa.array([["a", None], None]),
        "arrow list<decimal128(10,2)>": pa.array([[d("12345.67")], None], type=pa.list_(pa.decimal128(10, 2))),
        "arrow fixed_size_list<int32,2>": pa.array([[1, 2], None], type=pa.list_(pa.int32(), 2)),
        "arrow struct<a,b>": pa.array([{"a": 1, "b": "x"}, None]),
        "arrow struct<d decimal128(10,2)>": pa.array(
            [{"d": d("12345.67")}, None], type=pa.struct([("d", pa.decimal128(10, 2))])
        ),
        "arrow map<string,int32>": pa.array([[("k", 1), ("j", 2)], None], type=pa.map_(pa.string(), pa.int32())),
        "arrow map<int32,string>": pa.array([[(1, "x"), (2, "y")], None], type=pa.map_(pa.int32(), pa.string())),
        "arrow decimal128(10,2)": pa.array([d("12345.67"), None], type=pa.decimal128(10, 2)),
        "arrow date32": pa.array([datetime.date(2024, 3, 15), None]),
        "arrow timestamp[us]": pa.array([datetime.datetime(2024, 3, 15, 10, 20, 30), None]),
    }


_ARROW_NAMES = [
    "arrow bool",
    "arrow int8",
    "arrow list<int64>",
    "arrow list<string> with a NULL element",
    "arrow list<decimal128(10,2)>",
    "arrow fixed_size_list<int32,2>",
    "arrow struct<a,b>",
    "arrow struct<d decimal128(10,2)>",
    "arrow map<string,int32>",
    "arrow map<int32,string>",
    "arrow decimal128(10,2)",
    "arrow date32",
    "arrow timestamp[us]",
]


def _attempt(fn):
    try:
        return ("ok", repr(fn()))
    except Exception as exc:  # the observation IS the exception
        return ("raises", type(exc).__name__, str(exc))


def _column_values(frame, column):
    values = frame[column].tolist()
    if str(frame[column].dtype) == "str":  # pandas >= 3 string dtype
        values = [None if value != value else value for value in values]
    return values


def _frame_of(fn):
    try:
        frame = fn()
    except Exception as exc:
        return ("raises", type(exc).__name__, str(exc))
    if frame is None:
        return ("ok", None)
    dtypes = ["object" if str(t) == "str" else str(t) for t in frame.dtypes]
    return ("ok", list(frame.columns), dtypes, repr([_column_values(frame, c) for c in frame.columns]))


def _one_by_one(result):
    return _attempt(lambda: [result.fetchone(), result.fetchone(), result.fetchone(), result.fetchone()])


def _in_chunks(result):
    return _attempt(lambda: [result.fetchmany(2), result.fetchmany(2), result.fetchmany(2)])


def observe_cursor(client, query):
    return {
        "fetchall": _attempt(lambda: client.execute(query).fetchall()),
        "fetchone": _one_by_one(client.execute(query)),
        "fetchmany": _in_chunks(client.execute(query)),
        "description": _attempt(lambda: client.execute(query).description),
    }


def observe_result(conn, query):
    return {
        "fetchall": _attempt(lambda: conn.execute(query).fetchall()),
        "fetchone": _one_by_one(conn.execute(query)),
        "fetchmany": _in_chunks(conn.execute(query)),
        "df": _frame_of(lambda: conn.execute(query).df()),
    }


def observe_relation(conn, arrow_array):
    import pyarrow as pa

    table = pa.table({"v": arrow_array})
    return {
        "fetchall": _attempt(lambda: conn.from_object(table).fetchall()),
        "fetchone": _one_by_one(conn.from_object(table)),
        "df": _frame_of(lambda: conn.from_object(table).df()),
    }


def observe_cursor_entry_points(client, query, key_by_name, key_by_index):
    return {
        f"getitem {key_by_name!r}": _attempt(lambda: client.execute(query)[key_by_name]),
        f"getitem {key_by_index!r}": _attempt(lambda: client.execute(query)[key_by_index]),
        "repr": _attempt(lambda: repr(client.execute(query))),
        "iteration": _attempt(lambda: [row for row in client.execute(query)]),
        "len": _attempt(lambda: len(client.execute(query))),
    }


def _sampled(rows):
    return {"count": len(rows), "sample": [rows[i] for i in BIG_SAMPLE]}


def observe_big(handle):
    def frame_sample():
        frame = handle(BIG_QUERY).df()
        ids, texts = frame["id"].tolist(), _column_values(frame, "s")
        return {
            "dtypes": [str(frame["id"].dtype)],
            "count": len(frame),
            "sample": [(ids[i], texts[i]) for i in BIG_SAMPLE],
        }

    handles = []

    def fetch_sampled():
        handles.append(handle(BIG_QUERY))
        return _sampled(handles[0].fetchall())

    observation = {"fetchall": _attempt(fetch_sampled)}
    if handles and hasattr(handles[0], "df"):
        observation["df"] = _attempt(frame_sample)
    return observation


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    c = Client(str(tmp_path_factory.mktemp("snapshot_client")))

    def execute(sql):
        assert c.execute(sql).is_success(), sql[:80]

    _setup(execute)
    yield c


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
    connection = otterbrix.connect(str(tmp_path_factory.mktemp("snapshot_connection")))
    _setup(connection.execute)
    yield connection
    connection.close()


_LABELS = [label for label, _, _ in TABLE_CASES]


@pytest.mark.parametrize("index", range(len(TABLE_CASES)), ids=_LABELS)
def test_client_cursor_snapshot(client, index):
    assert observe_cursor(client, f"SELECT v FROM d.t{index} ORDER BY id;") == CURSOR[_LABELS[index]]


@pytest.mark.parametrize("index", range(len(TABLE_CASES)), ids=_LABELS)
def test_connection_result_snapshot(conn, index):
    assert observe_result(conn, f"SELECT v FROM d.t{index} ORDER BY id;") == RESULT[_LABELS[index]]


@pytest.mark.parametrize("query", QUERY_CASES)
def test_query_snapshot_on_both_paths(client, conn, query):
    assert {"cursor": observe_cursor(client, query), "result": observe_result(conn, query)} == QUERY[query]


@pytest.mark.parametrize("name", _ARROW_NAMES)
def test_relation_snapshot(tmp_path, name):
    pytest.importorskip("pyarrow")
    connection = otterbrix.connect(str(tmp_path / "relation"))
    try:
        assert observe_relation(connection, _arrow_cases()[name]) == RELATION[name]
    finally:
        connection.close()


def test_a_result_of_several_chunks_through_the_cursor(client):
    assert observe_big(lambda q: client.execute(q)) == BIG["cursor"]


def test_a_result_of_several_chunks_through_the_connection(conn):
    assert observe_big(lambda q: conn.execute(q)) == BIG["result"]


def test_cursor_entry_points_on_named_columns(client):
    observed = observe_cursor_entry_points(client, "SELECT id, s FROM d.q ORDER BY id;", "s", 1)
    assert observed == ENTRY_POINTS["named"]


def test_cursor_entry_points_on_unnamed_columns(client):
    observed = observe_cursor_entry_points(client, "SELECT 1, 'x';", 0, 1)
    assert observed == ENTRY_POINTS["unnamed"]


def test_a_uuid_column_cannot_be_filled_from_sql(tmp_path):
    # No SQL literal yields a UUID, so UUID values are unreachable from Python today.
    c = Client(str(tmp_path / "uuid"))
    c.execute("CREATE DATABASE d;")
    assert c.execute("CREATE TABLE d.t (id INTEGER, v UUID);").is_success()
    inserted = c.execute("INSERT INTO d.t (id, v) VALUES (1, '123e4567-e89b-12d3-a456-426614174000');")
    assert inserted.is_error()
    assert inserted.get_error() == UUID_INSERT_ERROR
    only_null = c.execute("INSERT INTO d.t (id, v) VALUES (1, NULL);")
    assert only_null.is_success()
    assert _attempt(lambda: c.execute("SELECT v FROM d.t;").fetchall()) == UUID_NULL_FETCH


# ---- pinned expectations (generated from the current build, then reviewed by hand) ----

# today's behaviour, see #726: `description` carries '' as the type name of every column.
CURSOR = {'BOOLEAN': {'fetchall': ('ok', '[(True,), (None,)]'),
             'fetchone': ('ok', '[(True,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(True,), (None,)], [], []]'),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'TINYINT': {'fetchall': ('ok', '[(-5,), (None,)]'),
             'fetchone': ('ok', '[(-5,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(-5,), (None,)], [], []]'),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'SMALLINT': {'fetchall': ('ok', '[(-300,), (None,)]'),
              'fetchone': ('ok', '[(-300,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(-300,), (None,)], [], []]'),
              'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'INTEGER': {'fetchall': ('ok', '[(-70000,), (None,)]'),
             'fetchone': ('ok', '[(-70000,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(-70000,), (None,)], [], []]'),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'BIGINT': {'fetchall': ('ok', '[(-5000000000,), (None,)]'),
            'fetchone': ('ok', '[(-5000000000,), (None,), None, None]'),
            'fetchmany': ('ok', '[[(-5000000000,), (None,)], [], []]'),
            'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: hugeint comes back as None
 'HUGEINT': {'fetchall': ('ok', '[(None,), (None,)]'),
             'fetchone': ('ok', '[(None,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(None,), (None,)], [], []]'),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'UTINYINT': {'fetchall': ('ok', '[(200,), (None,)]'),
              'fetchone': ('ok', '[(200,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(200,), (None,)], [], []]'),
              'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'USMALLINT': {'fetchall': ('ok', '[(60000,), (None,)]'),
               'fetchone': ('ok', '[(60000,), (None,), None, None]'),
               'fetchmany': ('ok', '[[(60000,), (None,)], [], []]'),
               'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'UINTEGER': {'fetchall': ('ok', '[(4000000000,), (None,)]'),
              'fetchone': ('ok', '[(4000000000,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(4000000000,), (None,)], [], []]'),
              'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'UBIGINT': {'fetchall': ('ok', '[(18446744073709551615,), (None,)]'),
             'fetchone': ('ok', '[(18446744073709551615,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(18446744073709551615,), (None,)], [], []]'),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'FLOAT': {'fetchall': ('ok', '[(1.5,), (None,)]'),
           'fetchone': ('ok', '[(1.5,), (None,), None, None]'),
           'fetchmany': ('ok', '[[(1.5,), (None,)], [], []]'),
           'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'DOUBLE': {'fetchall': ('ok', '[(2.25,), (None,)]'),
            'fetchone': ('ok', '[(2.25,), (None,), None, None]'),
            'fetchmany': ('ok', '[[(2.25,), (None,)], [], []]'),
            'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: decimal comes back as the raw unscaled integer
 'DECIMAL(10,2)': {'fetchall': ('ok', '[(1234567,), (None,)]'),
                   'fetchone': ('ok', '[(1234567,), (None,), None, None]'),
                   'fetchmany': ('ok', '[[(1234567,), (None,)], [], []]'),
                   'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: decimal comes back as the raw unscaled integer
 'DECIMAL(18,4)': {'fetchall': ('ok', '[(-31415,), (None,)]'),
                   'fetchone': ('ok', '[(-31415,), (None,), None, None]'),
                   'fetchmany': ('ok', '[[(-31415,), (None,)], [], []]'),
                   'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: decimal comes back as the raw unscaled integer
 'DECIMAL(5,0)': {'fetchall': ('ok', '[(42,), (None,)]'),
                  'fetchone': ('ok', '[(42,), (None,), None, None]'),
                  'fetchmany': ('ok', '[[(42,), (None,)], [], []]'),
                  'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: wide decimal comes back as None
 'DECIMAL(38,10)': {'fetchall': ('ok', '[(None,), (None,)]'),
                    'fetchone': ('ok', '[(None,), (None,), None, None]'),
                    'fetchmany': ('ok', '[[(None,), (None,)], [], []]'),
                    'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'VARCHAR': {'fetchall': ('ok', "[('hello',), (None,)]"),
             'fetchone': ('ok', "[('hello',), (None,), None, None]"),
             'fetchmany': ('ok', "[[('hello',), (None,)], [], []]"),
             'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: date is an int (days since 2000-01-01)
 'DATE': {'fetchall': ('ok', '[(8840,), (None,)]'),
          'fetchone': ('ok', '[(8840,), (None,), None, None]'),
          'fetchmany': ('ok', '[[(8840,), (None,)], [], []]'),
          'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: timestamp is an int (microseconds since 2000-01-01)
 'TIMESTAMP': {'fetchall': ('ok', '[(763813230000000,), (None,)]'),
               'fetchone': ('ok', '[(763813230000000,), (None,), None, None]'),
               'fetchmany': ('ok', '[[(763813230000000,), (None,)], [], []]'),
               'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: time is an int
 'TIME': {'fetchall': ('ok', '[(37230000000,), (None,)]'),
          'fetchone': ('ok', '[(37230000000,), (None,), None, None]'),
          'fetchmany': ('ok', '[[(37230000000,), (None,)], [], []]'),
          'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: interval is a {'': 0} dict
 'INTERVAL': {'fetchall': ('ok', "[({'': 0},), (None,)]"),
              'fetchone': ('ok', "[({'': 0},), (None,), None, None]"),
              'fetchmany': ('ok', "[[({'': 0},), (None,)], [], []]"),
              'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 # today's behaviour, see #726: timestamptz is an int
 'TIMESTAMP WITH TIME ZONE': {'fetchall': ('ok', '[(763813230000000,), (None,)]'),
                              'fetchone': ('ok', '[(763813230000000,), (None,), None, None]'),
                              'fetchmany': ('ok', '[[(763813230000000,), (None,)], [], []]'),
                              'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'INTEGER[]': {'fetchall': ('ok', '[([1, 2, 3],), (None,)]'),
               'fetchone': ('ok', '[([1, 2, 3],), (None,), None, None]'),
               'fetchmany': ('ok', '[[([1, 2, 3],), (None,)], [], []]'),
               'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'INTEGER[] with a NULL element': {'fetchall': ('ok', '[([1, None],), (None,)]'),
                                   'fetchone': ('ok', '[([1, None],), (None,), None, None]'),
                                   'fetchmany': ('ok', '[[([1, None],), (None,)], [], []]'),
                                   'description': ('ok', "[('v', '', None, None, None, None, None)]")},
 'VARCHAR[] without a NULL row': {'fetchall': ('ok', "[(['a', 'b'],), (['c'],)]"),
                                  'fetchone': ('ok', "[(['a', 'b'],), (['c'],), None, None]"),
                                  'fetchmany': ('ok', "[[(['a', 'b'],), (['c'],)], [], []]"),
                                  'description': ('ok', "[('v', '', None, None, None, None, None)]")}}

# today's behaviour, see #726: every integer width reaches df() as float64 when the column holds a NULL.
RESULT = {'BOOLEAN': {'fetchall': ('ok', '[(True,), (None,)]'),
             'fetchone': ('ok', '[(True,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(True,), (None,)], [], []]'),
             'df': ('ok', ['v'], ['object'], '[[True, None]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'TINYINT': {'fetchall': ('ok', '[(-5,), (None,)]'),
             'fetchone': ('ok', '[(-5,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(-5,), (None,)], [], []]'),
             'df': ('ok', ['v'], ['float64'], '[[-5.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'SMALLINT': {'fetchall': ('ok', '[(-300,), (None,)]'),
              'fetchone': ('ok', '[(-300,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(-300,), (None,)], [], []]'),
              'df': ('ok', ['v'], ['float64'], '[[-300.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'INTEGER': {'fetchall': ('ok', '[(-70000,), (None,)]'),
             'fetchone': ('ok', '[(-70000,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(-70000,), (None,)], [], []]'),
             'df': ('ok', ['v'], ['float64'], '[[-70000.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'BIGINT': {'fetchall': ('ok', '[(-5000000000,), (None,)]'),
            'fetchone': ('ok', '[(-5000000000,), (None,), None, None]'),
            'fetchmany': ('ok', '[[(-5000000000,), (None,)], [], []]'),
            'df': ('ok', ['v'], ['float64'], '[[-5000000000.0, nan]]')},
 # today's behaviour, see #726: hugeint raises
 'HUGEINT': {'fetchall': ('raises',
                          'RuntimeError',
                          "OtterBrix doen't support hugeint conversation to python object"),
             'fetchone': ('raises',
                          'RuntimeError',
                          "OtterBrix doen't support hugeint conversation to python object"),
             'fetchmany': ('raises',
                           'RuntimeError',
                           "OtterBrix doen't support hugeint conversation to python object"),
             'df': ('raises',
                    'RuntimeError',
                    "OtterBrix doen't support hugeint conversation to python object")},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'UTINYINT': {'fetchall': ('ok', '[(200,), (None,)]'),
              'fetchone': ('ok', '[(200,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(200,), (None,)], [], []]'),
              'df': ('ok', ['v'], ['float64'], '[[200.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'USMALLINT': {'fetchall': ('ok', '[(60000,), (None,)]'),
               'fetchone': ('ok', '[(60000,), (None,), None, None]'),
               'fetchmany': ('ok', '[[(60000,), (None,)], [], []]'),
               'df': ('ok', ['v'], ['float64'], '[[60000.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'UINTEGER': {'fetchall': ('ok', '[(4000000000,), (None,)]'),
              'fetchone': ('ok', '[(4000000000,), (None,), None, None]'),
              'fetchmany': ('ok', '[[(4000000000,), (None,)], [], []]'),
              'df': ('ok', ['v'], ['float64'], '[[4000000000.0, nan]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'UBIGINT': {'fetchall': ('ok', '[(18446744073709551615,), (None,)]'),
             'fetchone': ('ok', '[(18446744073709551615,), (None,), None, None]'),
             'fetchmany': ('ok', '[[(18446744073709551615,), (None,)], [], []]'),
             'df': ('ok', ['v'], ['float64'], '[[1.8446744073709552e+19, nan]]')},
 'FLOAT': {'fetchall': ('ok', '[(1.5,), (None,)]'),
           'fetchone': ('ok', '[(1.5,), (None,), None, None]'),
           'fetchmany': ('ok', '[[(1.5,), (None,)], [], []]'),
           'df': ('ok', ['v'], ['float64'], '[[1.5, nan]]')},
 'DOUBLE': {'fetchall': ('ok', '[(2.25,), (None,)]'),
            'fetchone': ('ok', '[(2.25,), (None,), None, None]'),
            'fetchmany': ('ok', '[[(2.25,), (None,)], [], []]'),
            'df': ('ok', ['v'], ['float64'], '[[2.25, nan]]')},
 # today's behaviour, see #726: decimal is divided by the scale again (12345.67 -> 617283.5)
 'DECIMAL(10,2)': {'fetchall': ('ok', "[(Decimal('617283.5'),), (None,)]"),
                   'fetchone': ('ok', "[(Decimal('617283.5'),), (None,), None, None]"),
                   'fetchmany': ('ok', "[[(Decimal('617283.5'),), (None,)], [], []]"),
                   'df': ('ok', ['v'], ['object'], "[[Decimal('617283.5'), None]]")},
 # today's behaviour, see #726: decimal is divided by the scale again
 'DECIMAL(18,4)': {'fetchall': ('ok', "[(Decimal('-7853.75'),), (None,)]"),
                   'fetchone': ('ok', "[(Decimal('-7853.75'),), (None,), None, None]"),
                   'fetchmany': ('ok', "[[(Decimal('-7853.75'),), (None,)], [], []]"),
                   'df': ('ok', ['v'], ['object'], "[[Decimal('-7853.75'), None]]")},
 # today's behaviour, see #726: scale 0 divides by zero
 'DECIMAL(5,0)': {'fetchall': ('raises', 'DivisionByZero', "[<class 'decimal.DivisionByZero'>]"),
                  'fetchone': ('raises', 'DivisionByZero', "[<class 'decimal.DivisionByZero'>]"),
                  'fetchmany': ('raises', 'DivisionByZero', "[<class 'decimal.DivisionByZero'>]"),
                  'df': ('raises', 'DivisionByZero', "[<class 'decimal.DivisionByZero'>]")},
 # today's behaviour, see #726: 123456.7890123456 is inserted, Decimal('123456789012345.6') comes back
 'DECIMAL(38,10)': {'fetchall': ('ok', "[(Decimal('123456789012345.6'),), (None,)]"),
                    'fetchone': ('ok', "[(Decimal('123456789012345.6'),), (None,), None, None]"),
                    'fetchmany': ('ok', "[[(Decimal('123456789012345.6'),), (None,)], [], []]"),
                    'df': ('ok', ['v'], ['object'], "[[Decimal('123456789012345.6'), None]]")},
 'VARCHAR': {'fetchall': ('ok', "[('hello',), (None,)]"),
             'fetchone': ('ok', "[('hello',), (None,), None, None]"),
             'fetchmany': ('ok', "[[('hello',), (None,)], [], []]"),
             'df': ('ok', ['v'], ['object'], "[['hello', None]]")},
 # today's behaviour, see #726: date raises 'Unsupported type'
 'DATE': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
          'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
          'fetchmany': ('raises', 'RuntimeError', 'Unsupported type: '),
          'df': ('raises', 'RuntimeError', 'Unsupported type: ')},
 # today's behaviour, see #726: timestamp is an int / float64 in df
 'TIMESTAMP': {'fetchall': ('ok', '[(763813230000000,), (None,)]'),
               'fetchone': ('ok', '[(763813230000000,), (None,), None, None]'),
               'fetchmany': ('ok', '[[(763813230000000,), (None,)], [], []]'),
               'df': ('ok', ['v'], ['float64'], '[[763813230000000.0, nan]]')},
 # today's behaviour, see #726: time raises 'Unsupported type'
 'TIME': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
          'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
          'fetchmany': ('raises', 'RuntimeError', 'Unsupported type: '),
          'df': ('raises', 'RuntimeError', 'Unsupported type: ')},
 # today's behaviour, see #726: interval raises 'Unsupported type'
 'INTERVAL': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
              'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
              'fetchmany': ('raises', 'RuntimeError', 'Unsupported type: '),
              'df': ('raises', 'RuntimeError', 'Unsupported type: ')},
 # today's behaviour, see #726: timestamptz raises 'Unsupported type'
 'TIMESTAMP WITH TIME ZONE': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
                              'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
                              'fetchmany': ('raises', 'RuntimeError', 'Unsupported type: '),
                              'df': ('raises', 'RuntimeError', 'Unsupported type: ')},
 'INTEGER[]': {'fetchall': ('ok', '[([1, 2, 3],), (None,)]'),
               'fetchone': ('ok', '[([1, 2, 3],), (None,), None, None]'),
               'fetchmany': ('ok', '[[([1, 2, 3],), (None,)], [], []]'),
               'df': ('ok', ['v'], ['object'], '[[[1, 2, 3], None]]')},
 'INTEGER[] with a NULL element': {'fetchall': ('ok', '[([1, None],), (None,)]'),
                                   'fetchone': ('ok', '[([1, None],), (None,), None, None]'),
                                   'fetchmany': ('ok', '[[([1, None],), (None,)], [], []]'),
                                   'df': ('ok', ['v'], ['object'], '[[[1, None], None]]')},
 'VARCHAR[] without a NULL row': {'fetchall': ('ok', "[(['a', 'b'],), (['c'],)]"),
                                  'fetchone': ('ok', "[(['a', 'b'],), (['c'],), None, None]"),
                                  'fetchmany': ('ok', "[[(['a', 'b'],), (['c'],)], [], []]"),
                                  'df': ('ok', ['v'], ['object'], "[[['a', 'b'], ['c']]]")}}

# today's behaviour, see #726: ROW(1,'a') is {'': 'a'} on the cursor path and (1, 'a') on the result path;
# unnamed columns have the name ''; `description` carries '' as the type name of every column.
QUERY = {"SELECT ROW(1,'a');": {'cursor': {'fetchall': ('ok', "[({'': 'a'},)]"),
                                   'fetchone': ('ok', "[({'': 'a'},), None, None, None]"),
                                   'fetchmany': ('ok', "[[({'': 'a'},)], [], []]"),
                                   'description': ('ok', "[('', '', None, None, None, None, None)]")},
                        'result': {'fetchall': ('ok', "[((1, 'a'),)]"),
                                   'fetchone': ('ok', "[((1, 'a'),), None, None, None]"),
                                   'fetchmany': ('ok', "[[((1, 'a'),)], [], []]"),
                                   'df': ('ok', [''], ['object'], "[[(1, 'a')]]")}},
 # today's behaviour, see #726: cursor: None; result: a tuple
 'SELECT ARRAY[1,2];': {'cursor': {'fetchall': ('ok', '[(None,)]'),
                                   'fetchone': ('ok', '[(None,), None, None, None]'),
                                   'fetchmany': ('ok', '[[(None,)], [], []]'),
                                   'description': ('ok', "[('', '', None, None, None, None, None)]")},
                        'result': {'fetchall': ('ok', '[((1, 2),)]'),
                                   'fetchone': ('ok', '[((1, 2),), None, None, None]'),
                                   'fetchmany': ('ok', '[[((1, 2),)], [], []]'),
                                   'df': ('ok', [''], ['object'], '[[(1, 2)]]')}},
 # today's behaviour, see #726: cursor: raw unscaled integer; result: Decimal('75'), divided by the scale again
 'SELECT 1.5::DECIMAL(10,2);': {'cursor': {'fetchall': ('ok', '[(150,)]'),
                                           'fetchone': ('ok', '[(150,), None, None, None]'),
                                           'fetchmany': ('ok', '[[(150,)], [], []]'),
                                           'description': ('ok', "[('', '', None, None, None, None, None)]")},
                                'result': {'fetchall': ('ok', "[(Decimal('75'),)]"),
                                           'fetchone': ('ok', "[(Decimal('75'),), None, None, None]"),
                                           'fetchmany': ('ok', "[[(Decimal('75'),)], [], []]"),
                                           'df': ('ok', [''], ['object'], "[[Decimal('75')]]")}},
 # today's behaviour, see #726: cursor: int; result raises 'Unsupported type'
 "SELECT DATE '2024-03-15';": {'cursor': {'fetchall': ('ok', '[(8840,)]'),
                                          'fetchone': ('ok', '[(8840,), None, None, None]'),
                                          'fetchmany': ('ok', '[[(8840,)], [], []]'),
                                          'description': ('ok', "[('', '', None, None, None, None, None)]")},
                               'result': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
                                          'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
                                          'fetchmany': ('raises', 'RuntimeError', 'Unsupported type: '),
                                          'df': ('raises', 'RuntimeError', 'Unsupported type: ')}},
 'SELECT NULL;': {'cursor': {'fetchall': ('ok', '[(None,)]'),
                             'fetchone': ('ok', '[(None,), None, None, None]'),
                             'fetchmany': ('ok', '[[(None,)], [], []]'),
                             'description': ('ok', "[('', '', None, None, None, None, None)]")},
                  'result': {'fetchall': ('ok', '[(None,)]'),
                             'fetchone': ('ok', '[(None,), None, None, None]'),
                             'fetchmany': ('ok', '[[(None,)], [], []]'),
                             'df': ('ok', [''], ['object'], '[[None]]')}},
 # today's behaviour, see #726: df keeps ONE column named '' (the second unnamed column wins)
 "SELECT 1, 'x';": {'cursor': {'fetchall': ('ok', "[(1, 'x')]"),
                               'fetchone': ('ok', "[(1, 'x'), None, None, None]"),
                               'fetchmany': ('ok', "[[(1, 'x')], [], []]"),
                               'description': ('ok',
                                               "[('', '', None, None, None, None, None), ('', '', None, "
                                               'None, None, None, None)]')},
                    'result': {'fetchall': ('ok', "[(1, 'x')]"),
                               'fetchone': ('ok', "[(1, 'x'), None, None, None]"),
                               'fetchmany': ('ok', "[[(1, 'x')], [], []]"),
                               'df': ('ok', [''], ['object'], "[['x']]")}},
 # today's behaviour, see #726: df collapses the duplicate column name to one column
 'SELECT i, i FROM d.q ORDER BY id;': {'cursor': {'fetchall': ('ok', '[(10, 10), (20, 20), (None, None)]'),
                                                  'fetchone': ('ok',
                                                               '[(10, 10), (20, 20), (None, None), None]'),
                                                  'fetchmany': ('ok',
                                                                '[[(10, 10), (20, 20)], [(None, None)], []]'),
                                                  'description': ('ok',
                                                                  "[('i', '', None, None, None, None, None), "
                                                                  "('i', '', None, None, None, None, "
                                                                  'None)]')},
                                       'result': {'fetchall': ('ok', '[(10, 10), (20, 20), (None, None)]'),
                                                  'fetchone': ('ok',
                                                               '[(10, 10), (20, 20), (None, None), None]'),
                                                  'fetchmany': ('ok',
                                                                '[[(10, 10), (20, 20)], [(None, None)], []]'),
                                                  'df': ('ok', ['i'], ['float64'], '[[10.0, 20.0, nan]]')}},
 # today's behaviour, see #726: df keeps only the last column of the same name
 'SELECT i AS a, s AS a FROM d.q ORDER BY id;': {'cursor': {'fetchall': ('ok',
                                                                         "[(10, 'a'), (20, None), (None, "
                                                                         "'c')]"),
                                                            'fetchone': ('ok',
                                                                         "[(10, 'a'), (20, None), (None, "
                                                                         "'c'), None]"),
                                                            'fetchmany': ('ok',
                                                                          "[[(10, 'a'), (20, None)], [(None, "
                                                                          "'c')], []]"),
                                                            'description': ('ok',
                                                                            "[('a', '', None, None, None, "
                                                                            "None, None), ('a', '', None, "
                                                                            'None, None, None, None)]')},
                                                 'result': {'fetchall': ('ok',
                                                                         "[(10, 'a'), (20, None), (None, "
                                                                         "'c')]"),
                                                            'fetchone': ('ok',
                                                                         "[(10, 'a'), (20, None), (None, "
                                                                         "'c'), None]"),
                                                            'fetchmany': ('ok',
                                                                          "[[(10, 'a'), (20, None)], [(None, "
                                                                          "'c')], []]"),
                                                            'df': ('ok',
                                                                   ['a'],
                                                                   ['object'],
                                                                   "[['a', None, 'c']]")}},
 'SELECT i FROM d.q WHERE false;': {'cursor': {'fetchall': ('ok', '[]'),
                                               'fetchone': ('ok', '[None, None, None, None]'),
                                               'fetchmany': ('ok', '[[], [], []]'),
                                               'description': ('ok',
                                                               "[('i', '', None, None, None, None, None)]")},
                                    'result': {'fetchall': ('ok', '[]'),
                                               'fetchone': ('ok', '[None, None, None, None]'),
                                               'fetchmany': ('ok', '[[], [], []]'),
                                               'df': ('ok', None)}},
 'SELECT i FROM d.n ORDER BY id;': {'cursor': {'fetchall': ('ok', '[(10,), (20,), (30,)]'),
                                               'fetchone': ('ok', '[(10,), (20,), (30,), None]'),
                                               'fetchmany': ('ok', '[[(10,), (20,)], [(30,)], []]'),
                                               'description': ('ok',
                                                               "[('i', '', None, None, None, None, None)]")},
                                    'result': {'fetchall': ('ok', '[(10,), (20,), (30,)]'),
                                               'fetchone': ('ok', '[(10,), (20,), (30,), None]'),
                                               'fetchmany': ('ok', '[[(10,), (20,)], [(30,)], []]'),
                                               'df': ('ok', ['i'], ['int64'], '[[10, 20, 30]]')}},
 # today's behaviour, see #726: integer column with NULL becomes float64 in df;
# the same column without NULL (d.n) stays int64
 'SELECT i FROM d.q ORDER BY id;': {'cursor': {'fetchall': ('ok', '[(10,), (20,), (None,)]'),
                                               'fetchone': ('ok', '[(10,), (20,), (None,), None]'),
                                               'fetchmany': ('ok', '[[(10,), (20,)], [(None,)], []]'),
                                               'description': ('ok',
                                                               "[('i', '', None, None, None, None, None)]")},
                                    'result': {'fetchall': ('ok', '[(10,), (20,), (None,)]'),
                                               'fetchone': ('ok', '[(10,), (20,), (None,), None]'),
                                               'fetchmany': ('ok', '[[(10,), (20,)], [(None,)], []]'),
                                               'df': ('ok', ['i'], ['float64'], '[[10.0, 20.0, nan]]')}},
 'SELECT CASE WHEN id = 1 THEN 1 ELSE 2.5 END FROM d.q ORDER BY id;': {'cursor': {'fetchall': ('ok',
                                                                                               '[(1.0,), '
                                                                                               '(2.5,), '
                                                                                               '(2.5,)]'),
                                                                                  'fetchone': ('ok',
                                                                                               '[(1.0,), '
                                                                                               '(2.5,), '
                                                                                               '(2.5,), '
                                                                                               'None]'),
                                                                                  'fetchmany': ('ok',
                                                                                                '[[(1.0,), '
                                                                                                '(2.5,)], '
                                                                                                '[(2.5,)], '
                                                                                                '[]]'),
                                                                                  'description': ('ok',
                                                                                                  "[('case_0', "
                                                                                                  "'', None, "
                                                                                                  'None, '
                                                                                                  'None, '
                                                                                                  'None, '
                                                                                                  'None)]')},
                                                                       'result': {'fetchall': ('ok',
                                                                                               '[(1.0,), '
                                                                                               '(2.5,), '
                                                                                               '(2.5,)]'),
                                                                                  'fetchone': ('ok',
                                                                                               '[(1.0,), '
                                                                                               '(2.5,), '
                                                                                               '(2.5,), '
                                                                                               'None]'),
                                                                                  'fetchmany': ('ok',
                                                                                                '[[(1.0,), '
                                                                                                '(2.5,)], '
                                                                                                '[(2.5,)], '
                                                                                                '[]]'),
                                                                                  'df': ('ok',
                                                                                         ['case_0'],
                                                                                         ['float64'],
                                                                                         '[[1.0, 2.5, '
                                                                                         '2.5]]')}},
 'SELECT COALESCE(i, 1.5) FROM d.q ORDER BY id;': {'cursor': {'fetchall': ('ok',
                                                                           '[(10.0,), (20.0,), (1.5,)]'),
                                                              'fetchone': ('ok',
                                                                           '[(10.0,), (20.0,), (1.5,), '
                                                                           'None]'),
                                                              'fetchmany': ('ok',
                                                                            '[[(10.0,), (20.0,)], [(1.5,)], '
                                                                            '[]]'),
                                                              'description': ('ok',
                                                                              "[('coalesce', '', None, None, "
                                                                              'None, None, None)]')},
                                                   'result': {'fetchall': ('ok',
                                                                           '[(10.0,), (20.0,), (1.5,)]'),
                                                              'fetchone': ('ok',
                                                                           '[(10.0,), (20.0,), (1.5,), '
                                                                           'None]'),
                                                              'fetchmany': ('ok',
                                                                            '[[(10.0,), (20.0,)], [(1.5,)], '
                                                                            '[]]'),
                                                              'df': ('ok',
                                                                     ['coalesce'],
                                                                     ['float64'],
                                                                     '[[10.0, 20.0, 1.5]]')}},
 'SELECT AVG(i) FROM d.q;': {'cursor': {'fetchall': ('ok', '[(15.0,)]'),
                                        'fetchone': ('ok', '[(15.0,), None, None, None]'),
                                        'fetchmany': ('ok', '[[(15.0,)], [], []]'),
                                        'description': ('ok', "[('avg', '', None, None, None, None, None)]")},
                             'result': {'fetchall': ('ok', '[(15.0,)]'),
                                        'fetchone': ('ok', '[(15.0,), None, None, None]'),
                                        'fetchmany': ('ok', '[[(15.0,)], [], []]'),
                                        'df': ('ok', ['avg'], ['float64'], '[[15.0]]')}}}

RELATION = {'arrow bool': {'fetchall': ('ok', '[(True,), (None,)]'),
                'fetchone': ('ok', '[(True,), (None,), None, None]'),
                'df': ('ok', ['v'], ['object'], '[[True, None]]')},
 # today's behaviour, see #726: integer column becomes float64 in df (NULL -> NaN)
 'arrow int8': {'fetchall': ('ok', '[(1,), (None,)]'),
                'fetchone': ('ok', '[(1,), (None,), None, None]'),
                'df': ('ok', ['v'], ['float64'], '[[1.0, nan]]')},
 'arrow list<int64>': {'fetchall': ('ok', '[([1, 2],), (None,)]'),
                       'fetchone': ('ok', '[([1, 2],), (None,), None, None]'),
                       'df': ('ok', ['v'], ['object'], '[[[1, 2], None]]')},
 'arrow list<string> with a NULL element': {'fetchall': ('ok', "[(['a', None],), (None,)]"),
                                            'fetchone': ('ok', "[(['a', None],), (None,), None, None]"),
                                            'df': ('ok', ['v'], ['object'], "[[['a', None], None]]")},
 # today's behaviour, see #726: decimal inside a list is divided by the scale again
 'arrow list<decimal128(10,2)>': {'fetchall': ('ok', "[([Decimal('617283.5')],), (None,)]"),
                                  'fetchone': ('ok', "[([Decimal('617283.5')],), (None,), None, None]"),
                                  'df': ('ok', ['v'], ['object'], "[[[Decimal('617283.5')], None]]")},
 # today's behaviour, see #726: a fixed-size list comes back as a tuple, not a list
 'arrow fixed_size_list<int32,2>': {'fetchall': ('ok', '[((1, 2),), (None,)]'),
                                    'fetchone': ('ok', '[((1, 2),), (None,), None, None]'),
                                    'df': ('ok', ['v'], ['object'], '[[(1, 2), None]]')},
 'arrow struct<a,b>': {'fetchall': ('ok', "[({'a': 1, 'b': 'x'},), (None,)]"),
                       'fetchone': ('ok', "[({'a': 1, 'b': 'x'},), (None,), None, None]"),
                       'df': ('ok', ['v'], ['object'], "[[{'a': 1, 'b': 'x'}, None]]")},
 # today's behaviour, see #726: decimal inside a struct is divided by the scale again
 'arrow struct<d decimal128(10,2)>': {'fetchall': ('ok', "[({'d': Decimal('617283.5')},), (None,)]"),
                                      'fetchone': ('ok',
                                                   "[({'d': Decimal('617283.5')},), (None,), None, None]"),
                                      'df': ('ok',
                                             ['v'],
                                             ['object'],
                                             "[[{'d': Decimal('617283.5')}, None]]")},
 'arrow map<string,int32>': {'fetchall': ('ok', "[({'k': 1, 'j': 2},), (None,)]"),
                             'fetchone': ('ok', "[({'k': 1, 'j': 2},), (None,), None, None]"),
                             'df': ('ok', ['v'], ['object'], "[[{'k': 1, 'j': 2}, None]]")},
 'arrow map<int32,string>': {'fetchall': ('ok', "[({1: 'x', 2: 'y'},), (None,)]"),
                             'fetchone': ('ok', "[({1: 'x', 2: 'y'},), (None,), None, None]"),
                             'df': ('ok', ['v'], ['object'], "[[{1: 'x', 2: 'y'}, None]]")},
 # today's behaviour, see #726: decimal is divided by the scale again (12345.67 -> 617283.5)
 'arrow decimal128(10,2)': {'fetchall': ('ok', "[(Decimal('617283.5'),), (None,)]"),
                            'fetchone': ('ok', "[(Decimal('617283.5'),), (None,), None, None]"),
                            'df': ('ok', ['v'], ['object'], "[[Decimal('617283.5'), None]]")},
 # today's behaviour, see #726: date raises 'Unsupported type'
 'arrow date32': {'fetchall': ('raises', 'RuntimeError', 'Unsupported type: '),
                  'fetchone': ('raises', 'RuntimeError', 'Unsupported type: '),
                  'df': ('raises', 'RuntimeError', 'Unsupported type: ')},
 # today's behaviour, see #726: timestamp is an int / float64 in df
 'arrow timestamp[us]': {'fetchall': ('ok', '[(763813230000000,), (None,)]'),
                         'fetchone': ('ok', '[(763813230000000,), (None,), None, None]'),
                         'df': ('ok', ['v'], ['float64'], '[[763813230000000.0, nan]]')}}

BIG = {'cursor': {'fetchall': ('ok',
                         "{'count': 5000, 'sample': [(1, 'row 1'), (2, 'row 2'), (2047, 'row 2047'), (2048, "
                         "'row 2048'), (2049, 'row 2049'), (2050, 'row 2050'), (5000, 'row 5000')]}")},
 'result': {'fetchall': ('ok',
                         "{'count': 5000, 'sample': [(1, 'row 1'), (2, 'row 2'), (2047, 'row 2047'), (2048, "
                         "'row 2048'), (2049, 'row 2049'), (2050, 'row 2050'), (5000, 'row 5000')]}"),
            'df': ('ok',
                   "{'dtypes': ['int64'], 'count': 5000, 'sample': [(1, 'row 1'), (2, 'row 2'), (2047, 'row "
                   "2047'), (2048, 'row 2048'), (2049, 'row 2049'), (2050, 'row 2050'), (5000, 'row "
                   "5000')]}")}}

# today's behaviour, see #726: a cursor's repr and iteration always read row 0 (iteration yields row 0 three
# times); cursor[1] is a row dict (row 1), not column 1; unnamed columns are keyed by int.
ENTRY_POINTS = {'named': {"getitem 's'": ('ok', "'a'"),
           'getitem 1': ('ok', "{'id': 2, 's': None}"),
           'repr': ('ok', '"{\'id\': 1, \'s\': \'a\'}"'),
           'iteration': ('ok', "[{'id': 1, 's': 'a'}, {'id': 1, 's': 'a'}, {'id': 1, 's': 'a'}]"),
           'len': ('ok', '3')},
 'unnamed': {'getitem 0': ('ok', "{0: 1, 1: 'x'}"),
             'getitem 1': ('ok', 'None'),
             'repr': ('ok', '"{0: 1, 1: \'x\'}"'),
             'iteration': ('ok', "[{0: 1, 1: 'x'}]"),
             'len': ('ok', '1')}}

UUID_INSERT_ERROR = ('',
 "insert_node: column 'v' is of type uuid but the inserted value is of type unknown; no cast between them may "
 'be applied on assignment')

UUID_NULL_FETCH = ('ok', '[(None,)]')
