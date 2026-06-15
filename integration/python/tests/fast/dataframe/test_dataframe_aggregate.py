import pandas as pd

from otterbrix import ColumnExpression, CountExpression


def _salaries(conn):
    df = pd.DataFrame({"dept": ["a", "a", "b"], "salary": [10, 20, 30]})
    return conn.from_df(df)


def test_group_avg_is_double_typed(conn):
    rel = _salaries(conn)
    grouped = rel.group(ColumnExpression("dept", conn), ColumnExpression("salary", conn).avg())

    assert str(grouped.types[-1]) == "DOUBLE"
    result = {r[0]: r[1] for r in grouped.fetchall()}
    assert result == {"a": 15.0, "b": 30.0}


def test_global_avg_is_double(conn):
    rel = _salaries(conn)
    grouped = rel.group(ColumnExpression("salary", conn).avg())

    assert str(grouped.types[-1]) == "DOUBLE"
    assert grouped.fetchall() == [(20.0,)]


def test_count_all_rows(conn):
    rel = _salaries(conn)
    grouped = rel.group(CountExpression(conn))
    assert grouped.fetchall() == [(3,)]


def test_count_per_group(conn):
    rel = _salaries(conn)
    grouped = rel.group(ColumnExpression("dept", conn), CountExpression(conn))
    assert {r[0]: r[1] for r in grouped.fetchall()} == {"a": 2, "b": 1}
