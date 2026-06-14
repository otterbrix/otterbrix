import pandas as pd

from otterbrix import ColumnExpression, ConstantExpression


def _states(conn):
    df = pd.DataFrame(
        {
            "state": ["OH", "CA", "OH", "NY", "NY", "OH"],
            "gender": ["M", "F", "F", "M", "M", "M"],
        }
    )
    return conn.from_df(df)


def test_filter_equality(conn):
    rel = _states(conn)
    state = ColumnExpression("state", conn)
    rows = rel.filter(state == ConstantExpression("OH", conn)).fetchall()
    assert len(rows) == 3
    assert all(r[0] == "OH" for r in rows)


def test_filter_negation(conn):
    rel = _states(conn)
    state = ColumnExpression("state", conn)
    rows = rel.filter(~(state == ConstantExpression("OH", conn))).fetchall()
    assert {r[0] for r in rows} == {"CA", "NY"}


def test_filter_conjunction(conn):
    rel = _states(conn)
    state = ColumnExpression("state", conn)
    gender = ColumnExpression("gender", conn)
    cond = (state == ConstantExpression("OH", conn)) & (gender == ConstantExpression("M", conn))
    rows = rel.filter(cond).fetchall()
    assert len(rows) == 2
    assert all(r[0] == "OH" and r[1] == "M" for r in rows)
