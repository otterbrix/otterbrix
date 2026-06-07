import pandas as pd

from otterbrix import ColumnExpression, ConstantExpression


def _rows(conn):
    df = pd.DataFrame(
        {
            "id": list(range(1, 13)),
            "grp": ["A" if i % 2 else "B" for i in range(1, 13)],
            "val": [float(i) for i in range(1, 13)],
        }
    )
    return conn.from_df(df)


def test_limit_truncates(conn):
    rel = _rows(conn)
    assert len(rel.limit(3).fetchall()) == 3


def test_limit_larger_than_rows(conn):
    rel = _rows(conn)
    assert len(rel.limit(100).fetchall()) == 12


def test_limit_after_sort_is_deterministic(conn):
    rel = _rows(conn)
    rows = rel.sort(ColumnExpression("val", conn)).limit(3).fetchall()
    assert [r[-1] for r in rows] == [1.0, 2.0, 3.0]


def test_limit_after_filter(conn):
    rel = _rows(conn)
    val = ColumnExpression("val", conn)
    rows = rel.filter(val > ConstantExpression(5, conn)).limit(2).fetchall()
    assert len(rows) == 2
