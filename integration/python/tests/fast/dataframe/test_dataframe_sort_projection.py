import pandas as pd

from otterbrix import ColumnExpression


def test_sort_ascending(conn):
    rel = conn.from_df(pd.DataFrame({"v": [3.0, 1.0, 2.0]}))
    rows = rel.sort(ColumnExpression("v", conn)).fetchall()
    assert [r[0] for r in rows] == [1.0, 2.0, 3.0]


def test_projection_selects_subset(conn):
    rel = conn.from_df(pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]}))
    projected = rel.select(ColumnExpression("a", conn), ColumnExpression("c", conn))
    assert projected.columns == ["a", "c"]
    assert projected.fetchall() == [(1, 5), (2, 6)]
