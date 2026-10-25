import pandas as pd

from otterbrix import ColumnExpression, ConstantExpression


def test_inner_join_on_key(conn):
    left = conn.from_df(pd.DataFrame({"id": [1, 2, 3], "l": ["a", "b", "c"]}))
    right = conn.from_df(pd.DataFrame({"id": [2, 3, 4], "r": ["x", "y", "z"]}))

    cond = ColumnExpression("id", conn, "left") == ColumnExpression("id", conn, "right")
    joined = left.join(right, cond, "inner")

    rows = sorted(joined.fetchall())
    assert rows == [(2, "b", 2, "x"), (3, "c", 3, "y")]


def test_join_of_chained_relations_qualifies_columns_through_nameless_aggregates(conn):
    left = conn.from_df(pd.DataFrame({"id": [1, 2, 3, 4], "l": ["a", "b", "c", "d"]}))
    right = conn.from_df(pd.DataFrame({"id": [2, 3, 4, 5], "r": ["x", "y", "z", "w"]}))

    filtered_left = left.filter(ColumnExpression("id", conn) > ConstantExpression(1, conn))
    limited_right = right.filter(ColumnExpression("id", conn) < ConstantExpression(5, conn)).limit(10)
    cond = ColumnExpression("id", conn, "left") == ColumnExpression("id", conn, "right")

    rows = sorted(filtered_left.join(limited_right, cond, "inner").fetchall())
    assert rows == [(2, "b", 2, "x"), (3, "c", 3, "y"), (4, "d", 4, "z")]
