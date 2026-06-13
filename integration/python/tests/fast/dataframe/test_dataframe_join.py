import pandas as pd

from otterbrix import ColumnExpression


def test_inner_join_on_key(conn):
    left = conn.from_df(pd.DataFrame({"id": [1, 2, 3], "l": ["a", "b", "c"]}))
    right = conn.from_df(pd.DataFrame({"id": [2, 3, 4], "r": ["x", "y", "z"]}))

    cond = ColumnExpression("id", conn, "left") == ColumnExpression("id", conn, "right")
    joined = left.join(right, cond, "inner")

    rows = sorted(joined.fetchall())
    assert rows == [(2, "b", 2, "x"), (3, "c", 3, "y")]
