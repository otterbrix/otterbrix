import pytest

pl = pytest.importorskip("polars")

import otterbrix


def test_from_df_accepts_polars_dataframe(conn):
    pldf = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    rel = conn.from_df(pldf)

    assert rel.columns == ["id", "name"]
    result = rel.df()
    assert list(result["id"]) == [1, 2, 3]
    assert list(result["name"]) == ["a", "b", "c"]


def test_polars_dataframe_supports_filter(conn):
    pldf = pl.DataFrame({"id": [1, 2, 3, 4], "v": [10, 20, 30, 40]})

    rel = conn.from_df(pldf)
    id_col = otterbrix.ColumnExpression("id", conn)
    threshold = otterbrix.ConstantExpression(2, conn)
    filtered = rel.filter(id_col > threshold)

    result = filtered.df()
    assert sorted(result["id"]) == [3, 4]
