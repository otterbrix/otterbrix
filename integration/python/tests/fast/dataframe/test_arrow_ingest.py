import pytest

import otterbrix


def test_polars_dataframe_round_trips_through_arrow(conn):
    pl = pytest.importorskip("polars")

    pldf = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "score": [1.5, 2.5, 3.5, 4.5],
        }
    )

    rel = conn.from_df(pldf)

    assert rel.columns == ["id", "name", "score"]
    result = rel.df()
    assert list(result["id"]) == [1, 2, 3, 4]
    assert list(result["name"]) == ["a", "b", "c", "d"]
    assert list(result["score"]) == [1.5, 2.5, 3.5, 4.5]


def test_polars_arrow_path_supports_filter(conn):
    pl = pytest.importorskip("polars")

    pldf = pl.DataFrame({"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50]})

    rel = conn.from_df(pldf)
    id_col = otterbrix.ColumnExpression("id", conn)
    threshold = otterbrix.ConstantExpression(3, conn)
    filtered = rel.filter(id_col > threshold)

    result = filtered.df()
    assert sorted(result["id"]) == [4, 5]


def test_pyarrow_backed_pandas_round_trips_through_arrow(conn):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    # A pyarrow-backed pandas DataFrame (every column is an ArrowDtype) must route through the
    # core arrow path (components::vector::arrow::data_chunk_from_arrow), not the numpy scanner.
    pdf = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["x", "y", "z"]}
    ).convert_dtypes(dtype_backend="pyarrow")

    rel = conn.from_df(pdf)

    assert rel.columns == ["id", "name"]
    result = rel.df()
    assert list(result["id"]) == [1, 2, 3]
    assert list(result["name"]) == ["x", "y", "z"]


def test_pyarrow_table_round_trips_through_arrow(conn):
    pa = pytest.importorskip("pyarrow")

    table = pa.table({"id": [7, 8, 9], "label": ["p", "q", "r"]})

    rel = conn.from_df(table)

    assert rel.columns == ["id", "label"]
    result = rel.df()
    assert list(result["id"]) == [7, 8, 9]
    assert list(result["label"]) == ["p", "q", "r"]
