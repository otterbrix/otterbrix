import numpy as np
import pandas as pd


def test_pandas_dataframe_round_trips(conn):
    rel = conn.from_df(pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    assert rel.columns == ["id", "name"]
    result = rel.df()
    assert list(result["id"]) == [1, 2, 3]
    assert list(result["name"]) == ["a", "b", "c"]


def test_numpy_dict_of_arrays(conn):
    rel = conn.from_df({"a": np.array([1, 2, 3]), "b": np.array([4, 5, 6])})
    assert rel.columns == ["a", "b"]
    assert rel.fetchall() == [(1, 4), (2, 5), (3, 6)]


def test_numpy_1d_ndarray(conn):
    rel = conn.from_df(np.array([10, 20, 30]))
    assert rel.columns == ["column0"]
    assert rel.fetchall() == [(10,), (20,), (30,)]
