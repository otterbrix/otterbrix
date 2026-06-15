"""Locks the dtype behavior of the Arrow-only ingest path (pandas>=2.2 __arrow_c_stream__).

After dropping the bespoke numpy column scanner, every pandas/numpy frame is ingested through
pandas' from_pandas export. These tests pin the edge-case semantics described in the migration:
NaN->NULL is preserved, object columns keep MAP / lenient-STRING handling via the prepare shim,
and categoricals (previously rejected) now ingest.
"""

import numpy as np
import pandas as pd


def test_float_nan_becomes_null(conn):
    # from_pandas maps numpy float NaN to an Arrow null, matching the old scanner's isnan->null.
    rel = conn.from_df(pd.DataFrame({"x": [1.0, np.nan, 3.0]}))
    assert rel.columns == ["x"]
    assert rel.fetchall() == [(1.0,), (None,), (3.0,)]


def test_nullable_int_na_becomes_null(conn):
    rel = conn.from_df(pd.DataFrame({"x": pd.array([1, None, 3], dtype="Int64")}))
    assert rel.columns == ["x"]
    assert rel.fetchall() == [(1,), (None,), (3,)]


def test_datetime_with_nat(conn):
    rel = conn.from_df(pd.DataFrame({"t": pd.to_datetime(["2021-01-01", None, "2021-01-03"])}))
    assert rel.columns == ["t"]
    rows = rel.fetchall()
    assert len(rows) == 3
    assert rows[1][0] is None  # NaT -> NULL


def test_mixed_object_column_falls_back_to_string(conn):
    # Heterogeneous object column: pyarrow cannot infer a clean type, so the prepare shim
    # stringifies it (the lenient STRING fallback the old pandas_analyzer applied).
    rel = conn.from_df(pd.DataFrame({"x": [1, "two", 3.0]}))
    assert rel.columns == ["x"]
    assert rel.fetchall() == [("1",), ("two",), ("3.0",)]


def test_categorical_ingests(conn):
    # The old numpy scanner rejected Categoricals; the Arrow path ingests them as dictionary arrays.
    rel = conn.from_df(pd.DataFrame({"c": pd.Categorical(["a", "b", "a"])}))
    assert rel.columns == ["c"]
    assert rel.fetchall() == [("a",), ("b",), ("a",)]


def test_map_format_dict_column_ingests(conn):
    # {"key": [...], "value": [...]} object columns are rebuilt as pyarrow MAP arrays by the shim.
    df = pd.DataFrame(
        {"m": [{"key": ["a", "b"], "value": [1, 2]}, {"key": ["c"], "value": [3]}]}
    )
    rel = conn.from_df(df)
    assert rel.columns == ["m"]
    assert len(rel.fetchall()) == 2


def test_generic_dict_column_ingests_as_struct(conn):
    df = pd.DataFrame({"s": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    rel = conn.from_df(df)
    assert rel.columns == ["s"]
    assert len(rel.fetchall()) == 2


def test_list_column_ingests(conn):
    rel = conn.from_df(pd.DataFrame({"l": [[1, 2], [3, 4]]}))
    assert rel.columns == ["l"]
    assert len(rel.fetchall()) == 2


def test_duplicate_column_names_are_deduplicated(conn):
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["x", "x"])
    rel = conn.from_df(df)
    assert len(rel.columns) == 2
    assert len(set(rel.columns)) == 2  # duplicate label was renamed


def test_numpy_2d_ndarray_rows_become_columns(conn):
    # NDARRAY2D iterates rows into column0..N (existing row-as-column behavior, kept verbatim).
    rel = conn.from_df(np.array([[1, 2, 3], [4, 5, 6]]))
    assert rel.columns == ["column0", "column1"]
    assert rel.fetchall() == [(1, 4), (2, 5), (3, 6)]


def test_pandas_index_is_ignored(conn):
    # A non-default index must NOT become a column: the old numpy/pandas scanner ingested
    # only df.columns, but the Arrow C-stream export would otherwise turn a named index into
    # a column. The prepare shim drops it via reset_index(drop=True). (PR #520 review.)
    df = pd.DataFrame({"x": [10, 20, 30]}, index=["alice", "bob", "carol"])
    rel = conn.from_df(df)
    assert rel.columns == ["x"]
    assert rel.fetchall() == [(10,), (20,), (30,)]
