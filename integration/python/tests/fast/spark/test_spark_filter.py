import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.types import (
    LongType,
    StructType,
    BooleanType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    Row,
    ArrayType,
    MapType,
)
from otterbrix.experimental.spark.sql.functions import col
from otterbrix.experimental.spark.errors import PySparkTypeError
import re


class TestDataFrameFilter(object):
    def test_dataframe_filter(self, spark):
        data = [
            ("OH", "M"),
            ("CA", "F"),
            ("OH", "F"),
            ("NY", "M"),
            ("NY", "M"),
            ("OH", "M"),
        ]

        schema = ["state", "gender"]

        df = spark.createDataFrame(data=data, schema=schema)

        # --- Tests ---

        # Using equals condition
        df2 = df.filter(df.state == "OH")
        res = df2.collect()
        assert res[0].state == 'OH'

        # not equals condition
        df2 = df.filter(df.state != "OH")
        df2 = df.filter(~(df.state == "OH"))
        res = df2.collect()
        for item in res:
            assert item.state == 'NY' or item.state == 'CA'
        
        df2 = df.filter(col("state") == "OH")
        res = df2.collect()
        assert res[0].state == 'OH'

        # Filter multiple condition
        df2 = df.filter((df.state == "OH") & (df.gender == "M"))
        res = df2.collect()
        assert len(res) == 2
        for item in res:
            assert item.gender == 'M' and item.state == 'OH'

        # contains
        df2 = df.filter(df.state.contains("H"))
        res = df2.collect()
        for item in res:
            assert item.state == 'OH'

        data2 = [(2, "Michael Rose"), (3, "Robert Williams"), (4, "Rames Rose"), (5, "Rames rose")]
        df2 = spark.createDataFrame(data=data2, schema=["id", "name"])

        # rlike - SQL RLIKE pattern (LIKE with Regex)
        df3 = df2.filter(df2.name.rlike("rose$"))
        res = df3.collect()
        assert res == [Row(id=5, name='Rames rose')]


        df3 = df2.filter(df2.name.startswith("R"))
        res = df3.collect()
        assert res == [(3, "Robert Williams"), (4, "Rames Rose"), (5, "Rames rose")]


        df3 = df2.filter(df2.name.endswith("ose"))
        res = df3.collect()
        assert res == [(2, "Michael Rose"), (4, "Rames Rose"), (5, "Rames rose")]

        
        df3 = df2.filter(df2.name.rlike("rose$"))
        res = df3.collect()
        assert res == [Row(id=5, name='Rames rose')]

    def test_invalid_condition_type(self, spark):
        df = spark.createDataFrame([(1, "A")], ["A", "B"])

        with pytest.raises(PySparkTypeError):
            df = df.filter(dict(a=1))

    def test_filter_matches_no_rows(self, spark):
        df = spark.createDataFrame([("OH", "M"), ("CA", "F")], ["state", "gender"])
        res = df.filter(df.state == "ZZ").collect()
        assert res == []

    def test_filter_matches_all_rows(self, spark):
        data = [("OH", "M"), ("CA", "F"), ("NY", "M")]
        df = spark.createDataFrame(data, ["state", "gender"])
        res = df.filter(df.state != "ZZ").collect()
        assert len(res) == len(data)

    def test_filter_or_condition(self, spark):
        data = [("OH", "M"), ("CA", "F"), ("OH", "F"), ("NY", "M")]
        df = spark.createDataFrame(data, ["state", "gender"])
        res = df.filter((df.state == "OH") | (df.state == "NY")).collect()
        assert sorted(r.state for r in res) == ["NY", "OH", "OH"]
        for r in res:
            assert r.state in ("OH", "NY")

    def test_filter_chained_equals_conjunction(self, spark):
        data = [("OH", "M"), ("CA", "F"), ("OH", "F"), ("NY", "M"), ("OH", "M")]
        df = spark.createDataFrame(data, ["state", "gender"])
        chained = df.filter(df.state == "OH").filter(df.gender == "M").collect()
        conjunction = df.filter((df.state == "OH") & (df.gender == "M")).collect()
        assert sorted(chained) == sorted(conjunction)
        assert len(chained) == 2

    def test_filter_empty_dataframe(self, spark):
        empty = spark.createDataFrame([], ["state", "gender"])
        res = empty.filter(col("state") == "OH").collect()
        assert res == []

    def test_filter_equality_excludes_nulls(self, spark):
        # NULL values arise from a left join with non-matching rows;
        # createDataFrame does not accept None directly.
        left = spark.createDataFrame([(1, 10), (2, 99), (3, 10)], ["id", "k"])
        right = spark.createDataFrame([(10, "x")], ["k", "v"])
        joined = left.join(right, "k", "left")
        res = joined.filter(joined.v == "x").collect()
        assert sorted(r.id for r in res) == [1, 3]
