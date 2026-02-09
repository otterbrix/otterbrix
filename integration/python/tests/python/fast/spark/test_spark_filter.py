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
import duckdb
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
