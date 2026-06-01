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
import otterbrix
import re


class TestDataFrameOrderBy(object):
    def test_order_by(self, spark):
        simpleData = [
            ("James", "Sales", "NY", 90000, 34, 10000),
            ("Michael", "Sales", "NY", 86000, 56, 20000),
            ("Robert", "Sales", "CA", 81000, 30, 23000),
            ("Maria", "Finance", "CA", 90000, 24, 23000),
            ("Raman", "Finance", "CA", 99000, 40, 24000),
            ("Scott", "Finance", "NY", 83000, 36, 19000),
            ("Jen", "Finance", "NY", 79000, 53, 15000),
            ("Jeff", "Marketing", "CA", 80000, 25, 18000),
            ("Kumar", "Marketing", "NY", 91000, 50, 21000),
        ]
        columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
        df = spark.createDataFrame(data=simpleData, schema=columns)

        df2 = df.sort("department", "state")
        res1 = df2.collect()
        for i in range(1, len(simpleData)):
            assert (
                    (res1[i - 1]["department"], res1[i - 1]["state"]) <=
                    (res1[i]["department"], res1[i]["state"])
            )

        df2 = df.sort(col("department"), col("state"))
        res2 = df2.collect()
        assert res2 == res1

        df2 = df.orderBy(col("department").asc(), col("state").asc())
        res3 = df2.collect()
        assert res3 == res1

        df2 = df.sort(df.department.asc(), df.state.desc())
        res1 = df2.collect()
        for i in range(1, len(simpleData)):
            assert (
                    (res1[i - 1]["department"] <= res1[i]["department"]) and
                    (res1[i - 1]["department"] != res1[i]["department"] or res1[i - 1]["state"] >= res1[i]["state"])
            )

        df2 = df.sort(col("department").asc(), col("state").desc())
        res2 = df2.collect()
        assert res2 == res1

        df2 = df.orderBy(col("department").asc(), col("state").desc())
        res3 = df2.collect()
        assert res3 == res1

    def test_order_by_empty_dataframe(self, spark):
        empty = spark.createDataFrame([], ["age", "name"])
        assert empty.orderBy("age").collect() == []

    def test_order_by_nullable_column(self, spark):
        # NULL values come from a left join with non-matching rows.
        left = spark.createDataFrame([(1, 10), (2, 99), (3, 20), (4, 99)], ["id", "k"])
        right = spark.createDataFrame([(10, "a"), (20, "b")], ["k", "v"])
        joined = left.join(right, "k", "left")
        vals = [r.v for r in joined.orderBy("v").collect()]
        assert vals == ["a", "b", None, None]

