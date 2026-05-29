import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

import otterbrix.experimental.spark.errors
from otterbrix.experimental.spark.sql.types import Row
from otterbrix.experimental.spark.errors import PySparkTypeError, PySparkValueError
from otterbrix.experimental.spark.sql.functions import col


class TestDataFrameSort(object):
    data = [(56, "Carol"), (20, "Alice"), (3, "Dave"), (3, "Anna"), (1, "Ben")]

    def test_sort_ascending(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])
        expected = [ 
            Row(age=1, name="Ben"),
            Row(age=3, name="Anna"),
            Row(age=3, name="Dave"),
            Row(age=20, name="Alice"),
            Row(age=56, name="Carol"),
        ]   

        df = df.sort(["age", "name"])
        assert df.collect() == expected

        df = df.sort("age", "name")
        assert df.collect() == expected


    def test_sort_ascending_by_name(self, spark):
        # Sorts ascending by name, then age (kept as a separate ascending case
        # because the primary key differs from test_sort_ascending).
        df = spark.createDataFrame(self.data, ["age", "name"])
        expected = [
            Row(age=20, name="Alice"),
            Row(age=3, name="Anna"),
            Row(age=1, name="Ben"),
            Row(age=56, name="Carol"),
            Row(age=3, name="Dave"),
        ]

        df = df.sort(["name", "age"])
        assert df.collect() == expected

        df = df.sort("name", "age")
        assert df.collect() == expected


    def test_sort_descending(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])
        # age desc, then name desc (so the age==3 tie is deterministic)
        expected = [
            Row(age=56, name="Carol"),
            Row(age=20, name="Alice"),
            Row(age=3, name="Dave"),
            Row(age=3, name="Anna"),
            Row(age=1, name="Ben"),
        ]

        # via the global ascending=False flag
        assert df.sort("age", "name", ascending=False).collect() == expected

        # via explicit .desc() on each column
        assert df.sort(col("age").desc(), col("name").desc()).collect() == expected


    def test_sort_mixed_directions(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])
        # age asc, name desc
        expected = [
            Row(age=1, name="Ben"),
            Row(age=3, name="Dave"),
            Row(age=3, name="Anna"),
            Row(age=20, name="Alice"),
            Row(age=56, name="Carol"),
        ]

        # via the per-column ascending list
        assert df.sort(["age", "name"], ascending=[True, False]).collect() == expected

        # via explicit .asc()/.desc()
        assert df.sort(col("age").asc(), col("name").desc()).collect() == expected


    def test_sort_negative_ordinal(self, spark):
        # negative ordinal means sort by that column descending (1-based)
        df = spark.createDataFrame(self.data, ["age", "name"])
        ages = [r.age for r in df.sort(-1).collect()]
        assert ages == sorted(ages, reverse=True)


    def test_sort_wrong_asc_params(self, spark):
        df = spark.createDataFrame(self.data, ["age", "name"])

        with pytest.raises(PySparkTypeError):
            df = df.sort(["age"], ascending="no")

    def test_sort_empty_dataframe(self, spark):
        empty = spark.createDataFrame([], ["age", "name"])
        assert empty.sort("age").collect() == []

    def test_sort_by_nullable_column(self, spark):
        # NULL values come from a left join with non-matching rows;
        # createDataFrame does not accept None directly.
        left = spark.createDataFrame([(1, 10), (2, 99), (3, 20), (4, 99)], ["id", "k"])
        right = spark.createDataFrame([(10, "a"), (20, "b")], ["k", "v"])
        joined = left.join(right, "k", "left")
        vals = [r.v for r in joined.sort("v").collect()]
        assert vals == ["a", "b", None, None]

