import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.types import Row
from otterbrix.experimental.spark.sql.functions import col


class TestDataFrameSelectProjection(object):
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (1, "Alice", 40),
        (3, "Carol", 30),
        (2, "Bob", 50),
    ]

    def test_select_preserves_duplicates(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select("id", "name")
        rows = projected.collect()

        assert len(rows) == 5, (
            f"Pure projection must preserve every input row, got {len(rows)}"
        )
        expected = sorted([(1, "Alice"), (2, "Bob"), (1, "Alice"), (3, "Carol"), (2, "Bob")])
        actual = sorted([(r.id, r.name) for r in rows])
        assert actual == expected

    def test_select_column_subset(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select("name")
        rows = projected.collect()
        assert len(rows) == 5
        assert sorted([r.name for r in rows]) == sorted(["Alice", "Bob", "Alice", "Carol", "Bob"])

    def test_select_via_col_expression(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select(col("id"), col("age"))
        rows = projected.collect()
        assert len(rows) == 5

    def test_select_duplicate_column(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        projected = df.select("id", "id")
        assert projected.columns == ["id", "id"]
        rows = projected.collect()
        assert len(rows) == 5
        for r in rows:
            assert r[0] == r[1]

    def test_select_empty_dataframe(self, spark):
        empty = spark.createDataFrame([], ["id", "name", "age"])
        rows = empty.select("id", "name").collect()
        assert rows == []

    def test_select_no_arguments_raises(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        with pytest.raises(AttributeError):
            df.select().collect()

    def test_select_nonexistent_column_raises(self, spark):
        df = spark.createDataFrame(self.data, ["id", "name", "age"])
        with pytest.raises(KeyError):
            df.select("does_not_exist")
