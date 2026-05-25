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
from otterbrix.experimental.spark.sql.functions import col, lit
from otterbrix.experimental.spark.sql.functions import (
    sum,
    avg,
    max,
    min,
    mean,
    count
)


class TestDataFrameGroupBy(object):
    def test_group_by(self, spark):
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

        schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
        df = spark.createDataFrame(data=simpleData, schema=schema)

        df2 = df.groupBy("department").sum("salary").sort("department")
        res = df2.collect()
        expected = "[Row(department='Finance', sum(salary)=351000), Row(department='Marketing', sum(salary)=171000), Row(department='Sales', sum(salary)=257000)]"
        assert str(res) == expected

        df2 = df.groupBy("department").count().sort("department")
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', count=4), Row(department='Marketing', count=2), Row(department='Sales', count=3)]"
        )

        df2 = df.groupBy("department").min("salary").sort("department")
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', min(salary)=79000), Row(department='Marketing', min(salary)=80000), Row(department='Sales', min(salary)=81000)]"
        )

        df2 = df.groupBy("department").max("salary").sort("department")
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', max(salary)=99000), Row(department='Marketing', max(salary)=91000), Row(department='Sales', max(salary)=90000)]"
        )

        df2 = df.groupBy("department").avg("salary").sort("department")
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', avg(salary)=87750.0), Row(department='Marketing', avg(salary)=85500.0), Row(department='Sales', avg(salary)=85666.66666666667)]"
        )

        df2 = df.groupBy("department").mean("salary").sort("department")
        res = df2.collect()
        expected_res_str = "[Row(department='Finance', avg(salary)=87750.0), Row(department='Marketing', avg(salary)=85500.0), Row(department='Sales', avg(salary)=85666.66666666667)]"
        assert str(res) == expected_res_str

        df2 = df.groupBy("department", "state").sum("salary", "bonus").sort("department", "state")
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', state='CA', sum(salary)=189000, sum(bonus)=47000), Row(department='Finance', state='NY', sum(salary)=162000, sum(bonus)=34000), Row(department='Marketing', state='CA', sum(salary)=80000, sum(bonus)=18000), Row(department='Marketing', state='NY', sum(salary)=91000, sum(bonus)=21000), Row(department='Sales', state='CA', sum(salary)=81000, sum(bonus)=23000), Row(department='Sales', state='NY', sum(salary)=176000, sum(bonus)=30000)]"
        )

        df2 = (
            df.groupBy("department")
            .agg(
                sum("salary").alias("sum_salary"),
                avg("salary").alias("avg_salary"),
                sum("bonus").alias("sum_bonus"),
                max("bonus").alias("max_bonus"),
            )
            .sort("department")
        )
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', sum_salary=351000, avg_salary=87750.0, sum_bonus=81000, max_bonus=24000), Row(department='Marketing', sum_salary=171000, avg_salary=85500.0, sum_bonus=39000, max_bonus=21000), Row(department='Sales', sum_salary=257000, avg_salary=85666.66666666667, sum_bonus=53000, max_bonus=23000)]"
        )

        df2 = (
            df.groupBy("department")
            .agg(
                sum("salary").alias("sum_salary"),
                avg("salary").alias("avg_salary"),
                sum("bonus").alias("sum_bonus"),
                max("bonus").alias("max_bonus"),
            )
            .where(col("sum_bonus") >= 50000)
            .sort("department")
        )
        res = df2.collect()
        assert (
            str(res)
            == "[Row(department='Finance', sum_salary=351000, avg_salary=87750.0, sum_bonus=81000, max_bonus=24000), Row(department='Sales', sum_salary=257000, avg_salary=85666.66666666667, sum_bonus=53000, max_bonus=23000)]"
        )

    def test_group_by_empty(self, spark):
        df = spark.createDataFrame(
            [(2, 1.0, "1"), (2, 2.0, "2"), (2, 3.0, "3"), (5, 4.0, "4")], schema=["age", "extra", "name"]
        )

        res = df.groupBy().avg().collect()
        assert str(res) == "[Row(avg(age)=2.75, avg(extra)=2.5)]"

        res = df.groupBy(["name", "age"]).count().sort("name").collect()
        assert (
            str(res)
            == "[Row(name='1', age=2, count=1), Row(name='2', age=2, count=1), Row(name='3', age=2, count=1), Row(name='4', age=5, count=1)]"
        )

        res = df.groupBy("name").count().columns
        assert res == ['name', 'count']

    def test_group_by_empty_dataframe(self, spark):
        empty = spark.createDataFrame([], ["dep", "salary"])
        res = empty.groupBy("dep").count().collect()
        assert res == []

    def test_group_by_single_row_groups(self, spark):
        df = spark.createDataFrame([("a", 10), ("b", 20), ("c", 30)], ["name", "val"])
        res = df.groupBy("name").sum("val").collect()
        sums = {r.name: r[1] for r in res}
        assert sums == {"a": 10, "b": 20, "c": 30}

    def test_group_by_null_in_key(self, spark):
        # NULL group keys arise from a left join with non-matching rows;
        # createDataFrame does not accept None directly.
        left = spark.createDataFrame([(1, 10), (2, 99), (3, 10)], ["id", "k"])
        right = spark.createDataFrame([(10, 100)], ["k", "val"])
        joined = left.join(right, "k", "left")
        counts = {r.k: r[1] for r in joined.groupBy("k").count().collect()}
        assert counts == {10: 2, None: 1}

    def test_group_by_aggregate_over_nulls(self, spark):
        # k=99 has no match, so its joined `val` is NULL for the whole group.
        left = spark.createDataFrame([(1, 10), (2, 99), (3, 10)], ["id", "k"])
        right = spark.createDataFrame([(10, 100)], ["k", "val"])
        joined = left.join(right, "k", "left")
        sums = {r.k: r[1] for r in joined.groupBy("k").sum("val").collect()}
        assert sums == {10: 200, None: None}
