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
from otterbrix.experimental.spark.sql import SparkSession
from otterbrix.experimental.spark.sql.functions import col, struct, when, lit, array_contains


@pytest.fixture
def dataframe_a(spark):
    emp = [
        (1, "Smith", -1, "2018", 10, "M", 3000),
        (2, "Rose", 1, "2010", 20, "M", 4000),
        (3, "Williams", 1, "2010", 10, "M", 1000),
        (4, "Jones", 2, "2005", 10, "F", 2000),
        (5, "Brown", 2, "2010", 40, "", -1),
        (6, "Brown", 2, "2010", 50, "", -1),
    ]
    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]
    dataframe = spark.createDataFrame(data=emp, schema=empColumns)
    yield dataframe


@pytest.fixture
def dataframe_b(spark):
    dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
    deptColumns = ["dept_name", "dept_id"]
    dataframe = spark.createDataFrame(data=dept, schema=deptColumns)
    yield dataframe


class TestDataFrameJoin(object):
    def test_inner_join(self, dataframe_a, dataframe_b):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, "inner")
        df = df.sort(*df.columns)
        res = df.collect()
        expected = [
            Row(
                emp_id=1,
                name='Smith',
                superior_emp_id=-1,
                year_joined='2018',
                emp_dept_id=10,
                gender='M',
                salary=3000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=2,
                name='Rose',
                superior_emp_id=1,
                year_joined='2010',
                emp_dept_id=20,
                gender='M',
                salary=4000,
                dept_name='Marketing',
                dept_id=20,
            ),
            Row(
                emp_id=3,
                name='Williams',
                superior_emp_id=1,
                year_joined='2010',
                emp_dept_id=10,
                gender='M',
                salary=1000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=4,
                name='Jones',
                superior_emp_id=2,
                year_joined='2005',
                emp_dept_id=10,
                gender='F',
                salary=2000,
                dept_name='Finance',
                dept_id=10,
            ),
            Row(
                emp_id=5,
                name='Brown',
                superior_emp_id=2,
                year_joined='2010',
                emp_dept_id=40,
                gender='',
                salary=-1,
                dept_name='IT',
                dept_id=40,
            ),
        ]
        assert sorted(res) == sorted(expected)

    @pytest.mark.parametrize('how', ['outer', 'fullouter', 'full', 'full_outer'])
    def test_outer_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
        res1 = df.collect()
        assert sorted(res1, key=lambda x: x.emp_id or 0) == sorted(
            [
                Row(
                    emp_id=1,
                    name='Smith',
                    superior_emp_id=-1,
                    year_joined='2018',
                    emp_dept_id=10,
                    gender='M',
                    salary=3000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=2,
                    name='Rose',
                    superior_emp_id=1,
                    year_joined='2010',
                    emp_dept_id=20,
                    gender='M',
                    salary=4000,
                    dept_name='Marketing',
                    dept_id=20,
                ),
                Row(
                    emp_id=3,
                    name='Williams',
                    superior_emp_id=1,
                    year_joined='2010',
                    emp_dept_id=10,
                    gender='M',
                    salary=1000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=4,
                    name='Jones',
                    superior_emp_id=2,
                    year_joined='2005',
                    emp_dept_id=10,
                    gender='F',
                    salary=2000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=5,
                    name='Brown',
                    superior_emp_id=2,
                    year_joined='2010',
                    emp_dept_id=40,
                    gender="",
                    salary=-1,
                    dept_name='IT',
                    dept_id=40,
                ),
                Row(
                    emp_id=6,
                    name='Brown',
                    superior_emp_id=2,
                    year_joined='2010',
                    emp_dept_id=50,
                    gender="",
                    salary=-1,
                    dept_name=None,
                    dept_id=None,
                ),
                Row(
                    emp_id=None,
                    name=None,
                    superior_emp_id=None,
                    year_joined=None,
                    emp_dept_id=None,
                    gender=None,
                    salary=None,
                    dept_name='Sales',
                    dept_id=30,
                ),
            ],
            key=lambda x: x.emp_id or 0,
        )

    @pytest.mark.parametrize('how', ['right', 'rightouter', 'right_outer'])
    def test_right_join(self, dataframe_a, dataframe_b, how):
        df = dataframe_a.join(dataframe_b, dataframe_a.emp_dept_id == dataframe_b.dept_id, how)
        df = df.sort(*df.columns)
        res = df.collect()
        assert sorted(res, key=lambda x: x.emp_id or 0) == sorted(
            [
                Row(
                    emp_id=1,
                    name='Smith',
                    superior_emp_id=-1,
                    year_joined='2018',
                    emp_dept_id=10,
                    gender='M',
                    salary=3000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=2,
                    name='Rose',
                    superior_emp_id=1,
                    year_joined='2010',
                    emp_dept_id=20,
                    gender='M',
                    salary=4000,
                    dept_name='Marketing',
                    dept_id=20,
                ),
                Row(
                    emp_id=3,
                    name='Williams',
                    superior_emp_id=1,
                    year_joined='2010',
                    emp_dept_id=10,
                    gender='M',
                    salary=1000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=4,
                    name='Jones',
                    superior_emp_id=2,
                    year_joined='2005',
                    emp_dept_id=10,
                    gender='F',
                    salary=2000,
                    dept_name='Finance',
                    dept_id=10,
                ),
                Row(
                    emp_id=5,
                    name='Brown',
                    superior_emp_id=2,
                    year_joined='2010',
                    emp_dept_id=40,
                    gender='',
                    salary=-1,
                    dept_name='IT',
                    dept_id=40,
                ),
                Row(
                    emp_id=None,
                    name=None,
                    superior_emp_id=None,
                    year_joined=None,
                    emp_dept_id=None,
                    gender=None,
                    salary=None,
                    dept_name='Sales',
                    dept_id=30,
                ),
            ],
            key=lambda x: x.emp_id or 0,
        )


    def test_inner_join_by_column_name(self):
        """Inner equi-join by column name — exercises hash join path."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "c", 10), (4, "d", 50)],
            schema=["id", "name", "dept_id"],
        )
        right = spark.createDataFrame(
            [(10, "Eng"), (20, "Sales"), (30, "HR")],
            schema=["dept_id", "dept_name"],
        )
        result = left.join(right, "dept_id", "inner").collect()
        assert len(result) == 3  # id 1,3 -> Eng, id 2 -> Sales; id 4 no match

    def test_left_join_by_column_name(self):
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, 20), (3, 50)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "Eng"), (20, "Sales"), (30, "HR")], schema=["dept_id", "dept_name"]
        )
        result = left.join(right, "dept_id", "left").collect()
        assert len(result) == 3  # all left rows; id=3 has None dept_name

    def test_right_join_by_column_name(self):
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, 20)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "Eng"), (20, "Sales"), (30, "HR")], schema=["dept_id", "dept_name"]
        )
        result = left.join(right, "dept_id", "right").collect()
        assert len(result) == 3  # all right rows; dept_id=30 has None id

    def test_full_join_by_column_name(self):
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, 50)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "Eng"), (30, "HR")], schema=["dept_id", "dept_name"]
        )
        result = left.join(right, "dept_id", "full").collect()
        assert len(result) == 3  # 1 match + 1 left-only + 1 right-only

    def test_join_duplicate_keys(self):
        """Multiple matches: 2 left x 3 right with same key = 6 result rows."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, 10)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "a"), (10, "b"), (10, "c")], schema=["dept_id", "label"]
        )
        result = left.join(right, "dept_id", "inner").collect()
        assert len(result) == 6  # 2 x 3

    def test_join_null_keys_never_match(self):
        """NULL keys must not match each other (SQL semantics)."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, -1)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "Eng"), (99, "Unknown")], schema=["dept_id", "dept_name"]
        )
        result = left.join(right, "dept_id", "inner").collect()
        assert len(result) == 1  # only id=1 matches dept_id=10

    def test_join_no_match_left_join(self):
        """Left join: non-matching left rows appear with NULL right side."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10), (2, 99)], schema=["id", "dept_id"]
        )
        right = spark.createDataFrame(
            [(10, "Eng")], schema=["dept_id", "dept_name"]
        )
        result = left.join(right, "dept_id", "left").collect()
        assert len(result) == 2  # id=1 matches, id=2 gets NULL right side

    def test_join_no_matching_keys(self):
        """When no keys match, inner join returns empty result."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame([(1, 100), (2, 200)], schema=["id", "dept_id"])
        right = spark.createDataFrame([(10, "Eng"), (20, "Sales")], schema=["dept_id", "dept_name"])
        result = left.join(right, "dept_id", "inner").collect()
        assert len(result) == 0

    def test_join_multi_key(self):
        """Equi-join on two columns."""
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        left = spark.createDataFrame(
            [(1, 10, "A"), (2, 10, "B"), (3, 20, "A")],
            schema=["id", "dept_id", "region"],
        )
        right = spark.createDataFrame(
            [(10, "A", "x"), (10, "B", "y"), (20, "C", "z")],
            schema=["dept_id", "region", "label"],
        )
        result = left.join(right, ["dept_id", "region"], "inner").collect()
        assert len(result) == 2  # (10,A) and (10,B) match

    def test_cross_join(self, spark):
        data1 = [(1, "Carol"), (2, "Alice"), (3, "Dave")]
        data2 = [(4, "A"), (5, "B")]
        df1 = spark.createDataFrame(data1, ["age", "name"])
        df2 = spark.createDataFrame(data2, ["id", "rank"])

        df = df1.crossJoin(df2)

        res = df.orderBy("rank", "age").collect()

        assert sorted(res) == sorted(
            [
                Row(age=1, name="Carol", id=4, rank="A"),
                Row(age=2, name="Alice", id=4, rank="A"),
                Row(age=3, name="Dave", id=4, rank="A"),
                Row(age=1, name="Carol", id=5, rank="B"),
                Row(age=2, name="Alice", id=5, rank="B"),
                Row(age=3, name="Dave", id=5, rank="B"),
            ]
        )

    def test_join_with_empty_side(self, spark):
        non_empty = spark.createDataFrame([(1, 10), (2, 20)], ["id", "k"])
        empty = spark.createDataFrame([], ["k", "v"])
        assert non_empty.join(empty, "k", "inner").collect() == []
        assert empty.join(non_empty, "k", "inner").collect() == []

    def test_self_join(self, spark):
        df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "k"])
        res = df.join(df, "k", "inner").collect()
        assert len(res) == 2
        assert sorted((r.id, r.k) for r in res) == [(1, 10), (2, 20)]
