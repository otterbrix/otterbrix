import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.functions import col


def _df(spark):
    data = [(i, "A" if i % 2 else "B", float(i)) for i in range(1, 13)]
    return spark.createDataFrame(data=data, schema=["id", "grp", "val"])


class TestDataFrameLimit(object):
    def test_limit_truncates(self, spark):
        df = _df(spark)
        assert len(df.limit(3).collect()) == 3

    def test_limit_zero(self, spark):
        df = _df(spark)
        assert df.limit(0).collect() == []

    def test_limit_larger_than_rows(self, spark):
        df = _df(spark)
        assert len(df.limit(100).collect()) == 12

    def test_head_take_first(self, spark):
        df = _df(spark)
        assert len(df.head(3)) == 3
        assert len(df.take(4)) == 4
        assert df.first() is not None

    def test_limit_after_sort_is_deterministic(self, spark):
        df = _df(spark)
        rows = df.sort("val").limit(3).collect()
        assert [r.val for r in rows] == [1.0, 2.0, 3.0]

    def test_limit_after_filter(self, spark):
        df = _df(spark)
        rows = df.filter(col("val") > 5).limit(2).collect()
        assert len(rows) == 2
        for r in rows:
            assert r.val > 5

    def test_limit_after_groupby(self, spark):
        df = _df(spark)
        rows = df.groupBy("grp").count().limit(1).collect()
        assert len(rows) == 1

    def test_limit_matches_unoptimized_and_optimized(self, spark):
        df = _df(spark)
        chain = df.sort("val").limit(5)
        a = sorted(tuple(r) for r in chain.collect(optimize=False))
        b = sorted(tuple(r) for r in chain.collect(optimize=True))
        assert a == b
        assert len(a) == 5
