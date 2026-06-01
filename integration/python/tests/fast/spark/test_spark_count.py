import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix.experimental.spark.sql.functions import col


def _df(spark):
    data = [(i, "A" if i % 2 else "B", float(i)) for i in range(1, 13)]
    return spark.createDataFrame(data=data, schema=["id", "grp", "val"])


class TestDataFrameCount(object):
    def test_count_returns_total_rows(self, spark):
        df = _df(spark)
        assert df.count() == 12

    def test_count_after_filter(self, spark):
        df = _df(spark)
        assert df.filter(col("val") > 6).count() == 6

    def test_count_on_empty_result_is_zero(self, spark):
        df = _df(spark)
        assert df.filter(col("val") > 1000).count() == 0

    def test_count_returns_int(self, spark):
        df = _df(spark)
        assert isinstance(df.count(), int)
