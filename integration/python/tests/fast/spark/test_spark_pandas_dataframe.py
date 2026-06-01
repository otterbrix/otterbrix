import pytest

_ = pytest.importorskip("otterbrix.experimental.spark")
pd = pytest.importorskip("pandas")

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
from pandas.testing import assert_frame_equal


@pytest.fixture
def pandasDF(spark):
    data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54], ['Ann', 34]]
    # Create the pandas DataFrame
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    yield df


class TestPandasDataFrame(object):
    def test_pd_conversion_basic(self, spark, pandasDF):
        sparkDF = spark.createDataFrame(pandasDF)
        res = sparkDF.collect()
        expected = [
            Row(Name='Scott', Age=50),
            Row(Name='Jeff', Age=45),
            Row(Name='Thomas', Age=54),
            Row(Name='Ann', Age=34),
        ]
        assert res == expected

    def test_pd_conversion_schema(self, spark, pandasDF):
        mySchema = StructType([StructField("First Name", StringType(), True), StructField("Age", IntegerType(), True)])
        sparkDF = spark.createDataFrame(pandasDF, schema=mySchema)
        res = sparkDF.collect()
        expected = "[Row(First Name='Scott', Age=50), Row(First Name='Jeff', Age=45), Row(First Name='Thomas', Age=54), Row(First Name='Ann', Age=34)]"
        assert str(res) == expected

    def test_spark_to_pandas_dataframe(self, spark, pandasDF):
        sparkDF = spark.createDataFrame(pandasDF)
        res = sparkDF.toPandas()
        assert_frame_equal(res, pandasDF)

    def test_pd_conversion_schema_applies_cast(self, spark, pandasDF):
        # Declared types must take effect: Age is int64 in pandas, declared as
        # StringType — values must come back as strings. The whitespace in
        # "First Name" also exercises identifier quoting in the cast path.
        mySchema = StructType([
            StructField("First Name", StringType(), True),
            StructField("Age", StringType(), True),
        ])
        sparkDF = spark.createDataFrame(pandasDF, schema=mySchema)
        assert sparkDF.schema["Age"].dataType == StringType()
        res = sparkDF.collect()
        ages = [row["Age"] for row in res]
        assert ages == ['50', '45', '54', '34']
