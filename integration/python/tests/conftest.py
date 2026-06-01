import pytest


@pytest.fixture(scope="session")
def spark():
    from otterbrix.experimental.spark.sql import SparkSession

    # Create a SparkSession for the tests
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-spark") \
        .getOrCreate()
    
    yield spark  # provide the session to the tests
    
    spark.stop()  # stop after all tests