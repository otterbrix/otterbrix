import pytest
from otterbrix.experimental.spark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Создание SparkSession для тестов
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-spark") \
        .getOrCreate()
    
    yield spark  # предоставляем сессию тестам
    
    spark.stop()  # останавливаем после всех тестов