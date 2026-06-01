import pytest

spark_namespace = pytest.importorskip("otterbrix.experimental.spark")

from otterbrix import (
    decimal_type,
    list_type,
    map_type,
    struct_type
)

from otterbrix.typing import (
    OtterBrixPyType,
)
from otterbrix.experimental.spark.sql.types import Row
from otterbrix.experimental.spark.sql.types import (
    NullType,
    StringType,
    BinaryType,
    BitstringType,
    UUIDType,
    BooleanType,
    TimestampNTZType,
    TimestampNanosecondNTZType,
    TimestampMilisecondNTZType,
    TimestampSecondNTZType,
    DecimalType,
    DoubleType,
    FloatType,
    ByteType,
    UnsignedByteType,
    ShortType,
    UnsignedShortType,
    IntegerType,
    UnsignedIntegerType,
    LongType,
    UnsignedLongType,
    HugeIntegerType,
    UnsignedHugeIntegerType,
    ArrayType,
    MapType,
    StructField,
    StructType,
)

class TestTypes(object):
    def test_all_types_schema(self):
        assert OtterBrixPyType('NULL') == NullType().otterbrix_type; 
        assert OtterBrixPyType('VARCHAR') == StringType().otterbrix_type; 
        assert OtterBrixPyType('BLOB') == BinaryType().otterbrix_type; 
        assert OtterBrixPyType('BIT') == BitstringType().otterbrix_type; 
        assert OtterBrixPyType('UUID') == UUIDType().otterbrix_type; 
        assert OtterBrixPyType('BOOLEAN') == BooleanType().otterbrix_type; 
        assert OtterBrixPyType('TIMESTAMP') == TimestampNTZType().otterbrix_type; 
        assert OtterBrixPyType('TIMESTAMP_NS') == TimestampNanosecondNTZType().otterbrix_type; 
        assert OtterBrixPyType('TIMESTAMP_MS') == TimestampMilisecondNTZType().otterbrix_type; 
        assert OtterBrixPyType('TIMESTAMP_S') == TimestampSecondNTZType().otterbrix_type; 
        assert OtterBrixPyType('DOUBLE') == DoubleType().otterbrix_type; 
        assert OtterBrixPyType('FLOAT') == FloatType().otterbrix_type; 
        assert OtterBrixPyType('TINYINT') == ByteType().otterbrix_type; 
        assert OtterBrixPyType('UTINYINT') == UnsignedByteType().otterbrix_type; 
        assert OtterBrixPyType('SMALLINT') == ShortType().otterbrix_type; 
        assert OtterBrixPyType('USMALLINT') == UnsignedShortType().otterbrix_type; 
        assert OtterBrixPyType('INTEGER') == IntegerType().otterbrix_type; 
        assert OtterBrixPyType('UINTEGER') == UnsignedIntegerType().otterbrix_type; 
        assert OtterBrixPyType('BIGINT') == LongType().otterbrix_type; 
        assert OtterBrixPyType('UBIGINT') == UnsignedLongType().otterbrix_type; 
        assert OtterBrixPyType('HUGEINT') == HugeIntegerType().otterbrix_type; 
        assert OtterBrixPyType('UHUGEINT') == UnsignedHugeIntegerType().otterbrix_type; 
        assert decimal_type(5, 1) == DecimalType(5, 1).otterbrix_type;
        assert list_type(OtterBrixPyType('INTEGER')) == ArrayType(IntegerType()).otterbrix_type;
        assert map_type(OtterBrixPyType('INTEGER'), OtterBrixPyType('FLOAT')) == MapType(IntegerType(), FloatType()).otterbrix_type;
        assert OtterBrixPyType('INTEGER') == StructField('name', IntegerType()).otterbrix_type
        fields = [
            StructField('name1', IntegerType()),
            StructField('name2', FloatType()),
            StructField('name3', StructType([StructField('inner', IntegerType())])),
            StructField('name4', DoubleType())
        ]
        struct = StructType(fields)
        assert len(struct) == 4
        assert OtterBrixPyType('INTEGER') == struct[2].dataType[0].otterbrix_type
        assert OtterBrixPyType('INTEGER') == struct[2].dataType['inner'].otterbrix_type

    def test_types_boundary_values(self, spark):
        df = spark.createDataFrame(
            [(9999999999, -2147483648, "")], ["big", "neg", "empty"]
        )
        assert df.collect() == [Row(big=9999999999, neg=-2147483648, empty="")]

    def test_types_null_roundtrip(self, spark):
        # NULL produced by a left join must round-trip back to Python as None.
        left = spark.createDataFrame([(1, 10), (2, 99)], ["id", "k"])
        right = spark.createDataFrame([(10, "x")], ["k", "v"])
        rows = left.join(right, "k", "left").collect()
        by_id = {r.id: r.v for r in rows}
        assert by_id == {1: "x", 2: None}

