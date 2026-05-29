from otterbrix.typing import OtterBrixPyType
from typing import List, Tuple, cast
from .types import (
    DataType,
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
    NullType,
    ArrayType,
    MapType,
    StructField,
    StructType,
    UnionType,
)

_sqltype_to_spark_class = {
    'boolean': BooleanType,
    'utinyint': UnsignedByteType,
    'tinyint': ByteType,
    'usmallint': UnsignedShortType,
    'smallint': ShortType,
    'uinteger': UnsignedIntegerType,
    'integer': IntegerType,
    'ubigint': UnsignedLongType,
    'bigint': LongType,
    'hugeint': HugeIntegerType,
    'uhugeint': UnsignedHugeIntegerType,
    'varchar': StringType,
    'blob': BinaryType,
    'bit': BitstringType,
    'uuid': UUIDType,
    'timestamp': TimestampNTZType,
    'timestamp_ms': TimestampNanosecondNTZType,
    'timestamp_ns': TimestampMilisecondNTZType,
    'timestamp_s': TimestampSecondNTZType,
    'list': ArrayType,
    'struct': StructType,
    'map': MapType,
    'union': UnionType,
    'null': NullType,
    # enum: C++ binding does not expose enum metadata yet, and Spark has no
    # native enum DataType — leave unmapped until both sides exist.
    'float': FloatType,
    'double': DoubleType,
    'decimal': DecimalType,
}


def convert_nested_type(dtype: OtterBrixPyType) -> DataType:
    id = dtype.id
    if id == 'list' or id == 'array':
        children = dtype.children
        return ArrayType(convert_type(children[0][1]))
    if id == 'union':
        return UnionType()
    if id == 'struct':
        children: List[Tuple[str, OtterBrixPyType]] = dtype.children
        fields = [StructField(x[0], convert_type(x[1])) for x in children]
        return StructType(fields)
    if id == 'map':
        return MapType(convert_type(dtype.key), convert_type(dtype.value))
    raise NotImplementedError


def convert_type(dtype: OtterBrixPyType) -> DataType:
    id = dtype.id
    if id in ['list', 'struct', 'map', 'array', 'union']:
        return convert_nested_type(dtype)
    if id == 'decimal':
        children: List[Tuple[str, OtterBrixPyType]] = dtype.children
        precision = cast(int, children[0][1])
        scale = cast(int, children[1][1])
        return DecimalType(precision, scale)
    spark_type = _sqltype_to_spark_class[id]
    return spark_type()


def otterbrix_to_spark_schema(names: List[str], types: List[OtterBrixPyType]) -> StructType:
    fields = [StructField(name, dtype) for name, dtype in zip(names, [convert_type(x) for x in types])]
    return StructType(fields)


# Spark DataType -> pandas dtype
# The engine doesn't cast values itself, so a declared schema is applied by converting to pandas first (df.astype) before from_df
# Unlisted types return None and are left as-is
_spark_class_to_pandas_dtype = {
    StringType: 'str',
    BooleanType: 'bool',
    ByteType: 'int8',
    UnsignedByteType: 'uint8',
    ShortType: 'int16',
    UnsignedShortType: 'uint16',
    IntegerType: 'int32',
    UnsignedIntegerType: 'uint32',
    LongType: 'int64',
    UnsignedLongType: 'uint64',
    FloatType: 'float32',
    DoubleType: 'float64',
}


def spark_type_to_pandas_dtype(dtype: DataType):
    return _spark_class_to_pandas_dtype.get(type(dtype))
