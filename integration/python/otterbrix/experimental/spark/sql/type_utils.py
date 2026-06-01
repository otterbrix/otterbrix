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
    ArrayType,
    MapType,
    StructField,
    StructType,
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
    # union
    # enum
    # null (???)
    'float': FloatType,
    'double': DoubleType,
    'decimal': DecimalType,
}


def convert_nested_type(dtype: OtterBrixPyType) -> DataType:
    id = dtype.id
    if id == 'list' or id == 'array':
        children = dtype.children
        return ArrayType(convert_type(children[0][1]))
    # TODO: add support for 'union'
    if id == 'struct':
        children: List[Tuple[str, OtterBrixPyType]] = dtype.children
        fields = [StructField(x[0], convert_type(x[1])) for x in children]
        return StructType(fields)
    if id == 'map':
        return MapType(convert_type(dtype.key), convert_type(dtype.value))
    raise NotImplementedError


def convert_type(dtype: OtterBrixPyType) -> DataType:
    id = dtype.id
    if id in ['list', 'struct', 'map', 'array']:
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
