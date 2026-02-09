from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from .dataframe import DataFrame
    from .session import SparkSession

PrimitiveType = Union[bool, float, int, str]
OptionalPrimitiveType = Optional[PrimitiveType]


class DataStreamWriter:
    def __init__(self, dataframe: "DataFrame"):
        self.dataframe = dataframe

    def toTable(self, table_name: str) -> None:
        # Should we register the dataframe or create a table from the contents?
        raise NotImplementedError


class DataStreamReader:
    def __init__(self, session: "SparkSession"):
        self.session = session

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Union[str, None] = None,
        **options: OptionalPrimitiveType
    ) -> "DataFrame":
        from otterbrix.experimental.spark.sql.dataframe import DataFrame

        raise NotImplementedError


__all__ = ["DataStreamReader", "DataStreamWriter"]
