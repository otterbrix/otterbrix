from typing import Optional, List, Any, Union, Iterable, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from .catalog import Catalog
    from pandas.core.frame import DataFrame as PandasDataFrame

from ..exception import ContributionsAcceptedError
from .types import StructType, AtomicType, DataType
from ..conf import SparkConf
from .dataframe import DataFrame
from .conf import RuntimeConfig
from .readwriter import DataFrameReader
from ..context import SparkContext
from .streaming import DataStreamReader
import otterbrix

from ..errors import (
    PySparkTypeError,
    PySparkValueError
)

from ..errors.error_classes import *

# In spark:
# SparkSession holds a SparkContext
# SparkContext gets created from SparkConf
# At this level the check is made to determine whether the instance already exists and just needs to be retrieved or it needs to be created

# For us this is done inside of `otterbrix.connect`, based on the passed in path + configuration
# SparkContext can be compared to our Connection class, and SparkConf to our ClientContext class


class SparkSession:
    def __init__(self, context: SparkContext):
        self.conn = context.connection
        self._context = context
        self._conf = RuntimeConfig(self.conn)

    def _create_dataframe(self, data: "PandasDataFrame") -> DataFrame:
        return DataFrame(self.conn.from_df(data), self)

    def _createDataFrameFromPandas(self, data: "PandasDataFrame", types, names) -> DataFrame:
        # Apply the declared schema by coercing pandas dtypes before handing
        # the frame to conn.from_df. The engine has no value-cast operator
        # (::TYPE is a path-selection hint, not a conversion), so this is the
        # only place an explicit schema can actually change column types.
        if names or types:
            data = data.copy()
        if names:
            data.columns = names
        if types:
            from .type_utils import spark_type_to_pandas_dtype
            dtype_map = {}
            target_names = names if names else list(data.columns)
            for col, t in zip(target_names, types):
                pandas_dtype = spark_type_to_pandas_dtype(t)
                if pandas_dtype is not None:
                    dtype_map[col] = pandas_dtype
            if dtype_map:
                data = data.astype(dtype_map)
        return self._create_dataframe(data)

    def createDataFrame(
        self,
        data: Union["PandasDataFrame", Iterable[Any]],
        schema: Optional[Union[StructType, List[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
        optimize: bool = False,
    ) -> DataFrame:
        if samplingRatio:
            raise NotImplementedError
        if not verifySchema:
            raise NotImplementedError
        types = None
        names = None

        if isinstance(data, DataFrame):
            raise PySparkTypeError(
                error_class="SHOULD_NOT_DATAFRAME",
                message_parameters={"arg_name": "data"},
            )

        if schema:
            if isinstance(schema, StructType):
                types, names = schema.extract_types_and_names()
            else:
                names = schema

        try:
            import pandas

            has_pandas = True
        except ImportError:
            has_pandas = False
        if not has_pandas:
            raise ImportError(
                "pandas is required to create a DataFrame from non-pandas data"
            )

        # Non-pandas inputs are coerced to pandas: SQL VALUES requires an alias and
        # segfaults when one is supplied, and conn.from_object crashes on dict/list
        # inputs, so conn.from_df is the only reliable path. forward_names is None
        # for the coerced case because pandas.DataFrame already applied columns=names.
        if isinstance(data, pandas.DataFrame):
            pandas_df = data
            forward_names = names
        else:
            pandas_df = pandas.DataFrame(data=data, columns=names)
            forward_names = None

        df = self._createDataFrameFromPandas(pandas_df, types, forward_names)
        df._optimize = optimize
        return df

    def newSession(self) -> "SparkSession":
        return SparkSession(self._context)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> "DataFrame":
        if numPartitions:
            raise ContributionsAcceptedError

        if end is None:
            end = start
            start = 0

        return DataFrame(self.conn.table_function("range", parameters=[start, end, step]),self)

    def sql(self, sqlQuery: str, **kwargs: Any) -> DataFrame:
        if kwargs:
            raise NotImplementedError
        relation = self.conn.sql(sqlQuery)
        return DataFrame(relation, self)

    def stop(self) -> None:
        self._context.stop()

    def table(self, tableName: str) -> DataFrame:
        relation = self.conn.table(tableName)
        return DataFrame(relation, self)

    def getActiveSession(self) -> "SparkSession":
        return self

    @property
    def catalog(self) -> "Catalog":
        if not hasattr(self, "_catalog"):
            from otterbrix.experimental.spark.sql.catalog import Catalog

            self._catalog = Catalog(self)
        return self._catalog

    @property
    def conf(self) -> RuntimeConfig:
        return self._conf

    @property
    def read(self) -> DataFrameReader:
        return DataFrameReader(self)

    @property
    def readStream(self) -> DataStreamReader:
        return DataStreamReader(self)

    @property
    def sparkContext(self) -> SparkContext:
        return self._context

    @property
    def streams(self) -> Any:
        raise ContributionsAcceptedError

    @property
    def version(self) -> str:
        return '1.0.0'

    class Builder:
        def __init__(self):
            pass

        def master(self, name: str) -> "SparkSession.Builder":
            # no-op
            return self

        def appName(self, name: str) -> "SparkSession.Builder":
            # no-op
            return self

        def remote(self, url: str) -> "SparkSession.Builder":
            # no-op
            return self

        def getOrCreate(self) -> "SparkSession":
            context = SparkContext("__ignored__")
            return SparkSession(context)

        def config(
            self, key: Optional[str] = None, value: Optional[Any] = None, conf: Optional[SparkConf] = None
        ) -> "SparkSession.Builder":
            return self

        def enableHiveSupport(self) -> "SparkSession.Builder":
            # no-op
            return self

    builder = Builder()


__all__ = ["SparkSession"]
