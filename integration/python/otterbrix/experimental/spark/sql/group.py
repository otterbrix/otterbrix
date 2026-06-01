#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ..exception import ContributionsAcceptedError
from typing import Callable, TYPE_CHECKING, overload, Dict, Union, List

from .column import Column
from .session import SparkSession
from .dataframe import DataFrame
from .functions import _to_column_expr
from .types import NumericType
from ._typing import ColumnOrName

from otterbrix import CountExpression, ColumnExpression
from otterbrix.experimental.spark.context import SparkContext

if TYPE_CHECKING:
    from ._typing import LiteralType

__all__ = ["GroupedData", "Grouping"]

def _api_internal(self: "GroupedData", name: str, *cols: str) -> DataFrame:
    expressions = ",".join(list(cols))
    group_by = str(self._grouping) if self._grouping else ""
    projections = self._grouping.get_columns()
    jdf = getattr(self._df.relation, "apply")(
        function_name=name,  # aggregate function
        function_aggr=expressions,  # inputs to aggregate
        group_expr=group_by,  # groups
        projected_columns=projections,  # projections
    )
    return DataFrame(jdf, self.session, optimize=self._optimize)

def df_varargs_api(f: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
    def _api(self: "GroupedData", *cols: str) -> DataFrame:
        name = f.__name__
        return _api_internal(self, name, *cols)

    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class Grouping:
    def __init__(self, *cols: "ColumnOrName", **kwargs):
        self._type = ""
        self._cols = [_to_column_expr(x) for x in cols]
        self._key_names = [c if isinstance(c, str) else str(c) for c in cols]
        if 'special' in kwargs:
            special = kwargs['special']
            accepted_special = ["cube", "rollup"]
            assert special in accepted_special
            self._type = special

    def get_columns(self) -> str:
        columns = ",".join([str(x) for x in self._cols])
        return columns

    def __str__(self):
        columns = self.get_columns()
        if self._type:
            return self._type + '(' + columns + ')'
        return columns

    def copy(self):
        import copy
        return copy.copy(self)


class GroupedData:
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.

    """

    def __init__(self, grouping: Grouping, df: DataFrame, optimize: bool = False):
        self._grouping = grouping
        self._df = df
        self.session: SparkSession = df.session
        self._optimize = optimize
    def __repr__(self) -> str:
        return str(self._df)

    def count(self) -> DataFrame:
        """Counts the number of records for each group.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name).count().sort("name").show()
        +-----+-----+
        | name|count|
        +-----+-----+
        |Alice|    2|
        |  Bob|    2|
        +-----+-----+
        """
        sc = SparkContext._active_spark_context
        count_expr = CountExpression(sc)
        return self.agg(Column(count_expr))

    def mean(self, *cols: str) -> DataFrame:
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.
        """
        agg_cols = list(cols)
        if not agg_cols:
            agg_cols = [field.name for field in self._df.schema if isinstance(field.dataType, NumericType)]
        from .functions import avg as avg_func
        agg_columns = [avg_func(c) for c in agg_cols]
        return self.agg(*agg_columns)

    def avg(self, *cols: str) -> DataFrame:
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the mean of the age in each group.

        >>> df.groupBy("name").avg('age').sort("name").show()
        +-----+--------+
        | name|avg(age)|
        +-----+--------+
        |Alice|     2.5|
        |  Bob|     7.5|
        +-----+--------+

        Calculate the mean of the age and height in all data.

        >>> df.groupBy().avg('age', 'height').show()
        +--------+-----------+
        |avg(age)|avg(height)|
        +--------+-----------+
        |     5.0|      110.0|
        +--------+-----------+
        """
        return self.mean(*cols)

    def max(self, *cols: str) -> DataFrame:
        """Computes the max value for each numeric columns for each group.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the max of the age in each group.

        >>> df.groupBy("name").max("age").sort("name").show()
        +-----+--------+
        | name|max(age)|
        +-----+--------+
        |Alice|       3|
        |  Bob|      10|
        +-----+--------+

        Calculate the max of the age and height in all data.

        >>> df.groupBy().max("age", "height").show()
        +--------+-----------+
        |max(age)|max(height)|
        +--------+-----------+
        |      10|        140|
        +--------+-----------+
        """
        agg_cols = list(cols)
        if not agg_cols:
            agg_cols = [field.name for field in self._df.schema if isinstance(field.dataType, NumericType)]
        from .functions import max as max_func
        agg_columns = [max_func(c) for c in agg_cols]
        return self.agg(*agg_columns)

    def min(self, *cols: str) -> DataFrame:
        """Computes the min value for each numeric column for each group.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the min of the age in each group.

        >>> df.groupBy("name").min("age").sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Calculate the min of the age and height in all data.

        >>> df.groupBy().min("age", "height").show()
        +--------+-----------+
        |min(age)|min(height)|
        +--------+-----------+
        |       2|         80|
        +--------+-----------+
        """
        agg_cols = list(cols)
        if not agg_cols:
            agg_cols = [field.name for field in self._df.schema if isinstance(field.dataType, NumericType)]
        from .functions import min as min_func
        agg_columns = [min_func(c) for c in agg_cols]
        return self.agg(*agg_columns)

    def sum(self, *cols: str) -> DataFrame:
        """Computes the sum for each numeric columns for each group.

        Parameters
        ----------
        cols : str
            column names. Non-numeric columns are ignored.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", 80), (3, "Alice", 100),
        ...     (5, "Bob", 120), (10, "Bob", 140)], ["age", "name", "height"])
        >>> df.show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  3|Alice|   100|
        |  5|  Bob|   120|
        | 10|  Bob|   140|
        +---+-----+------+

        Group-by name, and calculate the sum of the age in each group.

        >>> df.groupBy("name").sum("age").sort("name").show()
        +-----+--------+
        | name|sum(age)|
        +-----+--------+
        |Alice|       5|
        |  Bob|      15|
        +-----+--------+

        Calculate the sum of the age and height in all data.

        >>> df.groupBy().sum("age", "height").show()
        +--------+-----------+
        |sum(age)|sum(height)|
        +--------+-----------+
        |      20|        440|
        +--------+-----------+
        """
        agg_cols = list(cols)
        if not agg_cols:
            agg_cols = [field.name for field in self._df.schema if isinstance(field.dataType, NumericType)]
        from .functions import sum as sum_func
        agg_columns = [sum_func(c) for c in agg_cols]
        return self.agg(*agg_columns)

    @overload
    def agg(self, *exprs: Column) -> DataFrame:
        ...

    @overload
    def agg(self, __exprs: Dict[str, str]) -> DataFrame:
        ...

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> DataFrame:
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be:

        1. built-in aggregation functions, such as `avg`, `max`, `min`, `sum`, `count`

        2. group aggregate pandas UDFs, created with :func:`pyspark.sql.functions.pandas_udf`

           .. note:: There is no partial aggregation with group aggregate UDFs, i.e.,
               a full shuffle is required. Also, all the data of a group will be loaded into
               memory, so the user should be aware of the potential OOM risk if data is skewed
               and certain groups are too large to fit in memory.

           .. seealso:: :func:`pyspark.sql.functions.pandas_udf`

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        exprs : dict
            a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        Notes
        -----
        Built-in aggregation functions and group aggregate pandas UDFs cannot be mixed
        in a single call to this function.

        Examples
        --------
        >>> from pyspark.sql import functions as F
        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"])
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  3|Alice|
        |  5|  Bob|
        | 10|  Bob|
        +---+-----+

        Group-by name, and count each group.

        >>> df.groupBy(df.name)
        GroupedData[grouping...: [name...], value: [age: bigint, name: string], type: GroupBy]

        >>> df.groupBy(df.name).agg({"*": "count"}).sort("name").show()
        +-----+--------+
        | name|count(1)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       2|
        +-----+--------+

        Group-by name, and calculate the minimum age.

        >>> df.groupBy(df.name).agg(F.min(df.age)).sort("name").show()
        +-----+--------+
        | name|min(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Same as above but uses pandas UDF.

        >>> @pandas_udf('int', PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
        ... def min_udf(v):
        ...     return v.min()
        ...
        >>> df.groupBy(df.name).agg(min_udf(df.age)).sort("name").show()  # doctest: +SKIP
        +-----+------------+
        | name|min_udf(age)|
        +-----+------------+
        |Alice|           2|
        |  Bob|           5|
        +-----+------------+
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            raise ContributionsAcceptedError
        else:
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            group_keys = self._grouping._key_names

            sc = SparkContext._active_spark_context
            all_exprs = []
            for key in group_keys:
                all_exprs.append(ColumnExpression(key, sc))
            for c in exprs:
                all_exprs.append(c.expr)
            new_relation = self._df.relation.group(*all_exprs)
        return DataFrame(new_relation, self._df.session, optimize=self._df._optimize)

