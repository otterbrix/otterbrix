# _probe_ops.py - one-shot probe; not part of the test suite.
# Each candidate runs in its own subprocess so a segfault in one
# (the core can crash hard) does not abort the whole probe.
import subprocess
import sys

SETUP = """
import sys
from otterbrix.experimental.spark.sql import SparkSession
from otterbrix.experimental.spark.sql.functions import (
    col, lit, upper, lower, trim, ltrim, rtrim, length, concat_ws,
    ceil, floor, abs, sqrt, greatest, least, coalesce, nvl, ifnull, when,
)
spark = SparkSession.builder.master("local[1]").appName("probe").getOrCreate()
base = spark.createDataFrame(
    [(1, "a", 10), (2, "b", 20), (3, "c", 30)], ["id", "name", "val"]
)
"""

CANDIDATES = [
    # constructions for depth tests
    ("empty_df_construct", 'spark.createDataFrame([], ["id", "name"]).collect()'),
    ("null_value_construct", 'spark.createDataFrame([(1, None)], ["a", "b"]).collect()'),
    ("null_via_left_join",
     'l = spark.createDataFrame([(1, 10), (2, 99)], ["id", "k"]); '
     'r = spark.createDataFrame([(10, "x")], ["k", "v"]); '
     'l.join(r, "k", "left").collect()'),
    # transform operations
    ("withColumn", 'base.withColumn("x", col("val")).collect()'),
    ("withColumnRenamed", 'base.withColumnRenamed("val", "value").collect()'),
    ("drop", 'base.drop("val").collect()'),
    ("limit", 'base.limit(2).collect()'),
    ("head", 'base.head(2)'),
    ("take", 'base.take(2)'),
    ("count", 'base.count()'),
    ("distinct", 'base.distinct().collect()'),
    ("dropDuplicates", 'base.dropDuplicates(["name"]).collect()'),
    ("union", 'base.union(base).collect()'),
    ("unionByName", 'base.unionByName(base).collect()'),
    ("toDF", 'base.toDF("a", "b", "c").collect()'),
    # column operations
    ("cast", 'base.select(col("val").cast("string")).collect()'),
    ("isin", 'base.filter(col("id").isin(1, 2)).collect()'),
    ("like", 'base.filter(col("name").like("a")).collect()'),
    ("ilike", 'base.filter(col("name").ilike("A")).collect()'),
    ("alias_column", 'base.select(col("val").alias("v")).collect()'),
    ("when_otherwise",
     'base.select(when(col("id") == 1, lit("one")).otherwise(lit("other"))).collect()'),
    # functions
    ("upper", 'base.select(upper(col("name"))).collect()'),
    ("lower", 'base.select(lower(col("name"))).collect()'),
    ("trim", 'base.select(trim(col("name"))).collect()'),
    ("ltrim", 'base.select(ltrim(col("name"))).collect()'),
    ("rtrim", 'base.select(rtrim(col("name"))).collect()'),
    ("length", 'base.select(length(col("name"))).collect()'),
    ("concat_ws", 'base.select(concat_ws("-", col("name"), col("name"))).collect()'),
    ("ceil", 'base.select(ceil(col("val"))).collect()'),
    ("floor", 'base.select(floor(col("val"))).collect()'),
    ("abs", 'base.select(abs(col("val"))).collect()'),
    ("sqrt", 'base.select(sqrt(col("val"))).collect()'),
    ("greatest", 'base.select(greatest(col("id"), col("val"))).collect()'),
    ("least", 'base.select(least(col("id"), col("val"))).collect()'),
    ("coalesce", 'base.select(coalesce(col("name"), lit("x"))).collect()'),
    ("nvl", 'base.select(nvl(col("name"), lit("x"))).collect()'),
    ("ifnull", 'base.select(ifnull(col("name"), lit("x"))).collect()'),
]

MARK = "PROBE_RESULT_OK"

for name, code in CANDIDATES:
    prog = SETUP + code + f'\nspark.stop()\nprint("{MARK}")\n'
    proc = subprocess.run(
        [sys.executable, "-c", prog],
        capture_output=True, text=True,
    )
    if proc.returncode == 0 and MARK in proc.stdout:
        print(f"PASS  {name}")
    elif proc.returncode in (139, -11):
        print(f"FAIL  {name}: SEGFAULT (core crashed)")
    else:
        err = proc.stderr.strip().splitlines()
        msg = err[-1] if err else f"exit {proc.returncode}"
        print(f"FAIL  {name}: {msg}")
