# Databricks notebook source
# MAGIC %md
# MAGIC # 12.1 Testing Spark code
# MAGIC
# MAGIC The exam names the toolkit: `assertDataFrameEqual`,
# MAGIC `assertSchemaEqual`, `DataFrame.transform`, testing frameworks. We test
# MAGIC REAL course logic, the cart rules, on fake data, the way the docs say to:
# MAGIC "it is a best practice to not run unit tests against functions that work
# MAGIC with data in production." Then one integration test, on real silver rows.
# MAGIC
# MAGIC > Prereq: `lakeshore.silver.bus_orders` from Section 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Make logic testable: functions + transform
# MAGIC
# MAGIC You can't test a cell. You can test a FUNCTION that takes a DataFrame
# MAGIC and returns one — and `DataFrame.transform` chains such functions
# MAGIC readably.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

def add_cart_total(df: DataFrame) -> DataFrame:
    """items array → cart_total column (the S5 HOF, packaged)."""
    return df.withColumn("cart_total", expr(
        "round(aggregate(items, 0.0D, (acc, i) -> acc + i.quantity * i.unit_price), 2)"))

def flag_suspicious(df: DataFrame, max_total: float = 5000.0) -> DataFrame:
    """Orders beyond a plausible total get flagged, not dropped."""
    return df.withColumn("suspicious", col("cart_total") > max_total)

def pipeline(df: DataFrame) -> DataFrame:
    """The stage, as a readable chain of testable functions."""
    return df.transform(add_cart_total).transform(flag_suspicious)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fake data in, exact expectations out

# COMMAND ----------

from pyspark.sql.types import DoubleType, StructField, StructType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

ITEMS = "ARRAY<STRUCT<product_id: STRING, quantity: INT, unit_price: DOUBLE>>"

fake = spark.createDataFrame(
    [("O1", [("P1", 2, 100.0)]),                      # 200.00 — fine
     ("O2", [("P1", 10, 899.0)]),                     # 8990.00 — suspicious
     ("O3", [])],                                     # empty cart — 0.0
    f"order_id STRING, items {ITEMS}")

expected = spark.createDataFrame(
    [("O1", [("P1", 2, 100.0)], 200.0, False),
     ("O2", [("P1", 10, 899.0)], 8990.0, True),
     ("O3", [], 0.0, False)],
    f"order_id STRING, items {ITEMS}, cart_total DOUBLE, suspicious BOOLEAN")

assertDataFrameEqual(pipeline(fake), expected)
print("✓ assertDataFrameEqual: rows AND schema match, order-insensitive")

# COMMAND ----------

# MAGIC %md
# MAGIC `assertDataFrameEqual` compares data and schema. Its defaults are worth knowing:
# MAGIC row order doesn't matter (`checkRowOrder=True` if it should), floats compare with a small
# MAGIC tolerance (`rtol`, `atol`), and nullability differences are ignored
# MAGIC (`ignoreNullable=True`). Column order DOES matter unless you pass
# MAGIC `ignoreColumnOrder=True`. The empty-cart row is the kind of edge a test EXISTS for:
# MAGIC cell-by-cell eyeballing never checks it twice.

# COMMAND ----------

# Schema as its own contract:
assertSchemaEqual(pipeline(fake).schema, expected.schema)

# The defaults, proven. Same rows, different row order and nullability: both pass.
assertDataFrameEqual(pipeline(fake).orderBy(col("order_id").desc()), expected)
assertSchemaEqual(StructType([StructField("n", DoubleType(), False)]),
                  StructType([StructField("n", DoubleType(), True)]))

# Column order is NOT ignored by default:
reordered = pipeline(fake).select("suspicious", "cart_total", "items", "order_id")
try:
    assertDataFrameEqual(reordered, expected)
    raise AssertionError("expected a column-order failure")
except Exception as e:
    assert "expected a column-order failure" not in str(e)
assertDataFrameEqual(reordered, expected, ignoreColumnOrder=True)
print("✓ row order and nullability ignored, column order not, unless you say so")

# And the failure message quality — see WHY tests beat print():
try:
    broken = fake.withColumn("cart_total", expr("0.0D")) \
                 .withColumn("suspicious", expr("false"))
    assertDataFrameEqual(broken, expected)
except Exception as e:
    print("the diff a failing assert gives you:\n", str(e)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A unittest suite, runnable anywhere

# COMMAND ----------

import unittest

class CartRulesTest(unittest.TestCase):
    def test_totals(self):
        out = {r.order_id: r.cart_total
               for r in pipeline(fake).collect()}
        self.assertEqual(out["O1"], 200.0)
        self.assertEqual(out["O3"], 0.0)

    def test_suspicious_threshold_boundary(self):
        df = spark.createDataFrame([("OX", [("P", 1, 5000.0)])],
                                   f"order_id STRING, items {ITEMS}")
        row = pipeline(df).first()
        self.assertFalse(row.suspicious)   # 5000.00 is NOT beyond 5000

result = unittest.main(argv=["x"], exit=False, verbosity=2).result
assert result.wasSuccessful(), "unit tests failed"

# COMMAND ----------

# MAGIC %md
# MAGIC `pytest` runs this same style of test once the functions live in a module (next
# MAGIC lesson does exactly that): the docs' layout is `myfunctions.py` + `test_myfunctions.py`,
# MAGIC with pytest discovering `test_*`. The boundary test (exactly 5000) is the classic
# MAGIC off-by-one a reviewer misses and a test never does.
# MAGIC
# MAGIC ## 4. Unit vs integration: the line the exam draws
# MAGIC
# MAGIC | | Unit | Integration |
# MAGIC |---|---|---|
# MAGIC | Data | fake, in-memory | real (a copy/sample) |
# MAGIC | Scope | one function | a pipeline stage end-to-end |
# MAGIC | Where | anywhere (CI!) | workspace / job |
# MAGIC | Our examples | this notebook | the cell below, S1's fast-forward asserts, S10's leak test |
# MAGIC
# MAGIC The course has been integration-testing itself all along, in every notebook's asserts.
# MAGIC Here is one written on purpose: the same pipeline, on a sample of real silver data, with
# MAGIC invariants that must hold for ANY input, because real rows have no expected value you
# MAGIC could type in.

# COMMAND ----------

sample = (spark.table("lakeshore.silver.bus_orders")
          .filter("order_id IS NOT NULL")
          .orderBy("order_id", "offset")
          .limit(50)
          .select("order_id", "items"))

stage = pipeline(sample)

assert stage.count() == sample.count(), "the stage must not add or lose rows"
assert stage.filter("cart_total IS NULL OR cart_total < 0").count() == 0, "bad cart totals"
assert stage.filter("suspicious IS NULL").count() == 0, "every row gets a flag"
print(f"✓ integration test: {stage.count()} real rows, invariants hold")

# COMMAND ----------

# MAGIC %md
# MAGIC **Beyond the exam: the interactive debugger.** The current exam guide no longer names it,
# MAGIC but it works on serverless and it is worth 90 seconds. Turn on **Python Notebook
# MAGIC Interactive Debugger** in **Settings**, **Developer**. Click the gutter beside the
# MAGIC `return` in `flag_suspicious` to set a breakpoint, then run the first cell that calls
# MAGIC it with **Debug cell**. Tests keep working code working. The debugger is for finding out
# MAGIC why it isn't.

# COMMAND ----------

dbutils.notebook.exit("testing-complete")
