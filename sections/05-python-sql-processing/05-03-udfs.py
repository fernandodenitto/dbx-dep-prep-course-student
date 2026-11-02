# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.3 — UDFs: Python, pandas, Unity Catalog — and when not to
# MAGIC
# MAGIC The guide: "Develop User-Defined Functions (UDFs) using Pandas, Python, and SQL,
# MAGIC including Unity Catalog functions for the given constraints." Two questions decide
# MAGIC which one you write:
# MAGIC
# MAGIC 1. **What's the engine?** SQL, then pandas over Arrow batches, then row-at-a-time
# MAGIC    Python, in the order you should reach for them.
# MAGIC 2. **Who else needs it?** Unity Catalog if it's shared and governed, session-scoped
# MAGIC    if it's one notebook's helper.
# MAGIC
# MAGIC And rule zero, which the exam tests hardest: **if a built-in can do it,
# MAGIC the UDF is the wrong answer.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL UDF — a function the optimizer can see

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.shipping_band(order_total DOUBLE)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Shipping tier by order value'
# MAGIC RETURN CASE WHEN order_total >= 500 THEN 'free'
# MAGIC             WHEN order_total >= 150 THEN 'reduced'
# MAGIC             ELSE 'standard' END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lakeshore.silver.shipping_band(cart_total) AS band,
# MAGIC        count(*) AS orders
# MAGIC FROM (SELECT round(aggregate(items, 0.0D,
# MAGIC              (acc, i) -> acc + i.quantity * i.unit_price), 2) AS cart_total
# MAGIC       FROM lakeshore.silver.bus_orders)
# MAGIC GROUP BY band ORDER BY orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC It lives IN the catalog (`lakeshore.silver.shipping_band`): governed,
# MAGIC commented, grantable, and — because it's SQL — the optimizer inlines
# MAGIC it. When logic fits in SQL, this beats both Python options.
# MAGIC
# MAGIC ## 2. Python in Unity Catalog — when the logic needs Python and teammates need it
# MAGIC
# MAGIC Some logic does need Python, and a session-scoped function dies with your session.
# MAGIC `CREATE FUNCTION ... LANGUAGE PYTHON` stores it in the catalog, with the body between
# MAGIC `$$`. Same governance as the SQL one: three-level name, comment, and callers need
# MAGIC `EXECUTE`. It runs on serverless notebooks and jobs and on serverless or pro SQL
# MAGIC warehouses. Each function must handle NULLs itself.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.shipping_band_uc(order_total DOUBLE)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Shipping tier by order value (governed Python)'
# MAGIC AS $$
# MAGIC if order_total is None:
# MAGIC     return None
# MAGIC if order_total >= 500:
# MAGIC     return "free"
# MAGIC if order_total >= 150:
# MAGIC     return "reduced"
# MAGIC return "standard"
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lakeshore.silver.shipping_band_uc(600.0) AS high,
# MAGIC        lakeshore.silver.shipping_band_uc(200.0) AS mid,
# MAGIC        lakeshore.silver.shipping_band_uc(NULL)  AS unknown;

# COMMAND ----------

# MAGIC %md
# MAGIC Any notebook, job or warehouse with `EXECUTE` can call that, and it's discoverable in
# MAGIC Catalog Explorer. The UDFs below are the other kind: **session-scoped**.
# MAGIC
# MAGIC | | Unity Catalog function | Session-scoped UDF |
# MAGIC |---|---|---|
# MAGIC | Governed by | Unity Catalog permissions (`EXECUTE`) | Nothing: "Not governed or shared" |
# MAGIC | Lives | In the catalog, across sessions | In this SparkSession only |
# MAGIC | Languages | SQL, Python, Scala, Java | SQL, Python, Scala |
# MAGIC | SQL warehouses | Yes (serverless or pro) | Not listed in the docs |
# MAGIC | pandas non-scalar, UDAF | No | **Yes**: session only |
# MAGIC | Best for | Sharing across teams, notebooks, jobs, warehouses | Fast iteration inside one notebook or job |
# MAGIC
# MAGIC ## 3. Python UDF — row-at-a-time, and priced accordingly

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def shipping_band_py(order_total: float) -> str:
    if order_total is None:
        return None
    if order_total >= 500:
        return "free"
    if order_total >= 150:
        return "reduced"
    return "standard"

spark.udf.register("shipping_band_py", shipping_band_py)   # visible to SQL too
print("registered")

# COMMAND ----------

# MAGIC %md
# MAGIC Same logic, but now every row makes a round-trip to a Python worker —
# MAGIC serialize, execute, deserialize — and the optimizer treats the function
# MAGIC as a black box.
# MAGIC
# MAGIC ## 4. pandas UDF — vectorized redemption
# MAGIC
# MAGIC Same round-trip, but per **Arrow batch** (10,000 rows by default; on serverless the
# MAGIC platform manages the batch size), not per row. The docs say up to 100x
# MAGIC compared to row-at-a-time Python UDFs. Type hints declare the shape:

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def shipping_band_pd(order_total: pd.Series) -> pd.Series:
    return pd.cut(order_total, bins=[-1, 150, 500, float("inf")],
                  labels=["standard", "reduced", "free"],
                  right=False).astype(str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The race — same logic, three engines

# COMMAND ----------

import time
from pyspark.sql.functions import col, expr

test = spark.range(2_000_000).withColumn(
    "total", (col("id") % 700).cast("double"))

def bench(name, df):
    t0 = time.time()
    n = df.filter("band = 'free'").count()      # forces full evaluation
    print(f"{name:<12} {time.time()-t0:6.2f}s   free-tier rows: {n:,}")
    return n

n1 = bench("built-in", test.withColumn("band", expr(
    "CASE WHEN total >= 500 THEN 'free' "
    "WHEN total >= 150 THEN 'reduced' ELSE 'standard' END")))
n2 = bench("pandas UDF", test.withColumn("band", shipping_band_pd("total")))
n3 = bench("python UDF", test.withColumn("band", shipping_band_py("total")))
assert n1 == n2 == n3, "the three must agree"

# COMMAND ----------

# MAGIC %md
# MAGIC Exact timings vary run to run — the ORDER doesn't: built-in ≤ pandas
# MAGIC UDF ≤ Python UDF. The built-in runs native and optimizable; pandas
# MAGIC pays Arrow-batch freight; row-at-a-time Python pays per row.
# MAGIC
# MAGIC ## 6. The other pandas shapes
# MAGIC
# MAGIC **Iterator of Series** is for expensive setup, like loading a model: setup runs once
# MAGIC per task, then the loop runs per batch.

# COMMAND ----------

from typing import Iterator

@pandas_udf("double")
def with_vat(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    rate = 1.22                     # imagine loading a model here, once
    for total in batches:
        yield (total * rate).round(2)

vat_total = (spark.range(1000).withColumn("total", col("id").cast("double"))
             .select(with_vat("total").alias("gross"))
             .agg({"gross": "sum"}).first()[0])
assert round(vat_total) == round(sum(range(1000)) * 1.22), vat_total

# COMMAND ----------

# MAGIC %md
# MAGIC **Grouped map (`applyInPandas`)** is split-apply-combine: each group arrives as one
# MAGIC pandas DataFrame, and you return a DataFrame matching a declared schema. Use it for
# MAGIC per-group logic with no SQL shape, like this share of each customer's own spend.
# MAGIC The warning is the exam's: "All data for a group is loaded into memory before the
# MAGIC function is applied", and `maxRecordsPerBatch` doesn't apply to groups. One huge group
# MAGIC is an out-of-memory error, and serverless caps user code at 1 GB.

# COMMAND ----------

def share_of_customer(lines: pd.DataFrame) -> pd.DataFrame:
    return lines.assign(share=(lines["line_total"] / lines["line_total"].sum()).round(4))

shares = (spark.table("lakeshore.gold.fact_order_items")
          .select("customer_id", "order_id", "product_id", "line_total")
          .groupBy("customer_id")
          .applyInPandas(share_of_customer,
                         "customer_id string, order_id string, product_id string, "
                         "line_total double, share double"))

biggest_group = (spark.table("lakeshore.gold.fact_order_items")
                 .groupBy("customer_id").count().agg({"count": "max"}).first()[0])
per_customer = shares.groupBy("customer_id").agg({"share": "sum"}).withColumnRenamed("sum(share)", "total")
assert per_customer.filter("abs(total - 1.0) > 0.01").count() == 0, "shares must sum to 1 per group"
print(f"largest group: {biggest_group} rows. In memory, all at once. Check this first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Which one, for the given constraints
# MAGIC
# MAGIC | Constraint | Reach for |
# MAGIC |---|---|
# MAGIC | A built-in, HOF or join can do it | **No UDF** |
# MAGIC | Expressible in SQL, shared | UC SQL function: governed, and the optimizer inlines it |
# MAGIC | Needs Python, shared across teams or warehouses | **UC Python function** (`LANGUAGE PYTHON`) |
# MAGIC | Needs Python, one notebook, vectorizable | Session pandas UDF, Series to Series |
# MAGIC | Expensive setup per batch | Iterator pandas UDF, or a batch UC Python function |
# MAGIC | Per-group logic, groups fit in memory | `applyInPandas` (session only) |
# MAGIC | Per-row, can't be vectorized, one notebook | Session Python UDF, last resort |
# MAGIC
# MAGIC ## 8. When NOT to UDF — the discipline
# MAGIC
# MAGIC Reach for a UDF only when ALL of these are true:
# MAGIC - no built-in / HOF / SQL expression does it (check twice),
# MAGIC - the logic genuinely needs Python (a library, a model, regex beyond
# MAGIC   SQL),
# MAGIC - and then: **pandas UDF over Python UDF**, unless it's non-vectorizable.
# MAGIC
# MAGIC The docs agree: "Use UDFs for logic that is difficult to express with built-in Apache
# MAGIC Spark functions." What a UDF costs beyond speed: the optimizer can't see through it,
# MAGIC and Python dependencies now ship with your pipeline.

# COMMAND ----------

# The governed functions stay: they're useful, and they prove the point of this lesson.
# The session UDFs vanish with this session.
for name in ("shipping_band", "shipping_band_uc"):
    spark.sql(f"DESCRIBE FUNCTION lakeshore.silver.{name}").show(truncate=False)

# COMMAND ----------

dbutils.notebook.exit("udfs-complete")
