# Databricks notebook source
# MAGIC %md
# MAGIC # 2.7 — Table layouts at scale, in Delta and Iceberg
# MAGIC
# MAGIC The guide asks you to design layouts for large tables "by mapping partitioning
# MAGIC to data grain, aligning clustering to relationship access patterns, and
# MAGIC maintaining balanced file sizes through compaction". Free Edition has no large
# MAGIC tables, so the sizing rules come from the docs and we test the **behaviour**
# MAGIC here: what each format accepts, refuses, and silently changes.
# MAGIC
# MAGIC > Prereq: 2.5 has run. Every table here is a demo, dropped at the end.

# COMMAND ----------

DEMO = "lakeshore.gold.layout_demo"

def drop_demos():
    for suffix in ("part", "both", "compact", "ice_v3", "ice_v2", "ice_part"):
        spark.sql(f"DROP TABLE IF EXISTS {DEMO}_{suffix}")

def error_code(sql):
    """Run a statement that should fail; return the error class in its brackets."""
    try:
        spark.sql(sql)
    except Exception as err:
        return str(err).split("]")[0].split("[")[-1]
    raise RuntimeError(f"expected this to fail: {sql}")

def layout(table):
    """The two columns of DESCRIBE DETAIL that tell you the layout."""
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return {"format": detail["format"], "partitionColumns": detail["partitionColumns"],
            "clusteringColumns": detail["clusteringColumns"], "numFiles": detail["numFiles"]}

drop_demos()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Apply the sizing rule to a real table
# MAGIC
# MAGIC The docs' thresholds, as code. Our fact is tiny, and the rule says so.

# COMMAND ----------

def partition_verdict(size_bytes):
    """The partitioning guidance in the docs, for a table of this size."""
    terabytes = size_bytes / 1024 ** 4
    if terabytes < 1:
        return "don't partition"
    if terabytes < 100:
        return "use liquid clustering instead"
    return "partitioning might help: test liquid clustering first, keep 1 GB+ per partition"

fact = spark.sql("DESCRIBE DETAIL lakeshore.gold.fact_order_items").first()
print(f"fact_order_items: {fact['sizeInBytes']:,} bytes → {partition_verdict(fact['sizeInBytes'])}")
for tb in (5, 150):
    print(f"a {tb} TB table → {partition_verdict(tb * 1024 ** 4)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta: partition OR cluster, never both
# MAGIC
# MAGIC And a partitioned table can move to clustering without being rebuilt by hand.

# COMMAND ----------

print(error_code(f"CREATE TABLE {DEMO}_both (id INT, d DATE) PARTITIONED BY (d) CLUSTER BY (id)"))

spark.sql(f"CREATE TABLE {DEMO}_part (id INT, d DATE) PARTITIONED BY (d)")
print("partitioned:", layout(f"{DEMO}_part"))

spark.sql(f"ALTER TABLE {DEMO}_part REPLACE PARTITIONED BY WITH CLUSTER BY (d)")
print("converted:  ", layout(f"{DEMO}_part"))
assert layout(f"{DEMO}_part")["partitionColumns"] == []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Managed Iceberg: `USING ICEBERG`, and the version matters
# MAGIC
# MAGIC Without `USING ICEBERG` you get Delta. On Iceberg v3, clustering works the way
# MAGIC it does on Delta:

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE {DEMO}_ice_v3 (order_id STRING,
        order_date DATE, product_id STRING,
        line_total DOUBLE)
    USING ICEBERG
    TBLPROPERTIES ('format-version' = 3)
    CLUSTER BY (order_date, product_id)
""")
spark.sql(f"""
    INSERT INTO {DEMO}_ice_v3
    SELECT order_id, order_date, product_id, line_total FROM lakeshore.gold.fact_order_items
""")
spark.sql(f"OPTIMIZE {DEMO}_ice_v3")
print(layout(f"{DEMO}_ice_v3"))

# COMMAND ----------

# MAGIC %md
# MAGIC On the default version, the same `CLUSTER BY` is refused until you turn off
# MAGIC deletion vectors and row tracking, which v2 tables can't use:

# COMMAND ----------

print(error_code(f"CREATE TABLE {DEMO}_ice_v2 (id INT, d DATE) USING ICEBERG CLUSTER BY (d)"))

spark.sql(f"""
    CREATE TABLE {DEMO}_ice_v2 (id INT, d DATE) USING ICEBERG
    CLUSTER BY (d)
    TBLPROPERTIES ('delta.enableDeletionVectors' = false, 'delta.enableRowTracking' = false)
""")
print(layout(f"{DEMO}_ice_v2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The surprise: `PARTITIONED BY` on managed Iceberg
# MAGIC
# MAGIC The docs say managed Iceberg tables don't support `PARTITIONED BY`, so you'd
# MAGIC expect an error. Measured on Free Edition, 1 November 2026: the statement
# MAGIC **succeeds, and you get a clustering key**. This cell reports what happens on
# MAGIC your workspace today, whichever way it goes.

# COMMAND ----------

try:
    spark.sql(f"CREATE TABLE {DEMO}_ice_part (id INT, d DATE) USING ICEBERG PARTITIONED BY (d)")
    print("accepted:", layout(f"{DEMO}_ice_part"))
except Exception as err:
    print("refused:", str(err).split("]")[0].split("[")[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compaction: what "balanced file sizes" looks like
# MAGIC
# MAGIC Small writes leave small files. `OPTIMIZE` rewrites them toward the target size.
# MAGIC On a managed table, predictive optimization and auto compaction can do this for
# MAGIC you, so your count before `OPTIMIZE` may already be lower than the number of
# MAGIC inserts.

# COMMAND ----------

spark.sql(f"CREATE TABLE {DEMO}_compact (id INT, d DATE) CLUSTER BY (d)")
for i in range(5):
    spark.sql(f"INSERT INTO {DEMO}_compact VALUES ({i}, DATE'2026-08-01')")

files_before = layout(f"{DEMO}_compact")["numFiles"]
spark.sql(f"OPTIMIZE {DEMO}_compact")
files_after = layout(f"{DEMO}_compact")["numFiles"]
print(f"files: {files_before} → {files_after}")
assert files_after <= files_before

# COMMAND ----------

drop_demos()
print("✓ demo tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Under 1 TB, don't partition. Up to 100 TB, cluster. Above that, test. Delta and
# MAGIC Iceberg share liquid clustering, but Iceberg's version decides what else you must
# MAGIC switch off, and a managed Iceberg table has no real partitions. Section 7 measures
# MAGIC what clustering does to a query. Next section: streaming machinery, from multiplex
# MAGIC bronze to watermarked silver to the SCD2 dimension we left empty in 2.5.

# COMMAND ----------

dbutils.notebook.exit("table-layouts-complete")
