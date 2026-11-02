# Databricks notebook source
# MAGIC %md
# MAGIC # 6.6 — Iceberg, as a source and as a CDC target
# MAGIC
# MAGIC The guide says ingest "Delta Lake, Parquet, JSON, Iceberg, CSV, and Binary", and
# MAGIC "incremental CDC pipelines using Lakeflow Pipelines with Delta or Iceberg as the target
# MAGIC table format". So: read Iceberg, stream from it, then try every way of landing CDC
# MAGIC in it, and read what actually got created.
# MAGIC
# MAGIC > Demo tables only (`bronze.ice_*`), dropped at the end. Idempotent.

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

BRONZE = "lakeshore.bronze"
CKPT = "/Volumes/lakeshore/landing/files/zoo/_checkpoints/iceberg"

dbutils.fs.rm(CKPT, True)
for name in ("ice_orders", "ice_orders_copy", "ice_cdc_raw", "ice_customers_reads",
             "ice_customers_using", "ice_customers_merge", "ice_v3"):
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{name}")

def table_format(name):
    """What the table really is, whatever the DDL said."""
    return spark.sql(f"DESCRIBE DETAIL {BRONZE}.{name}").first().format

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A managed Iceberg table
# MAGIC Two words of DDL. Unity Catalog is the Iceberg catalog, and everything you know about
# MAGIC `UPDATE`, `DELETE`, `MERGE` and `DESCRIBE HISTORY` still works.

# COMMAND ----------

spark.sql(f"""CREATE TABLE {BRONZE}.ice_orders
              (order_id BIGINT, customer_id STRING, amount DOUBLE) USING ICEBERG""")
spark.sql(f"""INSERT INTO {BRONZE}.ice_orders VALUES
              (1, 'C001', 40.0), (2, 'C002', 25.5), (3, 'C003', 99.0)""")
spark.sql(f"UPDATE {BRONZE}.ice_orders SET amount = 45.0 WHERE order_id = 1")

assert table_format("ice_orders") == "iceberg"
spark.sql(f"DESCRIBE HISTORY {BRONZE}.ice_orders").select("version", "operation").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Iceberg as a source
# MAGIC Read it by name, in batch or as a stream. Here a stream copies it into a Delta bronze
# MAGIC table, on the only trigger serverless allows.

# COMMAND ----------

(spark.readStream.table(f"{BRONZE}.ice_orders")
 .writeStream
 .option("checkpointLocation", f"{CKPT}/copy")
 .trigger(availableNow=True)
 .toTable(f"{BRONZE}.ice_orders_copy")
 .awaitTermination())

copied = spark.table(f"{BRONZE}.ice_orders_copy").count()
print(f"Delta copy: {copied} rows, format {table_format('ice_orders_copy')}")
assert copied == 3 and table_format("ice_orders_copy") == "delta"

# COMMAND ----------

# MAGIC %md
# MAGIC A **foreign** Iceberg table, one that AWS Glue or another catalog owns, reads the same
# MAGIC way through Lakehouse Federation and is **read-only** here. 📖 Free Edition has no outside
# MAGIC catalog to federate, so that half is theory.
# MAGIC
# MAGIC ## 3. Iceberg as a CDC target: the documented route
# MAGIC
# MAGIC A small change feed, then `AUTO CDC` into a streaming table with the docs' Iceberg
# MAGIC properties. `FLOW AUTO CDC` inline means no pipeline to set up today. Section 8 builds
# MAGIC the same thing as a real pipeline.

# COMMAND ----------

spark.sql(f"""CREATE TABLE {BRONZE}.ice_cdc_raw
              (customer_id STRING, email STRING, op STRING, seq BIGINT)""")
spark.sql(f"""INSERT INTO {BRONZE}.ice_cdc_raw VALUES
              ('C001', 'ana@example.com',  'INSERT', 1),
              ('C002', 'ben@example.com',  'INSERT', 2),
              ('C001', 'ana2@example.com', 'UPDATE', 3)""")

spark.sql(f"""
CREATE OR REFRESH STREAMING TABLE {BRONZE}.ice_customers_reads
TBLPROPERTIES(
  'delta.columnMapping.mode' = 'name',
  'delta.enableRowTracking' = 'true',
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'pipelines.externalMetadata.enabled' = 'true')
FLOW AUTO CDC FROM stream({BRONZE}.ice_cdc_raw)
  KEYS (customer_id)
  APPLY AS DELETE WHEN op = 'DELETE'
  SEQUENCE BY seq
  COLUMNS * EXCEPT (op, seq)
  STORED AS SCD TYPE 1
""")
spark.table(f"{BRONZE}.ice_customers_reads").orderBy("customer_id").show()

# COMMAND ----------

# Two more changes: a delete and a new customer. REFRESH takes only the new rows.
spark.sql(f"""INSERT INTO {BRONZE}.ice_cdc_raw VALUES
              ('C002', 'ben@example.com', 'DELETE', 4),
              ('C003', 'cleo@example.com', 'INSERT', 5)""")
spark.sql(f"REFRESH STREAMING TABLE {BRONZE}.ice_customers_reads")

rows = {r.customer_id: r.email for r in spark.table(f"{BRONZE}.ice_customers_reads").collect()}
assert rows == {"C001": "ana2@example.com", "C003": "cleo@example.com"}, rows
print("after the refresh:", rows)

# COMMAND ----------

# Read what we actually built: a Delta streaming table carrying Iceberg metadata
print("format:", table_format("ice_customers_reads"))
(spark.sql(f"DESCRIBE EXTENDED {BRONZE}.ice_customers_reads")
 .filter("col_name IN ('Type', 'Provider', '# Delta Uniform Iceberg', "
         "'Converted delta version')")
 .select("col_name", "data_type").show(truncate=40))
assert table_format("ice_customers_reads") == "delta"

# COMMAND ----------

# MAGIC %md
# MAGIC That's the honest answer to "Iceberg as the target". The table is **Delta inside**, and
# MAGIC Databricks publishes **Iceberg v3 metadata** beside it, so an Iceberg client reads it
# MAGIC through the Iceberg REST catalog with no copy of the data. Those readers can't write to
# MAGIC it, and we have no Iceberg client here to prove the read end. 📖
# MAGIC
# MAGIC ## 4. The obvious syntax, and why you check
# MAGIC
# MAGIC The docs list `USING ICEBERG` for materialized views, not for streaming tables. Try it
# MAGIC anyway, because someone on your team will.

# COMMAND ----------

spark.sql(f"""
CREATE OR REFRESH STREAMING TABLE {BRONZE}.ice_customers_using USING ICEBERG
FLOW AUTO CDC FROM stream({BRONZE}.ice_cdc_raw)
  KEYS (customer_id)
  APPLY AS DELETE WHEN op = 'DELETE'
  SEQUENCE BY seq
  COLUMNS * EXCEPT (op, seq)
  STORED AS SCD TYPE 1
""")
actual = table_format("ice_customers_using")
print(f"asked for ICEBERG, got: {actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC It succeeds, with no warning, and on the day this lesson was validated the table came
# MAGIC out as **Delta**. Measured on Free Edition, 1 November 2026, and not something the docs
# MAGIC promise either way. The lesson outlives the behavior: `DESCRIBE DETAIL`, don't assume.
# MAGIC
# MAGIC ## 5. A real managed Iceberg target, by hand
# MAGIC
# MAGIC If the target must genuinely be a managed Iceberg table, a `writeStream` straight into it
# MAGIC fails with `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`. `foreachBatch` with `MERGE` works,
# MAGIC because `MERGE` on Iceberg is an ordinary batch statement. You've written this in S4.

# COMMAND ----------

spark.sql(f"""CREATE TABLE {BRONZE}.ice_customers_merge
              (customer_id STRING, email STRING, seq BIGINT) USING ICEBERG""")

def upsert_changes(batch_df, batch_id):
    """Keep the newest change per customer, then merge it into the Iceberg target."""
    newest = Window.partitionBy("customer_id").orderBy(col("seq").desc())
    (batch_df.withColumn("rn", row_number().over(newest)).filter("rn = 1").drop("rn")
     .createOrReplaceTempView("latest_changes"))
    batch_df.sparkSession.sql(f"""
        MERGE INTO {BRONZE}.ice_customers_merge AS t
        USING latest_changes AS s ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
        WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET email = s.email, seq = s.seq
        WHEN NOT MATCHED AND s.op <> 'DELETE' THEN
          INSERT (customer_id, email, seq) VALUES (s.customer_id, s.email, s.seq)""")

(spark.readStream.table(f"{BRONZE}.ice_cdc_raw")
 .writeStream.foreachBatch(upsert_changes)
 .option("checkpointLocation", f"{CKPT}/merge")
 .trigger(availableNow=True)
 .start().awaitTermination())

merged = {r.customer_id: r.email for r in spark.table(f"{BRONZE}.ice_customers_merge").collect()}
assert merged == rows, merged
assert table_format("ice_customers_merge") == "iceberg"
print("same answer as AUTO CDC, in a real Iceberg table:", merged)

# COMMAND ----------

# MAGIC %md
# MAGIC Same rows as `AUTO CDC`. You now own the ordering, the replay and the deletes, which is
# MAGIC what `AUTO CDC` would have owned for you.
# MAGIC
# MAGIC ## 6. Spec versions are not a footnote
# MAGIC
# MAGIC `CLUSTER BY AUTO` and change data feed both need Iceberg **v3**. Asking a default table
# MAGIC for `CLUSTER BY AUTO` fails, and `table_changes` on it fails. On v3, both work.

# COMMAND ----------

spark.sql(f"""CREATE TABLE {BRONZE}.ice_v3 (id INT, status STRING)
              USING ICEBERG TBLPROPERTIES ('format-version' = 3) CLUSTER BY AUTO""")
spark.sql(f"INSERT INTO {BRONZE}.ice_v3 VALUES (1, 'new'), (2, 'new')")
spark.sql(f"DELETE FROM {BRONZE}.ice_v3 WHERE id = 2")
spark.sql(f"""SELECT id, status, _change_type
              FROM table_changes('{BRONZE}.ice_v3', 0) ORDER BY id, _change_type""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC | You need | Use | What you get |
# MAGIC |---|---|---|
# MAGIC | CDC with the least code, Iceberg clients read it | `AUTO CDC` + Iceberg v3 properties | Delta streaming table with Iceberg metadata, no copy |
# MAGIC | Older clients, or path-based access | Compatibility Mode | a read-only generated copy, updated later |
# MAGIC | A genuine managed Iceberg table as the target | `foreachBatch` + `MERGE` | real Iceberg, and you own the logic |
# MAGIC | Read someone else's Iceberg | foreign catalog via Federation | read-only |

# COMMAND ----------

for name in ("ice_orders", "ice_orders_copy", "ice_cdc_raw", "ice_customers_reads",
             "ice_customers_using", "ice_customers_merge", "ice_v3"):
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{name}")
dbutils.fs.rm(CKPT, True)
dbutils.notebook.exit("iceberg-complete")
