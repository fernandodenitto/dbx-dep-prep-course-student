# Databricks notebook source
# MAGIC %md
# MAGIC # 7.3 — Deletion vectors & the write path
# MAGIC
# MAGIC Old Delta truth: "modifying a single row requires rewriting the entire
# MAGIC Parquet file containing that record." Deletion vectors retire it:
# MAGIC deletes become **metadata markers**, reads apply them at query time,
# MAGIC and the rewrite happens when YOU choose. Let's watch files not move.
# MAGIC
# MAGIC > Demo table `bronze.dv_demo`, dropped at the end.

# COMMAND ----------

from pyspark.sql.functions import col

spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.dv_demo")
spark.sql("""CREATE TABLE lakeshore.bronze.dv_demo
             (id BIGINT, device_id INT, reading DOUBLE)
             TBLPROPERTIES (delta.enableDeletionVectors = true)""")
(spark.range(500_000)
 .withColumn("device_id", (col("id") % 1000).cast("int"))
 .withColumn("reading", (col("id") % 997) / 10.0)
 .write.mode("append").saveAsTable("lakeshore.bronze.dv_demo"))

before = spark.sql("DESCRIBE DETAIL lakeshore.bronze.dv_demo").first()
print(f"before: {before.numFiles} files, {spark.table('lakeshore.bronze.dv_demo').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DELETE without rewriting

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM lakeshore.bronze.dv_demo WHERE device_id = 7;

# COMMAND ----------

after = spark.sql("DESCRIBE DETAIL lakeshore.bronze.dv_demo").first()
n = spark.table("lakeshore.bronze.dv_demo").count()
print(f"after DELETE: {after.numFiles} files (was {before.numFiles}), {n:,} rows")
assert after.numFiles == before.numFiles, "files should NOT have been rewritten"
assert n == 499_500, n

# COMMAND ----------

# MAGIC %md
# MAGIC 500 rows gone from queries; **file count identical**. The rows are
# MAGIC marked in a deletion vector; every read "applies the deletion vector
# MAGIC entries at query time to resolve the current table state". The proof
# MAGIC in the log:

# COMMAND ----------

from pyspark.sql.functions import expr

(spark.sql("DESCRIBE HISTORY lakeshore.bronze.dv_demo")
 .filter("operation = 'DELETE'")
 .select("version", "operation",
         expr("operationMetrics['numDeletionVectorsAdded']").alias("dvs_added"),
         expr("operationMetrics['numAddedFiles']").alias("files_added"),
         expr("operationMetrics['numRemovedFiles']").alias("files_removed"))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC Deletion vectors added; zero (or near-zero) files rewritten. The same
# MAGIC machinery accelerates `UPDATE` and `MERGE` — instead of rewriting a
# MAGIC 1 GB file to change one row, mark and move on.
# MAGIC
# MAGIC ## 2. The other shoe: soft-deleted ≠ gone
# MAGIC
# MAGIC The bytes are still in the Parquet files. Two-step physical removal —
# MAGIC and Section 10 will need this for compliance:
# MAGIC 1. `REORG TABLE … APPLY (PURGE)` — rewrites files, materializing the
# MAGIC    deletes (DVs gone)
# MAGIC 2. `VACUUM` — removes the superseded files once past retention

# COMMAND ----------

# MAGIC %sql
# MAGIC REORG TABLE lakeshore.bronze.dv_demo APPLY (PURGE);

# COMMAND ----------

purged = spark.sql("DESCRIBE DETAIL lakeshore.bronze.dv_demo").first()
n2 = spark.table("lakeshore.bronze.dv_demo").count()
print(f"after PURGE: {purged.numFiles} files, {n2:,} rows")
assert n2 == 499_500
# VACUUM would now remove the old files after retention:
spark.sql("VACUUM lakeshore.bronze.dv_demo")
print("VACUUM submitted (default retention respected — old files age out)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cost accounting — when DVs win, and their bill
# MAGIC
# MAGIC | | Without DVs | With DVs |
# MAGIC |---|---|---|
# MAGIC | 1-row DELETE in 1 GB file | rewrite 1 GB | write a marker |
# MAGIC | Read path | plain scan | scan + apply DV (small tax) |
# MAGIC | Physical removal | immediate | REORG PURGE + VACUUM later |
# MAGIC | Compliance "erase now" | done at delete | **NOT done** until purge |
# MAGIC
# MAGIC The first row is the scenario the guide samples: frequent small `MERGE`s
# MAGIC rewriting big files means deletion vectors. The last row is the fine print:
# MAGIC with DVs, `DELETE` alone does not physically remove PII. Section 10 builds
# MAGIC the full compliant purge.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lakeshore.bronze.dv_demo")
print("demo dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC DVs turn row modifications into metadata: fast DELETE/UPDATE/MERGE,
# MAGIC tiny read tax, and a two-step path (REORG PURGE → VACUUM) when bytes
# MAGIC must truly die. Next: data skipping, and the per-file statistics it
# MAGIC runs on.

# COMMAND ----------

dbutils.notebook.exit("deletion-vectors-complete")
