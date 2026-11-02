# Databricks notebook source
# MAGIC %md
# MAGIC # 7.1 — What predictive optimization did on your tables
# MAGIC
# MAGIC Slides say "the service runs OPTIMIZE, VACUUM and ANALYZE for you". This workspace can
# MAGIC show you the receipts. Read-only: nothing here changes a table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Is it on for this table, and who decided?

# COMMAND ----------

(spark.sql("DESCRIBE EXTENDED lakeshore.silver.payments")
 .filter("col_name = 'Predictive Optimization'")
 .select("col_name", "data_type")
 .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC The value tells you the setting and where it's inherited from. It's a chain: account,
# MAGIC then catalog, schema, table. Any level can override with
# MAGIC `ALTER { CATALOG | SCHEMA | TABLE } name { ENABLE | DISABLE | INHERIT } PREDICTIVE OPTIMIZATION`.
# MAGIC
# MAGIC ## 2. What it actually did
# MAGIC
# MAGIC System tables lag, and a table you created an hour ago has no history yet. An empty result
# MAGIC here means "not yet", not "broken".

# COMMAND ----------

ops = spark.sql("""
    SELECT schema_name, table_name, operation_type, operation_status,
           count(*) AS runs, max(end_time) AS last_run
    FROM system.storage.predictive_optimization_operations_history
    WHERE catalog_name = 'lakeshore'
    GROUP BY ALL
    ORDER BY last_run DESC
    LIMIT 20""")
ops.show(truncate=False)
print(f"{ops.count()} table and operation combinations so far")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Same tables you built, maintained by something you never scheduled, and billed as
# MAGIC serverless jobs. Next: the layout decision that maintenance works on.

# COMMAND ----------

dbutils.notebook.exit("predictive-optimization-complete")
