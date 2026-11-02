# Databricks notebook source
# MAGIC %md
# MAGIC # 9.3 — Job task: summarize_rejects
# MAGIC
# MAGIC The branch that runs only when the alert task reports TRIGGERED. The job in
# MAGIC `sections/08-production-pipelines/job/lakeshore-quarantine-branch.json` wires it up.

# COMMAND ----------

display(spark.sql("""SELECT reason, count(*) AS records
                     FROM lakeshore.silver.sdp_orders_quarantine
                     GROUP BY reason ORDER BY records DESC"""))

# COMMAND ----------

dbutils.notebook.exit("rejects-summarized")
