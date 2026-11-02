# Databricks notebook source
# MAGIC %md
# MAGIC # 8.7 — Job task: notify_ops
# MAGIC
# MAGIC The job's one task that isn't safe to run twice. It stands in for an email or a call to a paging
# MAGIC API: every execution "sends" one more message, so a retry after a late failure sends a duplicate.
# MAGIC Here "sending" means appending a row to `gold.platform_notifications`, so you can count them.
# MAGIC
# MAGIC It also needs a package from outside the serverless base environment. In the job that package
# MAGIC comes from the `team_env` environment. Run this notebook by hand with no `%pip` and no notebook
# MAGIC dependency, and the import fails: that's the point of lesson 8.7's first knob.

# COMMAND ----------

import humanize

dbutils.widgets.text("run_id", "manual")
run_id = dbutils.widgets.get("run_id")

valid_orders = spark.table("lakeshore.silver.sdp_orders_valid").count()
message = f"Platform refresh finished: {humanize.intcomma(valid_orders)} valid orders"

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.gold.platform_notifications (
               run_id STRING, sent_at TIMESTAMP, message STRING)""")

spark.sql("""INSERT INTO lakeshore.gold.platform_notifications
             VALUES (:run_id, current_timestamp(), :message)""",
          args={"run_id": run_id, "message": message})
print(message)

# COMMAND ----------

sent = spark.sql("""SELECT count(*) FROM lakeshore.gold.platform_notifications
                    WHERE run_id = :run_id""", args={"run_id": run_id}).first()[0]
print(f"messages for run {run_id}: {sent}. More than one means this task ran twice.")

# COMMAND ----------

dbutils.notebook.exit(f"notified:{run_id}")
