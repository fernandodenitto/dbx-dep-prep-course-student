# Databricks notebook source
# MAGIC %md
# MAGIC # 8.6 — Job task: quality_gate
# MAGIC Runs after the pipeline refresh. Checks the quarantine level and publishes a task
# MAGIC value that the downstream If/else condition task reads. The `max_quarantine` job
# MAGIC parameter reaches this widget: set it to 0 for a run to see the false branch.

# COMMAND ----------

dbutils.widgets.text("max_quarantine", "10")
threshold = int(dbutils.widgets.get("max_quarantine"))

quarantined = spark.table("lakeshore.silver.sdp_orders_quarantine").count()
ok = quarantined <= threshold

# The task-to-task channel: downstream tasks read this via
# {{tasks.quality_gate.values.quarantine_ok}}
dbutils.jobs.taskValues.set(key="quarantine_ok", value=str(ok).lower())
dbutils.jobs.taskValues.set(key="quarantine_count", value=str(quarantined))

print(f"quarantined={quarantined} · threshold={threshold} · ok={ok}")

# COMMAND ----------

dbutils.notebook.exit(f"gate:{'ok' if ok else 'breach'}:{quarantined}")
