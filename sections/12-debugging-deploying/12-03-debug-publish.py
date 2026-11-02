# Databricks notebook source
# MAGIC %md
# MAGIC # 12.3 Debug lab, task 3 of 3: publish
# MAGIC The downstream task. It depends on rank_top, so when rank_top fails this one is skipped,
# MAGIC and a repair runs it. It reads a task value set by `prepare`, which a repair does not
# MAGIC re-run: the value from the original attempt is still there.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakeshore")
dbutils.widgets.text("schema", "gold")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}'

products = dbutils.jobs.taskValues.get(taskKey="prepare", key="products", debugValue=-1)
published = spark.table(f"{target}.debug_lab_top").count()
assert published > 0, "nothing to publish"

dbutils.notebook.exit(f"debug-lab-ok:top{published}:of{products}")
