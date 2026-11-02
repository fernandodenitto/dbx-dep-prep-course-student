# Databricks notebook source
# MAGIC %md
# MAGIC # 12.3 Debug lab, task 2 of 3: rank_top
# MAGIC The breakable task: lesson 5.5's fail-fast discipline, seen from the operator's side.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakeshore")
dbutils.widgets.text("schema", "gold")
dbutils.widgets.text("top_n", "5")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}'
raw = dbutils.widgets.get("top_n")

top_n = int(raw)                      # a bad type dies HERE, loudly
assert top_n > 0, (
    f"top_n must be positive, got {top_n}: "
    "this run was started with an invalid parameter override")

# COMMAND ----------

top = (spark.table(f"{target}.debug_lab_revenue")
       .orderBy("revenue", ascending=False)
       .limit(top_n))
top.write.mode("overwrite").saveAsTable(f"{target}.debug_lab_top")
top.show()
