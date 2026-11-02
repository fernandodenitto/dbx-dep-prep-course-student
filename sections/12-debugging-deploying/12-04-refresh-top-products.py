# Databricks notebook source
# MAGIC %md
# MAGIC # 12.4 The ETL task behind `lakeshore-top-products`
# MAGIC A small, idempotent refresh. Lesson 12.4 creates a job around it three ways: UI, CLI, REST.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakeshore")
dbutils.widgets.text("schema", "gold")
dbutils.widgets.text("top_n", "5")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}'
top_n = int(dbutils.widgets.get("top_n"))
assert top_n > 0, f"top_n must be positive, got {top_n}"

# COMMAND ----------

top = (spark.table(f"{target}.fact_order_items")
       .groupBy("product_id").sum("line_total")
       .withColumnRenamed("sum(line_total)", "revenue")
       .orderBy("revenue", ascending=False)
       .limit(top_n))
top.write.mode("overwrite").saveAsTable(f"{target}.top_products")

# COMMAND ----------

dbutils.notebook.exit(f"top-products-ok:{top.count()}")
