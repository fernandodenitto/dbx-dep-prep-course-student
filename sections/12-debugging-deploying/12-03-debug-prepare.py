# Databricks notebook source
# MAGIC %md
# MAGIC # 12.3 Debug lab, task 1 of 3: prepare
# MAGIC The upstream task. It always succeeds, and it is expensive in real life, so the point of
# MAGIC this lab is that a repair never runs it twice.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakeshore")
dbutils.widgets.text("schema", "gold")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}'

revenue = (spark.table(f"{target}.fact_order_items")
           .groupBy("product_id").sum("line_total")
           .withColumnRenamed("sum(line_total)", "revenue"))

# Overwrite, not append: a repair re-runs a failed task from the beginning, so a task that
# appends can duplicate its output. This one is safe to run again.
revenue.write.mode("overwrite").saveAsTable(f"{target}.debug_lab_revenue")

products = spark.table(f"{target}.debug_lab_revenue").count()
dbutils.jobs.taskValues.set(key="products", value=products)
print(f"prepared revenue for {products} products")
