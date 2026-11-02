# Databricks notebook source
# MAGIC %md
# MAGIC # 8.6 — Job task: per_topic (the For each body)
# MAGIC One iteration per bus topic. The For each task passes the topic name as `{{input}}`
# MAGIC into this widget.

# COMMAND ----------

dbutils.widgets.text("topic", "orders")
topic = dbutils.widgets.get("topic")

stats = spark.sql(f"""
    SELECT count(*)      AS records,
           max(event_ts) AS latest_event
    FROM lakeshore.bronze.sdp_bus
    WHERE topic = '{topic}'
""").first()

print(f"[{topic}] records={stats.records} · latest={stats.latest_event}")
assert stats.records > 0, f"topic {topic} has no records — feed broken?"

# COMMAND ----------

dbutils.notebook.exit(f"{topic}:{stats.records}")
