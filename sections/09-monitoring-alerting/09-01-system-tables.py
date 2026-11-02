# Databricks notebook source
# MAGIC %md
# MAGIC # 9.1 — System tables: the platform's black box recorder
# MAGIC
# MAGIC Everything this course has done, every job run, every pipeline update, every query, was recorded.
# MAGIC `system.*` is where. The guide's statement: "Use Databricks system tables (billing, compute, access,
# MAGIC lakeflow) for cost analysis, auditing, and workload monitoring." Four schemas, plus `query`.
# MAGIC
# MAGIC **What Free Edition lets you see** (measured on our Free Edition workspace, 2026-11-01). All of these
# MAGIC answered: `billing.usage`, `billing.list_prices`, `lakeflow.*`, `access.audit`, the lineage tables,
# MAGIC `query.history`, `compute.warehouses`, `compute.warehouse_events`, `compute.node_types`. Two answered
# MAGIC with **zero rows**, correctly: `compute.clusters` and `compute.node_timeline` record classic compute,
# MAGIC and Free Edition has none. Your workspace may differ, so section 2 counts them for you.
# MAGIC
# MAGIC > Latency: the docs say records are typically available within an hour, longer for new workspaces.
# MAGIC > We query patterns over 30 days, not exact counts, and nothing here asserts a number.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cost: system.billing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT usage_date, sku_name,
# MAGIC        round(sum(usage_quantity), 3) AS dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date DESC, dbus DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBUs to money: usage meets the price in force when it was consumed
# MAGIC SELECT u.sku_name,
# MAGIC        round(sum(u.usage_quantity), 2)                     AS dbus,
# MAGIC        round(sum(u.usage_quantity * p.pricing.default), 2) AS usd_at_list_price
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC  AND u.usage_start_time >= p.price_start_time
# MAGIC  AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY u.sku_name ORDER BY usd_at_list_price DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC You aren't charged on Free Edition, but this number isn't zero: it's what your usage would cost at
# MAGIC list price. The query is the one you'd run on a real bill.
# MAGIC
# MAGIC **Attribution** is the step after "how much": which job spent it? `usage_metadata` carries the
# MAGIC `job_id`, `warehouse_id` or `dlt_pipeline_id` behind each usage row, and the name lives in `lakeflow.jobs`,
# MAGIC which is a slowly changing dimension: a job has one row per change. Take the latest row first, or
# MAGIC every edit of a job multiplies its cost.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_jobs AS (
# MAGIC   SELECT workspace_id, job_id, name
# MAGIC   FROM system.lakeflow.jobs
# MAGIC   QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
# MAGIC )
# MAGIC SELECT j.name AS job_name,
# MAGIC        round(sum(u.usage_quantity), 2) AS dbus
# MAGIC FROM system.billing.usage u
# MAGIC JOIN latest_jobs j
# MAGIC   ON u.workspace_id = j.workspace_id AND u.usage_metadata.job_id = j.job_id
# MAGIC WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY j.name ORDER BY dbus DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute: system.compute, and what it doesn't cover
# MAGIC
# MAGIC The docs are explicit: `clusters` and `node_timeline` record classic all-purpose, jobs and pipeline
# MAGIC compute. Serverless and SQL warehouses aren't in there. Warehouses have their own two tables.

# COMMAND ----------

for table in ["clusters", "node_timeline", "node_types", "warehouses", "warehouse_events"]:
    rows = spark.table(f"system.compute.{table}").count()
    print(f"system.compute.{table:<18} {rows:>8} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Warehouses are a slowly changing dimension too: latest row per warehouse
# MAGIC SELECT warehouse_name, warehouse_type, warehouse_size, auto_stop_minutes
# MAGIC FROM system.compute.warehouses
# MAGIC QUALIFY row_number() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How often did it start? Every STARTING is a cold start somebody waited for.
# MAGIC SELECT event_type, count(*) AS events
# MAGIC FROM system.compute.warehouse_events
# MAGIC WHERE event_time >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY event_type ORDER BY events DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC On a paid workspace with classic compute you'd read `node_timeline` for CPU and memory per minute,
# MAGIC and join `billing.usage` to `clusters` on `usage_metadata.cluster_id`. You can't run that here:
# MAGIC there's nothing in those tables to read.
# MAGIC
# MAGIC ## 3. Workloads: system.lakeflow
# MAGIC
# MAGIC Two facts make the obvious query wrong, and the exam can test both:
# MAGIC
# MAGIC 1. `lakeflow.jobs` is a slowly changing dimension. Join it raw and every run multiplies by the number
# MAGIC    of times the job was edited.
# MAGIC 2. `job_run_timeline` slices long runs: each row records up to an hour. A run's duration is the
# MAGIC    **sum of its rows**, and `result_state` sits only on the row that ends the run.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_jobs AS (
# MAGIC   SELECT workspace_id, job_id, name
# MAGIC   FROM system.lakeflow.jobs
# MAGIC   QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
# MAGIC ),
# MAGIC runs AS (
# MAGIC   SELECT workspace_id, job_id, run_id,
# MAGIC          min(period_start_time) AS started,
# MAGIC          sum(timestampdiff(SECOND, period_start_time, period_end_time)) AS seconds,
# MAGIC          max_by(result_state, period_end_time) AS result_state
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY workspace_id, job_id, run_id
# MAGIC )
# MAGIC SELECT j.name, r.started, r.result_state, r.seconds
# MAGIC FROM runs r JOIN latest_jobs j USING (workspace_id, job_id)
# MAGIC ORDER BY r.started DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC Your jobs and every other run in this workspace, one row per run. Duration trends are the professional
# MAGIC signal: a job that succeeds in four times its usual time is failing slowly. Here's the same data per
# MAGIC job, and `job_task_run_timeline` tells you which task got slower.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH runs AS (
# MAGIC   SELECT workspace_id, job_id, run_id,
# MAGIC          sum(timestampdiff(SECOND, period_start_time, period_end_time)) AS seconds
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY workspace_id, job_id, run_id
# MAGIC )
# MAGIC SELECT job_id, count(*) AS runs,
# MAGIC        round(avg(seconds)) AS avg_seconds,
# MAGIC        percentile(seconds, 0.9) AS p90_seconds
# MAGIC FROM runs GROUP BY job_id ORDER BY avg_seconds DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipelines have the same pair since October: lakeflow.pipelines (a slowly changing dimension)
# MAGIC -- and lakeflow.pipeline_update_timeline
# MAGIC WITH latest_pipelines AS (
# MAGIC   SELECT workspace_id, pipeline_id, name
# MAGIC   FROM system.lakeflow.pipelines
# MAGIC   QUALIFY row_number() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
# MAGIC )
# MAGIC SELECT p.name, t.update_id,
# MAGIC        min(t.period_start_time) AS started,
# MAGIC        sum(timestampdiff(SECOND, t.period_start_time, t.period_end_time)) AS seconds,
# MAGIC        max_by(t.result_state, t.period_end_time) AS result_state
# MAGIC FROM system.lakeflow.pipeline_update_timeline t
# MAGIC JOIN latest_pipelines p USING (workspace_id, pipeline_id)
# MAGIC WHERE t.period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY p.name, t.update_id
# MAGIC ORDER BY started DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Queries: system.query.history
# MAGIC
# MAGIC Slowest statements of the last 30 days. If you ran lesson 7.5, its wasteful query may be near the
# MAGIC top, and that's monitoring catching what profiling explained.
# MAGIC
# MAGIC > At work this column may read `<REDACTED>`: per the docs, `statement_text` needs an account admin or
# MAGIC > membership in the `databricks_pii_access` group. On our Free Edition workspace it came back readable.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT executed_by, statement_type, execution_status,
# MAGIC        round(total_duration_ms / 1000.0, 1) AS seconds,
# MAGIC        left(statement_text, 60)             AS query_start
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit: system.access.audit
# MAGIC
# MAGIC Who did what, when, to which object. In this list, allowing for latency: the moment you created
# MAGIC `lakeshore-platform-sdp` and `lakeshore-platform-refresh`, under your own email.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, user_identity.email AS actor, service_name, action_name,
# MAGIC        request_params['name'] AS object_name
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND service_name IN ('deltaPipelines', 'jobs')
# MAGIC   AND action_name IN ('create', 'delete')
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 15;

# COMMAND ----------

bill = spark.sql("""SELECT count(*) FROM system.billing.usage
                    WHERE usage_date >= current_date() - INTERVAL 30 DAYS""").first()[0]
runs = spark.sql("""SELECT count(DISTINCT run_id) FROM system.lakeflow.job_run_timeline
                    WHERE period_start_time >= current_date() - INTERVAL 30 DAYS""").first()[0]
print(f"billing rows (30d): {bill} · job runs (30d): {runs}")
if not (bill and runs):
    print("Empty so far: a new workspace can take a while. Come back in an hour.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Cost → `billing`. Compute configuration and classic node metrics → `compute`, with warehouses in
# MAGIC their own two tables. Workloads → `lakeflow`: latest row of the dimension, sum the slices. Queries →
# MAGIC `query.history`. Who did what → `access.audit`, plus lineage. Governed like any catalog: grants apply.
# MAGIC Next: the pipeline's own diary, the event log.

# COMMAND ----------

dbutils.notebook.exit("system-tables-complete")
