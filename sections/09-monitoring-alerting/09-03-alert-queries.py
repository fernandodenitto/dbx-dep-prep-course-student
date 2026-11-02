# Databricks notebook source
# MAGIC %md
# MAGIC # 9.3 — What an alert can watch
# MAGIC
# MAGIC The guide: "Use Databricks Lakehouse alerts to monitor governed business metrics, data quality and
# MAGIC anomaly signals, usage and cost, SQL warehouse/query health, audit and security events, AI agent
# MAGIC quality, and Lakeflow Job branching."
# MAGIC
# MAGIC The docs have no page called Lakehouse alerts. Their page on Databricks SQL alerts lists those same
# MAGIC seven uses, so we read the term as SQL alerts. That's our inference: no page states it.
# MAGIC
# MAGIC An alert is a query, so each use is a query you can run first. This notebook runs the ones Free Edition
# MAGIC can answer, and says which ones it can't. It creates no alert: the alert itself comes from
# MAGIC `alert/lakeshore-quarantine-alert.json`, or from the **Alerts** page.
# MAGIC
# MAGIC > Prereqs: lesson 2.6 (`gold.revenue_metrics`) and lesson 8.2 (`silver.sdp_orders_quarantine`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data quality: the alert we build

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS quarantined FROM lakeshore.silver.sdp_orders_quarantine;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Usage and cost: yesterday against the week before
# MAGIC
# MAGIC Condition: `ratio > 1.5`. A one-row result is what the alert's condition reads.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily AS (
# MAGIC   SELECT usage_date, sum(usage_quantity) AS dbus
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE usage_date >= current_date() - INTERVAL 8 DAYS
# MAGIC   GROUP BY usage_date
# MAGIC )
# MAGIC SELECT round(
# MAGIC          coalesce(max(CASE WHEN usage_date = current_date() - INTERVAL 1 DAY THEN dbus END), 0)
# MAGIC          / nullif(avg(CASE WHEN usage_date < current_date() - INTERVAL 1 DAY THEN dbus END), 0), 2) AS ratio
# MAGIC FROM daily;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL warehouse and query health: failed statements in the last day
# MAGIC
# MAGIC Condition: `failed_statements > 20`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS failed_statements
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 1 DAY
# MAGIC   AND execution_status = 'FAILED';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit and security: permission changes in the last day
# MAGIC
# MAGIC Condition: `permission_changes > 0`. Here zero tolerance is a defensible threshold, and it's the
# MAGIC one case in this lesson where `> 0` is the right call. System tables are typically an hour behind, so
# MAGIC this is a review signal, not an intrusion alarm.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS permission_changes
# MAGIC FROM system.access.audit
# MAGIC WHERE event_time >= current_timestamp() - INTERVAL 1 DAY
# MAGIC   AND service_name = 'unityCatalog'
# MAGIC   AND action_name = 'updatePermissions';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Governed business metrics: a metric view
# MAGIC
# MAGIC `gold.revenue_metrics` from lesson 2.6, by its full name. The worst month's share of booked revenue
# MAGIC that actually settled. Condition: `net_share < 0.6`. Run it first and set the threshold against your
# MAGIC own number. The refund arithmetic isn't restated here: change the definition in the metric view and
# MAGIC this alert follows. In production you'd also filter to recent months; our data is a fixed sample.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_month, MEASURE(net_share) AS net_share
# MAGIC FROM lakeshore.gold.revenue_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY net_share
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Anomaly signals: 📖 theory, not run here
# MAGIC
# MAGIC A threshold needs you to know the number. Anomaly detection (Public Preview) learns it per table:
# MAGIC **freshness** (is the next commit unusually late?) and **completeness** (are the last 24 hours' rows
# MAGIC below the predicted range?). You enable it per schema or catalog, and it has its own alert rules, in the
# MAGIC Data Quality Monitoring UI. Results land in `system.data_quality_monitoring.table_results`. On our Free
# MAGIC Edition workspace that system schema doesn't exist, and a course workspace has no history to train on, so
# MAGIC we can't show it. A hand-written threshold like query 1 is what you can practice.
# MAGIC
# MAGIC ## 7. AI agent quality: 📖 theory, not run here
# MAGIC
# MAGIC This course builds no agent. The docs' route is MLflow 3 production monitoring (Beta): scorers run on a
# MAGIC sample of an agent's traces and attach assessments to them. The SQL alerts page lists agent quality metrics
# MAGIC as a use, without showing the query, so we don't invent one. Scorers judge traces, not tables.
# MAGIC
# MAGIC ## 8. Job branching: the alert as a task
# MAGIC
# MAGIC That one isn't a query. It's `job/lakeshore-quarantine-branch.json` in Section 8's folder.
# MAGIC
# MAGIC ## 9. Evaluation history: alerts about alerts
# MAGIC
# MAGIC `system.alert` holds every alert's configuration and one row per evaluation. The docs say that by default
# MAGIC only users who are both account admin and metastore admin can read these tables. On our Free Edition
# MAGIC workspace they answered. `alerts` is a slowly changing dimension, like the job tables.

# COMMAND ----------

for table in ["alerts", "alert_evaluation_history"]:
    try:
        print(f"system.alert.{table}: {spark.table(f'system.alert.{table}').count()} rows")
    except Exception as err:
        print(f"system.alert.{table}: not readable here ({type(err).__name__})")

# COMMAND ----------

dbutils.notebook.exit("alert-queries-complete")
