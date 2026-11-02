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
# MAGIC The shape, from the docs' alert patterns. Reference the metric view by its full name and aggregate
# MAGIC with `MEASURE`, so the definition of revenue stays in one governed place:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT MEASURE(net_revenue) AS net_revenue
# MAGIC FROM <catalog>.<schema>.<metric_view>
# MAGIC WHERE order_date = current_date() - INTERVAL 1 DAY
# MAGIC ```
# MAGIC
# MAGIC ## 6. Anomaly signals: 📖 not run here
# MAGIC
# MAGIC Anomaly detection has its own alert rules, set in the Data Quality Monitoring UI per schema or catalog.
# MAGIC On our Free Edition workspace the system schema behind its results, `system.data_quality_monitoring`,
# MAGIC doesn't exist, so we can't show it. A hand-written threshold like query 1 is what you can practice.
# MAGIC
# MAGIC ## 7. AI agent quality: 📖 not run here
# MAGIC
# MAGIC This course builds no agent. The docs' route is production monitoring with MLflow scorers, then an alert
# MAGIC on those metrics. Know that it's on the guide's list, and that it's still the same mechanism.
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
