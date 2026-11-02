-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 2.4 — Streaming tables vs materialized views
-- MAGIC
-- MAGIC **Run these statements in the SQL editor, on the SQL warehouse.** It's the
-- MAGIC generally available path and it shows each statement cleanly. A notebook on
-- MAGIC serverless compute can also create and refresh both objects (the docs label
-- MAGIC that Beta; lesson 2.6 uses it for a materialized view). One catch: refresh a
-- MAGIC standalone object on the compute type that created it.
-- MAGIC
-- MAGIC Both objects are "tables a pipeline maintains for you". The fork:
-- MAGIC - **Streaming table (ST)** — incremental ingestion from append-only
-- MAGIC   sources; each input row processed exactly once.
-- MAGIC - **Materialized view (MV)** — precomputed query results, refreshed to
-- MAGIC   stay correct when ANY input changes (incrementally when possible).
-- MAGIC
-- MAGIC > **Free Edition honesty:** pipeline compute is scarce here. If a CREATE
-- MAGIC > fails with `RESOURCE_EXHAUSTED`, recent jobs/refreshes are still
-- MAGIC > holding capacity — wait a few minutes and re-run. Create these two
-- MAGIC > objects one at a time, not together.

-- COMMAND ----------

-- Safe to re-run: clear what a run that stopped halfway left behind.
DROP TABLE IF EXISTS lakeshore.gold.st_orders_demo;
DROP MATERIALIZED VIEW IF EXISTS lakeshore.gold.mv_revenue_by_method_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. A streaming table: incremental ingestion
-- MAGIC The STREAM keyword makes read_files incremental: files are discovered
-- MAGIC and processed ONCE. Delta-typed columns come out (no manual casting).

-- COMMAND ----------

CREATE STREAMING TABLE lakeshore.gold.st_orders_demo
COMMENT 'S2 demo — dropped at the end of this lesson'
AS SELECT order_id, customer_id, order_ts, items
   FROM STREAM read_files(
     '/Volumes/lakeshore/landing/files/orders/',
     format => 'json');

-- COMMAND ----------

SELECT count(*) AS orders_ingested FROM lakeshore.gold.st_orders_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Refresh semantics — the heart of the fork
-- MAGIC Re-running a refresh does NOT reprocess old files. Only files that
-- MAGIC arrived since the last refresh are ingested. Each input row is handled once.

-- COMMAND ----------

REFRESH STREAMING TABLE lakeshore.gold.st_orders_demo;

-- COMMAND ----------

-- Same count as before: no new files, no reprocessing, no duplicates
SELECT count(*) AS orders_after_refresh FROM lakeshore.gold.st_orders_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. A materialized view: precomputed correctness
-- MAGIC The MV stores the RESULT of the query. When inputs change — any input,
-- MAGIC including dimensions in joins — a refresh brings the result back to
-- MAGIC truth, incrementally when the engine can prove it's safe.

-- COMMAND ----------

CREATE MATERIALIZED VIEW lakeshore.gold.mv_revenue_by_method_demo
COMMENT 'S2 demo — dropped at the end of this lesson'
AS SELECT method,
          count(*)              AS payments,
          round(sum(amount), 2) AS revenue
   FROM lakeshore.silver.payments
   WHERE status = 'completed'
   GROUP BY method;

-- COMMAND ----------

SELECT * FROM lakeshore.gold.mv_revenue_by_method_demo ORDER BY revenue DESC;

-- COMMAND ----------

REFRESH MATERIALIZED VIEW lakeshore.gold.mv_revenue_by_method_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Keeping them fresh: you choose the trigger
-- MAGIC Neither object refreshes by itself unless you say so. Three options, written
-- MAGIC here as text so this lesson doesn't leave a schedule running:
-- MAGIC ```sql
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW m SCHEDULE EVERY 1 DAY AS SELECT ...;
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW m SCHEDULE CRON '0 30 3 * * ?' AT TIME ZONE 'UTC' AS SELECT ...;
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW m TRIGGER ON UPDATE AS SELECT ...;
-- MAGIC ```
-- MAGIC `TRIGGER ON UPDATE` fits upstreams with no predictable schedule. Streaming
-- MAGIC tables take `SCHEDULE` the same way. A Lakeflow Job calling `REFRESH` is the
-- MAGIC other common answer, and Section 8 shows it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Same machinery, different contract
-- MAGIC DESCRIBE EXTENDED shows both are pipeline-maintained UC objects —
-- MAGIC the difference is the processing contract, not the plumbing.

-- COMMAND ----------

DESCRIBE EXTENDED lakeshore.gold.st_orders_demo;

-- COMMAND ----------

DESCRIBE EXTENDED lakeshore.gold.mv_revenue_by_method_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Clean up
-- MAGIC These were teaching objects. The course's streaming tables live inside the
-- MAGIC Section 8 pipeline, where one pipeline's compute maintains many tables, which
-- MAGIC respects both Free Edition's capacity and a production budget.

-- COMMAND ----------

DROP TABLE IF EXISTS lakeshore.gold.st_orders_demo;
DROP MATERIALIZED VIEW IF EXISTS lakeshore.gold.mv_revenue_by_method_demo;
