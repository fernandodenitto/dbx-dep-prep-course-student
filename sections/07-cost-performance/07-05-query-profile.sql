-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 7.5 — Query Profile deep dive
-- MAGIC
-- MAGIC The guide: "Use the query profile to identify performance bottlenecks, such as
-- MAGIC data-skipping inefficiencies, join strategies, and shuffle operations." So we build one
-- MAGIC query of each kind and read it.
-- MAGIC
-- MAGIC **Run this in the SQL editor, or attach this notebook to a SQL warehouse or serverless.**
-- MAGIC After a query, open its profile: in the SQL editor, the link under the results →
-- MAGIC **See query profile**; in a notebook, **See performance** under the cell → the statement
-- MAGIC → **See query profile**. Or **Query History** in the sidebar.
-- MAGIC
-- MAGIC > Identical re-runs may be answered from the query result cache, and those have no
-- MAGIC > profile. Change anything trivial if you see "Query profile is not available". Lesson 7.8
-- MAGIC > explains why.
-- MAGIC >
-- MAGIC > Demo tables (`bronze.qp_*`), dropped at the end. Numbers below are from this seed.

-- COMMAND ----------

-- Six monthly loads into a table clustered by date: six files with tight date ranges,
-- which is what gives skipping something to skip. Auto compaction off, so the demo holds still.
CREATE OR REPLACE TABLE lakeshore.bronze.qp_sales
  (sale_id BIGINT, account_id BIGINT, product_id INT, sale_date DATE, amount DOUBLE)
  CLUSTER BY (sale_date)
  TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'false');

INSERT INTO lakeshore.bronze.qp_sales
SELECT id, pmod(hash(id, 1), 2000000), pmod(hash(id, 2), 500),
       date_add(DATE'2026-01-01', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);
INSERT INTO lakeshore.bronze.qp_sales
SELECT id + 1000000, pmod(hash(id, 3), 2000000), pmod(hash(id, 4), 500),
       date_add(DATE'2026-01-31', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);
INSERT INTO lakeshore.bronze.qp_sales
SELECT id + 2000000, pmod(hash(id, 5), 2000000), pmod(hash(id, 6), 500),
       date_add(DATE'2026-03-02', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);
INSERT INTO lakeshore.bronze.qp_sales
SELECT id + 3000000, pmod(hash(id, 7), 2000000), pmod(hash(id, 8), 500),
       date_add(DATE'2026-04-01', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);
INSERT INTO lakeshore.bronze.qp_sales
SELECT id + 4000000, pmod(hash(id, 9), 2000000), pmod(hash(id, 10), 500),
       date_add(DATE'2026-05-01', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);
INSERT INTO lakeshore.bronze.qp_sales
SELECT id + 5000000, pmod(hash(id, 11), 2000000), pmod(hash(id, 12), 500),
       date_add(DATE'2026-05-31', CAST(id % 30 AS INT)), (id % 997) / 10.0
FROM range(833334);

DESCRIBE DETAIL lakeshore.bronze.qp_sales;

-- COMMAND ----------

CREATE OR REPLACE TABLE lakeshore.bronze.qp_products AS
SELECT id AS product_id, concat('product_', id) AS product_name, CAST(id % 10 AS INT) AS category
FROM range(500);

-- COMMAND ----------

CREATE OR REPLACE TABLE lakeshore.bronze.qp_accounts AS
SELECT id AS account_id, concat('acct_', id) AS account_name, repeat('x', 200) AS notes,
       CAST(id % 40 AS INT) AS region
FROM range(4000000);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. The wasteful twin
-- MAGIC A pseudo-join through a hash bucket, and a filter wrapped in a function. Each sin is small
-- MAGIC enough to survive review. Together, they're a money fire.

-- COMMAND ----------

SELECT d.category, count(*) AS row_pairs, round(sum(f.amount), 1) AS total
FROM lakeshore.bronze.qp_sales f
JOIN lakeshore.bronze.qp_products d
  ON pmod(hash(f.product_id), 50) = pmod(hash(d.product_id), 50)
WHERE date_format(f.sale_date, 'yyyy-MM-dd') = '2026-03-10'
GROUP BY d.category ORDER BY d.category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open its profile. What to point at on screen:
-- MAGIC - **Query wall-clock duration vs Aggregated task time**: task time is summed across every
-- MAGIC   core, so it can dwarf wall-clock when work is parallel.
-- MAGIC - **Rows per operator** in the graph: the join emits far more rows than the scan fed it.
-- MAGIC   Buckets collide, so products in one bucket all pair with each other.
-- MAGIC - **The filter icon on the scan metrics**, the percent of data pruned:
-- MAGIC   `date_format(sale_date, ...)` wraps the column, so the file stats can't help.
-- MAGIC
-- MAGIC ## 2. The disciplined twin

-- COMMAND ----------

SELECT d.category, count(*) AS row_pairs, round(sum(f.amount), 1) AS total
FROM lakeshore.bronze.qp_sales f
JOIN lakeshore.bronze.qp_products d
  ON f.product_id = d.product_id
WHERE f.sale_date = DATE'2026-03-10'
GROUP BY d.category ORDER BY d.category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Look at `row_pairs`: about a tenth of twin A's, and the totals differ too. Twin A was
-- MAGIC wrong as well as expensive, in valid-looking rows, and only the profile and a sanity check
-- MAGIC tell you. Twin B has a real key, a bare column the min and max stats can evaluate, on a
-- MAGIC table of six date-ranged files, and no exploding operator.
-- MAGIC
-- MAGIC ## 3. Join strategies: read the operator name
-- MAGIC
-- MAGIC `EXPLAIN` prints the plan the optimizer chose before running. The profile shows the same
-- MAGIC operator with its real metrics after. Serverless blocks the threshold settings
-- MAGIC (`spark.sql.autoBroadcastJoinThreshold` fails with `CONFIG_NOT_AVAILABLE`), so we read
-- MAGIC strategy, and use hints only to see the difference.

-- COMMAND ----------

-- A 500-row dimension: expect a broadcast hash join. The dimension is sent to every task,
-- and the 5M-row side doesn't shuffle to reach the join.
EXPLAIN
SELECT d.category, sum(f.amount) AS total
FROM lakeshore.bronze.qp_sales f JOIN lakeshore.bronze.qp_products d
  ON f.product_id = d.product_id
GROUP BY d.category;

-- COMMAND ----------

-- Two big sides: both must shuffle on the key to meet. Look for ShuffledHashJoin.
-- The optimizer decides from size estimates, so shrink this table and it broadcasts instead.
EXPLAIN
SELECT a.region, sum(f.amount) AS total
FROM lakeshore.bronze.qp_sales f JOIN lakeshore.bronze.qp_accounts a
  ON f.account_id = a.account_id
GROUP BY a.region;

-- COMMAND ----------

-- The same small join, forced to the third strategy with a hint: SortMergeJoin.
EXPLAIN
SELECT /*+ MERGE(d) */ d.category, sum(f.amount) AS total
FROM lakeshore.bronze.qp_sales f JOIN lakeshore.bronze.qp_products d
  ON f.product_id = d.product_id
GROUP BY d.category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Three operators to recognise. On Photon, which serverless uses, they carry a `Photon`
-- MAGIC prefix where Photon runs them (`PhotonBroadcastHashJoin`, `PhotonShuffledHashJoin`),
-- MAGIC and a two-big-tables join came out as a *shuffled hash* join here, not the sort-merge
-- MAGIC join older material assumes. `SortMergeJoin` only appeared when we hinted it.
-- MAGIC
-- MAGIC | Operator | When | Cost signature in the profile |
-- MAGIC |---|---|---|
-- MAGIC | Broadcast hash join | one side small | no shuffle on the big side; memory on the build side |
-- MAGIC | Shuffled hash join | both big, one side can hash | Shuffle on both inputs |
-- MAGIC | Sort-merge join | both big, or hinted | Shuffle on both inputs, plus sorts |
-- MAGIC
-- MAGIC Run the two big ones now, and read the **Shuffle** operators. The docs say it plainly:
-- MAGIC shuffle operations are expensive because they move data between executors.

-- COMMAND ----------

SELECT a.region, round(sum(f.amount), 1) AS total
FROM lakeshore.bronze.qp_sales f JOIN lakeshore.bronze.qp_accounts a
  ON f.account_id = a.account_id
GROUP BY a.region ORDER BY a.region;

-- COMMAND ----------

SELECT /*+ MERGE(d) */ d.category, round(sum(f.amount), 1) AS total
FROM lakeshore.bronze.qp_sales f JOIN lakeshore.bronze.qp_products d
  ON f.product_id = d.product_id
GROUP BY d.category ORDER BY d.category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In each profile: open **Top operators** for the most expensive steps, and the
-- MAGIC **Performance insights** tab if it appears. The same numbers exist as columns in
-- MAGIC `system.query.history`; see the last cell.
-- MAGIC
-- MAGIC ## 4. Skew: same symptom, two different fixes
-- MAGIC
-- MAGIC One wholesale account owns 80% of the rows. We'll measure how much work one task would
-- MAGIC get, because the task view lives in the Spark UI, and serverless doesn't have one.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakeshore.bronze.qp_skew AS
SELECT id AS sale_id,
       CASE WHEN id % 10 < 8 THEN 'WHOLESALE-0001'
            ELSE concat('ACC-', lpad(CAST(id % 5000 AS STRING), 4, '0')) END AS account_key,
       id % 3000000 AS shopper_id
FROM range(10000000);

-- COMMAND ----------

-- BEFORE: rows per shuffle bucket when we group by the account. One bucket gets the hot key.
SELECT max(n) AS max_rows, percentile_approx(n, 0.5) AS median_rows, count(*) AS buckets
FROM (SELECT pmod(hash(account_key), 200) AS bucket, count(*) AS n
      FROM lakeshore.bronze.qp_skew GROUP BY 1);

-- COMMAND ----------

-- AFTER: add a salt to the key. The hot account now spreads over 16 buckets.
SELECT max(n) AS max_rows, percentile_approx(n, 0.5) AS median_rows, count(*) AS buckets
FROM (SELECT pmod(hash(account_key, pmod(hash(shopper_id), 16)), 200) AS bucket, count(*) AS n
      FROM lakeshore.bronze.qp_skew GROUP BY 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The biggest bucket drops from about 8 million rows to about half a million. Salting has a
-- MAGIC price: aggregate twice. First per key and salt, where the work is spread, then per key.
-- MAGIC The salt comes from `shopper_id` on purpose: one shopper always lands in one salt, so
-- MAGIC adding up the distinct counts is still correct.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW qp_plain AS
SELECT account_key, count(DISTINCT shopper_id) AS shoppers
FROM lakeshore.bronze.qp_skew GROUP BY account_key;

CREATE OR REPLACE TEMPORARY VIEW qp_salted AS
SELECT account_key, sum(n) AS shoppers
FROM (SELECT account_key, pmod(hash(shopper_id), 16) AS salt,
             count(DISTINCT shopper_id) AS n
      FROM lakeshore.bronze.qp_skew GROUP BY account_key, salt)
GROUP BY account_key;

-- COMMAND ----------

-- Same answer, row for row. assert_true fails the cell if any row differs.
SELECT assert_true(count(*) = 0, 'salted and plain answers differ') AS same_answer
FROM ((SELECT * FROM qp_plain EXCEPT SELECT * FROM qp_salted)
      UNION ALL
      (SELECT * FROM qp_salted EXCEPT SELECT * FROM qp_plain));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Don't quote a timing from this demo. Both versions ran in about a second here, because
-- MAGIC ten million rows on Photon isn't a problem yet. What you measured is the *distribution*,
-- MAGIC and that's what decides whether one task holds up a stage at real scale. A plain `SUM`
-- MAGIC also pre-aggregates before the shuffle (`partial_sum` in the plan), so it feels skew far
-- MAGIC less than `count(DISTINCT)` does.
-- MAGIC
-- MAGIC **The fork.** Skewed **join**: adaptive query execution (AQE) already handles it, on by
-- MAGIC default, in sort-merge and shuffle hash joins, by splitting the oversized partition.
-- MAGIC Nothing to write. It counts as skewed only if it's over 256 MB *and* over 5 times the
-- MAGIC median, and it never applies to broadcast joins. Skewed **groupBy**: AQE's skew handling
-- MAGIC doesn't apply, so you salt and aggregate twice.
-- MAGIC
-- MAGIC ## 5. Reading checklist
-- MAGIC
-- MAGIC | Symptom | Diagnosis | Fix |
-- MAGIC |---|---|---|
-- MAGIC | scan reads about the whole table | no skipping (function on the column, wrong layout) | bare columns, cluster on the filter |
-- MAGIC | one operator's rows ≫ its inputs | join explosion (bad keys, non-equi) | fix keys, pre-aggregate |
-- MAGIC | Shuffle on both sides of a small-dimension join | missed broadcast (stale stats, or a hint) | `ANALYZE`, remove the hint, or `BROADCAST` hint |
-- MAGIC | Shuffle dominates Top operators | wide data moving (columns, late filters) | select less, filter earlier, cluster |
-- MAGIC | spill above zero | memory pressure | fewer columns, better join order |
-- MAGIC | task time ≫ wall-clock | parallel burn, not always bad | prune and filter earlier |
-- MAGIC | one task runs far longer (Spark UI, classic compute) | skew | join: AQE. groupBy: salt |

-- COMMAND ----------

-- The same numbers as a table. It lags by a while: an empty result means "come back later".
SELECT left(statement_text, 60) AS query, read_files, pruned_files,
       shuffle_read_bytes, spilled_local_bytes, total_task_duration_ms, execution_duration_ms
FROM system.query.history
WHERE statement_text ILIKE '%bronze.qp_sales%'
  AND statement_type = 'SELECT'
  AND executed_by = current_user()
  AND start_time > current_timestamp() - INTERVAL 1 DAY
ORDER BY start_time DESC
LIMIT 10;

-- COMMAND ----------

DROP VIEW IF EXISTS qp_plain;
DROP VIEW IF EXISTS qp_salted;
DROP TABLE IF EXISTS lakeshore.bronze.qp_sales;
DROP TABLE IF EXISTS lakeshore.bronze.qp_products;
DROP TABLE IF EXISTS lakeshore.bronze.qp_accounts;
DROP TABLE IF EXISTS lakeshore.bronze.qp_skew;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Recap
-- MAGIC
-- MAGIC The profile is the meter: pruning, join operator, shuffle, spill, time split. Queries, not
-- MAGIC clusters, are usually the cost problem. And skew has two answers depending on one word in
-- MAGIC the stem. Next: what CDF does to the cost of staying fresh.
