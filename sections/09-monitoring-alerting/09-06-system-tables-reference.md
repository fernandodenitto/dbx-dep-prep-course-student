---
title: "Reference card: system tables"
section: S9 — Monitoring and Alerting
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com and live on Free Edition, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every table, retention and requirement
> below was checked against the docs on the date in the header. The Free Edition column is what answered on
> our workspace that day; yours may differ.

Lesson 9.1 queried system tables to answer a cost question. This card is the catalogue —
which table answers which question, and how long it keeps the answer.

---

## The map

| Table | Answers | Retention | Free Edition |
|---|---|---|---|
| `system.billing.usage` | **What did we spend, on what, and who ran it?** | 365 days | ✅ |
| `system.billing.list_prices` | Historical SKU prices, the multiplier for `usage` | Indefinite | ✅ |
| `system.access.audit` | **What security and compliance events happened?** | 365 days | ✅ |
| `system.access.table_lineage` | Which jobs, notebooks and queries read or wrote this table | 365 days | ✅ |
| `system.access.column_lineage` | The same, at column granularity | 365 days | ✅ |
| `system.query.history` | Queries on SQL warehouses and serverless compute | 365 days | ✅ |
| `system.compute.clusters` | Configuration history of **classic** compute | 365 days | 0 rows, no classic compute |
| `system.compute.node_timeline` | Per-minute utilization of **classic** compute | 90 days | 0 rows, no classic compute |
| `system.compute.node_types` | Available node types and their hardware | Indefinite | ✅ |
| `system.compute.warehouses` | SQL warehouse configuration over time | 365 days | ✅ |
| `system.compute.warehouse_events` | Warehouse starts, stops and scaling | 365 days | ✅ |
| `system.lakeflow.jobs` | Every job, one row per change | 365 days | ✅ |
| `system.lakeflow.job_tasks` | Every job task, one row per change | 365 days | ✅ |
| `system.lakeflow.job_run_timeline` | When each **run** started and ended, in slices of up to an hour | 365 days | ✅ |
| `system.lakeflow.job_task_run_timeline` | When each **task** ran, and what it consumed | 365 days | ✅ |
| `system.lakeflow.pipelines` | Every pipeline, one row per change | 365 days | ✅ |
| `system.lakeflow.pipeline_update_timeline` | When each pipeline **update** ran | 365 days | ✅ |
| `system.lakeflow_pipeline_events_preview.pipeline_events` | Event log entries across pipelines in a region (Beta, preview path) | 365 days | ✅ |
| `system.alert.alerts` | Alert configurations over time (Public Preview) | 365 days | ✅ |
| `system.alert.alert_evaluation_history` | One row per alert evaluation (Public Preview) | 365 days | ✅ |
| `system.storage.predictive_optimization_operations_history` | What predictive optimization did, and when | 180 days | ✅ |

Most of these support **streaming reads**, which is what makes it practical to build an
incremental monitoring pipeline on top of them rather than re-scanning a year of history.

---

## Getting access

1. The workspace must be **enabled for Unity Catalog**.
2. System schemas are enabled by an **account or metastore admin**.
3. Users then need the usual trio: `USE CATALOG` on `system`, `USE SCHEMA` on the schema,
   and `SELECT` on the table.
4. The exception: the `system.alert` tables default to users who are both account admin and metastore
   admin. The docs suggest a dynamic view to share them.

At work, `statement_text` in `query.history` reads `<REDACTED>` unless you're an account admin or in the
`databricks_pii_access` group.

**Trap:** "the analyst can't see `system.billing.usage`". It is either the schema not being
enabled at the account level, or the missing `USE CATALOG` / `USE SCHEMA` — the same
traversal rule as everywhere else in Unity Catalog.

---

## The three patterns worth memorising

**Cost, in money rather than DBUs** — `usage` records consumption, `list_prices` records
price. Neither alone answers "what did this cost":

```sql
SELECT u.usage_metadata.job_id,
       sum(u.usage_quantity * p.pricing.default) AS approx_cost
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_end_time BETWEEN p.price_start_time
                          AND coalesce(p.price_end_time, current_timestamp())
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY 1 ORDER BY approx_cost DESC;
```

The **time-bounded join on the price interval** is the part people get wrong: prices
change, so joining on `sku_name` alone silently applies today's price to last quarter.

**The latest row of a dimension.** `jobs`, `job_tasks`, `pipelines`, `warehouses`, `clusters` and `alert.alerts`
are slowly changing dimensions: one row per change. Joining one raw multiplies everything on the other side.

```sql
SELECT workspace_id, job_id, name
FROM system.lakeflow.jobs
QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
```

**A run's duration.** Each timeline row records up to an hour, and `result_state` sits only on the row that ends
the run. So group by `run_id`, sum the slices, and take `max_by(result_state, period_end_time)`. The
`run_duration_seconds` column logs 0 for multi-task runs, per the docs. "Why did it slow down?" is nearly always in
`job_task_run_timeline`.

---

## System tables vs the other observability surfaces

| Surface | Scope | Use for |
|---|---|---|
| **System tables** | Account-wide, historical, SQL | Trends, cost attribution, audit, anything across many jobs |
| **Pipeline event log** | One pipeline | Expectation metrics, update failures, per-flow detail |
| **Query profile** | One query | Why *this* query was slow |
| **Jobs UI run history** | One job | Reading a single failure quickly |

**Trap:** a question asking for a *trend across jobs* wants system tables. A question about
*one pipeline's data quality* wants the event log. The Beta `pipeline_events` system table now records event
log entries across pipelines in a region, but the exam's named tool for this stays the event log.

---

## Exam traps

- **365 days for most tables.** `node_timeline` keeps 90, the storage ones 180. `list_prices` and `node_types` are
  indefinite. Configurable retention is in Beta.
- **`usage` alone is not cost** — join `list_prices` on the SKU *and the price interval*.
- **Access needs an admin to enable the schema first**, then the standard UC trio.
- **Lineage lives in `system.access`**, not in a lineage-specific catalog.
- **Task-level detail is `job_task_run_timeline`**, not `job_run_timeline`.
- **`lakeflow.jobs` is a slowly changing dimension:** latest `change_time` first, or runs multiply.
- **Serverless and warehouses aren't in `compute.clusters` or `node_timeline`.** Warehouses have their own two tables.

## Resources

- System tables: https://docs.databricks.com/aws/en/admin/system-tables/
- Billing usage table: https://docs.databricks.com/aws/en/admin/system-tables/billing
- Lakeflow system tables: https://docs.databricks.com/aws/en/admin/system-tables/jobs
- Compute system tables: https://docs.databricks.com/aws/en/admin/system-tables/compute
- Warehouse events: https://docs.databricks.com/aws/en/admin/system-tables/warehouse-events
- Alert system tables: https://docs.databricks.com/aws/en/admin/system-tables/alerts
