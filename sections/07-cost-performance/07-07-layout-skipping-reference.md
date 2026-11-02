---
title: "Reference card: layout, skipping and automatic maintenance"
section: S7 — Cost and Performance
duration: ~8 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every property and default below was
> checked against the docs on the date in the header. Items marked *measured* were run on
> Free Edition on the same date.

Lesson 7.2 made you choose a layout and defend it, 7.3 measured deletion vectors, 7.4 read the
statistics that skipping runs on. Lesson 7.8, on caching, comes after this card. This is the whole
toolkit, in one place.

---

## The three layout eras

| Era | Mechanism | Status |
|---|---|---|
| 1 | **Partitioning** (`PARTITIONED BY`) | Wrong for most tables. Physical directories; a bad scheme needs a rewrite or a conversion |
| 2 | **Z-ordering** (`OPTIMIZE … ZORDER BY`) | Superseded. Colocates related data, must be re-run, keys fixed at optimize time |
| 3 | **Liquid clustering** (`CLUSTER BY`) | **Current recommendation for all new tables** |

The docs: liquid clustering *"replaces table partitioning and `ZORDER`"*, and *"clustering is not
compatible with partitioning or `ZORDER`."* Partitioning and Z-ordering can be combined with each
other. Clustering can be combined with neither.

### When to partition

| Table size | The docs say |
|---|---|
| Under 1 TB | Don't partition |
| 1 TB to 100 TB | Use liquid clustering; partitioning hurts more often than it helps |
| 100 TB or more | Partitioning might help; use liquid first and verify |
| Each partition | At least 1 GB |

Low-cardinality columns only, such as dates. You can't Z-order on a partition column, and Z-ordering
can't cross a partition boundary.

### Migrating to liquid

| You have | Run | Keys to use |
|---|---|---|
| Unpartitioned, Z-ordered | `ALTER TABLE t CLUSTER BY (…)` | the ZORDER columns |
| Partitioned | `ALTER TABLE t REPLACE PARTITIONED BY WITH CLUSTER BY (…)`, Runtime 18.1+ | the partition columns |
| Partitioned and Z-ordered | same `REPLACE` statement | both sets of columns |

*Measured:* plain `ALTER … CLUSTER BY` on a partitioned table fails with
`DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED`, and `OPTIMIZE … ZORDER BY` on a clustered
table fails with `DELTA_CLUSTERING_WITH_ZORDER_BY`. `REPLACE` doesn't apply to pipeline tables: change
the definition from `PARTITIONED BY` to `CLUSTER BY`.

---

## Liquid clustering

```sql
CREATE TABLE t (col0 INT, col1 STRING) CLUSTER BY (col0);
ALTER TABLE t CLUSTER BY (col0, col1);
ALTER TABLE t CLUSTER BY AUTO;        -- let Databricks pick the keys
ALTER TABLE t CLUSTER BY NONE;        -- stop clustering
OPTIMIZE t;                           -- cluster incrementally
OPTIMIZE t FULL;                      -- recluster what isn't on the current keys
```

| Fact | Value |
|---|---|
| Maximum clustering keys | **4**. Under 10 TB, fewer keys do better on single-column filters |
| Change keys later | **Yes**, without rewriting existing data |
| Applied by | `OPTIMIZE`, incrementally: it rewrites only what it must |
| Force a re-cluster | `OPTIMIZE t FULL` (Runtime 16.0+). Lesson 7.2 runs it. Can take hours on large tables never clustered on those keys |
| Partial re-cluster | `OPTIMIZE t FULL WHERE <predicate>` (Runtime 18.1+) |

**Trap:** `ALTER TABLE … CLUSTER BY` does **not** reorganise anything. New writes and later `OPTIMIZE`
runs use the new keys, while existing data stays put until `OPTIMIZE … FULL`.

### `CLUSTER BY AUTO`, or manual keys?

| | Manual keys | `CLUSTER BY AUTO` |
|---|---|---|
| Table type | any | **Unity Catalog managed only** |
| Requires | nothing extra | **predictive optimization** |
| Keys come from | you | historical query workload; changed only if savings beat cost |
| May choose none | no | yes: small tables, rarely queried, already well laid out |
| Pick it when | filters are known and stable | patterns are unknown or changing |

---

## Data skipping

Per-file statistics (min, max, null count) let a query skip files it can't match. Partitioning,
Z-ordering and clustering all exist to make those ranges tighter.

| Table | Statistics collected on |
|---|---|
| External | First 32 columns, or what the properties below say |
| Managed, with predictive optimization | Those, plus other frequently filtered columns the service selects |

| Property | Meaning |
|---|---|
| `delta.dataSkippingStatsColumns` | Names the columns. Supersedes the next one |
| `delta.dataSkippingNumIndexedCols` | Changes how many, by column order |

**Trap:** on an external table, a filter column at position 40 has no statistics, so no skipping,
however well the table is clustered. Fix: name it in `dataSkippingStatsColumns`, reorder the schema, or
raise the count. Changing the property doesn't recompute old files: run
`ANALYZE TABLE t COMPUTE DELTA STATISTICS`. A function around the column (`upper(col)`) defeats skipping too.

---

## File size and compaction

| Fact | Value |
|---|---|
| Autotuned target | 256 MB under 2.56 TB, growing linearly to 1 GB at 10 TB |
| Optimized writes | `autoOptimize.optimizeWrite`; 128 MB target when set to `true` |
| Auto compaction | `autoOptimize.autoCompact`; runs on the writing compute; history shows `auto = true` |
| Fixed target | `delta.targetFileSize`. On managed tables, only `OPTIMIZE` honors it |
| Tables over 1 TB | The docs recommend scheduled `OPTIMIZE` if predictive optimization isn't doing it |

Auto compaction and predictive optimization are independent features. "Auto optimize" is the name for
the two properties together.

---

## Deletion vectors

```sql
CREATE TABLE t (…) TBLPROPERTIES ('delta.enableDeletionVectors' = true);
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

They accelerate `DELETE`, `UPDATE` and `MERGE` by **marking rows in metadata instead of rewriting data
files**. Reads apply the markers. Frequent small `MERGE`s rewriting big files: this is the answer.

| To… | Run |
|---|---|
| Physically rewrite the affected files, without a guarantee for every change | `OPTIMIZE`, or a write with auto compaction |
| Rewrite **all** files with deletion vector changes | `REORG TABLE t APPLY (PURGE)` |
| Actually remove the old data from storage | `REORG … APPLY (PURGE)` **then** `VACUUM` |

**Trap:** a `DELETE` on a table with deletion vectors does **not** remove the bytes. Lesson 10.5 builds
the purge. You can't `ALTER` deletion vectors on a streaming table or materialized view.

---

## Predictive optimization

| Operation | What it does |
|---|---|
| `OPTIMIZE` | Fixes file sizes; triggers incremental clustering. **Doesn't run ZORDER, and ignores Z-ordered files** |
| `VACUUM` | Deletes files the table no longer references |
| `ANALYZE` | Collects statistics |

| Fact | Value |
|---|---|
| Applies to | **Unity Catalog managed tables only** (Delta and Iceberg) |
| Does **not** apply to | External and foreign tables, OpenSharing recipient tables |
| Default | **Enabled** for accounts created on or after 11 November 2024; docs expected the rollout to finish by August 2026 |
| Control | Account console, or `ALTER { CATALOG \| SCHEMA \| TABLE } … { ENABLE \| DISABLE \| INHERIT } PREDICTIVE OPTIMIZATION` |
| Billing | Serverless jobs SKU |
| Receipts | `system.storage.predictive_optimization_operations_history`; lesson 7.1 queries it |

---

## Iceberg, in this picture

| Fact | Value |
|---|---|
| Create | `USING iceberg`. Default is Delta |
| `PARTITIONED BY` on managed Iceberg | Read as clustering keys. *Measured:* it came back in `clusteringColumns` |
| `CLUSTER BY AUTO`, deletion vectors, row tracking, CDF | Need Iceberg **v3** (`'format-version' = 3`). *Measured:* refused on default tables |
| Statistics properties | `iceberg.dataSkippingStatsColumns` |
| File size | `iceberg.targetFileSize` |

Lesson 6.6 has the rest, including CDC targets.

---

## Choosing, in one table

| Situation | Answer |
|---|---|
| New table, managed, patterns unknown | `CLUSTER BY AUTO` |
| New table, filters known | `CLUSTER BY (keys)` |
| Under 1 TB and someone says "partition it" | Don't |
| Inherited partitioned table performing badly | `REPLACE PARTITIONED BY WITH CLUSTER BY` |
| Inherited Z-ordered table | `ALTER CLUSTER BY`, same columns, then `OPTIMIZE FULL` |
| Filter column has no statistics (external) | `dataSkippingStatsColumns` |
| Frequent small `DELETE` / `UPDATE` / `MERGE` | **Deletion vectors** |
| Data must genuinely leave storage | `REORG … APPLY (PURGE)` then `VACUUM` |
| Don't want to schedule maintenance | Managed tables + predictive optimization |
| Repeated reads of the same files | Disk cache, lesson 7.8 |

---

## Exam traps

- **Clustering can't combine with partitioning or ZORDER.** Those two can combine with each other.
- **`CLUSTER BY` changes nothing until `OPTIMIZE` runs**; old data needs `FULL`.
- **Statistics on the first 32 columns** is the external-table default, not a universal rule.
- **Deletion vectors soft-delete.** Bytes leave only after `REORG … APPLY (PURGE)` + `VACUUM`.
- **Predictive optimization covers managed tables only**, and doesn't run ZORDER.
- **`CLUSTER BY AUTO` needs a managed table and predictive optimization.**
- **Four clustering keys maximum.**

## Resources

- Liquid clustering: https://docs.databricks.com/aws/en/tables/clustering
- When to partition: https://docs.databricks.com/aws/en/tables/partitions
- Data skipping: https://docs.databricks.com/aws/en/tables/data-skipping
- Control file size: https://docs.databricks.com/aws/en/tables/tune-file-size
- Deletion vectors: https://docs.databricks.com/aws/en/tables/features/deletion-vectors
- Predictive optimization: https://docs.databricks.com/aws/en/optimizations/predictive-optimization
- OPTIMIZE: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize
