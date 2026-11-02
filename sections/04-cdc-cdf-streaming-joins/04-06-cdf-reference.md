---
title: "Reference card: Delta Change Data Feed"
section: S4 — CDC, CDF and streaming joins
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every behaviour below was checked
> against the docs on the date in the header.
>
> The **streaming join matrix** lives in the card at the end of Section 3, next to the
> watermark rules it depends on — one page, not two half-pages.

Lesson 4.3 read a change feed. This card is the feature, in the two forms the docs now
describe: **automatic** change data feed, which is what you get on a new managed table
today, and **legacy**, the table-property form older material and exam stems still use.

---

## Two forms, one read API

| | Automatic change data feed | Legacy change data feed |
|---|---|---|
| Status | GA since 1 September 2026; recommended | Still works; the docs say to migrate |
| Changes computed | **At query time**, from row tracking | **At write time**, into `_change_data` files |
| Setup | None, if the table qualifies | `delta.enableChangeDataFeed = true`, per table |
| Requires | Databricks Runtime 19+; a Unity Catalog table; Delta with row tracking, or Iceberg v3 | Delta Lake |
| Write cost | None added: `MERGE` and `UPDATE` about 15% faster than with legacy, per the release note | Extra change files on every commit that changes rows |
| Reads | `table_changes`, `readChangeFeed` | **The same** `table_changes`, `readChangeFeed` |
| Row filters, column masks | **Not supported** | See the docs before combining |

Measured on Free Edition, 2026-11-01: runtime 19.12, new managed tables get
`delta.enableRowTracking = true` by default, and `table_changes` returned all four change
types from version 0 with no property set. So on Free Edition, automatic is the form you
are already using.

You can't use both on one table. To migrate:

```sql
ALTER TABLE lakeshore.silver.customers
UNSET TBLPROPERTIES ('delta.enableChangeDataFeed');
```

**Automatic has limits worth knowing:** queries can't span a non-additive schema change (a
rename, drop, type change or nullability change), so split the range around it. It isn't
supported if the source was modified in a multi-statement transaction, and external Iceberg
clients can't query it.

---

## The legacy form

Exam stems and older workspaces use this, so know it cold:

```sql
ALTER TABLE lakeshore.silver.customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**⚠️ Legacy isn't retroactive.** Changes made *before* you enabled it are not in the feed. And
if you turn it off for a period and back on, **that interval is never queryable**.

If a stem has a table with this property and asks why last month's changes can't be read,
that's the answer. It doesn't apply to automatic, which computes changes from row tracking.

---

## Reading it

**SQL:**

```sql
SELECT * FROM table_changes('lakeshore.silver.customers', 0, 10);   -- versions 0..10
SELECT * FROM table_changes('lakeshore.silver.customers', 0);       -- version 0..latest
```

**DataFrame:**

```python
(spark.read
   .option("readChangeFeed", "true")
   .option("startingVersion", 0)
   .table("lakeshore.silver.customers"))

(spark.read
   .option("readChangeFeed", "true")
   .option("startingTimestamp", "2026-08-01 00:00:00")
   .table("lakeshore.silver.customers"))
```

The same options work on `readStream` — that is how CDF becomes the source of an
incremental downstream pipeline.

---

## The three metadata columns

| Column | Type | Values |
|---|---|---|
| `_change_type` | string | `insert` · `update_preimage` · `update_postimage` · `delete` |
| `_commit_version` | long | The Delta log version that contains the change |
| `_commit_timestamp` | timestamp | When that commit happened |

**An update produces two rows**, not one: `update_preimage` (the row before) and
`update_postimage` (the row after). Any count over a change feed that forgets this is
double-counting updates.

That pre-image is also what makes CDF genuinely useful — it is the only place the *previous*
version of a row is handed to you directly, which is why SCD2 and audit workloads reach for
it.

---

## Retention — the trap in either form

Change records are **transient** and only available within a retention window. For legacy
the mechanism is explicit: the `VACUUM` command deletes change data files, and changes
recorded in the transaction log follow the checkpoint retention policy. The docs don't
state a separate period for automatic.

**Consequence for design:** CDF is a mechanism for *incremental propagation between
pipeline runs*. For permanent history, the docs recommend writing the output to a separate
table incrementally. The docs do list audit trails as a use, within that window, so read
"audit" in a stem against how far back it needs to go.

---

## When to use CDF, and when not to

| Situation | Reach for |
|---|---|
| Propagate row-level changes to a downstream table | **CDF** |
| Downstream needs the *previous* value of a row | **CDF** (pre-image) |
| Source is append-only | **A plain stream on the table** — you don't need CDF |
| You need history months later | **Your own history table**, not CDF |
| You want the whole table as it was at version N | **Time travel**, not CDF |

**Time travel vs CDF, stated plainly:** time travel gives you a **state** at a point in
time. CDF gives you the **deltas between states**. Questions that ask "which rows changed"
want CDF; questions that ask "what did the table look like" want time travel.

The write-cost argument belongs to legacy: enabling it writes extra change files on commits
that change rows. Automatic removes that overhead and pays at read time. Lesson 7.6 takes up
the cost and latency trade-off.

---

## Exam traps

- **Legacy CDF is not retroactive**, and a disabled interval is permanently unqueryable.
- **Automatic CDF needs no property**, but it does need row tracking and Runtime 19+, and it
  doesn't work with row filters or column masks.
- **The read APIs are identical**, so a question about `table_changes` doesn't tell you which
  form is on.
- **An update emits two rows** — pre-image and post-image.
- **`VACUUM` deletes legacy change data.** Either way, CDF isn't permanent history.
- **Append-only source → you don't need CDF.**
- **"Which rows changed" = CDF · "what did it look like" = time travel.**

## Resources

- Change data feed: https://docs.databricks.com/aws/en/tables/features/change-data-feed
- September 2026 release notes (automatic CDF GA): https://docs.databricks.com/aws/en/release-notes/product/2026/september
- Table history and time travel: https://docs.databricks.com/aws/en/tables/history
