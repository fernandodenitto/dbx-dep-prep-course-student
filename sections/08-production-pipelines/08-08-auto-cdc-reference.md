---
title: "Reference card: AUTO CDC (formerly APPLY CHANGES INTO)"
section: S8 — Production Pipelines
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every clause below was checked against
> the docs on the date in the header.

Lessons 8.4 and 8.5 built SCD type 1 and type 2 declaratively. This card is the full
clause list, and the name mapping: this API was renamed, the guide says "AUTO CDC APIs
(formerly APPLY CHANGES)", and older material uses both.

---

## The name

> "The `AUTO CDC` APIs replace the `APPLY CHANGES` APIs and **have the same syntax**."

| Current | Former |
|---|---|
| `AUTO CDC INTO` | `APPLY CHANGES INTO` |
| `dp.create_auto_cdc_flow(…)` | `dlt.apply_changes(…)` |

Same behaviour, same clauses. When a question says `APPLY CHANGES INTO`, answer as if it
said `AUTO CDC INTO`.

**One limit worth knowing:** the `AUTO CDC` APIs are **not part of Apache Spark
Declarative Pipelines**. They're a Lakeflow pipelines feature on top of the open-source framework.

---

## SQL — SCD type 1

```sql
CREATE OR REFRESH STREAMING TABLE lakeshore.silver.sdp_customers_scd1;

CREATE FLOW sdp_customers_scd1_flow AS AUTO CDC INTO lakeshore.silver.sdp_customers_scd1
FROM stream(lakeshore.bronze.sdp_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, event_ts)
STORED AS SCD TYPE 1;
```

## SQL — SCD type 2

```sql
CREATE OR REFRESH STREAMING TABLE lakeshore.silver.sdp_customers_scd2;

CREATE FLOW sdp_customers_scd2_flow AS AUTO CDC INTO lakeshore.silver.sdp_customers_scd2
FROM stream(lakeshore.bronze.sdp_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, event_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (first_name);
```

---

## The clauses

| Clause | Does |
|---|---|
| `KEYS (…)` | The business key that identifies a row across changes |
| `SEQUENCE BY col` | **The ordering column.** Decides which change wins when events arrive out of order |
| `APPLY AS DELETE WHEN cond` | Rows matching the condition are treated as deletes |
| `APPLY AS TRUNCATE WHEN cond` | Rows matching are treated as a full truncate (SCD type 1 only) |
| `COLUMNS * EXCEPT (…)` / `COLUMNS (…)` | Which source columns land in the target — this is how the CDC bookkeeping columns are kept out |
| `IGNORE NULL UPDATES` | A null in the source does not overwrite a non-null target value |
| `STORED AS SCD TYPE 1 \| 2` | Overwrite in place, or keep history |
| `TRACK HISTORY ON … EXCEPT (…)` | **Type 2 only.** Which column changes actually open a new version |

### `SEQUENCE BY` is the whole point

It is what makes AUTO CDC correct on out-of-order data — the problem lesson 4.2 solved by
hand and 8.4 handed to the engine. The sequencing column decides the winner, **not arrival
order**. Get this column wrong and you get a table that is silently, plausibly wrong.

### `TRACK HISTORY ON` prevents version explosion

Without it, *any* column change opens a new type-2 version, including columns nobody cares
about. `TRACK HISTORY ON * EXCEPT (first_name)` says: keep the column, but don't let it create
history. Our own file 02 doesn't use this clause; it's here for the exam.

---

## Python

```python
dp.create_auto_cdc_flow(
    target = "silver.sdp_customers_scd2",
    source = "bronze.sdp_cdc_customers",
    keys = ["customer_id"],
    sequence_by = col("sequence_num"),
    apply_as_deletes = expr("operation = 'DELETE'"),
    except_column_list = ["operation", "sequence_num", "event_ts"],
    stored_as_scd_type = 2,
    track_history_except_column_list = ["first_name"])
```

---

## What SCD type 2 gives you

The target gains two columns automatically: **`__START_AT`** and **`__END_AT`**.

The current version of a row is the one whose `__END_AT` is `NULL`:

```sql
SELECT * FROM lakeshore.silver.sdp_customers_scd2 WHERE __END_AT IS NULL;
```

`__START_AT` / `__END_AT` take their values from the `SEQUENCE BY` column, so their type
follows it — they are not necessarily timestamps.

---

## Type 1 vs type 2

| | Type 1 | Type 2 |
|---|---|---|
| Old values | **Overwritten** | **Kept**, closed out with `__END_AT` |
| Row count | One per key | One per key *per version* |
| Answers | "What is true now?" | "What was true on 3 August?" |
| Cost | Lower | Higher — the table grows with change volume |

**Trap:** "we need to report on historical state" means type 2. "We need the current
customer record" means type 1. Choosing type 2 by default is a real cost mistake, and
choosing type 1 when history is required is unrecoverable — the old values are gone.

---

## Exam traps

- **`APPLY CHANGES INTO` is the former name of `AUTO CDC INTO`** — identical syntax.
- **`SEQUENCE BY`, not arrival order**, decides which change wins.
- **`TRACK HISTORY ON` is type 2 only**, and it exists to stop version explosion.
- **`__START_AT` / `__END_AT` come from the sequence column**, so they are not always
  timestamps; current rows have `__END_AT IS NULL`.
- **`APPLY AS TRUNCATE WHEN` is type 1 only.**
- **The target must exist** as a streaming table before the flow writes into it.

## Resources

- AUTO CDC in pipelines: https://docs.databricks.com/aws/en/ldp/cdc
- create_auto_cdc_flow reference: https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes
- Pipeline concepts: https://docs.databricks.com/aws/en/ldp/concepts
