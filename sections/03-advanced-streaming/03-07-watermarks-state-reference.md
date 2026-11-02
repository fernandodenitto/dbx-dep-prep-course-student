---
title: "Reference card: watermarks and streaming state"
section: S3 — Advanced Streaming
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every behaviour below was checked
> against the docs on the date in the header.

Lesson 3.5 set a watermark against a stated redelivery window. This card is the model
behind it. The guide's statement is "Apply Structured Streaming stateful-processing
semantics, including watermarks, output modes, foreachBatch, and checkpoints for
fault-tolerant exactly-once state recovery", and the official sample question on it wants
a watermark on the event-time column plus the query's checkpoint. Lesson 4.7 runs the
output modes and a restart live.

---

## What a watermark actually is

```python
df.withWatermark("event_time", "10 minutes")
```

Two arguments: **the event-time column** and **the lateness threshold**.

The engine tracks the **maximum event time it has seen**, subtracts the threshold, and
treats the result as the watermark. State older than the watermark is discarded.

The guarantee is asymmetric, and the exam tests that asymmetry:

| Record arrives | Treatment |
|---|---|
| Within the threshold | **Always** processed |
| Beyond the threshold | **May or may not** be processed — no guarantee |

So a watermark is not a filter you configure for correctness of *output*. It is the knob
that makes state **bounded**. Without it, a stateful query's state grows forever.

**Trap:** the watermark advances on **event time observed in the data**, not wall-clock
time. A stream with no new data does not advance its watermark, so pending state does not
flush. "The aggregate never emitted" is often this.

---

## Output modes

What each mode does to a stateful aggregation:

| Mode | When rows are written | State | On a Delta sink |
|---|---|---|---|
| **Append** (the default) | After the watermark passes, once per window. Writes are delayed by the lateness threshold | Dropped once the threshold passes | Supported, `toTable` |
| **Update** | Every row that changed in this trigger, as results are calculated. A row can change again later | Dropped once the threshold passes | **Not supported directly**: use `foreachBatch` with `MERGE` |
| **Complete** | All result rows, on every trigger: the target is rewritten | **Never dropped**, even with a watermark | Supported, but the docs point to materialized views for this need |

What is allowed where:

| Query | Allowed output modes |
|---|---|
| Event-time aggregation **with** watermark | append, update, complete |
| Aggregation **without** watermark | update, complete — **never append** |
| Stream-stream join | **append only** |
| Deduplication | Output mode has no effect |
| Stateless query | Behaves the same in every mode |

Append needs finality. Only a watermark can declare a group final.

---

## Deduplication

```python
(df.withWatermark("event_time", "10 hours")
   .dropDuplicatesWithinWatermark(["guid"]))
```

`dropDuplicatesWithinWatermark` (DBR 13.3+) deduplicates **within the threshold** without
keeping every key forever. Same asymmetry: duplicates inside the window are always removed;
outside it, no guarantee.

**How to size it:** take the source's documented redelivery window and use that as the
threshold. A bus that can redeliver within 45 minutes needs a watermark of at least 45
minutes — that is exactly the reasoning lesson 3.5 asked you to defend.

**Trap:** plain `dropDuplicates` on a stream keeps state for **every key, forever**. It is
correct and it is unbounded. The watermark variant is the production answer.

---

## Stream-stream joins

```python
(orders.withWatermark("order_ts", "1 hour")
  .join(
     payments.withWatermark("payment_ts", "2 hours"),
     expr("order_id = p_order_id AND payment_ts BETWEEN order_ts AND order_ts + interval 1 hour"),
     "inner"))
```

| Join type | Watermark | Notes |
|---|---|---|
| **Inner** | **Recommended** | Works without one, but state grows unbounded. With watermarks, old state is discarded |
| **Outer** (left / right / full) | **Mandatory** | An unmatched row can only be emitted once the threshold has passed — until then the engine cannot know it will stay unmatched |

**Joins between two streams support append mode only.**

The reason outer joins *require* a watermark is worth internalising rather than memorising:
emitting a NULL-padded row is a claim that no match will ever arrive. Only a watermark lets
the engine make that claim.

You also want a **time-range condition** in the join (a `BETWEEN` on event times), not just
an equality. Without one, every row must be kept against every future row.

---

## Stream-static joins

A stream joined to a static DataFrame or table:

- **No watermark is involved** — there is no second stream whose state must be bounded.
- The static side is **re-read on every micro-batch**, so it picks up changes.

**The trap:** a late-arriving dimension row. If the fact arrives before its dimension row
exists, an inner join misses it — and unlike a stream-stream join, no watermark will rescue
it later, because that micro-batch is done. That is a modelling problem, not a
streaming-configuration problem. Lesson 4.5 runs this join; it doesn't stage this failure.

---

## What the state store holds

Stateful operators — aggregations, deduplication, stream-stream joins — keep state in a
state store, checkpointed alongside progress.

Practical consequences:

1. **State is part of the checkpoint.** Delete it and you lose the state, not just position.
2. **Unbounded state is the classic production failure**: memory pressure, then spill,
   then ever-slower micro-batches. The diagnosis is a stateful operator without a watermark.
3. **Changing a stateful query's shape can make its checkpoint unusable.** See the table
   below.

---

## Checkpoints and exactly-once

The checkpoint holds four things: the **source offsets** processed in each micro-batch, the
**commit log** of which micro-batches reached the sink, the **state store** contents and
state schema, and metadata such as the query ID. A restart resumes from the offsets with
the state intact, so nothing is reprocessed and nothing is counted twice.

"Exactly-once" is the whole chain, not one setting:

| Link | What it contributes |
|---|---|
| Replayable source + offsets in the checkpoint | Nothing is skipped, and the query knows where it stopped |
| State in the checkpoint | Aggregates continue from where they were |
| Commit log + idempotent sink | A retried micro-batch doesn't write twice |
| `foreachBatch` | **Breaks the chain**: "only at-least-once write guarantees". Restore it with `txnAppId` + `txnVersion` bound to `batch_id` for Delta writes, or a `MERGE` that is idempotent |

Each query needs **its own checkpoint location**; queries must never share one. Delete the
files in a checkpoint and the next run begins fresh: lesson 4.7 does this on purpose and
double-counts.

| Change to a running query | New checkpoint? |
|---|---|
| Number or type of input sources | Generally yes |
| Subscribed Kafka topics, Auto Loader paths | Generally yes |
| Stateful operation types, state schema | Generally yes |
| Output sink type | Generally yes |
| Adding or removing filters | Generally safe |
| Rate limits, trigger intervals | Generally safe |

---

## Beyond the current guide

The guide doesn't name custom stateful processing, so this is orientation, not exam
material.

**`transformWithState`** applies a custom stateful processor: arbitrary per-key logic, with
timers and state TTL. It needs Databricks Runtime 16.2 or above. Databricks recommends it
over the legacy operators **`flatMapGroupsWithState`** and **`mapGroupsWithState`**, and
recommends built-in operators over custom logic whenever they fit. So: aggregation, dedup
or a join means a watermark and a built-in; per-micro-batch routing or upserts mean
`foreachBatch`; only timer-driven or per-key custom state needs `transformWithState`.

**State repartitioning.** On Databricks Runtime 18 LTS and above, changing the partition
count of a stateful query no longer needs a new checkpoint: stop it, set
`spark.sql.streaming.stateStore.partitions`, restart it on the same checkpoint.

---

## Exam traps

- **A watermark bounds state; it is not an output filter.** Late data *may* still be
  processed.
- **Watermarks advance on event time in the data**, not on the clock. No data, no advance.
- **Outer stream-stream joins require watermarks. Inner ones only recommend them.**
- **Stream-stream joins are append-only.**
- **`dropDuplicates` on a stream is unbounded**; `dropDuplicatesWithinWatermark` is not.
- **A stream-static join has no watermark** — late dimension rows are a modelling issue.
- **Update mode has no direct Delta sink.** It is `foreachBatch` plus `MERGE`.
- **Complete mode never drops state.** A watermark doesn't change that.
- **Watermark bounds state; checkpoint recovers it.** They answer different questions, and
  the official sample needs both.

## Resources

- Watermarks: https://docs.databricks.com/aws/en/structured-streaming/watermarks
- Output modes: https://docs.databricks.com/aws/en/structured-streaming/output-mode
- Checkpoints: https://docs.databricks.com/aws/en/structured-streaming/checkpoints
- foreachBatch: https://docs.databricks.com/aws/en/structured-streaming/foreach
- Stateful applications (transformWithState): https://docs.databricks.com/aws/en/stateful-applications/
- State repartitioning: https://docs.databricks.com/aws/en/structured-streaming/state-repartitioning
