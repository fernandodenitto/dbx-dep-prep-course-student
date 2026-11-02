# Databricks notebook source
# MAGIC %md
# MAGIC # 1.5 — datagen v2: buses, change feeds, delete requests
# MAGIC
# MAGIC The Associate course generated files an e-commerce actually produces:
# MAGIC exports, batches, a trickle of new orders. The Professional course needs
# MAGIC the feeds a *production platform* consumes:
# MAGIC
# MAGIC | Feed | Simulates | Lands in | Powers |
# MAGIC |---|---|---|---|
# MAGIC | bus records | Kafka-style message bus | `topics/` | multiplex bronze (S3) |
# MAGIC | CDC events | database change capture | `cdc/<entity>/` | SCD2 by hand (S3, S4), AUTO CDC, formerly APPLY CHANGES (S8) |
# MAGIC | delete requests | GDPR/CCPA erasure queue | `deletes/` | PII purge (S10) |
# MAGIC
# MAGIC **Honesty first**: Free Edition has no Kafka, and we don't pretend it
# MAGIC does. These files have the exact *shape* of Kafka records — every
# MAGIC payload-parsing skill transfers 1:1 — and where the transport itself
# MAGIC matters for the exam, we'll tell you exactly what it asks.
# MAGIC
# MAGIC > **Before running:** clone the student repo
# MAGIC > `fernandodenitto/dbx-dep-prep-course-student` as a Git folder and run this
# MAGIC > notebook from where it sits (it finds `datagen/` by walking up to the repo
# MAGIC > root). The fast-forward notebook must already have run.

# COMMAND ----------

import json
import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")                  # make datagen.py and feed.py importable

import datagen
import feed

LANDING = "/Volumes/lakeshore/landing/files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bus records — what a Kafka message really looks like
# MAGIC
# MAGIC A message bus doesn't hand you a tidy row. It hands you an envelope:
# MAGIC `key`, `value` (the payload — as BYTES; ours is a JSON string), `topic`,
# MAGIC `partition`, `offset`, `timestamp`. The payload schema is YOUR problem.

# COMMAND ----------

# 20 orders per batch: the size we land below (20 orders + 20 payments + 3 customers)
batch = datagen.generate_bus_batch(batch_num=1, orders_per_batch=20)
print(f"one batch = {len(batch)} records, topics mixed together:\n")
for record in batch[:3]:
    print(json.dumps(record, indent=2)[:400], "\n---")

# COMMAND ----------

# Count by topic: three streams share one drop — that's the MULTIPLEX pattern
from collections import Counter
Counter(r["topic"] for r in batch)

# COMMAND ----------

# MAGIC %md
# MAGIC The `value` is a STRING. Consuming it means: filter the topic, parse the
# MAGIC payload with a schema you declare. That's Section 3's whole opening move.
# MAGIC The exam guide names Kafka, Kinesis and Pub/Sub as sources, and every one of
# MAGIC them hands you an unparsed payload.

# COMMAND ----------

order_payload = json.loads(
    next(r for r in batch if r["topic"] == "orders")["value"])
order_payload

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CDC events — changes, not states
# MAGIC
# MAGIC A CDC feed says what HAPPENED: `operation` (INSERT/UPDATE/DELETE),
# MAGIC `sequence_num` (source commit order), `event_ts`, and the row image.
# MAGIC Two flags simulate what production actually throws at you:
# MAGIC - `with_late_events=True` — events that arrive HOURS after they happened
# MAGIC - `with_reordered_ops=True` — file order ≠ commit order

# COMMAND ----------

events = datagen.generate_cdc_batch("customers", batch_num=1)
for e in events[:4]:
    print(f"{e['operation']:<7} seq={e['sequence_num']}  {e['customer_id']}  "
          f"email={e['email']}")

# COMMAND ----------

reordered = datagen.generate_cdc_batch("customers", batch_num=1,
                                       with_reordered_ops=True)
print("file order  :", [e["sequence_num"] for e in reordered[:8]])
print("commit order:", sorted(e["sequence_num"] for e in reordered[:8]))

# COMMAND ----------

# MAGIC %md
# MAGIC Same events, scrambled file order. If you apply them as-is, an old UPDATE
# MAGIC can overwrite a newer one. Handling this is exactly what `SEQUENCE BY`
# MAGIC does in AUTO CDC (S8) — and what we'll first solve by hand with a window
# MAGIC rank (S4).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete requests — the compliance queue

# COMMAND ----------

datagen.generate_delete_requests(batch_num=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Land the first Professional batches
# MAGIC
# MAGIC New subfolders only — nothing from the Associate course is touched.
# MAGIC Deterministic: re-running rewrites identical files.

# COMMAND ----------

for folder in ("topics", "cdc/customers", "deletes"):
    os.makedirs(f"{LANDING}/{folder}", exist_ok=True)

paths = [
    feed.emit_batch("bus", f"{LANDING}/topics", 1),
    feed.emit_batch("bus", f"{LANDING}/topics", 2),
    feed.emit_batch("cdc", f"{LANDING}/cdc/customers", 1, entity="customers"),
    feed.emit_batch("deletes", f"{LANDING}/deletes", 1, batch_size=5),
]
for p in paths:
    print("✓", p)

# COMMAND ----------

for folder in ("topics", "cdc/customers", "deletes"):
    for f in dbutils.fs.ls(f"{LANDING}/{folder}"):
        print(f"{f.path}  ({f.size:,} bytes)")

# COMMAND ----------

# Sanity: bus batches are deterministic and their order ids can never collide
# with v1 data (v1 files: O000001-O001000, v1 feed: O001001-O001100)
rows = [json.loads(l) for l in open(f"{LANDING}/topics/bus_batch_0001.json")]
order_ids = [json.loads(r["value"])["order_id"] for r in rows
             if r["topic"] == "orders"]
assert min(order_ids) >= "O002001", order_ids[:3]
print(f"✓ {len(rows)} bus records, order ids from {min(order_ids)} — v1 safe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done
# MAGIC
# MAGIC You've seen every feed this course consumes, and you can read the Python that
# MAGIC produces them: about 500 lines across `datagen.py` and `feed.py`, each function
# MAGIC short. No magic, no hidden datasets. Next section: we stop admiring the bus
# MAGIC records and start modeling the lakehouse that will process them.

# COMMAND ----------

dbutils.notebook.exit("datagen-v2-complete")
