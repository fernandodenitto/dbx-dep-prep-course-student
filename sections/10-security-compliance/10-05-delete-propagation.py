# Databricks notebook source
# MAGIC %md
# MAGIC # 10.5 — The right to erasure: delete, propagate, purge
# MAGIC
# MAGIC The delete-requests queue has been waiting since Section 1. Today we honor it, following
# MAGIC the path the Databricks GDPR page lays out: delete at bronze first, propagate to silver
# MAGIC and gold, remember the upstream sources, then make the removal physical. The guide:
# MAGIC "a data purging strategy that satisfies data retention and deletion policies (e.g., GDPR
# MAGIC right-to-erasure...) using Delta Lake and Unity Catalog features."
# MAGIC
# MAGIC > Prereqs: S6.4 (`bronze.delete_requests`), S3–S4 (bus and dimension), S8 (pipeline
# MAGIC > tables, handled if they exist), 10.3 (vault), 10.4 (masked tables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The design, in the order the docs give it
# MAGIC
# MAGIC | Step | What | How |
# MAGIC |---|---|---|
# MAGIC | 0 | A **control table** of requests | `bronze.delete_requests`, read by a scheduled job |
# MAGIC | 1 | **Bronze first** | DML, with MERGE for many records at once |
# MAGIC | 2 | **Streaming tables** downstream | DML on the table itself, and `skipChangeCommits` on every stream that reads a table you just changed |
# MAGIC | 3 | **Materialized views** | nothing special: refresh them, and they handle deletions |
# MAGIC | 4 | **Upstream sources** | files, queues, databases: GDPR covers them too |
# MAGIC | 5 | **Physical removal** | `REORG ... APPLY (PURGE)` where deletion vectors are on, then VACUUM past retention, or predictive optimization |
# MAGIC | 6 | **Receipts** | erasure you can't prove is erasure you didn't do |
# MAGIC
# MAGIC And the one that surprises people, because Section 8 was right about something else:
# MAGIC a **CDC DELETE event is not erasure**. Through AUTO CDC (formerly APPLY CHANGES), SCD2
# MAGIC *closes* the version and history stays, with the email in it. SCD1 keeps the row as a
# MAGIC tombstone for two days by default. Feed deletes are how a source system's deletes reach
# MAGIC you. An erasure request needs DML on the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. First, feel why: a three-table lab
# MAGIC
# MAGIC A raw Delta table, a **streaming table** reading it, and a **materialized view** on top,
# MAGIC in a schema we'll drop. Creating these from a serverless notebook is in Beta, and each
# MAGIC refresh below takes a minute or so: that's a small pipeline starting behind the scenes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeshore.gdpr_lab;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lakeshore.gdpr_lab.raw (customer_id STRING, email STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO lakeshore.gdpr_lab.raw VALUES
# MAGIC   ('LAB1', 'ada@north.example'), ('LAB2', 'bob@north.example'), ('LAB3', 'eve@south.example');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE lakeshore.gdpr_lab.st
# MAGIC AS SELECT * FROM STREAM lakeshore.gdpr_lab.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW lakeshore.gdpr_lab.mv
# MAGIC AS SELECT substr(email, instr(email, '@') + 1) AS domain, count(*) AS customers
# MAGIC    FROM lakeshore.gdpr_lab.st GROUP BY 1;
# MAGIC
# MAGIC SELECT * FROM lakeshore.gdpr_lab.mv ORDER BY domain;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1: bronze first.** Delete LAB1 from the source, then refresh the stream.

# COMMAND ----------

spark.sql("DELETE FROM lakeshore.gdpr_lab.raw WHERE customer_id = 'LAB1'")
try:
    spark.sql("REFRESH STREAMING TABLE lakeshore.gdpr_lab.st")
    raise AssertionError("expected the refresh to fail")
except Exception as e:
    assert "DELTA_SOURCE_TABLE_IGNORE_CHANGES" in str(e), e
    print("✓ the stream broke: DELTA_SOURCE_TABLE_IGNORE_CHANGES")

# COMMAND ----------

# MAGIC %md
# MAGIC A stream from a Delta table accepts appends only, and a DELETE on the source breaks it.
# MAGIC That's the failure you'd meet at 3 a.m. the first time someone honors a request and
# MAGIC forgets the consumers. **Step 2:** tell the stream to skip change commits.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE lakeshore.gdpr_lab.st
# MAGIC AS SELECT * FROM STREAM lakeshore.gdpr_lab.raw WITH (SKIPCHANGECOMMITS);

# COMMAND ----------

still = spark.sql("SELECT count(*) FROM lakeshore.gdpr_lab.st WHERE customer_id = 'LAB1'").first()[0]
print(f"LAB1 rows still in the streaming table: {still}")
assert still == 1, "skipChangeCommits ignores the delete, it does not propagate it"

# COMMAND ----------

# MAGIC %md
# MAGIC The stream works again, and **LAB1 is still in there**. `skipChangeCommits` means ignore,
# MAGIC not propagate. So you delete from the streaming table yourself, with plain DML. This is
# MAGIC the documented path, and you may have heard the opposite ("never touch a pipeline
# MAGIC table"). The docs name DML on streaming tables as the GDPR route.

# COMMAND ----------

spark.sql("DELETE FROM lakeshore.gdpr_lab.st WHERE customer_id = 'LAB1'")
spark.sql("INSERT INTO lakeshore.gdpr_lab.raw VALUES ('LAB4', 'zed@south.example')")
spark.sql("REFRESH STREAMING TABLE lakeshore.gdpr_lab.st")
ids = sorted(r[0] for r in spark.table("lakeshore.gdpr_lab.st").select("customer_id").collect())
print("streaming table now holds:", ids)
assert ids == ["LAB2", "LAB3", "LAB4"], ids

# COMMAND ----------

# MAGIC %md
# MAGIC New rows flow, and the deleted one doesn't come back. **Step 3:** the materialized view.
# MAGIC It still counts LAB1 until it refreshes, and then it's right with no extra work.

# COMMAND ----------

def north():
    rows = spark.sql("""SELECT customers FROM lakeshore.gdpr_lab.mv
                        WHERE domain = 'north.example'""").collect()
    return rows[0][0] if rows else 0

before = north()
spark.sql("REFRESH MATERIALIZED VIEW lakeshore.gdpr_lab.mv")
after = north()
print(f"north.example customers: {before} before the refresh, {after} after")
assert (before, after) == (2, 1), (before, after)

# COMMAND ----------

# MAGIC %md
# MAGIC **The other route: Change Data Feed.** The docs call it the more robust implementation:
# MAGIC stream the change feed and handle deletes in your own code. Here's the one line that
# MAGIC matters, the `delete` rows the source's feed already recorded, applied to a plain
# MAGIC downstream mirror.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.gdpr_lab.mirror (customer_id STRING, email STRING);
# MAGIC INSERT INTO lakeshore.gdpr_lab.mirror VALUES
# MAGIC   ('LAB1', 'ada@north.example'), ('LAB2', 'bob@north.example'), ('LAB3', 'eve@south.example');
# MAGIC
# MAGIC MERGE INTO lakeshore.gdpr_lab.mirror AS t
# MAGIC USING (SELECT DISTINCT customer_id FROM table_changes('lakeshore.gdpr_lab.raw', 0)
# MAGIC        WHERE _change_type = 'delete') AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN DELETE;

# COMMAND ----------

mirrored = sorted(r[0] for r in spark.table("lakeshore.gdpr_lab.mirror").collect())
assert mirrored == ["LAB2", "LAB3"], mirrored
print("✓ the delete reached the mirror through the change feed:", mirrored)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5, previewed: the row isn't gone yet.** Purge and vacuum, then time travel.

# COMMAND ----------

spark.sql("REORG TABLE lakeshore.gdpr_lab.raw APPLY (PURGE)")
spark.sql("VACUUM lakeshore.gdpr_lab.raw")
back = spark.sql("""SELECT count(*) FROM lakeshore.gdpr_lab.raw VERSION AS OF 1
                    WHERE customer_id = 'LAB1'""").first()[0]
print(f"LAB1 via time travel, after REORG PURGE and VACUUM: {back} row")
assert back == 1, "files younger than retention are still there"

# COMMAND ----------

# MAGIC %md
# MAGIC Still readable, because VACUUM only removes files older than the retention window.
# MAGIC Which is the sentence for your data protection officer: **erasure completes at
# MAGIC request + retention**, not at DELETE. The default retention is 7 days and
# MAGIC `delta.deletedFileRetentionDuration` is the knob. Going below it on the fly is refused
# MAGIC (`DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT`): fast forgetting has to be deliberate.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA lakeshore.gdpr_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Now the real platform: the target list
# MAGIC
# MAGIC The list is declared, bronze first. Each entry says how to find a customer's rows. Two of
# MAGIC these hold the PII **inside a JSON payload**, which is what the 10.4 sweep turned up.
# MAGIC Pipeline tables from S8 are on the list like any other: they're streaming tables, and
# MAGIC DML is the supported route.

# COMMAND ----------

IN_PAYLOAD = "t.topic = 'customers' AND get_json_object(t.value, '$.customer_id') = s.customer_id"
BY_ID = "t.customer_id = s.customer_id"

TARGETS = [
    # bronze first
    ("lakeshore.bronze.bus_raw",              IN_PAYLOAD),
    ("lakeshore.bronze.sdp_bus",              IN_PAYLOAD),
    ("lakeshore.bronze.sdp_cdc_customers",    BY_ID),
    # then silver
    ("lakeshore.silver.dim_customer_scd2",    BY_ID),   # ALL versions: erasure outranks history
    ("lakeshore.silver.sdp_customers_scd1",   BY_ID),
    ("lakeshore.silver.sdp_customers_scd2",   BY_ID),
    ("lakeshore.silver.token_vault",          BY_ID),
    ("lakeshore.silver.bus_customers_masked", BY_ID),   # a hash is still personal data
    ("lakeshore.silver.bus_customers_contact", BY_ID),
    # then gold
    ("lakeshore.gold.customer_current",       BY_ID),
]
TARGETS = [(t, on) for t, on in TARGETS if spark.catalog.tableExists(t)]
print(len(TARGETS), "targets on this workspace:", [t for t, _ in TARGETS])

# COMMAND ----------

# MAGIC %md
# MAGIC Note `bus_customers_masked`. It holds a salted hash and `customer_id`, neither of which
# MAGIC depends on the token vault. So "delete the vault entry and everything downstream becomes
# MAGIC anonymous" is too good to be true: a vault only protects data that holds tokens. The
# MAGIC docs say it directly: complete deletion is preferable to obfuscation.
# MAGIC
# MAGIC ## 4. Propagate: a job that reads the control table
# MAGIC
# MAGIC The docs' pattern: a scheduled job queries the requests table and MERGEs. We re-apply
# MAGIC **every** request on every run, not only new ones. MERGE is idempotent, and it means a
# MAGIC table rebuilt from a source that still holds the row gets cleaned on the next run. A
# MAGIC one-off DELETE isn't durable if something can reload the row.

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS lakeshore.gold.deletion_audit (
               request_id STRING NOT NULL,
               customer_id STRING NOT NULL,
               requested_at TIMESTAMP,
               targets_processed INT,
               rows_deleted_in_run BIGINT,
               processed_at TIMESTAMP NOT NULL)
             COMMENT 'Receipts for PII deletion requests'""")

spark.sql("""SELECT DISTINCT customer_id FROM lakeshore.bronze.delete_requests""") \
     .createOrReplaceTempView("requested_ids")

deleted = {}
for table, on in TARGETS:
    result = spark.sql(f"""MERGE INTO {table} AS t USING requested_ids AS s
                           ON {on}
                           WHEN MATCHED THEN DELETE""").first()
    deleted[table] = result["num_deleted_rows"]
    print(f"{deleted[table]:>5} rows deleted from {table}")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO lakeshore.gold.deletion_audit AS t
    USING (SELECT request_id, customer_id, requested_at,
                  {len(TARGETS)} AS targets_processed,
                  {sum(deleted.values())} AS rows_deleted_in_run,
                  current_timestamp() AS processed_at
           FROM lakeshore.bronze.delete_requests) AS s
    ON t.request_id = s.request_id
    WHEN NOT MATCHED THEN INSERT *""")

audited = spark.table("lakeshore.gold.deletion_audit").count()
print(f"deletion receipts: {audited}")
assert audited >= 10, "expected receipts for the queued requests"

# COMMAND ----------

# MAGIC %md
# MAGIC (`rows_deleted_in_run` is a run-level number, a simplification. A per-customer breakdown
# MAGIC would count before deleting. The receipt's job is that each request has one row, and
# MAGIC re-runs don't add a second.)
# MAGIC
# MAGIC ## 5. Verify: the negative proof, and what you didn't touch

# COMMAND ----------

for table, on in TARGETS:
    left = spark.sql(f"""SELECT count(*) FROM {table} AS t JOIN requested_ids AS s
                         ON {on}""").first()[0]
    assert left == 0, f"{table} still holds {left} rows"
print(f"✓ no requested customer remains in any of the {len(TARGETS)} targets")

# COMMAND ----------

# MAGIC %md
# MAGIC That proves the list. It doesn't prove the list is complete. So we ask the catalog which
# MAGIC tables have a personal-data column, and report anything still holding a requested
# MAGIC customer. An audit reads this one.

# COMMAND ----------

pii_tables = [r[0] for r in spark.sql("""
    SELECT DISTINCT concat_ws('.', c.table_catalog, c.table_schema, c.table_name)
    FROM lakeshore.information_schema.columns c
    JOIN lakeshore.information_schema.columns k
      ON  c.table_schema = k.table_schema AND c.table_name = k.table_name
      AND k.column_name = 'customer_id'
    JOIN lakeshore.information_schema.tables t
      ON  c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE c.column_name IN ('email', 'first_name', 'last_name',
                            'shipping_street', 'billing_street')
      AND c.table_schema IN ('bronze', 'silver', 'gold')
      AND t.table_type IN ('MANAGED', 'STREAMING_TABLE')
      AND c.table_name NOT LIKE '\\_\\_%'
""").collect()]

on_the_list = {t for t, _ in TARGETS}
residual = {}
for table in sorted(set(pii_tables) - on_the_list):
    residual[table] = spark.sql(f"""SELECT count(*) FROM {table} AS t JOIN requested_ids AS s
                                    ON {BY_ID}""").first()[0]
print("Tables with a PII column that are not on the list, and rows still held:")
for table, n in residual.items():
    print(f"{n:>5}  {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read that output, because here's where this course stops short of production.** You'll
# MAGIC see at least `silver.customers` and `silver.addresses`, the shared baseline every other
# MAGIC lesson reads, and the fast-forward checks their row counts. We spare them so the rest of
# MAGIC the course keeps working. In a real platform they'd be on the list. If you did the
# MAGIC Associate course, you'll also see its tables: its pipeline and job still stream from
# MAGIC them, so we leave those alone too.
# MAGIC
# MAGIC And files. The landing volume still holds these people:

# COMMAND ----------

from pyspark.sql.functions import col

ids = [r[0] for r in spark.table("requested_ids").collect()]
pattern = "|".join(ids)
upstream = (spark.read.text("/Volumes/lakeshore/landing/files/cdc/customers/")
            .filter(col("value").rlike(pattern)).count())
print(f"lines in landing/cdc/customers/ that mention a requested customer: {upstream}")

# COMMAND ----------

# MAGIC %md
# MAGIC The docs: "you must also remember to delete data in upstream sources, such as queues and
# MAGIC cloud storage." In production that's a file rewrite or a landing-zone retention rule,
# MAGIC plus the Kafka topic's own retention. We don't rewrite them here because the datagen
# MAGIC files are the course's source of truth for rebuilds. One consequence you should be able
# MAGIC to say out loud: **a full refresh of a pipeline re-reads those files and the customer
# MAGIC comes back.** That's why this job re-applies every request on every run.
# MAGIC
# MAGIC ## 6. Your consumers now need skipChangeCommits
# MAGIC
# MAGIC We just changed `bus_raw`, `sdp_bus` and `sdp_cdc_customers`. Any stream reading them
# MAGIC fails on its next run with the error from the lab, unless it carries the option. Lesson
# MAGIC 10.4's streams do. The streaming notebooks in S3 and S4 don't: re-run one of them after
# MAGIC this lesson and you'll see that error, and adding the option is the fix. The pipeline
# MAGIC source in `pipeline/` has carried it since Section 8, because the docs' last step applies
# MAGIC there too: `parsed_orders()` reads `sdp_bus` with `.option("skipChangeCommits", "true")`,
# MAGIC and both AUTO CDC flows read `sdp_cdc_customers` through a temporary view that sets it. So
# MAGIC the next update of `lakeshore-platform-sdp` runs, and it doesn't bring these rows back.
# MAGIC
# MAGIC ## 7. Make it PHYSICAL, on every target
# MAGIC
# MAGIC `MERGE` marked rows (deletion vectors). The bytes still sit in Parquet and in history.

# COMMAND ----------

for table, _ in TARGETS:
    spark.sql(f"REORG TABLE {table} APPLY (PURGE)")
    spark.sql(f"VACUUM {table}")
print(f"✓ purged and vacuumed {len(TARGETS)} targets")

# COMMAND ----------

# MAGIC %md
# MAGIC Materialized views take REORG but not VACUUM (`EXPECT_TABLE_NOT_VIEW`, measured). For
# MAGIC streaming tables and materialized views, the docs put maintenance on predictive
# MAGIC optimization, or on Lakeflow pipelines within 24 hours of an update. Predictive
# MAGIC optimization is also the recommended route for your managed tables. What you own is the
# MAGIC arithmetic: if the policy says one month, then request + job cadence + retention has to
# MAGIC fit inside it.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Control table → bronze first → DML on streaming tables + `skipChangeCommits` → refresh
# MAGIC materialized views → upstream sources → REORG PURGE → VACUUM past retention → receipts,
# MAGIC negative proof, and an honest residual report. Next: the secret hiding in our salt.

# COMMAND ----------

dbutils.notebook.exit("delete-propagation-complete")
