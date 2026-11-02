# Databricks notebook source
# MAGIC %md
# MAGIC # 10.4 — A compliant PII pipeline: detect, mask, verify
# MAGIC
# MAGIC The guide's statement: "Implement a compliant data pipeline that enforces PII
# MAGIC detection and masking controls across batch and streaming workloads using Unity
# MAGIC Catalog features." That's three jobs, and most material only does the middle one:
# MAGIC
# MAGIC 1. **Detect** where the personal data is, across layers.
# MAGIC 2. **Mask**: a transform in motion, plus a Unity Catalog control at read time.
# MAGIC 3. **Verify**, on every run, batch or streaming.
# MAGIC
# MAGIC > Prereqs: S3 (`bronze.bus_raw`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Detection: what the platform does
# MAGIC
# MAGIC > **📖 Theory, this part.** Measured on this workspace on 2026-11-01:
# MAGIC > `databricks data-classification create-catalog-config catalogs/workspace` answers
# MAGIC > *"Data Classification is not available in this workspace."* You can't run it on Free
# MAGIC > Edition. Here's what it does and what the exam needs from it.
# MAGIC
# MAGIC **Data Classification** is a Unity Catalog feature you enable per catalog (you must own
# MAGIC it or hold MANAGE). An AI agent scans tables, streaming tables and materialized views
# MAGIC incrementally, and new tables and columns are typically scanned within 24 hours. Then:
# MAGIC
# MAGIC - Results land in **`system.data_classification.results`**, which includes sample
# MAGIC   values, so by default only account admins can read it.
# MAGIC - With **automatic tagging** turned on, detections get **system governed tags** on
# MAGIC   the column: `class.email_address`, `class.name`, `class.phone_number` and so on.
# MAGIC - **An ABAC policy matching `has_tag('class.email_address')` then masks the column
# MAGIC   with no further work.** Detection feeds enforcement. That's lesson 10.8.
# MAGIC
# MAGIC The tag definitions are visible here even though the classifier isn't. List them with
# MAGIC `databricks tag-policies list-tag-policies`, and you'll see the `class.*` family.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detection you can run here: a sweep across layers
# MAGIC
# MAGIC Without the classifier, we write the crude version ourselves, so you can see what the
# MAGIC product automates. Scan every string column of a table, sample it, and test for an
# MAGIC email pattern. It finds email addresses and nothing else, and that's the honest limit:
# MAGIC a name doesn't have a pattern.

# COMMAND ----------

from pyspark.sql.functions import col

EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}"

def scan_for_emails(table, sample=500):
    """Return the string columns of `table` holding something shaped like an email."""
    df = spark.table(table).limit(sample)
    found = []
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            hits = df.filter(col(field.name).rlike(EMAIL_PATTERN)).count()
            if hits:
                found.append((field.name, hits))
    return found

for t in ["lakeshore.bronze.bus_raw", "lakeshore.silver.customers"]:
    print(t, "→", scan_for_emails(t))

# COMMAND ----------

# MAGIC %md
# MAGIC Two different answers, and both matter. In `silver.customers` the column is called
# MAGIC `email`, and a name-based sweep would have found it. In `bronze.bus_raw` the PII hides
# MAGIC **inside a JSON string called `value`**, and only a look at the contents finds it. Keep
# MAGIC that in mind for the next lesson: when you erase, `bus_raw` is a target.
# MAGIC
# MAGIC Record what you found as metadata, using the same system tag the classifier would
# MAGIC apply. Now the classification is something you can query:

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TAG ON COLUMN lakeshore.silver.customers.email `class.email_address`;
# MAGIC
# MAGIC SELECT schema_name, table_name, column_name, tag_name
# MAGIC FROM lakeshore.information_schema.column_tags
# MAGIC WHERE tag_name LIKE 'class.%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The design, before the code
# MAGIC
# MAGIC - **Bronze keeps raw**, locked down by grants. It stays in scope for erasure (10.5).
# MAGIC - **The masking boundary is silver.** One transform applies the policy, and everything
# MAGIC   downstream inherits safety by construction.
# MAGIC - **One transform, both cadences.** A plain function, called from a stream and from a
# MAGIC   batch backfill.
# MAGIC - **Two controls, because they do different jobs.** The transform means nobody
# MAGIC   downstream holds the value. A Unity Catalog column mask covers the people who
# MAGIC   legitimately keep raw values.
# MAGIC
# MAGIC ## 4. The transform, in streaming and in batch

# COMMAND ----------

from pyspark.sql.functions import date_trunc, expr, from_json

CUSTOMER_PAYLOAD = ("customer_id STRING, first_name STRING, last_name STRING, "
                    "email STRING, created_at TIMESTAMP, updated_at TIMESTAMP")

def customers_topic(df):
    """Parse the customers topic out of the multiplex bus."""
    return (df.filter(col("topic") == "customers")
              .withColumn("p", from_json(col("value").cast("string"), CUSTOMER_PAYLOAD)))

def deidentify(df):
    """The compliance transform. One definition, for every cadence."""
    return customers_topic(df).select(
        col("p.customer_id"),
        # pseudonymization: salted hash, consistent and joinable, no address
        # (the salt is hard-coded here on purpose: lesson 10.6 is the fix)
        expr("sha2(concat(p.email, 'lakeshore-salt-2026'), 256)").alias("email_hash"),
        # generalization: exact timestamp to a month cohort
        date_trunc("MONTH", col("p.created_at")).alias("signup_cohort"),
        # suppression: first_name and last_name simply aren't selected
        col("event_ts"),
    )

# skipChangeCommits: lesson 10.5 deletes from bus_raw, and without this option a re-run of
# this cell after that lesson would fail. The next lesson explains why.
(spark.readStream.option("skipChangeCommits", "true")
 .table("lakeshore.bronze.bus_raw")
 .transform(deidentify)
 .writeStream
 .option("checkpointLocation",
         "/Volumes/lakeshore/landing/files/_checkpoints/bus_customers_masked")
 .trigger(availableNow=True)
 .toTable("lakeshore.silver.bus_customers_masked")
 .awaitTermination())

n = spark.table("lakeshore.silver.bus_customers_masked").count()
print(f"silver.bus_customers_masked: {n} de-identified update events")
assert n > 0

# COMMAND ----------

# Batch: same function, no stream. The backfill-and-reconcile cadence.
batch = spark.read.table("lakeshore.bronze.bus_raw").transform(deidentify)
streamed = spark.table("lakeshore.silver.bus_customers_masked")
assert batch.schema.simpleString() == streamed.schema.simpleString()
print(f"✓ same transform, same schema: batch sees {batch.count()} events")

# COMMAND ----------

# MAGIC %md
# MAGIC `availableNow` is the only trigger you can use on serverless, so that's how this runs
# MAGIC here. `processingTime` and the default trigger fail with
# MAGIC `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. For continuous cadence: a Lakeflow pipeline
# MAGIC in continuous mode, or classic compute. Either way, the transform doesn't change.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakeshore.silver.bus_customers_masked LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The Unity Catalog control: support keeps the address, nobody else sees it
# MAGIC
# MAGIC Support needs the real email, so a hash is no good for them. The stream feeds a second
# MAGIC table, and a **column mask** decides who sees what at read time. Same data path, and
# MAGIC this time the control lives in Unity Catalog. The stream writes real values, and readers
# MAGIC outside the group see the domain only.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.f_contact_mask(email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('lakeshore_support') THEN email
# MAGIC             ELSE concat('****', substr(email, instr(email, '@'))) END;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lakeshore.silver.bus_customers_contact (
# MAGIC   customer_id STRING, email STRING, event_ts TIMESTAMP)
# MAGIC COMMENT 'Customer contact details for support. Email is masked outside lakeshore_support.';
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.bus_customers_contact
# MAGIC   ALTER COLUMN email SET MASK lakeshore.silver.f_contact_mask;

# COMMAND ----------

(spark.readStream.option("skipChangeCommits", "true")
 .table("lakeshore.bronze.bus_raw")
 .transform(customers_topic)
 .select("p.customer_id", "p.email", "event_ts")
 .writeStream
 .option("checkpointLocation",
         "/Volumes/lakeshore/landing/files/_checkpoints/bus_customers_contact")
 .trigger(availableNow=True)
 .toTable("lakeshore.silver.bus_customers_contact")
 .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We aren't in lakeshore_support, so this is the locked-out view.
# MAGIC SELECT * FROM lakeshore.silver.bus_customers_contact LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prove the property, don't assume it
# MAGIC
# MAGIC A compliance pipeline ships with its own test. The sweep from section 2 is that test:
# MAGIC point it at the output, and no column may look like an email. This belongs in CI (S12)
# MAGIC and in a scheduled alert (S9).

# COMMAND ----------

assert scan_for_emails("lakeshore.silver.bus_customers_masked", sample=100000) == []
assert scan_for_emails("lakeshore.silver.bus_customers_contact", sample=100000) == []

cols = set(spark.table("lakeshore.silver.bus_customers_masked").columns)
assert not ({"first_name", "last_name", "email"} & cols), cols

contact_rows = spark.table("lakeshore.silver.bus_customers_contact").count()
assert contact_rows > 0
print("✓ no column in the masked table looks like an email, and PII columns are absent")
print(f"✓ all {contact_rows} contact rows read as masked for a non-member")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Joins still work: the point of pseudonymization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The bus marks updated emails with a 'new.' prefix. Hash the SAME value independently,
# MAGIC -- same salt, and the stand-ins line up. Determinism IS the linkage.
# MAGIC WITH ref AS (
# MAGIC   SELECT customer_id,
# MAGIC          sha2(concat('new.', email, 'lakeshore-salt-2026'), 256)
# MAGIC            AS email_hash
# MAGIC   FROM lakeshore.silver.customers)
# MAGIC SELECT count(*) AS linkable_updates
# MAGIC FROM lakeshore.silver.bus_customers_masked m
# MAGIC JOIN ref USING (email_hash);

# COMMAND ----------

matches = spark.sql("""
    WITH ref AS (SELECT sha2(concat('new.', email, 'lakeshore-salt-2026'), 256) AS h
                 FROM lakeshore.silver.customers)
    SELECT count(*) FROM lakeshore.silver.bus_customers_masked m
    JOIN ref ON m.email_hash = ref.h""").first()[0]
print(f"linkable updates via hash: {matches}")
assert matches > 0, "deterministic hashing must reproduce the stand-ins"

# COMMAND ----------

# MAGIC %md
# MAGIC Analytics keeps its joins; names and addresses stay home. The flip side: a customer
# MAGIC whose email CHANGED gets a different pseudonym, because hash linkage follows the VALUE.
# MAGIC When identity must survive value changes, that's the token vault's job (lesson 10.3):
# MAGIC tokens attach to the person, hashes to the datum.
# MAGIC
# MAGIC The grants close the loop: bronze (raw) goes to the platform team only, masked silver
# MAGIC to analysts. The privilege model from 10.1 IS part of the pipeline design.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Detect (Data Classification in paid workspaces, a sweep here) · mask in motion at one
# MAGIC boundary, one function for both cadences · a column mask for the people who keep raw
# MAGIC values · the property is TESTED · grants finish the job. Next: the queue we've been
# MAGIC carrying since Section 1. Delete requests finally get executed.

# COMMAND ----------

dbutils.notebook.exit("pii-pipeline-complete")
