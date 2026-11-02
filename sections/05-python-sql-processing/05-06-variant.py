# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 5.6 — VARIANT: semi-structured data without a fixed schema
# MAGIC
# MAGIC `bronze.bus_raw.value` is a JSON string, and it holds three shapes: orders,
# MAGIC payments and customers. In Section 3 each consumer declared a schema with
# MAGIC `from_json`. Fine for one topic. Now someone wants to explore all of them, and
# MAGIC tomorrow's payload may have a field nobody has declared yet.
# MAGIC
# MAGIC That is what the `VARIANT` type is for. The guide's statement: "Develop a model
# MAGIC and query semi-structured data using the VARIANT data type and related functions
# MAGIC (e.g., parse_json, variant_get)."
# MAGIC
# MAGIC > Prereqs: `bronze.bus_raw` with batches 1–4 (Section 3). Batch 4 matters: it
# MAGIC > holds the truncated JSON record.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `parse_json` meets production data
# MAGIC
# MAGIC `parse_json` turns a JSON string into a `VARIANT`. Try it on the whole table.
# MAGIC Remember batch 4 carries one truncated payload.

# COMMAND ----------

from pyspark.sql import functions as F

def expect_failure(label, action):
    """Run something that must fail, and show the first line of why."""
    try:
        action()
    except Exception as error:
        print(f"{label}:\n  {str(error).splitlines()[0][:160]}")
        return
    raise AssertionError(f"{label} should have failed")

bus = spark.table("lakeshore.bronze.bus_raw")
expect_failure("parse_json on the whole table",
               lambda: bus.select(F.parse_json("value")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC One bad record, whole query dead. Remember `from_json` in lesson 3.4: it returned
# MAGIC NULL silently. `parse_json` is the opposite, and `try_parse_json` is the one you
# MAGIC want on raw data: NULL instead of an error, so you can count and route the bad ones.
# MAGIC
# MAGIC ## 2. The model: one VARIANT column, typed columns around it
# MAGIC
# MAGIC The envelope keeps its real types, the payload becomes a `VARIANT`, and the
# MAGIC clustering keys stay ordinary columns, for a reason we'll hit in section 6.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.bus_events_v
# MAGIC CLUSTER BY (topic, event_date)
# MAGIC COMMENT 'All bus topics, payload as VARIANT. Exploration table, one row per delivery.'
# MAGIC AS SELECT key, topic, offset, event_ts, event_date,
# MAGIC           try_parse_json(value) AS payload
# MAGIC    FROM lakeshore.bronze.bus_raw;

# COMMAND ----------

unparseable = spark.table("lakeshore.silver.bus_events_v").filter("payload IS NULL")
print(f"rows with no parseable payload: {unparseable.count()}")
assert unparseable.count() >= 1, "the truncated batch-4 record must surface here"
display(unparseable.select("key", "topic", "offset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Colon paths, and the cast you can't skip
# MAGIC
# MAGIC `:` selects a field, `.` or `[n]` go deeper. No schema anywhere, and all three
# MAGIC topics answer from one table. A path returns a `VARIANT`, so cast it with `::`
# MAGIC before you treat it as a string or a number.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT topic,
# MAGIC        payload:order_id::string            AS order_id,
# MAGIC        payload:items[0].product_id::string AS first_product,
# MAGIC        payload:amount::double              AS amount
# MAGIC FROM lakeshore.silver.bus_events_v
# MAGIC WHERE topic IN ('orders', 'payments')
# MAGIC ORDER BY event_ts, topic LIMIT 8;

# COMMAND ----------

# MAGIC %md
# MAGIC Fields a topic doesn't have come back NULL, not as an error. And paths are
# MAGIC **case-sensitive**: the docs say "All variant path elements are matched in a
# MAGIC case-sensitive way."

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same row, two spellings: the wrong case finds nothing, silently
# MAGIC SELECT payload:order_id::string AS right_case,
# MAGIC        payload:ORDER_ID::string AS wrong_case
# MAGIC FROM lakeshore.silver.bus_events_v
# MAGIC WHERE topic = 'orders' AND payload IS NOT NULL LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `variant_get` vs `try_variant_get`
# MAGIC
# MAGIC `variant_get(payload, '$.path', 'type')` is the function form of a path plus a
# MAGIC cast, with a JSON path string. The two functions differ on exactly one thing:
# MAGIC
# MAGIC | Case | `variant_get` | `try_variant_get` |
# MAGIC |---|---|---|
# MAGIC | Path found, cast works | the value | the value |
# MAGIC | Path missing | NULL | NULL |
# MAGIC | Path found, **cast fails** | **`INVALID_VARIANT_CAST`, the query fails** | **NULL** |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT variant_get(payload, '$.amount', 'double')        AS amount,
# MAGIC        variant_get(payload, '$.no_such_field', 'double') AS missing_path,
# MAGIC        try_variant_get(payload, '$.payment_id', 'int')   AS failed_cast_is_null
# MAGIC FROM lakeshore.silver.bus_events_v
# MAGIC WHERE topic = 'payments' AND payload IS NOT NULL LIMIT 3;

# COMMAND ----------

payments = (spark.table("lakeshore.silver.bus_events_v")
            .filter("topic = 'payments' AND payload IS NOT NULL").limit(5))

# The same functions exist in PySpark, for use in a DataFrame or a stream
typed = payments.select(
    F.variant_get("payload", "$.amount", "double").alias("amount"),
    F.try_variant_get("payload", "$.payment_id", "int").alias("not_an_int"))
assert typed.filter("amount IS NULL").count() == 0
assert typed.filter("not_an_int IS NOT NULL").count() == 0

expect_failure("variant_get on a failed cast",
               lambda: payments.select(F.variant_get("payload", "$.payment_id", "int")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Arrays, and what shape is actually in there
# MAGIC
# MAGIC `[*]` isn't supported on `VARIANT`. To unpack an array, `variant_explode` goes in
# MAGIC the FROM clause with `LATERAL`, and `schema_of_variant_agg` tells you the combined
# MAGIC shape of a column you didn't design.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.key AS topic_key, i.pos,
# MAGIC        i.value:product_id::string AS product_id,
# MAGIC        i.value:quantity::int      AS quantity
# MAGIC FROM (SELECT key, payload FROM lakeshore.silver.bus_events_v
# MAGIC       WHERE topic = 'orders' AND payload IS NOT NULL ORDER BY key LIMIT 3) e,
# MAGIC      LATERAL variant_explode(e.payload:items) i
# MAGIC ORDER BY topic_key, i.pos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT topic, schema_of_variant_agg(payload) AS combined_schema
# MAGIC FROM lakeshore.silver.bus_events_v
# MAGIC GROUP BY topic ORDER BY topic;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The limits, run on purpose
# MAGIC
# MAGIC The docs: "`VARIANT` columns cannot be used for clustering keys, partitions, or
# MAGIC Z-order keys", and the type "cannot be used for comparisons, grouping, ordering,
# MAGIC and set operations." Let's see both. These are the facts a design question turns on.

# COMMAND ----------

expect_failure("cluster by a VARIANT", lambda: spark.sql("""
    CREATE OR REPLACE TABLE lakeshore.silver.zz_variant_lab (payload VARIANT)
    CLUSTER BY (payload)""").collect())
expect_failure("group by a VARIANT", lambda: spark.sql("""
    SELECT payload, count(*) FROM lakeshore.silver.bus_events_v GROUP BY payload""").collect())

# COMMAND ----------

# MAGIC %md
# MAGIC So the model answers itself. If you filter on a field, **promote it to a real
# MAGIC column** and cluster on that, as `topic` and `event_date` are here. Group on a cast
# MAGIC path, not on the `VARIANT`:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payload:method::string AS method, count(*) AS payments,
# MAGIC        round(sum(payload:amount::double), 2) AS amount
# MAGIC FROM lakeshore.silver.bus_events_v
# MAGIC WHERE topic = 'payments' AND payload IS NOT NULL
# MAGIC GROUP BY ALL ORDER BY amount DESC;

# COMMAND ----------

# Reconcile: the VARIANT path agrees with the declared-schema parse from Section 3
via_variant = (spark.table("lakeshore.silver.bus_events_v")
               .filter("topic = 'orders' AND payload IS NOT NULL")
               .select(F.expr("payload:order_id::string").alias("order_id"))
               .filter("order_id IS NOT NULL").distinct().count())
via_struct = spark.table("lakeshore.bronze.bus_raw").filter("topic = 'orders'").select(
    F.from_json("value", "order_id STRING").alias("p")).select("p.order_id").filter(
    "order_id IS NOT NULL").distinct().count()
print(f"distinct orders · VARIANT path: {via_variant} · from_json: {via_struct}")
assert via_variant == via_struct, (via_variant, via_struct)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Which model, when
# MAGIC
# MAGIC | | JSON `STRING` | `VARIANT` | Declared `STRUCT` |
# MAGIC |---|---|---|---|
# MAGIC | Schema needed up front | no | no | **yes** |
# MAGIC | A new field appears | invisible until parsed | **queryable at once** | needs a schema change |
# MAGIC | Query | parse on every read | path and cast | **dot, with real types** |
# MAGIC | Bad JSON | stays a string | NULL with `try_parse_json` | NULL from `from_json` |
# MAGIC | Cluster on a field | no | no, promote it | **yes** |
# MAGIC | Fits | raw byte-faithful bronze | varying shape, exploration | stable silver contract |
# MAGIC
# MAGIC Databricks recommends `VARIANT` over JSON strings for semi-structured data. That's
# MAGIC not the same as "over a declared schema": `silver.bus_orders` keeps its `STRUCT`,
# MAGIC because at silver types are a contract. One honest caveat: our bronze `value` stays
# MAGIC a string too, because it's the byte-faithful record of what the bus sent.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `parse_json` fails on bad input, `try_parse_json` returns NULL. Colon paths and `::`
# MAGIC for reading, `variant_get` and `try_variant_get` for functions and PySpark. Paths
# MAGIC are case-sensitive. No clustering, grouping or comparing on a `VARIANT` itself:
# MAGIC promote or cast. Next: fields no parser can extract, because they're free text.

# COMMAND ----------

dbutils.notebook.exit("variant-complete")
