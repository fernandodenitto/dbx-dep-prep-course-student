# Lakeshore platform pipeline — file 1: bus → orders (+quarantine) → gold
# Lakeflow pipelines source (built on Apache Spark Declarative Pipelines; formerly Delta Live Tables).
# Current import: pyspark.pipelines as dp. Older code says `import dlt`, which still works.
#
# silver.sdp_orders_valid is not called silver.sdp_orders on purpose: a table belongs to one
# pipeline, and the Associate course's pipeline owns that name. Lesson 8.2 shows the error.

from pyspark import pipelines as dp
from pyspark.sql.functions import (col, explode, expr, from_json,
                                   timestamp_millis, to_date)

BUS_PATH = "/Volumes/lakeshore/landing/files/topics"
BUS_SCHEMA = ("key STRING, value STRING, topic STRING, "
              "partition BIGINT, offset BIGINT, timestamp BIGINT")
ORDER_PAYLOAD = ("order_id STRING, customer_id STRING, order_ts TIMESTAMP, "
                 "items ARRAY<STRUCT<product_id: STRING, quantity: INT, "
                 "unit_price: DOUBLE>>")

# ---------------------------------------------------------------- bronze (ST)


@dp.table(
    name="bronze.sdp_bus",
    comment="Multiplex bus envelope, declarative ingestion (Auto Loader)",
    cluster_by=["topic", "event_date"],
)
def sdp_bus():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(BUS_SCHEMA)
            .load(BUS_PATH)
            .withColumn("event_ts", timestamp_millis(col("timestamp")))
            .withColumn("event_date",
                        to_date(timestamp_millis(col("timestamp"))))
            .withColumn("source_file", col("_metadata.file_path")))

# ------------------------------------------------- silver: gate + quarantine


def parsed_orders():
    """Shared parse: the S3 v3 consumer, declaratively.

    Dedup BEFORE the gate (expectations judge validity, not novelty) —
    same watermark discipline as Section 3, framework-managed state.

    skipChangeCommits: lesson 10.5 deletes from bronze.sdp_bus to honor erasure
    requests, and without this option the next update fails on that commit.
    """
    return (spark.readStream
            .option("skipChangeCommits", "true")
            .table("lakeshore.bronze.sdp_bus")
            .filter(col("topic") == "orders")
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["key"])
            .withColumn("payload",
                        from_json(col("value").cast("string"), ORDER_PAYLOAD))
            .select("payload.order_id", "payload.customer_id",
                    "payload.order_ts", "payload.items",
                    "event_ts", "offset", "source_file"))


@dp.table(
    name="silver.sdp_orders_valid",
    comment="Orders that passed the quality gate — trustworthy by construction",
)
@dp.expect_all_or_drop({
    "parsed_payload": "order_id IS NOT NULL",
    "has_customer": "customer_id IS NOT NULL",
    "positive_quantities": "NOT exists(items, i -> i.quantity <= 0)",
})
def sdp_orders_valid():
    return parsed_orders()


@dp.table(
    name="silver.sdp_orders_quarantine",
    comment="Rejected order records with reasons — the repair queue",
)
def sdp_orders_quarantine():
    return (parsed_orders()
            .withColumn("reason", expr("""CASE
                WHEN order_id IS NULL THEN 'unparseable payload'
                WHEN customer_id IS NULL THEN 'missing customer_id'
                WHEN exists(items, i -> i.quantity <= 0)
                     THEN 'non-positive quantity' END"""))
            .filter("reason IS NOT NULL"))

# -------------------------------------------------------------- gold (MV)


@dp.materialized_view(
    name="gold.sdp_category_day",
    comment="Daily revenue by category — recomputed to correctness",
)
def sdp_category_day():
    items = (spark.read.table("lakeshore.silver.sdp_orders_valid")
             .select("order_id", to_date("order_ts").alias("order_date"),
                     explode("items").alias("item")))
    dim = spark.read.table("lakeshore.gold.dim_product")
    return (items.join(dim, items.item.product_id == dim.product_id)
            .groupBy("order_date", "category")
            .agg(expr("round(sum(item.quantity * item.unit_price), 2)")
                 .alias("revenue"),
                 expr("sum(item.quantity)").alias("units")))
