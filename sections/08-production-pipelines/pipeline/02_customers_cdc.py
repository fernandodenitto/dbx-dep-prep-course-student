# Lakeshore platform pipeline — file 2: CDC customers → SCD1 + SCD2
# The S3/S4 choreography (rank, stale guard, close-and-insert) as ONE clause:
# the AUTO CDC APIs (formerly APPLY CHANGES INTO).

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

CDC_PATH = "/Volumes/lakeshore/landing/files/cdc/customers"
CDC_SCHEMA = ("operation STRING, sequence_num BIGINT, event_ts TIMESTAMP, "
              "customer_id STRING, first_name STRING, last_name STRING, "
              "email STRING")

# ------------------------------------------------------------ raw CDC (ST)


@dp.table(
    name="bronze.sdp_cdc_customers",
    comment="Raw customer change events, declarative ingestion",
)
def sdp_cdc_customers():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(CDC_SCHEMA)
            .load(CDC_PATH))

# ------------------------------------------------- the source both flows read
# An AUTO CDC source is a streaming read, so a DELETE on it breaks the flow. Lesson 10.5
# deletes from bronze.sdp_cdc_customers. create_auto_cdc_flow takes a name, not options, so
# the option goes on a temporary view and the flows read that.


@dp.temporary_view(name="cdc_customers_appends")
def cdc_customers_appends():
    return (spark.readStream
            .option("skipChangeCommits", "true")
            .table("lakeshore.bronze.sdp_cdc_customers"))

# --------------------------------------------------------------- SCD Type 1
# Target declared empty, then a flow feeds it: the AUTO CDC pattern.

dp.create_streaming_table(
    name="silver.sdp_customers_scd1",
    comment="Current-state customers (SCD1) via AUTO CDC",
)

dp.create_auto_cdc_flow(
    target="silver.sdp_customers_scd1",
    source="cdc_customers_appends",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num", "event_ts"],
    stored_as_scd_type=1,
)

# --------------------------------------------------------------- SCD Type 2
# Same source, same clause — one digit changes and history is kept.

dp.create_streaming_table(
    name="silver.sdp_customers_scd2",
    comment="Versioned customers (SCD2) via AUTO CDC — __START_AT/__END_AT",
)

dp.create_auto_cdc_flow(
    target="silver.sdp_customers_scd2",
    source="cdc_customers_appends",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num", "event_ts"],
    stored_as_scd_type=2,
)
