"""Unit tests for lakeshore_pro: fake data in, exact expectations out."""

import json

from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

import lakeshore_pro
from lakeshore_pro import add_cart_total, parse_bus_orders, pseudonymize_email, reject_reason
from lakeshore_pro.schemas import ORDER_PAYLOAD

BUS = "topic STRING, value STRING, event_ts TIMESTAMP, offset BIGINT, source_file STRING"
ITEMS = "ARRAY<STRUCT<product_id: STRING, quantity: INT, unit_price: DOUBLE>>"


def bus_row(payload, topic="orders"):
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return (topic, text, None, 1, "f.json")


def order(order_id="O1", customer_id="C1", quantity=2):
    return {"order_id": order_id, "customer_id": customer_id,
            "order_ts": "2026-01-01T00:00:00",
            "items": [{"product_id": "P1", "quantity": quantity, "unit_price": 100.0}]}


def test_the_package_exposes_its_api():
    assert lakeshore_pro.__version__
    assert lakeshore_pro.add_cart_total is add_cart_total


def test_cart_total_handles_an_empty_cart(spark):
    fake = spark.createDataFrame([("O1", [("P1", 2, 100.0)]), ("O2", [])],
                                 f"order_id STRING, items {ITEMS}")
    expected = spark.createDataFrame([("O1", 200.0), ("O2", 0.0)],
                                     "order_id STRING, cart_total DOUBLE")
    assertDataFrameEqual(add_cart_total(fake).select("order_id", "cart_total"), expected)


def test_parse_keeps_only_the_orders_topic(spark):
    bus = spark.createDataFrame([bus_row(order()), bus_row(order("O2"), topic="payments")], BUS)
    assert [r.order_id for r in parse_bus_orders(bus).collect()] == ["O1"]


def test_parse_declares_the_payload_schema(spark):
    bus = spark.createDataFrame([bus_row(order())], BUS)
    actual = parse_bus_orders(bus).select("order_id", "customer_id", "order_ts", "items")
    assertSchemaEqual(actual.schema, spark.createDataFrame([], ORDER_PAYLOAD).schema)


def test_reject_reason_walks_the_ladder(spark):
    bus = spark.createDataFrame(
        [bus_row(order("OK")), bus_row("{not json"),
         bus_row(order("NC", customer_id=None)), bus_row(order("NQ", quantity=0))], BUS)
    graded = reject_reason(parse_bus_orders(bus)).select("order_id", "reject_reason")
    expected = spark.createDataFrame(
        [("OK", None), (None, "unparseable payload"),
         ("NC", "missing customer_id"), ("NQ", "non-positive quantity")],
        "order_id STRING, reject_reason STRING")
    assertDataFrameEqual(graded, expected)


def test_pseudonymize_drops_the_raw_address(spark):
    fake = spark.createDataFrame([("C1", "ana@example.com")], "customer_id STRING, email STRING")
    out = pseudonymize_email(fake, salt="s3")
    assert out.columns == ["customer_id", "email_hash"]
    assert len(out.first().email_hash) == 64
