"""Lakeshore Outfitters — synthetic data generator.

Companion module for the course. It generates a small, realistic e-commerce
dataset with this shape:

    customers ──< addresses   (1-2 per customer: shipping / billing)
        │
        └──< orders           (each with a nested `items` array)
                └── payments  (exactly one per order)
                        └──< refunds  (a small share of payments)

Design rules (explained in lesson 1.5):
- Python standard library only. You can read every line — no magic.
- Deterministic: same seed => identical data. Your tables match the videos.
- Referential integrity: every foreign key points to a row that exists.
- Data-quality problems are OPT-IN via `dirty=True` — clean by default.
"""

import csv
import json
import random
from datetime import datetime, timedelta

# --------------------------------------------------------------- reference data

FIRST_NAMES = ["Ava", "Ben", "Carla", "Diego", "Elena", "Felix", "Grace", "Hugo",
               "Iris", "Jonas", "Karim", "Lena", "Marco", "Nadia", "Oscar", "Paula",
               "Quinn", "Rosa", "Sam", "Tara"]

LAST_NAMES = ["Alvarez", "Baker", "Costa", "Dietrich", "Evans", "Fischer", "Gomez",
              "Hansen", "Ito", "Jensen", "Keller", "Lindberg", "Moretti", "Novak",
              "Olsen", "Petrov", "Quintana", "Rossi", "Silva", "Tanaka"]

CITIES = [("Denver", "CO"), ("Seattle", "WA"), ("Portland", "OR"), ("Boulder", "CO"),
          ("Salt Lake City", "UT"), ("Bozeman", "MT"), ("Asheville", "NC"),
          ("Flagstaff", "AZ"), ("Burlington", "VT"), ("Bend", "OR"),
          ("Boise", "ID"), ("Missoula", "MT")]

STREETS = ["Alpine Way", "Basecamp Rd", "Cedar Ridge Ln", "Driftwood Ave",
           "Elk Meadow Dr", "Foothill Blvd", "Glacier St", "Highline Trail",
           "Juniper Ct", "Lakeview Dr", "Summit Pass", "Timberline Rd"]

# Fixed product catalog: (product_id, name, category, unit_price)
PRODUCTS = [
    ("P001", "Ridgeline 2P Tent",        "Camping",  249.00),
    ("P002", "Ridgeline 4P Tent",        "Camping",  399.00),
    ("P003", "Ember Down Sleeping Bag",  "Camping",  179.00),
    ("P004", "Basecamp Camping Stove",   "Camping",   89.00),
    ("P005", "Torrent 40L Backpack",     "Hiking",   139.00),
    ("P006", "Torrent 65L Backpack",     "Hiking",   189.00),
    ("P007", "Switchback Trekking Poles","Hiking",    69.00),
    ("P008", "Cascade Hiking Boots",     "Footwear", 159.00),
    ("P009", "Riverbed Trail Runners",   "Footwear", 129.00),
    ("P010", "Summit Insulated Jacket",  "Apparel",  199.00),
    ("P011", "Drizzle Rain Shell",       "Apparel",  119.00),
    ("P012", "Alpenglow Fleece",         "Apparel",   79.00),
    ("P013", "Merino Base Layer",        "Apparel",   59.00),
    ("P014", "Glacier Sunglasses",       "Gear",      89.00),
    ("P015", "Lumen 400 Headlamp",       "Gear",      45.00),
    ("P016", "Hydra 2L Water Bladder",   "Gear",      35.00),
    ("P017", "Trailside First Aid Kit",  "Gear",      29.00),
    ("P018", "Otter Dry Bag 20L",        "Water",     39.00),
    ("P019", "Lakeshore Touring Kayak",  "Water",    899.00),
    ("P020", "Driftline Paddle",         "Water",    129.00),
]

PAYMENT_METHODS = ["credit_card", "paypal", "gift_card", "bank_transfer"]

# Numeric status codes on purpose — decoding them into words is a Section 6 exercise.
PAYMENT_STATUS_CODES = {1: "completed", 2: "pending", 3: "failed"}

REFUND_REASONS = ["DAMAGED", "WRONG_ITEM", "TOO_SMALL", "TOO_LARGE", "CHANGED_MIND"]
REFUND_SOURCES = ["customer_support", "self_service"]

BASE_DATE = datetime(2026, 1, 5, 8, 0, 0)   # the shop "opened" here
DEFAULT_SEED = 2026

# ------------------------------------------------------------------- generators


def generate_customers(n=200, seed=DEFAULT_SEED, dirty=False):
    """Return `n` customers. With dirty=True: ~5% null emails, ~3% duplicates."""
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created = BASE_DATE + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
        rows.append({
            "customer_id": f"C{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{i}@example.com".lower(),
            "created_at": created.isoformat(),
        })
    if dirty:
        for row in rng.sample(rows, k=max(1, n // 20)):      # ~5% missing email
            row["email"] = None
        for row in rng.sample(rows, k=max(1, n * 3 // 100)):  # ~3% duplicated later
            dup = dict(row)
            later = datetime.fromisoformat(row["created_at"]) + timedelta(days=1)
            dup["created_at"] = later.isoformat()
            rows.append(dup)
    return rows


def generate_addresses(customers, seed=DEFAULT_SEED):
    """1 shipping address per customer, plus a billing address for ~70% of them.

    One ROW per address (long format). The fast-forward notebook pivots
    shipping/billing into one row per customer: `silver.addresses`.
    """
    rng = random.Random(seed)
    rows = []
    for cust in customers:
        types = ["shipping"] + (["billing"] if rng.random() < 0.7 else [])
        for addr_type in types:
            city, state = rng.choice(CITIES)
            rows.append({
                "customer_id": cust["customer_id"],
                "address_type": addr_type,
                "street": f"{rng.randint(1, 999)} {rng.choice(STREETS)}",
                "city": city,
                "state": state,
                "postcode": f"{rng.randint(10000, 99999)}",
            })
    return rows


def generate_products():
    """The fixed Lakeshore catalog (no randomness — it's reference data)."""
    return [{"product_id": p, "name": n, "category": c, "unit_price": u}
            for p, n, c, u in PRODUCTS]


def generate_orders(customers, n=1000, seed=DEFAULT_SEED, dirty=False):
    """Return `n` orders with a nested `items` array (1-4 products, qty 1-3).

    Every order belongs to an existing customer and happens AFTER that
    customer's signup. With dirty=True: ~4% of orders contain a duplicated
    item entry (deduplicating the array is a Section 6 exercise).
    """
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        cust = rng.choice(customers)
        signup = datetime.fromisoformat(cust["created_at"])
        order_ts = signup + timedelta(minutes=rng.randint(60, 60 * 24 * 60))
        items = []
        for prod_id, name, category, price in rng.sample(PRODUCTS, k=rng.randint(1, 4)):
            items.append({"product_id": prod_id,
                          "quantity": rng.randint(1, 3),
                          "unit_price": price})
        if dirty and rng.random() < 0.04:
            items.append(dict(items[0]))          # duplicated item entry
        rows.append({
            "order_id": f"O{i:06d}",
            "customer_id": cust["customer_id"],
            "order_ts": order_ts.isoformat(),
            "items": items,
        })
    return rows


def generate_payments(orders, seed=DEFAULT_SEED):
    """Exactly one payment per order. `status_code` is numeric on purpose."""
    rng = random.Random(seed)
    rows = []
    for order in orders:
        amount = sum(it["quantity"] * it["unit_price"] for it in order["items"])
        pay_ts = datetime.fromisoformat(order["order_ts"]) + \
            timedelta(minutes=rng.randint(0, 30))
        rows.append({
            "payment_id": order["order_id"].replace("O", "PAY"),
            "order_id": order["order_id"],
            "payment_ts": pay_ts.isoformat(),
            "amount": round(amount, 2),
            "method": rng.choice(PAYMENT_METHODS),
            "status_code": rng.choices([1, 2, 3], weights=[85, 10, 5])[0],
        })
    return rows


def generate_refunds(payments, seed=DEFAULT_SEED, rate=0.08):
    """Refund ~`rate` of COMPLETED payments, never more than the paid amount.

    `reason` is packed as "REASON|source" (e.g. "DAMAGED|self_service"). The
    fast-forward notebook splits it apart: `silver.refunds`.
    """
    rng = random.Random(seed)
    completed = [p for p in payments if p["status_code"] == 1]
    rows = []
    for i, pay in enumerate(rng.sample(completed, k=int(len(completed) * rate)), 1):
        refund_ts = datetime.fromisoformat(pay["payment_ts"]) + \
            timedelta(days=rng.randint(1, 30))
        rows.append({
            "refund_id": f"R{i:05d}",
            "payment_id": pay["payment_id"],
            "refund_ts": refund_ts.isoformat(),
            "amount": round(pay["amount"] * rng.choice([0.25, 0.5, 1.0]), 2),
            "reason": f"{rng.choice(REFUND_REASONS)}|{rng.choice(REFUND_SOURCES)}",
        })
    return rows


def generate_all(seed=DEFAULT_SEED, n_customers=200, n_orders=1000, dirty=False):
    """One-call entry point. Returns {entity_name: list_of_dicts}."""
    customers = generate_customers(n_customers, seed=seed, dirty=dirty)
    orders = generate_orders(customers, n_orders, seed=seed, dirty=dirty)
    payments = generate_payments(orders, seed=seed)
    return {
        "customers": customers,
        "addresses": generate_addresses(customers, seed=seed),
        "products": generate_products(),
        "orders": orders,
        "payments": payments,
        "refunds": generate_refunds(payments, seed=seed),
    }

# ---------------------------------------------------------------------- writers


def write_json(rows, path):
    """Write rows as JSON Lines (one JSON object per line) — Spark's favourite."""
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_csv(rows, path, delimiter=","):
    """Write rows as CSV with a header row. Use delimiter='\\t' for TSV."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)

# ==================================================================== v2 (Pro)
# Professional-course extensions. Same rules as v1: stdlib only, deterministic,
# referential integrity. v2 NEVER writes into v1 folders and all v2 event
# timestamps start after every v1 timestamp (V2_BASE below).

import zlib

V2_BASE = datetime(2026, 8, 1, 9, 0, 0)   # after all v1 data (v1 feed ended July)
FIRST_BUS_ORDER = 2001                     # v1 files used 1-1000, v1 feed 1001-1100
CDC_UPDATABLE = ["email", "last_name"]     # profile fields a customer can change


def wrap_as_bus_records(rows, topic, key_field, batch_num, base_ts, partitions=4):
    """Wrap entity rows as Kafka-style bus records (our honest bus SIMULATION).

    Each record: {key, value, topic, partition, offset, timestamp} — `value` is
    the row as a JSON STRING (consumers must parse it, like real Kafka bytes),
    `partition` is a stable hash of the key, `offset` increases per partition.
    """
    per_partition = {}
    records = []
    for i, row in enumerate(rows):
        key = row[key_field]
        part = zlib.crc32(key.encode()) % partitions          # stable across runs
        pos = per_partition.get(part, 0)
        per_partition[part] = pos + 1
        ts = base_ts + timedelta(seconds=i * 15)
        records.append({
            "key": key,
            "value": json.dumps(row),
            "topic": topic,
            "partition": part,
            "offset": batch_num * 10_000 + pos,
            "timestamp": int(ts.timestamp() * 1000),          # epoch millis
        })
    return records


def generate_bus_batch(batch_num, seed=DEFAULT_SEED, orders_per_batch=30,
                       redeliver_frac=0.0, with_bad_records=False):
    """One multiplex file drop: orders + payments + customer updates, mixed.

    Deterministic per (seed, batch_num). Order ids continue from
    FIRST_BUS_ORDER so they never collide with v1 files or the v1 feed.
    `redeliver_frac` > 0 simulates AT-LEAST-ONCE delivery: that fraction of
    the PREVIOUS batch's records is re-appended verbatim (same key, value,
    offset — a true redelivery, which is also LATE data by event time).
    `with_bad_records=True` corrupts three order payloads the way real buses
    do: a zero quantity, a null customer_id, and one truncated (unparseable)
    JSON value. All extras use separate RNG streams, so the default output
    is unchanged.
    """
    customers = generate_customers(200, seed=seed)            # same pool as v1
    rng = random.Random(f"{seed}-bus-{batch_num}")
    base_ts = V2_BASE + timedelta(minutes=30 * (batch_num - 1))
    start_id = FIRST_BUS_ORDER + (batch_num - 1) * orders_per_batch

    orders, payments = [], []
    for i in range(orders_per_batch):
        cust = rng.choice(customers)
        items = [{"product_id": p, "quantity": rng.randint(1, 3), "unit_price": u}
                 for p, _n, _c, u in rng.sample(PRODUCTS, k=rng.randint(1, 4))]
        order_ts = base_ts + timedelta(seconds=i * 20)
        order = {"order_id": f"O{start_id + i:06d}",
                 "customer_id": cust["customer_id"],
                 "order_ts": order_ts.isoformat(), "items": items}
        orders.append(order)
        payments.append({
            "payment_id": order["order_id"].replace("O", "PAY"),
            "order_id": order["order_id"],
            "payment_ts": (order_ts + timedelta(minutes=rng.randint(0, 30))).isoformat(),
            "amount": round(sum(it["quantity"] * it["unit_price"] for it in items), 2),
            "method": rng.choice(PAYMENT_METHODS),
            "status_code": rng.choices([1, 2, 3], weights=[85, 10, 5])[0],
        })

    updates = []
    for cust in rng.sample(customers, k=3):                   # profile updates
        changed = dict(cust)
        changed["email"] = f"new.{cust['email']}"
        changed["updated_at"] = (base_ts + timedelta(minutes=rng.randint(1, 25))).isoformat()
        updates.append(changed)

    records = (wrap_as_bus_records(orders, "orders", "order_id", batch_num, base_ts)
               + wrap_as_bus_records(payments, "payments", "payment_id", batch_num, base_ts)
               + wrap_as_bus_records(updates, "customers", "customer_id", batch_num, base_ts))
    rng.shuffle(records)                                      # one mixed drop

    if with_bad_records:                                      # bus-grade dirt
        bad = random.Random(f"{seed}-bus-bad-{batch_num}")
        order_recs = [r for r in records if r["topic"] == "orders"]
        broken = bad.sample(order_recs, k=min(3, len(order_recs)))
        p0 = json.loads(broken[0]["value"])                   # zero quantity
        p0["items"][0]["quantity"] = 0
        broken[0]["value"] = json.dumps(p0)
        p1 = json.loads(broken[1]["value"])                   # null customer
        p1["customer_id"] = None
        broken[1]["value"] = json.dumps(p1)
        broken[2]["value"] = broken[2]["value"][:len(broken[2]["value"]) // 2]

    if redeliver_frac > 0 and batch_num > 1:                  # at-least-once sim
        rd = random.Random(f"{seed}-bus-redeliver-{batch_num}")
        prev = generate_bus_batch(batch_num - 1, seed, orders_per_batch)
        redelivered = rd.sample(prev, k=max(1, int(len(prev) * redeliver_frac)))
        records = records + redelivered
        rd.shuffle(records)
    return records


def generate_cdc_batch(entity, batch_num, seed=DEFAULT_SEED, n_events=20,
                       with_late_events=False, with_reordered_ops=False):
    """Change events for `customers` or `addresses`: INSERT/UPDATE/DELETE.

    Flat payload (AUTO CDC-friendly) + `operation`, `sequence_num`, `event_ts`.
    `sequence_num` follows source commit order; `with_late_events` backdates
    some event_ts (late arrivals), `with_reordered_ops` shuffles file order so
    it no longer matches sequence order.
    """
    if entity not in ("customers", "addresses"):
        raise ValueError(f"CDC feed covers customers/addresses (got '{entity}')")
    customers = generate_customers(200, seed=seed)
    rng = random.Random(f"{seed}-cdc-{entity}-{batch_num}")
    base_ts = V2_BASE + timedelta(hours=batch_num - 1)
    next_new = 200 + (batch_num - 1) * n_events + 1           # fresh customer ids

    events = []
    for i in range(n_events):
        op = rng.choices(["INSERT", "UPDATE", "DELETE"], weights=[30, 60, 10])[0]
        event_ts = base_ts + timedelta(seconds=i * 10)
        if with_late_events and rng.random() < 0.2:
            event_ts -= timedelta(hours=rng.randint(2, 6))    # arrives late
        row = {"operation": op, "sequence_num": batch_num * 1000 + i,
               "event_ts": event_ts.isoformat()}
        if entity == "customers":
            row.update(_cdc_customer_payload(op, rng, customers, next_new))
            next_new += 1 if op == "INSERT" else 0
        else:
            row.update(_cdc_address_payload(op, rng, customers))
        events.append(row)

    if with_reordered_ops:
        rng.shuffle(events)                                   # file ≠ commit order
    return events


def _cdc_customer_payload(op, rng, customers, next_new):
    """Payload columns for one customer change event (full row image)."""
    if op == "INSERT":
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        return {"customer_id": f"C{next_new:05d}", "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}.{next_new}@example.com".lower()}
    target = rng.choice(customers)
    if op == "DELETE":
        return {"customer_id": target["customer_id"], "first_name": None,
                "last_name": None, "email": None}
    changed = {k: target[k] for k in ("customer_id", "first_name", "last_name", "email")}
    field = rng.choice(CDC_UPDATABLE)
    changed[field] = (f"upd.{target['email']}" if field == "email"
                      else rng.choice(LAST_NAMES))
    return changed


def _cdc_address_payload(op, rng, customers):
    """Payload columns for one address change event (full row image)."""
    cust_id = rng.choice(customers)["customer_id"]
    if op == "DELETE":
        return {"customer_id": cust_id, "address_type": "shipping", "street": None,
                "city": None, "state": None, "postcode": None}
    city, state = rng.choice(CITIES)
    return {"customer_id": cust_id,
            "address_type": rng.choice(["shipping", "billing"]),
            "street": f"{rng.randint(1, 999)} {rng.choice(STREETS)}",
            "city": city, "state": state, "postcode": f"{rng.randint(10000, 99999)}"}


def generate_delete_requests(batch_num, seed=DEFAULT_SEED, n=5):
    """PII delete requests: {request_id, customer_id, requested_at}."""
    customers = generate_customers(200, seed=seed)
    rng = random.Random(f"{seed}-deletes-{batch_num}")
    base_ts = V2_BASE + timedelta(days=batch_num - 1, hours=8)
    return [{"request_id": f"DR{batch_num:03d}{i:02d}",
             "customer_id": cust["customer_id"],
             "requested_at": (base_ts + timedelta(minutes=i)).isoformat()}
            for i, cust in enumerate(rng.sample(customers, k=n), 1)]
