"""Lakeshore live-feed simulator — v2, companion to datagen.py.

Drops small files into the landing volume so streaming lessons process
GENUINELY new data. v2 keeps the v1 API shape and adds three feed kinds:

    kind="orders"   → v1 behavior, untouched (plain order files, `orders/`)
    kind="bus"      → multiplex Kafka-style records        → `topics/`
    kind="cdc"      → change events (customers/addresses)  → `cdc/<entity>/`
    kind="deletes"  → PII delete requests                  → `deletes/`

Design rules (v1 rules still bind):
- Reuses datagen.py generators — no logic duplicated.
- Deterministic per (seed, kind, batch): re-emitting a batch rewrites an
  IDENTICAL file, so Auto Loader (which tracks files by path) ignores reruns.
- Event timestamps increase monotonically across batches; v2 feeds start
  after every v1 timestamp and never write into v1 entity folders.
"""

import random
import time
from datetime import datetime, timedelta

import datagen

# ------------------------------------------------------------------ v1 (orders)

FEED_BASE = datetime(2026, 7, 1, 9, 0, 0)
FIRST_FEED_ORDER = 1001          # batch_001 used order ids O000001–O001000


def _order_batch(batch_num, batch_size, seed, extra_fields=None):
    """Deterministic batch of orders: same (seed, batch_num) => same rows."""
    customers = datagen.generate_customers(200, seed=seed)   # same pool as batch_001
    rng = random.Random(f"{seed}-orders-feed-{batch_num}")
    base_ts = FEED_BASE + timedelta(minutes=30 * (batch_num - 1))
    start_id = FIRST_FEED_ORDER + (batch_num - 1) * batch_size

    rows = []
    for i in range(batch_size):
        cust = rng.choice(customers)
        items = []
        for prod_id, name, category, price in rng.sample(datagen.PRODUCTS,
                                                         k=rng.randint(1, 3)):
            items.append({"product_id": prod_id,
                          "quantity": rng.randint(1, 3),
                          "unit_price": price})
        row = {
            "order_id": f"O{start_id + i:06d}",
            "customer_id": cust["customer_id"],
            "order_ts": (base_ts + timedelta(seconds=i * 20)).isoformat(),
            "items": items,
        }
        if extra_fields:
            row.update(extra_fields)      # e.g. {"sales_channel": "mobile_app"}
        rows.append(row)
    return rows

# ------------------------------------------------------------------- v2 kinds


def emit_batch(kind, volume_path, batch_num, batch_size=20,
               seed=datagen.DEFAULT_SEED, entity=None, extra_fields=None,
               **flags):
    """Generate and write ONE feed file. Returns the file path.

    `kind`: "orders" (v1), "bus", "cdc" (needs entity=), or "deletes".
    `flags`: with_late_events=/with_reordered_ops= (cdc), redeliver_frac= (bus).
    """
    if kind == "orders":
        rows = _order_batch(batch_num, batch_size, seed, extra_fields)
        path = f"{volume_path}/orders_feed_{batch_num:04d}.json"
    elif kind == "bus":
        rows = datagen.generate_bus_batch(batch_num, seed=seed,
                                          orders_per_batch=batch_size, **flags)
        path = f"{volume_path}/bus_batch_{batch_num:04d}.json"
    elif kind == "cdc":
        if entity is None:
            raise ValueError("cdc feed needs entity= ('customers'/'addresses')")
        rows = datagen.generate_cdc_batch(entity, batch_num, seed=seed,
                                          n_events=batch_size, **flags)
        path = f"{volume_path}/cdc_{entity}_batch_{batch_num:04d}.json"
    elif kind == "deletes":
        rows = datagen.generate_delete_requests(batch_num, seed=seed,
                                                n=batch_size)
        path = f"{volume_path}/delete_requests_{batch_num:04d}.json"
    else:
        raise ValueError(f"unknown feed kind '{kind}'")
    datagen.write_json(rows, path)
    return path


def run_feed(kind, volume_path, batches=5, batch_size=20,
             interval_seconds=10, seed=datagen.DEFAULT_SEED, start_batch=1,
             entity=None, **flags):
    """Emit `batches` files, one every `interval_seconds`.

    Run this in a SECOND notebook while your streaming query runs in the
    first. Re-running with the same parameters rewrites identical files, so
    already-processed batches are not ingested twice.
    """
    last = start_batch + batches - 1
    for b in range(start_batch, last + 1):
        path = emit_batch(kind, volume_path, b, batch_size, seed,
                          entity=entity, **flags)
        print(f"[feed] {kind} batch {b:>3} → {path}")
        if b < last:
            time.sleep(interval_seconds)
    print(f"[feed] done: {batches} {kind} batches of {batch_size}")
