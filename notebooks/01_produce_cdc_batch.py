# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · CDC event generator (simulated WMS orders)
# MAGIC
# MAGIC Each run drops **one new JSON file** of change events into the landing volume, like a Debezium/Kafka
# MAGIC sink or a nightly export would. Run it several times to simulate a stream of changes.
# MAGIC
# MAGIC Every event has:
# MAGIC
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `order_id` | business key |
# MAGIC | `op` | `INSERT`, `UPDATE` or `DELETE` (Debezium would use `c`/`u`/`d`) |
# MAGIC | `event_ts` | when the change happened at the source — **this is the sequencing column** |
# MAGIC | `source_lsn` | monotonically increasing log position (another valid sequencing column) |
# MAGIC
# MAGIC To make CDC realistic each batch after the first also contains:
# MAGIC - a **duplicate** event (at-least-once delivery),
# MAGIC - a **late / out-of-order** event older than an update in the same batch,
# MAGIC - **deletes**, and updates to keys that may already be deleted.
# MAGIC
# MAGIC Your pipelines must end up with exactly one current row per `order_id` despite all that.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "tecsys_lab", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing = f"/Volumes/{catalog}/{schema}/raw/orders"

# COMMAND ----------

import json
import random
import datetime as dt

try:
    existing = [f for f in dbutils.fs.ls(landing) if f.name.endswith(".json")]
except Exception:
    raise RuntimeError(f"{landing} not found — run 00_setup first")

batch_no = len(existing)
random.seed(batch_no)

WAREHOUSES = ["MTL-01", "TOR-02", "VAN-03"]
SKUS = [f"SKU-{i:04d}" for i in range(1, 41)]
STATUSES = ["CREATED", "PICKED", "PACKED", "SHIPPED", "CANCELLED"]

base_ts = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(hours=batch_no)
lsn = batch_no * 10_000
events = []


def event(order_id, op, minute, status=None):
    global lsn
    lsn += 1
    return {
        "order_id": order_id,
        "op": op,
        "sku": random.choice(SKUS),
        "warehouse": random.choice(WAREHOUSES),
        "quantity": random.randint(1, 50),
        "status": status or random.choice(STATUSES),
        "event_ts": (base_ts + dt.timedelta(minutes=minute)).isoformat(),
        "source_lsn": lsn,
    }


if batch_no == 0:
    # Initial load: 100 new orders
    events = [event(i, "INSERT", minute=i % 60, status="CREATED") for i in range(1, 101)]
else:
    max_existing = 100 + 20 * (batch_no - 1)
    new_ids = range(max_existing + 1, max_existing + 21)
    events += [event(i, "INSERT", minute=1, status="CREATED") for i in new_ids]

    updated = random.sample(range(1, max_existing + 1), 30)
    events += [event(i, "UPDATE", minute=30) for i in updated]

    deleted = random.sample(range(1, max_existing + 1), 3)
    events += [event(i, "DELETE", minute=45) for i in deleted]

    # Duplicate delivery of an update (identical payload)
    events.append(dict(events[-5]))

    # Late event: an OLDER update for a key that was also updated at minute 30.
    # Correct CDC logic must ignore it (its event_ts is earlier).
    late = event(updated[0], "UPDATE", minute=5, status="LATE-SHOULD-BE-IGNORED")
    events.append(late)

random.shuffle(events)  # files are never ordered nicely in real life

path = f"{landing}/batch_{batch_no:04d}.json"
dbutils.fs.put(path, "\n".join(json.dumps(e) for e in events), overwrite=False)
print(f"Wrote {len(events)} events to {path}")

# COMMAND ----------

display(spark.read.json(path).orderBy("order_id", "event_ts"))
