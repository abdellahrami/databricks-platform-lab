import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lab_utils.transforms import cast_order_events, latest_per_key  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("lab-tests").getOrCreate()
    yield session
    session.stop()


def _events(spark, rows):
    cols = ["order_id", "op", "sku", "warehouse", "quantity", "status", "event_ts", "source_lsn"]
    return spark.createDataFrame(rows, cols)


def test_latest_per_key_keeps_newest_and_ignores_late(spark):
    df = cast_order_events(
        _events(
            spark,
            [
                (1, "update", "SKU-1", "MTL-01", 5, "PACKED", "2026-01-01T10:30:00+00:00", 3),
                (1, "update", "SKU-1", "MTL-01", 5, "LATE", "2026-01-01T10:05:00+00:00", 9),
                (2, "insert", "SKU-2", "TOR-02", 1, "CREATED", "2026-01-01T10:00:00+00:00", 1),
                (2, "insert", "SKU-2", "TOR-02", 1, "CREATED", "2026-01-01T10:00:00+00:00", 1),
            ],
        )
    )
    out = {r.order_id: r for r in latest_per_key(df, ["order_id"], "event_ts", "source_lsn").collect()}

    assert len(out) == 2
    assert out[1].status == "PACKED"  # newer event_ts wins even though the late one has a higher lsn
    assert out[2].op == "INSERT"  # op normalised to upper case


def test_cast_order_events_types(spark):
    df = cast_order_events(_events(spark, [("7", "delete", "S", "W", "3", "X", "2026-01-01T00:00:00Z", "11")]))
    types = dict(df.dtypes)
    assert types["order_id"] == "bigint"
    assert types["quantity"] == "int"
    assert types["event_ts"] == "timestamp"
