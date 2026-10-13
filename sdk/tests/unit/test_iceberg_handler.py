"""Handler tests against a local sqlite catalog + file warehouse — no services."""

from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pytest
from lakehouse.iceberg_io import OP_ID_COLUMN, IcebergHandler, SnapshotExpiredError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


def make_doc(tmp_path):
    return {
        "namespace": "ns",
        "name": "t",
        "version": 1,
        "partition_by": ["day"],
        "format": "iceberg",
        "connector": "pyiceberg",
        "warehouse_name": "test-wh",
        "physical_identifier": "ns.t",
        "catalog": {
            "kind": "sql",
            "config": {
                "uri": f"sqlite:///{tmp_path}/catalog.db",
                "warehouse": f"file://{tmp_path}/wh",
            },
        },
        "storage": {"kind": "file", "config": {}},
    }


def make_handler(tmp_path):
    handler = IcebergHandler(make_doc(tmp_path))
    fields = [
        NestedField(1, "day", StringType()),
        NestedField(2, "amount", LongType()),
        NestedField(3, OP_ID_COLUMN, StringType()),
    ]
    schema = Schema(*fields)
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "day"))
    handler._catalog.create_namespace_if_not_exists("ns")
    handler._catalog.create_table("ns.t", schema=schema, partition_spec=spec)
    return handler


@pytest.fixture
def handler(tmp_path):
    return make_handler(tmp_path)


def test_overwrite_stamps_op_id_and_snapshot_property(handler):
    result = handler.overwrite(pa.table({"day": ["a", "b"], "amount": [1, 2]}), "op-1")

    assert result["rows_written"] == 2
    assert result["partitions"] == [{"day": "a"}, {"day": "b"}]
    snapshot = handler._table().current_snapshot()
    assert snapshot.snapshot_id == result["snapshot_id"]
    assert snapshot.summary["lakehouse-op-id"] == "op-1"

    arrow, _ = handler.scan(None, None, None, include_lineage=True)
    assert set(arrow.column(OP_ID_COLUMN).to_pylist()) == {"op-1"}


def test_default_scan_hides_lineage_column(handler):
    handler.overwrite(pa.table({"day": ["a"], "amount": [1]}), "op-1")
    arrow, _ = handler.scan(None, None, None, include_lineage=False)
    assert OP_ID_COLUMN not in arrow.column_names
    assert arrow.column_names == ["day", "amount"]


def test_pandas_input_and_caller_op_id_restamped(handler):
    df = pd.DataFrame({"day": ["a"], "amount": [1], OP_ID_COLUMN: ["forged"]})
    handler.overwrite(df, "op-real")
    arrow, _ = handler.scan(None, None, None, include_lineage=True)
    assert arrow.column(OP_ID_COLUMN).to_pylist() == ["op-real"]


def test_overwrite_replaces_only_touched_partitions(handler):
    handler.overwrite(pa.table({"day": ["a", "b"], "amount": [1, 2]}), "op-1")
    result = handler.overwrite(pa.table({"day": ["a"], "amount": [10]}), "op-2")

    assert result["partitions"] == [{"day": "a"}]
    arrow, _ = handler.scan(None, None, None, False)
    rows = {(r["day"], r["amount"]) for r in arrow.to_pylist()}
    assert rows == {("a", 10), ("b", 2)}


def test_overwrite_rejects_empty_input(handler):
    handler.overwrite(pa.table({"day": ["a"], "amount": [1]}), "op-1")
    empty = pa.table({"day": pa.array([], pa.string()), "amount": pa.array([], pa.int64())})
    with pytest.raises(ValueError, match="at least one row"):
        handler.overwrite(empty, "op-2")


def test_scan_filter_and_projection(handler):
    handler.overwrite(pa.table({"day": ["a", "b"], "amount": [1, 2]}), "op-1")
    arrow, _ = handler.scan(["amount"], "day = 'b'", None, False)
    assert arrow.column_names == ["amount"]
    assert arrow.column("amount").to_pylist() == [2]


def test_time_travel_by_snapshot_id(handler):
    first = handler.overwrite(pa.table({"day": ["a"], "amount": [1]}), "op-1")
    handler.overwrite(pa.table({"day": ["a"], "amount": [10]}), "op-2")

    arrow, read_snapshot = handler.scan(None, None, first["snapshot_id"], False)
    assert arrow.column("amount").to_pylist() == [1]
    assert read_snapshot == first["snapshot_id"]


def test_scan_missing_snapshot_raises_expired(handler):
    handler.overwrite(pa.table({"day": ["a"], "amount": [1]}), "op-1")
    with pytest.raises(SnapshotExpiredError, match="beyond retention"):
        handler.scan(None, None, 999999, False)


def test_snapshot_for_timestamp(handler):
    first = handler.overwrite(pa.table({"day": ["a"], "amount": [1]}), "op-1")
    second = handler.overwrite(pa.table({"day": ["a"], "amount": [2]}), "op-2")

    first_snapshot = handler._table().snapshot_by_id(first["snapshot_id"])
    at_first = datetime.fromtimestamp(first_snapshot.timestamp_ms / 1000, tz=UTC)
    assert handler.snapshot_for_timestamp(at_first) == first["snapshot_id"]

    latest = handler._table().snapshot_by_id(second["snapshot_id"])
    after = datetime.fromtimestamp(latest.timestamp_ms / 1000 + 1, tz=UTC)
    assert handler.snapshot_for_timestamp(after) == second["snapshot_id"]

    before_everything = datetime.fromtimestamp(first_snapshot.timestamp_ms / 1000 - 10, tz=UTC)
    with pytest.raises(SnapshotExpiredError):
        handler.snapshot_for_timestamp(before_everything)


def test_unsupported_catalog_kind_rejected(tmp_path):
    doc = make_doc(tmp_path)
    doc["catalog"]["kind"] = "rest"
    with pytest.raises(ValueError, match="unsupported catalog kind"):
        IcebergHandler(doc)
