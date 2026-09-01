from datetime import UTC, datetime
from unittest import mock

import pyarrow as pa
import pytest
from lakehouse.client import Lakehouse
from lakehouse.dataset import HANDLERS, Dataset
from lakehouse.iceberg_io import IcebergHandler

DOC = {
    "namespace": "ns",
    "name": "t",
    "version": 1,
    "partition_by": [],
    "format": "iceberg",
    "connector": "pyiceberg",
    "warehouse_name": "wh",
    "physical_identifier": "ns.t",
    "catalog": {"kind": "sql", "config": {}},
    "storage": {"kind": "file", "config": {}},
}


@pytest.fixture
def handler():
    return mock.create_autospec(IcebergHandler, instance=True)


@pytest.fixture
def client():
    return mock.create_autospec(Lakehouse, instance=True)


@pytest.fixture
def dataset(handler, client):
    with mock.patch.dict(HANDLERS, {("iceberg", "pyiceberg"): lambda doc: handler}):
        return Dataset(client, DOC)


def test_unknown_format_rejected(client):
    doc = DOC | {"format": "hudi"}
    with pytest.raises(ValueError, match="no handler"):
        Dataset(client, doc)


def test_write_generates_op_id_and_reports(dataset, handler, client):
    handler.append.return_value = {
        "snapshot_id": 42,
        "rows_written": 2,
        "schema_id": 0,
        "partitions": [],
    }
    data = pa.table({"x": [1, 2]})

    op_id = dataset.append(data)

    handler.append.assert_called_once_with(data, op_id)
    client.report_op.assert_called_once_with(
        DOC,
        op_id=op_id,
        op_kind="append",
        snapshot_id=42,
        details={"rows_written": 2, "schema_id": 0, "partitions": []},
    )


def test_read_reports_resolved_snapshot(dataset, handler, client):
    arrow = pa.table({"x": [1]})
    handler.scan.return_value = (arrow, 42)

    result = dataset.to_arrow(columns=["x"], where="x > 0")

    assert result is arrow
    handler.scan.assert_called_once_with(["x"], "x > 0", None, False)
    kwargs = client.report_op.call_args.kwargs
    assert kwargs["op_kind"] == "read"
    assert kwargs["snapshot_id"] == 42
    assert kwargs["details"] == {"columns": ["x"], "where": "x > 0"}


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [(None, None), (42, 42), ("42", 42)],
)
def test_resolve_as_of_passthrough(dataset, as_of, expected):
    assert dataset._resolve_as_of(as_of) == expected


def test_resolve_as_of_op_id_via_ledger(dataset, client):
    client.get_op.return_value = {"snapshot_id": 7, "op_kind": "append"}
    op_id = "0b7c8a72-9f1e-4a3b-8d21-93f6a2f4c111"
    assert dataset._resolve_as_of(op_id) == 7
    client.get_op.assert_called_once_with(op_id)


def test_resolve_as_of_op_without_snapshot_rejected(dataset, client):
    client.get_op.return_value = {"snapshot_id": None, "op_kind": "create"}
    with pytest.raises(ValueError, match="has no snapshot"):
        dataset._resolve_as_of("0b7c8a72-9f1e-4a3b-8d21-93f6a2f4c111")


@pytest.mark.parametrize(
    "as_of",
    [datetime(2026, 1, 1, tzinfo=UTC), "2026-01-01T00:00:00+00:00"],
    ids=["datetime", "iso-string"],
)
def test_resolve_as_of_timestamp_uses_handler(dataset, handler, as_of):
    handler.snapshot_for_timestamp.return_value = 99
    assert dataset._resolve_as_of(as_of) == 99
    (passed,) = handler.snapshot_for_timestamp.call_args.args
    assert passed == datetime(2026, 1, 1, tzinfo=UTC)
