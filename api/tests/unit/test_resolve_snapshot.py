from unittest import mock

import pytest
from fastapi import HTTPException
from lakehouse_api.app import _resolve_snapshot
from pyiceberg.table import Table

OP_ID = "0b7c8a72-9f1e-4a3b-8d21-93f6a2f4c111"


def make_conn(row):
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = row
    return conn


@pytest.fixture
def table():
    return mock.create_autospec(Table, instance=True)


def test_op_id_resolves_via_ledger(table):
    table.snapshot_by_id.return_value = mock.Mock()
    assert _resolve_snapshot(make_conn({"snapshot_id": 7}), table, OP_ID) == 7


@pytest.mark.parametrize("row", [None, {"snapshot_id": None}], ids=["missing", "no-snapshot"])
def test_unknown_or_snapshotless_op_404(table, row):
    with pytest.raises(HTTPException) as exc:
        _resolve_snapshot(make_conn(row), table, OP_ID)
    assert exc.value.status_code == 404


def test_raw_snapshot_id_passthrough(table):
    table.snapshot_by_id.return_value = mock.Mock()
    assert _resolve_snapshot(make_conn(None), table, "42") == 42


def test_expired_snapshot_404(table):
    table.snapshot_by_id.return_value = None
    with pytest.raises(HTTPException) as exc:
        _resolve_snapshot(make_conn(None), table, "42")
    assert exc.value.status_code == 404
    assert "beyond retention" in exc.value.detail


def test_timestamp_resolves_via_snapshot_log(table):
    table.snapshot_as_of_timestamp.return_value = mock.Mock(snapshot_id=9)
    table.snapshot_by_id.return_value = mock.Mock()
    assert _resolve_snapshot(make_conn(None), table, "2026-01-01T00:00:00+00:00") == 9
    (ms,) = table.snapshot_as_of_timestamp.call_args.args
    assert ms == 1767225600000


def test_timestamp_before_history_404(table):
    table.snapshot_as_of_timestamp.return_value = None
    with pytest.raises(HTTPException) as exc:
        _resolve_snapshot(make_conn(None), table, "1990-01-01T00:00:00+00:00")
    assert exc.value.status_code == 404


def test_garbage_as_of_400(table):
    with pytest.raises(HTTPException) as exc:
        _resolve_snapshot(make_conn(None), table, "not-a-thing")
    assert exc.value.status_code == 400
