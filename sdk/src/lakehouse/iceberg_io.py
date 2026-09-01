"""The ("iceberg", "pyiceberg") handler: direct data I/O against a resolved placement.

Other formats/connectors (hudi, vendor catalogs) plug in as sibling classes registered in
lakehouse.dataset.HANDLERS — user code never sees this module.
"""

from datetime import datetime
from typing import Any

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

OP_ID_COLUMN = "_op_id"


class SnapshotExpiredError(LookupError):
    """The requested version is beyond the snapshot-retention horizon."""


class IcebergHandler:
    def __init__(self, doc: dict[str, Any]):
        if doc["catalog"]["kind"] != "sql":
            raise ValueError(
                f"pyiceberg connector: unsupported catalog kind {doc['catalog']['kind']!r}"
            )
        self._doc = doc
        props = {
            "uri": doc["catalog"]["config"]["uri"],
            "warehouse": doc["catalog"]["config"]["warehouse"],
        }
        storage = doc["storage"]
        if storage["kind"] == "s3":
            props |= {
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "s3.endpoint": storage["config"]["endpoint_url"],
                "s3.access-key-id": storage["config"]["access_key"],
                "s3.secret-access-key": storage["config"]["secret_key"],
                "s3.region": "local",
            }
        self._catalog = SqlCatalog(doc["warehouse_name"], **props)
        self._identifier = doc["physical_identifier"]

    def _table(self) -> Table:
        return self._catalog.load_table(self._identifier)

    def append(self, data, op_id: str) -> dict[str, Any]:
        table = self._table()
        arrow = self._stamp(data, op_id)
        table.append(arrow, snapshot_properties={"lakehouse-op-id": op_id})
        return self._write_result(table, arrow)

    def overwrite(self, data, op_id: str) -> dict[str, Any]:
        """Dynamic partition overwrite (replaces exactly the partitions present in `data`);
        full overwrite when the table is unpartitioned."""
        table = self._table()
        arrow = self._stamp(data, op_id)
        props = {"lakehouse-op-id": op_id}
        if table.spec().fields:
            table.dynamic_partition_overwrite(arrow, snapshot_properties=props)
        else:
            table.overwrite(arrow, snapshot_properties=props)
        return self._write_result(table, arrow)

    def scan(self, columns, where, snapshot_id, include_lineage) -> tuple[pa.Table, int | None]:
        table = self._table()
        if snapshot_id is not None and table.snapshot_by_id(snapshot_id) is None:
            raise SnapshotExpiredError(f"snapshot {snapshot_id} expired (beyond retention)")
        if columns is None:
            selected = tuple(f.name for f in table.schema().fields if f.name != OP_ID_COLUMN)
        else:
            selected = tuple(columns)
        if include_lineage and OP_ID_COLUMN not in selected:
            selected += (OP_ID_COLUMN,)
        scan = table.scan(
            selected_fields=selected,
            snapshot_id=snapshot_id,
            **({"row_filter": where} if where else {}),
        )
        if snapshot_id is None:
            current = table.current_snapshot()
            snapshot_id = current.snapshot_id if current else None
        return scan.to_arrow(), snapshot_id

    def snapshot_for_timestamp(self, ts: datetime) -> int:
        snapshot = self._table().snapshot_as_of_timestamp(int(ts.timestamp() * 1000))
        if snapshot is None:
            raise SnapshotExpiredError(
                f"no snapshot at or before {ts.isoformat()} (beyond retention?)"
            )
        return snapshot.snapshot_id

    def _stamp(self, data, op_id: str) -> pa.Table:
        """pandas/arrow -> arrow with the constant _op_id lineage column."""
        if not isinstance(data, pa.Table):
            data = pa.Table.from_pandas(data, preserve_index=False)
        if (
            OP_ID_COLUMN in data.column_names
        ):  # restamp: provenance is per-write, not caller-supplied
            data = data.drop_columns([OP_ID_COLUMN])
        return data.append_column(OP_ID_COLUMN, pa.array([op_id] * len(data), pa.string()))

    def _write_result(self, table: Table, arrow: pa.Table) -> dict[str, Any]:
        current = table.current_snapshot()
        partition_by = self._doc.get("partition_by") or []
        partitions = (
            sorted(
                {
                    tuple(str(v) for v in row)
                    for row in zip(
                        *(arrow.column(c).to_pylist() for c in partition_by), strict=True
                    )
                }
            )
            if partition_by
            else []
        )
        return {
            "snapshot_id": current.snapshot_id if current else None,
            "rows_written": len(arrow),
            "schema_id": table.schema().schema_id,
            "partitions": [dict(zip(partition_by, combo, strict=True)) for combo in partitions],
        }
