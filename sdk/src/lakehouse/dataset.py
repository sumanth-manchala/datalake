"""Format-agnostic Dataset facade; engine work is delegated to a handler.

HANDLERS maps (format, connector) from the resolution doc to an I/O implementation.
Adding Hudi or a non-pyiceberg warehouse later = one new handler class + one entry here;
user code never changes.
"""

import uuid as uuidlib
from datetime import datetime
from typing import Any

import duckdb
import pyarrow as pa

from lakehouse.iceberg_io import IcebergHandler

HANDLERS = {("iceberg", "pyiceberg"): IcebergHandler}


class Dataset:
    def __init__(self, client, doc: dict[str, Any]):
        key = (doc["format"], doc["connector"])
        if key not in HANDLERS:
            raise ValueError(f"no handler for format/connector {key}; known: {sorted(HANDLERS)}")
        self._client = client
        self.doc = doc
        self._handler = HANDLERS[key](doc)

    @property
    def qualified_name(self) -> str:
        return f"{self.doc['namespace']}.{self.doc['name']}"

    @property
    def version(self) -> int:
        return self.doc["version"]

    # ── writes ──────────────────────────────────────────────────────────────
    def overwrite(self, data) -> str:
        """Dynamic partition overwrite: replaces exactly the partitions present in `data`."""
        op_id = str(uuidlib.uuid4())
        result = self._handler.overwrite(data, op_id)
        self._client.report_op(
            self.doc,
            op_id=op_id,
            op_kind="overwrite",
            snapshot_id=result["snapshot_id"],
            details={k: result[k] for k in ("rows_written", "schema_id", "partitions")},
        )
        return op_id

    # ── reads ───────────────────────────────────────────────────────────────
    def to_arrow(self, columns=None, where=None, as_of=None, include_lineage=False) -> pa.Table:
        snapshot_id = self._resolve_as_of(as_of)
        arrow, read_snapshot = self._handler.scan(columns, where, snapshot_id, include_lineage)
        self._client.report_op(
            self.doc,
            op_id=str(uuidlib.uuid4()),
            op_kind="read",
            snapshot_id=read_snapshot,
            details={"columns": list(columns) if columns else None, "where": where},
        )
        return arrow

    def to_pandas(self, **kwargs):
        return self.to_arrow(**kwargs).to_pandas()

    def to_duckdb(self, name: str, conn=None, **kwargs) -> duckdb.DuckDBPyConnection:
        conn = conn or duckdb.connect()
        conn.register(name, self.to_arrow(**kwargs))
        return conn

    def _resolve_as_of(self, as_of) -> int | None:
        """op_id | datetime/ISO string | snapshot id -> pinned snapshot id (None = current)."""
        if as_of is None:
            return None
        if isinstance(as_of, int):
            return as_of
        if isinstance(as_of, datetime):
            return self._handler.snapshot_for_timestamp(as_of)
        op_key = None
        try:
            op_key = str(uuidlib.UUID(as_of))
        except ValueError:
            pass
        if op_key is not None:
            op = self._client.get_op(op_key)
            if op["snapshot_id"] is None:
                raise ValueError(f"op {as_of} has no snapshot (op_kind={op['op_kind']!r})")
            return op["snapshot_id"]
        if as_of.isdigit():
            return int(as_of)
        return self._handler.snapshot_for_timestamp(datetime.fromisoformat(as_of))

    # ── metadata / control-plane ────────────────────────────────────────────
    def alter(self, add_columns: dict[str, str]) -> dict[str, Any]:
        """Add optional columns — the only backwards-compatible change. Renames, drops and
        type changes need a new version: lake.create_dataset(..., new_version=True)."""
        return self._client.alter_dataset(self.doc, add_columns=add_columns)

    def rollback(self, to) -> dict[str, Any]:
        return self._client.rollback_dataset(self.doc, to=str(to))

    def history(self, kind: str = "all") -> list[dict[str, Any]]:
        return self._client.dataset_history(self.doc, kind=kind)

    def schema(self, as_of=None) -> dict[str, Any]:
        return self._client.dataset_schema(self.doc, as_of=as_of)

    def partitions(self, as_of=None) -> dict[str, Any]:
        return self._client.dataset_partitions(self.doc, as_of=as_of)
