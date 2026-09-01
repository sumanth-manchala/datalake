"""Lakehouse control plane: registry, resolution docs, and the lineage ledger.

Data bytes never flow through this API — clients resolve a dataset here, then read/write
storage directly. Server-side catalog work is limited to DDL-ish ops: create, alter,
rollback, drop.
"""

import uuid as uuidlib
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from psycopg2.extras import Json
from pydantic import BaseModel, Field

from lakehouse_api.catalog import (
    FIELD_TYPES,
    OP_ID_COLUMN,
    build_schema,
    load_warehouse_catalog,
    schema_as_json,
)
from lakehouse_api.db import get_db

app = FastAPI(title="lakehouse control plane")

Conn = Annotated[Any, Depends(get_db)]
Actor = Annotated[str, Header(alias="X-Lakehouse-Actor")]


class ProcessCtx(BaseModel):
    kind: str | None = None
    name: str | None = None
    run_id: str | None = None


class FieldSpec(BaseModel):
    name: str
    type: str
    required: bool = False


class CreateDataset(BaseModel):
    namespace: str
    name: str
    fields: list[FieldSpec] = Field(alias="schema")
    partition_by: list[str]  # required; [] = explicitly unpartitioned
    warehouse_name: str | None = None
    platform: str = "local"
    description: str | None = None
    new_version: bool = False
    process: ProcessCtx | None = None


class OpReport(BaseModel):
    op_id: str
    op_kind: str
    warehouse_name: str
    version: int | None = None
    snapshot_id: int | None = None
    process: ProcessCtx | None = None
    details: dict[str, Any] = {}


class AlterRequest(BaseModel):
    add_columns: dict[str, str] = {}
    rename_columns: dict[str, str] = {}
    drop_columns: list[str] = []
    version: int | None = None
    platform: str = "local"
    process: ProcessCtx | None = None


class RollbackRequest(BaseModel):
    to: str  # op_id | ISO timestamp | snapshot id
    version: int | None = None
    platform: str = "local"
    process: ProcessCtx | None = None


def _insert_op(
    conn,
    *,
    entity_id: str,
    op_kind: str,
    actor: str,
    warehouse_name: str | None = None,
    process: ProcessCtx | None = None,
    snapshot_id: int | None = None,
    details: dict[str, Any] | None = None,
    op_id: str | None = None,
) -> str:
    op_id = op_id or str(uuidlib.uuid4())
    process = process or ProcessCtx()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ops (op_id, entity_id, warehouse_name, op_kind, actor,
                                process_kind, process_name, process_run_id, snapshot_id, details)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                op_id,
                entity_id,
                warehouse_name,
                op_kind,
                actor,
                process.kind,
                process.name,
                process.run_id,
                snapshot_id,
                Json(details or {}),
            ),
        )
    return op_id


def _fetch_entity(conn, namespace: str, name: str, version: int | None, *, live_only: bool = True):
    q = """SELECT entity_id::text AS entity_id, entity_type, namespace, name, version,
                  partition_by, description, created_by, dropped_at
           FROM entities WHERE namespace = %s AND name = %s"""
    params: list[Any] = [namespace, name]
    if version is not None:
        q += " AND version = %s"
        params.append(version)
    elif live_only:
        q += " AND dropped_at IS NULL"
    q += " ORDER BY version DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(q, params)
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"dataset {namespace}.{name} not found")
    return row


def _fetch_placement(conn, entity_id: str, platform: str, warehouse_name: str | None = None):
    q = """SELECT p.physical_identifier, p.format, w.*
           FROM entity_placements p JOIN warehouses w USING (warehouse_name)
           WHERE p.entity_id = %s"""
    params: list[Any] = [entity_id]
    if warehouse_name is not None:
        q += " AND w.warehouse_name = %s"
        params.append(warehouse_name)
    else:
        q += " AND w.platform = %s"
        params.append(platform)
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(404, f"no placement for platform {platform!r}")
    if len(rows) > 1:
        raise HTTPException(
            409, f"multiple placements match platform {platform!r}; pass warehouse_name"
        )
    return rows[0]


def _fetch_warehouse(conn, warehouse_name: str | None, platform: str):
    with conn.cursor() as cur:
        if warehouse_name is not None:
            cur.execute("SELECT * FROM warehouses WHERE warehouse_name = %s", (warehouse_name,))
        else:
            cur.execute("SELECT * FROM warehouses WHERE platform = %s", (platform,))
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(404, f"no warehouse for {warehouse_name or platform!r}")
    if len(rows) > 1:
        raise HTTPException(
            409, f"multiple warehouses on platform {platform!r}; pass warehouse_name"
        )
    return rows[0]


def _load_table(conn, namespace: str, name: str, version: int | None, platform: str):
    entity = _fetch_entity(conn, namespace, name, version, live_only=False)
    placement = _fetch_placement(conn, entity["entity_id"], platform)
    catalog = load_warehouse_catalog(placement)
    return entity, placement, catalog.load_table(placement["physical_identifier"])


def _resolve_snapshot(conn, table, as_of: str) -> int:
    """as_of (op_id | int snapshot id | ISO timestamp) -> a snapshot id that still exists."""
    snapshot_id: int | None = None
    try:
        op_key = str(uuidlib.UUID(as_of))
    except ValueError:
        op_key = None
    if op_key is not None:
        with conn.cursor() as cur:
            cur.execute("SELECT snapshot_id FROM ops WHERE op_id = %s", (op_key,))
            row = cur.fetchone()
        if row is None or row["snapshot_id"] is None:
            raise HTTPException(404, f"op {as_of} not found or has no snapshot")
        snapshot_id = row["snapshot_id"]
    else:
        try:
            snapshot_id = int(as_of)
        except ValueError:
            try:
                ts = datetime.fromisoformat(as_of)
            except ValueError:
                raise HTTPException(
                    400, f"as_of must be an op id, snapshot id, or ISO timestamp: {as_of!r}"
                ) from None
            snap = table.snapshot_as_of_timestamp(int(ts.timestamp() * 1000))
            if snap is None:
                raise HTTPException(404, f"no snapshot at or before {as_of}") from None
            snapshot_id = snap.snapshot_id
    if table.snapshot_by_id(snapshot_id) is None:
        raise HTTPException(404, f"snapshot {snapshot_id} expired (beyond retention)")
    return snapshot_id


def _resolution_doc(entity, placement) -> dict[str, Any]:
    return {
        "entity_id": entity["entity_id"],
        "namespace": entity["namespace"],
        "name": entity["name"],
        "version": entity["version"],
        "partition_by": entity["partition_by"],
        "format": placement["format"],
        "connector": placement["connector"],
        "warehouse_name": placement["warehouse_name"],
        "physical_identifier": placement["physical_identifier"],
        "catalog": {"kind": placement["catalog_kind"], "config": placement["catalog_config"]},
        "storage": {"kind": placement["storage_kind"], "config": placement["storage_config"]},
    }


@app.post("/v1/datasets", status_code=201)
def create_dataset(body: CreateDataset, conn: Conn, actor: Actor = "unknown"):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(version) AS v FROM entities WHERE namespace = %s AND name = %s",
            (body.namespace, body.name),
        )
        latest = cur.fetchone()["v"]
    if latest is not None and not body.new_version:
        raise HTTPException(
            409,
            f"{body.namespace}.{body.name} exists; "
            "pass new_version=true for a backwards-incompatible schema",
        )
    version = (latest or 0) + 1

    warehouse = _fetch_warehouse(conn, body.warehouse_name, body.platform)
    try:
        schema, spec = build_schema([f.model_dump() for f in body.fields], body.partition_by)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    physical = f"{body.namespace}.{body.name}" + (f"_v{version}" if version > 1 else "")
    catalog = load_warehouse_catalog(warehouse)
    catalog.create_namespace_if_not_exists(body.namespace)
    catalog.create_table(physical, schema=schema, partition_spec=spec)

    entity_id = str(uuidlib.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO entities (entity_id, entity_type, namespace, name, version,
                                     partition_by, description, created_by)
               VALUES (%s, 'dataset', %s, %s, %s, %s, %s, %s)""",
            (
                entity_id,
                body.namespace,
                body.name,
                version,
                Json(body.partition_by),
                body.description,
                actor,
            ),
        )
        cur.execute(
            """INSERT INTO entity_placements
                   (entity_id, warehouse_name, physical_identifier, format)
               VALUES (%s, %s, %s, 'iceberg')""",
            (entity_id, warehouse["warehouse_name"], physical),
        )
    op_id = _insert_op(
        conn,
        entity_id=entity_id,
        op_kind="create",
        actor=actor,
        warehouse_name=warehouse["warehouse_name"],
        process=body.process,
        details={
            "schema": [f.model_dump() for f in body.fields],
            "partition_by": body.partition_by,
        },
    )
    return {
        "entity_id": entity_id,
        "version": version,
        "op_id": op_id,
        "warehouse_name": warehouse["warehouse_name"],
        "physical_identifier": physical,
    }


@app.get("/v1/datasets")
def list_datasets(conn: Conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT e.namespace, e.name, e.version, e.partition_by, e.description,
                      e.created_by, e.created_at, e.dropped_at,
                      array_agg(p.warehouse_name) AS warehouses
               FROM entities e LEFT JOIN entity_placements p USING (entity_id)
               WHERE e.entity_type = 'dataset'
               GROUP BY e.entity_id ORDER BY e.namespace, e.name, e.version""",
        )
        return cur.fetchall()


@app.get("/v1/datasets/{namespace}/{name}")
def resolve_dataset(
    namespace: str,
    name: str,
    conn: Conn,
    platform: str = "local",
    version: int | None = None,
    warehouse_name: str | None = None,
):
    entity = _fetch_entity(conn, namespace, name, version)
    placement = _fetch_placement(conn, entity["entity_id"], platform, warehouse_name)
    return _resolution_doc(entity, placement)


@app.post("/v1/datasets/{namespace}/{name}/ops", status_code=201)
def report_op(namespace: str, name: str, body: OpReport, conn: Conn, actor: Actor = "unknown"):
    entity = _fetch_entity(conn, namespace, name, body.version, live_only=False)
    op_id = _insert_op(
        conn,
        op_id=body.op_id,
        entity_id=entity["entity_id"],
        op_kind=body.op_kind,
        actor=actor,
        warehouse_name=body.warehouse_name,
        process=body.process,
        snapshot_id=body.snapshot_id,
        details=body.details,
    )
    return {"op_id": op_id}


@app.get("/v1/ops/{op_id}")
def get_op(op_id: str, conn: Conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT op_id::text AS op_id, entity_id::text AS entity_id, warehouse_name,
                      op_kind, actor, process_kind, process_name, process_run_id,
                      snapshot_id, details, occurred_at
               FROM ops WHERE op_id = %s""",
            (op_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"op {op_id} not found")
    return row


@app.get("/v1/datasets/{namespace}/{name}/history")
def history(
    namespace: str,
    name: str,
    conn: Conn,
    kind: str = Query("all", pattern="^(all|writes|reads)$"),
    version: int | None = None,
):
    entity = _fetch_entity(conn, namespace, name, version, live_only=False)
    q = """SELECT op_id::text AS op_id, warehouse_name, op_kind, actor,
                  process_kind, process_name, process_run_id, snapshot_id, details, occurred_at
           FROM ops WHERE entity_id = %s"""
    if kind == "reads":
        q += " AND op_kind = 'read'"
    elif kind == "writes":
        q += " AND op_kind != 'read'"
    q += " ORDER BY occurred_at DESC"
    with conn.cursor() as cur:
        cur.execute(q, (entity["entity_id"],))
        return cur.fetchall()


@app.post("/v1/datasets/{namespace}/{name}/alter")
def alter_dataset(
    namespace: str, name: str, body: AlterRequest, conn: Conn, actor: Actor = "unknown"
):
    protected = {OP_ID_COLUMN} & (
        set(body.add_columns) | set(body.rename_columns) | set(body.drop_columns)
    )
    if protected:
        raise HTTPException(400, f"{OP_ID_COLUMN!r} is reserved")
    entity, placement, table = _load_table(conn, namespace, name, body.version, body.platform)
    bad = set(body.add_columns.values()) - set(FIELD_TYPES)
    if bad:
        raise HTTPException(400, f"unsupported field types: {sorted(bad)}")
    with table.update_schema() as update:
        for col, type_name in body.add_columns.items():
            update.add_column(col, FIELD_TYPES[type_name])
        for old, new in body.rename_columns.items():
            update.rename_column(old, new)
        for col in body.drop_columns:
            update.delete_column(col)
    op_id = _insert_op(
        conn,
        entity_id=entity["entity_id"],
        op_kind="alter",
        actor=actor,
        warehouse_name=placement["warehouse_name"],
        process=body.process,
        details={
            "add_columns": body.add_columns,
            "rename_columns": body.rename_columns,
            "drop_columns": body.drop_columns,
            "schema_id": table.schema().schema_id,
        },
    )
    return {"op_id": op_id, "schema": schema_as_json(table.schema())}


@app.post("/v1/datasets/{namespace}/{name}/rollback")
def rollback_dataset(
    namespace: str, name: str, body: RollbackRequest, conn: Conn, actor: Actor = "unknown"
):
    entity, placement, table = _load_table(conn, namespace, name, body.version, body.platform)
    snapshot_id = _resolve_snapshot(conn, table, body.to)
    table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()
    op_id = _insert_op(
        conn,
        entity_id=entity["entity_id"],
        op_kind="rollback",
        actor=actor,
        warehouse_name=placement["warehouse_name"],
        process=body.process,
        snapshot_id=snapshot_id,
        details={"to": body.to},
    )
    return {"op_id": op_id, "snapshot_id": snapshot_id}


@app.get("/v1/datasets/{namespace}/{name}/schema")
def dataset_schema(
    namespace: str,
    name: str,
    conn: Conn,
    as_of: str | None = None,
    platform: str = "local",
    version: int | None = None,
):
    _, _, table = _load_table(conn, namespace, name, version, platform)
    if as_of is None:
        schema = table.schema()
    else:
        snapshot_id = _resolve_snapshot(conn, table, as_of)
        snapshot = table.snapshot_by_id(snapshot_id)
        schema = table.schemas()[snapshot.schema_id]
    return {"schema_id": schema.schema_id, "fields": schema_as_json(schema)}


@app.get("/v1/datasets/{namespace}/{name}/partitions")
def dataset_partitions(
    namespace: str,
    name: str,
    conn: Conn,
    as_of: str | None = None,
    platform: str = "local",
    version: int | None = None,
):
    entity, _, table = _load_table(conn, namespace, name, version, platform)
    kwargs = {}
    if as_of is not None:
        kwargs["snapshot_id"] = _resolve_snapshot(conn, table, as_of)
    rows = table.inspect.partitions(**kwargs).to_pylist()
    return {
        "partition_by": entity["partition_by"],
        "partitions": [
            {
                "partition": row.get("partition", {}),
                "record_count": row["record_count"],
                "file_count": row["file_count"],
                "spec_id": row.get("spec_id"),
            }
            for row in rows
        ],
    }


@app.get("/v1/warehouses")
def list_warehouses(conn: Conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM warehouses ORDER BY warehouse_name")
        return cur.fetchall()


@app.delete("/v1/datasets/{namespace}/{name}")
def drop_dataset(
    namespace: str, name: str, conn: Conn, version: int | None = None, actor: Actor = "unknown"
):
    entity = _fetch_entity(conn, namespace, name, version)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT p.physical_identifier, w.* FROM entity_placements p
               JOIN warehouses w USING (warehouse_name) WHERE p.entity_id = %s""",
            (entity["entity_id"],),
        )
        placements = cur.fetchall()
    for placement in placements:
        load_warehouse_catalog(placement).drop_table(placement["physical_identifier"])
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE entities SET dropped_at = now() WHERE entity_id = %s", (entity["entity_id"],)
        )
    op_id = _insert_op(
        conn,
        entity_id=entity["entity_id"],
        op_kind="drop",
        actor=actor,
        details={"warehouses": [p["warehouse_name"] for p in placements]},
    )
    return {"op_id": op_id, "version": entity["version"]}
