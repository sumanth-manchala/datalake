"""Lakehouse control plane: registry, resolution docs, and the lineage ledger.

Data bytes never flow through this API — clients resolve a dataset here, then read/write
storage directly. Server-side catalog work is limited to DDL-ish ops: create, alter,
rollback, drop.
"""

import uuid as uuidlib
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from psycopg2.extras import Json
from pydantic import BaseModel, ConfigDict, Field, RootModel, StringConstraints, model_validator

from lakehouse_api.catalog import (
    OP_ID_COLUMN,
    FieldType,
    build_schema,
    iceberg_type,
    load_warehouse_catalog,
    new_id_counter,
    schema_as_json,
)
from lakehouse_api.db import get_db

app = FastAPI(title="lakehouse control plane", separate_input_output_schemas=False)

Conn = Annotated[Any, Depends(get_db)]
Actor = Annotated[str, Header(alias="X-Lakehouse-Actor")]


class ProcessCtx(BaseModel):
    kind: str | None = None
    name: str | None = None
    run_id: str | None = None


# Schema JSON = Iceberg's type serialization without ids (ids in the input are ignored).
DecimalName = Annotated[str, StringConstraints(pattern=r"^decimal\(\d+,\s*\d+\)$")]


class ListSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)
    type: Literal["list"]
    element: "TypeSpec"
    element_required: bool = Field(False, alias="element-required")


class MapSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)
    type: Literal["map"]
    key: "TypeSpec"
    value: "TypeSpec"
    value_required: bool = Field(False, alias="value-required")


class StructSpec(BaseModel):
    type: Literal["struct"]
    fields: list["FieldSpec"] = Field(min_length=1)


class PrimitiveName(RootModel[FieldType]):
    """Named so OpenAPI codegen keeps the enum (unioned with DecimalName it collapses to str)."""


TypeSpec = (
    PrimitiveName
    | DecimalName
    | Annotated[ListSpec | MapSpec | StructSpec, Field(discriminator="type")]
)


class FieldSpec(BaseModel):
    name: str
    type: TypeSpec
    required: bool = False


ListSpec.model_rebuild()
MapSpec.model_rebuild()
StructSpec.model_rebuild()


class CreateDataset(BaseModel):
    namespace: str
    name: str
    fields: list[FieldSpec] = Field(alias="schema")
    partition_by: list[str] = Field(min_length=1)  # every write is a dynamic partition overwrite
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
    """Additive-only schema evolution: new columns are always optional, so existing writers
    (which omit them) and readers (which read null for old rows) keep working."""

    model_config = ConfigDict(extra="forbid")

    add_columns: dict[str, TypeSpec] = Field(min_length=1)
    version: int | None = None
    platform: str = "local"
    process: ProcessCtx | None = None

    @model_validator(mode="before")
    @classmethod
    def _additive_only(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.keys() & {"rename_columns", "drop_columns"}:
            raise ValueError(
                "only adding optional columns is backwards compatible; "
                "renames, drops and type changes need a new dataset version"
            )
        return data


# ── response models (typed so the UI's OpenAPI codegen gets real types) ──────


class CreatedDataset(BaseModel):
    entity_id: str
    version: int
    op_id: str
    warehouse_name: str
    physical_identifier: str


class DatasetSummary(BaseModel):
    namespace: str
    name: str
    version: int
    partition_by: list[str]
    description: str | None
    created_by: str
    created_at: datetime
    dropped_at: datetime | None
    warehouses: list[str]


class OpRow(BaseModel):
    op_id: str
    warehouse_name: str | None
    op_kind: str
    actor: str
    process_kind: str | None
    process_name: str | None
    process_run_id: str | None
    snapshot_id: int | None
    details: dict[str, Any]
    occurred_at: datetime


class OpDetail(OpRow):
    entity_id: str


class SchemaOut(BaseModel):
    schema_id: int
    fields: list[FieldSpec]


class PartitionOut(BaseModel):
    partition: dict[str, Any]
    record_count: int
    file_count: int
    spec_id: int | None


class PartitionsOut(BaseModel):
    partition_by: list[str]
    partitions: list[PartitionOut]


class WarehouseOut(BaseModel):
    # no catalog/storage config: those carry credentials
    warehouse_name: str
    platform: str
    catalog_kind: str
    storage_kind: str


class AlterResult(BaseModel):
    op_id: str
    schema_id: int
    fields: list[FieldSpec]


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


@app.post("/v1/datasets", status_code=201, response_model=CreatedDataset)
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


@app.get("/v1/datasets", response_model=list[DatasetSummary])
def list_datasets(conn: Conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT e.namespace, e.name, e.version, e.partition_by, e.description,
                      e.created_by, e.created_at, e.dropped_at,
                      array_remove(array_agg(p.warehouse_name), NULL) AS warehouses
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


@app.get("/v1/ops/{op_id}", response_model=OpDetail)
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


@app.get("/v1/datasets/{namespace}/{name}/history", response_model=list[OpRow])
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


@app.post("/v1/datasets/{namespace}/{name}/alter", response_model=AlterResult)
def alter_dataset(
    namespace: str, name: str, body: AlterRequest, conn: Conn, actor: Actor = "unknown"
):
    if OP_ID_COLUMN in body.add_columns:
        raise HTTPException(400, f"{OP_ID_COLUMN!r} is reserved")
    entity, placement, table = _load_table(conn, namespace, name, body.version, body.platform)
    if existing := sorted({f.name for f in table.schema().fields} & set(body.add_columns)):
        raise HTTPException(409, f"columns already exist: {existing}")
    add_columns = body.model_dump()["add_columns"]
    next_id = new_id_counter()  # placeholder ids; update_schema assigns the real ones
    try:
        with table.update_schema() as update:
            for col, spec in add_columns.items():
                update.add_column(col, iceberg_type(spec, next_id), required=False)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    op_id = _insert_op(
        conn,
        entity_id=entity["entity_id"],
        op_kind="alter",
        actor=actor,
        warehouse_name=placement["warehouse_name"],
        process=body.process,
        details={
            "add_columns": add_columns,
            "schema_id": table.schema().schema_id,
        },
    )
    schema = table.schema()
    return {"op_id": op_id, "schema_id": schema.schema_id, "fields": schema_as_json(schema)}


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


@app.get("/v1/datasets/{namespace}/{name}/schema", response_model=SchemaOut)
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


@app.get("/v1/datasets/{namespace}/{name}/partitions", response_model=PartitionsOut)
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


@app.get("/v1/warehouses", response_model=list[WarehouseOut])
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
