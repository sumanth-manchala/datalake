"""Warehouse row -> pyiceberg catalog, and schema JSON <-> Iceberg schema."""

from typing import Any

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

# Hidden lineage column stamped on every row at write time; survives compaction.
OP_ID_COLUMN = "_op_id"

FIELD_TYPES = {
    "string": StringType(),
    "int": LongType(),
    "long": LongType(),
    "float": DoubleType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamptz": TimestamptzType(),
    "binary": BinaryType(),
}


def storage_properties(storage_kind: str, storage_config: dict[str, Any]) -> dict[str, str]:
    if storage_kind != "s3":
        return {}
    return {
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": storage_config["endpoint_url"],
        "s3.access-key-id": storage_config["access_key"],
        "s3.secret-access-key": storage_config["secret_key"],
        "s3.region": "local",
    }


def load_warehouse_catalog(warehouse: dict[str, Any]) -> Catalog:
    if warehouse["catalog_kind"] != "sql":
        raise ValueError(f"unsupported catalog_kind {warehouse['catalog_kind']!r}")
    cfg = warehouse["catalog_config"]
    return SqlCatalog(
        warehouse["warehouse_name"],
        uri=cfg["uri"],
        warehouse=cfg["warehouse"],
        **storage_properties(warehouse["storage_kind"], warehouse["storage_config"]),
    )


def build_schema(
    fields: list[dict[str, Any]], partition_by: list[str]
) -> tuple[Schema, PartitionSpec]:
    """JSON field specs -> Iceberg schema (with the _op_id lineage column) + partition spec."""
    names = [f["name"] for f in fields]
    if unknown := set(partition_by) - set(names):
        raise ValueError(f"partition_by columns not in schema: {sorted(unknown)}")
    if OP_ID_COLUMN in names:
        raise ValueError(f"{OP_ID_COLUMN!r} is a reserved column name")
    bad_types = {f["type"] for f in fields} - set(FIELD_TYPES)
    if bad_types:
        raise ValueError(
            f"unsupported field types: {sorted(bad_types)} (supported: {sorted(FIELD_TYPES)})"
        )

    nested = [
        NestedField(
            field_id=i,
            name=f["name"],
            field_type=FIELD_TYPES[f["type"]],
            required=f.get("required", False),
        )
        for i, f in enumerate(fields, start=1)
    ]
    nested.append(
        NestedField(
            field_id=len(nested) + 1, name=OP_ID_COLUMN, field_type=StringType(), required=False
        )
    )
    schema = Schema(*nested)

    if not partition_by:
        return schema, UNPARTITIONED_PARTITION_SPEC
    spec = PartitionSpec(
        *[
            PartitionField(
                source_id=schema.find_field(col).field_id,
                field_id=1000 + i,
                transform=IdentityTransform(),
                name=col,
            )
            for i, col in enumerate(partition_by)
        ]
    )
    return schema, spec


def schema_as_json(schema: Schema) -> list[dict[str, Any]]:
    return [
        {"name": f.name, "type": str(f.field_type), "required": f.required} for f in schema.fields
    ]
