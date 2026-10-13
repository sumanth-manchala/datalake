"""Warehouse row -> pyiceberg catalog, and schema JSON <-> Iceberg schema.

Schema JSON is Iceberg's own type serialization minus field ids (we assign them; the catalog
reassigns anyway): primitives are names ("long", "decimal(18,2)"), nested types are
{"type": "list"|"map"|"struct", ...} exactly as in the Iceberg spec.
"""

import itertools
import re
from collections.abc import Callable
from typing import Any, Literal

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)

# Hidden lineage column stamped on every row at write time; survives compaction.
OP_ID_COLUMN = "_op_id"

FieldType = Literal[
    "string",
    "int",
    "long",
    "float",
    "double",
    "boolean",
    "date",
    "timestamp",
    "timestamptz",
    "binary",
]

FIELD_TYPES: dict[FieldType, Any] = {
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


DECIMAL = re.compile(r"decimal\((\d+),\s*(\d+)\)")


def iceberg_type(spec: str | dict[str, Any], next_id: Callable[[], int]) -> IcebergType:
    """One schema-JSON type -> Iceberg type. Nested ids come from next_id."""
    if isinstance(spec, str):
        if m := DECIMAL.fullmatch(spec):
            return DecimalType(int(m[1]), int(m[2]))
        if spec not in FIELD_TYPES:
            raise ValueError(
                f"unsupported type {spec!r} (supported: {sorted(FIELD_TYPES)}, "
                "decimal(p,s), list, map, struct)"
            )
        return FIELD_TYPES[spec]
    match spec["type"]:
        case "list":
            return ListType(
                next_id(),
                iceberg_type(spec["element"], next_id),
                element_required=spec.get("element-required", False),
            )
        case "map":
            return MapType(
                next_id(),
                iceberg_type(spec["key"], next_id),
                next_id(),
                iceberg_type(spec["value"], next_id),
                value_required=spec.get("value-required", False),
            )
        case "struct":
            return StructType(*(_nested_field(f, next_id) for f in spec["fields"]))
    raise ValueError(f"unsupported type {spec['type']!r}")


def _nested_field(field: dict[str, Any], next_id: Callable[[], int]) -> NestedField:
    return NestedField(
        field_id=next_id(),
        name=field["name"],
        field_type=iceberg_type(field["type"], next_id),
        required=field.get("required", False),
    )


def new_id_counter() -> Callable[[], int]:
    return itertools.count(1).__next__


def build_schema(
    fields: list[dict[str, Any]], partition_by: list[str]
) -> tuple[Schema, PartitionSpec]:
    """JSON field specs -> Iceberg schema (with the _op_id lineage column) + identity partition
    spec. partition_by is non-empty: every write is a dynamic partition overwrite."""
    names = [f["name"] for f in fields]
    if unknown := set(partition_by) - set(names):
        raise ValueError(f"partition_by columns not in schema: {sorted(unknown)}")
    if OP_ID_COLUMN in names:
        raise ValueError(f"{OP_ID_COLUMN!r} is a reserved column name")

    next_id = new_id_counter()
    nested = [_nested_field(f, next_id) for f in fields]
    nested.append(
        NestedField(field_id=next_id(), name=OP_ID_COLUMN, field_type=StringType(), required=False)
    )
    schema = Schema(*nested)
    if complex_ := [c for c in partition_by if not schema.find_type(c).is_primitive]:
        raise ValueError(f"partition columns must be primitive types: {complex_}")
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


def type_json(t: IcebergType) -> str | dict[str, Any]:
    """Iceberg type -> schema JSON (inverse of iceberg_type, ids dropped)."""
    if isinstance(t, ListType):
        return {
            "type": "list",
            "element": type_json(t.element_type),
            "element-required": t.element_required,
        }
    if isinstance(t, MapType):
        return {
            "type": "map",
            "key": type_json(t.key_type),
            "value": type_json(t.value_type),
            "value-required": t.value_required,
        }
    if isinstance(t, StructType):
        return {"type": "struct", "fields": [_field_json(f) for f in t.fields]}
    if isinstance(t, DecimalType):
        return f"decimal({t.precision},{t.scale})"
    return str(t)


def _field_json(f: NestedField) -> dict[str, Any]:
    return {"name": f.name, "type": type_json(f.field_type), "required": f.required}


def schema_as_json(schema: Schema) -> list[dict[str, Any]]:
    return [_field_json(f) for f in schema.fields]
