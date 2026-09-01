import pytest
from lakehouse_api.catalog import (
    OP_ID_COLUMN,
    build_schema,
    load_warehouse_catalog,
    schema_as_json,
    storage_properties,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform

FIELDS = [
    {"name": "day", "type": "string"},
    {"name": "amount", "type": "long", "required": True},
]


def test_build_schema_appends_lineage_column_and_identity_spec():
    schema, spec = build_schema(FIELDS, ["day"])

    assert [f.name for f in schema.fields] == ["day", "amount", OP_ID_COLUMN]
    assert schema.find_field("amount").required
    assert not schema.find_field(OP_ID_COLUMN).required
    assert [(f.name, f.transform) for f in spec.fields] == [("day", IdentityTransform())]
    assert spec.fields[0].source_id == schema.find_field("day").field_id


def test_build_schema_unpartitioned():
    _, spec = build_schema(FIELDS, [])
    assert spec.fields == ()


@pytest.mark.parametrize(
    ("fields", "partition_by", "match"),
    [
        (FIELDS, ["nope"], "partition_by columns not in schema"),
        (FIELDS + [{"name": OP_ID_COLUMN, "type": "string"}], [], "reserved"),
        ([{"name": "x", "type": "uint128"}], [], "unsupported field types"),
    ],
)
def test_build_schema_rejects(fields, partition_by, match):
    with pytest.raises(ValueError, match=match):
        build_schema(fields, partition_by)


def test_storage_properties_s3_and_other():
    props = storage_properties(
        "s3",
        {"endpoint_url": "http://localhost:1", "access_key": "k", "secret_key": "s"},
    )
    assert props["s3.endpoint"] == "http://localhost:1"
    assert props["s3.access-key-id"] == "k"
    assert props["s3.secret-access-key"] == "s"
    assert storage_properties("file", {}) == {}


def test_load_warehouse_catalog_sql(tmp_path):
    catalog = load_warehouse_catalog(
        {
            "warehouse_name": "wh",
            "catalog_kind": "sql",
            "catalog_config": {
                "uri": f"sqlite:///{tmp_path}/cat.db",
                "warehouse": f"file://{tmp_path}/wh",
            },
            "storage_kind": "file",
            "storage_config": {},
        }
    )
    assert isinstance(catalog, SqlCatalog)


def test_load_warehouse_catalog_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unsupported catalog_kind"):
        load_warehouse_catalog({"catalog_kind": "glue"})


def test_schema_as_json():
    schema, _ = build_schema(FIELDS, [])
    assert schema_as_json(schema) == [
        {"name": "day", "type": "string", "required": False},
        {"name": "amount", "type": "long", "required": True},
        {"name": OP_ID_COLUMN, "type": "string", "required": False},
    ]
