from datetime import UTC, datetime
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from lakehouse_api.app import AlterRequest, CreateDataset, app
from lakehouse_api.catalog import build_schema
from lakehouse_api.db import get_db
from pydantic import ValidationError
from pyiceberg.catalog.sql import SqlCatalog

BODY = {"namespace": "ns", "name": "t", "schema": [{"name": "day", "type": "string"}]}


@pytest.mark.parametrize(
    "overrides",
    [
        {"partition_by": []},
        {"partition_by": ["day"], "schema": [{"name": "day", "type": "uint128"}]},
        {"partition_by": ["day"], "schema": [{"name": "day", "type": "decimal(18)"}]},
        {
            "partition_by": ["day"],
            "schema": [{"name": "s", "type": {"type": "struct", "fields": []}}],
        },
    ],
    ids=["unpartitioned", "unknown-type", "bad-decimal", "empty-struct"],
)
def test_create_dataset_body_rejected(overrides):
    with pytest.raises(ValidationError):
        CreateDataset.model_validate(BODY | overrides)


def test_create_dataset_accepts_iceberg_json_with_ids():
    """Iceberg's own serialization (e.g. from pyarrow via pyiceberg) is accepted; ids ignored."""
    body = CreateDataset.model_validate(
        BODY
        | {
            "partition_by": ["day"],
            "schema": [
                {"id": -1, "name": "day", "type": "string", "required": True},
                {
                    "id": -1,
                    "name": "tags",
                    "type": {"type": "list", "element-id": -1, "element": "string"},
                    "required": False,
                },
                {"id": -1, "name": "amount", "type": "decimal(18, 2)", "required": False},
            ],
        }
    )
    assert body.fields[1].model_dump()["type"] == {
        "type": "list",
        "element": "string",
        "element-required": False,
    }


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ({"add_columns": {"x": "uint128"}}, "literal_error"),
        ({"add_columns": {}}, "too_short"),
        ({"add_columns": {"x": "long"}, "rename_columns": {"a": "b"}}, "new dataset version"),
        ({"add_columns": {"x": "long"}, "drop_columns": ["a"]}, "new dataset version"),
        ({"add_columns": {"x": "long"}, "alter_type": {"a": "long"}}, "extra_forbidden"),
    ],
    ids=["unknown-type", "nothing-to-add", "rename", "drop", "unknown-key"],
)
def test_alter_body_is_additive_only(body, match):
    with pytest.raises(ValidationError, match=match):
        AlterRequest.model_validate(body)


@pytest.fixture
def iceberg_table(tmp_path):
    catalog = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh")
    catalog.create_namespace("ns")
    schema, spec = build_schema([{"name": "day", "type": "string"}], ["day"])
    return catalog.create_table("ns.t", schema=schema, partition_spec=spec)


@pytest.fixture
def alter_client(iceberg_table):
    app.dependency_overrides[get_db] = lambda: mock.MagicMock()
    entity = {"entity_id": "e"}
    placement = {"warehouse_name": "wh"}
    with mock.patch(
        "lakehouse_api.app._load_table",
        autospec=True,
        return_value=(entity, placement, iceberg_table),
    ):
        yield TestClient(app)
    app.dependency_overrides.clear()


def test_alter_adds_optional_columns(alter_client):
    response = alter_client.post(
        "/v1/datasets/ns/t/alter", json={"add_columns": {"channel": "string"}}
    )
    assert response.status_code == 200
    assert {"name": "channel", "type": "string", "required": False} in response.json()["fields"]


def test_alter_adds_optional_nested_column(alter_client):
    nested = {"type": "struct", "fields": [{"name": "id", "type": "long", "required": True}]}
    response = alter_client.post(
        "/v1/datasets/ns/t/alter", json={"add_columns": {"customer": nested}}
    )
    assert response.status_code == 200
    assert {"name": "customer", "type": nested, "required": False} in response.json()["fields"]


def test_alter_existing_column_conflicts(alter_client):
    response = alter_client.post("/v1/datasets/ns/t/alter", json={"add_columns": {"day": "long"}})
    assert response.status_code == 409
    assert "already exist" in response.json()["detail"]


@pytest.fixture
def rows():
    conn = mock.MagicMock()
    app.dependency_overrides[get_db] = lambda: conn
    yield conn.cursor.return_value.__enter__.return_value.fetchall
    app.dependency_overrides.clear()


def test_warehouses_hide_credentials(rows):
    rows.return_value = [
        {
            "warehouse_name": "wh",
            "platform": "local",
            "catalog_kind": "sql",
            "catalog_config": {"uri": "postgresql://u:secret@h/db"},
            "storage_kind": "s3",
            "storage_config": {"secret_key": "secret"},
            "connector": "pyiceberg",
        }
    ]
    response = TestClient(app).get("/v1/warehouses")
    assert response.json() == [
        {"warehouse_name": "wh", "platform": "local", "catalog_kind": "sql", "storage_kind": "s3"}
    ]


def test_list_datasets_typed(rows):
    rows.return_value = [
        {
            "namespace": "ns",
            "name": "t",
            "version": 1,
            "partition_by": ["day"],
            "description": None,
            "created_by": "me",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "dropped_at": None,
            "warehouses": ["wh"],
        }
    ]
    (row,) = TestClient(app).get("/v1/datasets").json()
    assert row["created_at"] == "2026-01-01T00:00:00Z"
    assert row["warehouses"] == ["wh"]
