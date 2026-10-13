"""End-to-end flow through the API + SDK against real Postgres and both object stores."""

from decimal import Decimal

import pyarrow as pa
import pytest
from lakehouse import LakehouseError


@pytest.mark.parametrize("warehouse", ["local-seaweedfs", "local-rustfs"])
def test_dataset_lifecycle(lake, warehouse):
    name = f"sales_{warehouse.removeprefix('local-')}.orders"
    ds = lake.create_dataset(
        name,
        schema={"day": "string", "amount": "long"},
        partition_by=["day"],
        warehouse_name=warehouse,
    )

    op1 = ds.overwrite(pa.table({"day": ["2026-01-01", "2026-01-02"], "amount": [1, 2]}))
    op2 = ds.overwrite(pa.table({"day": ["2026-01-01"], "amount": [10]}))  # only day-01 replaced

    df = ds.to_pandas()
    assert sorted(df["amount"]) == [2, 10]
    assert "_op_id" not in df.columns

    lineage = ds.to_pandas(include_lineage=True)
    assert set(lineage["_op_id"]) == {op1, op2}

    conn = ds.to_duckdb("orders")
    assert conn.execute("SELECT sum(amount) FROM orders").fetchone()[0] == 12

    # time travel to the pre-overwrite write
    old = ds.to_pandas(as_of=op1)
    assert sorted(old["amount"]) == [1, 2]

    # compatible schema evolution, then write with the new column
    ds.alter(add_columns={"channel": "string"})
    ds.overwrite(pa.table({"day": ["2026-01-03"], "amount": [5], "channel": ["web"]}))
    assert "channel" in [f["name"] for f in ds.schema()["fields"]]
    assert "channel" not in [f["name"] for f in ds.schema(as_of=op1)["fields"]]

    partitions = ds.partitions()
    assert partitions["partition_by"] == ["day"]
    assert {p["partition"]["day"] for p in partitions["partitions"]} == {
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    }

    history = ds.history()
    assert {h["op_kind"] for h in history} >= {"create", "overwrite", "alter", "read"}
    assert {h["actor"] for h in history} == {"it-tester"}
    assert {h["process_kind"] for h in history} == {"adhoc"}
    reads = ds.history(kind="reads")
    assert reads and all(h["op_kind"] == "read" for h in reads)
    assert all(h["snapshot_id"] is not None for h in reads)

    # rollback to the first write; current read shows pre-overwrite data again
    ds.rollback(to=op1)
    assert sorted(ds.to_pandas()["amount"]) == [1, 2]


def test_duplicate_create_conflicts(lake):
    lake.create_dataset(
        "mkt.spend",
        schema={"day": "string", "amount": "long"},
        partition_by=["day"],
        warehouse_name="local-seaweedfs",
    )
    with pytest.raises(LakehouseError, match="409"):
        lake.create_dataset(
            "mkt.spend",
            schema={"day": "string", "amount": "long"},
            partition_by=["day"],
            warehouse_name="local-seaweedfs",
        )


def test_unpartitioned_create_rejected(lake):
    with pytest.raises(LakehouseError, match="422"):
        lake.create_dataset(
            "mkt.flat",
            schema={"amount": "long"},
            partition_by=[],
            warehouse_name="local-seaweedfs",
        )


def test_new_version_for_incompatible_schema(lake):
    ds1 = lake.create_dataset(
        "mkt.clicks",
        schema={"day": "string", "amount": "long"},
        partition_by=["day"],
        warehouse_name="local-seaweedfs",
    )
    ds1.overwrite(pa.table({"day": ["d"], "amount": [1]}))

    ds2 = lake.create_dataset(
        "mkt.clicks",
        schema={"day": "string", "amount": "string"},
        partition_by=["day"],
        warehouse_name="local-rustfs",
        new_version=True,
    )
    assert ds2.version == 2
    assert ds2.doc["physical_identifier"].endswith("_v2")
    ds2.overwrite(pa.table({"day": ["d"], "amount": ["a lot"]}))

    # latest resolves to v2; v1 stays pinned and readable
    assert lake.dataset("mkt.clicks").version == 2
    pinned = lake.dataset("mkt.clicks", version=1)
    assert pinned.to_pandas()["amount"].tolist() == [1]
    assert lake.dataset("mkt.clicks").to_pandas()["amount"].tolist() == ["a lot"]


def test_drop_is_soft_and_history_survives(lake):
    ds = lake.create_dataset(
        "tmp.scratch",
        schema={"day": "string", "x": "long"},
        partition_by=["day"],
        warehouse_name="local-rustfs",
    )
    ds.overwrite(pa.table({"day": ["d"], "x": [1]}))
    lake.drop_dataset("tmp.scratch")

    with pytest.raises(LakehouseError, match="404"):
        lake.dataset("tmp.scratch")
    # ledger survives the drop (soft delete)
    history = lake.dataset_history(ds.doc, kind="all")
    assert {h["op_kind"] for h in history} >= {"create", "overwrite", "drop"}


def test_complex_types_round_trip(lake):
    arrow = pa.schema(
        [
            pa.field("day", pa.string(), nullable=False),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("attrs", pa.map_(pa.string(), pa.int64())),
            pa.field("customer", pa.struct([("id", pa.int64()), ("email", pa.string())])),
            pa.field("amount", pa.decimal128(18, 2)),
        ]
    )
    ds = lake.create_dataset(
        "crm.events", schema=arrow, partition_by=["day"], warehouse_name="local-seaweedfs"
    )
    rows = pa.Table.from_pylist(
        [
            {
                "day": "d1",
                "tags": ["a", "b"],
                "attrs": [("k", 1)],
                "customer": {"id": 7, "email": "x@y"},
                "amount": Decimal("12.50"),
            }
        ],
        schema=arrow,
    )
    ds.overwrite(rows)

    (row,) = ds.to_arrow().to_pylist()
    assert row["tags"] == ["a", "b"]
    assert row["attrs"] == [("k", 1)]
    assert row["customer"] == {"id": 7, "email": "x@y"}
    assert row["amount"] == Decimal("12.50")
    fields = {f["name"]: f["type"] for f in ds.schema()["fields"]}
    assert fields["customer"]["type"] == "struct"
    assert fields["amount"] == "decimal(18,2)"

    # additive nested column; existing writers keep working without it
    ds.alter(add_columns={"labels": {"type": "list", "element": "string"}})
    subset = pa.schema([arrow.field("day"), arrow.field("tags")])  # required day stays non-null
    ds.overwrite(pa.table({"day": ["d2"], "tags": [["c"]]}, schema=subset))
    assert {f["name"] for f in ds.schema()["fields"]} >= {"labels"}
    assert [r["labels"] for r in ds.to_arrow().to_pylist()] == [None, None]
