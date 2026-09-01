"""End-to-end flow through the API + SDK against real Postgres and both object stores."""

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

    op1 = ds.append(pa.table({"day": ["2026-01-01", "2026-01-02"], "amount": [1, 2]}))
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
    ds.append(pa.table({"day": ["2026-01-03"], "amount": [5], "channel": ["web"]}))
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
    assert {h["op_kind"] for h in history} >= {"create", "append", "overwrite", "alter", "read"}
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
        schema={"amount": "long"},
        partition_by=[],
        warehouse_name="local-seaweedfs",
    )
    with pytest.raises(LakehouseError, match="409"):
        lake.create_dataset(
            "mkt.spend",
            schema={"amount": "long"},
            partition_by=[],
            warehouse_name="local-seaweedfs",
        )


def test_new_version_for_incompatible_schema(lake):
    ds1 = lake.create_dataset(
        "mkt.clicks",
        schema={"amount": "long"},
        partition_by=[],
        warehouse_name="local-seaweedfs",
    )
    ds1.append(pa.table({"amount": [1]}))

    ds2 = lake.create_dataset(
        "mkt.clicks",
        schema={"amount": "string"},
        partition_by=[],
        warehouse_name="local-rustfs",
        new_version=True,
    )
    assert ds2.version == 2
    assert ds2.doc["physical_identifier"].endswith("_v2")
    ds2.append(pa.table({"amount": ["a lot"]}))

    # latest resolves to v2; v1 stays pinned and readable
    assert lake.dataset("mkt.clicks").version == 2
    pinned = lake.dataset("mkt.clicks", version=1)
    assert pinned.to_pandas()["amount"].tolist() == [1]
    assert lake.dataset("mkt.clicks").to_pandas()["amount"].tolist() == ["a lot"]


def test_drop_is_soft_and_history_survives(lake):
    ds = lake.create_dataset(
        "tmp.scratch",
        schema={"x": "long"},
        partition_by=[],
        warehouse_name="local-rustfs",
    )
    ds.append(pa.table({"x": [1]}))
    lake.drop_dataset("tmp.scratch")

    with pytest.raises(LakehouseError, match="404"):
        lake.dataset("tmp.scratch")
    # ledger survives the drop (soft delete)
    history = lake.dataset_history(ds.doc, kind="all")
    assert {h["op_kind"] for h in history} >= {"create", "append", "drop"}
