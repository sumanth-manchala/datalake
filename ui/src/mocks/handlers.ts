import { http, HttpResponse } from "msw/http";

import type { components } from "openapi-gen/schema";

type S = components["schemas"];

export const datasets: S["DatasetSummary"][] = [
  {
    namespace: "sales",
    name: "orders",
    version: 1,
    partition_by: ["day"],
    description: "Order lines",
    created_by: "alice",
    created_at: "2026-10-01T00:00:00Z",
    dropped_at: null,
    warehouses: ["local-seaweedfs"],
  },
  {
    namespace: "tmp",
    name: "scratch",
    version: 1,
    partition_by: ["day"],
    description: null,
    created_by: "bob",
    created_at: "2026-10-02T00:00:00Z",
    dropped_at: "2026-10-03T00:00:00Z",
    warehouses: ["local-rustfs"],
  },
];

export const schema: S["SchemaOut"] = {
  schema_id: 0,
  fields: [
    { name: "day", type: "string", required: false },
    { name: "amount", type: "long", required: true },
    { name: "_op_id", type: "string", required: false },
  ],
};

export const partitions: S["PartitionsOut"] = {
  partition_by: ["day"],
  partitions: [{ partition: { day: "2026-10-01" }, record_count: 1200, file_count: 2, spec_id: 0 }],
};

export const history: S["OpRow"][] = [
  {
    op_id: "0b7c8a72-9f1e-4a3b-8d21-93f6a2f4c111",
    warehouse_name: "local-seaweedfs",
    op_kind: "overwrite",
    actor: "alice",
    process_kind: "pipeline",
    process_name: "daily_orders",
    process_run_id: "run-1",
    snapshot_id: 42,
    details: { rows_written: 1200, partitions: [{ day: "2026-10-01" }] },
    occurred_at: "2026-10-01T01:00:00Z",
  },
];

export const warehouses: S["WarehouseOut"][] = [
  { warehouse_name: "local-seaweedfs", platform: "local", catalog_kind: "sql", storage_kind: "s3" },
  { warehouse_name: "local-rustfs", platform: "local", catalog_kind: "sql", storage_kind: "s3" },
];

export const handlers = [
  http.get("*/v1/datasets", () => HttpResponse.json(datasets)),
  http.get("*/v1/warehouses", () => HttpResponse.json(warehouses)),
  http.get("*/v1/datasets/:ns/:name/schema", () => HttpResponse.json(schema)),
  http.get("*/v1/datasets/:ns/:name/partitions", () => HttpResponse.json(partitions)),
  http.get("*/v1/datasets/:ns/:name/history", () => HttpResponse.json(history)),
];
