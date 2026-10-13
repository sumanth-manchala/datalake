import type { components } from "openapi-gen/schema";
import { $api } from "src/queryClient";

import type { DatasetRef } from "./index";

type OpRow = components["schemas"]["OpRow"];

/** "Oct 13, 11:40 · overwrite · alice · day=d1, day=d2" — how a point in time is named in the UI. */
export const opLabel = (op: OpRow) => {
  const when = new Date(op.occurred_at).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
  const partitions = (op.details.partitions as Record<string, string>[] | undefined) ?? [];
  const touched = partitions
    .slice(0, 3)
    .map((p) =>
      Object.entries(p)
        .map(([k, v]) => `${k}=${v}`)
        .join("/"),
    )
    .join(", ");
  const more = partitions.length > 3 ? ` +${partitions.length - 3}` : "";
  return [when, op.op_kind, op.actor, touched + more].filter(Boolean).join(" · ");
};

/** Writes that produced a snapshot = the points you can time-travel to, newest first. */
export const useTimeTravelPoints = ({ namespace, name, version }: DatasetRef) => {
  const query = $api.useQuery(
    "get",
    "/v1/datasets/{namespace}/{name}/history",
    { params: { path: { namespace, name }, query: { version, kind: "writes" } } },
    { enabled: version !== undefined },
  );
  return { ...query, data: query.data?.filter((op) => op.snapshot_id !== null) };
};
