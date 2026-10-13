import { Stack, Table, Text } from "@chakra-ui/react";

import { $api } from "src/queryClient";

import { AsOfPicker, useAsOf } from "./AsOfPicker";
import type { DatasetRef } from "./index";
import { QueryState } from "./QueryState";

export const PartitionsTab = ({ dataset }: { dataset: DatasetRef }) => {
  const { namespace, name, version } = dataset;
  const as_of = useAsOf();
  const { data, error, isLoading } = $api.useQuery(
    "get",
    "/v1/datasets/{namespace}/{name}/partitions",
    { params: { path: { namespace, name }, query: { version, as_of } } },
    { enabled: version !== undefined },
  );

  return (
    <Stack gap={4} pt={4}>
      <AsOfPicker dataset={dataset} />
      <QueryState error={error} isLoading={isLoading} />
      {data && data.partitions.length === 0 ? (
        <Text color="fg.muted">No data written yet.</Text>
      ) : undefined}
      {data && data.partitions.length > 0 ? (
        <Table.Root size="sm">
          <Table.Header>
            <Table.Row>
              {data.partition_by.map((c) => (
                <Table.ColumnHeader key={c}>{c}</Table.ColumnHeader>
              ))}
              <Table.ColumnHeader textAlign="end">Rows</Table.ColumnHeader>
              <Table.ColumnHeader textAlign="end">Files</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {data.partitions.map((p) => (
              <Table.Row key={JSON.stringify(p.partition)}>
                {data.partition_by.map((c) => (
                  <Table.Cell key={c}>{String(p.partition[c] ?? "")}</Table.Cell>
                ))}
                <Table.Cell textAlign="end">{p.record_count.toLocaleString()}</Table.Cell>
                <Table.Cell textAlign="end">{p.file_count}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      ) : undefined}
    </Stack>
  );
};
