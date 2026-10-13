import {
  Box,
  Clipboard,
  Code,
  HStack,
  IconButton,
  Link,
  NativeSelect,
  Stack,
  Table,
  Text,
} from "@chakra-ui/react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import { $api } from "src/queryClient";

import type { DatasetRef } from "./index";
import { QueryState } from "./QueryState";

const KINDS = ["all", "writes", "reads"] as const;
type Kind = (typeof KINDS)[number];

export const HistoryTab = ({ dataset: { namespace, name, version } }: { dataset: DatasetRef }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const kind = (searchParams.get("kind") ?? "all") as Kind;
  const { data, error, isLoading } = $api.useQuery(
    "get",
    "/v1/datasets/{namespace}/{name}/history",
    { params: { path: { namespace, name }, query: { version, kind } } },
    { enabled: version !== undefined },
  );

  /** Same page, another tab, pinned to the snapshot this op produced. */
  const viewAt = (tab: string, opId: string) => {
    const params = new URLSearchParams({ tab, as_of: opId });
    if (version !== undefined) params.set("version", String(version));
    return `?${params}`;
  };

  return (
    <Stack gap={4} pt={4}>
      <NativeSelect.Root width="40">
        <NativeSelect.Field
          aria-label="Operation kind"
          value={kind}
          onChange={(e) =>
            setSearchParams((prev) => {
              prev.set("kind", e.target.value);
              return prev;
            })
          }
        >
          {KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </NativeSelect.Field>
        <NativeSelect.Indicator />
      </NativeSelect.Root>
      <QueryState error={error} isLoading={isLoading} />
      {data && data.length === 0 ? <Text color="fg.muted">No operations.</Text> : undefined}
      {data && data.length > 0 ? (
        <Table.Root size="sm">
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>When</Table.ColumnHeader>
              <Table.ColumnHeader>Op</Table.ColumnHeader>
              <Table.ColumnHeader>Op id</Table.ColumnHeader>
              <Table.ColumnHeader>Actor</Table.ColumnHeader>
              <Table.ColumnHeader>Process</Table.ColumnHeader>
              <Table.ColumnHeader>Snapshot</Table.ColumnHeader>
              <Table.ColumnHeader>Details</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {data.map((op) => (
              <Table.Row key={op.op_id} verticalAlign="top">
                <Table.Cell whiteSpace="nowrap">
                  {new Date(op.occurred_at).toLocaleString()}
                </Table.Cell>
                <Table.Cell>{op.op_kind}</Table.Cell>
                <Table.Cell>
                  <HStack gap={1}>
                    <Code title={op.op_id}>{op.op_id.slice(0, 8)}</Code>
                    <Clipboard.Root value={op.op_id}>
                      <Clipboard.Trigger asChild>
                        <IconButton
                          aria-label={`Copy op id ${op.op_id}`}
                          size="2xs"
                          variant="ghost"
                        >
                          <Clipboard.Indicator />
                        </IconButton>
                      </Clipboard.Trigger>
                    </Clipboard.Root>
                  </HStack>
                </Table.Cell>
                <Table.Cell>{op.actor}</Table.Cell>
                <Table.Cell>
                  {[op.process_kind, op.process_name, op.process_run_id]
                    .filter(Boolean)
                    .join(" / ")}
                </Table.Cell>
                <Table.Cell>
                  {op.snapshot_id ?? "—"}
                  {/* reads also carry a snapshot, but "view as of" belongs to the write that made it */}
                  {op.snapshot_id !== null && op.op_kind !== "read" ? (
                    <HStack gap={2} fontSize="xs">
                      <Link asChild>
                        <RouterLink to={viewAt("schema", op.op_id)}>schema</RouterLink>
                      </Link>
                      <Link asChild>
                        <RouterLink to={viewAt("partitions", op.op_id)}>partitions</RouterLink>
                      </Link>
                    </HStack>
                  ) : undefined}
                </Table.Cell>
                <Table.Cell>
                  <Box as="details">
                    <summary>details</summary>
                    <Code as="pre" display="block" whiteSpace="pre-wrap" mt={2}>
                      {JSON.stringify(op.details, null, 2)}
                    </Code>
                  </Box>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      ) : undefined}
    </Stack>
  );
};
