import { Badge, Stack, Table, Text } from "@chakra-ui/react";

import { $api } from "src/queryClient";
import { nestedFields, typeLabel, type FieldSpec } from "src/utils/fieldTypes";

import { AddColumns } from "./AddColumns";
import { AsOfPicker, useAsOf } from "./AsOfPicker";
import type { DatasetRef } from "./index";
import { QueryState } from "./QueryState";

type Row = { field: FieldSpec; depth: number; path: string };

/** Nested struct fields (also under list elements / map values) become indented child rows. */
const flatten = (fields: FieldSpec[], depth = 0, prefix = ""): Row[] =>
  fields.flatMap((field) => {
    const path = prefix + field.name;
    return [{ field, depth, path }, ...flatten(nestedFields(field.type), depth + 1, `${path}.`)];
  });

export const SchemaTab = ({ dataset }: { dataset: DatasetRef }) => {
  const { namespace, name, version } = dataset;
  const as_of = useAsOf();
  const { data, error, isLoading } = $api.useQuery(
    "get",
    "/v1/datasets/{namespace}/{name}/schema",
    { params: { path: { namespace, name }, query: { version, as_of } } },
    { enabled: version !== undefined },
  );

  return (
    <Stack gap={4} pt={4}>
      <AsOfPicker dataset={dataset} />
      <QueryState error={error} isLoading={isLoading} />
      {/* edits apply to the current schema only, never to a past point */}
      {data && !as_of ? <AddColumns dataset={dataset} /> : undefined}
      {data ? (
        <Table.Root size="sm">
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Column</Table.ColumnHeader>
              <Table.ColumnHeader>Type</Table.ColumnHeader>
              <Table.ColumnHeader>Required</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {flatten(data.fields).map(({ field: f, depth, path }) => (
              <Table.Row key={path}>
                <Table.Cell ps={4 + depth * 6}>
                  {depth > 0 ? (
                    <Text as="span" color="fg.subtle">
                      └{" "}
                    </Text>
                  ) : undefined}
                  {f.name}
                  {f.name === "_op_id" ? (
                    <Badge ml={2} title="hidden lineage column: the op that wrote the row">
                      lineage
                    </Badge>
                  ) : undefined}
                </Table.Cell>
                <Table.Cell>{typeLabel(f.type)}</Table.Cell>
                <Table.Cell>{f.required ? "yes" : "no"}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      ) : undefined}
    </Stack>
  );
};
