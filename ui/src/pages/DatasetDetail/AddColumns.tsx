import { Alert, Button, HStack, IconButton, Input, Stack, Text } from "@chakra-ui/react";
import { useState, type FormEvent } from "react";

import { TypeEditor } from "src/components/TypeEditor";
import { $api, errorMessage, queryClient } from "src/queryClient";
import { hasUnnamed, type TypeSpec } from "src/utils/fieldTypes";

import type { DatasetRef } from "./index";

type NewColumn = { name: string; type: TypeSpec };

/** Additive-only schema evolution: new columns are optional; anything else needs a new version. */
export const AddColumns = ({ dataset: { namespace, name, version } }: { dataset: DatasetRef }) => {
  const [columns, setColumns] = useState<NewColumn[]>();
  const alter = $api.useMutation("post", "/v1/datasets/{namespace}/{name}/alter", {
    onSuccess: async () => {
      setColumns(undefined);
      // schema, history (new alter op) and the time-travel picker all changed
      await queryClient.invalidateQueries();
    },
  });

  if (!columns)
    return (
      <Button
        variant="outline"
        alignSelf="start"
        onClick={() => setColumns([{ name: "", type: "string" }])}
      >
        Add columns
      </Button>
    );

  const set = (i: number, patch: Partial<NewColumn>) =>
    setColumns(columns.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const named = columns.filter((c) => c.name.trim());
  const incomplete = named.some((c) => hasUnnamed(c.type));

  const submit = (e: FormEvent) => {
    e.preventDefault();
    alter.mutate({
      params: { path: { namespace, name } },
      body: {
        version,
        add_columns: Object.fromEntries(named.map((c) => [c.name.trim(), c.type])),
      },
    });
  };

  return (
    <form onSubmit={submit}>
      <Stack gap={3} borderWidth="1px" borderColor="border" rounded="md" p={4}>
        <Text fontSize="sm" color="fg.muted">
          New columns are optional: existing rows read them as null and current writers keep working
          without them. Renames, drops and type changes need a new dataset version.
        </Text>
        {columns.map((c, i) => (
          <HStack key={i} align="start">
            <Input
              width="48"
              aria-label={`New column ${i + 1} name`}
              placeholder="column name"
              value={c.name}
              onChange={(e) => set(i, { name: e.target.value })}
            />
            <TypeEditor
              label={`New column ${i + 1}`}
              value={c.type}
              onChange={(type) => set(i, { type })}
            />
            <IconButton
              aria-label={`Remove new column ${i + 1}`}
              variant="ghost"
              disabled={columns.length === 1}
              onClick={() => setColumns(columns.filter((_, j) => j !== i))}
            >
              ✕
            </IconButton>
          </HStack>
        ))}
        {alter.error ? (
          <Alert.Root status="error">
            <Alert.Indicator />
            <Alert.Title>{errorMessage(alter.error)}</Alert.Title>
          </Alert.Root>
        ) : undefined}
        <HStack>
          <Button
            variant="ghost"
            onClick={() => setColumns([...columns, { name: "", type: "string" }])}
          >
            Another column
          </Button>
          <Button variant="ghost" onClick={() => setColumns(undefined)}>
            Cancel
          </Button>
          <Button
            type="submit"
            loading={alter.isPending}
            disabled={named.length === 0 || incomplete}
          >
            Add {columns.length === 1 ? "column" : "columns"}
          </Button>
        </HStack>
      </Stack>
    </form>
  );
};
