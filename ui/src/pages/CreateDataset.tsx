import {
  Alert,
  Button,
  Checkbox,
  Field,
  Fieldset,
  HStack,
  Heading,
  Input,
  NativeSelect,
  Stack,
  Text,
} from "@chakra-ui/react";
import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { FieldsEditor } from "src/components/FieldsEditor";
import { $api, errorMessage } from "src/queryClient";
import { emptyField, hasUnnamed, isPrimitive, type FieldSpec } from "src/utils/fieldTypes";

export const CreateDataset = () => {
  const navigate = useNavigate();
  const [namespace, setNamespace] = useState("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [warehouse, setWarehouse] = useState<string>();
  const [fields, setFields] = useState<FieldSpec[]>([emptyField()]);
  const [partitionBy, setPartitionBy] = useState<string[]>([]);
  const [newVersion, setNewVersion] = useState(false);

  const warehouses = $api.useQuery("get", "/v1/warehouses");
  const create = $api.useMutation("post", "/v1/datasets", {
    onSuccess: (created) => navigate(`/datasets/${namespace}/${name}?version=${created.version}`),
  });

  const named = fields.filter((f) => f.name);
  // partitions are identity transforms, so only top-level primitive columns qualify
  const fieldNames = named.filter((f) => isPrimitive(f.type)).map((f) => f.name);
  const incomplete = named.some((f) => hasUnnamed(f.type));
  // drop partition columns whose field was renamed or removed
  const partitions = partitionBy.filter((c) => fieldNames.includes(c));
  const selectedWarehouse = warehouse ?? warehouses.data?.[0]?.warehouse_name;

  const submit = (e: FormEvent) => {
    e.preventDefault();
    create.mutate({
      body: {
        namespace,
        name,
        description: description || null,
        warehouse_name: selectedWarehouse,
        schema: named,
        partition_by: partitions,
        new_version: newVersion,
      },
    });
  };

  return (
    <form onSubmit={submit}>
      <Stack gap={4} maxW="2xl">
        <Heading size="lg">New dataset</Heading>
        <HStack align="start">
          <Field.Root required>
            <Field.Label>Namespace</Field.Label>
            <Input value={namespace} onChange={(e) => setNamespace(e.target.value)} />
          </Field.Root>
          <Field.Root required>
            <Field.Label>Name</Field.Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} />
          </Field.Root>
        </HStack>
        <Field.Root>
          <Field.Label>Description</Field.Label>
          <Input value={description} onChange={(e) => setDescription(e.target.value)} />
        </Field.Root>
        <Field.Root>
          <Field.Label>Warehouse</Field.Label>
          <NativeSelect.Root>
            <NativeSelect.Field
              value={selectedWarehouse ?? ""}
              onChange={(e) => setWarehouse(e.target.value)}
            >
              {warehouses.data?.map((w) => (
                <option key={w.warehouse_name} value={w.warehouse_name}>
                  {w.warehouse_name} ({w.platform})
                </option>
              ))}
            </NativeSelect.Field>
            <NativeSelect.Indicator />
          </NativeSelect.Root>
        </Field.Root>

        <Text fontWeight="medium">Columns</Text>
        <FieldsEditor label="Column" fields={fields} onChange={setFields} />

        <Fieldset.Root invalid={partitions.length === 0}>
          <Fieldset.Legend>Partition by</Fieldset.Legend>
          <HStack wrap="wrap" gap={4}>
            {fieldNames.map((c) => (
              <Checkbox.Root
                key={c}
                checked={partitions.includes(c)}
                onCheckedChange={(e) =>
                  setPartitionBy(e.checked ? [...partitions, c] : partitions.filter((p) => p !== c))
                }
              >
                <Checkbox.HiddenInput />
                <Checkbox.Control />
                <Checkbox.Label>{c}</Checkbox.Label>
              </Checkbox.Root>
            ))}
          </HStack>
          <Fieldset.ErrorText>
            Pick at least one partition column — every write is a dynamic partition overwrite.
          </Fieldset.ErrorText>
        </Fieldset.Root>

        <Checkbox.Root checked={newVersion} onCheckedChange={(e) => setNewVersion(!!e.checked)}>
          <Checkbox.HiddenInput />
          <Checkbox.Control />
          <Checkbox.Label>
            New version (backwards-incompatible schema of an existing dataset)
          </Checkbox.Label>
        </Checkbox.Root>

        {create.error ? (
          <Alert.Root status="error">
            <Alert.Indicator />
            <Alert.Title>{errorMessage(create.error)}</Alert.Title>
          </Alert.Root>
        ) : undefined}

        <Button
          type="submit"
          alignSelf="start"
          loading={create.isPending}
          disabled={partitions.length === 0 || incomplete}
        >
          Create dataset
        </Button>
      </Stack>
    </form>
  );
};
