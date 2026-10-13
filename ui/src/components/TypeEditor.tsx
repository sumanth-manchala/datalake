import { Box, HStack, Input, NativeSelect, Stack, Text } from "@chakra-ui/react";

import {
  COMPLEX_KINDS,
  defaultFor,
  FIELD_TYPES,
  kindOf,
  type Kind,
  type TypeSpec,
} from "src/utils/fieldTypes";

import { FieldsEditor } from "./FieldsEditor";

type Props = { label: string; value: TypeSpec; onChange: (value: TypeSpec) => void };

/** Recursive editor for one Iceberg type: primitive, decimal(p,s), list, map or struct. */
export const TypeEditor = ({ label, value, onChange }: Props) => {
  const kind = kindOf(value);
  const [precision, scale] =
    typeof value === "string" && kind === "decimal" ? (value.match(/\d+/g) ?? ["18", "2"]) : [];

  return (
    <Stack gap={2} flex="1">
      <HStack>
        <NativeSelect.Root width="40">
          <NativeSelect.Field
            aria-label={`${label} type`}
            value={kind}
            onChange={(e) => onChange(defaultFor(e.target.value as Kind))}
          >
            {[...FIELD_TYPES, ...COMPLEX_KINDS].map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </NativeSelect.Field>
          <NativeSelect.Indicator />
        </NativeSelect.Root>
        {kind === "decimal" ? (
          <>
            <Input
              aria-label={`${label} precision`}
              type="number"
              min={1}
              max={38}
              width="24"
              value={precision}
              onChange={(e) => onChange(`decimal(${e.target.value},${scale})`)}
            />
            <Input
              aria-label={`${label} scale`}
              type="number"
              min={0}
              width="24"
              value={scale}
              onChange={(e) => onChange(`decimal(${precision},${e.target.value})`)}
            />
          </>
        ) : undefined}
      </HStack>
      {typeof value === "string" ? undefined : (
        <Box borderStartWidth="2px" borderColor="border" ps={3}>
          {value.type === "list" ? (
            <HStack align="start">
              <Text fontSize="sm" color="fg.muted" pt={2} minW="12">
                element
              </Text>
              <TypeEditor
                label={`${label} element`}
                value={value.element}
                onChange={(element) => onChange({ ...value, element })}
              />
            </HStack>
          ) : undefined}
          {value.type === "map" ? (
            <Stack gap={2}>
              <HStack align="start">
                <Text fontSize="sm" color="fg.muted" pt={2} minW="12">
                  key
                </Text>
                <TypeEditor
                  label={`${label} key`}
                  value={value.key}
                  onChange={(key) => onChange({ ...value, key })}
                />
              </HStack>
              <HStack align="start">
                <Text fontSize="sm" color="fg.muted" pt={2} minW="12">
                  value
                </Text>
                <TypeEditor
                  label={`${label} value`}
                  value={value.value}
                  onChange={(v) => onChange({ ...value, value: v })}
                />
              </HStack>
            </Stack>
          ) : undefined}
          {value.type === "struct" ? (
            <FieldsEditor
              label={`${label} field`}
              fields={value.fields}
              onChange={(fields) => onChange({ ...value, fields })}
            />
          ) : undefined}
        </Box>
      )}
    </Stack>
  );
};
