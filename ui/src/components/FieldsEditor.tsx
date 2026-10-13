import { Button, Checkbox, HStack, IconButton, Input, Stack } from "@chakra-ui/react";

import { emptyField, type FieldSpec } from "src/utils/fieldTypes";

import { TypeEditor } from "./TypeEditor";

type Props = {
  /** aria-label prefix, e.g. "Column" -> "Column 1 name", "Column 1 type" */
  label: string;
  fields: FieldSpec[];
  onChange: (fields: FieldSpec[]) => void;
};

/** Rows of name / type / required — top-level columns and struct fields alike. */
export const FieldsEditor = ({ label, fields, onChange }: Props) => {
  const set = (i: number, patch: Partial<FieldSpec>) =>
    onChange(fields.map((f, j) => (j === i ? { ...f, ...patch } : f)));

  return (
    <Stack gap={3}>
      {fields.map((f, i) => (
        <HStack key={i} align="start">
          <Input
            aria-label={`${label} ${i + 1} name`}
            placeholder="name"
            width="48"
            value={f.name}
            onChange={(e) => set(i, { name: e.target.value })}
          />
          <TypeEditor
            label={`${label} ${i + 1}`}
            value={f.type}
            onChange={(type) => set(i, { type })}
          />
          <Checkbox.Root
            pt={2}
            checked={f.required}
            onCheckedChange={(e) => set(i, { required: !!e.checked })}
          >
            <Checkbox.HiddenInput aria-label={`${label} ${i + 1} required`} />
            <Checkbox.Control />
            <Checkbox.Label>Required</Checkbox.Label>
          </Checkbox.Root>
          <IconButton
            aria-label={`Remove ${label.toLowerCase()} ${i + 1}`}
            variant="ghost"
            disabled={fields.length === 1}
            onClick={() => onChange(fields.filter((_, j) => j !== i))}
          >
            ✕
          </IconButton>
        </HStack>
      ))}
      <Button
        variant="outline"
        size="sm"
        alignSelf="start"
        onClick={() => onChange([...fields, emptyField()])}
      >
        Add {label.toLowerCase()}
      </Button>
    </Stack>
  );
};
