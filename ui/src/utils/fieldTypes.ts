import type { components } from "openapi-gen/schema";

export type FieldType = components["schemas"]["PrimitiveName"];
export type FieldSpec = components["schemas"]["FieldSpec"];
export type TypeSpec = FieldSpec["type"];

// Record<FieldType, …> makes tsc fail if the API's type enum and this list drift apart.
export const FIELD_TYPES = Object.keys({
  string: 1,
  int: 1,
  long: 1,
  float: 1,
  double: 1,
  boolean: 1,
  date: 1,
  timestamp: 1,
  timestamptz: 1,
  binary: 1,
} satisfies Record<FieldType, 1>) as FieldType[];

export const COMPLEX_KINDS = ["decimal", "list", "map", "struct"] as const;
export type Kind = FieldType | (typeof COMPLEX_KINDS)[number];

export const kindOf = (spec: TypeSpec): Kind =>
  typeof spec === "string"
    ? spec.startsWith("decimal(")
      ? "decimal"
      : (spec as FieldType)
    : spec.type;

export const emptyField = (): FieldSpec => ({ name: "", type: "string", required: false });

export const defaultFor = (kind: Kind): TypeSpec => {
  switch (kind) {
    case "decimal":
      return "decimal(18,2)";
    case "list":
      return { type: "list", element: "string" };
    case "map":
      return { type: "map", key: "string", value: "string" };
    case "struct":
      return { type: "struct", fields: [emptyField()] };
    default:
      return kind;
  }
};

/** Compact display form: list<string>, map<string,long>; structs stay "struct" (their
 * fields are shown as child rows). */
export const typeLabel = (spec: TypeSpec): string => {
  if (typeof spec === "string") return spec;
  switch (spec.type) {
    case "list":
      return `list<${typeLabel(spec.element)}>`;
    case "map":
      return `map<${typeLabel(spec.key)},${typeLabel(spec.value)}>`;
    case "struct":
      return "struct";
  }
};

/** Struct fields reachable under a type (through list elements and map values). */
export const nestedFields = (spec: TypeSpec): FieldSpec[] => {
  if (typeof spec === "string") return [];
  switch (spec.type) {
    case "struct":
      return spec.fields;
    case "list":
      return nestedFields(spec.element);
    case "map":
      return nestedFields(spec.value);
  }
};

/** True if any nested struct field is still unnamed (the form blocks submit). */
export const hasUnnamed = (spec: TypeSpec): boolean =>
  nestedFields(spec).some((f) => !f.name.trim() || hasUnnamed(f.type)) ||
  (typeof spec !== "string" && spec.type === "map" && hasUnnamed(spec.key));

export const isPrimitive = (spec: TypeSpec) => typeof spec === "string";
