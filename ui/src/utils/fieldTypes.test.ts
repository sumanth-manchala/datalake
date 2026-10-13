import { describe, expect, it } from "vitest";

import { hasUnnamed, kindOf, typeLabel, type TypeSpec } from "./fieldTypes";

const customer: TypeSpec = {
  type: "struct",
  fields: [
    { name: "id", type: "long", required: true },
    { name: "emails", type: { type: "list", element: "string" }, required: false },
  ],
};

describe("fieldTypes", () => {
  it.each<[TypeSpec, string]>([
    ["long", "long"],
    ["decimal(18,2)", "decimal(18,2)"],
    [{ type: "list", element: "string" }, "list<string>"],
    [
      { type: "map", key: "string", value: { type: "list", element: "long" } },
      "map<string,list<long>>",
    ],
    [customer, "struct"],
    [{ type: "list", element: customer }, "list<struct>"],
  ])("labels %j as %s", (spec, label) => {
    expect(typeLabel(spec)).toBe(label);
  });

  it.each<[TypeSpec, string]>([
    ["string", "string"],
    ["decimal(10,0)", "decimal"],
    [customer, "struct"],
  ])("kindOf %j = %s", (spec, kind) => {
    expect(kindOf(spec)).toBe(kind);
  });

  it.each<[TypeSpec, boolean]>([
    ["long", false],
    [customer, false],
    [{ type: "struct", fields: [{ name: " ", type: "long" }] }, true],
    [{ type: "list", element: { type: "struct", fields: [{ name: "", type: "long" }] } }, true],
    [
      { type: "map", key: { type: "struct", fields: [{ name: "", type: "long" }] }, value: "long" },
      true,
    ],
  ])("hasUnnamed %j = %s", (spec, expected) => {
    expect(hasUnnamed(spec)).toBe(expected);
  });
});
