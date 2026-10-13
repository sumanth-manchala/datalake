import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import type { TypeSpec } from "src/utils/fieldTypes";
import { Wrapper } from "src/utils/Wrapper";

import { TypeEditor } from "./TypeEditor";

let latest: TypeSpec;

const Harness = ({ initial }: { initial: TypeSpec }) => {
  const [value, setValue] = useState(initial);
  const onChange = (next: TypeSpec) => {
    latest = next;
    setValue(next);
  };
  return <TypeEditor label="Col" value={value} onChange={onChange} />;
};

describe("TypeEditor", () => {
  it("builds a list of structs", async () => {
    render(<Harness initial="string" />, { wrapper: Wrapper });
    await userEvent.selectOptions(screen.getByLabelText("Col type"), "list");
    await userEvent.selectOptions(screen.getByLabelText("Col element type"), "struct");
    await userEvent.type(screen.getByLabelText("Col element field 1 name"), "id");
    await userEvent.selectOptions(screen.getByLabelText("Col element field 1 type"), "long");
    await userEvent.click(screen.getByLabelText("Col element field 1 required"));

    expect(latest).toEqual({
      type: "list",
      element: { type: "struct", fields: [{ name: "id", type: "long", required: true }] },
    });
  });

  it("edits decimal precision and scale", async () => {
    render(<Harness initial="string" />, { wrapper: Wrapper });
    await userEvent.selectOptions(screen.getByLabelText("Col type"), "decimal");
    expect(latest).toBe("decimal(18,2)");
    await userEvent.clear(screen.getByLabelText("Col scale"));
    await userEvent.type(screen.getByLabelText("Col scale"), "4");
    expect(latest).toBe("decimal(18,4)");
  });

  it("edits map key and value", async () => {
    render(<Harness initial="string" />, { wrapper: Wrapper });
    await userEvent.selectOptions(screen.getByLabelText("Col type"), "map");
    await userEvent.selectOptions(screen.getByLabelText("Col value type"), "long");
    expect(latest).toEqual({ type: "map", key: "string", value: "long" });
  });

  it("adds and removes struct fields", async () => {
    render(<Harness initial={{ type: "struct", fields: [{ name: "a", type: "long" }] }} />, {
      wrapper: Wrapper,
    });
    expect(screen.getByRole("button", { name: "Remove col field 1" })).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Add col field" }));
    await userEvent.type(screen.getByLabelText("Col field 2 name"), "b");
    await userEvent.click(screen.getByRole("button", { name: "Remove col field 1" }));
    expect(latest).toEqual({
      type: "struct",
      fields: [{ name: "b", type: "string", required: false }],
    });
  });
});
