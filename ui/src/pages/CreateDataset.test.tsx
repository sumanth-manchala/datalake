import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw/http";
import { describe, expect, it } from "vitest";

import { server } from "src/mocks/server";
import { renderAt } from "src/utils/Wrapper";

const fillBasics = async () => {
  await userEvent.type(screen.getByLabelText(/Namespace/), "sales");
  await userEvent.type(screen.getByLabelText(/^Name\b/), "orders");
  await userEvent.type(screen.getByLabelText("Column 1 name"), "day");
};

describe("CreateDataset", () => {
  it("requires a partition column before submitting", async () => {
    renderAt("/datasets/new");
    await fillBasics();
    expect(screen.getByRole("button", { name: "Create dataset" })).toBeDisabled();
    expect(screen.getByText(/at least one partition column/)).toBeInTheDocument();
  });

  it("posts the dataset and navigates to it", async () => {
    let body: unknown;
    server.use(
      http.post("*/v1/datasets", async ({ request }) => {
        body = await request.json();
        return HttpResponse.json(
          {
            entity_id: "e",
            version: 1,
            op_id: "o",
            warehouse_name: "local-rustfs",
            physical_identifier: "sales.orders",
          },
          { status: 201 },
        );
      }),
    );
    const router = renderAt("/datasets/new");
    await screen.findByRole("option", { name: "local-seaweedfs (local)" });
    await fillBasics();
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    await userEvent.type(screen.getByLabelText("Column 2 name"), "amount");
    await userEvent.selectOptions(screen.getByLabelText("Column 2 type"), "long");
    await userEvent.selectOptions(screen.getByLabelText("Warehouse"), "local-rustfs");
    await userEvent.click(screen.getByLabelText("day"));
    await userEvent.click(screen.getByRole("button", { name: "Create dataset" }));

    await waitFor(() => expect(router.state.location.pathname).toBe("/datasets/sales/orders"));
    expect(body).toEqual({
      namespace: "sales",
      name: "orders",
      description: null,
      warehouse_name: "local-rustfs",
      schema: [
        { name: "day", type: "string", required: false },
        { name: "amount", type: "long", required: false },
      ],
      partition_by: ["day"],
      new_version: false,
    });
  });

  it("shows API errors inline", async () => {
    server.use(
      http.post("*/v1/datasets", () =>
        HttpResponse.json({ detail: "sales.orders exists" }, { status: 409 }),
      ),
    );
    renderAt("/datasets/new");
    await fillBasics();
    await userEvent.click(screen.getByLabelText("day"));
    await userEvent.click(screen.getByRole("button", { name: "Create dataset" }));
    expect(await screen.findByText("sales.orders exists")).toBeInTheDocument();
  });

  it("drops a partition column when its field is renamed", async () => {
    renderAt("/datasets/new");
    await fillBasics();
    await userEvent.click(screen.getByLabelText("day"));
    expect(screen.getByRole("button", { name: "Create dataset" })).toBeEnabled();
    await userEvent.type(screen.getByLabelText("Column 1 name"), "s");
    expect(screen.getByRole("button", { name: "Create dataset" })).toBeDisabled();
  });

  it("posts complex types and only offers primitive columns for partitioning", async () => {
    let body: { schema?: unknown } = {};
    server.use(
      http.post("*/v1/datasets", async ({ request }) => {
        body = (await request.json()) as typeof body;
        return HttpResponse.json(
          { entity_id: "e", version: 1, op_id: "o", warehouse_name: "w", physical_identifier: "p" },
          { status: 201 },
        );
      }),
    );
    renderAt("/datasets/new");
    await fillBasics();
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    await userEvent.type(screen.getByLabelText("Column 2 name"), "customer");
    await userEvent.selectOptions(screen.getByLabelText("Column 2 type"), "struct");

    expect(screen.queryByRole("checkbox", { name: "customer" })).not.toBeInTheDocument();
    await userEvent.click(screen.getByLabelText("day"));
    // the struct's field is still unnamed
    expect(screen.getByRole("button", { name: "Create dataset" })).toBeDisabled();

    await userEvent.type(screen.getByLabelText("Column 2 field 1 name"), "id");
    await userEvent.click(screen.getByRole("button", { name: "Create dataset" }));
    await waitFor(() =>
      expect(body.schema).toEqual([
        { name: "day", type: "string", required: false },
        {
          name: "customer",
          type: { type: "struct", fields: [{ name: "id", type: "string", required: false }] },
          required: false,
        },
      ]),
    );
  });
});
