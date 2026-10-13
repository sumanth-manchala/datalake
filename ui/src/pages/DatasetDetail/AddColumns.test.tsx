import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw/http";
import { describe, expect, it } from "vitest";

import { server } from "src/mocks/server";
import { renderAt } from "src/utils/Wrapper";

const openForm = async () => {
  await userEvent.click(await screen.findByRole("button", { name: "Add columns" }));
};

describe("AddColumns", () => {
  it("adds optional columns for the viewed version and refreshes the schema", async () => {
    let body: unknown;
    let added = false;
    server.use(
      http.post("*/v1/datasets/:ns/:name/alter", async ({ request }) => {
        body = await request.json();
        added = true;
        return HttpResponse.json({ op_id: "o", schema_id: 1, fields: [] });
      }),
      http.get("*/v1/datasets/:ns/:name/schema", () =>
        HttpResponse.json({
          schema_id: added ? 1 : 0,
          fields: [
            { name: "day", type: "string", required: false },
            ...(added ? [{ name: "channel", type: "string", required: false }] : []),
          ],
        }),
      ),
    );
    renderAt("/datasets/sales/orders?version=1");
    await openForm();
    await userEvent.type(screen.getByLabelText("New column 1 name"), "channel");
    await userEvent.click(screen.getByRole("button", { name: "Another column" }));
    await userEvent.type(screen.getByLabelText("New column 2 name"), "  qty ");
    await userEvent.selectOptions(screen.getByLabelText("New column 2 type"), "long");
    await userEvent.click(screen.getByRole("button", { name: "Add columns" }));

    await waitFor(() =>
      expect(body).toEqual({ version: 1, add_columns: { channel: "string", qty: "long" } }),
    );
    expect(await screen.findByText("channel")).toBeInTheDocument();
    expect(screen.queryByLabelText("New column 1 name")).not.toBeInTheDocument();
  });

  it("explains that only optional additions are allowed", async () => {
    renderAt("/datasets/sales/orders");
    await openForm();
    expect(
      screen.getByText(/Renames, drops and type changes need a new dataset version/),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add column" })).toBeDisabled();
  });

  it("shows API errors", async () => {
    server.use(
      http.post("*/v1/datasets/:ns/:name/alter", () =>
        HttpResponse.json({ detail: "columns already exist: ['day']" }, { status: 409 }),
      ),
    );
    renderAt("/datasets/sales/orders");
    await openForm();
    await userEvent.type(screen.getByLabelText("New column 1 name"), "day");
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    expect(await screen.findByText("columns already exist: ['day']")).toBeInTheDocument();
  });

  it("is hidden while viewing a past point", async () => {
    renderAt("/datasets/sales/orders?as_of=42");
    await screen.findByText("amount");
    expect(screen.queryByRole("button", { name: "Add columns" })).not.toBeInTheDocument();
  });

  it("cancels without calling the API", async () => {
    renderAt("/datasets/sales/orders");
    await openForm();
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByRole("button", { name: "Add columns" })).toBeInTheDocument();
  });

  it("adds a nested column", async () => {
    let body: unknown;
    server.use(
      http.post("*/v1/datasets/:ns/:name/alter", async ({ request }) => {
        body = await request.json();
        return HttpResponse.json({ op_id: "o", schema_id: 1, fields: [] });
      }),
    );
    renderAt("/datasets/sales/orders?version=1");
    await openForm();
    await userEvent.type(screen.getByLabelText("New column 1 name"), "labels");
    await userEvent.selectOptions(screen.getByLabelText("New column 1 type"), "list");
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    await waitFor(() =>
      expect(body).toEqual({
        version: 1,
        add_columns: { labels: { type: "list", element: "string" } },
      }),
    );
  });
});
