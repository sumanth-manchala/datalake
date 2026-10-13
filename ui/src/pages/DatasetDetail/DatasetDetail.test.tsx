import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw/http";
import { describe, expect, it, vi } from "vitest";

import { history } from "src/mocks/handlers";
import { server } from "src/mocks/server";
import { renderAt } from "src/utils/Wrapper";

const OP_ID = history[0].op_id;

describe("DatasetDetail", () => {
  it("shows the schema tab by default with the lineage column marked", async () => {
    renderAt("/datasets/sales/orders");
    expect(await screen.findByText("amount")).toBeInTheDocument();
    expect(screen.getByText("lineage")).toBeInTheDocument();
    expect(screen.getByText("Order lines", { exact: false })).toBeInTheDocument();
  });

  it("shows nested types as indented child rows", async () => {
    server.use(
      http.get("*/v1/datasets/:ns/:name/schema", () =>
        HttpResponse.json({
          schema_id: 0,
          fields: [
            {
              name: "customer",
              required: false,
              type: {
                type: "struct",
                fields: [
                  { name: "id", type: "long", required: true },
                  {
                    name: "emails",
                    type: { type: "list", element: "string", "element-required": false },
                    required: false,
                  },
                ],
              },
            },
            { name: "amount", type: "decimal(18,2)", required: false },
          ],
        }),
      ),
    );
    renderAt("/datasets/sales/orders");
    const id = (await screen.findByText("id")).closest("tr")!;
    expect(within(id).getByText("└")).toBeInTheDocument();
    expect(within(id).getByText("long")).toBeInTheDocument();
    expect(
      within(screen.getByText("customer").closest("tr")!).getByText("struct"),
    ).toBeInTheDocument();
    expect(
      within(screen.getByText("emails").closest("tr")!).getByText("list<string>"),
    ).toBeInTheDocument();
    expect(screen.getByText("decimal(18,2)")).toBeInTheDocument();
  });

  it("time-travels via the write picker and shows a banner", async () => {
    let asOf: string | null = null;
    server.use(
      http.get("*/v1/datasets/:ns/:name/schema", ({ request }) => {
        asOf = new URL(request.url).searchParams.get("as_of");
        return HttpResponse.json({ schema_id: 0, fields: [] });
      }),
    );
    const router = renderAt("/datasets/sales/orders");
    const picker = await screen.findByLabelText("View as of");
    const option = await within(picker).findByRole("option", {
      name: /overwrite · alice · day=2026-10-01/,
    });
    await userEvent.selectOptions(picker, option);

    await vi.waitFor(() => expect(asOf).toBe(OP_ID));
    expect(router.state.location.search).toContain(`as_of=${OP_ID}`);
    expect(screen.getByText(/Viewing as of .*overwrite · alice/)).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Back to current" }));
    expect(router.state.location.search).not.toContain("as_of");
    expect(screen.queryByText(/Viewing as of/)).not.toBeInTheDocument();
  });

  it("keeps an unlisted as_of from a link selectable", async () => {
    renderAt("/datasets/sales/orders?as_of=42");
    expect(await screen.findByText("Viewing as of 42")).toBeInTheDocument();
  });

  it("shows partitions", async () => {
    renderAt("/datasets/sales/orders?tab=partitions");
    expect(await screen.findByText("2026-10-01")).toBeInTheDocument();
    expect(screen.getByText("1,200")).toBeInTheDocument();
  });

  it("shows history and filters by kind", async () => {
    let kind: string | null = null;
    server.use(
      http.get("*/v1/datasets/:ns/:name/history", ({ request }) => {
        kind = new URL(request.url).searchParams.get("kind");
        return HttpResponse.json([]);
      }),
    );
    renderAt("/datasets/sales/orders?tab=history");
    expect(await screen.findByText("No operations.")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Operation kind"), "reads");
    await vi.waitFor(() => expect(kind).toBe("reads"));
  });

  it("renders op rows with op id, process and details", async () => {
    renderAt("/datasets/sales/orders?tab=history");
    const row = (await screen.findByText("overwrite")).closest("tr")!;
    expect(within(row).getByText(OP_ID.slice(0, 8))).toHaveAttribute("title", OP_ID);
    expect(within(row).getByRole("button", { name: `Copy op id ${OP_ID}` })).toBeInTheDocument();
    expect(within(row).getByText("pipeline / daily_orders / run-1")).toBeInTheDocument();
    expect(within(row).getByText(/"rows_written": 1200/)).toBeInTheDocument();
  });

  it("links a write to its schema and partitions at that point", async () => {
    const router = renderAt("/datasets/sales/orders?tab=history&version=1");
    const row = (await screen.findByText("overwrite")).closest("tr")!;
    await userEvent.click(within(row).getByRole("link", { name: "partitions" }));
    const params = new URLSearchParams(router.state.location.search);
    expect(Object.fromEntries(params)).toEqual({ tab: "partitions", as_of: OP_ID, version: "1" });
    expect(await screen.findByText(/Viewing as of .*overwrite/)).toBeInTheDocument();
  });

  it("offers no time-travel links on reads", async () => {
    server.use(
      http.get("*/v1/datasets/:ns/:name/history", () =>
        HttpResponse.json([{ ...history[0], op_kind: "read" }]),
      ),
    );
    renderAt("/datasets/sales/orders?tab=history");
    const row = (await screen.findByText("read")).closest("tr")!;
    expect(within(row).queryByRole("link")).not.toBeInTheDocument();
  });

  it("disables schema and partitions for dropped datasets", async () => {
    renderAt("/datasets/tmp/scratch?tab=history");
    await screen.findByText("dropped");
    expect(screen.getByRole("tab", { name: "Schema" })).toBeDisabled();
    expect(screen.getByRole("tab", { name: "Partitions" })).toBeDisabled();
  });

  it("shows API errors", async () => {
    server.use(
      http.get("*/v1/datasets/:ns/:name/schema", () =>
        HttpResponse.json({ detail: "snapshot 42 expired" }, { status: 404 }),
      ),
    );
    renderAt("/datasets/sales/orders?as_of=42");
    expect(await screen.findByText("snapshot 42 expired")).toBeInTheDocument();
  });
});
