import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw/http";
import { describe, expect, it } from "vitest";

import { server } from "src/mocks/server";
import { renderAt } from "src/utils/Wrapper";

describe("DatasetList", () => {
  it("lists live datasets and links to the version", async () => {
    renderAt("/datasets");
    const link = await screen.findByRole("link", { name: "sales.orders" });
    expect(link).toHaveAttribute("href", "/datasets/sales/orders?version=1");
    expect(screen.getByText("local-seaweedfs")).toBeInTheDocument();
    expect(screen.queryByText("tmp.scratch")).not.toBeInTheDocument();
  });

  it("shows dropped datasets when toggled, kept in the URL", async () => {
    const router = renderAt("/datasets");
    await screen.findByText("sales.orders");
    await userEvent.click(screen.getByLabelText("Show dropped"));
    expect(await screen.findByText("tmp.scratch")).toBeInTheDocument();
    expect(screen.getByText("dropped")).toBeInTheDocument();
    expect(router.state.location.search).toBe("?dropped=1");
  });

  it("shows the empty state", async () => {
    server.use(http.get("*/v1/datasets", () => HttpResponse.json([])));
    renderAt("/datasets");
    expect(await screen.findByText("No datasets yet.")).toBeInTheDocument();
  });

  it("shows API errors", async () => {
    server.use(
      http.get("*/v1/datasets", () => HttpResponse.json({ detail: "db down" }, { status: 500 })),
    );
    renderAt("/datasets");
    expect(await screen.findByText("db down")).toBeInTheDocument();
  });
});
