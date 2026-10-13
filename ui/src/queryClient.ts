import { QueryClient } from "@tanstack/react-query";
import createFetchClient from "openapi-fetch";
import createClient from "openapi-react-query";

import type { paths } from "openapi-gen/schema";

export const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
});

export const fetchClient = createFetchClient<paths>({
  baseUrl: window.location.origin,
  // ponytail: fixed actor until authn exists
  headers: { "X-Lakehouse-Actor": "ui" },
  // resolve fetch per call so test interceptors installed after import still apply
  fetch: (request) => globalThis.fetch(request),
});

export const $api = createClient(fetchClient);

type ApiError = { detail?: string | { loc: (string | number)[]; msg: string }[] } | undefined;

/** FastAPI error body -> one readable line. */
export const errorMessage = (error: unknown): string => {
  const detail = (error as ApiError)?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail))
    return detail.map((d) => `${d.loc.slice(1).join(".")}: ${d.msg}`).join("; ");
  return "Request failed";
};
