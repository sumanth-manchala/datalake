import { ChakraProvider } from "@chakra-ui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactNode } from "react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";

import { queryClient } from "src/queryClient";
import { routes } from "src/router";
import { system } from "src/theme";

export const Wrapper = ({ children }: { children: ReactNode }) => (
  <ChakraProvider value={system}>
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  </ChakraProvider>
);

/** Render the real route tree at `url`; returns the router so tests can assert navigation. */
export const renderAt = (url: string) => {
  const router = createMemoryRouter(routes, { initialEntries: [url] });
  render(<RouterProvider router={router} />, { wrapper: Wrapper });
  return router;
};
