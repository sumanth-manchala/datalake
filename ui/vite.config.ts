/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { src: "/src", "openapi-gen": "/openapi-gen" } },
  // dev only: same-origin API calls, so the control plane needs no CORS
  server: { proxy: { "/v1": "http://localhost:8000" } },
  test: {
    environment: "jsdom",
    setupFiles: ["src/testSetup.ts"],
  },
});
