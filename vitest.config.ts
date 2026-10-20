import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      "@grab-mini/ui": path.resolve(__dirname, "packages/ui/src/index.ts"),
      "@grab-mini/client": path.resolve(__dirname, "packages/client/src/index.ts"),
      "@grab-mini/events": path.resolve(__dirname, "packages/events/src/index.ts"),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    include: [
      "packages/*/src/**/*.test.{ts,tsx}",
      "apps/*/src/**/*.test.{ts,tsx}",
    ],
  },
});
