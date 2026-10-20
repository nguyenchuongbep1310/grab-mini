import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  retries: process.env.CI ? 1 : 0,
  reporter: "list",
  use: {
    trace: "on-first-retry",
    viewport: { width: 430, height: 860 },
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 430, height: 860 },
        // Fall back to system Chrome if Playwright's bundled Chromium isn't installed locally
        ...(process.env.CI ? {} : { channel: "chrome" }),
      },
    },
  ],
  webServer: [
    {
      command: "pnpm --filter @grab-mini/rider dev --port 5171 --strictPort",
      url: "http://localhost:5171",
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
    },
    {
      command: "pnpm --filter @grab-mini/driver dev --port 5172 --strictPort",
      url: "http://localhost:5172",
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
    },
    {
      command: "pnpm --filter @grab-mini/ops dev --port 5173 --strictPort",
      url: "http://localhost:5173",
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
    },
  ],
});
