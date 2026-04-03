import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  use: {
    baseURL: "http://localhost:5173",
  },
  webServer: {
    command: "pnpm demo",
    url: "http://localhost:5173/parquet-as-virtual-zarr.js/",
    reuseExistingServer: !process.env.CI,
  },
});
