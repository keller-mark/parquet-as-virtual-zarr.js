import { test, expect } from "@playwright/test";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const athletesParquet = path.resolve(__dirname, "../../fixtures/athletes.parquet");

test("loads athletes.parquet with no errors", async ({ page }) => {
  const errors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(msg.text());
  });
  page.on("pageerror", (err) => errors.push(err.message));

  await page.goto("/parquet-as-virtual-zarr.js/");

  await page.locator("#file-input").setInputFiles(athletesParquet);

  // Wait for the first-chunk table to appear.
  await expect(page.locator("#chunk-table-container table")).toBeVisible();

  expect(errors).toEqual([]);
});
