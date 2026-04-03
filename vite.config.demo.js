import { defineConfig } from "vite";
import { resolve } from "path";

export default defineConfig({
  base: "/parquet-as-virtual-zarr.js/",
  mode: "development",
  resolve: {
    alias: {
      "parquet-as-virtual-zarr": resolve(import.meta.dirname, "src/index.ts"),
    },
  },
  build: {
    outDir: "demo-dist",
    minify: false,
  },
});
